import json
import logging
import os
import shutil
import subprocess

import asyncpg

from backend.config import settings

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None

# Detect if running inside a Databricks App
IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

# Detect if a Lakebase resource binding injected PG env vars
HAS_PG_RESOURCE_BINDING = bool(os.environ.get("PGHOST"))


def _generate_db_credential() -> str:
    """Generate a Lakebase database credential via the Databricks REST API.

    Supports both Provisioned Lakebase (instance_names) and
    Autoscaling Lakebase (project/branch/endpoint).
    """
    import ssl
    import urllib.request
    from databricks.sdk import WorkspaceClient

    host = os.environ.get("DATABRICKS_HOST", "")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    w = WorkspaceClient(host=host) if host else WorkspaceClient()

    api_host = w.config.host.rstrip("/")
    auth_headers = w.config.authenticate()

    if settings.LAKEBASE_INSTANCE_NAME:
        # Provisioned Lakebase
        url = f"{api_host}/api/2.0/database/credentials"
        payload = json.dumps({
            "request_id": "app-cred",
            "instance_names": [settings.LAKEBASE_INSTANCE_NAME],
        }).encode("utf-8")
    else:
        # Autoscaling Lakebase
        endpoint_path = (
            f"projects/{settings.LAKEBASE_PROJECT}"
            f"/branches/{settings.LAKEBASE_BRANCH}"
            f"/endpoints/{settings.LAKEBASE_ENDPOINT}"
        )
        url = f"{api_host}/api/2.0/postgres/credentials"
        payload = json.dumps({"endpoint": endpoint_path}).encode("utf-8")

    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    for k, v in auth_headers.items():
        req.add_header(k, v)

    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, context=ctx) as resp:
        data = json.loads(resp.read().decode())

    token = data.get("token")
    if not token:
        raise RuntimeError(f"No token in credential response: {data}")
    logger.info(f"Generated DB credential, token length={len(token)}")
    return token


# ---------------------------------------------------------------------------
# CLI-based auth (for local development)
# ---------------------------------------------------------------------------

def _databricks_cmd():
    env_path = os.environ.get("DATABRICKS_CLI_PATH")
    if env_path:
        return env_path
    path_bin = shutil.which("databricks")
    if path_bin:
        r = subprocess.run([path_bin, "--version"], capture_output=True, text=True, timeout=5)
        version_str = r.stdout.strip().split()[-1] if r.returncode == 0 else "0.0.0"
        parts = version_str.replace("v", "").split(".")
        try:
            if int(parts[1]) >= 285:
                return path_bin
        except (IndexError, ValueError):
            pass
    brew_path = "/opt/homebrew/bin/databricks"
    if os.path.exists(brew_path):
        return brew_path
    return "databricks"


def _profile_args():
    profile = os.environ.get("DATABRICKS_CLI_PROFILE", "")
    if profile:
        return ["--profile", profile]
    return []


def _cli_get_lakebase_host() -> str:
    cli = _databricks_cmd()
    branch_path = f"projects/{settings.LAKEBASE_PROJECT}/branches/{settings.LAKEBASE_BRANCH}"
    result = subprocess.run(
        [cli, "postgres", "list-endpoints", branch_path, "--output", "json"] + _profile_args(),
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to get Lakebase endpoint: {result.stderr}")
    endpoints = json.loads(result.stdout)
    return endpoints[0]["status"]["hosts"]["host"]


def _cli_get_oauth_token() -> str:
    cli = _databricks_cmd()
    endpoint_path = (
        f"projects/{settings.LAKEBASE_PROJECT}"
        f"/branches/{settings.LAKEBASE_BRANCH}"
        f"/endpoints/{settings.LAKEBASE_ENDPOINT}"
    )
    result = subprocess.run(
        [cli, "postgres", "generate-database-credential", endpoint_path, "--output", "json"] + _profile_args(),
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to generate credential: {result.stderr}")
    return json.loads(result.stdout)["token"]


def _cli_get_user_email() -> str:
    cli = _databricks_cmd()
    result = subprocess.run(
        [cli, "current-user", "me", "--output", "json"] + _profile_args(),
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to get current user: {result.stderr}")
    return json.loads(result.stdout)["userName"]


# ---------------------------------------------------------------------------
# Pool management
# ---------------------------------------------------------------------------

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is not None and not _pool._closed:
        return _pool

    if HAS_PG_RESOURCE_BINDING:
        # Use the Lakebase resource binding with SDK-generated credential.
        host = os.environ["PGHOST"]
        port = int(os.environ.get("PGPORT", "5432"))
        user = os.environ.get("PGUSER", "")
        database = os.environ.get("PGDATABASE", "postgres")
        sslmode = os.environ.get("PGSSLMODE", "require")
        password = os.environ.get("PGPASSWORD", "") or _generate_db_credential()

        logger.info(f"Using resource binding: {host}:{port}/{database} as {user}")

        ssl_param = "require" if sslmode == "require" else True

        _pool = await asyncpg.create_pool(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            ssl=ssl_param,
            min_size=2,
            max_size=10,
            command_timeout=30,
            server_settings={'search_path': 'support_app,public'},
        )
    else:
        # Local development: use CLI to get credentials
        host = settings.LAKEBASE_HOST or _cli_get_lakebase_host()
        token = _cli_get_oauth_token()
        user = _cli_get_user_email()
        database = settings.LAKEBASE_DATABASE
        port = 5432

        logger.info(f"Using CLI auth: {host}:{port}/{database} as {user}")

        _pool = await asyncpg.create_pool(
            host=host,
            port=port,
            database=database,
            user=user,
            password=token,
            ssl="require",
            min_size=2,
            max_size=10,
            command_timeout=30,
        )

    return _pool


async def close_pool():
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def refresh_pool():
    await close_pool()
    return await get_pool()


_RETRYABLE = (asyncpg.ConnectionDoesNotExistError, asyncpg.InvalidPasswordError)


async def execute(query: str, *args):
    pool = await get_pool()
    try:
        return await pool.execute(query, *args)
    except _RETRYABLE:
        pool = await refresh_pool()
        return await pool.execute(query, *args)


async def fetch(query: str, *args) -> list[asyncpg.Record]:
    pool = await get_pool()
    try:
        return await pool.fetch(query, *args)
    except _RETRYABLE:
        pool = await refresh_pool()
        return await pool.fetch(query, *args)


async def fetchrow(query: str, *args) -> asyncpg.Record | None:
    pool = await get_pool()
    try:
        return await pool.fetchrow(query, *args)
    except _RETRYABLE:
        pool = await refresh_pool()
        return await pool.fetchrow(query, *args)


async def fetchval(query: str, *args):
    pool = await get_pool()
    try:
        return await pool.fetchval(query, *args)
    except _RETRYABLE:
        pool = await refresh_pool()
        return await pool.fetchval(query, *args)
