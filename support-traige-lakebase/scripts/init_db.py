"""Initialize the Lakebase database schema."""

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))
HAS_PG_RESOURCE_BINDING = bool(os.environ.get("PGHOST"))


def _generate_db_credential():
    """Generate a Lakebase database credential via the Databricks REST API."""
    import ssl
    import urllib.request
    from databricks.sdk import WorkspaceClient

    host_env = os.environ.get("DATABRICKS_HOST", "")
    if host_env and not host_env.startswith("http"):
        host_env = f"https://{host_env}"
    w = WorkspaceClient(host=host_env) if host_env else WorkspaceClient()

    api_host = w.config.host.rstrip("/")
    auth_headers = w.config.authenticate()

    project = os.environ.get("LAKEBASE_PROJECT", "support-lakebase")
    branch = os.environ.get("LAKEBASE_BRANCH", "production")
    endpoint_name = os.environ.get("LAKEBASE_ENDPOINT", "primary")
    endpoint_path = f"projects/{project}/branches/{branch}/endpoints/{endpoint_name}"

    url = f"{api_host}/api/2.0/postgres/credentials"
    payload = json.dumps({"endpoint": endpoint_path}).encode("utf-8")

    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    for k, v in auth_headers.items():
        req.add_header(k, v)

    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, context=ctx) as resp:
        data = json.loads(resp.read().decode())

    return data.get("token", "")


def _get_connection_params_resource_binding():
    host = os.environ["PGHOST"]
    port = int(os.environ.get("PGPORT", "5432"))
    user = os.environ.get("PGUSER", "")
    database = os.environ.get("PGDATABASE", "postgres")
    password = os.environ.get("PGPASSWORD", "") or _generate_db_credential()
    sslmode = os.environ.get("PGSSLMODE", "require")
    return host, port, user, password, database, sslmode


def _get_connection_params_cli():
    import shutil

    project = os.environ.get("LAKEBASE_PROJECT", "support-lakebase")
    branch = os.environ.get("LAKEBASE_BRANCH", "production")
    endpoint = os.environ.get("LAKEBASE_ENDPOINT", "primary")
    database = os.environ.get("LAKEBASE_DATABASE", "postgres")

    profile_args = []
    profile = os.environ.get("DATABRICKS_CLI_PROFILE", "vm")
    if profile:
        profile_args = ["--profile", profile]

    cli = os.environ.get("DATABRICKS_CLI_PATH") or shutil.which("databricks") or "/opt/homebrew/bin/databricks"

    branch_path = f"projects/{project}/branches/{branch}"
    result = subprocess.run(
        [cli, "postgres", "list-endpoints", branch_path, "--output", "json"] + profile_args,
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        print(f"Error getting endpoint: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    endpoints = json.loads(result.stdout)
    host = endpoints[0]["status"]["hosts"]["host"]

    endpoint_path = f"projects/{project}/branches/{branch}/endpoints/{endpoint}"
    result = subprocess.run(
        [cli, "postgres", "generate-database-credential", endpoint_path, "--output", "json"] + profile_args,
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        print(f"Error generating credential: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    token = json.loads(result.stdout)["token"]

    result = subprocess.run(
        [cli, "current-user", "me", "--output", "json"] + profile_args,
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        print(f"Error getting user: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    email = json.loads(result.stdout)["userName"]

    return host, 5432, email, token, database, "require"


async def _apply_schema(host, port, user, password, database, sslmode, schema_sql):
    """Apply schema to the target database."""
    import asyncpg

    ssl_param = "require" if sslmode == "require" else True

    conn = await asyncpg.connect(
        host=host, port=port, database=database,
        user=user, password=password, ssl=ssl_param,
    )
    try:
        await conn.execute(schema_sql)
        print("Schema applied successfully!")
    finally:
        await conn.close()


def main():
    print("Initializing Support Portal database...")

    if HAS_PG_RESOURCE_BINDING:
        host, port, user, password, database, sslmode = _get_connection_params_resource_binding()
    else:
        host, port, user, password, database, sslmode = _get_connection_params_cli()

    schema_path = Path(__file__).parent.parent / "backend" / "schema.sql"
    schema_sql = schema_path.read_text()

    print(f"Host: {host}")
    print(f"User: {user}")
    print(f"Database: {database}")

    asyncio.run(_apply_schema(host, port, user, password, database, sslmode, schema_sql))
    print("Database initialization complete!")


if __name__ == "__main__":
    main()
