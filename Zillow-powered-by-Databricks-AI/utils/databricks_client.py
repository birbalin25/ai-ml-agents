"""Shared Databricks SDK initialization for Databricks Apps."""

import os
from functools import lru_cache
from databricks.sdk import WorkspaceClient


@lru_cache(maxsize=1)
def get_workspace_client() -> WorkspaceClient:
    """Return a cached Databricks WorkspaceClient.

    When running as a Databricks App, the service principal's OAuth
    credentials are injected automatically via environment variables.
    For local dev, set DATABRICKS_HOST and DATABRICKS_TOKEN.
    """
    return WorkspaceClient()


def get_databricks_host() -> str:
    """Return the Databricks workspace host URL."""
    client = get_workspace_client()
    return client.config.host.rstrip("/")


def get_token() -> str:
    """Return a valid auth token for the OpenAI-compatible client.

    Works with both PAT (local dev) and OAuth (Databricks App).
    """
    client = get_workspace_client()
    # authenticate() returns a dict of HTTP headers
    headers = client.config.authenticate()
    auth = headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:]
    return auth
