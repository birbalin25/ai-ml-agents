"""
Centralized configuration for Databricks Platform Insights.

All runtime configuration is read from environment variables,
which are set via databricks.yml bundle variables at deploy time.
"""

import os

# ---------------------------------------------------------------------------
# App Environment
# ---------------------------------------------------------------------------
APP_ENVIRONMENT = os.environ.get("APP_ENVIRONMENT", "dev")

# ---------------------------------------------------------------------------
# Lakebase Connection
# ---------------------------------------------------------------------------
LAKEBASE_HOST = os.environ.get("LAKEBASE_HOST")
LAKEBASE_PORT = int(os.environ.get("LAKEBASE_PORT", "5432"))
LAKEBASE_DATABASE = os.environ.get("LAKEBASE_DATABASE", "databricks_postgres")

# ---------------------------------------------------------------------------
# Lakebase Schema (where synced MV tables land)
# ---------------------------------------------------------------------------
LAKEBASE_SCHEMA = "admin_insight"

# ---------------------------------------------------------------------------
# App Settings
# ---------------------------------------------------------------------------
QUERY_CACHE_TTL = int(os.environ.get("QUERY_CACHE_TTL_SECONDS", "300"))
