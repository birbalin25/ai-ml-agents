"""
Lakebase (PostgreSQL) client for Databricks Platform Insights.
Sub-second reads from pre-aggregated MVs synced to Lakebase.
Uses Databricks SDK to generate OAuth tokens for authentication.
"""

import logging
from datetime import date, datetime
from decimal import Decimal
import pandas as pd
import streamlit as st
import psycopg2
from databricks.sdk import WorkspaceClient

from config import (
    LAKEBASE_HOST,
    LAKEBASE_PORT,
    LAKEBASE_DATABASE,
    QUERY_CACHE_TTL,
)

logger = logging.getLogger(__name__)


@st.cache_resource
def _get_workspace_client() -> WorkspaceClient:
    return WorkspaceClient()


@st.cache_resource
def _get_lakebase_user() -> str:
    """Discover the app's service principal client ID for use as PG username."""
    w = _get_workspace_client()
    me = w.current_user.me()
    user = me.user_name
    logger.info(f"Resolved Lakebase user: {user}")
    return user


def _get_lakebase_token() -> str:
    """Generate a fresh OAuth token for Lakebase auth via Databricks SDK."""
    w = _get_workspace_client()
    headers = w.config.authenticate()
    auth = headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:]
    return auth


def _get_connection():
    """Get a fresh PostgreSQL connection with a current OAuth token."""
    return psycopg2.connect(
        host=LAKEBASE_HOST,
        port=LAKEBASE_PORT,
        database=LAKEBASE_DATABASE,
        user=_get_lakebase_user(),
        password=_get_lakebase_token(),
        sslmode="require",
    )


def execute_query(sql: str, params=None) -> pd.DataFrame:
    """Execute a SQL query against Lakebase and return results as a DataFrame."""
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description:
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                df = pd.DataFrame(rows, columns=columns)
                for col in df.columns:
                    if df[col].empty:
                        continue
                    sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                    if sample is None:
                        continue
                    if isinstance(sample, Decimal):
                        df[col] = pd.to_numeric(df[col], errors="ignore")
                    elif isinstance(sample, (datetime, date)):
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                return df
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Query execution error: {e}")
        conn.rollback()
        return pd.DataFrame()
    finally:
        conn.close()


@st.cache_data(ttl=QUERY_CACHE_TTL, show_spinner="Querying Lakebase...")
def cached_query(sql: str, params=None) -> pd.DataFrame:
    """Execute and cache a SQL query result."""
    return execute_query(sql, params)


def run_query(query) -> pd.DataFrame:
    """
    Primary entry point for running queries.
    Accepts either a raw SQL string or a (sql, params) tuple.
    """
    if isinstance(query, tuple):
        sql, params = query
        return cached_query(sql, params)
    return cached_query(query)
