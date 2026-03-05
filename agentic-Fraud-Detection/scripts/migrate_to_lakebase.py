"""
Migrate data from Delta tables (SQL Warehouse) to Lakebase Postgres.
One-time migration script for real_time_fraud_triage and active_session_risks.

Usage:
    pip install databricks-sdk psycopg2-binary
    python scripts/migrate_to_lakebase.py
"""

import os
import subprocess
import json
import psycopg2
from psycopg2.extras import execute_values
from databricks.sdk import WorkspaceClient

# Config
CATALOG = os.getenv("CATALOG", "serverless_bir_catalog")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "19be9738b181575a")
LAKEBASE_HOST = os.getenv("LAKEBASE_HOST", "instance-2af707c9-c7d7-4bc9-91b0-632db640ccb4.database.cloud.databricks.com")
LAKEBASE_DB = os.getenv("LAKEBASE_DB", "fraud_ops")
LAKEBASE_INSTANCE = "fraud-triage-ops"
CLI_PATH = "/opt/homebrew/bin/databricks"
PROFILE = "vm"
BATCH_SIZE = 1000


def get_pg_token():
    """Generate OAuth token for Provisioned Lakebase via CLI."""
    result = subprocess.run(
        [CLI_PATH, "database", "generate-database-credential",
         "--json", json.dumps({"instance_names": [LAKEBASE_INSTANCE]}),
         "--profile", PROFILE, "-o", "json"],
        capture_output=True, text=True
    )
    data = json.loads(result.stdout)
    return data["token"]


def get_pg_connection():
    """Create a psycopg2 connection to Lakebase."""
    token = get_pg_token()
    return psycopg2.connect(
        host=LAKEBASE_HOST,
        port=5432,
        dbname=LAKEBASE_DB,
        user="birbal.das@databricks.com",
        password=token,
        sslmode="require",
    )


def fetch_delta_rows(w, table, offset=0, limit=5000):
    """Fetch rows from Delta table via SQL Warehouse."""
    sql = f"SELECT * FROM {CATALOG}.fraud_operations.{table} ORDER BY 1 LIMIT {limit} OFFSET {offset}"
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=sql, wait_timeout="50s"
    )
    if resp.status.state.value != "SUCCEEDED":
        raise RuntimeError(f"Query failed: {resp.status.error}")
    columns = [col.name for col in resp.manifest.schema.columns]
    rows = resp.result.data_array or []
    return columns, rows


def migrate_table(w, table, insert_sql, transform_fn):
    """Migrate all rows from a Delta table to Lakebase Postgres."""
    conn = get_pg_connection()
    cur = conn.cursor()

    offset = 0
    total_inserted = 0
    while True:
        columns, rows = fetch_delta_rows(w, table, offset=offset, limit=BATCH_SIZE)
        if not rows:
            break

        col_map = {name: idx for idx, name in enumerate(columns)}
        values = [transform_fn(row, col_map) for row in rows]

        execute_values(cur, insert_sql, values, page_size=BATCH_SIZE)
        conn.commit()

        total_inserted += len(rows)
        print(f"  Inserted {total_inserted} rows into {table}...")

        if len(rows) < BATCH_SIZE:
            break
        offset += BATCH_SIZE

    cur.close()
    conn.close()
    return total_inserted


def transform_triage(row, col_map):
    """Transform a Delta row to Postgres tuple for real_time_fraud_triage."""
    def val(name):
        return row[col_map[name]] if col_map.get(name) is not None else None

    return (
        val("transaction_id"),
        val("user_id"),
        float(val("amount")) if val("amount") else None,
        val("txn_type"),
        float(val("risk_score")) if val("risk_score") else None,
        val("risk_category"),
        val("automated_action"),
        val("explanation"),
        val("risk_factors"),
        val("analyst_decision"),
        val("analyst_notes"),
        val("created_at"),
        val("reviewed_at"),
        int(val("ttl_decision_ms")) if val("ttl_decision_ms") else None,
    )


def transform_session(row, col_map):
    """Transform a Delta row to Postgres tuple for active_session_risks."""
    def val(name):
        return row[col_map[name]] if col_map.get(name) is not None else None

    return (
        val("session_id"),
        val("user_id"),
        int(val("current_risk")) if val("current_risk") else None,
        val("ip_address"),
        val("geo_location"),
        val("last_activity"),
        val("is_blocked") in ("true", "True", True, "1") if val("is_blocked") else False,
    )


def verify_counts(w, conn, table):
    """Compare row counts between Delta and Postgres."""
    # Delta count
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=f"SELECT COUNT(*) as cnt FROM {CATALOG}.fraud_operations.{table}",
        wait_timeout="30s",
    )
    delta_count = int(resp.result.data_array[0][0])

    # Postgres count
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    pg_count = cur.fetchone()[0]
    cur.close()

    print(f"  {table}: Delta={delta_count}, Postgres={pg_count}, Match={'YES' if delta_count == pg_count else 'NO'}")
    return delta_count == pg_count


def main():
    w = WorkspaceClient(profile=PROFILE)

    print("=== Migrating real_time_fraud_triage ===")
    triage_insert = """
        INSERT INTO real_time_fraud_triage
        (transaction_id, user_id, amount, txn_type, risk_score, risk_category,
         automated_action, explanation, risk_factors, analyst_decision, analyst_notes,
         created_at, reviewed_at, ttl_decision_ms)
        VALUES %s
        ON CONFLICT (transaction_id) DO NOTHING
    """
    triage_count = migrate_table(w, "real_time_fraud_triage", triage_insert, transform_triage)
    print(f"  Total inserted: {triage_count}")

    print("\n=== Migrating active_session_risks ===")
    session_insert = """
        INSERT INTO active_session_risks
        (session_id, user_id, current_risk, ip_address, geo_location, last_activity, is_blocked)
        VALUES %s
        ON CONFLICT (session_id) DO NOTHING
    """
    session_count = migrate_table(w, "active_session_risks", session_insert, transform_session)
    print(f"  Total inserted: {session_count}")

    print("\n=== Verifying row counts ===")
    conn = get_pg_connection()
    verify_counts(w, conn, "real_time_fraud_triage")
    verify_counts(w, conn, "active_session_risks")
    conn.close()

    print("\nMigration complete!")


if __name__ == "__main__":
    main()
