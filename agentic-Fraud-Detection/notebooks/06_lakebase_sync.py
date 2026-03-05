# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Sync: Delta → Provisioned Lakebase (Postgres)
# MAGIC
# MAGIC Syncs `real_time_fraud_triage` from the DLT-managed Delta table to Lakebase Postgres.
# MAGIC
# MAGIC **Key behavior:** Uses `INSERT ... ON CONFLICT (transaction_id) DO NOTHING` so that
# MAGIC analyst decisions already in Lakebase are preserved. Only new transactions are added.
# MAGIC
# MAGIC **Runs after:** DLT pipeline (Task 2 in the Fraud Triage Workflow)

# COMMAND ----------

# MAGIC %pip install psycopg2-binary databricks-sdk --upgrade --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import psycopg2
from psycopg2.extras import execute_values
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# Configuration — base_parameters arrive as widgets, not spark.conf
dbutils.widgets.text("catalog", "serverless_bir_catalog")
CATALOG = dbutils.widgets.get("catalog")
LAKEBASE_INSTANCE = "fraud-triage-ops"
LAKEBASE_HOST = "instance-2af707c9-c7d7-4bc9-91b0-632db640ccb4.database.cloud.databricks.com"
LAKEBASE_DB = "fraud_ops"
LAKEBASE_USER = "birbal.das@databricks.com"
BATCH_SIZE = 2000

DELTA_TABLE = f"{CATALOG}.fraud_detection.real_time_fraud_triage"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Lakebase Credential

# COMMAND ----------

def get_pg_token():
    """Generate OAuth token for Provisioned Lakebase via Databricks SDK."""
    w = WorkspaceClient()
    cred = w.database.generate_database_credential(instance_names=[LAKEBASE_INSTANCE])
    return cred.token


def get_pg_connection():
    """Create a psycopg2 connection to Lakebase Postgres."""
    token = get_pg_token()
    return psycopg2.connect(
        host=LAKEBASE_HOST,
        port=5432,
        dbname=LAKEBASE_DB,
        user=LAKEBASE_USER,
        password=token,
        sslmode="require",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Delta Table

# COMMAND ----------

delta_df = spark.read.table(DELTA_TABLE)
total_delta = delta_df.count()
print(f"Delta table {DELTA_TABLE}: {total_delta} rows")

# Collect all rows as list of dicts
rows = delta_df.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync to Lakebase

# COMMAND ----------

INSERT_SQL = """
    INSERT INTO real_time_fraud_triage
    (transaction_id, user_id, amount, txn_type, risk_score, risk_category,
     automated_action, explanation, risk_factors, analyst_decision, analyst_notes,
     created_at, reviewed_at, ttl_decision_ms)
    VALUES %s
    ON CONFLICT (transaction_id) DO NOTHING
"""


def transform_row(row):
    """Convert a Spark Row to a Postgres-ready tuple."""
    return (
        row.transaction_id,
        row.user_id,
        float(row.amount) if row.amount is not None else None,
        row.txn_type,
        int(row.risk_score) if row.risk_score is not None else None,
        row.risk_category,
        row.automated_action,
        row.explanation,
        row.risk_factors,
        row.analyst_decision,
        row.analyst_notes,
        str(row.created_at) if row.created_at is not None else None,
        str(row.reviewed_at) if row.reviewed_at is not None else None,
        int(row.ttl_decision_ms) if row.ttl_decision_ms is not None else None,
    )


conn = get_pg_connection()
cur = conn.cursor()

inserted = 0
skipped = 0

for i in range(0, len(rows), BATCH_SIZE):
    batch = rows[i : i + BATCH_SIZE]
    values = [transform_row(r) for r in batch]

    cur_before = conn.cursor()
    cur_before.execute("SELECT COUNT(*) FROM real_time_fraud_triage")
    count_before = cur_before.fetchone()[0]
    cur_before.close()

    execute_values(cur, INSERT_SQL, values, page_size=BATCH_SIZE)
    conn.commit()

    cur_after = conn.cursor()
    cur_after.execute("SELECT COUNT(*) FROM real_time_fraud_triage")
    count_after = cur_after.fetchone()[0]
    cur_after.close()

    batch_inserted = count_after - count_before
    batch_skipped = len(batch) - batch_inserted
    inserted += batch_inserted
    skipped += batch_skipped

    print(f"  Batch {i // BATCH_SIZE + 1}: {len(batch)} attempted, {batch_inserted} inserted, {batch_skipped} skipped (already existed)")

cur.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM real_time_fraud_triage")
pg_total = cur.fetchone()[0]

cur.execute("""
    SELECT risk_category, COUNT(*) as cnt
    FROM real_time_fraud_triage
    GROUP BY risk_category ORDER BY cnt DESC
""")
distribution = cur.fetchall()

cur.execute("""
    SELECT COUNT(*) FROM real_time_fraud_triage
    WHERE analyst_decision IS NOT NULL
""")
reviewed = cur.fetchone()[0]
cur.close()
conn.close()

print(f"\n{'=' * 50}")
print(f"SYNC COMPLETE")
print(f"{'=' * 50}")
print(f"Delta source rows:     {total_delta}")
print(f"New rows inserted:     {inserted}")
print(f"Skipped (existing):    {skipped}")
print(f"Lakebase total rows:   {pg_total}")
print(f"Analyst-reviewed rows: {reviewed} (preserved)")
print(f"\nDistribution in Lakebase:")
for cat, cnt in distribution:
    print(f"  {cat}: {cnt}")
