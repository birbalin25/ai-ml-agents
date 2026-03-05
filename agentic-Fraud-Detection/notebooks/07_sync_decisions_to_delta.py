# Databricks notebook source
# MAGIC %md
# MAGIC # Reverse Sync: Lakebase → Delta (Analyst Decisions)
# MAGIC
# MAGIC Refreshes the **operational** Delta table `fraud_operations.real_time_fraud_triage`
# MAGIC by combining the DLT materialized view (latest risk scoring) with analyst decisions
# MAGIC from Lakebase. This ensures Genie Space users see up-to-date data.
# MAGIC
# MAGIC **Architecture:**
# MAGIC - `fraud_detection.real_time_fraud_triage` = DLT materialized view (read-only)
# MAGIC - `fraud_operations.real_time_fraud_triage` = mutable Delta table for Genie (this script writes here)
# MAGIC - Lakebase = source of truth for analyst decisions (BLOCK/RELEASE/ESCALATE)
# MAGIC
# MAGIC **Runs after:** Lakebase sync (Task 3 in the Fraud Triage Workflow)

# COMMAND ----------

# MAGIC %pip install psycopg2-binary databricks-sdk --upgrade --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import psycopg2
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# Configuration
dbutils.widgets.text("catalog", "serverless_bir_catalog")
CATALOG = dbutils.widgets.get("catalog")
LAKEBASE_INSTANCE = "fraud-triage-ops"
LAKEBASE_HOST = "instance-2af707c9-c7d7-4bc9-91b0-632db640ccb4.database.cloud.databricks.com"
LAKEBASE_DB = "fraud_ops"
LAKEBASE_USER = "birbal.das@databricks.com"

DLT_VIEW = f"{CATALOG}.fraud_detection.real_time_fraud_triage"
OPS_TABLE = f"{CATALOG}.fraud_operations.real_time_fraud_triage"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Refresh operational table from DLT materialized view

# COMMAND ----------

# Refresh the operational Delta table with latest data from DLT
# This is a full overwrite — the DLT MV has the latest risk scoring
spark.sql(f"""
    CREATE OR REPLACE TABLE {OPS_TABLE}
    USING DELTA
    CLUSTER BY (risk_category, automated_action, user_id)
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
    AS SELECT * FROM {DLT_VIEW}
""")

ops_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {OPS_TABLE}").collect()[0].cnt
print(f"Operational table refreshed from DLT: {ops_count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Fetch analyst decisions from Lakebase

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


conn = get_pg_connection()
cur = conn.cursor()

cur.execute("""
    SELECT transaction_id, analyst_decision, analyst_notes,
           risk_category, automated_action, reviewed_at::text
    FROM real_time_fraud_triage
    WHERE analyst_decision IS NOT NULL
""")

decisions = cur.fetchall()
print(f"Found {len(decisions)} analyst decisions in Lakebase")

cur.close()
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Merge analyst decisions into operational table

# COMMAND ----------

if len(decisions) == 0:
    print("No analyst decisions to merge. Operational table matches DLT output.")
else:
    from pyspark.sql.types import StructType, StructField, StringType

    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("analyst_decision", StringType(), True),
        StructField("analyst_notes", StringType(), True),
        StructField("risk_category", StringType(), True),
        StructField("automated_action", StringType(), True),
        StructField("reviewed_at", StringType(), True),
    ])

    decisions_df = spark.createDataFrame(
        [(txn_id, decision, notes, cat, action, reviewed)
         for txn_id, decision, notes, cat, action, reviewed in decisions],
        schema=schema,
    )

    decisions_df.createOrReplaceTempView("lakebase_decisions")

    merge_result = spark.sql(f"""
        MERGE INTO {OPS_TABLE} AS target
        USING lakebase_decisions AS source
        ON target.transaction_id = source.transaction_id
        WHEN MATCHED THEN
            UPDATE SET
                target.analyst_decision = source.analyst_decision,
                target.analyst_notes = source.analyst_notes,
                target.risk_category = source.risk_category,
                target.automated_action = source.automated_action,
                target.reviewed_at = source.reviewed_at
    """)
    merge_result.show()

    merged_count = spark.sql(
        f"SELECT COUNT(*) as cnt FROM {OPS_TABLE} WHERE analyst_decision IS NOT NULL"
    ).collect()[0].cnt
    print(f"Analyst decisions merged: {merged_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

total = spark.sql(f"SELECT COUNT(*) as cnt FROM {OPS_TABLE}").collect()[0].cnt
reviewed = spark.sql(
    f"SELECT COUNT(*) as cnt FROM {OPS_TABLE} WHERE analyst_decision IS NOT NULL"
).collect()[0].cnt
pending = spark.sql(
    f"SELECT COUNT(*) as cnt FROM {OPS_TABLE} WHERE analyst_decision IS NULL AND risk_category IN ('RED', 'YELLOW')"
).collect()[0].cnt

dist = spark.sql(f"""
    SELECT risk_category, automated_action,
           COUNT(*) as total,
           SUM(CASE WHEN analyst_decision IS NOT NULL THEN 1 ELSE 0 END) as reviewed
    FROM {OPS_TABLE}
    GROUP BY risk_category, automated_action
    ORDER BY total DESC
""").collect()

print(f"\n{'=' * 55}")
print(f"REVERSE SYNC COMPLETE")
print(f"{'=' * 55}")
print(f"DLT source rows:             {ops_count}")
print(f"Lakebase decisions fetched:  {len(decisions)}")
print(f"Operational table total:     {total}")
print(f"Analyst-reviewed rows:       {reviewed}")
print(f"Pending review (RED/YELLOW): {pending}")
print(f"\nDistribution:")
print(f"  {'Category':<10} {'Action':<15} {'Total':>7} {'Reviewed':>10}")
print(f"  {'-'*10} {'-'*15} {'-'*7} {'-'*10}")
for row in dist:
    print(f"  {row.risk_category:<10} {row.automated_action:<15} {row.total:>7} {row.reviewed:>10}")
print(f"\nGenie Space now reflects analyst decisions.")
