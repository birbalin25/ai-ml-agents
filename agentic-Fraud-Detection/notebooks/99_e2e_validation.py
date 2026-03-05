# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End Validation: Analyst Decision Round-Trip
# MAGIC Tests the full flow: Lakebase → analyst decision → reverse sync → Delta (Genie)

# COMMAND ----------

# MAGIC %pip install psycopg2-binary databricks-sdk --upgrade --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import psycopg2
from databricks.sdk import WorkspaceClient

CATALOG = "serverless_bir_catalog"
LAKEBASE_INSTANCE = "fraud-triage-ops"
LAKEBASE_HOST = "instance-2af707c9-c7d7-4bc9-91b0-632db640ccb4.database.cloud.databricks.com"
LAKEBASE_DB = "fraud_ops"
LAKEBASE_USER = "birbal.das@databricks.com"
DLT_VIEW = f"{CATALOG}.fraud_detection.real_time_fraud_triage"
OPS_TABLE = f"{CATALOG}.fraud_operations.real_time_fraud_triage"

def get_pg_connection():
    w = WorkspaceClient()
    cred = w.database.generate_database_credential(instance_names=[LAKEBASE_INSTANCE])
    return psycopg2.connect(host=LAKEBASE_HOST, port=5432, dbname=LAKEBASE_DB,
                            user=LAKEBASE_USER, password=cred.token, sslmode="require")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Check current state

# COMMAND ----------

conn = get_pg_connection()
cur = conn.cursor()

# Lakebase stats
cur.execute("SELECT COUNT(*) FROM real_time_fraud_triage")
lb_total = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM real_time_fraud_triage WHERE analyst_decision IS NOT NULL")
lb_reviewed = cur.fetchone()[0]

# Find a YELLOW transaction with no decision to use as test
cur.execute("""
    SELECT transaction_id, risk_score, risk_category, automated_action
    FROM real_time_fraud_triage
    WHERE risk_category = 'YELLOW' AND analyst_decision IS NULL
    ORDER BY risk_score DESC LIMIT 1
""")
test_txn = cur.fetchone()

# Ops table stats
ops_total = spark.sql(f"SELECT COUNT(*) as c FROM {OPS_TABLE}").collect()[0].c
ops_reviewed = spark.sql(f"SELECT COUNT(*) as c FROM {OPS_TABLE} WHERE analyst_decision IS NOT NULL").collect()[0].c

print(f"=== BEFORE TEST ===")
print(f"Lakebase:  {lb_total} total, {lb_reviewed} reviewed")
print(f"Ops Delta: {ops_total} total, {ops_reviewed} reviewed")
print(f"Test txn:  {test_txn}")

cur.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Simulate analyst decision (RELEASE) in Lakebase

# COMMAND ----------

if test_txn is None:
    print("No unreviewed YELLOW transactions found. Skipping test.")
    dbutils.notebook.exit("SKIP - no test candidates")

test_txn_id = test_txn[0]
print(f"Making RELEASE decision on: {test_txn_id} (was {test_txn[2]}/{test_txn[3]}, score={test_txn[1]})")

cur = conn.cursor()
cur.execute("""
    UPDATE real_time_fraud_triage
    SET analyst_decision = 'RELEASE',
        analyst_notes = 'E2E validation test - auto released',
        risk_category = 'GREEN',
        automated_action = 'ALLOW',
        reviewed_at = NOW()
    WHERE transaction_id = %s
""", (test_txn_id,))
conn.commit()

# Verify in Lakebase
cur.execute("SELECT analyst_decision, risk_category, automated_action, analyst_notes FROM real_time_fraud_triage WHERE transaction_id = %s", (test_txn_id,))
lb_result = cur.fetchone()
print(f"Lakebase after update: decision={lb_result[0]}, category={lb_result[1]}, action={lb_result[2]}")

cur.execute("SELECT COUNT(*) FROM real_time_fraud_triage WHERE analyst_decision IS NOT NULL")
lb_reviewed_after = cur.fetchone()[0]
print(f"Lakebase reviewed count: {lb_reviewed} → {lb_reviewed_after}")

cur.close()
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify Ops Delta table does NOT yet have the decision

# COMMAND ----------

before_check = spark.sql(f"""
    SELECT analyst_decision, risk_category, automated_action
    FROM {OPS_TABLE} WHERE transaction_id = '{test_txn_id}'
""").collect()

if before_check:
    row = before_check[0]
    print(f"Ops Delta BEFORE reverse sync: decision={row.analyst_decision}, category={row.risk_category}, action={row.automated_action}")
else:
    print(f"Transaction {test_txn_id} not found in Ops table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Run reverse sync (inline — same logic as 07_sync_decisions_to_delta)

# COMMAND ----------

# Refresh ops table from DLT MV
spark.sql(f"""
    CREATE OR REPLACE TABLE {OPS_TABLE}
    USING DELTA
    CLUSTER BY (risk_category, automated_action, user_id)
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.autoOptimize.optimizeWrite' = 'true')
    AS SELECT * FROM {DLT_VIEW}
""")
print(f"Ops table refreshed from DLT MV")

# Fetch decisions from Lakebase
conn = get_pg_connection()
cur = conn.cursor()
cur.execute("""
    SELECT transaction_id, analyst_decision, analyst_notes,
           risk_category, automated_action, reviewed_at::text
    FROM real_time_fraud_triage WHERE analyst_decision IS NOT NULL
""")
decisions = cur.fetchall()
cur.close()
conn.close()
print(f"Fetched {len(decisions)} decisions from Lakebase")

# Merge decisions
from pyspark.sql.types import StructType, StructField, StringType
schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("analyst_decision", StringType(), True),
    StructField("analyst_notes", StringType(), True),
    StructField("risk_category", StringType(), True),
    StructField("automated_action", StringType(), True),
    StructField("reviewed_at", StringType(), True),
])
decisions_df = spark.createDataFrame(decisions, schema=schema)
decisions_df.createOrReplaceTempView("lakebase_decisions")

spark.sql(f"""
    MERGE INTO {OPS_TABLE} AS target
    USING lakebase_decisions AS source
    ON target.transaction_id = source.transaction_id
    WHEN MATCHED THEN UPDATE SET
        target.analyst_decision = source.analyst_decision,
        target.analyst_notes = source.analyst_notes,
        target.risk_category = source.risk_category,
        target.automated_action = source.automated_action,
        target.reviewed_at = source.reviewed_at
""")
print("MERGE complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify the decision now appears in Ops Delta (Genie source)

# COMMAND ----------

after_check = spark.sql(f"""
    SELECT analyst_decision, risk_category, automated_action, analyst_notes
    FROM {OPS_TABLE} WHERE transaction_id = '{test_txn_id}'
""").collect()

ops_reviewed_after = spark.sql(f"SELECT COUNT(*) as c FROM {OPS_TABLE} WHERE analyst_decision IS NOT NULL").collect()[0].c

all_decisions = spark.sql(f"""
    SELECT transaction_id, risk_score, risk_category, automated_action, analyst_decision, analyst_notes
    FROM {OPS_TABLE} WHERE analyst_decision IS NOT NULL ORDER BY risk_score DESC
""").collect()

print(f"\n{'='*60}")
print(f"E2E VALIDATION RESULTS")
print(f"{'='*60}")

if after_check:
    row = after_check[0]
    passed = row.analyst_decision == "RELEASE" and row.risk_category == "GREEN"
    status = "PASS" if passed else "FAIL"
    print(f"\n[{status}] Test transaction {test_txn_id}:")
    print(f"  analyst_decision = {row.analyst_decision} (expected: RELEASE)")
    print(f"  risk_category    = {row.risk_category} (expected: GREEN)")
    print(f"  automated_action = {row.automated_action} (expected: ALLOW)")
    print(f"  analyst_notes    = {row.analyst_notes}")
else:
    print(f"\n[FAIL] Transaction {test_txn_id} not found in Ops table!")

print(f"\nOps Delta reviewed count: {ops_reviewed} → {ops_reviewed_after}")
print(f"\nAll analyst decisions in Ops Delta ({len(all_decisions)} total):")
for d in all_decisions:
    print(f"  {d.transaction_id} | score={d.risk_score} | {d.risk_category} | {d.automated_action} | {d.analyst_decision} | {d.analyst_notes}")

# Genie-equivalent query: pending review
pending = spark.sql(f"""
    SELECT COUNT(*) as c FROM {OPS_TABLE}
    WHERE risk_category IN ('RED','YELLOW') AND analyst_decision IS NULL
""").collect()[0].c
print(f"\nGenie 'pending review' count: {pending}")
print(f"{'='*60}")
