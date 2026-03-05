# Databricks notebook source
# MAGIC %md
# MAGIC # Fraud Triage Agent - End-to-End Validation
# MAGIC
# MAGIC Run this notebook to validate all components of the Agentic Fraud Triage solution.
# MAGIC Each cell tests a different component and shows PASS/FAIL status.

# COMMAND ----------

CATALOG = "serverless_bir_catalog"
print(f"Validating against catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: Bronze Tables - Raw Data Integrity

# COMMAND ----------

results = {}

# Check all bronze tables exist and have data
bronze_tables = {
    "user_profiles": 5000,
    "transactions": 100000,
    "login_logs": 200000,
    "known_fraud_signatures": 200,
}

print("=" * 60)
print("TEST 1: Bronze Tables")
print("=" * 60)

all_pass = True
for table, expected_min in bronze_tables.items():
    count = spark.sql(f"SELECT count(*) FROM {CATALOG}.fraud_detection.{table}").collect()[0][0]
    status = "PASS" if count >= expected_min else "FAIL"
    if status == "FAIL":
        all_pass = False
    print(f"  {status} | {table}: {count:,} rows (expected >= {expected_min:,})")

results["bronze"] = all_pass
print(f"\nBronze Tables: {'PASS' if all_pass else 'FAIL'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Volume Files

# COMMAND ----------

print("=" * 60)
print("TEST 2: Volume Files")
print("=" * 60)

files_df = spark.sql(f"LIST '/Volumes/{CATALOG}/fraud_detection/source_files/'")
file_count = files_df.count()
status = "PASS" if file_count == 5 else "FAIL"
print(f"  {status} | Volume contains {file_count} files (expected 5)")
files_df.show(truncate=False)
results["volume"] = file_count == 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Silver Enriched Transactions - Joins & Risk Signals

# COMMAND ----------

print("=" * 60)
print("TEST 3: Silver Enriched Transactions")
print("=" * 60)

silver_df = spark.table(f"{CATALOG}.fraud_detection.silver_enriched_transactions")
silver_count = silver_df.count()
print(f"  {'PASS' if silver_count > 100000 else 'FAIL'} | Row count: {silver_count:,}")

# Check risk signals are computed
risk_cols = ["impossible_travel", "mfa_change_high_value", "high_value_wire_after_ip_change",
             "abnormal_typing", "amount_anomaly", "rule_based_risk_score"]
missing_cols = [c for c in risk_cols if c not in silver_df.columns]
print(f"  {'PASS' if not missing_cols else 'FAIL'} | Risk signal columns: {len(risk_cols) - len(missing_cols)}/{len(risk_cols)} present")
if missing_cols:
    print(f"    Missing: {missing_cols}")

# Check impossible travel detections exist
impossible_count = silver_df.filter("impossible_travel = true").count()
print(f"  {'PASS' if impossible_count > 0 else 'FAIL'} | Impossible travel detections: {impossible_count}")

# Check MFA + high value detections
mfa_count = silver_df.filter("mfa_change_high_value = true").count()
print(f"  {'PASS' if mfa_count > 0 else 'FAIL'} | MFA change + high value: {mfa_count}")

# Check risk score distribution
high_risk = silver_df.filter("rule_based_risk_score >= 50").count()
print(f"  {'PASS' if high_risk > 0 else 'FAIL'} | High risk (score >= 50): {high_risk}")

results["silver"] = silver_count > 100000 and not missing_cols and impossible_count > 0

# Show sample high-risk transactions
print("\nSample high-risk transactions:")
silver_df.filter("rule_based_risk_score >= 50").select(
    "transaction_id", "user_id", "amount", "txn_type",
    "rule_based_risk_score", "impossible_travel", "mfa_change_high_value",
    "geo_distance_miles", "time_since_prev_login_min"
).orderBy("rule_based_risk_score", ascending=False).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Gold KPIs

# COMMAND ----------

print("=" * 60)
print("TEST 4: Gold KPIs")
print("=" * 60)

# Fraud KPIs
kpis = spark.table(f"{CATALOG}.fraud_operations.gold_fraud_kpis")
kpi_count = kpis.count()
print(f"  {'PASS' if kpi_count > 0 else 'FAIL'} | Fraud KPIs: {kpi_count} daily records")

# ATO Rate
ato = spark.table(f"{CATALOG}.fraud_operations.gold_account_takeover")
ato_count = ato.count()
print(f"  {'PASS' if ato_count > 0 else 'FAIL'} | ATO Rate: {ato_count} daily records")

# Fraud by Pattern
patterns = spark.table(f"{CATALOG}.fraud_operations.gold_fraud_by_pattern")
pattern_count = patterns.count()
print(f"  {'PASS' if pattern_count > 0 else 'FAIL'} | Fraud Patterns: {pattern_count} pattern types")

results["gold"] = kpi_count > 0 and ato_count > 0 and pattern_count > 0

print("\nFraud KPIs:")
kpis.orderBy("report_date", ascending=False).show(5, truncate=False)

print("Fraud by Pattern:")
patterns.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: Velocity Anomaly Detection

# COMMAND ----------

print("=" * 60)
print("TEST 5: Velocity Anomaly Detection")
print("=" * 60)

velocity = spark.table(f"{CATALOG}.fraud_detection.silver_velocity_anomalies")
vel_count = velocity.count()
vel_anomaly_count = velocity.filter("is_velocity_anomaly = true").count()
print(f"  {'PASS' if vel_count > 0 else 'FAIL'} | Velocity windows (3+ txns): {vel_count}")
print(f"  {'PASS' if vel_anomaly_count > 0 else 'FAIL'} | True anomalies (5+ txns in 5min): {vel_anomaly_count}")

results["velocity"] = vel_count > 0

print("\nTop velocity bursts:")
velocity.filter("is_velocity_anomaly = true").orderBy("txn_count", ascending=False).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6: Operational Triage Store

# COMMAND ----------

print("=" * 60)
print("TEST 6: Operational Triage Store")
print("=" * 60)

triage = spark.table(f"{CATALOG}.fraud_operations.real_time_fraud_triage")
triage_count = triage.count()
print(f"  {'PASS' if triage_count > 0 else 'FAIL'} | Total triage records: {triage_count:,}")

# Check distribution
from pyspark.sql.functions import col, count, avg, round as spark_round

dist = triage.groupBy("risk_category", "automated_action").agg(
    count("*").alias("cnt"),
    spark_round(avg("risk_score"), 1).alias("avg_score")
).orderBy("avg_score", ascending=False).collect()

red_count = sum(r["cnt"] for r in dist if r["risk_category"] == "RED")
yellow_count = sum(r["cnt"] for r in dist if r["risk_category"] == "YELLOW")
green_count = sum(r["cnt"] for r in dist if r["risk_category"] == "GREEN")

print(f"  {'PASS' if red_count > 0 else 'FAIL'} | RED (BLOCK): {red_count}")
print(f"  {'PASS' if yellow_count > 0 else 'FAIL'} | YELLOW (FLAG): {yellow_count}")
print(f"  {'PASS' if green_count > 0 else 'FAIL'} | GREEN (ALLOW): {green_count:,}")

# Check explanations are populated
has_explanation = triage.filter("explanation IS NOT NULL AND explanation != ''").count()
print(f"  {'PASS' if has_explanation == triage_count else 'FAIL'} | Explanations populated: {has_explanation:,}/{triage_count:,}")

# Check latency
avg_latency = triage.agg({"ttl_decision_ms": "avg"}).collect()[0][0]
print(f"  {'PASS' if avg_latency and avg_latency < 200 else 'FAIL'} | Avg decision latency: {avg_latency:.0f}ms (target < 200ms)")

results["triage"] = red_count > 0 and yellow_count > 0 and has_explanation > 0

print("\nRED (Blocked) transactions:")
triage.filter("risk_category = 'RED'").select(
    "transaction_id", "user_id", "amount", "risk_score", "explanation"
).show(truncate=False)

print("YELLOW (Pending Review) transactions:")
triage.filter("risk_category = 'YELLOW'").select(
    "transaction_id", "user_id", "amount", "risk_score", "explanation"
).orderBy("risk_score", ascending=False).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 7: PII Masking (ABAC Compliance)

# COMMAND ----------

print("=" * 60)
print("TEST 7: PII Masking")
print("=" * 60)

profiles = spark.sql(f"SELECT user_id, card_number, email, phone FROM {CATALOG}.fraud_detection.user_profiles LIMIT 5")
rows = profiles.collect()

card_masked = all("****" in str(r["card_number"]) for r in rows)
email_masked = all("***@" in str(r["email"]) for r in rows)
phone_masked = all("+1***" in str(r["phone"]) for r in rows)

print(f"  {'PASS' if card_masked else 'FAIL'} | Card numbers masked: {rows[0]['card_number']}")
print(f"  {'PASS' if email_masked else 'FAIL'} | Emails masked: {rows[0]['email']}")
print(f"  {'PASS' if phone_masked else 'FAIL'} | Phones masked: {rows[0]['phone']}")

results["pii"] = card_masked and email_masked and phone_masked

profiles.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 8: Genie Space Investigation Query
# MAGIC
# MAGIC Simulating the key investigation query: *"Show me all wire transfers over $10k where the user changed their MFA settings in the last 24 hours"*

# COMMAND ----------

print("=" * 60)
print("TEST 8: Genie Investigation Query")
print("=" * 60)

genie_query = spark.sql(f"""
    SELECT t.transaction_id, t.user_id, t.amount, t.txn_type, t.txn_timestamp,
           t.mfa_change_flag, t.mfa_change_timestamp, t.login_ip,
           t.device_fingerprint, t.rule_based_risk_score
    FROM {CATALOG}.fraud_detection.silver_enriched_transactions t
    WHERE t.txn_type = 'wire_transfer'
      AND t.amount > 10000
      AND t.mfa_change_flag = true
      AND t.mfa_change_timestamp >= t.txn_timestamp - INTERVAL 24 HOURS
    ORDER BY t.amount DESC
""")

query_count = genie_query.count()
print(f"  {'PASS' if query_count > 0 else 'FAIL'} | Wire transfers > $10K after MFA change: {query_count}")
results["genie_query"] = query_count > 0

genie_query.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 9: Simulate New Transaction & Triage

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import Row
from datetime import datetime
import uuid

print("=" * 60)
print("TEST 9: Simulate New Transaction")
print("=" * 60)

# Create a suspicious transaction
new_txn_id = f"TXN-TEST-{uuid.uuid4().hex[:8].upper()}"
print(f"Creating test transaction: {new_txn_id}")

spark.sql(f"""
    INSERT INTO {CATALOG}.fraud_operations.real_time_fraud_triage
    VALUES (
        '{new_txn_id}',
        'USR-000042',
        52000.00,
        'wire_transfer',
        92,
        'RED',
        'BLOCK',
        'CRITICAL: Test transaction - Wire transfer of $52,000 initiated from new IP address (Lagos, Nigeria) 7 minutes after login from New York. MFA settings changed during session. Typing cadence score 0.22 indicates automated input. Pattern matches known account takeover signatures.',
        '{{"risk_factors": ["impossible_travel", "mfa_change", "bot_signature", "high_value_wire"]}}',
        NULL,
        NULL,
        current_timestamp(),
        NULL,
        78
    )
""")

# Verify it was inserted
verify = spark.sql(f"""
    SELECT transaction_id, risk_score, risk_category, automated_action, explanation
    FROM {CATALOG}.fraud_operations.real_time_fraud_triage
    WHERE transaction_id = '{new_txn_id}'
""")
inserted = verify.count()
print(f"  {'PASS' if inserted == 1 else 'FAIL'} | Transaction inserted and visible in triage queue")

results["simulate"] = inserted == 1

verify.show(truncate=False)

# Now simulate an analyst reviewing and blocking it
spark.sql(f"""
    UPDATE {CATALOG}.fraud_operations.real_time_fraud_triage
    SET analyst_decision = 'BLOCK',
        analyst_notes = 'Confirmed fraudulent - impossible travel pattern with bot signature. Account locked.',
        reviewed_at = current_timestamp()
    WHERE transaction_id = '{new_txn_id}'
""")

reviewed = spark.sql(f"""
    SELECT transaction_id, analyst_decision, analyst_notes, reviewed_at
    FROM {CATALOG}.fraud_operations.real_time_fraud_triage
    WHERE transaction_id = '{new_txn_id}'
""")
print(f"\nAnalyst review applied:")
reviewed.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 10: Reasoning Agent (Foundation Model API)

# COMMAND ----------

print("=" * 60)
print("TEST 10: AI Reasoning Agent")
print("=" * 60)

try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()

    # Pick a high-risk transaction for AI analysis
    sample = spark.sql(f"""
        SELECT * FROM {CATALOG}.fraud_detection.silver_enriched_transactions
        WHERE rule_based_risk_score >= 50
        ORDER BY rule_based_risk_score DESC LIMIT 1
    """).collect()[0]

    prompt = f"""Analyze this banking transaction for fraud risk and provide a JSON response with keys: risk_score (0-100), explanation (2-3 sentences), action (BLOCK/YELLOW_FLAG/ALLOW), risk_factors (list).

Transaction: ${sample['amount']:,.2f} {sample['txn_type']} via {sample['txn_channel']}
MFA Changed: {sample['mfa_change_flag']}, IP Changed: {sample['ip_change_flag'] == 1}
Geo Distance: {sample['geo_distance_miles']:.0f} miles, Time Since Prev Login: {sample['time_since_prev_login_min']} min
Typing Cadence: {sample['typing_cadence_score']}, Bot Signature: {sample['is_bot_signature']}
Account Age: {sample['account_age_days']} days, Avg Monthly: ${sample['avg_monthly_txn']:,.2f}
Rule-Based Score: {sample['rule_based_risk_score']}"""

    response = w.serving_endpoints.query(
        name="databricks-claude-sonnet-4-5",
        messages=[
            {"role": "system", "content": "You are a fraud analyst AI. Respond in valid JSON only."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=400,
        temperature=0.1
    )

    ai_response = response.choices[0].message.content
    print(f"  PASS | AI Reasoning Agent responded successfully")
    print(f"\n  Transaction: {sample['transaction_id']}")
    print(f"  Rule-based score: {sample['rule_based_risk_score']}")
    print(f"\n  AI Assessment:")
    print(f"  {ai_response}")
    results["agent"] = True

except Exception as e:
    print(f"  FAIL | AI Reasoning Agent error: {str(e)[:200]}")
    results["agent"] = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Summary

# COMMAND ----------

print("=" * 60)
print("VALIDATION SUMMARY")
print("=" * 60)

test_names = {
    "bronze": "Bronze Tables (Raw Data)",
    "volume": "Volume Files",
    "silver": "Silver Enriched Transactions",
    "gold": "Gold KPIs",
    "velocity": "Velocity Anomaly Detection",
    "triage": "Operational Triage Store",
    "pii": "PII Masking (GDPR/CCPA)",
    "genie_query": "Genie Investigation Query",
    "simulate": "Transaction Simulation & Review",
    "agent": "AI Reasoning Agent",
}

passed = 0
failed = 0
for key, name in test_names.items():
    status = results.get(key, False)
    icon = "PASS" if status else "FAIL"
    if status:
        passed += 1
    else:
        failed += 1
    print(f"  {icon} | {name}")

print(f"\n{'=' * 60}")
print(f"Results: {passed} passed, {failed} failed out of {passed + failed} tests")
print(f"{'=' * 60}")

if failed == 0:
    print("\nAll validations passed! The Fraud Triage Agent is fully operational.")
else:
    print(f"\n{failed} test(s) need attention. Review the output above for details.")

print(f"""
RESOURCES:
  Workspace: https://fevm-serverless-bir.cloud.databricks.com
  App:       https://live-fraud-queue-7474655709876177.aws.databricksapps.com
  Genie:     Open from workspace sidebar > Genie > 'Fraud Triage Investigator'
  Notebooks: /Users/birbal.das@databricks.com/fraud_triage_agent/
""")
