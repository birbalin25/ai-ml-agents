"""Verify all data exists in the vm workspace."""
import subprocess
import json

PROFILE = "vm"
WAREHOUSE = "19be9738b181575a"
CATALOG = "serverless_bir_catalog"


def run_sql(sql, description=""):
    print(f">>> {description}")
    payload = json.dumps({
        "warehouse_id": WAREHOUSE,
        "statement": sql,
        "wait_timeout": "50s"
    })
    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "--profile", PROFILE, "--json", payload],
        capture_output=True, text=True
    )
    try:
        data = json.loads(result.stdout)
        state = data.get("status", {}).get("state", "UNKNOWN")
        if state == "SUCCEEDED":
            rows = data.get("result", {}).get("data_array", [])
            total = data.get("manifest", {}).get("total_row_count", "n/a")
            print(f"    -> OK (rows: {total})")
            for row in rows[:15]:
                print(f"       {row}")
        else:
            error = data.get("status", {}).get("error", {}).get("message", "")
            print(f"    -> {state}: {error[:200]}")
    except Exception as e:
        print(f"    -> Error: {e}")
    print()


print("=" * 60)
print(f"VERIFICATION: {CATALOG} on profile={PROFILE}")
print("=" * 60)

# 1. Check schemas
run_sql(f"SHOW SCHEMAS IN {CATALOG}", "Schemas")

# 2. Check Volume files
run_sql(f"LIST '/Volumes/{CATALOG}/fraud_detection/source_files/'", "Volume files")

# 3. Check all tables and row counts
run_sql(f"""
SELECT 'fraud_detection.user_profiles' as tbl, count(*) as cnt FROM {CATALOG}.fraud_detection.user_profiles
UNION ALL SELECT 'fraud_detection.transactions', count(*) FROM {CATALOG}.fraud_detection.transactions
UNION ALL SELECT 'fraud_detection.login_logs', count(*) FROM {CATALOG}.fraud_detection.login_logs
UNION ALL SELECT 'fraud_detection.known_fraud_signatures', count(*) FROM {CATALOG}.fraud_detection.known_fraud_signatures
UNION ALL SELECT 'fraud_detection.silver_enriched_transactions', count(*) FROM {CATALOG}.fraud_detection.silver_enriched_transactions
UNION ALL SELECT 'fraud_detection.silver_velocity_anomalies', count(*) FROM {CATALOG}.fraud_detection.silver_velocity_anomalies
UNION ALL SELECT 'fraud_operations.gold_fraud_kpis', count(*) FROM {CATALOG}.fraud_operations.gold_fraud_kpis
UNION ALL SELECT 'fraud_operations.gold_account_takeover', count(*) FROM {CATALOG}.fraud_operations.gold_account_takeover
UNION ALL SELECT 'fraud_operations.gold_fraud_by_pattern', count(*) FROM {CATALOG}.fraud_operations.gold_fraud_by_pattern
UNION ALL SELECT 'fraud_operations.real_time_fraud_triage', count(*) FROM {CATALOG}.fraud_operations.real_time_fraud_triage
UNION ALL SELECT 'fraud_operations.active_session_risks', count(*) FROM {CATALOG}.fraud_operations.active_session_risks
""", "All table row counts")

# 4. Triage distribution
run_sql(f"""
SELECT risk_category, automated_action, COUNT(*) as cnt, ROUND(AVG(risk_score), 1) as avg_score
FROM {CATALOG}.fraud_operations.real_time_fraud_triage
GROUP BY risk_category, automated_action ORDER BY avg_score DESC
""", "Triage distribution (RED/YELLOW/GREEN)")

# 5. Sample high-risk transactions
run_sql(f"""
SELECT transaction_id, user_id, ROUND(amount,2) as amount, risk_score, risk_category, LEFT(explanation, 120) as explanation
FROM {CATALOG}.fraud_operations.real_time_fraud_triage
WHERE risk_category IN ('RED', 'YELLOW')
ORDER BY risk_score DESC LIMIT 5
""", "Top 5 flagged transactions")

# 6. PII masking check
run_sql(f"""
SELECT user_id, card_number, email, phone FROM {CATALOG}.fraud_detection.user_profiles LIMIT 3
""", "PII masking verification (should be masked)")

# 7. Gold KPIs sample
run_sql(f"""
SELECT report_date, total_transactions, red_flagged, yellow_flagged, false_positive_ratio_pct, fraud_detection_rate_pct
FROM {CATALOG}.fraud_operations.gold_fraud_kpis ORDER BY report_date DESC LIMIT 5
""", "Gold fraud KPIs (sample)")

# 8. Check notebooks
print(">>> Workspace notebooks")
r = subprocess.run(
    ["databricks", "workspace", "list", "/Users/birbal.das@databricks.com/fraud_triage_agent",
     "--profile", PROFILE],
    capture_output=True, text=True
)
print(f"    {r.stdout.strip()}")
print()

# 9. Check DLT pipeline
print(">>> DLT Pipelines")
r = subprocess.run(
    ["databricks", "pipelines", "list", "--profile", PROFILE],
    capture_output=True, text=True
)
for line in r.stdout.strip().split("\n"):
    if "fraud" in line.lower() or "Pipeline" in line:
        print(f"    {line}")
print()

# 10. Check App
print(">>> Databricks App")
r = subprocess.run(
    ["/opt/homebrew/bin/databricks", "apps", "get", "live-fraud-queue",
     "--profile", PROFILE, "-o", "json"],
    capture_output=True, text=True
)
try:
    d = json.loads(r.stdout)
    print(f"    Name: {d['name']}")
    print(f"    URL: {d.get('url', 'N/A')}")
    print(f"    Compute: {d.get('compute_status', {}).get('state', 'unknown')}")
    print(f"    Deployment: {d.get('active_deployment', {}).get('status', {}).get('state', 'unknown')}")
except:
    print(f"    {r.stdout[:200]}")
print()

# 11. Check Genie Space
print(">>> Genie Space")
r = subprocess.run(
    ["/opt/homebrew/bin/databricks", "genie", "list-spaces", "--profile", PROFILE, "-o", "json"],
    capture_output=True, text=True
)
try:
    d = json.loads(r.stdout)
    spaces = d.get("spaces", [])
    for s in spaces:
        if "fraud" in s.get("title", "").lower():
            print(f"    ID: {s['space_id']}")
            print(f"    Title: {s['title']}")
except:
    print(f"    {r.stdout[:200]}")

print("\n" + "=" * 60)
print("VERIFICATION COMPLETE")
print("=" * 60)
