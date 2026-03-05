"""
Full deployment of Fraud Triage Agent to a Databricks workspace.
Recreates: schemas, volume, tables, silver/gold layers, triage store, PII masking, Genie Space, DLT pipeline.
"""
import subprocess
import json
import time
import sys

PROFILE = "vm"
WAREHOUSE = "19be9738b181575a"
CATALOG = "serverless_bir_catalog"
NEW_CLI = "/opt/homebrew/bin/databricks"
DATA_DIR = "/Users/birbal.das/tko/fraud-triage-agent/data"
NOTEBOOK_DIR = "/Users/birbal.das/tko/fraud-triage-agent/notebooks"
WS_PATH = "/Users/birbal.das@databricks.com/fraud_triage_agent"


def run_sql(sql, description="", timeout_polls=60):
    print(f"\n>>> {description}")
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
    except:
        print(f"    -> Parse error: {result.stderr[:300]}")
        return None

    state = data.get("status", {}).get("state", "UNKNOWN")

    if state == "SUCCEEDED":
        rows = data.get("result", {}).get("data_array", [])
        total = data.get("manifest", {}).get("total_row_count", "n/a")
        print(f"    -> OK (rows: {total})")
        for row in rows[:10]:
            print(f"       {row}")
        return data
    elif state in ("PENDING", "RUNNING"):
        stmt_id = data.get("statement_id")
        print(f"    -> {state}, polling...")
        for i in range(timeout_polls):
            time.sleep(5)
            poll = subprocess.run(
                ["databricks", "api", "get",
                 f"/api/2.0/sql/statements/{stmt_id}", "--profile", PROFILE],
                capture_output=True, text=True
            )
            pd = json.loads(poll.stdout)
            ps = pd.get("status", {}).get("state")
            if ps == "SUCCEEDED":
                rows = pd.get("result", {}).get("data_array", [])
                total = pd.get("manifest", {}).get("total_row_count", "n/a")
                print(f"    -> OK (rows: {total})")
                for row in rows[:10]:
                    print(f"       {row}")
                return pd
            elif ps in ("FAILED", "CANCELED", "CLOSED"):
                error = pd.get("status", {}).get("error", {}).get("message", "")
                print(f"    -> FAILED: {error[:300]}")
                return pd
            if i % 6 == 5:
                print(f"    ... still {ps} ({(i+1)*5}s)")
        print("    -> TIMEOUT")
        return None
    else:
        error = data.get("status", {}).get("error", {}).get("message", "")
        print(f"    -> {state}: {error[:300]}")
        return data


def run_cmd(cmd, description=""):
    print(f"\n>>> {description}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode == 0:
        print(f"    -> OK")
        if result.stdout.strip():
            print(f"       {result.stdout.strip()[:200]}")
    else:
        print(f"    -> Error: {result.stderr[:200]}")
    return result


# ================================================================
# PHASE 1: Unity Catalog Setup
# ================================================================
print("=" * 70)
print(f"DEPLOYING FRAUD TRIAGE AGENT TO {CATALOG}")
print(f"Workspace: vm | Warehouse: {WAREHOUSE}")
print("=" * 70)

print("\n--- Phase 1: Unity Catalog Setup ---")
run_sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.fraud_detection COMMENT 'Raw and enriched fraud detection data'",
        "Creating fraud_detection schema")
run_sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.fraud_investigation COMMENT 'Investigation tools'",
        "Creating fraud_investigation schema")
run_sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.fraud_operations COMMENT 'Operational data and KPIs'",
        "Creating fraud_operations schema")
run_sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.fraud_detection.source_files COMMENT 'Mock banking source files'",
        "Creating source_files volume")


# ================================================================
# PHASE 2: Upload mock data to Volume
# ================================================================
print("\n--- Phase 2: Upload Mock Data ---")
files = ["transactions.csv", "login_logs.csv", "user_profiles.csv",
         "known_fraud_signatures.csv", "known_fraud_signatures.json"]
for f in files:
    run_cmd(["databricks", "fs", "cp",
             f"{DATA_DIR}/{f}",
             f"dbfs:/Volumes/{CATALOG}/fraud_detection/source_files/{f}",
             "--profile", PROFILE, "--overwrite"],
            f"Uploading {f}")


# ================================================================
# PHASE 3: Create Bronze tables
# ================================================================
print("\n--- Phase 3: Create Bronze Tables ---")
for tbl, fname in [("user_profiles", "user_profiles.csv"),
                     ("transactions", "transactions.csv"),
                     ("login_logs", "login_logs.csv"),
                     ("known_fraud_signatures", "known_fraud_signatures.csv")]:
    run_sql(f"""CREATE OR REPLACE TABLE {CATALOG}.fraud_detection.{tbl}
    AS SELECT * FROM read_files(
      '/Volumes/{CATALOG}/fraud_detection/source_files/{fname}',
      format => 'csv', header => true, inferSchema => true
    )""", f"Creating {tbl} table")

run_sql(f"""SELECT 'user_profiles' as tbl, count(*) FROM {CATALOG}.fraud_detection.user_profiles
UNION ALL SELECT 'transactions', count(*) FROM {CATALOG}.fraud_detection.transactions
UNION ALL SELECT 'login_logs', count(*) FROM {CATALOG}.fraud_detection.login_logs
UNION ALL SELECT 'known_fraud_signatures', count(*) FROM {CATALOG}.fraud_detection.known_fraud_signatures""",
        "Verifying Bronze row counts")


# ================================================================
# PHASE 4: Silver & Gold Tables
# ================================================================
print("\n--- Phase 4: Silver Enriched Transactions ---")
run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.fraud_detection.silver_enriched_transactions AS
WITH login_with_prev AS (
    SELECT l.*,
        LAG(ip_address) OVER (PARTITION BY user_id ORDER BY login_timestamp) AS prev_ip,
        LAG(geo_lat) OVER (PARTITION BY user_id ORDER BY login_timestamp) AS prev_lat,
        LAG(geo_lon) OVER (PARTITION BY user_id ORDER BY login_timestamp) AS prev_lon,
        LAG(login_timestamp) OVER (PARTITION BY user_id ORDER BY login_timestamp) AS prev_login_time,
        CASE WHEN LAG(ip_address) OVER (PARTITION BY user_id ORDER BY login_timestamp) IS NOT NULL
             AND LAG(ip_address) OVER (PARTITION BY user_id ORDER BY login_timestamp) != ip_address
            THEN 1 ELSE 0 END AS ip_change_flag,
        CASE WHEN LAG(geo_lat) OVER (PARTITION BY user_id ORDER BY login_timestamp) IS NOT NULL
            THEN 3959 * ACOS(LEAST(1.0, GREATEST(-1.0,
                SIN(RADIANS(geo_lat)) * SIN(RADIANS(LAG(geo_lat) OVER (PARTITION BY user_id ORDER BY login_timestamp)))
                + COS(RADIANS(geo_lat)) * COS(RADIANS(LAG(geo_lat) OVER (PARTITION BY user_id ORDER BY login_timestamp)))
                * COS(RADIANS(LAG(geo_lon) OVER (PARTITION BY user_id ORDER BY login_timestamp)) - RADIANS(geo_lon))
            ))) ELSE 0 END AS geo_distance_miles,
        CASE WHEN LAG(login_timestamp) OVER (PARTITION BY user_id ORDER BY login_timestamp) IS NOT NULL
            THEN TIMESTAMPDIFF(MINUTE, LAG(login_timestamp) OVER (PARTITION BY user_id ORDER BY login_timestamp), login_timestamp)
            ELSE NULL END AS time_since_prev_login_min
    FROM {CATALOG}.fraud_detection.login_logs l
),
txn_with_session AS (
    SELECT t.*, lp.session_id, lp.ip_address AS login_ip, lp.geo_lat AS login_geo_lat,
        lp.geo_lon AS login_geo_lon, lp.device_fingerprint, lp.mfa_change_flag,
        lp.mfa_change_timestamp, lp.typing_cadence_score, lp.is_bot_signature,
        lp.ip_change_flag, lp.geo_distance_miles, lp.time_since_prev_login_min, lp.login_timestamp,
        ROW_NUMBER() OVER (PARTITION BY t.transaction_id
            ORDER BY ABS(TIMESTAMPDIFF(SECOND, t.txn_timestamp, lp.login_timestamp))) AS rn
    FROM {CATALOG}.fraud_detection.transactions t
    LEFT JOIN login_with_prev lp ON t.user_id = lp.user_id
        AND lp.login_timestamp BETWEEN t.txn_timestamp - INTERVAL 2 HOURS AND t.txn_timestamp + INTERVAL 30 MINUTES
)
SELECT s.transaction_id, s.user_id, s.amount, s.currency, s.txn_type, s.merchant_id,
    s.merchant_name, s.merchant_category, s.channel AS txn_channel, s.txn_timestamp,
    s.card_number_masked, s.is_international, s.is_fraud, s.fraud_pattern,
    s.session_id, s.login_ip, s.login_geo_lat, s.login_geo_lon, s.device_fingerprint,
    s.mfa_change_flag, s.mfa_change_timestamp, s.typing_cadence_score, s.is_bot_signature,
    s.ip_change_flag, s.geo_distance_miles, s.time_since_prev_login_min, s.login_timestamp,
    p.account_age_days, p.avg_monthly_txn, p.home_city, p.risk_tier,
    CASE WHEN s.geo_distance_miles > 500 AND s.time_since_prev_login_min < 10 THEN TRUE ELSE FALSE END AS impossible_travel,
    CASE WHEN s.mfa_change_flag = true AND s.amount > 10000 THEN TRUE ELSE FALSE END AS mfa_change_high_value,
    CASE WHEN s.txn_type = 'wire_transfer' AND s.amount > 10000 AND s.ip_change_flag = 1 THEN TRUE ELSE FALSE END AS high_value_wire_after_ip_change,
    CASE WHEN s.typing_cadence_score < 0.45 THEN TRUE ELSE FALSE END AS abnormal_typing,
    CASE WHEN s.amount > 5 * p.avg_monthly_txn THEN TRUE ELSE FALSE END AS amount_anomaly,
    LEAST(100, GREATEST(0,
        CASE WHEN s.geo_distance_miles > 500 AND s.time_since_prev_login_min < 10 THEN 40 ELSE 0 END
        + CASE WHEN s.mfa_change_flag = true AND s.amount > 10000 THEN 30 ELSE 0 END
        + CASE WHEN s.ip_change_flag = 1 AND s.txn_type = 'wire_transfer' AND s.amount > 10000 THEN 25 ELSE 0 END
        + CASE WHEN s.typing_cadence_score < 0.45 THEN 15 ELSE 0 END
        + CASE WHEN s.is_bot_signature = true THEN 20 ELSE 0 END
        + CASE WHEN s.amount > 5 * p.avg_monthly_txn THEN 10 ELSE 0 END
        + CASE WHEN s.is_international = true AND p.account_age_days < 90 THEN 15 ELSE 0 END
    )) AS rule_based_risk_score
FROM txn_with_session s
LEFT JOIN {CATALOG}.fraud_detection.user_profiles p ON s.user_id = p.user_id
WHERE s.rn = 1 OR s.rn IS NULL
""", "Creating Silver: enriched_transactions")

print("\n--- Phase 4b: Silver Velocity Anomalies ---")
run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.fraud_detection.silver_velocity_anomalies AS
SELECT user_id, window(txn_timestamp, '5 minutes').start AS window_start,
    window(txn_timestamp, '5 minutes').end AS window_end, COUNT(*) AS txn_count,
    SUM(amount) AS total_amount, COLLECT_LIST(transaction_id) AS txn_ids,
    CASE WHEN COUNT(*) >= 5 THEN TRUE ELSE FALSE END AS is_velocity_anomaly
FROM {CATALOG}.fraud_detection.transactions
GROUP BY user_id, window(txn_timestamp, '5 minutes') HAVING COUNT(*) >= 3
ORDER BY txn_count DESC
""", "Creating Silver: velocity_anomalies")

print("\n--- Phase 4c: Gold KPIs ---")
run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.fraud_operations.gold_fraud_kpis AS
SELECT DATE(txn_timestamp) AS report_date, COUNT(*) AS total_transactions,
    SUM(CASE WHEN rule_based_risk_score >= 80 THEN 1 ELSE 0 END) AS red_flagged,
    SUM(CASE WHEN rule_based_risk_score >= 50 AND rule_based_risk_score < 80 THEN 1 ELSE 0 END) AS yellow_flagged,
    SUM(CASE WHEN rule_based_risk_score < 50 THEN 1 ELSE 0 END) AS green_allowed,
    SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END) AS actual_fraud_count,
    ROUND(SUM(CASE WHEN rule_based_risk_score >= 50 AND is_fraud = false THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN rule_based_risk_score >= 50 THEN 1 ELSE 0 END), 0), 2) AS false_positive_ratio_pct,
    ROUND(SUM(CASE WHEN rule_based_risk_score >= 50 AND is_fraud = true THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END), 0), 2) AS fraud_detection_rate_pct,
    ROUND(AVG(rule_based_risk_score), 2) AS avg_risk_score,
    SUM(CASE WHEN rule_based_risk_score >= 50 THEN amount ELSE 0 END) AS amount_at_risk
FROM {CATALOG}.fraud_detection.silver_enriched_transactions
GROUP BY DATE(txn_timestamp) ORDER BY report_date DESC
""", "Creating Gold: fraud_kpis")

run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.fraud_operations.gold_account_takeover AS
SELECT DATE(txn_timestamp) AS report_date, COUNT(DISTINCT user_id) AS total_active_users,
    COUNT(DISTINCT CASE WHEN impossible_travel = true THEN user_id END) AS impossible_travel_users,
    COUNT(DISTINCT CASE WHEN mfa_change_high_value = true THEN user_id END) AS mfa_abuse_users,
    COUNT(DISTINCT CASE WHEN impossible_travel = true OR mfa_change_high_value = true
        OR (is_bot_signature = true AND amount > 5000) THEN user_id END) AS suspected_ato_users,
    ROUND(COUNT(DISTINCT CASE WHEN impossible_travel = true OR mfa_change_high_value = true
        OR (is_bot_signature = true AND amount > 5000) THEN user_id END) * 100.0
        / NULLIF(COUNT(DISTINCT user_id), 0), 4) AS ato_rate_pct
FROM {CATALOG}.fraud_detection.silver_enriched_transactions
GROUP BY DATE(txn_timestamp) ORDER BY report_date DESC
""", "Creating Gold: account_takeover")

run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.fraud_operations.gold_fraud_by_pattern AS
SELECT COALESCE(fraud_pattern, 'legitimate') AS pattern_type, COUNT(*) AS txn_count,
    SUM(amount) AS total_amount, ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(AVG(rule_based_risk_score), 2) AS avg_risk_score,
    COUNT(DISTINCT user_id) AS unique_users
FROM {CATALOG}.fraud_detection.silver_enriched_transactions
GROUP BY COALESCE(fraud_pattern, 'legitimate') ORDER BY txn_count DESC
""", "Creating Gold: fraud_by_pattern")


# ================================================================
# PHASE 5: Triage Store
# ================================================================
print("\n--- Phase 5: Operational Triage Store ---")
run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.fraud_operations.real_time_fraud_triage (
    transaction_id STRING NOT NULL, user_id STRING NOT NULL, amount DOUBLE, txn_type STRING,
    risk_score INT, risk_category STRING, automated_action STRING, explanation STRING,
    risk_factors STRING, analyst_decision STRING, analyst_notes STRING,
    created_at TIMESTAMP, reviewed_at TIMESTAMP, ttl_decision_ms INT
) USING DELTA CLUSTER BY (risk_category, automated_action, user_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.autoOptimize.optimizeWrite' = 'true')
""", "Creating real_time_fraud_triage table")

run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.fraud_operations.active_session_risks (
    session_id STRING NOT NULL, user_id STRING NOT NULL, current_risk INT,
    ip_address STRING, geo_location STRING, last_activity TIMESTAMP, is_blocked BOOLEAN
) USING DELTA CLUSTER BY (user_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.autoOptimize.optimizeWrite' = 'true')
""", "Creating active_session_risks table")

run_sql(f"""
INSERT INTO {CATALOG}.fraud_operations.real_time_fraud_triage
SELECT transaction_id, user_id, amount, txn_type, rule_based_risk_score AS risk_score,
    CASE WHEN rule_based_risk_score >= 80 THEN 'RED' WHEN rule_based_risk_score >= 50 THEN 'YELLOW' ELSE 'GREEN' END,
    CASE WHEN rule_based_risk_score >= 80 THEN 'BLOCK' WHEN rule_based_risk_score >= 50 THEN 'YELLOW_FLAG' ELSE 'ALLOW' END,
    CASE
        WHEN impossible_travel = true AND mfa_change_high_value = true
            THEN CONCAT('CRITICAL: Impossible travel detected (', CAST(ROUND(geo_distance_miles, 0) AS STRING), ' miles in ', CAST(ROUND(time_since_prev_login_min, 0) AS STRING), ' min) combined with MFA change before $', CAST(ROUND(amount, 2) AS STRING), ' ', txn_type, '.')
        WHEN impossible_travel = true
            THEN CONCAT('HIGH RISK: Geolocation jumped ', CAST(ROUND(geo_distance_miles, 0) AS STRING), ' miles in ', CAST(ROUND(time_since_prev_login_min, 0) AS STRING), ' minutes. Previous login from ', home_city, '.')
        WHEN mfa_change_high_value = true
            THEN CONCAT('HIGH RISK: MFA changed followed by high-value ', txn_type, ' of $', CAST(ROUND(amount, 2) AS STRING), '. Matches known account takeover patterns.')
        WHEN high_value_wire_after_ip_change = true
            THEN CONCAT('ELEVATED: IP changed before ', txn_type, ' of $', CAST(ROUND(amount, 2) AS STRING), '.')
        WHEN abnormal_typing = true AND amount > 5000
            THEN CONCAT('ELEVATED: Abnormal typing cadence (', CAST(typing_cadence_score AS STRING), ') for $', CAST(ROUND(amount, 2) AS STRING), ' ', txn_type, '.')
        WHEN amount_anomaly = true
            THEN CONCAT('MODERATE: $', CAST(ROUND(amount, 2) AS STRING), ' exceeds avg $', CAST(ROUND(avg_monthly_txn, 2) AS STRING), ' (', CAST(ROUND(amount / avg_monthly_txn, 1) AS STRING), 'x).')
        ELSE CONCAT('LOW RISK: $', CAST(ROUND(amount, 2) AS STRING), ' ', txn_type, '. No anomalies. Score: ', CAST(rule_based_risk_score AS STRING), '.')
    END,
    CASE
        WHEN impossible_travel = true THEN '["impossible_travel","geo_anomaly"]'
        WHEN mfa_change_high_value = true THEN '["mfa_change","high_value_transfer"]'
        WHEN high_value_wire_after_ip_change = true THEN '["ip_change","high_value_wire"]'
        WHEN abnormal_typing = true THEN '["abnormal_typing","bot_signature"]'
        WHEN amount_anomaly = true THEN '["amount_anomaly"]'
        ELSE '[]'
    END,
    NULL, NULL, txn_timestamp, NULL, CAST(RAND() * 150 + 20 AS INT)
FROM {CATALOG}.fraud_detection.silver_enriched_transactions
""", "Populating triage store")


# ================================================================
# PHASE 6: PII Masking
# ================================================================
print("\n--- Phase 6: PII Masking ---")
run_sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.fraud_detection.mask_card_number(card_number STRING)
RETURNS STRING RETURN CONCAT('****-****-****-', RIGHT(card_number, 4))""", "Card masking function")
run_sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.fraud_detection.mask_email(email STRING)
RETURNS STRING RETURN CONCAT(LEFT(email, 2), '***@', SPLIT(email, '@')[1])""", "Email masking function")
run_sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.fraud_detection.mask_phone(phone STRING)
RETURNS STRING RETURN CONCAT('+1***', RIGHT(phone, 4))""", "Phone masking function")
run_sql(f"""ALTER TABLE {CATALOG}.fraud_detection.user_profiles ALTER COLUMN card_number SET MASK {CATALOG}.fraud_detection.mask_card_number""",
        "Apply card mask")
run_sql(f"""ALTER TABLE {CATALOG}.fraud_detection.user_profiles ALTER COLUMN email SET MASK {CATALOG}.fraud_detection.mask_email""",
        "Apply email mask")
run_sql(f"""ALTER TABLE {CATALOG}.fraud_detection.user_profiles ALTER COLUMN phone SET MASK {CATALOG}.fraud_detection.mask_phone""",
        "Apply phone mask")


# ================================================================
# PHASE 7: Upload Notebooks
# ================================================================
print("\n--- Phase 7: Upload Notebooks ---")
run_cmd(["databricks", "workspace", "mkdirs", WS_PATH, "--profile", PROFILE],
        "Creating workspace directory")
for nb, lang in [("01_dlt_fraud_pipeline.py", "PYTHON"),
                  ("02_fraud_reasoning_agent.py", "PYTHON"),
                  ("03_genie_certified_sql.sql", "SQL"),
                  ("06_lakebase_sync.py", "PYTHON"),
                  ("07_sync_decisions_to_delta.py", "PYTHON")]:
    name = nb.rsplit(".", 1)[0]
    run_cmd(["databricks", "workspace", "import", f"{WS_PATH}/{name}",
             "--profile", PROFILE, f"--file={NOTEBOOK_DIR}/{nb}",
             "--format=SOURCE", f"--language={lang}", "--overwrite"],
            f"Uploading {nb}")


# ================================================================
# PHASE 8: DLT Pipeline
# ================================================================
print("\n--- Phase 8: Create DLT Pipeline ---")
pipeline_json = json.dumps({
    "name": "Fraud Triage Agent - DLT Pipeline",
    "catalog": CATALOG,
    "target": "fraud_detection",
    "serverless": True,
    "continuous": False,
    "channel": "CURRENT",
    "libraries": [{"notebook": {"path": f"{WS_PATH}/01_dlt_fraud_pipeline"}}],
    "configuration": {"pipelines.enableTrackHistory": "true"}
})
result = subprocess.run(
    ["databricks", "pipelines", "create", "--profile", PROFILE, "--json", pipeline_json],
    capture_output=True, text=True
)
if result.returncode == 0:
    pipeline_data = json.loads(result.stdout)
    pipeline_id = pipeline_data.get("pipeline_id", "unknown")
    print(f"    -> DLT Pipeline created: {pipeline_id}")
else:
    print(f"    -> Error: {result.stderr[:200]}")
    pipeline_id = None


# ================================================================
# PHASE 9: Genie Space
# ================================================================
print("\n--- Phase 9: Create Genie Space ---")
tables = sorted([
    f"{CATALOG}.fraud_detection.silver_enriched_transactions",
    f"{CATALOG}.fraud_detection.silver_velocity_anomalies",
    f"{CATALOG}.fraud_detection.user_profiles",
    f"{CATALOG}.fraud_operations.gold_account_takeover",
    f"{CATALOG}.fraud_operations.gold_fraud_by_pattern",
    f"{CATALOG}.fraud_operations.gold_fraud_kpis",
    f"{CATALOG}.fraud_operations.real_time_fraud_triage",
])

# Create empty space
genie_create = json.dumps({
    "warehouse_id": WAREHOUSE,
    "title": "Fraud Triage Investigator",
    "description": "Conversational investigation space for fraud analysts.",
    "serialized_space": json.dumps({"version": 2})
})
result = subprocess.run(
    [NEW_CLI, "api", "post", "/api/2.0/genie/spaces",
     "--profile", PROFILE, "--json", genie_create],
    capture_output=True, text=True
)
try:
    genie_data = json.loads(result.stdout)
    space_id = genie_data.get("space_id", "unknown")
    print(f"    -> Genie Space created: {space_id}")

    # Update with tables
    genie_update = json.dumps({
        "warehouse_id": WAREHOUSE,
        "title": "Fraud Triage Investigator",
        "description": "Conversational investigation space for fraud analysts. Ask: 'Show me all wire transfers over $10k where the user changed MFA in the last 24 hours'",
        "serialized_space": json.dumps({
            "version": 2,
            "data_sources": {
                "tables": [{"identifier": t} for t in tables]
            }
        })
    })
    result2 = subprocess.run(
        [NEW_CLI, "genie", "update-space", space_id,
         "--profile", PROFILE, "--json", genie_update],
        capture_output=True, text=True
    )
    if result2.returncode == 0:
        print(f"    -> Genie Space updated with {len(tables)} tables")
    else:
        print(f"    -> Update error: {result2.stderr[:200]}")
except:
    print(f"    -> Error: {result.stderr[:200]}")
    space_id = None


# ================================================================
# VERIFICATION
# ================================================================
print("\n" + "=" * 70)
print("VERIFICATION")
print("=" * 70)

run_sql(f"""
SELECT 'user_profiles' as tbl, count(*) as cnt FROM {CATALOG}.fraud_detection.user_profiles
UNION ALL SELECT 'transactions', count(*) FROM {CATALOG}.fraud_detection.transactions
UNION ALL SELECT 'login_logs', count(*) FROM {CATALOG}.fraud_detection.login_logs
UNION ALL SELECT 'known_fraud_signatures', count(*) FROM {CATALOG}.fraud_detection.known_fraud_signatures
UNION ALL SELECT 'silver_enriched_transactions', count(*) FROM {CATALOG}.fraud_detection.silver_enriched_transactions
UNION ALL SELECT 'silver_velocity_anomalies', count(*) FROM {CATALOG}.fraud_detection.silver_velocity_anomalies
UNION ALL SELECT 'gold_fraud_kpis', count(*) FROM {CATALOG}.fraud_operations.gold_fraud_kpis
UNION ALL SELECT 'gold_account_takeover', count(*) FROM {CATALOG}.fraud_operations.gold_account_takeover
UNION ALL SELECT 'gold_fraud_by_pattern', count(*) FROM {CATALOG}.fraud_operations.gold_fraud_by_pattern
UNION ALL SELECT 'real_time_fraud_triage', count(*) FROM {CATALOG}.fraud_operations.real_time_fraud_triage
""", "All table row counts")

run_sql(f"""
SELECT risk_category, automated_action, COUNT(*) as cnt, ROUND(AVG(risk_score), 1) as avg_score
FROM {CATALOG}.fraud_operations.real_time_fraud_triage
GROUP BY risk_category, automated_action ORDER BY avg_score DESC
""", "Triage distribution")

run_sql(f"""
SELECT transaction_id, risk_score, risk_category, automated_action, LEFT(explanation, 100)
FROM {CATALOG}.fraud_operations.real_time_fraud_triage
WHERE risk_category IN ('RED', 'YELLOW') ORDER BY risk_score DESC LIMIT 5
""", "Top high-risk transactions")


# ================================================================
# SUMMARY
# ================================================================
print("\n" + "=" * 70)
print("DEPLOYMENT SUMMARY")
print("=" * 70)
print(f"Workspace:   fevm-serverless-bir.cloud.databricks.com")
print(f"Profile:     {PROFILE}")
print(f"Catalog:     {CATALOG}")
print(f"Warehouse:   {WAREHOUSE}")
print(f"DLT Pipeline: {pipeline_id}")
print(f"Genie Space: {space_id}")
print(f"Notebooks:   {WS_PATH}/")
print(f"")
print("Schemas:")
print(f"  - {CATALOG}.fraud_detection (Bronze + Silver tables)")
print(f"  - {CATALOG}.fraud_investigation")
print(f"  - {CATALOG}.fraud_operations (Gold + Triage tables)")
print(f"")
print("Remaining: Build Databricks App (Live Fraud Queue)")
print("=" * 70)
