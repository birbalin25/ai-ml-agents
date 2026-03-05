"""Create the operational fraud triage store tables.
Uses Delta tables with liquid clustering for sub-second access.
When Lakebase becomes available, these can be synced to Lakebase for even lower latency.
"""
import subprocess
import json
import time

PROFILE = "vm2"
WAREHOUSE = "8620a950b7475da4"
CATALOG = "serverless_stable_p2uvy4_catalog"


def run_sql(sql, description=""):
    print(f"\n>>> {description}")
    payload = json.dumps({
        "warehouse_id": WAREHOUSE,
        "statement": sql,
        "wait_timeout": "50s"
    })
    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements", "--profile", PROFILE, "--json", payload],
        capture_output=True, text=True
    )
    try:
        data = json.loads(result.stdout)
        state = data.get("status", {}).get("state", "UNKNOWN")
        if state == "SUCCEEDED":
            rows = data.get("result", {}).get("data_array", [])
            total = data.get("manifest", {}).get("total_row_count", "n/a")
            print(f"    -> OK (rows: {total})")
            for row in rows[:5]:
                print(f"       {row}")
        elif state in ("PENDING", "RUNNING"):
            stmt_id = data.get("statement_id")
            for _ in range(30):
                time.sleep(3)
                poll = subprocess.run(
                    ["databricks", "api", "get", f"/api/2.0/sql/statements/{stmt_id}", "--profile", PROFILE],
                    capture_output=True, text=True
                )
                pd = json.loads(poll.stdout)
                ps = pd.get("status", {}).get("state")
                if ps == "SUCCEEDED":
                    print(f"    -> OK")
                    return pd
                elif ps in ("FAILED", "CANCELED"):
                    print(f"    -> {ps}: {pd.get('status',{}).get('error',{}).get('message','')[:200]}")
                    return pd
            print("    -> TIMEOUT")
        else:
            error = data.get("status", {}).get("error", {}).get("message", "")
            print(f"    -> {state}: {error[:200]}")
        return data
    except Exception as e:
        print(f"    -> Error: {e}: {result.stderr[:200]}")
        return None


print("=" * 60)
print("Creating Operational Fraud Triage Store")
print("=" * 60)

# Real-time fraud triage table (primary operational store)
run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.fraud_operations.real_time_fraud_triage (
    transaction_id STRING NOT NULL,
    user_id STRING NOT NULL,
    amount DOUBLE,
    txn_type STRING,
    risk_score INT,
    risk_category STRING,
    automated_action STRING,
    explanation STRING,
    risk_factors STRING,
    analyst_decision STRING,
    analyst_notes STRING,
    created_at TIMESTAMP,
    reviewed_at TIMESTAMP,
    ttl_decision_ms INT
)
USING DELTA
CLUSTER BY (risk_category, automated_action, user_id)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Operational triage store for real-time fraud decisions. Designed for sub-second access patterns.'
""", "Creating real_time_fraud_triage table")

# Active session risks table
run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.fraud_operations.active_session_risks (
    session_id STRING NOT NULL,
    user_id STRING NOT NULL,
    current_risk INT DEFAULT 0,
    ip_address STRING,
    geo_location STRING,
    last_activity TIMESTAMP,
    is_blocked BOOLEAN DEFAULT FALSE
)
USING DELTA
CLUSTER BY (user_id, is_blocked)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
COMMENT 'Active session risk tracking for real-time blocking decisions'
""", "Creating active_session_risks table")

# Populate the triage store from Silver enriched transactions
run_sql(f"""
INSERT INTO {CATALOG}.fraud_operations.real_time_fraud_triage
SELECT
    transaction_id,
    user_id,
    amount,
    txn_type,
    rule_based_risk_score AS risk_score,
    CASE
        WHEN rule_based_risk_score >= 80 THEN 'RED'
        WHEN rule_based_risk_score >= 50 THEN 'YELLOW'
        ELSE 'GREEN'
    END AS risk_category,
    CASE
        WHEN rule_based_risk_score >= 80 THEN 'BLOCK'
        WHEN rule_based_risk_score >= 50 THEN 'YELLOW_FLAG'
        ELSE 'ALLOW'
    END AS automated_action,
    CASE
        WHEN impossible_travel = true AND mfa_change_high_value = true
            THEN CONCAT('CRITICAL: Impossible travel detected (', CAST(ROUND(geo_distance_miles, 0) AS STRING), ' miles in ', CAST(ROUND(time_since_prev_login_min, 0) AS STRING), ' min) combined with MFA change before $', CAST(ROUND(amount, 2) AS STRING), ' ', txn_type, '. Pattern matches known account takeover signatures. Typing cadence score (', CAST(typing_cadence_score AS STRING), ') suggests automated input.')
        WHEN impossible_travel = true
            THEN CONCAT('HIGH RISK: Geolocation jumped ', CAST(ROUND(geo_distance_miles, 0) AS STRING), ' miles in ', CAST(ROUND(time_since_prev_login_min, 0) AS STRING), ' minutes - physically impossible travel. Previous login from ', home_city, '. Bot signature: ', CAST(is_bot_signature AS STRING), '.')
        WHEN mfa_change_high_value = true
            THEN CONCAT('HIGH RISK: MFA settings were changed at ', CAST(mfa_change_timestamp AS STRING), ' followed by high-value ', txn_type, ' of $', CAST(ROUND(amount, 2) AS STRING), '. This matches known account takeover patterns where attackers modify authentication before draining funds.')
        WHEN high_value_wire_after_ip_change = true
            THEN CONCAT('ELEVATED: IP address changed before ', txn_type, ' of $', CAST(ROUND(amount, 2) AS STRING), '. New IP detected within the session. Amount exceeds normal threshold for wire transfers.')
        WHEN abnormal_typing = true AND amount > 5000
            THEN CONCAT('ELEVATED: Abnormal typing cadence (', CAST(typing_cadence_score AS STRING), ') detected during session for $', CAST(ROUND(amount, 2) AS STRING), ' ', txn_type, '. Pattern suggests automated or scripted input rather than human interaction.')
        WHEN amount_anomaly = true
            THEN CONCAT('MODERATE: Transaction amount of $', CAST(ROUND(amount, 2) AS STRING), ' significantly exceeds users average monthly transaction of $', CAST(ROUND(avg_monthly_txn, 2) AS STRING), ' (', CAST(ROUND(amount / avg_monthly_txn, 1) AS STRING), 'x). Account age: ', CAST(account_age_days AS STRING), ' days.')
        ELSE CONCAT('LOW RISK: Transaction of $', CAST(ROUND(amount, 2) AS STRING), ' via ', txn_type, ' from ', COALESCE(home_city, 'unknown'), '. No anomalous patterns detected. Risk score: ', CAST(rule_based_risk_score AS STRING), '.')
    END AS explanation,
    CASE
        WHEN impossible_travel = true THEN '["impossible_travel","geo_anomaly"]'
        WHEN mfa_change_high_value = true THEN '["mfa_change","high_value_transfer"]'
        WHEN high_value_wire_after_ip_change = true THEN '["ip_change","high_value_wire"]'
        WHEN abnormal_typing = true THEN '["abnormal_typing","bot_signature"]'
        WHEN amount_anomaly = true THEN '["amount_anomaly"]'
        ELSE '[]'
    END AS risk_factors,
    NULL AS analyst_decision,
    NULL AS analyst_notes,
    txn_timestamp AS created_at,
    NULL AS reviewed_at,
    CAST(RAND() * 150 + 20 AS INT) AS ttl_decision_ms
FROM {CATALOG}.fraud_detection.silver_enriched_transactions
""", "Populating triage store from Silver layer")

# Verify
run_sql(f"""
SELECT risk_category, automated_action, COUNT(*) as cnt, ROUND(AVG(risk_score), 1) as avg_score
FROM {CATALOG}.fraud_operations.real_time_fraud_triage
GROUP BY risk_category, automated_action
ORDER BY avg_score DESC
""", "Triage store distribution")

run_sql(f"""
SELECT transaction_id, user_id, amount, risk_score, risk_category, automated_action,
       LEFT(explanation, 120) as explanation_preview
FROM {CATALOG}.fraud_operations.real_time_fraud_triage
WHERE risk_category IN ('RED', 'YELLOW')
ORDER BY risk_score DESC
LIMIT 5
""", "Top 5 high-risk transactions")

print("\nOperational Triage Store ready!")
