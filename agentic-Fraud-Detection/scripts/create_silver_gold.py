"""Create Silver (enriched) and Gold (KPI) tables for Fraud Triage."""
import subprocess
import json
import time

PROFILE = "vm2"
WAREHOUSE = "8620a950b7475da4"
CATALOG = "serverless_stable_p2uvy4_catalog"


def run_sql(sql, description="", timeout_polls=60):
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
        print(f"    -> {state}, polling {stmt_id}...")
        for i in range(timeout_polls):
            time.sleep(5)
            poll = subprocess.run(
                ["databricks", "api", "get", f"/api/2.0/sql/statements/{stmt_id}", "--profile", PROFILE],
                capture_output=True, text=True
            )
            poll_data = json.loads(poll.stdout)
            poll_state = poll_data.get("status", {}).get("state")
            if poll_state == "SUCCEEDED":
                rows = poll_data.get("result", {}).get("data_array", [])
                total = poll_data.get("manifest", {}).get("total_row_count", "n/a")
                print(f"    -> OK (rows: {total})")
                for row in rows[:10]:
                    print(f"       {row}")
                return poll_data
            elif poll_state in ("FAILED", "CANCELED", "CLOSED"):
                error = poll_data.get("status", {}).get("error", {}).get("message", "")
                print(f"    -> FAILED: {error[:300]}")
                return poll_data
            if i % 6 == 5:
                print(f"    ... still {poll_state} ({(i+1)*5}s)")
        print("    -> TIMEOUT")
        return None
    else:
        error = data.get("status", {}).get("error", {}).get("message", "")
        print(f"    -> {state}: {error[:300]}")
        return data


print("=" * 60)
print("Building Silver & Gold Layers")
print("=" * 60)

# ----------------------------------------------------------------
# SILVER: Enriched Transactions (join txns with login sessions)
# ----------------------------------------------------------------
run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.fraud_detection.silver_enriched_transactions AS
WITH login_with_prev AS (
    SELECT
        l.*,
        LAG(ip_address) OVER (PARTITION BY user_id ORDER BY login_timestamp) AS prev_ip,
        LAG(geo_lat) OVER (PARTITION BY user_id ORDER BY login_timestamp) AS prev_lat,
        LAG(geo_lon) OVER (PARTITION BY user_id ORDER BY login_timestamp) AS prev_lon,
        LAG(login_timestamp) OVER (PARTITION BY user_id ORDER BY login_timestamp) AS prev_login_time,
        CASE
            WHEN LAG(ip_address) OVER (PARTITION BY user_id ORDER BY login_timestamp) IS NOT NULL
             AND LAG(ip_address) OVER (PARTITION BY user_id ORDER BY login_timestamp) != ip_address
            THEN 1 ELSE 0
        END AS ip_change_flag,
        -- Haversine distance in miles from previous login
        CASE
            WHEN LAG(geo_lat) OVER (PARTITION BY user_id ORDER BY login_timestamp) IS NOT NULL
            THEN 3959 * ACOS(
                LEAST(1.0, GREATEST(-1.0,
                    SIN(RADIANS(geo_lat)) * SIN(RADIANS(LAG(geo_lat) OVER (PARTITION BY user_id ORDER BY login_timestamp)))
                    + COS(RADIANS(geo_lat)) * COS(RADIANS(LAG(geo_lat) OVER (PARTITION BY user_id ORDER BY login_timestamp)))
                    * COS(RADIANS(LAG(geo_lon) OVER (PARTITION BY user_id ORDER BY login_timestamp)) - RADIANS(geo_lon))
                ))
            )
            ELSE 0
        END AS geo_distance_miles,
        -- Time diff in minutes from previous login
        CASE
            WHEN LAG(login_timestamp) OVER (PARTITION BY user_id ORDER BY login_timestamp) IS NOT NULL
            THEN TIMESTAMPDIFF(MINUTE,
                LAG(login_timestamp) OVER (PARTITION BY user_id ORDER BY login_timestamp),
                login_timestamp)
            ELSE NULL
        END AS time_since_prev_login_min
    FROM {CATALOG}.fraud_detection.login_logs l
),
-- Get the most recent login session for each user before/around each transaction
txn_with_session AS (
    SELECT
        t.*,
        lp.session_id,
        lp.ip_address AS login_ip,
        lp.geo_lat AS login_geo_lat,
        lp.geo_lon AS login_geo_lon,
        lp.device_fingerprint,
        lp.mfa_change_flag,
        lp.mfa_change_timestamp,
        lp.typing_cadence_score,
        lp.is_bot_signature,
        lp.ip_change_flag,
        lp.geo_distance_miles,
        lp.time_since_prev_login_min,
        lp.login_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY t.transaction_id
            ORDER BY ABS(TIMESTAMPDIFF(SECOND, t.txn_timestamp, lp.login_timestamp))
        ) AS rn
    FROM {CATALOG}.fraud_detection.transactions t
    LEFT JOIN login_with_prev lp
        ON t.user_id = lp.user_id
        AND lp.login_timestamp BETWEEN t.txn_timestamp - INTERVAL 2 HOURS
            AND t.txn_timestamp + INTERVAL 30 MINUTES
)
SELECT
    s.transaction_id,
    s.user_id,
    s.amount,
    s.currency,
    s.txn_type,
    s.merchant_id,
    s.merchant_name,
    s.merchant_category,
    s.channel AS txn_channel,
    s.txn_timestamp,
    s.card_number_masked,
    s.is_international,
    s.is_fraud,
    s.fraud_pattern,
    -- Session context
    s.session_id,
    s.login_ip,
    s.login_geo_lat,
    s.login_geo_lon,
    s.device_fingerprint,
    s.mfa_change_flag,
    s.mfa_change_timestamp,
    s.typing_cadence_score,
    s.is_bot_signature,
    s.ip_change_flag,
    s.geo_distance_miles,
    s.time_since_prev_login_min,
    s.login_timestamp,
    -- User profile context
    p.account_age_days,
    p.avg_monthly_txn,
    p.home_city,
    p.risk_tier,
    -- Derived risk signals
    CASE WHEN s.geo_distance_miles > 500 AND s.time_since_prev_login_min < 10 THEN TRUE ELSE FALSE END AS impossible_travel,
    CASE WHEN s.mfa_change_flag = true AND s.amount > 10000 THEN TRUE ELSE FALSE END AS mfa_change_high_value,
    CASE WHEN s.txn_type = 'wire_transfer' AND s.amount > 10000 AND s.ip_change_flag = 1 THEN TRUE ELSE FALSE END AS high_value_wire_after_ip_change,
    CASE WHEN s.typing_cadence_score < 0.45 THEN TRUE ELSE FALSE END AS abnormal_typing,
    CASE WHEN s.amount > 5 * p.avg_monthly_txn THEN TRUE ELSE FALSE END AS amount_anomaly,
    -- Composite risk score (rule-based, 0-100)
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
""", "Creating Silver: enriched_transactions (joined + risk signals)")


# ----------------------------------------------------------------
# Velocity anomaly table (transactions per user per 5-min window)
# ----------------------------------------------------------------
run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.fraud_detection.silver_velocity_anomalies AS
SELECT
    user_id,
    window(txn_timestamp, '5 minutes').start AS window_start,
    window(txn_timestamp, '5 minutes').end AS window_end,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_amount,
    COLLECT_LIST(transaction_id) AS txn_ids,
    CASE WHEN COUNT(*) >= 5 THEN TRUE ELSE FALSE END AS is_velocity_anomaly
FROM {CATALOG}.fraud_detection.transactions
GROUP BY user_id, window(txn_timestamp, '5 minutes')
HAVING COUNT(*) >= 3
ORDER BY txn_count DESC
""", "Creating Silver: velocity_anomalies")


# ----------------------------------------------------------------
# GOLD: Fraud KPIs
# ----------------------------------------------------------------
run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.fraud_operations.gold_fraud_kpis AS
SELECT
    DATE(txn_timestamp) AS report_date,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN rule_based_risk_score >= 80 THEN 1 ELSE 0 END) AS red_flagged,
    SUM(CASE WHEN rule_based_risk_score >= 50 AND rule_based_risk_score < 80 THEN 1 ELSE 0 END) AS yellow_flagged,
    SUM(CASE WHEN rule_based_risk_score < 50 THEN 1 ELSE 0 END) AS green_allowed,
    SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END) AS actual_fraud_count,
    -- False Positive Ratio: flagged but not actually fraud / all flagged
    ROUND(
        SUM(CASE WHEN rule_based_risk_score >= 50 AND is_fraud = false THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN rule_based_risk_score >= 50 THEN 1 ELSE 0 END), 0),
    2) AS false_positive_ratio_pct,
    -- Detection Rate: caught fraud / total fraud
    ROUND(
        SUM(CASE WHEN rule_based_risk_score >= 50 AND is_fraud = true THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END), 0),
    2) AS fraud_detection_rate_pct,
    -- Average risk score
    ROUND(AVG(rule_based_risk_score), 2) AS avg_risk_score,
    -- Total amount at risk
    SUM(CASE WHEN rule_based_risk_score >= 50 THEN amount ELSE 0 END) AS amount_at_risk
FROM {CATALOG}.fraud_detection.silver_enriched_transactions
GROUP BY DATE(txn_timestamp)
ORDER BY report_date DESC
""", "Creating Gold: fraud_kpis (daily)")


# ----------------------------------------------------------------
# GOLD: Account Takeover Rate
# ----------------------------------------------------------------
run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.fraud_operations.gold_account_takeover AS
SELECT
    DATE(txn_timestamp) AS report_date,
    COUNT(DISTINCT user_id) AS total_active_users,
    COUNT(DISTINCT CASE WHEN impossible_travel = true THEN user_id END) AS impossible_travel_users,
    COUNT(DISTINCT CASE WHEN mfa_change_high_value = true THEN user_id END) AS mfa_abuse_users,
    COUNT(DISTINCT CASE WHEN impossible_travel = true OR mfa_change_high_value = true OR (is_bot_signature = true AND amount > 5000) THEN user_id END) AS suspected_ato_users,
    ROUND(
        COUNT(DISTINCT CASE WHEN impossible_travel = true OR mfa_change_high_value = true OR (is_bot_signature = true AND amount > 5000) THEN user_id END) * 100.0
        / NULLIF(COUNT(DISTINCT user_id), 0),
    4) AS ato_rate_pct
FROM {CATALOG}.fraud_detection.silver_enriched_transactions
GROUP BY DATE(txn_timestamp)
ORDER BY report_date DESC
""", "Creating Gold: account_takeover rate")


# ----------------------------------------------------------------
# GOLD: Fraud by pattern type
# ----------------------------------------------------------------
run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.fraud_operations.gold_fraud_by_pattern AS
SELECT
    COALESCE(fraud_pattern, 'legitimate') AS pattern_type,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_amount,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(AVG(rule_based_risk_score), 2) AS avg_risk_score,
    COUNT(DISTINCT user_id) AS unique_users
FROM {CATALOG}.fraud_detection.silver_enriched_transactions
GROUP BY COALESCE(fraud_pattern, 'legitimate')
ORDER BY txn_count DESC
""", "Creating Gold: fraud_by_pattern")


print("\n" + "=" * 60)
print("Silver & Gold layers complete!")
print("=" * 60)

# Quick verification
run_sql(f"""
SELECT
    'silver_enriched_transactions' as tbl, count(*) as cnt FROM {CATALOG}.fraud_detection.silver_enriched_transactions
UNION ALL SELECT 'silver_velocity_anomalies', count(*) FROM {CATALOG}.fraud_detection.silver_velocity_anomalies
UNION ALL SELECT 'gold_fraud_kpis', count(*) FROM {CATALOG}.fraud_operations.gold_fraud_kpis
UNION ALL SELECT 'gold_account_takeover', count(*) FROM {CATALOG}.fraud_operations.gold_account_takeover
UNION ALL SELECT 'gold_fraud_by_pattern', count(*) FROM {CATALOG}.fraud_operations.gold_fraud_by_pattern
""", "Final row counts")
