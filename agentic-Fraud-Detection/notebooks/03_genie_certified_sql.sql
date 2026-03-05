-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Fraud Triage Agent - Genie Space Certified SQL
-- MAGIC
-- MAGIC Pre-certified SQL queries for the Genie Space that fraud investigators can use
-- MAGIC via natural language. These serve as trusted query templates.
-- MAGIC
-- MAGIC **Tables Available:**
-- MAGIC - `fraud_detection.silver_enriched_transactions` - Enriched transactions with session + risk signals
-- MAGIC - `fraud_detection.silver_velocity_anomalies` - Velocity burst detection
-- MAGIC - `fraud_operations.gold_fraud_kpis` - Daily fraud KPIs
-- MAGIC - `fraud_operations.gold_account_takeover` - ATO rate metrics
-- MAGIC - `fraud_operations.gold_fraud_by_pattern` - Fraud by pattern type
-- MAGIC - `fraud_operations.real_time_fraud_triage` - Active triage decisions
-- MAGIC - `fraud_detection.user_profiles` - User profiles (PII masked)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Investigation Queries

-- COMMAND ----------

-- DBTITLE 1,Wire transfers over $10K where user changed MFA in last 24 hours
SELECT
    t.transaction_id,
    t.user_id,
    t.amount,
    t.txn_type,
    t.txn_timestamp,
    t.mfa_change_flag,
    t.mfa_change_timestamp,
    t.login_ip,
    t.device_fingerprint,
    t.rule_based_risk_score,
    t.explanation
FROM serverless_bir_catalog.fraud_detection.silver_enriched_transactions t
LEFT JOIN serverless_bir_catalog.fraud_operations.real_time_fraud_triage r
    ON t.transaction_id = r.transaction_id
WHERE t.txn_type = 'wire_transfer'
  AND t.amount > 10000
  AND t.mfa_change_flag = true
  AND t.mfa_change_timestamp >= t.txn_timestamp - INTERVAL 24 HOURS
ORDER BY t.amount DESC;

-- COMMAND ----------

-- DBTITLE 1,Impossible travel detections - users logging in from 500+ miles apart in under 10 minutes
SELECT
    user_id,
    transaction_id,
    amount,
    txn_type,
    txn_timestamp,
    home_city,
    login_geo_lat,
    login_geo_lon,
    ROUND(geo_distance_miles, 0) AS distance_miles,
    ROUND(time_since_prev_login_min, 1) AS minutes_between_logins,
    device_fingerprint,
    typing_cadence_score,
    is_bot_signature,
    rule_based_risk_score
FROM serverless_bir_catalog.fraud_detection.silver_enriched_transactions
WHERE impossible_travel = true
ORDER BY geo_distance_miles DESC;

-- COMMAND ----------

-- DBTITLE 1,Velocity anomalies - users with 5+ transactions in 5 minutes
SELECT
    user_id,
    window_start,
    window_end,
    txn_count,
    ROUND(total_amount, 2) AS total_amount,
    is_velocity_anomaly
FROM serverless_bir_catalog.fraud_detection.silver_velocity_anomalies
WHERE is_velocity_anomaly = true
ORDER BY txn_count DESC;

-- COMMAND ----------

-- DBTITLE 1,Bot signature detections with high-value transactions
SELECT
    transaction_id,
    user_id,
    amount,
    txn_type,
    txn_timestamp,
    typing_cadence_score,
    is_bot_signature,
    device_fingerprint,
    login_ip,
    rule_based_risk_score,
    account_age_days
FROM serverless_bir_catalog.fraud_detection.silver_enriched_transactions
WHERE is_bot_signature = true
  AND amount > 5000
ORDER BY amount DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Banking KPIs

-- COMMAND ----------

-- DBTITLE 1,False Positive Ratio (FPR) - Daily
SELECT
    report_date,
    total_transactions,
    red_flagged,
    yellow_flagged,
    actual_fraud_count,
    false_positive_ratio_pct AS fpr_pct,
    fraud_detection_rate_pct AS detection_rate_pct,
    avg_risk_score,
    ROUND(amount_at_risk, 2) AS amount_at_risk_usd
FROM serverless_bir_catalog.fraud_operations.gold_fraud_kpis
ORDER BY report_date DESC;

-- COMMAND ----------

-- DBTITLE 1,Account Takeover (ATO) Rate - Daily
SELECT
    report_date,
    total_active_users,
    impossible_travel_users,
    mfa_abuse_users,
    suspected_ato_users,
    ato_rate_pct
FROM serverless_bir_catalog.fraud_operations.gold_account_takeover
ORDER BY report_date DESC;

-- COMMAND ----------

-- DBTITLE 1,Fraud by Attack Pattern
SELECT
    pattern_type,
    txn_count,
    ROUND(total_amount, 2) AS total_amount_usd,
    avg_amount AS avg_amount_usd,
    avg_risk_score,
    unique_users AS affected_users
FROM serverless_bir_catalog.fraud_operations.gold_fraud_by_pattern
ORDER BY txn_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Triage Queue Queries

-- COMMAND ----------

-- DBTITLE 1,Pending yellow-flagged transactions for analyst review
SELECT
    transaction_id,
    user_id,
    ROUND(amount, 2) AS amount,
    txn_type,
    risk_score,
    risk_category,
    automated_action,
    explanation,
    created_at,
    ttl_decision_ms
FROM serverless_bir_catalog.fraud_operations.real_time_fraud_triage
WHERE risk_category = 'YELLOW'
  AND analyst_decision IS NULL
ORDER BY risk_score DESC, created_at ASC;

-- COMMAND ----------

-- DBTITLE 1,Blocked (RED) transactions summary
SELECT
    transaction_id,
    user_id,
    ROUND(amount, 2) AS amount,
    txn_type,
    risk_score,
    explanation,
    created_at
FROM serverless_bir_catalog.fraud_operations.real_time_fraud_triage
WHERE risk_category = 'RED'
ORDER BY risk_score DESC;

-- COMMAND ----------

-- DBTITLE 1,Triage queue summary statistics
SELECT
    risk_category,
    automated_action,
    COUNT(*) AS total_count,
    ROUND(AVG(risk_score), 1) AS avg_risk_score,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(ttl_decision_ms), 0) AS avg_latency_ms,
    SUM(CASE WHEN analyst_decision IS NOT NULL THEN 1 ELSE 0 END) AS reviewed,
    SUM(CASE WHEN analyst_decision IS NULL AND risk_category = 'YELLOW' THEN 1 ELSE 0 END) AS pending_review
FROM serverless_bir_catalog.fraud_operations.real_time_fraud_triage
GROUP BY risk_category, automated_action
ORDER BY avg_risk_score DESC;

-- COMMAND ----------

-- DBTITLE 1,High-risk users with multiple flagged transactions
SELECT
    user_id,
    COUNT(*) AS flagged_txn_count,
    ROUND(SUM(amount), 2) AS total_flagged_amount,
    ROUND(AVG(risk_score), 1) AS avg_risk_score,
    MAX(risk_score) AS max_risk_score,
    COLLECT_SET(automated_action) AS actions_taken,
    MIN(created_at) AS first_flag,
    MAX(created_at) AS last_flag
FROM serverless_bir_catalog.fraud_operations.real_time_fraud_triage
WHERE risk_category IN ('RED', 'YELLOW')
GROUP BY user_id
HAVING COUNT(*) > 1
ORDER BY flagged_txn_count DESC, avg_risk_score DESC;
