"""
Fraud Triage Agent - Databricks Connect Local Development
Use this script with Databricks Connect to run fraud detection logic
against the platform's distributed compute from your local IDE.
"""

from databricks.connect import DatabricksSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Connect to the vm workspace
spark = DatabricksSession.builder.remote(
    host="https://fevm-serverless-bir.cloud.databricks.com",
    cluster_id="<your-cluster-id>",  # Replace with your cluster ID
).profile("vm").getOrCreate()

CATALOG = "serverless_bir_catalog"


def detect_impossible_travel(user_id: str = None, window_minutes: int = 10,
                              distance_threshold_miles: int = 500):
    """
    Flag users with geolocation jumps > 500 miles in < 10 minutes.
    This indicates compromised credentials being used from a different location.
    """
    login_df = spark.table(f"{CATALOG}.fraud_detection.login_logs")
    user_window = Window.partitionBy("user_id").orderBy("login_timestamp")

    result = (login_df
        .withColumn("prev_lat", lag("geo_lat").over(user_window))
        .withColumn("prev_lon", lag("geo_lon").over(user_window))
        .withColumn("prev_time", lag("login_timestamp").over(user_window))
        .withColumn("time_diff_min",
            (unix_timestamp("login_timestamp") - unix_timestamp("prev_time")) / 60)
        .withColumn("distance_miles",
            when(col("prev_lat").isNotNull(),
                lit(3959) * acos(
                    least(lit(1.0), greatest(lit(-1.0),
                        sin(radians(col("geo_lat"))) * sin(radians(col("prev_lat")))
                        + cos(radians(col("geo_lat"))) * cos(radians(col("prev_lat")))
                        * cos(radians(col("prev_lon")) - radians(col("geo_lon")))
                    ))
                )
            ).otherwise(lit(0)))
        .filter(
            (col("time_diff_min") < window_minutes) &
            (col("distance_miles") > distance_threshold_miles)
        )
    )

    if user_id:
        result = result.filter(col("user_id") == user_id)

    return result.select(
        "user_id", "session_id", "ip_address",
        "geo_lat", "geo_lon", "prev_lat", "prev_lon",
        "login_timestamp", "prev_time",
        round("time_diff_min", 1).alias("minutes_between"),
        round("distance_miles", 0).alias("distance_miles"),
        "device_fingerprint", "is_bot_signature"
    ).orderBy(col("distance_miles").desc())


def detect_velocity_anomalies(min_txn_count: int = 5, window_minutes: int = 5):
    """
    Find users with unusually high transaction frequency.
    """
    txn_df = spark.table(f"{CATALOG}.fraud_detection.transactions")

    return (txn_df
        .groupBy("user_id", window("txn_timestamp", f"{window_minutes} minutes"))
        .agg(
            count("*").alias("txn_count"),
            sum("amount").alias("total_amount"),
            collect_list("transaction_id").alias("txn_ids"),
        )
        .filter(col("txn_count") >= min_txn_count)
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .orderBy(col("txn_count").desc())
    )


def get_high_risk_transactions(min_score: int = 50, limit: int = 20):
    """
    Get top high-risk transactions from the enriched Silver layer.
    """
    return (spark.table(f"{CATALOG}.fraud_detection.silver_enriched_transactions")
        .filter(col("rule_based_risk_score") >= min_score)
        .select(
            "transaction_id", "user_id", "amount", "txn_type",
            "rule_based_risk_score", "impossible_travel", "mfa_change_high_value",
            "high_value_wire_after_ip_change", "abnormal_typing", "amount_anomaly",
            "geo_distance_miles", "time_since_prev_login_min",
            "home_city", "account_age_days"
        )
        .orderBy(col("rule_based_risk_score").desc())
        .limit(limit)
    )


def get_fraud_kpis():
    """Get fraud detection KPIs."""
    return spark.table(f"{CATALOG}.fraud_operations.gold_fraud_kpis").orderBy(col("report_date").desc())


# --- Main ---
if __name__ == "__main__":
    print("=== Impossible Travel Detections ===")
    impossible = detect_impossible_travel()
    impossible.show(10, truncate=False)

    print("\n=== Velocity Anomalies ===")
    velocity = detect_velocity_anomalies()
    velocity.show(10, truncate=False)

    print("\n=== High Risk Transactions ===")
    high_risk = get_high_risk_transactions()
    high_risk.show(20, truncate=False)

    print("\n=== Fraud KPIs ===")
    kpis = get_fraud_kpis()
    kpis.show(10, truncate=False)
