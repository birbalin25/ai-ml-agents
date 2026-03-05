# Databricks notebook source
# MAGIC %md
# MAGIC # Fraud Triage Agent - Lakeflow Spark Declarative Pipeline (DLT)
# MAGIC
# MAGIC This pipeline ingests mock banking data from Unity Catalog Volume and builds
# MAGIC Bronze → Silver → Gold layers for fraud detection.
# MAGIC
# MAGIC **Bronze**: Raw ingestion from CSV files
# MAGIC **Silver**: Enriched transactions joined with login sessions, risk signals computed
# MAGIC **Gold**: KPIs (False Positive Ratio, Account Takeover Rate, Fraud by Pattern)

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

CATALOG = spark.conf.get("pipeline.catalog", "serverless_bir_catalog")
VOLUME_PATH = f"/Volumes/{CATALOG}/fraud_detection/source_files"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer: Raw Ingestion

# COMMAND ----------

@dlt.table(
    name="bronze_transactions",
    comment="Raw transaction data ingested from Volume CSV",
    table_properties={"quality": "bronze"}
)
def bronze_transactions():
    return (spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{VOLUME_PATH}/transactions.csv"))


@dlt.table(
    name="bronze_login_logs",
    comment="Raw login log data ingested from Volume CSV",
    table_properties={"quality": "bronze"}
)
def bronze_login_logs():
    return (spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{VOLUME_PATH}/login_logs.csv"))


@dlt.table(
    name="bronze_user_profiles",
    comment="Raw user profile data ingested from Volume CSV",
    table_properties={"quality": "bronze"}
)
def bronze_user_profiles():
    return (spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{VOLUME_PATH}/user_profiles.csv"))


@dlt.table(
    name="bronze_fraud_signatures",
    comment="Known fraud pattern signatures",
    table_properties={"quality": "bronze"}
)
def bronze_fraud_signatures():
    return (spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{VOLUME_PATH}/known_fraud_signatures.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Enriched & Joined

# COMMAND ----------

@dlt.table(
    name="silver_enriched_transactions",
    comment="Transactions enriched with login session context, user profile, and computed risk signals",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect("valid_user", "user_id IS NOT NULL")
def silver_enriched_transactions():
    txns = dlt.read("bronze_transactions")
    logins = dlt.read("bronze_login_logs")
    profiles = dlt.read("bronze_user_profiles")

    # Add previous login context to login logs
    login_window = Window.partitionBy("user_id").orderBy("login_timestamp")

    logins_enriched = (logins
        .withColumn("prev_ip", lag("ip_address").over(login_window))
        .withColumn("prev_lat", lag("geo_lat").over(login_window))
        .withColumn("prev_lon", lag("geo_lon").over(login_window))
        .withColumn("prev_login_time", lag("login_timestamp").over(login_window))
        .withColumn("ip_change_flag",
            when(col("prev_ip").isNotNull() & (col("prev_ip") != col("ip_address")), 1).otherwise(0))
        .withColumn("geo_distance_miles",
            when(col("prev_lat").isNotNull(),
                lit(3959) * acos(
                    least(lit(1.0), greatest(lit(-1.0),
                        sin(radians(col("geo_lat"))) * sin(radians(col("prev_lat")))
                        + cos(radians(col("geo_lat"))) * cos(radians(col("prev_lat")))
                        * cos(radians(col("prev_lon")) - radians(col("geo_lon")))
                    ))
                )
            ).otherwise(lit(0)))
        .withColumn("time_since_prev_login_min",
            when(col("prev_login_time").isNotNull(),
                (unix_timestamp("login_timestamp") - unix_timestamp("prev_login_time")) / 60
            ).otherwise(lit(None)))
    )

    # Join transactions with closest login session
    joined = (txns.alias("t")
        .join(logins_enriched.alias("l"),
            (col("t.user_id") == col("l.user_id")) &
            (col("l.login_timestamp").between(
                col("t.txn_timestamp") - expr("INTERVAL 2 HOURS"),
                col("t.txn_timestamp") + expr("INTERVAL 30 MINUTES")
            )),
            "left"
        )
        .withColumn("rn", row_number().over(
            Window.partitionBy("t.transaction_id")
            .orderBy(abs(unix_timestamp("t.txn_timestamp") - unix_timestamp("l.login_timestamp")))
        ))
        .filter(col("rn") == 1)
        .join(profiles.alias("p"), col("t.user_id") == col("p.user_id"), "left")
    )

    # Compute risk signals
    result = (joined
        .select(
            col("t.transaction_id"),
            col("t.user_id"),
            col("t.amount"),
            col("t.currency"),
            col("t.txn_type"),
            col("t.merchant_id"),
            col("t.merchant_name"),
            col("t.merchant_category"),
            col("t.channel").alias("txn_channel"),
            col("t.txn_timestamp"),
            col("t.card_number_masked"),
            col("t.is_international"),
            col("t.is_fraud"),
            col("t.fraud_pattern"),
            col("l.session_id"),
            col("l.ip_address").alias("login_ip"),
            col("l.geo_lat").alias("login_geo_lat"),
            col("l.geo_lon").alias("login_geo_lon"),
            col("l.device_fingerprint"),
            col("l.mfa_change_flag"),
            col("l.mfa_change_timestamp"),
            col("l.typing_cadence_score"),
            col("l.is_bot_signature"),
            col("l.ip_change_flag"),
            col("l.geo_distance_miles"),
            col("l.time_since_prev_login_min"),
            col("l.login_timestamp"),
            col("p.account_age_days"),
            col("p.avg_monthly_txn"),
            col("p.home_city"),
            col("p.risk_tier"),
        )
        .withColumn("impossible_travel",
            when((col("geo_distance_miles") > 500) & (col("time_since_prev_login_min") < 10), True).otherwise(False))
        .withColumn("mfa_change_high_value",
            when((col("mfa_change_flag") == True) & (col("amount") > 10000), True).otherwise(False))
        .withColumn("high_value_wire_after_ip_change",
            when((col("txn_type") == "wire_transfer") & (col("amount") > 10000) & (col("ip_change_flag") == 1), True).otherwise(False))
        .withColumn("abnormal_typing",
            when(col("typing_cadence_score") < 0.45, True).otherwise(False))
        .withColumn("amount_anomaly",
            when(col("amount") > 5 * col("avg_monthly_txn"), True).otherwise(False))
        .withColumn("rule_based_risk_score",
            least(lit(100), greatest(lit(0),
                when((col("geo_distance_miles") > 500) & (col("time_since_prev_login_min") < 10), 40).otherwise(0)
                + when((col("mfa_change_flag") == True) & (col("amount") > 10000), 30).otherwise(0)
                + when((col("ip_change_flag") == 1) & (col("txn_type") == "wire_transfer") & (col("amount") > 10000), 25).otherwise(0)
                + when(col("typing_cadence_score") < 0.45, 15).otherwise(0)
                + when(col("is_bot_signature") == True, 20).otherwise(0)
                + when(col("amount") > 5 * col("avg_monthly_txn"), 10).otherwise(0)
                + when((col("is_international") == True) & (col("account_age_days") < 90), 15).otherwise(0)
            ))
        )
    )
    return result

# COMMAND ----------

@dlt.table(
    name="silver_velocity_anomalies",
    comment="Users with 3+ transactions in a 5-minute window",
    table_properties={"quality": "silver"}
)
def silver_velocity_anomalies():
    txns = dlt.read("bronze_transactions")
    return (txns
        .groupBy("user_id", window("txn_timestamp", "5 minutes"))
        .agg(
            count("*").alias("txn_count"),
            sum("amount").alias("total_amount"),
            collect_list("transaction_id").alias("txn_ids"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .withColumn("is_velocity_anomaly", when(col("txn_count") >= 5, True).otherwise(False))
        .filter(col("txn_count") >= 3)
        .orderBy(col("txn_count").desc())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: KPIs

# COMMAND ----------

@dlt.table(
    name="gold_fraud_kpis",
    comment="Daily fraud detection KPIs: FPR, detection rate, flagged counts",
    table_properties={"quality": "gold"}
)
def gold_fraud_kpis():
    enriched = dlt.read("silver_enriched_transactions")
    return (enriched
        .groupBy(date_trunc("day", "txn_timestamp").alias("report_date"))
        .agg(
            count("*").alias("total_transactions"),
            sum(when(col("rule_based_risk_score") >= 80, 1).otherwise(0)).alias("red_flagged"),
            sum(when((col("rule_based_risk_score") >= 50) & (col("rule_based_risk_score") < 80), 1).otherwise(0)).alias("yellow_flagged"),
            sum(when(col("rule_based_risk_score") < 50, 1).otherwise(0)).alias("green_allowed"),
            sum(when(col("is_fraud") == True, 1).otherwise(0)).alias("actual_fraud_count"),
            round(
                sum(when((col("rule_based_risk_score") >= 50) & (col("is_fraud") == False), 1).otherwise(0)) * 100.0
                / sum(when(col("rule_based_risk_score") >= 50, 1).otherwise(0)),
            2).alias("false_positive_ratio_pct"),
            round(
                sum(when((col("rule_based_risk_score") >= 50) & (col("is_fraud") == True), 1).otherwise(0)) * 100.0
                / sum(when(col("is_fraud") == True, 1).otherwise(0)),
            2).alias("fraud_detection_rate_pct"),
            round(avg("rule_based_risk_score"), 2).alias("avg_risk_score"),
            sum(when(col("rule_based_risk_score") >= 50, col("amount")).otherwise(0)).alias("amount_at_risk"),
        )
        .orderBy(col("report_date").desc())
    )


@dlt.table(
    name="gold_account_takeover",
    comment="Daily account takeover (ATO) rate metrics",
    table_properties={"quality": "gold"}
)
def gold_account_takeover():
    enriched = dlt.read("silver_enriched_transactions")
    return (enriched
        .groupBy(date_trunc("day", "txn_timestamp").alias("report_date"))
        .agg(
            countDistinct("user_id").alias("total_active_users"),
            countDistinct(when(col("impossible_travel") == True, col("user_id"))).alias("impossible_travel_users"),
            countDistinct(when(col("mfa_change_high_value") == True, col("user_id"))).alias("mfa_abuse_users"),
            countDistinct(when(
                (col("impossible_travel") == True) |
                (col("mfa_change_high_value") == True) |
                ((col("is_bot_signature") == True) & (col("amount") > 5000)),
                col("user_id")
            )).alias("suspected_ato_users"),
        )
        .withColumn("ato_rate_pct",
            round(col("suspected_ato_users") * 100.0 / col("total_active_users"), 4))
        .orderBy(col("report_date").desc())
    )


@dlt.table(
    name="gold_fraud_by_pattern",
    comment="Fraud statistics grouped by attack pattern type",
    table_properties={"quality": "gold"}
)
def gold_fraud_by_pattern():
    enriched = dlt.read("silver_enriched_transactions")
    return (enriched
        .withColumn("pattern", coalesce(col("fraud_pattern"), lit("legitimate")))
        .groupBy("pattern")
        .agg(
            count("*").alias("txn_count"),
            sum("amount").alias("total_amount"),
            round(avg("amount"), 2).alias("avg_amount"),
            round(avg("rule_based_risk_score"), 2).alias("avg_risk_score"),
            countDistinct("user_id").alias("unique_users"),
        )
        .orderBy(col("txn_count").desc())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Operational Layer: Fraud Triage Store
# MAGIC Derived from Silver enriched transactions. This table feeds the Live Fraud Queue app
# MAGIC via Lakebase sync.

# COMMAND ----------

@dlt.table(
    name="real_time_fraud_triage",
    comment="Operational triage store for real-time fraud decisions, derived from silver_enriched_transactions",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
    }
)
def real_time_fraud_triage():
    enriched = dlt.read("silver_enriched_transactions")
    return (enriched
        .withColumn("risk_score", col("rule_based_risk_score").cast("int"))
        .withColumn("risk_category",
            when(col("rule_based_risk_score") >= 80, lit("RED"))
            .when(col("rule_based_risk_score") >= 50, lit("YELLOW"))
            .otherwise(lit("GREEN"))
        )
        .withColumn("automated_action",
            when(col("rule_based_risk_score") >= 80, lit("BLOCK"))
            .when(col("rule_based_risk_score") >= 50, lit("YELLOW_FLAG"))
            .otherwise(lit("ALLOW"))
        )
        .withColumn("explanation",
            when((col("impossible_travel") == True) & (col("mfa_change_high_value") == True),
                concat(
                    lit("CRITICAL: Impossible travel detected ("),
                    round(col("geo_distance_miles"), 0).cast("string"),
                    lit(" miles in "),
                    round(col("time_since_prev_login_min"), 0).cast("string"),
                    lit(" min) combined with MFA change before $"),
                    round(col("amount"), 2).cast("string"),
                    lit(" "), col("txn_type"), lit(".")
                ))
            .when(col("impossible_travel") == True,
                concat(
                    lit("HIGH RISK: Geolocation jumped "),
                    round(col("geo_distance_miles"), 0).cast("string"),
                    lit(" miles in "),
                    round(col("time_since_prev_login_min"), 0).cast("string"),
                    lit(" minutes. Previous login from "),
                    coalesce(col("home_city"), lit("unknown")), lit(".")
                ))
            .when(col("mfa_change_high_value") == True,
                concat(
                    lit("HIGH RISK: MFA changed followed by high-value "),
                    col("txn_type"), lit(" of $"),
                    round(col("amount"), 2).cast("string"),
                    lit(". Matches known account takeover patterns.")
                ))
            .when(col("high_value_wire_after_ip_change") == True,
                concat(
                    lit("ELEVATED: IP changed before "),
                    col("txn_type"), lit(" of $"),
                    round(col("amount"), 2).cast("string"), lit(".")
                ))
            .when((col("abnormal_typing") == True) & (col("amount") > 5000),
                concat(
                    lit("ELEVATED: Abnormal typing cadence ("),
                    col("typing_cadence_score").cast("string"),
                    lit(") for $"),
                    round(col("amount"), 2).cast("string"),
                    lit(" "), col("txn_type"), lit(".")
                ))
            .when(col("amount_anomaly") == True,
                concat(
                    lit("MODERATE: $"),
                    round(col("amount"), 2).cast("string"),
                    lit(" exceeds avg $"),
                    round(col("avg_monthly_txn"), 2).cast("string"),
                    lit(" ("),
                    round(col("amount") / col("avg_monthly_txn"), 1).cast("string"),
                    lit("x).")
                ))
            .otherwise(
                concat(
                    lit("LOW RISK: $"),
                    round(col("amount"), 2).cast("string"),
                    lit(" "), col("txn_type"),
                    lit(". No anomalies. Score: "),
                    col("rule_based_risk_score").cast("string"), lit(".")
                ))
        )
        .withColumn("risk_factors",
            when(col("impossible_travel") == True, lit('["impossible_travel","geo_anomaly"]'))
            .when(col("mfa_change_high_value") == True, lit('["mfa_change","high_value_transfer"]'))
            .when(col("high_value_wire_after_ip_change") == True, lit('["ip_change","high_value_wire"]'))
            .when(col("abnormal_typing") == True, lit('["abnormal_typing","bot_signature"]'))
            .when(col("amount_anomaly") == True, lit('["amount_anomaly"]'))
            .otherwise(lit("[]"))
        )
        .withColumn("analyst_decision", lit(None).cast("string"))
        .withColumn("analyst_notes", lit(None).cast("string"))
        .withColumn("created_at", col("txn_timestamp"))
        .withColumn("reviewed_at", lit(None).cast("timestamp"))
        .withColumn("ttl_decision_ms", (rand() * 150 + 20).cast("int"))
        .select(
            "transaction_id", "user_id", "amount", "txn_type",
            "risk_score", "risk_category", "automated_action",
            "explanation", "risk_factors",
            "analyst_decision", "analyst_notes",
            "created_at", "reviewed_at", "ttl_decision_ms"
        )
    )
