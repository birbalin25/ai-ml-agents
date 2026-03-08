# Databricks notebook source
# MAGIC %md
# MAGIC # Admin Observability — Materialized View Definitions (DLT Pipeline)
# MAGIC
# MAGIC This notebook defines all 11 Materialized Views as a DLT/SDP pipeline.
# MAGIC Each `@dlt.table()` with a batch source creates a **Materialized View**
# MAGIC in the pipeline's target catalog/schema (`bircatalog.admin`).
# MAGIC
# MAGIC **180-day rolling window** on all views.

# COMMAND ----------

import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ### MV 1: mv_billing_daily_by_sku
# MAGIC Grain: (usage_date, workspace_id, sku_name)

# COMMAND ----------

@dlt.table(
    name="mv_billing_daily_by_sku",
    comment="Billing pre-aggregated by date, workspace, SKU with category classifications (180-day window)",
)
def mv_billing_daily_by_sku():
    return spark.sql("""
        SELECT
            u.usage_date,
            u.workspace_id,
            u.sku_name,
            CASE
                WHEN (u.sku_name LIKE '%MODEL_SERVING%' OR u.sku_name LIKE '%FOUNDATION_MODEL%'
                      OR u.sku_name LIKE '%INFERENCE%' OR u.sku_name LIKE '%GPU%') THEN 'ML'
                WHEN (u.sku_name LIKE '%SQL%' OR u.sku_name LIKE '%WAREHOUSE%') THEN 'DW'
                WHEN (u.sku_name LIKE '%ALL_PURPOSE%' OR u.sku_name LIKE '%JOBS%' OR u.sku_name LIKE '%DLT%')
                     AND NOT (u.sku_name LIKE '%MODEL_SERVING%' OR u.sku_name LIKE '%FOUNDATION_MODEL%'
                              OR u.sku_name LIKE '%INFERENCE%' OR u.sku_name LIKE '%GPU%') THEN 'DE'
                ELSE 'Other'
            END AS category,
            CASE
                WHEN u.sku_name LIKE '%SERVERLESS%' THEN 'Serverless'
                WHEN u.sku_name LIKE '%JOBS%' THEN 'Job Clusters'
                WHEN u.sku_name LIKE '%ALL_PURPOSE%' THEN 'All-Purpose'
                WHEN u.sku_name LIKE '%DLT%' THEN 'DLT Pipelines'
                ELSE 'Other'
            END AS cluster_type,
            CASE
                WHEN u.sku_name LIKE '%MODEL_SERVING%' THEN 'Model Serving'
                WHEN u.sku_name LIKE '%FOUNDATION_MODEL%' THEN 'Foundation Models'
                WHEN u.sku_name LIKE '%INFERENCE%' THEN 'Real-Time Inference'
                WHEN u.sku_name LIKE '%GPU%' THEN 'GPU Compute'
                ELSE 'Other ML'
            END AS ml_workload_type,
            CASE
                WHEN u.sku_name LIKE '%SERVERLESS%' THEN 'Serverless'
                WHEN u.sku_name LIKE '%PRO%' THEN 'Pro'
                ELSE 'Classic'
            END AS warehouse_type,
            SUM(u.usage_quantity) AS total_dbu,
            SUM(u.usage_quantity * COALESCE(p.pricing.effective_list.default, 0.07)) AS total_cost
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices p
            ON u.sku_name = p.sku_name
            AND u.cloud = p.cloud
            AND u.usage_date >= COALESCE(p.price_start_time, u.usage_date)
            AND u.usage_date < COALESCE(p.price_end_time, DATE '9999-12-31')
        WHERE u.usage_date >= DATEADD(DAY, -180, CURRENT_DATE())
        GROUP BY u.usage_date, u.workspace_id, u.sku_name
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MV 2: mv_billing_daily_by_user
# MAGIC Grain: (usage_date, workspace_id, sku_name, user_identity, is_job)

# COMMAND ----------

@dlt.table(
    name="mv_billing_daily_by_user",
    comment="Billing per-user aggregation with category classification (180-day window)",
)
def mv_billing_daily_by_user():
    return spark.sql("""
        SELECT
            u.usage_date,
            u.workspace_id,
            u.sku_name,
            u.identity_metadata.run_as AS user_identity,
            (u.usage_metadata.job_run_id IS NOT NULL) AS is_job,
            CASE
                WHEN (u.sku_name LIKE '%MODEL_SERVING%' OR u.sku_name LIKE '%FOUNDATION_MODEL%'
                      OR u.sku_name LIKE '%INFERENCE%' OR u.sku_name LIKE '%GPU%') THEN 'ML'
                WHEN (u.sku_name LIKE '%SQL%' OR u.sku_name LIKE '%WAREHOUSE%') THEN 'DW'
                WHEN (u.sku_name LIKE '%ALL_PURPOSE%' OR u.sku_name LIKE '%JOBS%' OR u.sku_name LIKE '%DLT%')
                     AND NOT (u.sku_name LIKE '%MODEL_SERVING%' OR u.sku_name LIKE '%FOUNDATION_MODEL%'
                              OR u.sku_name LIKE '%INFERENCE%' OR u.sku_name LIKE '%GPU%') THEN 'DE'
                ELSE 'Other'
            END AS category,
            SUM(u.usage_quantity) AS total_dbu,
            SUM(u.usage_quantity * COALESCE(p.pricing.effective_list.default, 0.07)) AS total_cost
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices p
            ON u.sku_name = p.sku_name
            AND u.cloud = p.cloud
            AND u.usage_date >= COALESCE(p.price_start_time, u.usage_date)
            AND u.usage_date < COALESCE(p.price_end_time, DATE '9999-12-31')
        WHERE u.usage_date >= DATEADD(DAY, -180, CURRENT_DATE())
            AND u.identity_metadata.run_as IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5, 6
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MV 3: mv_cluster_details
# MAGIC Grain: (cluster_id, workspace_id)

# COMMAND ----------

@dlt.table(
    name="mv_cluster_details",
    comment="Cluster inventory with computed uptime and status (180-day window)",
)
def mv_cluster_details():
    return spark.sql("""
        SELECT cluster_id, workspace_id, cluster_name, cluster_source,
            owned_by, driver_node_type, worker_node_type,
            create_time, delete_time, uptime_hours, status_category
        FROM (
            SELECT
                cluster_id,
                workspace_id,
                cluster_name,
                cluster_source,
                owned_by,
                driver_node_type,
                worker_node_type,
                create_time,
                delete_time,
                TIMESTAMPDIFF(HOUR, create_time, COALESCE(delete_time, CURRENT_TIMESTAMP())) AS uptime_hours,
                CASE
                    WHEN delete_time IS NULL AND TIMESTAMPDIFF(HOUR, create_time, CURRENT_TIMESTAMP()) > 24
                        THEN 'LONG_RUNNING'
                    WHEN delete_time IS NULL THEN 'ACTIVE'
                    ELSE 'TERMINATED'
                END AS status_category,
                ROW_NUMBER() OVER (PARTITION BY cluster_id, workspace_id ORDER BY create_time DESC) AS rn
            FROM system.compute.clusters
            WHERE delete_time IS NULL
               OR delete_time >= DATEADD(DAY, -180, CURRENT_DATE())
        )
        WHERE rn = 1
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MV 4: mv_job_run_timeline
# MAGIC Grain: (job_id, run_id, workspace_id)

# COMMAND ----------

@dlt.table(
    name="mv_job_run_timeline",
    comment="Job run details with computed runtime (180-day window)",
)
def mv_job_run_timeline():
    return spark.sql("""
        SELECT job_id, run_id, workspace_id, run_name, run_type, result_state,
            period_start_time, period_end_time, runtime_seconds, compute_ids
        FROM (
            SELECT
                job_id,
                run_id,
                workspace_id,
                run_name,
                run_type,
                result_state,
                period_start_time,
                period_end_time,
                TIMESTAMPDIFF(SECOND, period_start_time, period_end_time) AS runtime_seconds,
                CAST(compute_ids AS STRING) AS compute_ids,
                ROW_NUMBER() OVER (PARTITION BY job_id, run_id, workspace_id ORDER BY period_end_time DESC) AS rn
            FROM system.lakeflow.job_run_timeline
            WHERE period_start_time >= DATEADD(DAY, -180, CURRENT_DATE())
        )
        WHERE rn = 1
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MV 5: mv_query_history_daily
# MAGIC Grain: (query_date, workspace_id, warehouse_id)

# COMMAND ----------

@dlt.table(
    name="mv_query_history_daily",
    comment="Query history aggregated daily per warehouse (180-day window)",
)
def mv_query_history_daily():
    return spark.sql("""
        SELECT
            DATE(start_time) AS query_date,
            workspace_id,
            compute.warehouse_id AS warehouse_id,
            COUNT(*) AS total_queries,
            SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_queries,
            AVG(total_duration_ms) AS avg_duration_ms,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_duration_ms) AS p95_duration_ms,
            SUM(total_duration_ms) AS total_duration_ms,
            SUM(CASE WHEN total_duration_ms > 300000 THEN 1 ELSE 0 END) AS slow_queries,
            SUM(read_bytes) AS total_read_bytes,
            SUM(read_rows) AS total_read_rows,
            SUM(produced_rows) AS total_produced_rows
        FROM system.query.history
        WHERE start_time >= DATEADD(DAY, -180, CURRENT_DATE())
        GROUP BY DATE(start_time), workspace_id, compute.warehouse_id
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MV 6: mv_query_history_by_user
# MAGIC Grain: (query_date, workspace_id, executed_by)

# COMMAND ----------

@dlt.table(
    name="mv_query_history_by_user",
    comment="Query history aggregated by user (180-day window)",
)
def mv_query_history_by_user():
    return spark.sql("""
        SELECT
            DATE(start_time) AS query_date,
            workspace_id,
            executed_by,
            COUNT(*) AS queries_run,
            AVG(total_duration_ms) AS avg_duration_ms,
            SUM(total_duration_ms) AS total_duration_ms,
            SUM(read_rows) AS total_read_rows,
            SUM(produced_rows) AS total_produced_rows
        FROM system.query.history
        WHERE start_time >= DATEADD(DAY, -180, CURRENT_DATE())
            AND executed_by IS NOT NULL
        GROUP BY DATE(start_time), workspace_id, executed_by
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MV 7: mv_query_runtime_distribution
# MAGIC Grain: (query_date, workspace_id, duration_bucket)

# COMMAND ----------

@dlt.table(
    name="mv_query_runtime_distribution",
    comment="Query runtime distribution buckets (180-day window)",
)
def mv_query_runtime_distribution():
    return spark.sql("""
        SELECT
            DATE(start_time) AS query_date,
            workspace_id,
            CASE
                WHEN total_duration_ms < 1000 THEN '< 1s'
                WHEN total_duration_ms < 5000 THEN '1-5s'
                WHEN total_duration_ms < 30000 THEN '5-30s'
                WHEN total_duration_ms < 60000 THEN '30s-1m'
                WHEN total_duration_ms < 300000 THEN '1-5m'
                WHEN total_duration_ms < 600000 THEN '5-10m'
                ELSE '> 10m'
            END AS duration_bucket,
            CASE
                WHEN total_duration_ms < 1000 THEN 1
                WHEN total_duration_ms < 5000 THEN 2
                WHEN total_duration_ms < 30000 THEN 3
                WHEN total_duration_ms < 60000 THEN 4
                WHEN total_duration_ms < 300000 THEN 5
                WHEN total_duration_ms < 600000 THEN 6
                ELSE 7
            END AS bucket_order,
            COUNT(*) AS query_count
        FROM system.query.history
        WHERE start_time >= DATEADD(DAY, -180, CURRENT_DATE())
        GROUP BY 1, 2, 3, 4
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MV 8: mv_warehouse_concurrency
# MAGIC Grain: (query_date, query_hour, workspace_id, warehouse_id)

# COMMAND ----------

@dlt.table(
    name="mv_warehouse_concurrency",
    comment="Query concurrency by hour per warehouse (180-day window)",
)
def mv_warehouse_concurrency():
    return spark.sql("""
        SELECT
            DATE(start_time) AS query_date,
            HOUR(start_time) AS query_hour,
            workspace_id,
            compute.warehouse_id AS warehouse_id,
            COUNT(*) AS concurrent_queries
        FROM system.query.history
        WHERE start_time >= DATEADD(DAY, -180, CURRENT_DATE())
            AND compute.warehouse_id IS NOT NULL
        GROUP BY DATE(start_time), HOUR(start_time), workspace_id, compute.warehouse_id
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MV 9: mv_long_running_queries
# MAGIC Grain: (statement_id, workspace_id) — filtered to queries > 5 min

# COMMAND ----------

@dlt.table(
    name="mv_long_running_queries",
    comment="Queries exceeding 5 minutes with truncated SQL text (180-day window)",
)
def mv_long_running_queries():
    return spark.sql("""
        SELECT
            statement_id,
            workspace_id,
            executed_by,
            SUBSTRING(statement_text, 1, 200) AS query_text,
            total_duration_ms / 1000.0 AS runtime_seconds,
            compute.warehouse_id AS warehouse_id,
            read_rows AS rows_scanned,
            read_bytes,
            execution_status AS status,
            start_time
        FROM system.query.history
        WHERE start_time >= DATEADD(DAY, -180, CURRENT_DATE())
            AND total_duration_ms > 300000
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MV 10: mv_serving_endpoints
# MAGIC Grain: (endpoint_name, model_type, workspace_id)

# COMMAND ----------

@dlt.table(
    name="mv_serving_endpoints",
    comment="Model serving endpoint activity summary (180-day window)",
)
def mv_serving_endpoints():
    return spark.sql("""
        SELECT
            served_entity_name AS endpoint_name,
            entity_type AS model_type,
            workspace_id,
            MIN(change_time) AS first_seen,
            MAX(change_time) AS last_seen
        FROM system.serving.served_entities
        WHERE change_time >= DATEADD(DAY, -180, CURRENT_DATE())
        GROUP BY served_entity_name, entity_type, workspace_id
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MV 11: mv_experiment_runs
# MAGIC Grain: (experiment_id, run_id, workspace_id)

# COMMAND ----------

@dlt.table(
    name="mv_experiment_runs",
    comment="MLflow experiment runs with computed runtime (180-day window)",
)
def mv_experiment_runs():
    return spark.sql("""
        SELECT
            experiment_id,
            run_id,
            workspace_id,
            status,
            start_time,
            end_time,
            TIMESTAMPDIFF(SECOND, start_time, COALESCE(end_time, CURRENT_TIMESTAMP())) AS runtime_seconds,
            created_by AS user_id
        FROM system.mlflow.runs_latest
        WHERE start_time >= DATEADD(DAY, -180, CURRENT_DATE())
    """)
