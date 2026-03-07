"""
SQL queries for Databricks Platform Insights.
All queries target Lakebase (PostgreSQL) synced materialized view tables.
Functions return (sql, params) tuples for parameterized execution.
"""

from config import LAKEBASE_SCHEMA

# ---------------------------------------------------------------------------
# Table references — derived from the configured Lakebase schema
# ---------------------------------------------------------------------------
_BILLING_SKU = f"{LAKEBASE_SCHEMA}.synced_mv_billing_daily_by_sku"
_BILLING_USER = f"{LAKEBASE_SCHEMA}.synced_mv_billing_daily_by_user"
_CLUSTERS = f"{LAKEBASE_SCHEMA}.synced_mv_cluster_details"
_JOBS = f"{LAKEBASE_SCHEMA}.synced_mv_job_run_timeline"
_QUERY_DAILY = f"{LAKEBASE_SCHEMA}.synced_mv_query_history_daily"
_QUERY_USER = f"{LAKEBASE_SCHEMA}.synced_mv_query_history_by_user"
_QUERY_DIST = f"{LAKEBASE_SCHEMA}.synced_mv_query_runtime_distribution"
_CONCURRENCY = f"{LAKEBASE_SCHEMA}.synced_mv_warehouse_concurrency"
_LONG_QUERIES = f"{LAKEBASE_SCHEMA}.synced_mv_long_running_queries"
_SERVING = f"{LAKEBASE_SCHEMA}.synced_mv_serving_endpoints"
_EXPERIMENTS = f"{LAKEBASE_SCHEMA}.synced_mv_experiment_runs"


# ===============================================================================
# FILTER HELPERS
# ===============================================================================

def _ws_clause(workspace_ids, params):
    """Append workspace filter clause and params if needed."""
    if workspace_ids:
        if isinstance(workspace_ids, (list, tuple)) and len(workspace_ids) > 0:
            placeholders = ", ".join(["%s"] * len(workspace_ids))
            params.extend(workspace_ids)
            return f" AND workspace_id IN ({placeholders})"
        params.append(workspace_ids)
        return " AND workspace_id = %s"
    return ""


# ===============================================================================
# LISTING QUERIES (for filter dropdowns)
# ===============================================================================

def list_workspaces(start_date: str, end_date: str):
    params = [start_date, end_date]
    return (
        f"SELECT DISTINCT workspace_id "
        f"FROM {_BILLING_SKU} "
        f"WHERE usage_date BETWEEN %s AND %s AND workspace_id IS NOT NULL "
        f"ORDER BY workspace_id",
        tuple(params),
    )


def last_refreshed():
    """Return the most recent usage_date in the billing MV (data freshness indicator)."""
    return (
        f"SELECT MAX(usage_date) AS last_refreshed FROM {_BILLING_SKU}",
        (),
    )


# ===============================================================================
# OVERVIEW / CROSS-PLATFORM QUERIES
# ===============================================================================

def overview_total_dbu_and_cost(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT COALESCE(SUM(total_dbu), 0) AS total_dbu, "
        f"COALESCE(SUM(total_cost), 0) AS total_cost "
        f"FROM {_BILLING_SKU} "
        f"WHERE usage_date BETWEEN %s AND %s{ws}",
        tuple(params),
    )


def overview_dbu_by_category(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT category, SUM(total_dbu) AS total_dbu, SUM(total_cost) AS total_cost "
        f"FROM {_BILLING_SKU} "
        f"WHERE usage_date BETWEEN %s AND %s{ws} "
        f"GROUP BY category ORDER BY total_cost DESC",
        tuple(params),
    )


def overview_cost_trend(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT usage_date, category, "
        f"SUM(total_dbu) AS total_dbu, SUM(total_cost) AS total_cost "
        f"FROM {_BILLING_SKU} "
        f"WHERE usage_date BETWEEN %s AND %s{ws} "
        f"GROUP BY usage_date, category ORDER BY usage_date",
        tuple(params),
    )


def overview_cost_by_workspace(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT workspace_id, SUM(total_dbu) AS total_dbu, SUM(total_cost) AS total_cost "
        f"FROM {_BILLING_SKU} "
        f"WHERE usage_date BETWEEN %s AND %s{ws} "
        f"GROUP BY workspace_id ORDER BY total_cost DESC LIMIT 50",
        tuple(params),
    )


def overview_cost_by_sku(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT sku_name, SUM(total_dbu) AS total_dbu, SUM(total_cost) AS total_cost "
        f"FROM {_BILLING_SKU} "
        f"WHERE usage_date BETWEEN %s AND %s{ws} "
        f"GROUP BY sku_name ORDER BY total_cost DESC LIMIT 30",
        tuple(params),
    )


def overview_cost_by_user(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT is_job, user_identity, "
        f"SUM(total_dbu) AS total_dbu, SUM(total_cost) AS total_cost "
        f"FROM {_BILLING_USER} "
        f"WHERE usage_date BETWEEN %s AND %s{ws} "
        f"GROUP BY is_job, user_identity ORDER BY total_cost DESC LIMIT 50",
        tuple(params),
    )


def overview_active_clusters(start_date: str, end_date: str, workspace_id=None):
    params = [end_date, start_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT COUNT(DISTINCT cluster_id) AS active_clusters "
        f"FROM {_CLUSTERS} "
        f"WHERE create_time::date <= %s AND (delete_time IS NULL OR delete_time::date >= %s){ws}",
        tuple(params),
    )


def overview_active_warehouses(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT COUNT(DISTINCT warehouse_id) AS active_warehouses "
        f"FROM {_QUERY_DAILY} "
        f"WHERE query_date BETWEEN %s AND %s{ws}",
        tuple(params),
    )


def overview_active_users(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT COUNT(DISTINCT user_identity) AS active_users "
        f"FROM {_BILLING_USER} "
        f"WHERE usage_date BETWEEN %s AND %s{ws}",
        tuple(params),
    )


# ===============================================================================
# DATA ENGINEERING (DE) QUERIES
# ===============================================================================

def de_summary_kpis(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT COALESCE(SUM(total_dbu), 0) AS total_dbu, "
        f"COALESCE(SUM(total_cost), 0) AS total_cost "
        f"FROM {_BILLING_SKU} "
        f"WHERE usage_date BETWEEN %s AND %s AND category = 'DE'{ws}",
        tuple(params),
    )


def de_cluster_kpis(start_date: str, end_date: str, workspace_id=None):
    params = [end_date, start_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT COUNT(DISTINCT cluster_id) AS total_clusters, "
        f"AVG(EXTRACT(EPOCH FROM (COALESCE(delete_time, NOW()) - create_time)) / 3600) "
        f"AS avg_uptime_hours "
        f"FROM {_CLUSTERS} "
        f"WHERE create_time::date <= %s AND (delete_time IS NULL OR delete_time::date >= %s){ws}",
        tuple(params),
    )


def de_job_kpis(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT COUNT(*) AS total_jobs, "
        f"SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed_jobs, "
        f"ROUND(SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) * 100.0 "
        f"/ NULLIF(COUNT(*), 0), 2) AS failed_rate, "
        f"AVG(runtime_seconds) AS avg_runtime_seconds "
        f"FROM {_JOBS} "
        f"WHERE period_start_time::date BETWEEN %s AND %s{ws}",
        tuple(params),
    )


def de_dbu_by_cluster_type(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT cluster_type, SUM(total_dbu) AS total_dbu, SUM(total_cost) AS total_cost "
        f"FROM {_BILLING_SKU} "
        f"WHERE usage_date BETWEEN %s AND %s AND category = 'DE'{ws} "
        f"GROUP BY cluster_type ORDER BY total_cost DESC",
        tuple(params),
    )


def de_dbu_trend(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT usage_date, cluster_type, "
        f"SUM(total_dbu) AS total_dbu, SUM(total_cost) AS total_cost "
        f"FROM {_BILLING_SKU} "
        f"WHERE usage_date BETWEEN %s AND %s AND category = 'DE'{ws} "
        f"GROUP BY usage_date, cluster_type ORDER BY usage_date",
        tuple(params),
    )


def de_top_users(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT user_identity AS user, "
        f"SUM(total_dbu) AS total_dbu, SUM(total_cost) AS total_cost "
        f"FROM {_BILLING_USER} "
        f"WHERE usage_date BETWEEN %s AND %s AND category = 'DE'{ws} "
        f"GROUP BY user_identity ORDER BY total_cost DESC LIMIT 20",
        tuple(params),
    )


def de_long_running_jobs(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT job_id, run_id, run_name, run_type, result_state AS status, "
        f"period_start_time AS start_time, period_end_time AS end_time, "
        f"runtime_seconds, compute_ids "
        f"FROM {_JOBS} "
        f"WHERE period_start_time::date BETWEEN %s AND %s AND runtime_seconds > 3600{ws} "
        f"ORDER BY runtime_seconds DESC LIMIT 100",
        tuple(params),
    )


def de_cluster_efficiency(start_date: str, end_date: str, workspace_id=None):
    params = [end_date, start_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT cluster_id, cluster_name, cluster_source, "
        f"driver_node_type, worker_node_type, create_time, delete_time, "
        f"uptime_hours, status_category "
        f"FROM {_CLUSTERS} "
        f"WHERE create_time::date <= %s AND (delete_time IS NULL OR delete_time::date >= %s){ws} "
        f"ORDER BY uptime_hours DESC LIMIT 50",
        tuple(params),
    )


def de_pipeline_metrics(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT sku_name, usage_date, "
        f"SUM(total_dbu) AS total_dbu, SUM(total_cost) AS total_cost "
        f"FROM {_BILLING_SKU} "
        f"WHERE usage_date BETWEEN %s AND %s AND cluster_type = 'DLT Pipelines'{ws} "
        f"GROUP BY sku_name, usage_date ORDER BY usage_date",
        tuple(params),
    )


# ===============================================================================
# MACHINE LEARNING (ML) QUERIES
# ===============================================================================

def ml_summary_kpis(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT COALESCE(SUM(total_dbu), 0) AS total_dbu, "
        f"COALESCE(SUM(total_cost), 0) AS total_cost "
        f"FROM {_BILLING_SKU} "
        f"WHERE usage_date BETWEEN %s AND %s AND category = 'ML'{ws}",
        tuple(params),
    )


def ml_dbu_trend(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT usage_date, ml_workload_type, "
        f"SUM(total_dbu) AS total_dbu, SUM(total_cost) AS total_cost "
        f"FROM {_BILLING_SKU} "
        f"WHERE usage_date BETWEEN %s AND %s AND category = 'ML'{ws} "
        f"GROUP BY usage_date, ml_workload_type ORDER BY usage_date",
        tuple(params),
    )


def ml_cost_by_workload_type(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT ml_workload_type AS workload_type, "
        f"SUM(total_dbu) AS total_dbu, SUM(total_cost) AS total_cost "
        f"FROM {_BILLING_SKU} "
        f"WHERE usage_date BETWEEN %s AND %s AND category = 'ML'{ws} "
        f"GROUP BY ml_workload_type ORDER BY total_cost DESC",
        tuple(params),
    )


def ml_top_users(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT user_identity AS user, "
        f"SUM(total_dbu) AS total_dbu, SUM(total_cost) AS total_cost "
        f"FROM {_BILLING_USER} "
        f"WHERE usage_date BETWEEN %s AND %s AND category = 'ML'{ws} "
        f"GROUP BY user_identity ORDER BY total_cost DESC LIMIT 20",
        tuple(params),
    )


def ml_serving_endpoints(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT endpoint_name, model_type, first_seen, last_seen "
        f"FROM {_SERVING} "
        f"WHERE last_seen::date >= %s AND first_seen::date <= %s{ws} "
        f"ORDER BY last_seen DESC LIMIT 50",
        tuple(params),
    )


def ml_serving_endpoint_usage(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT usage_date, sku_name, "
        f"SUM(total_dbu) AS total_dbu, SUM(total_cost) AS total_cost "
        f"FROM {_BILLING_SKU} "
        f"WHERE usage_date BETWEEN %s AND %s "
        f"AND (sku_name LIKE '%%MODEL_SERVING%%' OR sku_name LIKE '%%INFERENCE%%' "
        f"OR sku_name LIKE '%%FOUNDATION_MODEL%%'){ws} "
        f"GROUP BY usage_date, sku_name ORDER BY usage_date",
        tuple(params),
    )


def ml_experiment_runs(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT experiment_id, run_id, status, start_time, end_time, "
        f"runtime_seconds, user_id "
        f"FROM {_EXPERIMENTS} "
        f"WHERE start_time::date BETWEEN %s AND %s{ws} "
        f"ORDER BY runtime_seconds DESC LIMIT 100",
        tuple(params),
    )


def ml_long_running_training(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT experiment_id, run_id, status, start_time, end_time, "
        f"runtime_seconds, user_id "
        f"FROM {_EXPERIMENTS} "
        f"WHERE start_time::date BETWEEN %s AND %s AND runtime_seconds > 3600{ws} "
        f"ORDER BY runtime_seconds DESC LIMIT 50",
        tuple(params),
    )


# ===============================================================================
# DATA WAREHOUSING (DW) QUERIES
# ===============================================================================

def dw_summary_kpis(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT COALESCE(SUM(total_dbu), 0) AS total_dbu, "
        f"COALESCE(SUM(total_cost), 0) AS total_cost "
        f"FROM {_BILLING_SKU} "
        f"WHERE usage_date BETWEEN %s AND %s AND category = 'DW'{ws}",
        tuple(params),
    )


def dw_query_kpis(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT "
        f"SUM(total_queries) AS total_queries, "
        f"SUM(failed_queries) AS failed_queries, "
        f"ROUND(SUM(failed_queries) * 100.0 / NULLIF(SUM(total_queries), 0), 2) AS failure_rate, "
        f"SUM(total_duration_ms) / NULLIF(SUM(total_queries), 0) / 1000.0 AS avg_runtime_seconds, "
        f"MAX(p95_duration_ms) / 1000.0 AS p95_runtime_seconds "
        f"FROM {_QUERY_DAILY} "
        f"WHERE query_date BETWEEN %s AND %s{ws}",
        tuple(params),
    )


def dw_dbu_by_warehouse_type(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT warehouse_type, "
        f"SUM(total_dbu) AS total_dbu, SUM(total_cost) AS total_cost "
        f"FROM {_BILLING_SKU} "
        f"WHERE usage_date BETWEEN %s AND %s AND category = 'DW'{ws} "
        f"GROUP BY warehouse_type ORDER BY total_cost DESC",
        tuple(params),
    )


def dw_dbu_trend(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT usage_date, warehouse_type, "
        f"SUM(total_dbu) AS total_dbu, SUM(total_cost) AS total_cost "
        f"FROM {_BILLING_SKU} "
        f"WHERE usage_date BETWEEN %s AND %s AND category = 'DW'{ws} "
        f"GROUP BY usage_date, warehouse_type ORDER BY usage_date",
        tuple(params),
    )


def dw_cost_by_warehouse(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT warehouse_id, "
        f"SUM(total_queries) AS total_queries, "
        f"SUM(total_duration_ms) / NULLIF(SUM(total_queries), 0) / 1000.0 AS avg_runtime_seconds, "
        f"SUM(total_duration_ms) / 1000.0 AS total_runtime_seconds "
        f"FROM {_QUERY_DAILY} "
        f"WHERE query_date BETWEEN %s AND %s AND warehouse_id IS NOT NULL{ws} "
        f"GROUP BY warehouse_id ORDER BY total_queries DESC LIMIT 30",
        tuple(params),
    )


def dw_top_users(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT executed_by AS user, "
        f"SUM(queries_run) AS queries_run, "
        f"SUM(total_duration_ms) / NULLIF(SUM(queries_run), 0) / 1000.0 AS avg_runtime_seconds, "
        f"SUM(total_duration_ms) / 1000.0 AS total_runtime_seconds, "
        f"SUM(total_read_rows) AS total_rows_read, "
        f"SUM(total_produced_rows) AS total_rows_produced "
        f"FROM {_QUERY_USER} "
        f"WHERE query_date BETWEEN %s AND %s{ws} "
        f"GROUP BY executed_by ORDER BY queries_run DESC LIMIT 20",
        tuple(params),
    )


def dw_long_running_queries(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT statement_id AS query_id, executed_by AS user, query_text, "
        f"runtime_seconds, warehouse_id, rows_scanned, read_bytes, status "
        f"FROM {_LONG_QUERIES} "
        f"WHERE start_time::date BETWEEN %s AND %s{ws} "
        f"ORDER BY runtime_seconds DESC LIMIT 100",
        tuple(params),
    )


def dw_query_runtime_distribution(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT duration_bucket, SUM(query_count) AS query_count, bucket_order "
        f"FROM {_QUERY_DIST} "
        f"WHERE query_date BETWEEN %s AND %s{ws} "
        f"GROUP BY duration_bucket, bucket_order ORDER BY bucket_order",
        tuple(params),
    )


def dw_query_performance_trend(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT query_date, "
        f"SUM(total_queries) AS total_queries, "
        f"SUM(total_duration_ms) / NULLIF(SUM(total_queries), 0) / 1000.0 AS avg_runtime_seconds, "
        f"MAX(p95_duration_ms) / 1000.0 AS p95_runtime_seconds, "
        f"SUM(slow_queries) AS slow_queries, "
        f"SUM(total_read_bytes) AS total_data_scanned "
        f"FROM {_QUERY_DAILY} "
        f"WHERE query_date BETWEEN %s AND %s{ws} "
        f"GROUP BY query_date ORDER BY query_date",
        tuple(params),
    )


def dw_warehouse_concurrency(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT warehouse_id, query_date, query_hour, "
        f"SUM(concurrent_queries) AS concurrent_queries "
        f"FROM {_CONCURRENCY} "
        f"WHERE query_date BETWEEN %s AND %s AND warehouse_id IS NOT NULL{ws} "
        f"GROUP BY warehouse_id, query_date, query_hour "
        f"ORDER BY query_date, query_hour",
        tuple(params),
    )


# ===============================================================================
# ANOMALY DETECTION QUERIES
# ===============================================================================

def anomaly_dbu_daily(start_date: str, end_date: str, workspace_id=None):
    params = [start_date, end_date]
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT usage_date, "
        f"SUM(total_dbu) AS total_dbu, SUM(total_cost) AS total_cost "
        f"FROM {_BILLING_SKU} "
        f"WHERE usage_date BETWEEN %s AND %s{ws} "
        f"GROUP BY usage_date ORDER BY usage_date",
        tuple(params),
    )


def anomaly_idle_clusters(workspace_id=None):
    """Re-derives uptime using NOW() for live accuracy."""
    params = []
    ws = _ws_clause(workspace_id, params)
    return (
        f"SELECT cluster_id, cluster_name, cluster_source, owned_by, create_time, "
        f"EXTRACT(EPOCH FROM (NOW() - create_time)) / 3600 AS uptime_hours, "
        f"driver_node_type, worker_node_type "
        f"FROM {_CLUSTERS} "
        f"WHERE delete_time IS NULL "
        f"AND EXTRACT(EPOCH FROM (NOW() - create_time)) / 3600 > 12{ws} "
        f"ORDER BY uptime_hours DESC LIMIT 50",
        tuple(params),
    )
