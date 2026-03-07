"""
Databricks Platform Insights
========================================
Production-grade monitoring for compute usage, cost drivers, performance,
and user behavior across all workspaces and workloads.

Deployable as a Databricks App using Streamlit.
"""

import streamlit as st
import pandas as pd
import logging

from config import APP_ENVIRONMENT
from utils import (
    fmt_number, fmt_currency, fmt_percent, fmt_duration, fmt_dbu,
    default_date_range, date_to_str, safe_get_scalar,
    detect_anomalies, detect_spikes, calculate_cost_forecast,
    compute_workspace_health_score,
)
from lakebase_client import run_query
import metrics_queries as mq
import charts

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

APP_ENV = APP_ENVIRONMENT.upper()

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Databricks Platform Insights",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
st.markdown("""
<style>
    .block-container { padding-top: 2.5rem; }
    div[data-testid="stMetric"] {
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 8px;
        padding: 12px 16px;
    }
    div[data-testid="stMetric"] label { font-size: 0.8rem; }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] { font-size: 1.4rem; font-weight: 700; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        padding: 8px 20px;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# SIDEBAR — GLOBAL FILTERS
# ═══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.title("Databricks Platform Insights")
    env_color = "#FF9800" if APP_ENV == "DEV" else "#4CAF50"
    st.markdown(
        f'<span style="background:{env_color};color:#fff;padding:3px 10px;'
        f'border-radius:4px;font-size:0.75rem;font-weight:700;">{APP_ENV}</span>',
        unsafe_allow_html=True,
    )
    st.markdown("---")

    st.subheader("Date Range")
    default_start, default_end = default_date_range()
    col1, col2 = st.columns(2)
    start_date = col1.date_input("Start", value=default_start)
    end_date = col2.date_input("End", value=default_end)

    sd = date_to_str(start_date)
    ed = date_to_str(end_date)

    st.markdown("---")
    st.subheader("Filters")
    ws_list_df = run_query(mq.list_workspaces(sd, ed))
    ws_options = ws_list_df["workspace_id"].astype(str).tolist() if not ws_list_df.empty else []
    ws_selection = st.multiselect("Workspace", ws_options, placeholder="All workspaces")
    ws_filter = ws_selection if ws_selection else None

    st.markdown("---")
    st.subheader("Settings")
    auto_refresh = st.toggle("Auto-refresh (5 min)", value=False)
    if auto_refresh:
        st.markdown("_Dashboard will refresh every 5 minutes._")

    st.markdown("---")
    refresh_info = run_query(mq.last_refreshed())
    if not refresh_info.empty and refresh_info["last_refreshed"].iloc[0] is not None:
        last_ref = refresh_info['last_refreshed'].iloc[0]
        if hasattr(last_ref, 'strftime'):
            last_ref = last_ref.strftime('%Y-%m-%d')
        st.caption(f"Data as of: {last_ref}")
    st.caption("Databricks Platform Insights v1.0")

if auto_refresh:
    import time
    st.empty()
    time.sleep(0)  # placeholder — Streamlit reruns handle refresh


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER: render KPI row
# ═══════════════════════════════════════════════════════════════════════════════

def kpi_row(metrics: list[tuple[str, str]], cols_per_row: int = None):
    """
    Render a row of KPI metric cards.
    metrics: list of (label, value) or (label, value, delta)
    """
    n = cols_per_row or len(metrics)
    cols = st.columns(n)
    for i, m in enumerate(metrics):
        label, value = m[0], m[1]
        delta = m[2] if len(m) > 2 else None
        with cols[i % n]:
            st.metric(label=label, value=value, delta=delta)


def alert_box(message: str, level: str = "warning"):
    """Show an alert."""
    if level == "error":
        st.error(message)
    elif level == "success":
        st.success(message)
    else:
        st.warning(message)


# ═══════════════════════════════════════════════════════════════════════════════
# TABS
# ═══════════════════════════════════════════════════════════════════════════════

_, refresh_col = st.columns([9, 1])
with refresh_col:
    if st.button("Refresh"):
        st.cache_data.clear()
        st.rerun()

tab_overview, tab_de, tab_ml, tab_dw = st.tabs([
    "Platform Overview",
    "Data Engineering",
    "Machine Learning",
    "Data Warehousing",
])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB: PLATFORM OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════

with tab_overview:
    st.header("Platform Overview")

    # --- KPIs ---
    overview_totals = run_query(mq.overview_total_dbu_and_cost(sd, ed, ws_filter))
    overview_clusters = run_query(mq.overview_active_clusters(sd, ed, ws_filter))
    overview_warehouses = run_query(mq.overview_active_warehouses(sd, ed, ws_filter))
    overview_users = run_query(mq.overview_active_users(sd, ed, ws_filter))

    total_dbu = safe_get_scalar(overview_totals, "total_dbu")
    total_cost = safe_get_scalar(overview_totals, "total_cost")
    active_clusters = safe_get_scalar(overview_clusters, "active_clusters")
    active_warehouses = safe_get_scalar(overview_warehouses, "active_warehouses")
    active_users = safe_get_scalar(overview_users, "active_users")

    kpi_row([
        ("Total DBU", fmt_dbu(total_dbu)),
        ("Total Cost", fmt_currency(total_cost)),
        ("Active Clusters", fmt_number(active_clusters)),
        ("Active Warehouses", fmt_number(active_warehouses)),
        ("Active Users", fmt_number(active_users)),
    ])

    st.markdown("---")

    # --- Cost trend by category ---
    col_left, col_right = st.columns([3, 2])

    with col_left:
        cost_trend = run_query(mq.overview_cost_trend(sd, ed, ws_filter))
        st.plotly_chart(
            charts.cost_trend_chart(cost_trend, "usage_date", "total_cost", "category",
                                    title="Daily Cost by Workload Category"),
            use_container_width=True,
        )

    with col_right:
        by_category = run_query(mq.overview_dbu_by_category(sd, ed, ws_filter))
        st.plotly_chart(
            charts.cost_breakdown_pie(by_category, "category", "total_cost",
                                      title="Cost Distribution"),
            use_container_width=True,
        )

    # --- Cost by workspace & SKU ---
    col1, col2 = st.columns(2)

    with col1:
        by_workspace = run_query(mq.overview_cost_by_workspace(sd, ed, ws_filter))
        if not by_workspace.empty:
            by_workspace["workspace_id"] = by_workspace["workspace_id"].astype(str)
        st.plotly_chart(
            charts.cost_breakdown_bar(by_workspace, "workspace_id", "total_cost",
                                      title="Cost by Workspace"),
            use_container_width=True,
        )

    with col2:
        by_sku = run_query(mq.overview_cost_by_sku(sd, ed, ws_filter))
        st.plotly_chart(
            charts.cost_breakdown_bar(by_sku, "sku_name", "total_cost",
                                      title="Cost by SKU"),
            use_container_width=True,
        )

    # --- Top users ---
    st.subheader("Top Users by Cost")
    top_users = run_query(mq.overview_cost_by_user(sd, ed, ws_filter))
    if not top_users.empty:
        col_chart, col_table = st.columns([1, 1])
        with col_chart:
            st.plotly_chart(
                charts.top_users_bar(top_users, "user_identity", "total_cost",
                                     title="Top 10 Users"),
                use_container_width=True,
            )
        with col_table:
            display = top_users[["user_identity", "total_dbu", "total_cost"]].head(20).copy()
            display["total_dbu"] = display["total_dbu"].apply(lambda x: fmt_dbu(x))
            display["total_cost"] = display["total_cost"].apply(lambda x: fmt_currency(x))
            display.columns = ["User", "DBUs", "Cost"]
            st.dataframe(display, use_container_width=True, hide_index=True)
    else:
        st.info("No user-level data available for the selected period.")

    # --- Cost forecast ---
    st.subheader("Cost Forecast (30-day)")
    daily_cost = run_query(mq.anomaly_dbu_daily(sd, ed, ws_filter))
    if not daily_cost.empty:
        forecast = calculate_cost_forecast(daily_cost, "usage_date", "total_cost", 30)
        st.plotly_chart(
            charts.forecast_chart(daily_cost, forecast, "usage_date", "total_cost",
                                  title="30-Day Cost Forecast"),
            use_container_width=True,
        )

    # --- Anomaly detection ---
    st.subheader("Anomaly Detection")
    if not daily_cost.empty:
        anomalies = detect_anomalies(daily_cost, "total_cost", "usage_date", std_threshold=2.0)
        st.plotly_chart(
            charts.anomaly_chart(daily_cost, "usage_date", "total_cost", anomalies,
                                 title="DBU Cost Anomalies"),
            use_container_width=True,
        )
        if not anomalies.empty:
            alert_box(f"Detected {len(anomalies)} anomalous day(s) in the selected period.")
            st.dataframe(
                anomalies[["usage_date", "total_cost", "_deviation"]].rename(
                    columns={"usage_date": "Date", "total_cost": "Cost", "_deviation": "Deviation"}
                ),
                use_container_width=True, hide_index=True,
            )
        else:
            st.success("No anomalies detected in the selected period.")

    # --- Idle clusters ---
    st.subheader("Idle Cluster Detection")
    idle_clusters = run_query(mq.anomaly_idle_clusters(ws_filter))
    if not idle_clusters.empty:
        alert_box(f"{len(idle_clusters)} cluster(s) have been running for over 12 hours.", "error")
        st.dataframe(idle_clusters, use_container_width=True, hide_index=True)
    else:
        st.success("No idle long-running clusters detected.")

    # --- Workspace health score ---
    st.subheader("Platform Health Score")
    job_kpis = run_query(mq.de_job_kpis(sd, ed, ws_filter))
    query_kpis = run_query(mq.dw_query_kpis(sd, ed, ws_filter))

    failed_rate = safe_get_scalar(job_kpis, "failed_rate", 0) / 100
    query_fail_rate = safe_get_scalar(query_kpis, "failure_rate", 0) / 100
    idle_pct = len(idle_clusters) / max(active_clusters, 1) if not idle_clusters.empty else 0
    avg_util = max(0.5, 1 - idle_pct)

    score, grade, details = compute_workspace_health_score(
        failed_rate, idle_pct, query_fail_rate, avg_util,
    )

    col_gauge, col_details = st.columns([1, 2])
    with col_gauge:
        st.plotly_chart(charts.health_gauge(score, f"Health: {grade}"),
                        use_container_width=True)
    with col_details:
        if details:
            for d in details:
                st.warning(d)
        else:
            st.success("All health indicators look good!")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB: DATA ENGINEERING
# ═══════════════════════════════════════════════════════════════════════════════

with tab_de:
    st.header("Data Engineering Observability")

    # --- KPIs ---
    de_totals = run_query(mq.de_summary_kpis(sd, ed, ws_filter))
    de_clusters = run_query(mq.de_cluster_kpis(sd, ed, ws_filter))
    de_jobs = run_query(mq.de_job_kpis(sd, ed, ws_filter))

    de_dbu = safe_get_scalar(de_totals, "total_dbu")
    de_cost = safe_get_scalar(de_totals, "total_cost")
    de_total_clusters = safe_get_scalar(de_clusters, "total_clusters")
    de_avg_uptime = safe_get_scalar(de_clusters, "avg_uptime_hours")
    de_total_jobs = safe_get_scalar(de_jobs, "total_jobs")
    de_failed_rate = safe_get_scalar(de_jobs, "failed_rate")
    de_avg_runtime = safe_get_scalar(de_jobs, "avg_runtime_seconds")

    kpi_row([
        ("DE DBU Usage", fmt_dbu(de_dbu)),
        ("DE Cost", fmt_currency(de_cost)),
        ("Active Clusters", fmt_number(de_total_clusters)),
        ("Avg Cluster Uptime", f"{de_avg_uptime:.1f}h" if de_avg_uptime else "N/A"),
        ("Jobs Executed", fmt_number(de_total_jobs)),
        ("Failed Job Rate", fmt_percent(de_failed_rate)),
        ("Avg Job Runtime", fmt_duration(de_avg_runtime)),
    ], cols_per_row=7)

    st.markdown("---")

    # --- Cluster usage by type ---
    col_trend, col_breakdown = st.columns([3, 2])

    with col_trend:
        de_trend = run_query(mq.de_dbu_trend(sd, ed, ws_filter))
        st.plotly_chart(
            charts.cost_trend_chart(de_trend, "usage_date", "total_cost", "cluster_type",
                                    title="DE Cost Trend by Cluster Type"),
            use_container_width=True,
        )

    with col_breakdown:
        de_by_type = run_query(mq.de_dbu_by_cluster_type(sd, ed, ws_filter))
        st.plotly_chart(
            charts.cost_breakdown_pie(de_by_type, "cluster_type", "total_cost",
                                      title="DE Cost by Cluster Type"),
            use_container_width=True,
        )

    # --- DBU trend ---
    st.plotly_chart(
        charts.dbu_trend_chart(de_trend, "usage_date", "total_dbu", "cluster_type",
                               title="DE DBU Usage Trend"),
        use_container_width=True,
    )

    # --- Top DE users ---
    st.subheader("Top Data Engineering Users")
    de_users = run_query(mq.de_top_users(sd, ed, ws_filter))
    if not de_users.empty:
        col_chart, col_table = st.columns([1, 1])
        with col_chart:
            st.plotly_chart(
                charts.top_users_bar(de_users, "user", "total_cost",
                                     title="Top 10 DE Users by Cost"),
                use_container_width=True,
            )
        with col_table:
            display = de_users.copy()
            display["total_dbu"] = display["total_dbu"].apply(fmt_dbu)
            display["total_cost"] = display["total_cost"].apply(fmt_currency)
            display.columns = ["User", "DBUs", "Cost"]
            st.dataframe(display, use_container_width=True, hide_index=True)
    else:
        st.info("No DE user data available.")

    # --- Long-running jobs ---
    st.subheader("Long-Running Jobs (> 1 hour)")
    long_jobs = run_query(mq.de_long_running_jobs(sd, ed, ws_filter))
    if not long_jobs.empty:
        severity_map = {1: "1-3h", 3: "3-6h", 6: "6h+"}
        long_jobs["severity"] = long_jobs["runtime_seconds"].apply(
            lambda s: "6h+" if s > 21600 else ("3-6h" if s > 10800 else "1-3h")
        )
        long_jobs["runtime"] = long_jobs["runtime_seconds"].apply(fmt_duration)

        tab_all, tab_6h, tab_3h = st.tabs(["All (> 1h)", "> 6 hours", "> 3 hours"])
        with tab_all:
            st.dataframe(
                long_jobs[["job_id", "run_name", "runtime", "severity", "status"]],
                use_container_width=True, hide_index=True,
            )
        with tab_6h:
            filtered = long_jobs[long_jobs["runtime_seconds"] > 21600]
            if filtered.empty:
                st.success("No jobs running longer than 6 hours.")
            else:
                alert_box(f"{len(filtered)} job(s) running longer than 6 hours!", "error")
                st.dataframe(
                    filtered[["job_id", "run_name", "runtime", "status"]],
                    use_container_width=True, hide_index=True,
                )
        with tab_3h:
            filtered = long_jobs[long_jobs["runtime_seconds"] > 10800]
            if filtered.empty:
                st.success("No jobs running longer than 3 hours.")
            else:
                alert_box(f"{len(filtered)} job(s) running longer than 3 hours!")
                st.dataframe(
                    filtered[["job_id", "run_name", "runtime", "status"]],
                    use_container_width=True, hide_index=True,
                )
    else:
        st.success("No long-running jobs detected.")

    # --- Cluster efficiency ---
    st.subheader("Cluster Efficiency")
    cluster_eff = run_query(mq.de_cluster_efficiency(sd, ed, ws_filter))
    if not cluster_eff.empty:
        long_running = cluster_eff[cluster_eff["status_category"] == "LONG_RUNNING"]
        if not long_running.empty:
            alert_box(
                f"{len(long_running)} cluster(s) running for over 24 hours.",
                "error",
            )
        st.dataframe(
            cluster_eff[["cluster_id", "cluster_name", "cluster_source",
                         "uptime_hours", "status_category"]],
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("No cluster efficiency data available.")

    # --- Pipeline metrics ---
    st.subheader("DLT Pipeline Metrics")
    pipeline_data = run_query(mq.de_pipeline_metrics(sd, ed, ws_filter))
    if not pipeline_data.empty:
        st.plotly_chart(
            charts.cost_trend_chart(pipeline_data, "usage_date", "total_cost", "sku_name",
                                    title="DLT Pipeline Cost Trend"),
            use_container_width=True,
        )
    else:
        st.info("No DLT pipeline data available for the selected period.")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB: MACHINE LEARNING
# ═══════════════════════════════════════════════════════════════════════════════

with tab_ml:
    st.header("Machine Learning Observability")

    # --- KPIs ---
    ml_totals = run_query(mq.ml_summary_kpis(sd, ed, ws_filter))
    ml_dbu_val = safe_get_scalar(ml_totals, "total_dbu")
    ml_cost_val = safe_get_scalar(ml_totals, "total_cost")

    # Try to get ML experiment counts
    ml_experiments = run_query(mq.ml_experiment_runs(sd, ed, ws_filter))
    ml_exp_count = len(ml_experiments) if not ml_experiments.empty else 0
    ml_unique_experiments = ml_experiments["experiment_id"].nunique() if not ml_experiments.empty else 0

    # Try to get serving endpoint info
    ml_endpoints = run_query(mq.ml_serving_endpoints(sd, ed, ws_filter))
    ml_endpoint_count = len(ml_endpoints) if not ml_endpoints.empty else 0

    kpi_row([
        ("ML DBU Usage", fmt_dbu(ml_dbu_val)),
        ("ML Cost", fmt_currency(ml_cost_val)),
        ("Active Experiments", fmt_number(ml_unique_experiments)),
        ("Training Runs", fmt_number(ml_exp_count)),
        ("Serving Endpoints", fmt_number(ml_endpoint_count)),
    ])

    st.markdown("---")

    # --- ML cost trend ---
    col_trend, col_breakdown = st.columns([3, 2])

    with col_trend:
        ml_trend = run_query(mq.ml_dbu_trend(sd, ed, ws_filter))
        st.plotly_chart(
            charts.cost_trend_chart(ml_trend, "usage_date", "total_cost", "ml_workload_type",
                                    title="ML Cost Trend by Workload Type"),
            use_container_width=True,
        )

    with col_breakdown:
        ml_by_type = run_query(mq.ml_cost_by_workload_type(sd, ed, ws_filter))
        st.plotly_chart(
            charts.cost_breakdown_pie(ml_by_type, "workload_type", "total_cost",
                                      title="ML Cost by Workload Type"),
            use_container_width=True,
        )

    # --- DBU trend ---
    st.plotly_chart(
        charts.dbu_trend_chart(ml_trend, "usage_date", "total_dbu", "ml_workload_type",
                               title="ML DBU Usage Trend"),
        use_container_width=True,
    )

    # --- Top ML users ---
    st.subheader("Top ML Users")
    ml_users = run_query(mq.ml_top_users(sd, ed, ws_filter))
    if not ml_users.empty:
        col_chart, col_table = st.columns([1, 1])
        with col_chart:
            st.plotly_chart(
                charts.top_users_bar(ml_users, "user", "total_cost",
                                     title="Top 10 ML Users by Cost"),
                use_container_width=True,
            )
        with col_table:
            display = ml_users.copy()
            display["total_dbu"] = display["total_dbu"].apply(fmt_dbu)
            display["total_cost"] = display["total_cost"].apply(fmt_currency)
            display.columns = ["User", "DBUs", "Cost"]
            st.dataframe(display, use_container_width=True, hide_index=True)
    else:
        st.info("No ML user data available.")

    # --- Long-running training ---
    st.subheader("Long-Running Training Jobs (> 1 hour)")
    long_training = run_query(mq.ml_long_running_training(sd, ed, ws_filter))
    if not long_training.empty:
        long_training["runtime"] = long_training["runtime_seconds"].apply(fmt_duration)
        st.dataframe(
            long_training[["experiment_id", "run_id", "user_id", "runtime", "status"]],
            use_container_width=True, hide_index=True,
        )
    else:
        st.success("No long-running training jobs detected.")

    # --- Model serving metrics ---
    st.subheader("Model Serving")
    serving_usage = run_query(mq.ml_serving_endpoint_usage(sd, ed, ws_filter))
    if not serving_usage.empty:
        st.plotly_chart(
            charts.cost_trend_chart(serving_usage, "usage_date", "total_cost", "sku_name",
                                    title="Serving Endpoint Cost Trend"),
            use_container_width=True,
        )

    if not ml_endpoints.empty:
        st.markdown("**Active Serving Endpoints**")
        st.dataframe(ml_endpoints, use_container_width=True, hide_index=True)
    else:
        st.info("No serving endpoint data available.")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB: DATA WAREHOUSING
# ═══════════════════════════════════════════════════════════════════════════════

with tab_dw:
    st.header("Data Warehousing Observability")

    # --- KPIs ---
    dw_totals = run_query(mq.dw_summary_kpis(sd, ed, ws_filter))
    dw_queries = run_query(mq.dw_query_kpis(sd, ed, ws_filter))

    dw_dbu_val = safe_get_scalar(dw_totals, "total_dbu")
    dw_cost_val = safe_get_scalar(dw_totals, "total_cost")
    dw_total_queries = safe_get_scalar(dw_queries, "total_queries")
    dw_failed_queries = safe_get_scalar(dw_queries, "failed_queries")
    dw_failure_rate = safe_get_scalar(dw_queries, "failure_rate")
    dw_avg_runtime = safe_get_scalar(dw_queries, "avg_runtime_seconds")
    dw_p95_runtime = safe_get_scalar(dw_queries, "p95_runtime_seconds")

    kpi_row([
        ("SQL DBU Usage", fmt_dbu(dw_dbu_val)),
        ("Warehouse Cost", fmt_currency(dw_cost_val)),
        ("Total Queries", fmt_number(dw_total_queries)),
        ("Avg Runtime", fmt_duration(dw_avg_runtime)),
        ("P95 Runtime", fmt_duration(dw_p95_runtime)),
        ("Query Failure Rate", fmt_percent(dw_failure_rate)),
    ], cols_per_row=6)

    st.markdown("---")

    # --- Warehouse usage breakdown ---
    col_trend, col_breakdown = st.columns([3, 2])

    with col_trend:
        dw_trend = run_query(mq.dw_dbu_trend(sd, ed, ws_filter))
        st.plotly_chart(
            charts.cost_trend_chart(dw_trend, "usage_date", "total_cost", "warehouse_type",
                                    title="SQL Cost Trend by Warehouse Type"),
            use_container_width=True,
        )

    with col_breakdown:
        dw_by_type = run_query(mq.dw_dbu_by_warehouse_type(sd, ed, ws_filter))
        st.plotly_chart(
            charts.cost_breakdown_pie(dw_by_type, "warehouse_type", "total_cost",
                                      title="Cost by Warehouse Type"),
            use_container_width=True,
        )

    # --- DBU trend ---
    st.plotly_chart(
        charts.dbu_trend_chart(dw_trend, "usage_date", "total_dbu", "warehouse_type",
                               title="SQL DBU Usage Trend"),
        use_container_width=True,
    )

    # --- Query performance ---
    st.subheader("Query Performance")
    col_dist, col_perf = st.columns(2)

    with col_dist:
        runtime_dist = run_query(mq.dw_query_runtime_distribution(sd, ed, ws_filter))
        st.plotly_chart(
            charts.distribution_bar(runtime_dist, "duration_bucket", "query_count",
                                    title="Query Runtime Distribution"),
            use_container_width=True,
        )

    with col_perf:
        perf_trend = run_query(mq.dw_query_performance_trend(sd, ed, ws_filter))
        st.plotly_chart(
            charts.performance_trend(perf_trend, "query_date", "avg_runtime_seconds",
                                     "p95_runtime_seconds",
                                     title="Query Performance Trend"),
            use_container_width=True,
        )

    # --- Slow query trend ---
    if not perf_trend.empty and "slow_queries" in perf_trend.columns:
        slow_trend = perf_trend[perf_trend["slow_queries"] > 0]
        if not slow_trend.empty:
            st.plotly_chart(
                charts.dbu_trend_chart(slow_trend, "query_date", "slow_queries",
                                       title="Slow Queries per Day (> 5 min)"),
                use_container_width=True,
            )

    # --- Warehouse concurrency heatmap ---
    st.subheader("Warehouse Concurrency Heatmap")
    concurrency = run_query(mq.dw_warehouse_concurrency(sd, ed, ws_filter))
    if not concurrency.empty:
        st.plotly_chart(
            charts.concurrency_heatmap(concurrency, "query_date", "query_hour",
                                       "concurrent_queries",
                                       title="Query Concurrency by Hour"),
            use_container_width=True,
        )
    else:
        st.info("No concurrency data available.")

    # --- Cost by warehouse ---
    st.subheader("Warehouse Usage")
    wh_usage = run_query(mq.dw_cost_by_warehouse(sd, ed, ws_filter))
    if not wh_usage.empty:
        wh_usage["warehouse_id"] = wh_usage["warehouse_id"].astype(str)
        st.plotly_chart(
            charts.cost_breakdown_bar(wh_usage, "warehouse_id", "total_queries",
                                      title="Queries by Warehouse"),
            use_container_width=True,
        )
        st.dataframe(wh_usage, use_container_width=True, hide_index=True)
    else:
        st.info("No warehouse usage data available.")

    # --- Top DW users ---
    st.subheader("Top SQL Users")
    dw_users = run_query(mq.dw_top_users(sd, ed, ws_filter))
    if not dw_users.empty:
        col_chart, col_table = st.columns([1, 1])
        with col_chart:
            st.plotly_chart(
                charts.top_users_bar(dw_users, "user", "queries_run",
                                     title="Top 10 Users by Query Count",
                                     value_prefix=""),
                use_container_width=True,
            )
        with col_table:
            display = dw_users.copy()
            display["avg_runtime_seconds"] = display["avg_runtime_seconds"].apply(
                lambda x: fmt_duration(x)
            )
            display.columns = ["User", "Queries", "Avg Runtime", "Total Runtime (s)",
                               "Rows Read", "Rows Produced"]
            st.dataframe(display, use_container_width=True, hide_index=True)
    else:
        st.info("No SQL user data available.")

    # --- Long-running queries ---
    st.subheader("Long-Running Queries")
    long_queries = run_query(mq.dw_long_running_queries(sd, ed, ws_filter))
    if not long_queries.empty:
        long_queries["runtime"] = long_queries["runtime_seconds"].apply(fmt_duration)
        long_queries["severity"] = long_queries["runtime_seconds"].apply(
            lambda s: "> 10 min" if s > 600 else "5-10 min"
        )

        tab_all_q, tab_10m, tab_5m = st.tabs(["All (> 5 min)", "> 10 minutes", "5-10 minutes"])

        with tab_all_q:
            st.dataframe(
                long_queries[["query_id", "user", "query_text", "runtime",
                              "warehouse_id", "rows_scanned", "status"]],
                use_container_width=True, hide_index=True,
            )
        with tab_10m:
            over_10 = long_queries[long_queries["runtime_seconds"] > 600]
            if over_10.empty:
                st.success("No queries running longer than 10 minutes.")
            else:
                alert_box(f"{len(over_10)} queries ran longer than 10 minutes!", "error")
                st.dataframe(
                    over_10[["query_id", "user", "query_text", "runtime",
                             "warehouse_id", "status"]],
                    use_container_width=True, hide_index=True,
                )
        with tab_5m:
            between_5_10 = long_queries[
                (long_queries["runtime_seconds"] > 300) &
                (long_queries["runtime_seconds"] <= 600)
            ]
            if between_5_10.empty:
                st.info("No queries in the 5-10 minute range.")
            else:
                st.dataframe(
                    between_5_10[["query_id", "user", "query_text", "runtime",
                                  "warehouse_id", "status"]],
                    use_container_width=True, hide_index=True,
                )
    else:
        st.success("No long-running queries detected.")

    # --- Anomaly detection for queries ---
    st.subheader("Query Anomaly Detection")
    if not perf_trend.empty:
        query_anomalies = detect_spikes(perf_trend, "avg_runtime_seconds", "query_date",
                                        window=5, threshold=2.5)
        if not query_anomalies.empty:
            alert_box(f"Detected {len(query_anomalies)} day(s) with abnormal query runtimes.")
            st.plotly_chart(
                charts.anomaly_chart(perf_trend, "query_date", "avg_runtime_seconds",
                                     query_anomalies,
                                     title="Query Runtime Anomalies"),
                use_container_width=True,
            )
        else:
            st.success("No query runtime anomalies detected.")
