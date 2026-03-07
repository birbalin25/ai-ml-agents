"""
Utility module for Databricks Platform Insights.
Formatters, anomaly detection, and helper functions.
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from config import QUERY_CACHE_TTL

CACHE_TTL = QUERY_CACHE_TTL


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------
def fmt_number(value, decimals=0) -> str:
    """Format a number with commas."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "N/A"
    if decimals == 0:
        return f"{int(value):,}"
    return f"{value:,.{decimals}f}"


def fmt_currency(value, decimals=2) -> str:
    """Format as USD currency."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "N/A"
    return f"${value:,.{decimals}f}"


def fmt_percent(value, decimals=1) -> str:
    """Format as percentage."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "N/A"
    return f"{value:.{decimals}f}%"


def fmt_duration(seconds) -> str:
    """Format seconds into human-readable duration."""
    if seconds is None or (isinstance(seconds, float) and np.isnan(seconds)):
        return "N/A"
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours}h {minutes}m"


def fmt_dbu(value) -> str:
    """Format DBU value."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "N/A"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value / 1_000:.1f}K"
    return f"{value:.1f}"


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------
def default_date_range() -> tuple:
    """Return default 30-day date range."""
    end = datetime.now().date()
    start = end - timedelta(days=30)
    return start, end


def date_to_str(d) -> str:
    """Convert date to ISO string."""
    if isinstance(d, str):
        return d
    return d.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Anomaly detection
# ---------------------------------------------------------------------------
def detect_anomalies(df: pd.DataFrame, value_col: str, time_col: str = None,
                     std_threshold: float = 2.0) -> pd.DataFrame:
    """
    Detect anomalies using z-score method.
    Returns rows where value exceeds mean +/- std_threshold * std.
    """
    if df.empty or value_col not in df.columns:
        return pd.DataFrame()

    mean = df[value_col].mean()
    std = df[value_col].std()

    if std == 0 or np.isnan(std):
        return pd.DataFrame()

    df = df.copy()
    df["_z_score"] = (df[value_col] - mean) / std
    df["_is_anomaly"] = df["_z_score"].abs() > std_threshold
    anomalies = df[df["_is_anomaly"]].copy()
    anomalies["_deviation"] = anomalies["_z_score"].apply(
        lambda z: f"{abs(z):.1f}x std {'above' if z > 0 else 'below'} mean"
    )
    return anomalies


def detect_spikes(df: pd.DataFrame, value_col: str, time_col: str,
                  window: int = 7, threshold: float = 3.0) -> pd.DataFrame:
    """
    Detect spikes using rolling window comparison.
    Flags points where value exceeds rolling_mean + threshold * rolling_std.
    """
    if df.empty or len(df) < window:
        return pd.DataFrame()

    df = df.sort_values(time_col).copy()
    df["_rolling_mean"] = df[value_col].rolling(window, min_periods=1).mean()
    df["_rolling_std"] = df[value_col].rolling(window, min_periods=1).std()
    df["_upper_bound"] = df["_rolling_mean"] + threshold * df["_rolling_std"]
    df["_is_spike"] = df[value_col] > df["_upper_bound"]
    return df[df["_is_spike"]].copy()


# ---------------------------------------------------------------------------
# Cost forecast
# ---------------------------------------------------------------------------
def calculate_cost_forecast(df: pd.DataFrame, date_col: str, cost_col: str,
                            forecast_days: int = 30) -> pd.DataFrame:
    """
    Simple linear cost forecast.
    Returns DataFrame with forecasted dates and costs.
    """
    if df.empty or len(df) < 3:
        return pd.DataFrame()

    df = df.sort_values(date_col).copy()
    df["_day_num"] = (pd.to_datetime(df[date_col]) - pd.to_datetime(df[date_col].min())).dt.days

    x = df["_day_num"].values
    y = df[cost_col].values

    valid = ~np.isnan(y)
    if valid.sum() < 3:
        return pd.DataFrame()

    coeffs = np.polyfit(x[valid], y[valid], 1)

    last_date = pd.to_datetime(df[date_col].max())
    last_day = int(x.max())
    forecast_dates = [last_date + timedelta(days=i + 1) for i in range(forecast_days)]
    forecast_days_num = [last_day + i + 1 for i in range(forecast_days)]
    forecast_values = [max(0, np.polyval(coeffs, d)) for d in forecast_days_num]

    return pd.DataFrame({
        date_col: forecast_dates,
        cost_col: forecast_values,
        "type": "forecast",
    })


# ---------------------------------------------------------------------------
# Health scoring
# ---------------------------------------------------------------------------
def compute_workspace_health_score(
    failed_job_rate: float,
    idle_cluster_pct: float,
    query_failure_rate: float,
    avg_cluster_utilization: float,
) -> tuple:
    """
    Compute a 0-100 health score for a workspace.
    Returns (score, grade, details).
    """
    penalties = 0.0
    details = []

    if failed_job_rate > 0.10:
        p = min(25, (failed_job_rate - 0.10) * 100)
        penalties += p
        details.append(f"High job failure rate: {fmt_percent(failed_job_rate * 100)}")
    if idle_cluster_pct > 0.20:
        p = min(25, (idle_cluster_pct - 0.20) * 80)
        penalties += p
        details.append(f"Excessive idle clusters: {fmt_percent(idle_cluster_pct * 100)}")
    if query_failure_rate > 0.05:
        p = min(25, (query_failure_rate - 0.05) * 100)
        penalties += p
        details.append(f"High query failure rate: {fmt_percent(query_failure_rate * 100)}")
    if avg_cluster_utilization < 0.50:
        p = min(25, (0.50 - avg_cluster_utilization) * 60)
        penalties += p
        details.append(f"Low cluster utilization: {fmt_percent(avg_cluster_utilization * 100)}")

    score = max(0, 100 - penalties)

    if score >= 90:
        grade = "A"
    elif score >= 75:
        grade = "B"
    elif score >= 60:
        grade = "C"
    elif score >= 40:
        grade = "D"
    else:
        grade = "F"

    return round(score, 1), grade, details


# ---------------------------------------------------------------------------
# DataFrame helpers
# ---------------------------------------------------------------------------
def safe_get_scalar(df: pd.DataFrame, col: str, default=0):
    """Safely get a single scalar value from a DataFrame."""
    if df is None or df.empty or col not in df.columns:
        return default
    val = df[col].iloc[0]
    if pd.isna(val):
        return default
    return val
