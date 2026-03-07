"""
Plotly visualization module for Databricks Admin Observability Platform.
All chart functions return Plotly Figure objects for Streamlit rendering.
"""

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ---------------------------------------------------------------------------
# Color palette
# ---------------------------------------------------------------------------
COLORS = {
    "primary": "#FF3621",
    "secondary": "#1B3139",
    "accent": "#00A972",
    "warning": "#FF9800",
    "danger": "#F44336",
    "info": "#2196F3",
    "light_bg": "#FAFAFA",
}

CATEGORY_COLORS = {
    "DE": "#1F77B4",
    "ML": "#FF7F0E",
    "DW": "#2CA02C",
    "Other": "#9467BD",
}

PALETTE = px.colors.qualitative.Set2

CHART_LAYOUT = dict(
    template="plotly_white",
    font=dict(family="Inter, sans-serif", size=12),
    margin=dict(l=40, r=40, t=70, b=40),
    height=400,
    legend=dict(orientation="h", yanchor="bottom", y=1.06, xanchor="right", x=1),
)


def _apply_layout(fig, title: str = None, height: int = None):
    """Apply standard layout to a figure."""
    layout = {**CHART_LAYOUT}
    if title:
        layout["title"] = dict(text=title, x=0.0, font=dict(size=16))
    if height:
        layout["height"] = height
    fig.update_layout(**layout)
    return fig


# ═══════════════════════════════════════════════════════════════════════════════
# TIME-SERIES CHARTS
# ═══════════════════════════════════════════════════════════════════════════════

def cost_trend_chart(df: pd.DataFrame, date_col: str, cost_col: str,
                     category_col: str = None, title: str = "Cost Trend") -> go.Figure:
    """Stacked area chart of cost over time, optionally split by category."""
    if df.empty:
        return _empty_chart(title)

    if category_col and category_col in df.columns:
        fig = px.area(
            df, x=date_col, y=cost_col, color=category_col,
            color_discrete_sequence=PALETTE,
        )
    else:
        fig = px.area(df, x=date_col, y=cost_col, color_discrete_sequence=[COLORS["primary"]])

    fig.update_traces(hovertemplate="%{x}<br>$%{y:,.2f}<extra></extra>")
    fig.update_yaxes(title_text="Cost (USD)", tickprefix="$")
    fig.update_xaxes(title_text="")
    return _apply_layout(fig, title)


def dbu_trend_chart(df: pd.DataFrame, date_col: str, dbu_col: str,
                    category_col: str = None, title: str = "DBU Usage Trend") -> go.Figure:
    """Stacked area chart of DBU usage over time."""
    if df.empty:
        return _empty_chart(title)

    if category_col and category_col in df.columns:
        fig = px.area(
            df, x=date_col, y=dbu_col, color=category_col,
            color_discrete_sequence=PALETTE,
        )
    else:
        fig = px.area(df, x=date_col, y=dbu_col, color_discrete_sequence=[COLORS["info"]])

    fig.update_traces(hovertemplate="%{x}<br>%{y:,.0f} DBUs<extra></extra>")
    fig.update_yaxes(title_text="DBUs")
    fig.update_xaxes(title_text="")
    return _apply_layout(fig, title)


def dual_axis_trend(df: pd.DataFrame, date_col: str, dbu_col: str, cost_col: str,
                    title: str = "DBU & Cost Trend") -> go.Figure:
    """Dual-axis chart with DBU bars and cost line."""
    if df.empty:
        return _empty_chart(title)

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Bar(x=df[date_col], y=df[dbu_col], name="DBUs",
               marker_color=COLORS["info"], opacity=0.7),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=df[date_col], y=df[cost_col], name="Cost",
                   line=dict(color=COLORS["primary"], width=2), mode="lines"),
        secondary_y=True,
    )

    fig.update_yaxes(title_text="DBUs", secondary_y=False)
    fig.update_yaxes(title_text="Cost (USD)", tickprefix="$", secondary_y=True)
    fig.update_xaxes(title_text="")
    return _apply_layout(fig, title)


# ═══════════════════════════════════════════════════════════════════════════════
# BREAKDOWN & COMPARISON CHARTS
# ═══════════════════════════════════════════════════════════════════════════════

def cost_breakdown_pie(df: pd.DataFrame, names_col: str, values_col: str,
                       title: str = "Cost Breakdown") -> go.Figure:
    """Donut chart for cost breakdown."""
    if df.empty:
        return _empty_chart(title)

    fig = px.pie(
        df, names=names_col, values=values_col,
        color_discrete_sequence=PALETTE, hole=0.45,
    )
    fig.update_traces(
        textposition="inside", textinfo="percent+label",
        hovertemplate="%{label}<br>$%{value:,.2f}<br>%{percent}<extra></extra>",
    )
    fig = _apply_layout(fig, title, height=420)
    fig.update_layout(
        legend=dict(orientation="h", yanchor="top", y=-0.05, xanchor="center", x=0.5),
        margin=dict(t=70, b=60),
    )
    return fig


def cost_breakdown_bar(df: pd.DataFrame, names_col: str, values_col: str,
                       title: str = "Cost Breakdown", horizontal: bool = True) -> go.Figure:
    """Horizontal bar chart for cost comparison."""
    if df.empty:
        return _empty_chart(title)

    df_sorted = df.sort_values(values_col, ascending=True)
    if horizontal:
        fig = px.bar(
            df_sorted, y=names_col, x=values_col, orientation="h",
            color_discrete_sequence=[COLORS["primary"]],
        )
        fig.update_xaxes(title_text="Cost (USD)", tickprefix="$")
        fig.update_yaxes(title_text="")
    else:
        fig = px.bar(
            df_sorted, x=names_col, y=values_col,
            color_discrete_sequence=[COLORS["primary"]],
        )
        fig.update_yaxes(title_text="Cost (USD)", tickprefix="$")
        fig.update_xaxes(title_text="")

    fig.update_traces(hovertemplate="%{y}<br>$%{x:,.2f}<extra></extra>" if horizontal
                      else "%{x}<br>$%{y:,.2f}<extra></extra>")
    return _apply_layout(fig, title)


def top_users_bar(df: pd.DataFrame, user_col: str, value_col: str,
                  title: str = "Top Users by Cost", value_prefix: str = "$",
                  n: int = 10) -> go.Figure:
    """Horizontal bar chart ranking top users."""
    if df.empty:
        return _empty_chart(title)

    top = df.nlargest(n, value_col).sort_values(value_col, ascending=True)

    fig = px.bar(
        top, y=user_col, x=value_col, orientation="h",
        color_discrete_sequence=[COLORS["info"]],
    )
    if value_prefix == "$":
        fig.update_xaxes(tickprefix="$", title_text="Cost (USD)")
    else:
        fig.update_xaxes(title_text=value_col.replace("_", " ").title())
    fig.update_yaxes(title_text="")
    return _apply_layout(fig, title, height=max(300, n * 35))


# ═══════════════════════════════════════════════════════════════════════════════
# DISTRIBUTION & PERFORMANCE CHARTS
# ═══════════════════════════════════════════════════════════════════════════════

def distribution_bar(df: pd.DataFrame, bucket_col: str, count_col: str,
                     title: str = "Distribution") -> go.Figure:
    """Bar chart showing distribution buckets."""
    if df.empty:
        return _empty_chart(title)

    fig = px.bar(
        df, x=bucket_col, y=count_col,
        color_discrete_sequence=[COLORS["accent"]],
    )
    fig.update_traces(hovertemplate="%{x}<br>%{y:,} queries<extra></extra>")
    fig.update_yaxes(title_text="Count")
    fig.update_xaxes(title_text="")
    return _apply_layout(fig, title)


def performance_trend(df: pd.DataFrame, date_col: str, avg_col: str,
                      p95_col: str = None,
                      title: str = "Performance Trend") -> go.Figure:
    """Line chart showing performance metrics over time."""
    if df.empty:
        return _empty_chart(title)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df[date_col], y=df[avg_col], name="Average",
        line=dict(color=COLORS["info"], width=2), mode="lines",
    ))
    if p95_col and p95_col in df.columns:
        fig.add_trace(go.Scatter(
            x=df[date_col], y=df[p95_col], name="P95",
            line=dict(color=COLORS["warning"], width=2, dash="dash"), mode="lines",
        ))

    fig.update_yaxes(title_text="Runtime (seconds)")
    fig.update_xaxes(title_text="")
    return _apply_layout(fig, title)


# ═══════════════════════════════════════════════════════════════════════════════
# HEATMAP
# ═══════════════════════════════════════════════════════════════════════════════

def concurrency_heatmap(df: pd.DataFrame, date_col: str, hour_col: str,
                        value_col: str,
                        title: str = "Query Concurrency Heatmap") -> go.Figure:
    """Heatmap showing activity by day and hour."""
    if df.empty:
        return _empty_chart(title)

    pivot = df.pivot_table(index=hour_col, columns=date_col, values=value_col,
                           aggfunc="sum", fill_value=0)

    fig = px.imshow(
        pivot.values,
        labels=dict(x="Date", y="Hour of Day", color=value_col.replace("_", " ").title()),
        x=[str(c) for c in pivot.columns],
        y=[f"{h:02d}:00" for h in pivot.index],
        color_continuous_scale="Blues",
        aspect="auto",
    )
    return _apply_layout(fig, title, height=450)


# ═══════════════════════════════════════════════════════════════════════════════
# FORECAST CHART
# ═══════════════════════════════════════════════════════════════════════════════

def forecast_chart(actual_df: pd.DataFrame, forecast_df: pd.DataFrame,
                   date_col: str, value_col: str,
                   title: str = "Cost Forecast") -> go.Figure:
    """Line chart with actual data and forecast."""
    fig = go.Figure()

    if not actual_df.empty:
        fig.add_trace(go.Scatter(
            x=actual_df[date_col], y=actual_df[value_col], name="Actual",
            line=dict(color=COLORS["info"], width=2), mode="lines",
        ))

    if not forecast_df.empty:
        fig.add_trace(go.Scatter(
            x=forecast_df[date_col], y=forecast_df[value_col], name="Forecast",
            line=dict(color=COLORS["warning"], width=2, dash="dash"), mode="lines",
            fill="tonexty", fillcolor="rgba(255,152,0,0.1)",
        ))

    fig.update_yaxes(title_text="Cost (USD)", tickprefix="$")
    fig.update_xaxes(title_text="")
    return _apply_layout(fig, title)


# ═══════════════════════════════════════════════════════════════════════════════
# HEALTH SCORE GAUGE
# ═══════════════════════════════════════════════════════════════════════════════

def health_gauge(score: float, title: str = "Platform Health") -> go.Figure:
    """Gauge chart showing a health score 0–100."""
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        number={"suffix": "/100"},
        gauge=dict(
            axis=dict(range=[0, 100]),
            bar=dict(color=COLORS["secondary"]),
            steps=[
                dict(range=[0, 40], color="#FFCDD2"),
                dict(range=[40, 60], color="#FFE0B2"),
                dict(range=[60, 75], color="#FFF9C4"),
                dict(range=[75, 90], color="#C8E6C9"),
                dict(range=[90, 100], color="#A5D6A7"),
            ],
            threshold=dict(line=dict(color=COLORS["danger"], width=2), thickness=0.8, value=score),
        ),
    ))
    return _apply_layout(fig, title, height=280)


# ═══════════════════════════════════════════════════════════════════════════════
# ANOMALY CHART
# ═══════════════════════════════════════════════════════════════════════════════

def anomaly_chart(df: pd.DataFrame, date_col: str, value_col: str,
                  anomaly_df: pd.DataFrame = None,
                  title: str = "Anomaly Detection") -> go.Figure:
    """Time series with anomaly points highlighted."""
    if df.empty:
        return _empty_chart(title)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df[date_col], y=df[value_col], name="Value",
        line=dict(color=COLORS["info"], width=2), mode="lines",
    ))

    if anomaly_df is not None and not anomaly_df.empty:
        fig.add_trace(go.Scatter(
            x=anomaly_df[date_col], y=anomaly_df[value_col], name="Anomaly",
            mode="markers",
            marker=dict(color=COLORS["danger"], size=10, symbol="circle-open", line=dict(width=2)),
        ))

    fig.update_yaxes(title_text=value_col.replace("_", " ").title())
    fig.update_xaxes(title_text="")
    return _apply_layout(fig, title)


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER
# ═══════════════════════════════════════════════════════════════════════════════

def _empty_chart(title: str = "No Data") -> go.Figure:
    """Return a placeholder chart when no data is available."""
    fig = go.Figure()
    fig.add_annotation(
        text="No data available for the selected period",
        xref="paper", yref="paper", x=0.5, y=0.5,
        showarrow=False, font=dict(size=14, color="#999"),
    )
    return _apply_layout(fig, title, height=300)
