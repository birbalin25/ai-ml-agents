"""Zillow Re-imagined with AI — Databricks App (Streamlit)."""

import sys
import os
import pathlib
import traceback
import streamlit as st

st.set_page_config(
    page_title="Zillow AI — Powered by Databricks",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Load custom CSS — resolve path relative to this file
_app_dir = pathlib.Path(__file__).parent
_css_path = _app_dir / "style.css"
if _css_path.exists():
    st.markdown(f"<style>{_css_path.read_text()}</style>", unsafe_allow_html=True)

# Ensure app directory is on sys.path for imports
if str(_app_dir) not in sys.path:
    sys.path.insert(0, str(_app_dir))

import plotly.express as px
import pandas as pd

try:
    from components.search_bar import render_search_bar
    from components.property_card import render_property_grid
    from components.sidebar_filters import render_sidebar_filters
    from components.chat import render_chat
    from components.map_view import render_map
except Exception as e:
    st.error(f"Failed to load components: {e}")
    st.code(traceback.format_exc())
    st.stop()

# ── Sidebar ──────────────────────────────────────────────────────────────────
st.sidebar.markdown("## 🏠 Zillow AI")
st.sidebar.markdown("**Re-imagined with AI**")
st.sidebar.markdown("---")
filters = render_sidebar_filters()
st.sidebar.markdown("---")
st.sidebar.caption("Powered by Databricks | Unity Catalog | Vector Search | MLflow | Foundation Models")

# ── Session State ────────────────────────────────────────────────────────────
if "search_results" not in st.session_state:
    st.session_state.search_results = []
if "last_query" not in st.session_state:
    st.session_state.last_query = ""
if "zestimates" not in st.session_state:
    st.session_state.zestimates = []

# ── Tabs ─────────────────────────────────────────────────────────────────────
tab_search, tab_chat, tab_insights = st.tabs(["Search", "AI Assistant", "Market Insights"])

# ── Tab 1: Search ────────────────────────────────────────────────────────────
with tab_search:
    query = render_search_bar()

    if query:
        st.session_state.last_query = query
        try:
            from utils.rag import search_properties
            from utils.price_predictor import predict_prices_batch

            with st.spinner("Searching with AI..."):
                results = search_properties(query, num_results=12, filters=filters if filters else None)
                st.session_state.search_results = results

                if results:
                    zestimates = predict_prices_batch(results)
                    st.session_state.zestimates = zestimates
        except Exception as e:
            st.error(f"Search failed: {e}")

    results = st.session_state.search_results
    zestimates = st.session_state.zestimates

    if results:
        st.markdown(f"### {len(results)} results for *\"{st.session_state.last_query}\"*")

        with st.expander("Map View", expanded=True):
            render_map(results)

        render_property_grid(results, zestimates)
    elif st.session_state.last_query:
        # User searched but no results matched
        st.markdown(
            f"""
            <div style="text-align: center; padding: 2.5rem 1.5rem; background: white;
                        border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); margin: 1rem 0 2rem 0;">
                <div style="font-size: 2.5rem; margin-bottom: 0.5rem;">🏠</div>
                <h3 style="color: #2A2A33; margin: 0 0 0.5rem 0;">No homes matched your search</h3>
                <p style="color: #586069; font-size: 1rem; margin: 0 0 0.3rem 0;">
                    We couldn't find properties matching <strong>"{st.session_state.last_query}"</strong>
                </p>
                <p style="color: #586069; font-size: 0.9rem; margin: 0;">
                    Try adjusting your filters, broadening your price range, or searching a different area.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("#### Popular searches to explore")
        examples = [
            ("Modern condos in Seattle with rooftop views", "Condos in Seattle"),
            ("Family homes near top schools in Austin under $600k", "Austin family homes"),
            ("Walkable neighborhoods in Chicago with 3+ bedrooms", "Chicago 3+ bed"),
            ("Luxury homes in San Francisco with smart home features", "SF luxury homes"),
            ("Single family homes in Denver under $500k", "Denver under $500k"),
            ("Pet friendly apartments in Portland with parking", "Portland pet friendly"),
        ]
        cols = st.columns(3)
        for i, (full_query, label) in enumerate(examples):
            with cols[i % 3]:
                st.markdown(
                    f"""
                    <div style="background: white; border: 1px solid #E0E0E0; border-radius: 10px;
                                padding: 1rem; margin-bottom: 0.75rem; cursor: pointer;
                                transition: box-shadow 0.2s;">
                        <p style="color: #006AFF; font-weight: 600; font-size: 0.9rem; margin: 0 0 4px 0;">
                            {label}
                        </p>
                        <p style="color: #586069; font-size: 0.8rem; margin: 0;">
                            "{full_query}"
                        </p>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
    else:
        # Initial state — no search yet
        st.markdown("#### Discover your next home with AI-powered search")
        st.caption("Type a natural language query — include price, location, features, and more.")
        examples = [
            ("Modern condos in Seattle with rooftop views", "Condos in Seattle"),
            ("Family homes near top schools in Austin under $600k", "Austin family homes"),
            ("Walkable neighborhoods in Chicago with 3+ bedrooms", "Chicago 3+ bed"),
            ("Luxury homes in San Francisco with smart home features", "SF luxury homes"),
            ("Single family homes in Denver under $500k", "Denver under $500k"),
            ("Pet friendly apartments in Portland with parking", "Portland pet friendly"),
        ]
        cols = st.columns(3)
        for i, (full_query, label) in enumerate(examples):
            with cols[i % 3]:
                st.markdown(
                    f"""
                    <div style="background: white; border: 1px solid #E0E0E0; border-radius: 10px;
                                padding: 1rem; margin-bottom: 0.75rem;">
                        <p style="color: #006AFF; font-weight: 600; font-size: 0.9rem; margin: 0 0 4px 0;">
                            {label}
                        </p>
                        <p style="color: #586069; font-size: 0.8rem; margin: 0;">
                            "{full_query}"
                        </p>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

# ── Tab 2: AI Assistant ─────────────────────────────────────────────────────
with tab_chat:
    render_chat()

# ── Tab 3: Market Insights ──────────────────────────────────────────────────
with tab_insights:
    st.markdown("### Market Insights")
    st.caption("Real-time analytics from Unity Catalog Delta tables")

    try:
        from utils.data_access import (
            get_market_summary,
            get_price_distribution,
            get_property_type_breakdown,
            get_neighborhood_stats,
            get_total_stats,
        )

        stats = get_total_stats()
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Listings", f"{stats['total_listings']:,}")
        m2.metric("Avg Price", f"${stats['avg_price']:,}")
        m3.metric("Markets", stats["num_cities"])
        m4.metric("Avg Days on Market", stats["avg_dom"])

        st.markdown("---")

        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown("#### Average Price by City")
            summary = get_market_summary()
            if not summary.empty:
                summary["avg_price"] = pd.to_numeric(summary["avg_price"], errors="coerce")
                summary["listing_count"] = pd.to_numeric(summary["listing_count"], errors="coerce")
                fig = px.bar(
                    summary.sort_values("avg_price", ascending=True),
                    x="avg_price", y="city",
                    orientation="h",
                    color="avg_price",
                    color_continuous_scale="Blues",
                    labels={"avg_price": "Average Price ($)", "city": "City"},
                )
                fig.update_layout(showlegend=False, height=400)
                st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.markdown("#### Property Type Breakdown")
            pt = get_property_type_breakdown()
            if not pt.empty:
                pt["count"] = pd.to_numeric(pt["count"], errors="coerce")
                fig2 = px.pie(
                    pt, values="count", names="property_type",
                    color_discrete_sequence=px.colors.sequential.Blues_r,
                )
                fig2.update_layout(height=400)
                st.plotly_chart(fig2, use_container_width=True)

        st.markdown("---")
        st.markdown("#### Neighborhood Deep Dive")
        selected_city = st.selectbox("Select a city", [
            "Austin", "Boston", "Chicago", "Denver", "Miami",
            "Nashville", "New York", "Portland", "San Francisco", "Seattle",
        ])

        if selected_city:
            nb = get_neighborhood_stats(selected_city)
            if not nb.empty:
                nb["avg_price"] = pd.to_numeric(nb["avg_price"], errors="coerce")
                nb["avg_school_rating"] = pd.to_numeric(nb["avg_school_rating"], errors="coerce")
                nb["avg_walk_score"] = pd.to_numeric(nb["avg_walk_score"], errors="coerce")
                nb["listings"] = pd.to_numeric(nb["listings"], errors="coerce")
                nb = nb.dropna(subset=["avg_price", "avg_walk_score", "listings"])

                c1, c2 = st.columns(2)
                with c1:
                    fig3 = px.bar(
                        nb, x="neighborhood", y="avg_price",
                        color="avg_school_rating",
                        color_continuous_scale="Greens",
                        labels={"avg_price": "Avg Price ($)", "neighborhood": "Neighborhood"},
                        title=f"Neighborhoods in {selected_city}",
                    )
                    fig3.update_layout(height=350)
                    st.plotly_chart(fig3, use_container_width=True)
                with c2:
                    fig4 = px.scatter(
                        nb, x="avg_walk_score", y="avg_price",
                        size="listings", text="neighborhood",
                        labels={"avg_walk_score": "Walk Score", "avg_price": "Avg Price ($)"},
                        title="Walk Score vs Price",
                    )
                    fig4.update_traces(textposition="top center")
                    fig4.update_layout(height=350)
                    st.plotly_chart(fig4, use_container_width=True)

                st.dataframe(nb, use_container_width=True)

    except Exception as e:
        st.error(f"Could not load market data. Make sure the notebooks have been run. Error: {e}")
