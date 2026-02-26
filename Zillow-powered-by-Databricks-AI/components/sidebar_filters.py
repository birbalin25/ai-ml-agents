"""Sidebar filter controls."""

import streamlit as st
from config import METROS, PROPERTY_TYPES


def render_sidebar_filters() -> dict:
    """Render sidebar filters and return the current filter selections."""
    st.sidebar.markdown("### Filters")

    city = st.sidebar.selectbox(
        "City",
        options=["All"] + sorted(METROS.keys()),
        index=0,
        key="filter_city",
    )

    property_type = st.sidebar.selectbox(
        "Property Type",
        options=["All"] + PROPERTY_TYPES,
        index=0,
        key="filter_property_type",
    )

    price_range = st.sidebar.slider(
        "Price Range",
        min_value=0,
        max_value=3_000_000,
        value=(0, 3_000_000),
        step=50_000,
        format="$%d",
        key="filter_price",
    )

    beds_min = st.sidebar.selectbox(
        "Min Bedrooms",
        options=[0, 1, 2, 3, 4, 5],
        index=0,
        key="filter_beds",
    )

    baths_min = st.sidebar.selectbox(
        "Min Bathrooms",
        options=[0.0, 1.0, 1.5, 2.0, 2.5, 3.0],
        index=0,
        key="filter_baths",
    )

    sqft_range = st.sidebar.slider(
        "Square Feet",
        min_value=0,
        max_value=8_000,
        value=(0, 8_000),
        step=100,
        key="filter_sqft",
    )

    year_range = st.sidebar.slider(
        "Year Built",
        min_value=1920,
        max_value=2024,
        value=(1920, 2024),
        key="filter_year",
    )

    filters = {}
    if city != "All":
        filters["city"] = city
    if property_type != "All":
        filters["property_type"] = property_type
    if price_range != (0, 3_000_000):
        filters["price"] = price_range
    if beds_min > 0:
        filters["beds"] = (beds_min, 10)
    if baths_min > 0:
        filters["baths"] = (baths_min, 10)
    if sqft_range != (0, 8_000):
        filters["sqft"] = sqft_range
    if year_range != (1920, 2024):
        filters["year_built"] = year_range

    return filters
