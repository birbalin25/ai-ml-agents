"""Pydeck map visualization of property search results."""

import streamlit as st
import pydeck as pdk
import pandas as pd


def render_map(properties: list[dict]):
    """Render a scatter plot map of properties using pydeck."""
    if not properties:
        return

    df = pd.DataFrame(properties)

    # Ensure numeric lat/lon
    df["latitude"] = pd.to_numeric(df.get("latitude", pd.Series(dtype=float)), errors="coerce")
    df["longitude"] = pd.to_numeric(df.get("longitude", pd.Series(dtype=float)), errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"])

    if df.empty:
        st.warning("No location data available for map.")
        return

    df["price"] = pd.to_numeric(df.get("price", pd.Series(dtype=float)), errors="coerce").fillna(0)
    df["beds"] = pd.to_numeric(df.get("beds", pd.Series(dtype=float)), errors="coerce").fillna(0).astype(int)
    df["baths"] = pd.to_numeric(df.get("baths", pd.Series(dtype=float)), errors="coerce").fillna(0)
    df["sqft"] = pd.to_numeric(df.get("sqft", pd.Series(dtype=float)), errors="coerce").fillna(0).astype(int)

    # Tooltip fields — pydeck HTML tooltip references column names directly
    df["price_fmt"] = df["price"].apply(lambda x: f"${x:,.0f}")
    df["sqft_fmt"] = df["sqft"].apply(lambda x: f"{x:,}")
    df["address"] = df.get("address", "").astype(str)
    df["city"] = df.get("city", "").astype(str)
    df["property_type"] = df.get("property_type", "").astype(str)
    df["neighborhood"] = df.get("neighborhood", "").astype(str)

    # Color by price tier — vibrant gradient from green (affordable) to red (expensive)
    price_min = df["price"].min()
    price_max = df["price"].max()
    price_range = price_max - price_min if price_max > price_min else 1

    def price_color(p):
        t = (p - price_min) / price_range  # 0 = cheapest, 1 = most expensive
        r = int(40 + 215 * t)
        g = int(180 - 130 * t)
        b = int(100 - 60 * t)
        return r, g, b

    colors = df["price"].apply(price_color)
    df["color_r"] = colors.apply(lambda c: c[0])
    df["color_g"] = colors.apply(lambda c: c[1])
    df["color_b"] = colors.apply(lambda c: c[2])

    center_lat = df["latitude"].mean()
    center_lon = df["longitude"].mean()

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["longitude", "latitude"],
        get_fill_color=["color_r", "color_g", "color_b", 220],
        get_line_color=[255, 255, 255, 200],
        line_width_min_pixels=2,
        get_radius=400,
        pickable=True,
        auto_highlight=True,
        highlight_color=[0, 106, 255, 100],
        stroked=True,
    )

    view = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lon,
        zoom=10,
        pitch=30,
    )

    tooltip = {
        "html": """
            <div style="font-family: Arial, sans-serif; padding: 4px 0;">
                <div style="font-size: 16px; font-weight: 700; color: #006AFF;">{price_fmt}</div>
                <div style="font-size: 13px; margin: 4px 0;">
                    <b>{beds}</b> bed / <b>{baths}</b> ba / <b>{sqft_fmt}</b> sqft
                </div>
                <div style="font-size: 12px; color: #555;">{address}</div>
                <div style="font-size: 12px; color: #555;">{neighborhood}, {city}</div>
                <div style="font-size: 11px; color: #888; margin-top: 2px;">{property_type}</div>
            </div>
        """,
        "style": {
            "backgroundColor": "white",
            "color": "#2A2A33",
            "border": "1px solid #ddd",
            "border-radius": "8px",
            "padding": "8px 12px",
            "box-shadow": "0 2px 8px rgba(0,0,0,0.15)",
        },
    }

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view,
        tooltip=tooltip,
        map_style="mapbox://styles/mapbox/streets-v12",
    )

    st.pydeck_chart(deck)
