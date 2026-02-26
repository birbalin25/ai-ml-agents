"""Property card grid rendering."""

import streamlit as st


def _status_class(status: str) -> str:
    """Map listing status to CSS class."""
    s = status.lower().replace(" ", "-")
    return f"status-{s}"


def _to_float(val, default=0.0) -> float:
    """Safely convert a value to float."""
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def _to_int(val, default=0) -> int:
    """Safely convert a value to int."""
    try:
        return int(float(val)) if val is not None else default
    except (ValueError, TypeError):
        return default


def render_property_card(prop: dict, zestimate: float | None = None):
    """Render a single property card as HTML."""
    price = _to_float(prop.get("price"))
    beds = _to_int(prop.get("beds"))
    baths = _to_float(prop.get("baths"))
    sqft = _to_int(prop.get("sqft"))
    address = prop.get("address", "")
    city = prop.get("city", "")
    state = prop.get("state", "")
    zip_code = prop.get("zip_code", "")
    prop_type = prop.get("property_type", "")
    status = prop.get("listing_status", "For Sale")
    image_url = prop.get("image_url", "https://picsum.photos/640/400")
    features = prop.get("features", [])
    neighborhood = prop.get("neighborhood", "")

    if isinstance(features, list):
        features_html = "".join(f'<span class="feature-tag">{f}</span>' for f in features[:4])
    else:
        features_html = ""

    # Build card HTML
    card_html = (
        f'<div class="property-card">'
        f'<img src="{image_url}" alt="Property" onerror="this.src=\'https://picsum.photos/640/400\'">'
        f'<div class="card-body">'
        f'<span class="status-badge {_status_class(status)}">{status}</span>'
        f'<p class="price">${price:,.0f}</p>'
        f'<p class="details">'
        f'<strong>{beds}</strong> bd | <strong>{baths}</strong> ba | <strong>{sqft:,}</strong> sqft'
        f' -- {prop_type}</p>'
        f'<p class="address">{address}, {neighborhood}, {city}, {state} {zip_code}</p>'
        f'<div>{features_html}</div>'
        f'</div></div>'
    )
    st.markdown(card_html, unsafe_allow_html=True)

    # Render Zestimate as a separate Streamlit markdown element
    if zestimate is not None and price > 0:
        zestimate = _to_float(zestimate)
        diff = zestimate - price
        diff_pct = diff / price * 100
        if diff >= 0:
            arrow = "▲"
            color = "green"
        else:
            arrow = "▼"
            color = "red"
        st.markdown(
            f"**Zestimate:** ${zestimate:,.0f} &nbsp; :{color}[{arrow} {abs(diff_pct):.1f}%]"
        )


def render_property_grid(properties: list[dict], zestimates: list[float | None] | None = None, cols: int = 3):
    """Render a grid of property cards."""
    if not properties:
        st.info("No properties found. Try a different search.")
        return

    zestimates = zestimates or [None] * len(properties)

    for i in range(0, len(properties), cols):
        row = st.columns(cols)
        for j, col in enumerate(row):
            idx = i + j
            if idx < len(properties):
                with col:
                    render_property_card(properties[idx], zestimates[idx])
