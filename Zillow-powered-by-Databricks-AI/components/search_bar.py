"""Hero search bar component."""

import streamlit as st


def render_search_bar() -> str | None:
    """Render the hero search section. Returns the search query or None."""
    st.markdown(
        """
        <div class="hero-section">
            <h1>Find Your Dream Home with AI</h1>
            <p>Search using natural language — powered by Databricks Vector Search & Foundation Models</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns([5, 1])
    with col1:
        query = st.text_input(
            "Search",
            placeholder="Search homes with AI... e.g. 'modern condo near good schools in Seattle under $800k'",
            label_visibility="collapsed",
            key="search_input",
        )
    with col2:
        search_clicked = st.button("Search", type="primary", use_container_width=True)

    if search_clicked and query:
        return query
    if query and st.session_state.get("last_query") != query:
        return query
    return None
