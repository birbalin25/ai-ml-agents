"""AI chat interface component."""

import streamlit as st


def render_chat():
    """Render the AI assistant chat interface."""
    st.markdown("### AI Real Estate Assistant")
    st.caption(
        "Ask me anything about properties, neighborhoods, market trends, "
        "or home-buying advice. Powered by Databricks Foundation Model API + RAG."
    )

    # Initialize chat history
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # Display chat history
    for msg in st.session_state.chat_history:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # Chat input
    user_input = st.chat_input("Ask about homes, neighborhoods, market trends...")

    if user_input:
        try:
            from utils.rag import search_properties, stream_chat_with_rag

            # Show user message
            st.session_state.chat_history.append({"role": "user", "content": user_input})
            with st.chat_message("user"):
                st.markdown(user_input)

            # Retrieve relevant properties for context
            with st.spinner("Searching properties..."):
                context_results = search_properties(user_input, num_results=5)

            # Stream assistant response
            with st.chat_message("assistant"):
                response_placeholder = st.empty()
                full_response = ""
                for chunk in stream_chat_with_rag(
                    user_message=user_input,
                    chat_history=st.session_state.chat_history[:-1],
                    search_results=context_results,
                ):
                    full_response += chunk
                    response_placeholder.markdown(full_response + "▌")
                response_placeholder.markdown(full_response)

            st.session_state.chat_history.append({"role": "assistant", "content": full_response})

            # Show source properties if any
            if context_results:
                with st.expander("Properties referenced in this response"):
                    for p in context_results[:3]:
                        st.markdown(
                            f"- **${p.get('price', 0):,.0f}** — {p.get('beds', 0)}bd/{p.get('baths', 0)}ba, "
                            f"{p.get('sqft', 0):,} sqft — {p.get('address', '')}, {p.get('city', '')}"
                        )
        except Exception as e:
            st.error(f"Chat error: {e}")
