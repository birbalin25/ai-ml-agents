"""Vector Search retrieval + Foundation Model API generation."""

import re
from databricks.vector_search.client import VectorSearchClient
from openai import OpenAI
from config import VS_ENDPOINT_NAME, VS_INDEX_NAME, LLM_MODEL
from utils.databricks_client import get_workspace_client, get_databricks_host, get_token

SEARCH_COLUMNS = [
    "id", "address", "city", "state", "price", "beds", "baths", "sqft",
    "property_type", "neighborhood", "description", "features",
    "school_rating", "walk_score", "image_url", "listing_status",
    "latitude", "longitude", "year_built", "price_per_sqft",
    "lot_size", "hoa_fee", "parking", "days_on_market", "zip_code",
]


def _get_vs_client() -> VectorSearchClient:
    """Create a VectorSearchClient using the shared workspace auth."""
    w = get_workspace_client()
    return VectorSearchClient(
        workspace_url=w.config.host,
        personal_access_token=get_token(),
        service_principal_client_id=w.config.client_id,
        service_principal_client_secret=w.config.client_secret,
    )


def _parse_query_constraints(query: str) -> dict:
    """Extract numeric constraints from a natural language query.

    Supports patterns like:
      "under $300k", "below $500,000", "less than $1m"
      "over $200k", "above $400,000", "more than $600k", "at least $300k"
      "between $200k and $500k"
      "3 bed", "3+ bed", "at least 3 bed"
      "2 bath", "2+ bath"
      "under 2000 sqft", "over 1500 sq ft"
    """
    constraints = {}
    q = query.lower()

    # Normalize price shorthand: $300k -> 300000, $1.5m -> 1500000
    def _parse_price(s: str) -> float | None:
        s = s.replace(",", "").replace("$", "").strip()
        m = re.match(r"([\d.]+)\s*(k|m)?", s)
        if not m:
            return None
        val = float(m.group(1))
        unit = m.group(2)
        if unit == "k":
            val *= 1_000
        elif unit == "m":
            val *= 1_000_000
        return val

    # Price: under/below/less than / max
    m = re.search(r"(?:under|below|less than|up to|max|no more than|cheaper than)\s*\$?([\d.,]+\s*[km]?)", q)
    if m:
        price = _parse_price(m.group(1))
        if price:
            constraints["price_max"] = price

    # Price: over/above/more than / min / at least / starting
    m = re.search(r"(?:over|above|more than|at least|min|starting|from)\s*\$?([\d.,]+\s*[km]?)", q)
    if m:
        price = _parse_price(m.group(1))
        if price:
            constraints["price_min"] = price

    # Price: between X and Y
    m = re.search(r"between\s*\$?([\d.,]+\s*[km]?)\s*(?:and|-)\s*\$?([\d.,]+\s*[km]?)", q)
    if m:
        lo = _parse_price(m.group(1))
        hi = _parse_price(m.group(2))
        if lo and hi:
            constraints["price_min"] = lo
            constraints["price_max"] = hi

    # Beds: "3 bed", "3+ bed", "at least 3 bed"
    m = re.search(r"(\d+)\+?\s*(?:bed|br|bedroom)", q)
    if m:
        constraints["beds_min"] = int(m.group(1))

    # Baths: "2 bath", "2+ bath"
    m = re.search(r"(\d+\.?\d*)\+?\s*(?:bath|ba|bathroom)", q)
    if m:
        constraints["baths_min"] = float(m.group(1))

    # Sqft: under/over X sqft
    m = re.search(r"(?:under|below|less than)\s*([\d,]+)\s*(?:sq\s*ft|sqft|square feet)", q)
    if m:
        constraints["sqft_max"] = float(m.group(1).replace(",", ""))
    m = re.search(r"(?:over|above|more than|at least)\s*([\d,]+)\s*(?:sq\s*ft|sqft|square feet)", q)
    if m:
        constraints["sqft_min"] = float(m.group(1).replace(",", ""))

    return constraints


def _apply_constraints(properties: list[dict], constraints: dict) -> list[dict]:
    """Filter properties by parsed query constraints."""
    if not constraints:
        return properties

    filtered = []
    for prop in properties:
        try:
            price = float(prop.get("price", 0))
            beds = float(prop.get("beds", 0))
            baths = float(prop.get("baths", 0))
            sqft = float(prop.get("sqft", 0))
        except (ValueError, TypeError):
            continue

        if "price_max" in constraints and price > constraints["price_max"]:
            continue
        if "price_min" in constraints and price < constraints["price_min"]:
            continue
        if "beds_min" in constraints and beds < constraints["beds_min"]:
            continue
        if "baths_min" in constraints and baths < constraints["baths_min"]:
            continue
        if "sqft_max" in constraints and sqft > constraints["sqft_max"]:
            continue
        if "sqft_min" in constraints and sqft < constraints["sqft_min"]:
            continue

        filtered.append(prop)

    return filtered


def search_properties(query: str, num_results: int = 12, filters: dict | None = None) -> list[dict]:
    """Run a Vector Search similarity query and return matching properties."""
    vsc = _get_vs_client()
    index = vsc.get_index(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME)

    # Parse constraints from natural language query
    nl_constraints = _parse_query_constraints(query)

    # Standard VS endpoints require dict filters: {"column": "value"}
    # Range filters not supported in dict format, so we filter those client-side
    filter_dict = None
    range_filters = {}
    if filters:
        filter_dict = {}
        for key, value in filters.items():
            if value is not None and value != "All":
                if isinstance(value, str):
                    filter_dict[key] = value
                elif isinstance(value, (list, tuple)) and len(value) == 2:
                    range_filters[key] = value
        if not filter_dict:
            filter_dict = None

    # Fetch more results if we need to post-filter
    needs_post_filter = bool(range_filters) or bool(nl_constraints)
    fetch_count = num_results * 4 if needs_post_filter else num_results

    results = index.similarity_search(
        query_text=query,
        columns=SEARCH_COLUMNS,
        num_results=fetch_count,
        filters=filter_dict,
    )

    rows = results.get("result", {}).get("data_array", [])
    columns = [col["name"] for col in results.get("manifest", {}).get("columns", [])]

    properties = []
    for row in rows:
        prop = dict(zip(columns, row))
        if isinstance(prop.get("features"), str):
            prop["features"] = [f.strip() for f in prop["features"].strip("[]").replace("'", "").split(",")]
        properties.append(prop)

    # Apply sidebar range filters client-side
    if range_filters:
        filtered = []
        for prop in properties:
            match = True
            for key, (lo, hi) in range_filters.items():
                try:
                    val = float(prop.get(key, 0))
                    if val < lo or val > hi:
                        match = False
                        break
                except (ValueError, TypeError):
                    pass
            if match:
                filtered.append(prop)
        properties = filtered

    # Apply natural language constraints (price under $X, 3+ beds, etc.)
    if nl_constraints:
        properties = _apply_constraints(properties, nl_constraints)

    return properties[:num_results]


def _build_context(properties: list[dict]) -> str:
    """Format retrieved properties into context for the LLM."""
    if not properties:
        return "No properties found matching the search criteria."

    lines = []
    for i, p in enumerate(properties[:6], 1):
        lines.append(
            f"{i}. {p.get('address', 'N/A')}, {p.get('city', '')}, {p.get('state', '')} — "
            f"${p.get('price', 0):,.0f} | {p.get('beds', 0)} bed / {p.get('baths', 0)} bath | "
            f"{p.get('sqft', 0):,} sqft | {p.get('property_type', '')} | "
            f"School: {p.get('school_rating', 'N/A')}/10 | Walk: {p.get('walk_score', 'N/A')}\n"
            f"   {p.get('description', '')}"
        )
    return "\n\n".join(lines)


def _get_openai_client() -> OpenAI:
    """Create an OpenAI client using Databricks Foundation Model API."""
    host = get_databricks_host()
    token = get_token()
    return OpenAI(
        api_key=token,
        base_url=f"{host}/serving-endpoints",
    )


def chat_with_rag(
    user_message: str,
    chat_history: list[dict],
    search_results: list[dict] | None = None,
) -> str:
    """Generate a response using Foundation Model API with optional RAG context."""
    client = _get_openai_client()

    system_prompt = (
        "You are a knowledgeable and friendly AI real estate assistant for Zillow. "
        "Help users find homes, understand neighborhoods, and make informed decisions. "
        "Use the provided property listings as context when available. "
        "Be concise but helpful. Format prices with commas. "
        "If you don't have specific data, say so honestly."
    )

    if search_results:
        context = _build_context(search_results)
        system_prompt += f"\n\nHere are relevant property listings:\n{context}"

    messages = [{"role": "system", "content": system_prompt}]
    messages.extend(chat_history[-10:])
    messages.append({"role": "user", "content": user_message})

    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=messages,
        max_tokens=1024,
        temperature=0.7,
    )

    return response.choices[0].message.content


def stream_chat_with_rag(
    user_message: str,
    chat_history: list[dict],
    search_results: list[dict] | None = None,
):
    """Stream a response using Foundation Model API with optional RAG context."""
    client = _get_openai_client()

    system_prompt = (
        "You are a knowledgeable and friendly AI real estate assistant for Zillow. "
        "Help users find homes, understand neighborhoods, and make informed decisions. "
        "Use the provided property listings as context when available. "
        "Be concise but helpful. Format prices with commas. "
        "If you don't have specific data, say so honestly."
    )

    if search_results:
        context = _build_context(search_results)
        system_prompt += f"\n\nHere are relevant property listings:\n{context}"

    messages = [{"role": "system", "content": system_prompt}]
    messages.extend(chat_history[-10:])
    messages.append({"role": "user", "content": user_message})

    stream = client.chat.completions.create(
        model=LLM_MODEL,
        messages=messages,
        max_tokens=1024,
        temperature=0.7,
        stream=True,
    )

    for chunk in stream:
        if chunk.choices and chunk.choices[0].delta.content:
            yield chunk.choices[0].delta.content
