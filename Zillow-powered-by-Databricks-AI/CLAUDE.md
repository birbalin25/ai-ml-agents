# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Zillow Re-imagined with AI** — a Databricks-powered Streamlit app combining real estate property search with AI-driven insights. Uses Vector Search for semantic property matching, Foundation Models for chat, and MLflow for price predictions (Zestimate).

## Build & Run Commands

```bash
# Local development
pip install -r requirements.txt
export DATABRICKS_HOST="https://your-workspace.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_WAREHOUSE_ID="148ccb90800933a1"
streamlit run app.py

# Deploy via Databricks Asset Bundles
databricks bundle validate -t prod -p azure11
databricks bundle deploy -t prod -p azure11
databricks apps deploy bir-zillow-ai-demo --source-code-path /Workspace/bir_app_root/zillow-ai-demo/prod/files -p azure11

# Run setup pipeline (one-time: data generation, VS index, model training)
databricks bundle run zillow_setup_pipeline -t prod -p azure11
```

## Architecture

```
User Query → Search Bar → Query Constraint Parser (regex, rag.py)
  → Vector Search (similarity) → Dict filters (exact match) + Client-side filters (ranges)
  → Property Results → Grid + Map + Zestimates (MLflow or heuristic fallback)

Chat → RAG retrieval (top 5 properties as context) → Foundation Model API (streaming)

Market Insights → SQL queries via statement_execution API → Plotly charts
```

### Key Layers

- **`config.py`**: Single source of truth — catalog (`bircatalog.zillow`), VS endpoint/index names, model names, SQL warehouse ID, metro definitions
- **`utils/databricks_client.py`**: Cached `WorkspaceClient` (LRU). Handles both OAuth (Databricks Apps) and PAT (local dev). `get_token()` extracts Bearer token for OpenAI-compatible API
- **`utils/rag.py`**: Vector Search retrieval + NL query constraint parsing + Foundation Model chat (streaming and non-streaming)
- **`utils/data_access.py`**: SQL queries via `statement_execution` API against Unity Catalog Delta tables
- **`utils/price_predictor.py`**: MLflow model with heuristic fallback (±8% variance from listing price, deterministic per property)
- **`components/`**: Streamlit UI components (search bar, property cards, sidebar filters, chat, pydeck map)
- **`notebooks/`**: One-time setup — data generation (01), Vector Search index (02), ML model training (03)

## Critical Patterns

**Lazy imports**: `app.py` and `chat.py` import backend modules (`utils.rag`, `utils.price_predictor`, `utils.data_access`) inside functions/try-except blocks, not at module level. This prevents the app from crashing if one backend is unavailable.

**Vector Search filters**: Standard endpoints only accept dict filters (`{"city": "Seattle"}`). Range comparisons (`>=`, `<=`) are NOT supported — they must be applied client-side after fetching extra results (4x `num_results`).

**NL constraint parsing** (`_parse_query_constraints`): Regex extracts price ("under $300k"), beds ("3+ bed"), sqft ("over 1500 sqft") from the search query. Price shorthand: `k` = 1,000, `m` = 1,000,000.

**Price predictor fallback**: Attempts MLflow model load once (global `_model_loaded` flag). If unavailable, every call returns a heuristic estimate. Never raises exceptions.

**Auth for Databricks Apps**: The app runs as a service principal with OAuth. `WorkspaceClient()` auto-detects credentials. `get_token()` parses Bearer token from `client.config.authenticate()` headers for the OpenAI-compatible Foundation Model API client.

**SQL data types**: All values from `statement_execution` API come back as strings. Must call `pd.to_numeric()` before using in Plotly charts or arithmetic.

**CSS visibility**: Streamlit on Databricks Apps can have dark-mode conflicts. All text colors use `!important` overrides. All inputs have explicit `background-color: white`, `color: var(--zillow-dark)`, and `caret-color` set.

## Deployment Notes

- **DAB targets**: `dev` (default) and `prod` (workspace: `adb-984752964297111.11.azuredatabricks.net`)
- **Two-step deploy**: `databricks bundle deploy` uploads files, then `databricks apps deploy` restarts the app with new source
- **App name**: `bir-zillow-ai-demo` (must be lowercase alphanumeric + hyphens)
- Notebooks use hardcoded catalog/schema (`bircatalog.zillow`) — must match `config.py`
- Jobs use serverless compute (`environment_key: default`)
