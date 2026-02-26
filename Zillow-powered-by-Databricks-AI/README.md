# Zillow Re-imagined with AI — Powered by Databricks

A full-stack AI real estate application that reimagines Zillow using the Databricks platform. Search for homes using natural language, chat with an AI assistant about properties and neighborhoods, and explore market insights — all powered by Vector Search, Foundation Models, MLflow, and Unity Catalog.

## What This App Does

| Tab | Description |
|-----|-------------|
| **Search** | Type a natural language query (e.g., *"modern condo near good schools in Seattle under $800k"*) and get matching properties displayed as cards on a map. Results include AI-generated Zestimates (price predictions). |
| **AI Assistant** | Chat with an AI real estate agent that can answer questions about properties, neighborhoods, and market trends. Uses RAG to ground responses in actual listing data. |
| **Market Insights** | Interactive Plotly dashboards — average price by city, property type breakdown, neighborhood deep dives with school rating and walk score correlations. |

## Architecture

```
Notebooks (one-time setup)                Streamlit App (runtime)
┌─────────────────────┐
│ 01_generate_data    │──► Delta Table ──────────► SQL queries (Market Insights tab)
│ 02_vector_search    │──► VS Index ─────────────► Semantic search + RAG (Search & Chat tabs)
│ 03_train_model      │──► MLflow Model (UC) ────► Zestimate predictions (Search tab)
└─────────────────────┘                            Foundation Model API ──► AI Chat
```

### Databricks Services Used

- **Unity Catalog** — Data governance, Delta tables, model registry
- **Vector Search** — Semantic similarity search over property listings (BGE Large EN embeddings)
- **Foundation Model API** — LLM chat via OpenAI-compatible endpoint (Llama 3.1 70B)
- **MLflow** — Price prediction model training, tracking, and serving
- **SQL Warehouse** — Market insights analytics queries
- **Databricks Apps** — Hosting the Streamlit application

## Prerequisites

- A **Databricks workspace** (Azure, AWS, or GCP) with Unity Catalog enabled
- A **SQL Warehouse** (serverless or classic) for analytics queries
- **Databricks CLI** installed and configured with a profile (e.g., `azure11`)
- Python 3.10+

## User Configuration Required

Before running or deploying, you **must** update the following values in the project to match your Databricks environment:

### 1. `config.py` — Catalog, Schema, and Endpoints

Open `config.py` and update these values:

```python
# Unity Catalog — use your own catalog and schema
CATALOG = "bircatalog"           # <-- Change to your catalog name
SCHEMA = "zillow"                # <-- Change to your schema name (will be created by notebook 01)

# Vector Search — endpoint name must be unique in your workspace
VS_ENDPOINT_NAME = "bir_zillow_vs_endpoint"   # <-- Change to a unique endpoint name

# Foundation Model API — must match an available serving endpoint in your workspace
LLM_MODEL = "databricks-meta-llama-3-1-70b-instruct"  # <-- Change if your workspace uses a different model

# SQL Warehouse — fallback ID if env var is not set
SQL_WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "148ccb90800933a1")  # <-- Change the default ID
```

> **How to find your SQL Warehouse ID:** In the Databricks UI, go to **SQL Warehouses**, click on your warehouse, and copy the ID from the URL or the overview page.

> **How to find available Foundation Model endpoints:** In the Databricks UI, go to **Serving** and look for available Foundation Model endpoints. Common options: `databricks-meta-llama-3-1-70b-instruct`, `databricks-dbrx-instruct`, `databricks-mixtral-8x7b-instruct`.

### 2. `databricks.yml` — Deployment Target and App Name

Open `databricks.yml` and update:

```yaml
resources:
  apps:
    zillow_app:
      name: "bir-zillow-ai-demo"     # <-- Change to a unique app name (lowercase, hyphens only)
      config:
        env:
          - name: "DATABRICKS_WAREHOUSE_ID"
            value: "148ccb90800933a1"  # <-- Change to your SQL Warehouse ID
      resources:
        - name: "zillow-sql-warehouse"
          sql_warehouse:
            id: "148ccb90800933a1"     # <-- Change to your SQL Warehouse ID

targets:
  prod:
    workspace:
      host: https://adb-984752964297111.11.azuredatabricks.net  # <-- Change to your workspace URL
```

### 3. Notebooks — Catalog and Schema (must match `config.py`)

The notebooks (`notebooks/01_generate_data.py`, `02_vector_search_setup.py`, `03_train_price_model.py`) have hardcoded catalog and schema names. Update the `CATALOG` and `SCHEMA` variables at the top of each notebook to match your `config.py` values.

## Setup Guide

### Step 1: Install dependencies (local development only)

```bash
pip install -r requirements.txt
```

### Step 2: Configure Databricks CLI

Ensure you have a Databricks CLI profile configured:

```bash
databricks configure --profile <your-profile-name>
```

### Step 3: Deploy the bundle to your workspace

```bash
# Validate the bundle configuration
databricks bundle validate -t prod -p <your-profile>

# Deploy files to the workspace
databricks bundle deploy -t prod -p <your-profile>
```

### Step 4: Run the setup pipeline (one-time)

This runs three notebooks sequentially on your workspace:
1. **01_generate_data** — Creates the Unity Catalog schema and populates ~1,000 synthetic property listings into a Delta table
2. **02_vector_search_setup** — Creates a Vector Search endpoint and a Delta Sync index with managed embeddings
3. **03_train_price_model** — Trains a GradientBoosting price prediction model and registers it in MLflow/Unity Catalog

```bash
databricks bundle run zillow_setup_pipeline -t prod -p <your-profile>
```

> This step takes 5-15 minutes. The Vector Search index may need additional time to sync. You can check its status in the Databricks UI under **Compute > Vector Search Endpoints**.

### Step 5: Deploy and launch the app

```bash
# Deploy the Streamlit app to Databricks Apps
databricks apps deploy <your-app-name> \
  --source-code-path /Workspace/<your-root-path>/files \
  -p <your-profile>
```

The app URL will be shown in the Databricks UI under **Apps**.

## Running Locally (Optional)

You can run the app locally for development. Set the required environment variables and start Streamlit:

```bash
export DATABRICKS_HOST="https://your-workspace.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."                    # Personal Access Token
export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"   # SQL Warehouse ID

streamlit run app.py
```

> Local mode requires that the setup pipeline (Step 4) has already been run on the workspace, since the app reads data from Unity Catalog, Vector Search, and MLflow.

## Project Structure

```
zillow/
├── app.py                         # Streamlit entry point (3 tabs: Search, Chat, Insights)
├── app.yaml                       # Databricks App runtime config
├── config.py                      # Shared constants — catalog, endpoints, models
├── databricks.yml                 # Databricks Asset Bundles deployment config
├── requirements.txt               # Python dependencies
├── style.css                      # Zillow-inspired CSS theme
├── components/
│   ├── chat.py                    # AI chat interface with streaming responses
│   ├── map_view.py                # Pydeck scatter map of search results
│   ├── property_card.py           # Property card grid with Zestimate badges
│   ├── search_bar.py              # Hero search bar with natural language input
│   └── sidebar_filters.py         # Sidebar filter controls (city, price, beds, etc.)
├── notebooks/
│   ├── 01_generate_data.py        # Generate 1,000 synthetic listings → Delta table
│   ├── 02_vector_search_setup.py  # Create Vector Search endpoint + index
│   └── 03_train_price_model.py    # Train price model → MLflow + UC registry
└── utils/
    ├── databricks_client.py       # Auth layer (OAuth for Apps, PAT for local)
    ├── data_access.py             # SQL queries via statement_execution API
    ├── price_predictor.py         # MLflow model scoring + heuristic fallback
    └── rag.py                     # Vector Search retrieval + Foundation Model chat
```

## Sample Search Queries

The search bar supports natural language with automatic constraint extraction:

- `"modern condos in Seattle with rooftop views"`
- `"family homes near top schools in Austin under $600k"`
- `"3+ bedroom single family home in Denver between $400k and $700k"`
- `"walkable neighborhoods in Chicago with 3+ bedrooms"`
- `"pet friendly apartments in Portland with parking"`

Price, bedroom, bathroom, and square footage constraints are parsed from the query and applied as post-filters on top of semantic search results.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| **App shows "App Not Available"** | Check app logs in Databricks UI. Ensure `requirements.txt` dependencies installed correctly. |
| **"TABLE_OR_VIEW_NOT_FOUND"** | Run the setup pipeline (Step 4). Verify catalog/schema in `config.py` matches the notebooks. |
| **"ENDPOINT_NOT_FOUND" in AI Assistant** | The Foundation Model endpoint name in `config.py` (`LLM_MODEL`) doesn't exist in your workspace. Check available endpoints under **Serving**. |
| **Search returns no results** | Verify the Vector Search index is ONLINE. Check status at **Compute > Vector Search Endpoints > your-endpoint**. |
| **"Filter string is not supported"** | This was a known issue (fixed). Ensure `rag.py` uses dict filters, not SQL filter strings. |
| **Market Insights shows error** | Verify `SQL_WAREHOUSE_ID` matches an active SQL Warehouse in your workspace. |
| **Text invisible in UI** | Clear browser cache. The CSS uses `!important` overrides for Databricks Apps dark-mode conflicts. |
