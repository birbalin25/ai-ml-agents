# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Deployment Commands

This project uses **Databricks Asset Bundles (DAB)** for deployment. The new CLI at `/opt/homebrew/bin/databricks` (v0.292.0) is required — the old CLI at `/usr/local/bin/databricks` (v0.216.0) lacks bundle support.

```bash
# Validate bundle configuration
/opt/homebrew/bin/databricks bundle validate -t dev

# Deploy all resources (DLT pipeline, workflow, app)
/opt/homebrew/bin/databricks bundle deploy -t dev

# Run the workflow manually
/opt/homebrew/bin/databricks bundle run -t dev fraud_triage_workflow

# Deploy to production
/opt/homebrew/bin/databricks bundle deploy -t prod
```

Both targets use the `vm` Databricks CLI profile (workspace: `fevm-serverless-bir.cloud.databricks.com`).

### Running Scripts (Bootstrap / One-Time Setup)

Scripts in `scripts/` are standalone Python utilities for initial workspace setup (schemas, volumes, data upload, Genie Space, PII masking, permissions). They are **not** deployed by DAB and use the old CLI via `subprocess`. Run them with `python scripts/<script>.py` from the project root.

## Architecture

The system is a **fraud triage pipeline** with three deployment artifacts managed by DAB:

### Data Flow

```
Volume (CSV) → DLT Pipeline → Delta Tables → Lakebase Postgres → FastAPI App
                                    ↑                |
                                    └── reverse sync ─┘ (analyst decisions)
```

### 1. DLT Pipeline (`resources/pipeline.yml` → `notebooks/01_dlt_fraud_pipeline.py`)

Serverless DLT pipeline writing to `{catalog}.fraud_detection` schema:
- **Bronze**: Raw CSV ingestion (transactions, login_logs, user_profiles, fraud_signatures)
- **Silver**: `silver_enriched_transactions` (transactions joined with login sessions + user profiles, risk signals computed), `silver_velocity_anomalies`
- **Gold**: `gold_fraud_kpis`, `gold_account_takeover`, `gold_fraud_by_pattern`
- **Operational**: `real_time_fraud_triage` (materialized view feeding Lakebase)

The DLT notebook reads its catalog from `spark.conf.get("pipeline.catalog", ...)` which DAB sets automatically.

### 2. Workflow Job (`resources/job.yml`)

Three-task DAG running every 4 hours:
1. `run_dlt_pipeline` — triggers DLT refresh
2. `sync_to_lakebase` — `notebooks/06_lakebase_sync.py`: inserts new triage rows into Lakebase Postgres using `ON CONFLICT DO NOTHING` (preserves analyst decisions)
3. `sync_decisions_to_delta` — `notebooks/07_sync_decisions_to_delta.py`: merges analyst decisions from Lakebase back into `fraud_operations.real_time_fraud_triage` Delta table

Notebooks receive `catalog` via `base_parameters` → `dbutils.widgets.get("catalog")`. The job environment installs `psycopg2-binary` and `databricks-sdk`.

### 3. FastAPI App (`resources/app.yml` → `app/app.py`)

Dual-data-source architecture:
- **Lakebase Postgres** (low-latency): `/api/stats`, `/api/queue`, `/api/decision`, `/api/user/{id}`, `/api/transaction/{id}`
- **SQL Warehouse** (analytical): `/api/kpis`, `/api/patterns`

The app reads config from environment variables (`LAKEBASE_HOST`, `LAKEBASE_DB`, `LAKEBASE_INSTANCE`, `CATALOG`, `WAREHOUSE_ID`). DAB injects these via `resources/app.yml`. The frontend is an embedded SPA in `app.py` (no separate UI build step).

Lakebase authentication uses Databricks SDK `database.generate_database_credential()` with token caching (1-hour TTL, 5-min pre-expiry refresh).

## Bundle Configuration

All environment-specific values live in `variables.yml` — **no hardcoded values in `databricks.yml` or `resources/*.yml`**. Key variables:

| Variable | Purpose |
|----------|---------|
| `catalog` | Unity Catalog name |
| `warehouse_id` | SQL Warehouse for the app |
| `lakebase_host/db/instance/user` | Lakebase Postgres connection |
| `dlt_pipeline_name/job_name/app_name` | Resource display names (prefixed `[DEV]` in dev) |
| `job_schedule_status` | `PAUSED` (dev) / `UNPAUSED` (prod) |

## Risk Scoring Model

Rule-based scoring (0-100) computed in both DLT pipeline and deploy script:
- Impossible travel (>500mi in <10min): +40
- MFA change + high-value (>$10K): +30
- High-value wire after IP change: +25
- Bot signature: +20, Abnormal typing (<0.45): +15
- International + new account (<90d): +15, Amount anomaly (>5x avg): +10

Categories: RED (>=80, BLOCK), YELLOW (50-79, YELLOW_FLAG), GREEN (<50, ALLOW).

## Schemas

- `{catalog}.fraud_detection` — Bronze, Silver tables + DLT MV
- `{catalog}.fraud_operations` — Gold KPIs + mutable `real_time_fraud_triage` operational table
- `{catalog}.fraud_investigation` — Investigation tools (Genie Space)
