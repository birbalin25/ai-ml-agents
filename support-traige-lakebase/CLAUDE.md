# CLAUDE.md - Support Triage Portal

## Project Overview

Support ticket management app deployed as a Databricks App with Lakebase backend.
FastAPI (Python) backend + React/Vite frontend (static SPA served by FastAPI).

## Key Commands

```bash
# Run locally
uv run start-app

# Deploy dev
databricks bundle deploy -t dev -p vm
databricks apps deploy support-portal --source-code-path /Workspace/Users/birbal.das@databricks.com/.bundle/support-portal/dev/files -p vm

# Deploy prod
databricks bundle deploy -t prod -p e2demofe
databricks apps deploy support-portal-prod --source-code-path /Workspace/bir_support/prod/files -p e2demofe

# Validate
databricks bundle validate -t dev -p vm
databricks bundle validate -t prod -p e2demofe

# Init database schema
uv run init-db

# Build frontend only
cd frontend && npm install && npm run build
```

## Architecture

- **Backend**: `backend/` — FastAPI + asyncpg
- **Frontend**: `frontend/` — React + Vite (built to `frontend/dist/`, served by FastAPI)
- **Entrypoint**: `scripts/start_app.py` — builds frontend, inits DB, starts uvicorn
- **Database**: Lakebase (PostgreSQL), tables in `support_app` schema

## Deployment

- Config is split: `databricks.yml` (bundle) + `variables.yml` (all variable defaults)
- `app.yaml` is what the Databricks Apps platform reads at runtime — env vars MUST be in `app.yaml`, not just in `databricks.yml`
- `databricks bundle deploy` sets the app resource config but `databricks apps deploy` reads `app.yaml` from source and overrides it

### Targets

| Target | Workspace | Profile | Lakebase Type |
|---|---|---|---|
| dev | fevm-serverless-bir.cloud.databricks.com | vm | Autoscaling (project/branch/endpoint) |
| prod | e2-demo-field-eng.cloud.databricks.com | e2demofe | Provisioned (instance_name) |

## Database Details

- **Dev**: Autoscaling Lakebase project `support-lakebase`, database `postgres`, schema `support_app`
- **Prod**: Provisioned Lakebase instance `bir-support-lakebase`, database `databricks_postgres`, schema `support_app`
- Pool sets `search_path = 'support_app,public'`
- Credential generation in `database.py` handles both Autoscaling (`/api/2.0/postgres/credentials`) and Provisioned (`/api/2.0/database/credentials`) APIs based on `LAKEBASE_INSTANCE_NAME` env var

## Important Patterns

### Lakebase Autoscaling vs Provisioned
- **Autoscaling**: Uses `projects/{name}/branches/{branch}/endpoints/{endpoint}` paths. DDL works on `postgres` database.
- **Provisioned**: Uses `instance_names` array. DDL only works on `databricks_postgres` database (not `postgres`). Must grant `USAGE ON SCHEMA` to other users/SPs.

### Resource Bindings
- The Databricks Apps platform injects `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE`, `PGSSLMODE` from the resource binding
- `PGPASSWORD` may NOT be injected — the app falls back to `_generate_db_credential()` in `database.py`
- The app's service principal needs appropriate API scopes to generate credentials

### Workspace Auth
- `workspace.host` and `workspace.profile` in `databricks.yml` CANNOT use `${var.xxx}` — auth is resolved before variable substitution

## File Locations

| File | Purpose |
|---|---|
| `databricks.yml` | Bundle config, targets, app resource definition |
| `variables.yml` | All variable definitions with defaults |
| `app.yaml` | Runtime config read by Databricks Apps platform |
| `backend/schema.sql` | DDL for all tables and indexes |
| `backend/database.py` | Connection pool, credential generation, query helpers |
| `backend/config.py` | Environment-based settings |
| `scripts/init_db.py` | One-time schema migration |
| `scripts/start_app.py` | App entrypoint (frontend build + DB init + uvicorn) |

## CLI Paths

- Databricks CLI (v0.290+): `/opt/homebrew/bin/databricks`
- Always use this path for Lakebase operations (`postgres` subcommand)
