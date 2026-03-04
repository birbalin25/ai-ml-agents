# Support Triage Portal

A full-stack support ticket management app built as a [Databricks App](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html) with a [Lakebase](https://docs.databricks.com/en/lakebase/index.html) (PostgreSQL) backend.

## Architecture

- **Backend**: FastAPI + asyncpg (Python 3.11+)
- **Frontend**: React + Vite (SPA served by FastAPI)
- **Database**: Lakebase (PostgreSQL) — supports both Autoscaling and Provisioned instances
- **Deployment**: Databricks Asset Bundles with dev/prod targets
- **Auth**: Databricks SSO via app proxy headers

## Project Structure

```
support-portal/
  backend/
    main.py          # FastAPI app, lifespan, SPA serving
    config.py        # Environment-based settings
    database.py      # asyncpg pool, credential generation, retry logic
    models.py        # Pydantic models (Ticket, Comment, History, Stats)
    schema.sql       # DDL for support_app schema + tables
    routes/
      tickets.py     # CRUD + search/filter/pagination
      comments.py    # Ticket comments
      stats.py       # Dashboard statistics
  frontend/
    src/
      components/    # React components (Dashboard, TicketList, etc.)
      api.js         # API client
  scripts/
    start_app.py     # Entrypoint: builds frontend, inits DB, starts uvicorn
    init_db.py       # Schema migration script
  databricks.yml     # Bundle config with variable substitution
  variables.yml      # All configurable variables with defaults
  app.yaml           # Runtime config for Databricks Apps platform
```

## Database Schema

All tables live in the `support_app` schema:

| Table | Purpose |
|---|---|
| `tickets` | Support tickets with status, priority, severity, category |
| `comments` | Ticket comments (public and internal) |
| `ticket_history` | Audit trail of field changes |
| `attachments` | File attachment metadata |

## Deployment Targets

| Target | Workspace | App Name | Lakebase Type |
|---|---|---|---|
| `dev` | `fevm-serverless-bir` | `support-portal` | Autoscaling |
| `prod` | `e2-demo-field-eng` | `support-portal-prod` | Provisioned |

Configuration is parameterized via `variables.yml` and overridden per-target in `databricks.yml`.

## Quick Start

### Prerequisites

- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html) (v0.285+)
- [uv](https://docs.astral.sh/uv/) (Python package manager)
- Node.js (for frontend build)

### Deploy

```bash
# Validate
databricks bundle validate -t dev

# Deploy to dev
databricks bundle deploy -t dev
databricks apps deploy support-portal --source-code-path <workspace-path>

# Deploy to prod
databricks bundle deploy -t prod
databricks apps deploy support-portal-prod --source-code-path <workspace-path>
```

### Initialize Database

```bash
# Via CLI (local)
uv run init-db

# Or automatically on first app start via start_app.py
```

### Run Locally

```bash
uv run start-app
```

## Environment Variables

Set via `app.yaml` (runtime) and `databricks.yml` (bundle config):

| Variable | Description | Default |
|---|---|---|
| `LAKEBASE_PROJECT` | Autoscaling Lakebase project name | `support-lakebase` |
| `LAKEBASE_BRANCH` | Autoscaling Lakebase branch | `production` |
| `LAKEBASE_ENDPOINT` | Autoscaling Lakebase endpoint | `primary` |
| `LAKEBASE_DATABASE` | Logical database name for queries | `support_portal` |
| `LAKEBASE_HOST` | Public Lakebase endpoint host | (empty) |
| `LAKEBASE_INSTANCE_NAME` | Provisioned Lakebase instance name | (empty) |

When deployed as a Databricks App with a Lakebase resource binding, `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE`, and `PGSSLMODE` are injected automatically by the platform.

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/health` | Health check |
| `GET` | `/api/stats` | Dashboard statistics |
| `GET` | `/api/tickets` | List tickets (paginated, filterable) |
| `POST` | `/api/tickets` | Create ticket |
| `GET` | `/api/tickets/{id}` | Get ticket detail |
| `PUT` | `/api/tickets/{id}` | Update ticket |
| `DELETE` | `/api/tickets/{id}` | Delete ticket |
| `GET` | `/api/tickets/{id}/comments` | List comments |
| `POST` | `/api/tickets/{id}/comments` | Add comment |
| `GET` | `/api/tickets/{id}/history` | Audit trail |
