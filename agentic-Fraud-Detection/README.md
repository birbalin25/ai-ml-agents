# Agentic Fraud Detection & Triage System

An end-to-end fraud detection pipeline built on Databricks Lakehouse, combining rule-based risk scoring with AI-powered reasoning to triage suspicious banking transactions in real time. Fraud analysts review flagged transactions through a live web app backed by Lakebase (Postgres) for sub-second operational queries and a SQL Warehouse for historical analytics.

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           DATA FLOW                                      │
│                                                                          │
│  CSV Files          DLT Pipeline              Lakebase         App       │
│  (Volume)     ┌───────────────────────┐      (Postgres)    (FastAPI)     │
│     │         │ Bronze → Silver → Gold│         │              │         │
│     └────────►│                       ├────►  Delta  ────►  Sync ──►  UI│
│               │   Risk Scoring        │      Tables     ON CONFLICT     │
│               │   Triage Store (MV)   │         │        DO NOTHING     │
│               └───────────────────────┘         │              │         │
│                                                 │   ◄──────────┘         │
│                                            Reverse Sync                  │
│                                         (Analyst Decisions)              │
└──────────────────────────────────────────────────────────────────────────┘
```

### Components

| Component | Technology | Description |
|-----------|-----------|-------------|
| **DLT Pipeline** | Delta Live Tables (Serverless) | Bronze/Silver/Gold medallion architecture with rule-based risk scoring |
| **AI Reasoning Agent** | Foundation Model API (Claude Sonnet 4.5) + MLflow | Explainable risk assessments for regulatory compliance (GDPR/CCPA) |
| **Operational Store** | Lakebase (Provisioned Postgres) | Sub-second reads/writes for the live fraud queue |
| **Analytical Store** | SQL Warehouse + Delta Tables | Historical KPIs, pattern analysis, enrichment data |
| **Live Fraud Queue** | FastAPI + embedded SPA | Analyst UI for reviewing and actioning flagged transactions |
| **Orchestration** | Databricks Workflows (DAB) | 3-task job running every 4 hours |
| **IaC** | Databricks Asset Bundles | Declarative deployment with dev/prod targets |

### Data Flow (3-Task Workflow)

1. **Task 1 — DLT Pipeline Refresh**: Ingests CSV from Unity Catalog Volume, builds Bronze → Silver (enriched transactions with risk signals) → Gold (KPIs) → Operational triage store (materialized view)
2. **Task 2 — Delta → Lakebase Sync** (`06_lakebase_sync.py`): Inserts new triage rows into Lakebase Postgres using `INSERT ... ON CONFLICT DO NOTHING` to preserve analyst decisions already recorded
3. **Task 3 — Reverse Sync** (`07_sync_decisions_to_delta.py`): Merges analyst decisions (BLOCK/RELEASE/ESCALATE) from Lakebase back into the Delta operational table so Genie Space users see up-to-date data

### Risk Scoring

Rule-based scoring (0–100) computed in the DLT Silver layer:

| Signal | Points | Condition |
|--------|--------|-----------|
| Impossible travel | +40 | >500 miles in <10 minutes |
| MFA change + high-value | +30 | MFA changed before >$10K transaction |
| High-value wire after IP change | +25 | IP changed before >$10K wire transfer |
| Bot signature | +20 | Automated session detected |
| Abnormal typing cadence | +15 | Typing score < 0.45 |
| International + new account | +15 | International txn + account < 90 days |
| Amount anomaly | +10 | Amount > 5x user's avg monthly spend |

**Categories**: RED (>=80 → BLOCK), YELLOW (50–79 → YELLOW_FLAG for review), GREEN (<50 → ALLOW)

## Project Structure

```
fraud-triage-agent/
├── databricks.yml              # DAB main config (dev + prod targets)
├── variables.yml               # All environment variables
├── resources/
│   ├── pipeline.yml            # DLT pipeline resource
│   ├── job.yml                 # 3-task workflow resource
│   └── app.yml                 # Databricks App resource
├── app/
│   ├── app.py                  # FastAPI app (dual data source: Lakebase + SQL Warehouse)
│   ├── app.yaml                # App config (overridden by DAB at deploy time)
│   └── requirements.txt        # fastapi, uvicorn, databricks-sdk, psycopg2-binary
├── notebooks/
│   ├── 01_dlt_fraud_pipeline.py        # DLT pipeline (Bronze → Silver → Gold → Triage MV)
│   ├── 02_fraud_reasoning_agent.py     # AI fraud analyst agent (Claude Sonnet 4.5 + MLflow)
│   ├── 04_databricks_connect_local.py  # Local dev helper
│   ├── 05_validate_solution.py         # Component validation tests
│   ├── 06_lakebase_sync.py             # Delta → Lakebase sync
│   ├── 07_sync_decisions_to_delta.py   # Lakebase → Delta reverse sync
│   └── 99_e2e_validation.py            # End-to-end validation
├── scripts/
│   ├── deploy_all.py                   # Full one-time deployment script
│   ├── generate_mock_data.py           # Mock data generator (5K users, 100K txns, 200K logins)
│   ├── create_workflow.py              # Workflow creation (standalone)
│   ├── create_genie_space.py           # Genie Space for conversational investigation
│   ├── grant_app_permissions.py        # Service principal permissions
│   ├── setup_pii_masking.py            # PII column masking (card, email, phone)
│   └── ...                             # Other setup utilities
├── data/                               # Mock CSV datasets (not deployed by DAB)
│   ├── transactions.csv                # 100K banking transactions
│   ├── login_logs.csv                  # 200K login events with device/geo/typing signals
│   ├── user_profiles.csv               # 5K user profiles
│   └── known_fraud_signatures.csv      # 200 fraud pattern signatures
└── CLAUDE.md                           # Claude Code guidance
```

## Prerequisites

- **Databricks workspace** with Unity Catalog, Serverless compute, and Lakebase enabled
- **Databricks CLI** v0.250+ installed (`/opt/homebrew/bin/databricks` on macOS)
- **CLI profile** configured (this project uses profile `vm`)
- **Lakebase instance** provisioned (instance name: `fraud-triage-ops`, database: `fraud_ops`)
- **SQL Warehouse** running and accessible

## Step-by-Step Setup

### Step 1: Clone the Repository

```bash
git clone https://github.com/birbalin25/ai-ml-agents.git
cd ai-ml-agents/agentic-Fraud-Detection
```

### Step 2: Configure Your Environment

Edit `variables.yml` to match your workspace:

```yaml
variables:
  catalog:
    default: your_catalog_name          # Unity Catalog
  warehouse_id:
    default: "your_warehouse_id"        # SQL Warehouse ID
  lakebase_host:
    default: "your-instance.database.cloud.databricks.com"
  lakebase_db:
    default: fraud_ops
  lakebase_instance:
    default: your-lakebase-instance
  lakebase_user:
    default: "your.email@company.com"
  service_principal_id:
    default: "your-sp-app-id"
```

Update the target profile in `databricks.yml` to match your CLI profile:
```yaml
targets:
  dev:
    workspace:
      profile: your_profile    # must match ~/.databrickscfg
```

### Step 3: Generate Mock Data (Optional)

If starting fresh without existing data:

```bash
python scripts/generate_mock_data.py
```

This creates 5K users, 100K transactions, 200K login events, and 200 fraud signatures in `data/`.

### Step 4: Bootstrap the Workspace (One-Time)

Run the full deployment script to set up Unity Catalog schemas, upload data to Volumes, create base tables, and configure PII masking:

```bash
python scripts/deploy_all.py
```

This script creates:
- Schemas: `fraud_detection`, `fraud_investigation`, `fraud_operations`
- Volume: `fraud_detection.source_files`
- Bronze/Silver/Gold Delta tables
- Operational triage store
- PII masking functions on sensitive columns
- DLT pipeline and Genie Space

### Step 5: Set Up Lakebase

Create the Lakebase Postgres tables (run once):

```bash
python scripts/migrate_to_lakebase.py
```

Grant the app service principal access to fraud tables:

```bash
python scripts/grant_app_permissions.py
```

### Step 6: Validate the Bundle

```bash
/opt/homebrew/bin/databricks bundle validate -t dev
```

Expected output: `Validation OK!`

### Step 7: Deploy with DAB

```bash
/opt/homebrew/bin/databricks bundle deploy -t dev
```

This deploys all three resources:
- **DLT Pipeline**: `[DEV] Fraud Triage - DLT Pipeline`
- **Workflow Job**: `[DEV] Fraud Triage - Pipeline + Sync` (3 tasks, schedule PAUSED)
- **Databricks App**: `live-fraud-queue-dev`

### Step 8: Run the Workflow

Trigger the full pipeline manually:

```bash
/opt/homebrew/bin/databricks bundle run -t dev fraud_triage_workflow
```

This runs: DLT refresh → Delta-to-Lakebase sync → Reverse sync of analyst decisions.

### Step 9: Access the App

After deployment, the app URL will be displayed in the Databricks workspace under **Apps**. The Live Fraud Queue provides:

- **Dashboard**: Blocked/Pending/Allowed counts, amount at risk, avg decision latency
- **Queue View**: Filter by YELLOW, RED, or ALL flagged transactions; sort by risk score or amount
- **Analyst Actions**: BLOCK, RELEASE, or ESCALATE each transaction with notes
- **Detail Modal**: Full transaction enrichment, user profile, and risk factor breakdown
- **KPIs**: 30-day fraud detection rate, false positive ratio, pattern analysis

### Step 10: Deploy to Production

```bash
/opt/homebrew/bin/databricks bundle deploy -t prod
```

Production differences: workflow schedule is UNPAUSED (runs every 4 hours), resource names drop the `[DEV]` prefix.

## Unity Catalog Schema Layout

| Schema | Tables | Purpose |
|--------|--------|---------|
| `fraud_detection` | `bronze_*`, `silver_enriched_transactions`, `silver_velocity_anomalies`, `real_time_fraud_triage` (DLT MV) | Raw ingestion, enrichment, risk scoring |
| `fraud_operations` | `gold_fraud_kpis`, `gold_account_takeover`, `gold_fraud_by_pattern`, `real_time_fraud_triage` (mutable) | KPIs and operational triage (app reads from here) |
| `fraud_investigation` | Genie Space tables | Conversational fraud investigation |

## API Endpoints

| Endpoint | Source | Description |
|----------|--------|-------------|
| `GET /api/stats` | Lakebase | Dashboard stats (blocked, pending, allowed, avg latency) |
| `GET /api/queue?filter=YELLOW&sort=risk_score&limit=50` | Lakebase | Fraud queue with filtering and pagination |
| `POST /api/decision` | Lakebase | Submit analyst decision (BLOCK/RELEASE/ESCALATE) |
| `GET /api/user/{user_id}` | Lakebase | User risk profile and transaction history |
| `GET /api/transaction/{txn_id}` | Lakebase + Warehouse | Full transaction detail with enrichment |
| `GET /api/kpis` | SQL Warehouse | 30-day fraud detection KPIs |
| `GET /api/patterns` | SQL Warehouse | Fraud statistics by attack pattern |

## Key Design Decisions

1. **Dual data source**: Lakebase for operational (< 50ms latency) vs SQL Warehouse for analytical (complex aggregations). The app routes queries to the appropriate backend.
2. **ON CONFLICT DO NOTHING**: The sync preserves analyst decisions already in Lakebase — only new transactions are inserted.
3. **Reverse sync**: Analyst decisions flow back to Delta so Genie Space (conversational BI) always shows current state.
4. **DLT materialized view**: The `real_time_fraud_triage` table in `fraud_detection` schema is a read-only DLT MV. A separate mutable copy in `fraud_operations` receives merged analyst decisions.
5. **Embedded SPA**: The frontend is embedded in `app.py` to avoid a separate build/deploy step — ideal for internal tools.
