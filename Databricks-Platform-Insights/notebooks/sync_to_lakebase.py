# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Platform Insights — Sync Materialized Views to Lakebase
# MAGIC
# MAGIC Creates synced tables in Lakebase (Snapshot mode) for all 11 MVs.
# MAGIC Uses the Database Synced Tables REST API (`/api/2.0/database/synced_tables`).
# MAGIC
# MAGIC **Idempotent**: existing synced tables are skipped.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import requests

w = WorkspaceClient()

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
LAKEBASE_INSTANCE = dbutils.widgets.get("lakebase_instance")
LAKEBASE_DATABASE = dbutils.widgets.get("lakebase_database")

print(f"Catalog:   {CATALOG}")
print(f"Schema:    {SCHEMA}")
print(f"Instance:  {LAKEBASE_INSTANCE}")
print(f"Database:  {LAKEBASE_DATABASE}")

# COMMAND ----------

# Each MV with its primary key columns
mv_specs = [
    {"name": "mv_billing_daily_by_sku", "pk": ["usage_date", "workspace_id", "sku_name"]},
    {"name": "mv_billing_daily_by_user", "pk": ["usage_date", "workspace_id", "sku_name", "user_identity", "is_job"]},
    {"name": "mv_cluster_details", "pk": ["cluster_id", "workspace_id"]},
    {"name": "mv_job_run_timeline", "pk": ["job_id", "run_id", "workspace_id"]},
    {"name": "mv_query_history_daily", "pk": ["query_date", "workspace_id", "warehouse_id"]},
    {"name": "mv_query_history_by_user", "pk": ["query_date", "workspace_id", "executed_by"]},
    {"name": "mv_query_runtime_distribution", "pk": ["query_date", "workspace_id", "duration_bucket"]},
    {"name": "mv_warehouse_concurrency", "pk": ["query_date", "query_hour", "workspace_id", "warehouse_id"]},
    {"name": "mv_long_running_queries", "pk": ["statement_id", "workspace_id"]},
    {"name": "mv_serving_endpoints", "pk": ["endpoint_name", "model_type", "workspace_id"]},
    {"name": "mv_experiment_runs", "pk": ["experiment_id", "run_id", "workspace_id"]},
]

# COMMAND ----------

host = w.config.host.rstrip("/")
headers = w.config.authenticate()
headers["Content-Type"] = "application/json"

results = []

for spec in mv_specs:
    mv = spec["name"]
    synced_name = f"{CATALOG}.{SCHEMA}.synced_{mv}"
    source_name = f"{CATALOG}.{SCHEMA}.{mv}"
    print(f"Syncing {mv} ...")

    payload = {
        "name": synced_name,
        "spec": {
            "source_table_full_name": source_name,
            "primary_key_columns": spec["pk"],
            "scheduling_policy": "SNAPSHOT",
        },
        "database_instance_name": LAKEBASE_INSTANCE,
        "logical_database_name": LAKEBASE_DATABASE,
    }

    try:
        resp = requests.post(
            f"{host}/api/2.0/database/synced_tables",
            headers=headers,
            json=payload,
            timeout=120,
        )

        if resp.status_code == 200:
            data = resp.json()
            state = data.get("data_synchronization_status", {}).get("detailed_state", "UNKNOWN")
            results.append({"mv": mv, "status": "OK", "state": state})
            print(f"  -> OK ({state})")
        elif resp.status_code == 409 or "ALREADY_EXISTS" in resp.text:
            results.append({"mv": mv, "status": "OK", "state": "ALREADY_EXISTS"})
            print(f"  -> OK (already exists)")
        else:
            error_msg = resp.text[:500]
            results.append({"mv": mv, "status": "FAILED", "error": error_msg})
            print(f"  -> FAILED ({resp.status_code}): {error_msg}")

    except Exception as e:
        results.append({"mv": mv, "status": "FAILED", "error": str(e)})
        print(f"  -> FAILED: {e}")

# COMMAND ----------

# Summary
print("\n" + "=" * 70)
print("LAKEBASE SYNC SUMMARY")
print("=" * 70)

failed = [r for r in results if r["status"] == "FAILED"]
succeeded = [r for r in results if r["status"] == "OK"]

for r in results:
    icon = "OK" if r["status"] == "OK" else "FAIL"
    extra = ""
    if r["status"] == "FAILED":
        extra = f" - {r.get('error', '')}"
    elif r.get("state"):
        extra = f" ({r['state']})"
    print(f"  [{icon}] {r['mv']}{extra}")

print(f"\n  {len(succeeded)}/{len(results)} succeeded, {len(failed)} failed")

if failed:
    failed_names = ", ".join(r["mv"] for r in failed)
    raise Exception(f"Lakebase sync failed for: {failed_names}")
