"""
Create a Databricks Workflow (Job) that orchestrates:
  Task 1: Run the DLT fraud pipeline
  Task 2: Sync Delta triage table to Lakebase (depends on Task 1)
  Task 3: Sync analyst decisions from Lakebase back to Delta (depends on Task 2)

Schedule: every 4 hours (paused initially), max 1 concurrent run.

Usage:
    python scripts/create_workflow.py
"""
import subprocess
import json
import sys

PROFILE = "vm"
CATALOG = "serverless_bir_catalog"
DLT_PIPELINE_ID = "9224bb4f-6926-4ff9-b195-e61f1cbaeae2"
WS_PATH = "/Users/birbal.das@databricks.com/fraud_triage_agent"
SYNC_NOTEBOOK = f"{WS_PATH}/06_lakebase_sync"
REVERSE_SYNC_NOTEBOOK = f"{WS_PATH}/07_sync_decisions_to_delta"


def run_cli(args, description=""):
    print(f"\n>>> {description}")
    result = subprocess.run(args, capture_output=True, text=True, timeout=60)
    if result.returncode == 0:
        print(f"    -> OK")
        return json.loads(result.stdout) if result.stdout.strip() else {}
    else:
        print(f"    -> Error: {result.stderr[:300]}")
        return None


def main():
    job_config = {
        "name": "Fraud Triage Agent - Pipeline + Lakebase Sync",
        "max_concurrent_runs": 1,
        "schedule": {
            "quartz_cron_expression": "0 0 */4 * * ?",
            "timezone_id": "UTC",
            "pause_status": "PAUSED",
        },
        "tags": {
            "project": "fraud-triage-agent",
            "owner": "birbal.das",
        },
        "tasks": [
            {
                "task_key": "run_dlt_pipeline",
                "description": "Run the DLT fraud detection pipeline (Bronze → Silver → Gold → Triage)",
                "pipeline_task": {
                    "pipeline_id": DLT_PIPELINE_ID,
                    "full_refresh": False,
                },
                "timeout_seconds": 1800,
            },
            {
                "task_key": "sync_to_lakebase",
                "description": "Sync DLT triage Delta table to Lakebase Postgres (ON CONFLICT DO NOTHING)",
                "depends_on": [{"task_key": "run_dlt_pipeline"}],
                "notebook_task": {
                    "notebook_path": SYNC_NOTEBOOK,
                    "base_parameters": {
                        "catalog": CATALOG,
                    },
                    "source": "WORKSPACE",
                },
                "environment_key": "Default",
                "timeout_seconds": 900,
            },
            {
                "task_key": "sync_decisions_to_delta",
                "description": "Reverse sync analyst decisions from Lakebase back to Delta for Genie Space",
                "depends_on": [{"task_key": "sync_to_lakebase"}],
                "notebook_task": {
                    "notebook_path": REVERSE_SYNC_NOTEBOOK,
                    "base_parameters": {
                        "catalog": CATALOG,
                    },
                    "source": "WORKSPACE",
                },
                "environment_key": "Default",
                "timeout_seconds": 900,
            },
        ],
        "environments": [
            {
                "environment_key": "Default",
                "spec": {
                    "client": "1",
                    "dependencies": ["psycopg2-binary"],
                },
            }
        ],
    }

    print("=" * 60)
    print("Creating Fraud Triage Workflow")
    print("=" * 60)
    print(f"DLT Pipeline:         {DLT_PIPELINE_ID}")
    print(f"Lakebase Sync:        {SYNC_NOTEBOOK}")
    print(f"Reverse Sync (Delta): {REVERSE_SYNC_NOTEBOOK}")

    result = run_cli(
        ["databricks", "jobs", "create", "--profile", PROFILE,
         "--json", json.dumps(job_config)],
        "Creating workflow job",
    )

    if result and "job_id" in result:
        job_id = result["job_id"]
        print(f"\n{'=' * 60}")
        print(f"Workflow created successfully!")
        print(f"  Job ID:   {job_id}")
        print(f"  Schedule: Every 4 hours (PAUSED)")
        print(f"  Tasks:")
        print(f"    1. run_dlt_pipeline         → DLT pipeline {DLT_PIPELINE_ID}")
        print(f"    2. sync_to_lakebase         → {SYNC_NOTEBOOK} (depends on task 1)")
        print(f"    3. sync_decisions_to_delta  → {REVERSE_SYNC_NOTEBOOK} (depends on task 2)")
        print(f"\nTo run manually:")
        print(f"  databricks jobs run-now {job_id} --profile {PROFILE}")
        print(f"\nTo unpause schedule:")
        print(f"  databricks jobs update {job_id} --profile {PROFILE} --json '{{\"schedule\": {{\"pause_status\": \"UNPAUSED\"}}}}'")
        print(f"{'=' * 60}")
    else:
        print("\nFailed to create workflow. Check errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
