# Databricks notebook source
# MAGIC %md
# MAGIC # Admin Observability — Setup Lakebase Instance
# MAGIC
# MAGIC Creates the Lakebase (provisioned) database instance if it doesn't exist.
# MAGIC Waits for the instance to become ACTIVE before completing.
# MAGIC
# MAGIC **Idempotent**: skips creation if instance already exists.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import requests
import time

w = WorkspaceClient()
host = w.config.host.rstrip("/")
headers = w.config.authenticate()
headers["Content-Type"] = "application/json"

LAKEBASE_INSTANCE = dbutils.widgets.get("lakebase_instance")
LAKEBASE_DATABASE = dbutils.widgets.get("lakebase_database")
LAKEBASE_CAPACITY = dbutils.widgets.get("lakebase_capacity")

print(f"Instance: {LAKEBASE_INSTANCE}")
print(f"Database: {LAKEBASE_DATABASE}")
print(f"Capacity: {LAKEBASE_CAPACITY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Check if instance already exists

# COMMAND ----------

resp = requests.get(
    f"{host}/api/2.0/database/instances/{LAKEBASE_INSTANCE}",
    headers=headers,
    timeout=60,
)

instance_exists = resp.status_code == 200
if instance_exists:
    state = resp.json().get("state", "UNKNOWN")
    pg_host = resp.json().get("pg_host", "")
    print(f"Instance '{LAKEBASE_INSTANCE}' already exists (state={state}, host={pg_host})")
else:
    print(f"Instance '{LAKEBASE_INSTANCE}' does not exist. Creating...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create instance if needed

# COMMAND ----------

if not instance_exists:
    payload = {
        "name": LAKEBASE_INSTANCE,
        "capacity": LAKEBASE_CAPACITY,
    }
    resp = requests.post(
        f"{host}/api/2.0/database/instances",
        headers=headers,
        json=payload,
        timeout=120,
    )
    if resp.status_code in (200, 201):
        print(f"Instance creation initiated: {resp.json().get('state', 'UNKNOWN')}")
    elif resp.status_code == 409 or "ALREADY_EXISTS" in resp.text:
        print("Instance already exists (race condition). Continuing...")
        instance_exists = True
    else:
        raise Exception(f"Failed to create instance: {resp.status_code} - {resp.text[:500]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Wait for instance to become ACTIVE

# COMMAND ----------

MAX_WAIT_SECONDS = 600
POLL_INTERVAL = 15
elapsed = 0

while elapsed < MAX_WAIT_SECONDS:
    # Re-authenticate in case token expired during wait
    headers = w.config.authenticate()
    headers["Content-Type"] = "application/json"

    resp = requests.get(
        f"{host}/api/2.0/database/instances/{LAKEBASE_INSTANCE}",
        headers=headers,
        timeout=60,
    )
    if resp.status_code != 200:
        raise Exception(f"Failed to get instance status: {resp.status_code} - {resp.text[:500]}")

    data = resp.json()
    state = data.get("state", "UNKNOWN")
    pg_host = data.get("pg_host", "")

    print(f"  [{elapsed}s] Instance state: {state}")

    if state == "ACTIVE":
        print(f"\nInstance is ACTIVE!")
        print(f"  Host: {pg_host}")
        print(f"  Database: {LAKEBASE_DATABASE}")
        break
    elif state in ("FAILED", "DELETED"):
        raise Exception(f"Instance entered terminal state: {state}")

    time.sleep(POLL_INTERVAL)
    elapsed += POLL_INTERVAL
else:
    raise Exception(f"Instance did not become ACTIVE within {MAX_WAIT_SECONDS}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary

# COMMAND ----------

print("=" * 70)
print("LAKEBASE INSTANCE SETUP COMPLETE")
print("=" * 70)
print(f"  Instance: {LAKEBASE_INSTANCE}")
print(f"  Host:     {pg_host}")
print(f"  Database: {LAKEBASE_DATABASE}")
print(f"  State:    ACTIVE")
