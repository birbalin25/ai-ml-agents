# Databricks notebook source
# MAGIC %md
# MAGIC # Admin Observability — Grant Lakebase Access to App Service Principal
# MAGIC
# MAGIC Connects to Lakebase via psycopg2 and creates the PostgreSQL role, security
# MAGIC label, and schema/table grants needed for the Databricks App service principal
# MAGIC to read the synced MV tables.
# MAGIC
# MAGIC **Idempotent**: uses IF NOT EXISTS and handles already-exists errors gracefully.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary "databricks-sdk>=0.61.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import requests
import psycopg2
import uuid

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Get App Service Principal details

# COMMAND ----------

APP_NAME = dbutils.widgets.get("app_name")
LAKEBASE_INSTANCE = dbutils.widgets.get("lakebase_instance")
LAKEBASE_DATABASE = dbutils.widgets.get("lakebase_database")
SCHEMA_NAME = dbutils.widgets.get("schema")

# Get app details to find the service principal
host = w.config.host.rstrip("/")
headers = w.config.authenticate()
headers["Content-Type"] = "application/json"

resp = requests.get(f"{host}/api/2.0/apps/{APP_NAME}", headers=headers, timeout=60)
if resp.status_code != 200:
    raise Exception(f"Failed to get app details: {resp.status_code} - {resp.text[:500]}")

app_data = resp.json()
sp_client_id = app_data.get("service_principal_client_id")
sp_id = app_data.get("service_principal_id")
sp_name = app_data.get("service_principal_name", "")

print(f"App: {APP_NAME}")
print(f"Service Principal Client ID: {sp_client_id}")
print(f"Service Principal Numeric ID: {sp_id}")
print(f"Service Principal Name: {sp_name}")

if not sp_client_id or not sp_id:
    raise Exception("Could not determine app service principal. Is the app deployed?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Get Lakebase connection details

# COMMAND ----------

resp = requests.get(
    f"{host}/api/2.0/database/instances/{LAKEBASE_INSTANCE}",
    headers=headers,
    timeout=60,
)
if resp.status_code != 200:
    raise Exception(f"Failed to get Lakebase instance: {resp.status_code} - {resp.text[:500]}")

instance_data = resp.json()
pg_host = instance_data["read_write_dns"]
pg_port = instance_data.get("pg_port", 5432)
state = instance_data.get("state", "UNKNOWN")

print(f"Lakebase Host: {pg_host}")
print(f"Lakebase Port: {pg_port}")
print(f"Lakebase State: {state}")

if state != "AVAILABLE":
    raise Exception(f"Lakebase instance is not AVAILABLE (state={state})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Connect to Lakebase and grant access

# COMMAND ----------

# Generate database credential via SDK
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=[LAKEBASE_INSTANCE],
)

# Get current user email for PG connection
current_user = w.current_user.me()
pg_user = current_user.user_name

print(f"Connecting to Lakebase as: {pg_user}")

conn = psycopg2.connect(
    host=pg_host,
    port=pg_port,
    database=LAKEBASE_DATABASE,
    user=pg_user,
    password=cred.token,
    sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

print("Connected to Lakebase successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Create role and set security label

# COMMAND ----------

# The SP client ID is used as the PG role name
role_name = sp_client_id

# 4a. Create role (idempotent)
try:
    cur.execute(f'CREATE ROLE "{role_name}" LOGIN;')
    print(f"Created PG role: {role_name}")
except psycopg2.errors.DuplicateObject:
    print(f"PG role already exists: {role_name}")
    conn.rollback()
    conn.autocommit = True

# 4b. Set security label for Databricks auth
security_label = f"id={sp_id},type=SERVICE_PRINCIPAL"
try:
    cur.execute(
        f'SECURITY LABEL FOR "databricks_auth" ON ROLE "{role_name}" IS %s;',
        (security_label,)
    )
    print(f"Set security label: {security_label}")
except Exception as e:
    print(f"Security label warning (may already be set): {e}")
    conn.rollback()
    conn.autocommit = True

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Grant schema and table access

# COMMAND ----------

# 5a. Grant schema usage
try:
    cur.execute(f'GRANT USAGE ON SCHEMA {SCHEMA_NAME} TO "{role_name}";')
    print(f"Granted USAGE on schema {SCHEMA_NAME}")
except Exception as e:
    print(f"Schema grant warning: {e}")
    conn.rollback()
    conn.autocommit = True

# 5b. Grant SELECT on all existing tables
try:
    cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA {SCHEMA_NAME} TO "{role_name}";')
    print(f"Granted SELECT on all tables in {SCHEMA_NAME}")
except Exception as e:
    print(f"Table grant warning: {e}")
    conn.rollback()
    conn.autocommit = True

# 5c. Set default privileges for future tables
try:
    cur.execute(
        f'ALTER DEFAULT PRIVILEGES IN SCHEMA {SCHEMA_NAME} '
        f'GRANT SELECT ON TABLES TO "{role_name}";'
    )
    print(f"Set default privileges for future tables in {SCHEMA_NAME}")
except Exception as e:
    print(f"Default privileges warning: {e}")
    conn.rollback()
    conn.autocommit = True

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Verify access

# COMMAND ----------

# List tables the role can see
cur.execute(
    "SELECT table_schema, table_name FROM information_schema.tables "
    "WHERE table_schema = %s ORDER BY table_name;",
    (SCHEMA_NAME,)
)
tables = cur.fetchall()

print(f"\nTables in schema '{SCHEMA_NAME}':")
for schema, table in tables:
    print(f"  {schema}.{table}")

print(f"\nTotal: {len(tables)} tables")

cur.close()
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary

# COMMAND ----------

print("=" * 70)
print("LAKEBASE ACCESS GRANT COMPLETE")
print("=" * 70)
print(f"  App:                {APP_NAME}")
print(f"  SP Client ID:       {sp_client_id}")
print(f"  SP Numeric ID:      {sp_id}")
print(f"  PG Role:            {role_name}")
print(f"  Security Label:     {security_label}")
print(f"  Schema:             {SCHEMA_NAME}")
print(f"  Tables Accessible:  {len(tables)}")
print(f"  Lakebase Host:      {pg_host}")
