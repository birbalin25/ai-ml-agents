"""Grant the app service principal permissions to access fraud tables."""
import subprocess
import json

PROFILE = "vm"
WAREHOUSE = "19be9738b181575a"
CATALOG = "serverless_bir_catalog"
# Use the service principal's applicationId (UUID format)
SP_APP_ID = "32fc6cfe-33be-41f9-885a-adaa4bd63616"


def run_sql(sql, description=""):
    print(f">>> {description}")
    payload = json.dumps({
        "warehouse_id": WAREHOUSE,
        "statement": sql,
        "wait_timeout": "50s"
    })
    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "--profile", PROFILE, "--json", payload],
        capture_output=True, text=True
    )
    try:
        data = json.loads(result.stdout)
        state = data.get("status", {}).get("state", "UNKNOWN")
        if state == "SUCCEEDED":
            print(f"    -> OK")
        else:
            error = data.get("status", {}).get("error", {}).get("message", "")
            print(f"    -> {state}: {error[:300]}")
    except Exception as e:
        print(f"    -> Error: {e}")
    print()


print("Granting app service principal access to fraud tables...")
print(f"Service Principal App ID: {SP_APP_ID}")
print("=" * 60)

# Grant USE CATALOG
run_sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{SP_APP_ID}`",
        "Grant USE CATALOG")

# Grant USE SCHEMA on all fraud schemas
for schema in ["fraud_detection", "fraud_investigation", "fraud_operations"]:
    run_sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{schema} TO `{SP_APP_ID}`",
            f"Grant USE SCHEMA on {schema}")

# Grant SELECT on all tables in fraud_detection
run_sql(f"GRANT SELECT ON SCHEMA {CATALOG}.fraud_detection TO `{SP_APP_ID}`",
        "Grant SELECT on fraud_detection schema")

# Grant SELECT + MODIFY on fraud_operations (for analyst decisions)
run_sql(f"GRANT SELECT, MODIFY ON SCHEMA {CATALOG}.fraud_operations TO `{SP_APP_ID}`",
        "Grant SELECT, MODIFY on fraud_operations schema")

print("Done!")
