"""Create a Genie Space for Fraud Investigation using REST API."""
import subprocess
import json

PROFILE = "vm2"
CATALOG = "serverless_stable_p2uvy4_catalog"
WAREHOUSE_ID = "8620a950b7475da4"

# Build the API payload
payload = {
    "warehouse_id": WAREHOUSE_ID,
    "title": "Fraud Triage Investigator",
    "description": "Conversational investigation space for fraud analysts. Ask questions about transactions, login patterns, risk scores, and fraud KPIs.",
    "table_identifiers": [
        f"{CATALOG}.fraud_detection.silver_enriched_transactions",
        f"{CATALOG}.fraud_detection.silver_velocity_anomalies",
        f"{CATALOG}.fraud_operations.gold_fraud_kpis",
        f"{CATALOG}.fraud_operations.gold_account_takeover",
        f"{CATALOG}.fraud_operations.gold_fraud_by_pattern",
        f"{CATALOG}.fraud_operations.real_time_fraud_triage",
        f"{CATALOG}.fraud_detection.user_profiles",
    ],
}

# Use the REST API directly
cmd = [
    "/opt/homebrew/bin/databricks", "api", "post",
    "/api/2.0/genie/spaces",
    "--profile", PROFILE,
    "--json", json.dumps(payload)
]

print("Creating Genie Space via REST API...")
result = subprocess.run(cmd, capture_output=True, text=True)
print(f"stdout: {result.stdout[:1000]}")
if result.stderr:
    print(f"stderr: {result.stderr[:500]}")

try:
    data = json.loads(result.stdout)
    space_id = data.get("space_id", data.get("id", "unknown"))
    print(f"\nGenie Space created! ID: {space_id}")
except:
    print("\nTrying alternate approach with serialized_space...")

    # Try with serialized_space as a JSON string
    space_data = {
        "table_identifiers": [
            f"{CATALOG}.fraud_detection.silver_enriched_transactions",
            f"{CATALOG}.fraud_detection.silver_velocity_anomalies",
            f"{CATALOG}.fraud_operations.gold_fraud_kpis",
            f"{CATALOG}.fraud_operations.gold_account_takeover",
            f"{CATALOG}.fraud_operations.gold_fraud_by_pattern",
            f"{CATALOG}.fraud_operations.real_time_fraud_triage",
            f"{CATALOG}.fraud_detection.user_profiles",
        ],
    }

    payload2 = {
        "warehouse_id": WAREHOUSE_ID,
        "title": "Fraud Triage Investigator",
        "description": "Conversational investigation space for fraud analysts.",
        "serialized_space": json.dumps(space_data)
    }

    cmd2 = [
        "/opt/homebrew/bin/databricks", "api", "post",
        "/api/2.0/genie/spaces",
        "--profile", PROFILE,
        "--json", json.dumps(payload2)
    ]
    result2 = subprocess.run(cmd2, capture_output=True, text=True)
    print(f"stdout: {result2.stdout[:1000]}")
    if result2.stderr:
        print(f"stderr: {result2.stderr[:500]}")

    try:
        data2 = json.loads(result2.stdout)
        space_id = data2.get("space_id", data2.get("id", "unknown"))
        print(f"\nGenie Space created! ID: {space_id}")
    except:
        print("Could not create via API. Will provide manual instructions.")
