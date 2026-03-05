"""Create Delta tables from CSV files in Unity Catalog Volume."""
import subprocess
import json
import sys
import time

PROFILE = "vm2"
WAREHOUSE = "8620a950b7475da4"
CATALOG = "serverless_stable_p2uvy4_catalog"


def run_sql(sql, description=""):
    print(f">>> {description}")
    payload = json.dumps({
        "warehouse_id": WAREHOUSE,
        "statement": sql,
        "wait_timeout": "50s"
    })
    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements", "--profile", PROFILE, "--json", payload],
        capture_output=True, text=True
    )
    try:
        data = json.loads(result.stdout)
        state = data.get("status", {}).get("state", "UNKNOWN")
        if state == "SUCCEEDED":
            rows = data.get("result", {}).get("data_array", [])
            total = data.get("manifest", {}).get("total_row_count", "n/a")
            print(f"    -> OK (total_rows: {total})")
            if rows:
                for row in rows[:10]:
                    print(f"       {row}")
            return data
        elif state == "PENDING" or state == "RUNNING":
            stmt_id = data.get("statement_id")
            print(f"    -> {state}, polling statement {stmt_id}...")
            for _ in range(30):
                time.sleep(5)
                poll = subprocess.run(
                    ["databricks", "api", "get", f"/api/2.0/sql/statements/{stmt_id}", "--profile", PROFILE],
                    capture_output=True, text=True
                )
                poll_data = json.loads(poll.stdout)
                poll_state = poll_data.get("status", {}).get("state", "UNKNOWN")
                if poll_state == "SUCCEEDED":
                    rows = poll_data.get("result", {}).get("data_array", [])
                    total = poll_data.get("manifest", {}).get("total_row_count", "n/a")
                    print(f"    -> OK (total_rows: {total})")
                    if rows:
                        for row in rows[:10]:
                            print(f"       {row}")
                    return poll_data
                elif poll_state in ("FAILED", "CANCELED", "CLOSED"):
                    error = poll_data.get("status", {}).get("error", {}).get("message", "Unknown error")
                    print(f"    -> FAILED: {error[:300]}")
                    return poll_data
                print(f"    ... still {poll_state}")
            print("    -> TIMEOUT waiting for statement")
            return None
        else:
            error = data.get("status", {}).get("error", {}).get("message", "Unknown error")
            print(f"    -> {state}: {error[:300]}")
            return data
    except Exception as e:
        print(f"    -> Error parsing response: {e}")
        print(f"       stdout: {result.stdout[:500]}")
        print(f"       stderr: {result.stderr[:500]}")
        return None


print("=" * 60)
print("Creating base Delta tables from Volume CSVs")
print("=" * 60)

# User Profiles
run_sql(
    f"""CREATE OR REPLACE TABLE {CATALOG}.fraud_detection.user_profiles
    AS SELECT * FROM read_files(
      '/Volumes/{CATALOG}/fraud_detection/source_files/user_profiles.csv',
      format => 'csv', header => true, inferSchema => true
    )""",
    "Creating user_profiles table"
)

# Transactions
run_sql(
    f"""CREATE OR REPLACE TABLE {CATALOG}.fraud_detection.transactions
    AS SELECT * FROM read_files(
      '/Volumes/{CATALOG}/fraud_detection/source_files/transactions.csv',
      format => 'csv', header => true, inferSchema => true
    )""",
    "Creating transactions table"
)

# Login Logs
run_sql(
    f"""CREATE OR REPLACE TABLE {CATALOG}.fraud_detection.login_logs
    AS SELECT * FROM read_files(
      '/Volumes/{CATALOG}/fraud_detection/source_files/login_logs.csv',
      format => 'csv', header => true, inferSchema => true
    )""",
    "Creating login_logs table"
)

# Known Fraud Signatures
run_sql(
    f"""CREATE OR REPLACE TABLE {CATALOG}.fraud_detection.known_fraud_signatures
    AS SELECT * FROM read_files(
      '/Volumes/{CATALOG}/fraud_detection/source_files/known_fraud_signatures.csv',
      format => 'csv', header => true, inferSchema => true
    )""",
    "Creating known_fraud_signatures table"
)

print("\n" + "=" * 60)
print("Verifying row counts")
print("=" * 60)

run_sql(
    f"""SELECT 'user_profiles' as tbl, count(*) as cnt FROM {CATALOG}.fraud_detection.user_profiles
    UNION ALL SELECT 'transactions', count(*) FROM {CATALOG}.fraud_detection.transactions
    UNION ALL SELECT 'login_logs', count(*) FROM {CATALOG}.fraud_detection.login_logs
    UNION ALL SELECT 'known_fraud_signatures', count(*) FROM {CATALOG}.fraud_detection.known_fraud_signatures""",
    "Row counts for all tables"
)

print("\nDone!")
