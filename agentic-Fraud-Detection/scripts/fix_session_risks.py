import subprocess, json
PROFILE = "vm2"
WAREHOUSE = "8620a950b7475da4"
CATALOG = "serverless_stable_p2uvy4_catalog"

def run_sql(sql, desc=""):
    print(f">>> {desc}")
    payload = json.dumps({"warehouse_id": WAREHOUSE, "statement": sql, "wait_timeout": "50s"})
    r = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "--profile", PROFILE, "--json", payload], capture_output=True, text=True)
    d = json.loads(r.stdout)
    state = d.get("status",{}).get("state","?")
    if state == "SUCCEEDED":
        print(f"    -> OK")
    else:
        print(f"    -> {state}: {d.get('status',{}).get('error',{}).get('message','')[:200]}")

run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.fraud_operations.active_session_risks (
    session_id STRING NOT NULL,
    user_id STRING NOT NULL,
    current_risk INT,
    ip_address STRING,
    geo_location STRING,
    last_activity TIMESTAMP,
    is_blocked BOOLEAN
)
USING DELTA
CLUSTER BY (user_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.autoOptimize.optimizeWrite' = 'true')
""", "Creating active_session_risks (fixed clustering)")
