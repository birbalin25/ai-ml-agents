"""Set up PII masking and ABAC policies for fraud triage tables."""
import subprocess
import json

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
            print("    -> OK")
        else:
            error = data.get("status", {}).get("error", {}).get("message", "Unknown")
            print(f"    -> {state}: {error[:200]}")
        return data
    except:
        print(f"    -> Error: {result.stderr[:200]}")
        return None


print("Setting up PII Masking Policies")
print("=" * 50)

# Create masking function for card numbers
run_sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.fraud_detection.mask_card_number(card_number STRING)
RETURNS STRING
RETURN CONCAT('****-****-****-', RIGHT(card_number, 4))
""", "Creating card number masking function")

# Create masking function for email
run_sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.fraud_detection.mask_email(email STRING)
RETURNS STRING
RETURN CONCAT(LEFT(email, 2), '***@', SPLIT(email, '@')[1])
""", "Creating email masking function")

# Create masking function for phone
run_sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.fraud_detection.mask_phone(phone STRING)
RETURNS STRING
RETURN CONCAT('+1***', RIGHT(phone, 4))
""", "Creating phone masking function")

# Create masking function for IP addresses
run_sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.fraud_detection.mask_ip(ip_address STRING)
RETURNS STRING
RETURN CONCAT(SPLIT(ip_address, '\\\\.')[0], '.xxx.xxx.', SPLIT(ip_address, '\\\\.')[3])
""", "Creating IP masking function")

# Apply column masks to user_profiles
run_sql(f"""
ALTER TABLE {CATALOG}.fraud_detection.user_profiles
ALTER COLUMN card_number SET MASK {CATALOG}.fraud_detection.mask_card_number
""", "Applying card_number mask to user_profiles")

run_sql(f"""
ALTER TABLE {CATALOG}.fraud_detection.user_profiles
ALTER COLUMN email SET MASK {CATALOG}.fraud_detection.mask_email
""", "Applying email mask to user_profiles")

run_sql(f"""
ALTER TABLE {CATALOG}.fraud_detection.user_profiles
ALTER COLUMN phone SET MASK {CATALOG}.fraud_detection.mask_phone
""", "Applying phone mask to user_profiles")

print("\nPII Masking setup complete!")
