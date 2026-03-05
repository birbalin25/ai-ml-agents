#!/bin/bash
PROFILE="vm2"
WAREHOUSE="8620a950b7475da4"
CATALOG="serverless_stable_p2uvy4_catalog"

run_sql() {
    local sql="$1"
    local desc="$2"
    echo ">>> $desc"
    result=$(databricks api post /api/2.0/sql/statements --profile=$PROFILE --json "{
        \"warehouse_id\": \"$WAREHOUSE\",
        \"statement\": $(python3 -c "import json; print(json.dumps('''$sql'''))"),
        \"wait_timeout\": \"60s\"
    }" 2>&1)
    state=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status',{}).get('state','UNKNOWN'))" 2>/dev/null)
    if [ "$state" = "SUCCEEDED" ]; then
        rows=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); r=d.get('manifest',{}).get('total_row_count','n/a'); print(r)" 2>/dev/null)
        echo "    -> OK (rows: $rows)"
    else
        error=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status',{}).get('error',{}).get('message','Unknown error')[:200])" 2>/dev/null)
        echo "    -> $state: $error"
    fi
    echo ""
}

echo "Creating base tables from Volume CSVs..."
echo "========================================="

# User Profiles table
run_sql "CREATE OR REPLACE TABLE $CATALOG.fraud_detection.user_profiles
AS SELECT * FROM read_files(
  '/Volumes/$CATALOG/fraud_detection/source_files/user_profiles.csv',
  format => 'csv',
  header => true,
  inferSchema => true
)" "Creating user_profiles table"

# Transactions table
run_sql "CREATE OR REPLACE TABLE $CATALOG.fraud_detection.transactions
AS SELECT * FROM read_files(
  '/Volumes/$CATALOG/fraud_detection/source_files/transactions.csv',
  format => 'csv',
  header => true,
  inferSchema => true
)" "Creating transactions table"

# Login Logs table
run_sql "CREATE OR REPLACE TABLE $CATALOG.fraud_detection.login_logs
AS SELECT * FROM read_files(
  '/Volumes/$CATALOG/fraud_detection/source_files/login_logs.csv',
  format => 'csv',
  header => true,
  inferSchema => true
)" "Creating login_logs table"

# Known Fraud Signatures table
run_sql "CREATE OR REPLACE TABLE $CATALOG.fraud_detection.known_fraud_signatures
AS SELECT * FROM read_files(
  '/Volumes/$CATALOG/fraud_detection/source_files/known_fraud_signatures.csv',
  format => 'csv',
  header => true,
  inferSchema => true
)" "Creating known_fraud_signatures table"

echo "Verifying row counts..."
echo "======================="

run_sql "SELECT 'user_profiles' as tbl, count(*) as cnt FROM $CATALOG.fraud_detection.user_profiles
UNION ALL SELECT 'transactions', count(*) FROM $CATALOG.fraud_detection.transactions
UNION ALL SELECT 'login_logs', count(*) FROM $CATALOG.fraud_detection.login_logs
UNION ALL SELECT 'known_fraud_signatures', count(*) FROM $CATALOG.fraud_detection.known_fraud_signatures" "Row counts"
