#!/bin/bash
# Helper to run SQL statements against Databricks
PROFILE="vm2"
WAREHOUSE="8620a950b7475da4"

run_sql() {
    local sql="$1"
    echo ">>> $sql"
    result=$(databricks api post /api/2.0/sql/statements --profile=$PROFILE --json "{
        \"warehouse_id\": \"$WAREHOUSE\",
        \"statement\": \"$sql\",
        \"wait_timeout\": \"30s\"
    }" 2>&1)
    state=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status',{}).get('state','UNKNOWN'))" 2>/dev/null)
    if [ "$state" = "SUCCEEDED" ]; then
        echo "    -> OK"
    else
        error=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status',{}).get('error',{}).get('message','Unknown error'))" 2>/dev/null)
        echo "    -> $state: $error"
    fi
    echo ""
}

CATALOG="serverless_stable_p2uvy4_catalog"

run_sql "CREATE SCHEMA IF NOT EXISTS $CATALOG.fraud_detection COMMENT 'Raw and enriched fraud detection data'"
run_sql "CREATE SCHEMA IF NOT EXISTS $CATALOG.fraud_investigation COMMENT 'Investigation tools and Genie Space data'"
run_sql "CREATE SCHEMA IF NOT EXISTS $CATALOG.fraud_operations COMMENT 'Operational data KPIs and model artifacts'"
run_sql "CREATE VOLUME IF NOT EXISTS $CATALOG.fraud_detection.source_files COMMENT 'Volume for mock banking source files'"
