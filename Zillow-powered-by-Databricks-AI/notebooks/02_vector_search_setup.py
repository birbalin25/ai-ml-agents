# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Vector Search Index Setup
# MAGIC Creates a Vector Search endpoint and a Delta Sync index with managed embeddings.

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

CATALOG = "bircatalog"
SCHEMA = "zillow"
TABLE = "properties"
VS_ENDPOINT = "bir_zillow_vs_endpoint"
VS_INDEX = f"{CATALOG}.{SCHEMA}.properties_vs_index"
EMBEDDING_MODEL = "databricks-bge-large-en"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

vsc = VectorSearchClient()

# Create endpoint (no-op if already exists)
try:
    vsc.get_endpoint(VS_ENDPOINT)
    print(f"Endpoint '{VS_ENDPOINT}' already exists.")
except Exception:
    print(f"Creating endpoint '{VS_ENDPOINT}'...")
    vsc.create_endpoint(name=VS_ENDPOINT, endpoint_type="STANDARD")
    print("Endpoint creation initiated. Waiting for it to become ONLINE...")

# COMMAND ----------

import time

# Wait for endpoint to be ready
for _ in range(60):
    ep = vsc.get_endpoint(VS_ENDPOINT)
    status = ep.get("endpoint_status", {}).get("state", "UNKNOWN")
    if status == "ONLINE":
        print(f"Endpoint '{VS_ENDPOINT}' is ONLINE.")
        break
    print(f"  Endpoint status: {status} — waiting 30s...")
    time.sleep(30)
else:
    raise TimeoutError("Endpoint did not become ONLINE within 30 minutes.")

# COMMAND ----------

# Create Delta Sync index with managed embeddings
try:
    vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)
    print(f"Index '{VS_INDEX}' already exists.")
except Exception:
    print(f"Creating Delta Sync index '{VS_INDEX}'...")
    vsc.create_delta_sync_index(
        endpoint_name=VS_ENDPOINT,
        index_name=VS_INDEX,
        source_table_name=SOURCE_TABLE,
        primary_key="id",
        pipeline_type="TRIGGERED",
        embedding_source_column="text_for_embedding",
        embedding_model_endpoint_name=EMBEDDING_MODEL,
    )
    print("Index creation initiated.")

# COMMAND ----------

# Wait for index to be ready
for _ in range(60):
    idx = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)
    status = idx.describe().get("status", {}).get("ready", False)
    if status:
        print(f"Index '{VS_INDEX}' is ONLINE and ready.")
        break
    print(f"  Index not ready yet — waiting 30s...")
    time.sleep(30)
else:
    raise TimeoutError("Index did not become ready within 30 minutes.")

# COMMAND ----------

# Test similarity search
results = idx.similarity_search(
    query_text="modern condo with rooftop deck in downtown Seattle",
    columns=["id", "address", "city", "price", "beds", "baths", "sqft", "description"],
    num_results=3,
)

print("Test query: 'modern condo with rooftop deck in downtown Seattle'")
for row in results.get("result", {}).get("data_array", []):
    print(f"  {row}")
