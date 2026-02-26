# Zillow Re-imagined with AI — Shared Configuration

# Unity Catalog
CATALOG = "bircatalog"
SCHEMA = "zillow"
TABLE = "properties"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE}"

# Vector Search
VS_ENDPOINT_NAME = "bir_zillow_vs_endpoint"
VS_INDEX_NAME = f"{CATALOG}.{SCHEMA}.properties_vs_index"
EMBEDDING_MODEL = "databricks-bge-large-en"

# Foundation Model API
LLM_MODEL = "databricks-meta-llama-3-1-70b-instruct"

# SQL Warehouse
import os
SQL_WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "148ccb90800933a1")

# MLflow / Model Serving
REGISTERED_MODEL_NAME = f"{CATALOG}.{SCHEMA}.price_prediction_model"
MODEL_ALIAS = "production"

# Data Generation
NUM_LISTINGS = 1000
RANDOM_SEED = 42

# Metro areas with neighborhoods
METROS = {
    "San Francisco": {
        "state": "CA", "zip_prefix": "941", "lat": 37.77, "lon": -122.42,
        "neighborhoods": ["Mission District", "SoMa", "Pacific Heights", "Castro", "Richmond"],
        "price_base": 1_200_000,
    },
    "Seattle": {
        "state": "WA", "zip_prefix": "981", "lat": 47.61, "lon": -122.33,
        "neighborhoods": ["Capitol Hill", "Ballard", "Fremont", "Queen Anne", "Wallingford"],
        "price_base": 850_000,
    },
    "Austin": {
        "state": "TX", "zip_prefix": "787", "lat": 30.27, "lon": -97.74,
        "neighborhoods": ["Downtown", "East Austin", "South Lamar", "Mueller", "Zilker"],
        "price_base": 550_000,
    },
    "Denver": {
        "state": "CO", "zip_prefix": "802", "lat": 39.74, "lon": -104.99,
        "neighborhoods": ["LoDo", "RiNo", "Capitol Hill", "Cherry Creek", "Highlands"],
        "price_base": 620_000,
    },
    "New York": {
        "state": "NY", "zip_prefix": "100", "lat": 40.71, "lon": -74.01,
        "neighborhoods": ["Upper West Side", "Williamsburg", "Astoria", "Park Slope", "Harlem"],
        "price_base": 1_500_000,
    },
    "Chicago": {
        "state": "IL", "zip_prefix": "606", "lat": 41.88, "lon": -87.63,
        "neighborhoods": ["Lincoln Park", "Wicker Park", "Logan Square", "Hyde Park", "Lakeview"],
        "price_base": 450_000,
    },
    "Miami": {
        "state": "FL", "zip_prefix": "331", "lat": 25.76, "lon": -80.19,
        "neighborhoods": ["Brickell", "Wynwood", "Coconut Grove", "Coral Gables", "Little Havana"],
        "price_base": 680_000,
    },
    "Nashville": {
        "state": "TN", "zip_prefix": "372", "lat": 36.16, "lon": -86.78,
        "neighborhoods": ["East Nashville", "The Gulch", "Germantown", "12 South", "Sylvan Park"],
        "price_base": 520_000,
    },
    "Portland": {
        "state": "OR", "zip_prefix": "972", "lat": 45.52, "lon": -122.68,
        "neighborhoods": ["Pearl District", "Alberta Arts", "Hawthorne", "Division", "St. Johns"],
        "price_base": 580_000,
    },
    "Boston": {
        "state": "MA", "zip_prefix": "021", "lat": 42.36, "lon": -71.06,
        "neighborhoods": ["Back Bay", "South End", "Cambridge", "Somerville", "Jamaica Plain"],
        "price_base": 950_000,
    },
}

PROPERTY_TYPES = ["Single Family", "Condo", "Townhouse", "Multi-Family"]
LISTING_STATUSES = ["For Sale", "Pending", "Recently Sold"]
