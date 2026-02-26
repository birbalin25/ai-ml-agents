# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Generate Synthetic Property Listings
# MAGIC Creates ~1,000 realistic property listings and writes them to a Delta table in Unity Catalog.

# COMMAND ----------

# MAGIC %pip install faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import random
import uuid
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, DoubleType, ArrayType,
)
import numpy as np

# COMMAND ----------

CATALOG = "bircatalog"
SCHEMA = "zillow"
TABLE = "properties"
NUM_LISTINGS = 1000
SEED = 42

spark = SparkSession.builder.getOrCreate()

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

fake = Faker()
Faker.seed(SEED)
random.seed(SEED)
np.random.seed(SEED)

METROS = {
    "San Francisco": {"state": "CA", "zip_prefix": "941", "lat": 37.77, "lon": -122.42,
        "neighborhoods": ["Mission District", "SoMa", "Pacific Heights", "Castro", "Richmond"],
        "price_base": 1_200_000},
    "Seattle": {"state": "WA", "zip_prefix": "981", "lat": 47.61, "lon": -122.33,
        "neighborhoods": ["Capitol Hill", "Ballard", "Fremont", "Queen Anne", "Wallingford"],
        "price_base": 850_000},
    "Austin": {"state": "TX", "zip_prefix": "787", "lat": 30.27, "lon": -97.74,
        "neighborhoods": ["Downtown", "East Austin", "South Lamar", "Mueller", "Zilker"],
        "price_base": 550_000},
    "Denver": {"state": "CO", "zip_prefix": "802", "lat": 39.74, "lon": -104.99,
        "neighborhoods": ["LoDo", "RiNo", "Capitol Hill", "Cherry Creek", "Highlands"],
        "price_base": 620_000},
    "New York": {"state": "NY", "zip_prefix": "100", "lat": 40.71, "lon": -74.01,
        "neighborhoods": ["Upper West Side", "Williamsburg", "Astoria", "Park Slope", "Harlem"],
        "price_base": 1_500_000},
    "Chicago": {"state": "IL", "zip_prefix": "606", "lat": 41.88, "lon": -87.63,
        "neighborhoods": ["Lincoln Park", "Wicker Park", "Logan Square", "Hyde Park", "Lakeview"],
        "price_base": 450_000},
    "Miami": {"state": "FL", "zip_prefix": "331", "lat": 25.76, "lon": -80.19,
        "neighborhoods": ["Brickell", "Wynwood", "Coconut Grove", "Coral Gables", "Little Havana"],
        "price_base": 680_000},
    "Nashville": {"state": "TN", "zip_prefix": "372", "lat": 36.16, "lon": -86.78,
        "neighborhoods": ["East Nashville", "The Gulch", "Germantown", "12 South", "Sylvan Park"],
        "price_base": 520_000},
    "Portland": {"state": "OR", "zip_prefix": "972", "lat": 45.52, "lon": -122.68,
        "neighborhoods": ["Pearl District", "Alberta Arts", "Hawthorne", "Division", "St. Johns"],
        "price_base": 580_000},
    "Boston": {"state": "MA", "zip_prefix": "021", "lat": 42.36, "lon": -71.06,
        "neighborhoods": ["Back Bay", "South End", "Cambridge", "Somerville", "Jamaica Plain"],
        "price_base": 950_000},
}

PROPERTY_TYPES = ["Single Family", "Condo", "Townhouse", "Multi-Family"]
LISTING_STATUSES = ["For Sale", "For Sale", "For Sale", "Pending", "Recently Sold"]

FEATURES_POOL = [
    "Hardwood Floors", "Stainless Steel Appliances", "Granite Countertops",
    "Central AC", "In-Unit Laundry", "Rooftop Deck", "Smart Home",
    "EV Charging", "Solar Panels", "Pool", "Hot Tub", "Fireplace",
    "Walk-In Closet", "Open Floor Plan", "Updated Kitchen",
    "Home Office", "Finished Basement", "Garage", "Garden", "Pet Friendly",
]

# COMMAND ----------

def generate_listing(idx: int) -> dict:
    city = random.choice(list(METROS.keys()))
    metro = METROS[city]
    neighborhood = random.choice(metro["neighborhoods"])
    prop_type = random.choices(PROPERTY_TYPES, weights=[0.4, 0.3, 0.2, 0.1])[0]

    # Calibrate by property type
    type_mult = {"Single Family": 1.0, "Condo": 0.65, "Townhouse": 0.8, "Multi-Family": 1.3}
    base = metro["price_base"] * type_mult[prop_type]

    beds = random.choices([1, 2, 3, 4, 5], weights=[0.1, 0.25, 0.35, 0.2, 0.1])[0]
    baths = max(1, beds - random.randint(0, 1)) + random.choice([0, 0.5])
    sqft = int(beds * random.randint(400, 700) + random.randint(200, 600))

    # Price correlates with beds, sqft, school_rating
    school_rating = round(random.uniform(3, 10), 1)
    walk_score = random.randint(30, 99)
    price = int(base * (0.7 + 0.1 * beds + sqft / 8000 + school_rating / 40) * random.uniform(0.85, 1.15))
    price = round(price, -3)  # round to nearest $1,000
    price_per_sqft = round(price / sqft, 2)

    year_built = random.randint(1920, 2024)
    lot_size = round(random.uniform(0.05, 0.8), 2) if prop_type != "Condo" else 0.0
    hoa_fee = random.choice([0, 150, 250, 350, 500]) if prop_type in ("Condo", "Townhouse") else 0
    parking = random.choice(["Garage", "Driveway", "Street", "Covered", "None"])
    days_on_market = random.randint(1, 120)
    status = random.choice(LISTING_STATUSES)

    lat = metro["lat"] + random.uniform(-0.05, 0.05)
    lon = metro["lon"] + random.uniform(-0.05, 0.05)
    zip_code = metro["zip_prefix"] + str(random.randint(10, 99))
    address = fake.street_address()

    features = random.sample(FEATURES_POOL, k=random.randint(3, 7))

    description = (
        f"Beautiful {prop_type.lower()} in {neighborhood}, {city}. "
        f"This {beds}-bedroom, {baths}-bath home offers {sqft:,} sqft of living space. "
        f"Built in {year_built}, features include {', '.join(features[:3])}. "
        f"Located near top-rated schools (rating {school_rating}/10) with a walk score of {walk_score}."
    )

    image_url = f"https://picsum.photos/seed/{idx}/640/400"

    text_for_embedding = (
        f"{prop_type} in {neighborhood}, {city}, {metro['state']}. "
        f"{beds} beds, {baths} baths, {sqft:,} sqft. "
        f"Price ${price:,}. Built {year_built}. "
        f"Features: {', '.join(features)}. "
        f"School rating {school_rating}/10. Walk score {walk_score}. "
        f"{description}"
    )

    return {
        "id": str(uuid.uuid4()),
        "address": address,
        "city": city,
        "state": metro["state"],
        "zip_code": zip_code,
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "price": price,
        "beds": beds,
        "baths": float(baths),
        "sqft": sqft,
        "lot_size": lot_size,
        "year_built": year_built,
        "property_type": prop_type,
        "listing_status": status,
        "days_on_market": days_on_market,
        "hoa_fee": hoa_fee,
        "parking": parking,
        "neighborhood": neighborhood,
        "school_rating": school_rating,
        "walk_score": walk_score,
        "description": description,
        "features": features,
        "image_url": image_url,
        "price_per_sqft": price_per_sqft,
        "text_for_embedding": text_for_embedding,
    }

# COMMAND ----------

listings = [generate_listing(i) for i in range(NUM_LISTINGS)]
df = spark.createDataFrame(listings)

df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{SCHEMA}.{TABLE}"
)

print(f"Wrote {df.count()} listings to {CATALOG}.{SCHEMA}.{TABLE}")
display(df.limit(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT city, count(*) as cnt, round(avg(price)) as avg_price
# MAGIC FROM bircatalog.zillow.properties
# MAGIC GROUP BY city ORDER BY avg_price DESC
