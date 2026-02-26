# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Train Price Prediction Model
# MAGIC Trains a GradientBoosting model for "Zestimate" price predictions.
# MAGIC Logs to MLflow and registers in Unity Catalog.

# COMMAND ----------

import mlflow
import pandas as pd
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, r2_score

# COMMAND ----------

CATALOG = "bircatalog"
SCHEMA = "zillow"
TABLE = "properties"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.price_prediction_model"

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA}.{TABLE}").toPandas()

feature_cols = ["beds", "baths", "sqft", "lot_size", "year_built",
                "school_rating", "walk_score", "hoa_fee",
                "city", "property_type", "neighborhood"]
target = "price"

df_model = df[feature_cols + [target]].dropna()
print(f"Training on {len(df_model)} rows")

# COMMAND ----------

X = df_model[feature_cols]
y = df_model[target]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

cat_features = ["city", "property_type", "neighborhood"]
num_features = ["beds", "baths", "sqft", "lot_size", "year_built",
                "school_rating", "walk_score", "hoa_fee"]

preprocessor = ColumnTransformer([
    ("num", StandardScaler(), num_features),
    ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat_features),
])

pipeline = Pipeline([
    ("preprocessor", preprocessor),
    ("regressor", GradientBoostingRegressor(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.1,
        random_state=42,
    )),
])

# COMMAND ----------

mlflow.sklearn.autolog(log_models=False)

with mlflow.start_run(run_name="zillow_price_model") as run:
    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    mape = mean_absolute_percentage_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    mlflow.log_metrics({"mae": mae, "mape": mape, "r2": r2})
    print(f"MAE:  ${mae:,.0f}")
    print(f"MAPE: {mape:.2%}")
    print(f"R²:   {r2:.4f}")

    # Log model with input example for signature inference
    input_example = X_test.head(1)
    mlflow.sklearn.log_model(
        pipeline,
        artifact_path="model",
        input_example=input_example,
        registered_model_name=MODEL_NAME,
    )
    print(f"Model registered as '{MODEL_NAME}'")

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()

# Get latest version
versions = client.search_model_versions(f"name='{MODEL_NAME}'")
latest_version = max(v.version for v in versions)

# Set production alias
client.set_registered_model_alias(MODEL_NAME, "production", latest_version)
print(f"Set alias @production → version {latest_version}")

# COMMAND ----------

# Quick sanity check with the registered model
model_uri = f"models:/{MODEL_NAME}@production"
loaded_model = mlflow.sklearn.load_model(model_uri)

sample = X_test.head(5)
preds = loaded_model.predict(sample)
for i, (_, row) in enumerate(sample.iterrows()):
    actual = y_test.iloc[i] if i < len(y_test) else "N/A"
    print(f"  {row['city']}, {row['property_type']}: predicted ${preds[i]:,.0f} (actual ${actual:,.0f})")
