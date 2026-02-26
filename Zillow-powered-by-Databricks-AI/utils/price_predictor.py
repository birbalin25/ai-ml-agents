"""MLflow model scoring for Zestimate predictions.

Falls back to a heuristic estimator if the MLflow model is unavailable.
"""

import os
import random
import pandas as pd
from functools import lru_cache
from utils.databricks_client import get_databricks_host, get_token

_model = None
_model_loaded = False


def _try_load_model():
    """Attempt to load the MLflow model once."""
    global _model, _model_loaded
    if _model_loaded:
        return _model
    _model_loaded = True
    try:
        import mlflow
        from config import REGISTERED_MODEL_NAME, MODEL_ALIAS
        host = get_databricks_host()
        os.environ["DATABRICKS_HOST"] = host
        os.environ["DATABRICKS_TOKEN"] = get_token()
        mlflow.set_tracking_uri("databricks")
        mlflow.set_registry_uri("databricks-uc")
        model_uri = f"models:/{REGISTERED_MODEL_NAME}@{MODEL_ALIAS}"
        _model = mlflow.sklearn.load_model(model_uri)
    except Exception:
        _model = None
    return _model


def _to_float(val, default=0.0) -> float:
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def _heuristic_zestimate(prop: dict) -> float:
    """Simple heuristic Zestimate based on price with a realistic variance."""
    price = _to_float(prop.get("price"))
    if price <= 0:
        return 0.0
    # Simulate a Zestimate: vary by -8% to +8% from listing price
    # Use a deterministic seed based on property attributes for consistency
    seed_val = hash(
        f"{prop.get('address','')}{prop.get('city','')}{prop.get('beds','')}{prop.get('sqft','')}"
    )
    rng = random.Random(seed_val)
    pct = rng.uniform(-0.08, 0.08)
    estimate = price * (1 + pct)
    return round(estimate, -3)


def predict_price(property_data: dict) -> float | None:
    """Predict the price (Zestimate) for a single property."""
    model = _try_load_model()
    if model is not None:
        try:
            feature_cols = [
                "beds", "baths", "sqft", "lot_size", "year_built",
                "school_rating", "walk_score", "hoa_fee",
                "city", "property_type", "neighborhood",
            ]
            row = {col: property_data.get(col) for col in feature_cols}
            df = pd.DataFrame([row])
            prediction = model.predict(df)[0]
            return round(float(prediction), -3)
        except Exception:
            pass
    return _heuristic_zestimate(property_data)


def predict_prices_batch(properties: list[dict]) -> list[float | None]:
    """Predict prices for a batch of properties."""
    model = _try_load_model()
    if model is not None:
        try:
            feature_cols = [
                "beds", "baths", "sqft", "lot_size", "year_built",
                "school_rating", "walk_score", "hoa_fee",
                "city", "property_type", "neighborhood",
            ]
            rows = [{col: p.get(col) for col in feature_cols} for p in properties]
            df = pd.DataFrame(rows)
            predictions = model.predict(df)
            return [round(float(p), -3) for p in predictions]
        except Exception:
            pass
    # Fallback: heuristic for each property
    return [_heuristic_zestimate(p) for p in properties]
