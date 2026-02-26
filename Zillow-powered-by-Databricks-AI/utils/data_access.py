"""Unity Catalog SQL queries for market analytics."""

import pandas as pd
from config import FULL_TABLE_NAME, SQL_WAREHOUSE_ID
from utils.databricks_client import get_workspace_client


def _execute_query(sql: str) -> pd.DataFrame:
    """Execute SQL via the Databricks SDK statement execution API."""
    w = get_workspace_client()
    result = w.statement_execution.execute_statement(
        warehouse_id=SQL_WAREHOUSE_ID,
        statement=sql,
        wait_timeout="30s",
    )
    columns = [col.name for col in result.manifest.schema.columns]
    rows = []
    if result.result and result.result.data_array:
        rows = result.result.data_array
    return pd.DataFrame(rows, columns=columns)


def get_market_summary() -> pd.DataFrame:
    """Avg price, count, avg sqft by city."""
    return _execute_query(f"""
        SELECT city, state,
               COUNT(*) as listing_count,
               ROUND(AVG(price)) as avg_price,
               ROUND(AVG(sqft)) as avg_sqft,
               ROUND(AVG(price_per_sqft), 2) as avg_price_per_sqft,
               ROUND(AVG(days_on_market)) as avg_dom
        FROM {FULL_TABLE_NAME}
        GROUP BY city, state
        ORDER BY avg_price DESC
    """)


def get_price_distribution(city: str | None = None) -> pd.DataFrame:
    """Price distribution for histogram."""
    where = f"WHERE city = '{city}'" if city else ""
    return _execute_query(f"""
        SELECT price, property_type, city
        FROM {FULL_TABLE_NAME}
        {where}
    """)


def get_property_type_breakdown(city: str | None = None) -> pd.DataFrame:
    """Listing count by property type."""
    where = f"WHERE city = '{city}'" if city else ""
    return _execute_query(f"""
        SELECT property_type, COUNT(*) as count, ROUND(AVG(price)) as avg_price
        FROM {FULL_TABLE_NAME}
        {where}
        GROUP BY property_type
        ORDER BY count DESC
    """)


def get_neighborhood_stats(city: str) -> pd.DataFrame:
    """Neighborhood-level stats for a given city."""
    return _execute_query(f"""
        SELECT neighborhood,
               COUNT(*) as listings,
               ROUND(AVG(price)) as avg_price,
               ROUND(AVG(school_rating), 1) as avg_school_rating,
               ROUND(AVG(walk_score)) as avg_walk_score
        FROM {FULL_TABLE_NAME}
        WHERE city = '{city}'
        GROUP BY neighborhood
        ORDER BY avg_price DESC
    """)


def get_total_stats() -> dict:
    """High-level stats for the hero section."""
    df = _execute_query(f"""
        SELECT COUNT(*) as total_listings,
               ROUND(AVG(price)) as avg_price,
               COUNT(DISTINCT city) as num_cities,
               ROUND(AVG(days_on_market)) as avg_dom
        FROM {FULL_TABLE_NAME}
    """)
    if len(df) > 0:
        row = df.iloc[0]
        return {
            "total_listings": int(float(row["total_listings"])),
            "avg_price": int(float(row["avg_price"])),
            "num_cities": int(float(row["num_cities"])),
            "avg_dom": int(float(row["avg_dom"])),
        }
    return {"total_listings": 0, "avg_price": 0, "num_cities": 0, "avg_dom": 0}
