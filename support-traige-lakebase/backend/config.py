import os


class Settings:
    LAKEBASE_PROJECT: str = os.environ.get("LAKEBASE_PROJECT", "support-lakebase")
    LAKEBASE_BRANCH: str = os.environ.get("LAKEBASE_BRANCH", "production")
    LAKEBASE_ENDPOINT: str = os.environ.get("LAKEBASE_ENDPOINT", "primary")
    LAKEBASE_DATABASE: str = os.environ.get("LAKEBASE_DATABASE", "support_portal")
    LAKEBASE_HOST: str = os.environ.get("LAKEBASE_HOST", "")
    LAKEBASE_INSTANCE_NAME: str = os.environ.get("LAKEBASE_INSTANCE_NAME", "")
    DATABRICKS_HOST: str = os.environ.get("DATABRICKS_HOST", "")
    PORT: int = int(os.environ.get("PORT", "8000"))
    # In Databricks Apps, user identity comes from these headers
    USER_EMAIL_HEADER: str = "X-Forwarded-Email"
    USER_NAME_HEADER: str = "X-Forwarded-Preferred-Username"


settings = Settings()
