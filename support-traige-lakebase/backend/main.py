import logging
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from backend import database as db
from backend.models import HistoryEntry
from backend.routes import comments, stats, tickets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent.parent / "frontend" / "dist"


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Support Portal...")
    try:
        pool = await db.get_pool()
        logger.info("Database connection pool established")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
    yield
    await db.close_pool()
    logger.info("Support Portal shut down")


app = FastAPI(
    title="Support Triage Portal",
    version="1.0.0",
    lifespan=lifespan,
)

api = APIRouter(prefix="/api")

# Health check
@api.get("/health")
async def health():
    try:
        result = await db.fetchval("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "database": str(e)},
        )


@api.get("/debug")
async def debug():
    import os
    from backend.config import settings
    info = {
        "PGHOST": os.environ.get("PGHOST", ""),
        "PGPORT": os.environ.get("PGPORT", ""),
        "PGUSER": os.environ.get("PGUSER", ""),
        "PGDATABASE": os.environ.get("PGDATABASE", ""),
        "PGSSLMODE": os.environ.get("PGSSLMODE", ""),
        "PGPASSWORD_set": bool(os.environ.get("PGPASSWORD", "")),
        "DATABRICKS_HOST": os.environ.get("DATABRICKS_HOST", ""),
        "DATABRICKS_APP_NAME": os.environ.get("DATABRICKS_APP_NAME", ""),
        "LAKEBASE_INSTANCE_NAME": settings.LAKEBASE_INSTANCE_NAME,
        "LAKEBASE_PROJECT": settings.LAKEBASE_PROJECT,
        "IS_DATABRICKS_APP": db.IS_DATABRICKS_APP,
        "HAS_PG_RESOURCE_BINDING": db.HAS_PG_RESOURCE_BINDING,
    }
    try:
        row = await db.fetchrow(
            "SELECT current_database() as db, current_schema() as schema, current_user as usr"
        )
        info["connected_db"] = row["db"]
        info["connected_schema"] = row["schema"]
        info["connected_user"] = row["usr"]
        schemas = await db.fetch(
            "SELECT schema_name FROM information_schema.schemata"
        )
        info["schemas"] = [r["schema_name"] for r in schemas]
        tables = await db.fetch(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema = 'support_app'"
        )
        info["support_app_tables"] = [r["table_name"] for r in tables]
    except Exception as e:
        info["db_error"] = str(e)
    return info


# Ticket history endpoint
@api.get("/tickets/{ticket_id}/history", response_model=list[HistoryEntry])
async def get_ticket_history(ticket_id: uuid.UUID):
    rows = await db.fetch(
        "SELECT * FROM ticket_history WHERE ticket_id = $1 ORDER BY created_at DESC",
        ticket_id,
    )
    return [
        HistoryEntry(
            id=r["id"],
            ticket_id=r["ticket_id"],
            changed_by_email=r["changed_by_email"],
            changed_by_name=r["changed_by_name"] or "",
            field_name=r["field_name"],
            old_value=r["old_value"],
            new_value=r["new_value"],
            created_at=r["created_at"],
        )
        for r in rows
    ]


# Include route modules
api.include_router(tickets.router)
api.include_router(comments.router)
api.include_router(stats.router)

app.include_router(api)

# Serve frontend static files
if STATIC_DIR.exists():
    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(request: Request, full_path: str):
        # Try to serve static file first
        file_path = STATIC_DIR / full_path
        if full_path and file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        # Fall back to index.html for SPA routing
        index = STATIC_DIR / "index.html"
        if index.exists():
            return FileResponse(index)
        return JSONResponse({"error": "Frontend not built"}, status_code=404)
else:
    @app.get("/")
    async def root():
        return {"message": "Support Portal API. Frontend not built yet."}
