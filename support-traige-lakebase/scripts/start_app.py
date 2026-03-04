"""Startup script for the Support Portal Databricks App."""

import logging
import os
import subprocess
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_frontend():
    """Build the React frontend if needed."""
    frontend_dir = Path(__file__).parent.parent / "frontend"
    dist_dir = frontend_dir / "dist"

    if dist_dir.exists() and (dist_dir / "index.html").exists():
        logger.info("Frontend already built, skipping build")
        return

    logger.info("Building frontend...")
    # Install deps
    subprocess.run(["npm", "install"], cwd=frontend_dir, check=True, timeout=120)
    # Build
    subprocess.run(["npm", "run", "build"], cwd=frontend_dir, check=True, timeout=120)
    logger.info("Frontend built successfully")


def init_database():
    """Initialize database schema."""
    logger.info("Initializing database...")
    try:
        from scripts.init_db import main as init_main
        init_main()
    except Exception as e:
        logger.warning(f"Database init warning (may already be initialized): {e}")


def main():
    """Main entry point."""
    import uvicorn

    # Build frontend
    try:
        build_frontend()
    except Exception as e:
        logger.warning(f"Frontend build skipped: {e}")

    # Init DB
    try:
        init_database()
    except Exception as e:
        logger.warning(f"DB init skipped: {e}")

    port = int(os.environ.get("PORT", "8000"))
    logger.info(f"Starting Support Portal on port {port}")

    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0",
        port=port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
