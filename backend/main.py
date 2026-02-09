"""FastAPI application entry point for the B2B Sheet Generator service.

Start the server with::

    uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from backend.config import UPLOAD_DIR
from backend.routers import agent, export, extraction

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan -- startup / shutdown logic
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan handler.

    * On startup: ensure the output directory tree exists.
    * On shutdown: (no-op for now; add cleanup here if needed.)
    """
    # Startup
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    logger.info("Output directory ready: %s", UPLOAD_DIR)

    # Initialize database (agent features)
    from backend.db import init_db, close_db

    try:
        await init_db()
    except Exception:
        logger.warning("Database init failed -- agent features unavailable", exc_info=True)

    logger.info("B2B Sheet Generator service started.")

    yield

    # Shutdown
    try:
        await close_db()
    except Exception:
        logger.warning("Database close failed", exc_info=True)
    logger.info("B2B Sheet Generator service shutting down.")


# ---------------------------------------------------------------------------
# Application instance
# ---------------------------------------------------------------------------
app = FastAPI(
    title="B2B Sheet Generator",
    description=(
        "Upload invoices and packing lists (PDF), extract structured data "
        "with AI, and download XpressB2B bulk-upload Excel sheets."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
app.include_router(extraction.router)
app.include_router(export.router)
app.include_router(agent.router)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.get("/api/health", tags=["health"])
async def health_check() -> dict[str, str]:
    """Lightweight health-check endpoint for probes and monitoring."""
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Serve frontend static files (must be the LAST mount so API routes match first)
# ---------------------------------------------------------------------------
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# In production (Docker), built files are in frontend_dist/
# In development, Vite serves the frontend separately on :5173
_frontend_dist = os.path.join(_project_root, "frontend_dist")
_frontend_dev = os.path.join(_project_root, "frontend")

if os.path.isdir(_frontend_dist):
    app.mount("/", StaticFiles(directory=_frontend_dist, html=True), name="frontend")
    logger.info("Serving frontend from: %s", _frontend_dist)
elif os.path.isdir(_frontend_dev):
    app.mount("/", StaticFiles(directory=_frontend_dev, html=True), name="frontend")
    logger.info("Serving frontend from: %s", _frontend_dev)
else:
    logger.warning(
        "Frontend directory not found -- static file serving disabled.",
    )
