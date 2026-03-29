"""
Land Valuation API — FastAPI application entry.

Registers v1 routers under ``/api/v1`` for transactions, benchmarks, Stage 2 placeholders
(subjects, valuations), and factor config. Lifespan opens the DB schema and starts
the benchmark refresh scheduler. See README for Stage 1 vs Stage 2 scope.
"""
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.core.database import create_tables
from app.core.config import settings
from app.core.logging_config import configure_logging
from app.api.v1.routes import transactions, benchmarks, subjects, valuations, configs
from app.jobs.benchmark_refresh_job import create_scheduler

configure_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup: ensure SQLAlchemy metadata exists, then start APScheduler for cron benchmark runs.
    Shutdown stops the scheduler. DB file is normally pre-seeded; ``create_tables`` is idempotent.
    """
    await create_tables()
    logger.info("Database tables verified.")
    scheduler = create_scheduler(app)
    logger.info("Scheduler started.")
    yield
    scheduler.shutdown(wait=False)
    logger.info("Scheduler shutdown.")


app = FastAPI(
    title="Land Valuation System — Market Method",
    description=(
        "Stage 1: Benchmark selection (active) | "
        "Stage 2: Comparable scoring & pricing engine (designed, not yet executed)"
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(transactions.router, prefix="/api/v1", tags=["Transactions"])
app.include_router(benchmarks.router, prefix="/api/v1", tags=["Benchmarks"])
app.include_router(subjects.router, prefix="/api/v1", tags=["Subjects - Stage 2"])
app.include_router(valuations.router, prefix="/api/v1", tags=["Valuations - Stage 2"])
app.include_router(configs.router, prefix="/api/v1", tags=["Config"])


@app.get("/health")
async def health():
    """Liveness and high-level feature flags for operators."""
    return {
        "status": "ok",
        "database": "SQLite (file: data/land_valuation.db)",
        "stage1": "active",
        "stage2": "designed — not yet implemented",
    }
