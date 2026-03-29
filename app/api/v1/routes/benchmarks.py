"""
Benchmark selection API — Stage 1.

POST runs the full pipeline (orchestrator); GET endpoints read persisted runs/results.
Response shape is stable: five benchmark slots, each populated or null.
"""
import logging
from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.core.config import settings
from app.repositories.benchmark_repo import BenchmarkRepository
from app.services.benchmark.benchmark_orchestrator import run_benchmark_selection
from app.services.benchmark.benchmark_response_mapper import build_benchmark_run_response
from app.schemas.benchmark import (
    BenchmarkRunResponse,
    BenchmarkRunSummary,
    BenchmarkRunsListResponse,
)
from app.core.exceptions import NotFoundError, ServiceError

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/benchmarks/run", response_model=BenchmarkRunResponse, status_code=status.HTTP_201_CREATED)
async def trigger_benchmark_run(db: AsyncSession = Depends(get_db)):
    """
    Run Stage 1 end-to-end; persist run and five result rows (nullable slots).

    Commits via ``get_db``. Reloads the latest completed run to return the same
    snapshot clients would see from GET ``/benchmarks/latest``.
    """
    await run_benchmark_selection(db, settings, trigger_type="manual")

    bm_repo = BenchmarkRepository(db)
    run = await bm_repo.get_latest_run()
    if not run:
        raise ServiceError("Benchmark run completed but could not be reloaded for response.")
    results = await bm_repo.get_results_for_run(run.id)
    return build_benchmark_run_response(run, results)


@router.get("/benchmarks/latest", response_model=BenchmarkRunResponse)
async def get_latest_benchmark(db: AsyncSession = Depends(get_db)):
    """Latest ``status=completed`` run only; 404 if none exists yet."""
    bm_repo = BenchmarkRepository(db)
    run = await bm_repo.get_latest_run()
    if not run:
        raise NotFoundError("No completed benchmark runs found. POST /api/v1/benchmarks/run to create one.")
    results = await bm_repo.get_results_for_run(run.id)
    return build_benchmark_run_response(run, results)


@router.get("/benchmarks/runs", response_model=BenchmarkRunsListResponse)
async def list_benchmark_runs(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """List all benchmark selection runs with status summaries."""
    bm_repo = BenchmarkRepository(db)
    total, runs = await bm_repo.list_runs(limit=limit, offset=offset)
    return BenchmarkRunsListResponse(
        total=total,
        items=[BenchmarkRunSummary.model_validate(r) for r in runs],
    )
