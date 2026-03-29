"""Persistence for benchmark selection runs and per-type results."""
import json
import logging
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from app.models.benchmark_run import BenchmarkRun
from app.models.benchmark_result import BenchmarkResult

logger = logging.getLogger(__name__)


def parse_json_field(value):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


class BenchmarkRepository:
    """CRUD and listing for ``benchmark_selection_runs`` and ``benchmark_results``."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_run(self, run: BenchmarkRun) -> BenchmarkRun:
        self.session.add(run)
        await self.session.flush()
        return run

    async def update_run(self, run: BenchmarkRun) -> BenchmarkRun:
        await self.session.flush()
        return run

    async def create_result(self, result: BenchmarkResult) -> BenchmarkResult:
        self.session.add(result)
        await self.session.flush()
        return result

    async def get_latest_run(self) -> Optional[BenchmarkRun]:
        """Most recent completed run (excludes ``running`` / ``failed``)."""
        result = await self.session.execute(
            select(BenchmarkRun)
            .where(BenchmarkRun.status == "completed")
            .order_by(BenchmarkRun.id.desc())
            .limit(1)
        )
        run = result.scalar_one_or_none()
        if run:
            run.filter_config_json = parse_json_field(run.filter_config_json)
            run.validation_result_json = parse_json_field(run.validation_result_json)
        return run

    async def get_results_for_run(self, run_id: int) -> list[BenchmarkResult]:
        result = await self.session.execute(
            select(BenchmarkResult).where(BenchmarkResult.run_id == run_id)
        )
        items = result.scalars().all()
        for item in items:
            item.score_breakdown_json = parse_json_field(item.score_breakdown_json)
            item.validation_flags_json = parse_json_field(item.validation_flags_json)
        return list(items)

    async def list_runs(self, limit: int = 50, offset: int = 0) -> tuple[int, list[BenchmarkRun]]:
        count_result = await self.session.execute(select(func.count()).select_from(BenchmarkRun))
        total = count_result.scalar_one()
        result = await self.session.execute(
            select(BenchmarkRun).order_by(BenchmarkRun.id.desc()).limit(limit).offset(offset)
        )
        return total, list(result.scalars().all())
