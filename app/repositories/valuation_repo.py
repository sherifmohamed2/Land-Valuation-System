"""Valuation runs — Stage 2 persistence (stub usage until execution exists)."""
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.valuation_run import ValuationRun


class ValuationRepository:
    """Async access to ``valuation_runs``."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, run: ValuationRun) -> ValuationRun:
        self.session.add(run)
        await self.session.flush()
        return run
