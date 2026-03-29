"""Zoning reference data access (future compatibility / admin tooling)."""
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.lookup_zoning import ZoningLookup

logger = logging.getLogger(__name__)


class ZoningService:
    """Read ``zoning_lookup`` rows."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_all(self) -> list[ZoningLookup]:
        result = await self.session.execute(select(ZoningLookup).order_by(ZoningLookup.id))
        return list(result.scalars().all())
