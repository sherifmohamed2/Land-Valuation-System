"""Lookup helpers for district centroids (used by benchmark tiebreaker scoring)."""
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.lookup_location import LocationLookup

logger = logging.getLogger(__name__)


class LocationService:
    """Read ``location_lookup`` for scoring and future Stage 2 distance rules."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_all(self) -> list[LocationLookup]:
        result = await self.session.execute(select(LocationLookup).order_by(LocationLookup.id))
        return list(result.scalars().all())

    async def get_district_centroid(self, city: str, district: str):
        result = await self.session.execute(
            select(LocationLookup).where(
                LocationLookup.city == city,
                LocationLookup.district == district,
            )
        )
        row = result.scalar_one_or_none()
        if row and row.latitude_centroid is not None and row.longitude_centroid is not None:
            return (row.latitude_centroid, row.longitude_centroid)
        return None
