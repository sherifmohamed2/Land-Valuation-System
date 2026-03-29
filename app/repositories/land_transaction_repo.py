"""Data access for ``land_transactions`` — listing, single fetch, pipeline bulk updates."""
import json
import logging
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from app.models.land_transaction import LandTransaction

logger = logging.getLogger(__name__)


def parse_json_field(value):
    """Normalize JSON columns that may arrive as str or already-parsed dict/list."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


class LandTransactionRepository:
    """Async queries and updates for the land transaction fact table."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_all(self, limit: int = 100, offset: int = 0) -> tuple[int, list[LandTransaction]]:
        count_result = await self.session.execute(select(func.count()).select_from(LandTransaction))
        total = count_result.scalar_one()
        result = await self.session.execute(
            select(LandTransaction).order_by(LandTransaction.id).limit(limit).offset(offset)
        )
        items = result.scalars().all()
        for item in items:
            item.utilities_json = parse_json_field(item.utilities_json)
        return total, list(items)

    async def get_by_id(self, transaction_id: int) -> Optional[LandTransaction]:
        result = await self.session.execute(
            select(LandTransaction).where(LandTransaction.id == transaction_id)
        )
        item = result.scalar_one_or_none()
        if item:
            item.utilities_json = parse_json_field(item.utilities_json)
        return item

    async def get_all_for_processing(self) -> list[LandTransaction]:
        """Fetch all records for pipeline processing (normalization + benchmarking)."""
        result = await self.session.execute(
            select(LandTransaction).order_by(LandTransaction.id)
        )
        items = result.scalars().all()
        for item in items:
            item.utilities_json = parse_json_field(item.utilities_json)
        return list(items)

    async def bulk_update_cleaning_fields(self, updates: list[dict]) -> None:
        """
        Apply normalization/outlier outputs row-wise (``is_cleaned``, ``is_outlier``,
        ``price_per_sqm``, percentiles, city medians, etc.). Caller flushes/commits.
        """
        for upd in updates:
            tx_id = upd.pop("id")
            result = await self.session.execute(
                select(LandTransaction).where(LandTransaction.id == tx_id)
            )
            tx = result.scalar_one_or_none()
            if tx:
                for k, v in upd.items():
                    setattr(tx, k, v)
        await self.session.flush()
