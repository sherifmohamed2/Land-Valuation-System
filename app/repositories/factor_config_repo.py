"""Load factor configuration rows for Stage 2 scoring (active-only or full list)."""
import json
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.factor_config import FactorConfig

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


class FactorConfigRepository:
    """Read ``factor_configs``; parses ``params_json`` after load."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_all_active(self) -> list[FactorConfig]:
        result = await self.session.execute(
            select(FactorConfig).where(FactorConfig.is_active == True).order_by(FactorConfig.id)
        )
        items = result.scalars().all()
        for item in items:
            item.params_json = parse_json_field(item.params_json)
        return list(items)

    async def get_all(self) -> list[FactorConfig]:
        result = await self.session.execute(
            select(FactorConfig).order_by(FactorConfig.id)
        )
        items = result.scalars().all()
        for item in items:
            item.params_json = parse_json_field(item.params_json)
        return list(items)
