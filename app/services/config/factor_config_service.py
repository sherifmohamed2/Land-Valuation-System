"""Thin service over ``FactorConfigRepository`` for API/config routes."""
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from app.repositories.factor_config_repo import FactorConfigRepository

logger = logging.getLogger(__name__)


class FactorConfigService:
    """Expose factor rows to HTTP layer (Stage 2 weights)."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_all_active(self):
        repo = FactorConfigRepository(self.session)
        return await repo.get_all_active()

    async def get_all(self):
        repo = FactorConfigRepository(self.session)
        return await repo.get_all()
