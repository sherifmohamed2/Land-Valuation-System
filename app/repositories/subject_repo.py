"""Subject land parcels — Stage 2 persistence (minimal API surface today)."""
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.subject_land import SubjectLand


class SubjectRepository:
    """Async access to ``subject_lands``."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, subject: SubjectLand) -> SubjectLand:
        self.session.add(subject)
        await self.session.flush()
        return subject

    async def get_by_id(self, subject_id: int):
        result = await self.session.execute(
            select(SubjectLand).where(SubjectLand.id == subject_id)
        )
        return result.scalar_one_or_none()
