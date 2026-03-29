from __future__ import annotations
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.audit_log import AuditLog
from datetime import datetime

logger = logging.getLogger(__name__)


class AuditService:
    """Append-only audit trail rows tied to the current DB session."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def log(
        self,
        entity_type: str,
        entity_id: int,
        action: str,
        before_data: dict | None = None,
        after_data: dict | None = None,
        performed_by: str = "system",
    ) -> None:
        """Persist a structured audit event (before/after JSON optional)."""
        entry = AuditLog(
            entity_type=entity_type,
            entity_id=entity_id,
            action=action,
            before_json=before_data,
            after_json=after_data,
            performed_by=performed_by,
            performed_at=datetime.utcnow(),
        )
        self.session.add(entry)
        await self.session.flush()
        logger.debug(f"Audit: {entity_type}#{entity_id} → {action}")
