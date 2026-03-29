from datetime import datetime
from sqlalchemy import String, Integer, JSON, DateTime
from typing import Optional
from sqlalchemy.orm import Mapped, mapped_column
from app.core.database import Base


class AuditLog(Base):
    """Generic append-only event log (entity type/id, action, optional JSON payloads)."""

    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    entity_type: Mapped[Optional[str]] = mapped_column(String(100))
    entity_id: Mapped[Optional[int]] = mapped_column(Integer)
    action: Mapped[Optional[str]] = mapped_column(String(100))
    before_json: Mapped[Optional[dict]] = mapped_column(JSON)
    after_json: Mapped[Optional[dict]] = mapped_column(JSON)
    performed_by: Mapped[str] = mapped_column(String(100), default="system")
    performed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
