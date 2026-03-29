import uuid
from datetime import datetime
from sqlalchemy import String, Integer, JSON, DateTime, Text
from typing import Optional
from sqlalchemy.orm import Mapped, mapped_column
from app.core.database import Base


class BenchmarkRun(Base):
    """One Stage 1 execution: status, counts, frozen filter config, validation snapshot."""

    __tablename__ = "benchmark_selection_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_uuid: Mapped[str] = mapped_column(String(36), unique=True, default=lambda: str(uuid.uuid4()))
    run_name: Mapped[Optional[str]] = mapped_column(String(200))
    status: Mapped[str] = mapped_column(String(50), default="pending")
    trigger_type: Mapped[Optional[str]] = mapped_column(String(50))
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    total_candidates: Mapped[Optional[int]] = mapped_column(Integer)
    clean_candidates: Mapped[Optional[int]] = mapped_column(Integer)
    benchmarks_found: Mapped[Optional[int]] = mapped_column(Integer)
    filter_config_json: Mapped[Optional[dict]] = mapped_column(JSON)
    validation_result_json: Mapped[Optional[dict]] = mapped_column(JSON)
    notes: Mapped[Optional[str]] = mapped_column(Text)
    created_by: Mapped[str] = mapped_column(String(100), default="system")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
