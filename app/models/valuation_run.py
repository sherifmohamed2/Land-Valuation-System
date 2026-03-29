import uuid
from datetime import datetime
from sqlalchemy import String, Integer, Float, DateTime, ForeignKey
from typing import Optional
from sqlalchemy.orm import Mapped, mapped_column
from app.core.database import Base


class ValuationRun(Base):
    """Single Stage 2 run: subject, optional benchmark run link, pricing outputs."""

    __tablename__ = "valuation_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_uuid: Mapped[str] = mapped_column(String(36), unique=True, default=lambda: str(uuid.uuid4()))
    subject_land_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("subject_lands.id"))
    benchmark_run_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("benchmark_selection_runs.id"))
    status: Mapped[str] = mapped_column(String(50), default="pending")
    comparables_evaluated: Mapped[Optional[int]] = mapped_column(Integer)
    comparables_accepted: Mapped[Optional[int]] = mapped_column(Integer)
    average_point_value: Mapped[Optional[float]] = mapped_column(Float)
    target_points: Mapped[Optional[float]] = mapped_column(Float)
    estimated_price_per_sqm: Mapped[Optional[float]] = mapped_column(Float)
    estimated_total_value: Mapped[Optional[float]] = mapped_column(Float)
    factor_config_version: Mapped[Optional[int]] = mapped_column(Integer)
    rule_version: Mapped[Optional[str]] = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
