from datetime import datetime
from sqlalchemy import String, Integer, Float, JSON, DateTime, ForeignKey
from typing import Optional
from sqlalchemy.orm import Mapped, mapped_column
from app.core.database import Base


class ValuationFactorScore(Base):
    """Per-factor audit line for subject or comparable within a valuation run."""

    __tablename__ = "valuation_factor_scores"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    valuation_run_id: Mapped[int] = mapped_column(Integer, ForeignKey("valuation_runs.id"))
    asset_kind: Mapped[str] = mapped_column(String(20), nullable=False)
    asset_ref_id: Mapped[int] = mapped_column(Integer, nullable=False)
    factor_key: Mapped[Optional[str]] = mapped_column(String(100))
    weight: Mapped[Optional[float]] = mapped_column(Float)
    multiplier: Mapped[Optional[float]] = mapped_column(Float)
    score_points: Mapped[Optional[float]] = mapped_column(Float)
    raw_inputs_snapshot_json: Mapped[Optional[dict]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
