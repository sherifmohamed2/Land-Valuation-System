import uuid
from datetime import datetime
from sqlalchemy import String, Integer, Float, JSON, DateTime, UniqueConstraint, ForeignKey
from typing import Optional
from sqlalchemy.orm import Mapped, mapped_column
from app.core.database import Base


class BenchmarkResult(Base):
    """Chosen parcel (or empty slot) per benchmark type for a given run."""

    __tablename__ = "benchmark_results"
    __table_args__ = (UniqueConstraint("run_id", "benchmark_type"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(Integer, ForeignKey("benchmark_selection_runs.id"))
    land_transaction_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("land_transactions.id"))
    benchmark_type: Mapped[str] = mapped_column(String(50), nullable=False)
    city: Mapped[Optional[str]] = mapped_column(String(100))
    district: Mapped[Optional[str]] = mapped_column(String(100))
    zoning: Mapped[Optional[str]] = mapped_column(String(100))
    area_sqm: Mapped[Optional[float]] = mapped_column(Float)
    road_width_m: Mapped[Optional[float]] = mapped_column(Float)
    price_per_sqm: Mapped[Optional[float]] = mapped_column(Float)
    selection_score: Mapped[Optional[float]] = mapped_column(Float)
    score_breakdown_json: Mapped[Optional[dict]] = mapped_column(JSON)
    selection_method: Mapped[Optional[str]] = mapped_column(String(50), default="rule_based")
    cluster_metadata_json: Mapped[Optional[dict]] = mapped_column(JSON)
    assigned_valuation_method: Mapped[str] = mapped_column(String(100), default="Market Method")
    validation_flags_json: Mapped[Optional[dict]] = mapped_column(JSON)
    candidate_pool_size: Mapped[Optional[int]] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
