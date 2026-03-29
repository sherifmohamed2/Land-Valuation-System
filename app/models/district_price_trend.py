from datetime import datetime
from sqlalchemy import String, Integer, Float, DateTime, UniqueConstraint
from typing import Optional
from sqlalchemy.orm import Mapped, mapped_column
from app.core.database import Base


class DistrictPriceTrend(Base):
    """Aggregated district stats; ``trend_direction`` feeds Emerging benchmark eligibility."""

    __tablename__ = "district_price_trends"
    __table_args__ = (UniqueConstraint("city", "district", "period_start"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    city: Mapped[Optional[str]] = mapped_column(String(100))
    district: Mapped[Optional[str]] = mapped_column(String(100))
    period_start: Mapped[Optional[str]] = mapped_column(String(10))
    period_end: Mapped[Optional[str]] = mapped_column(String(10))
    avg_price_per_sqm: Mapped[Optional[float]] = mapped_column(Float)
    transaction_count: Mapped[Optional[int]] = mapped_column(Integer)
    trend_direction: Mapped[Optional[str]] = mapped_column(String(20))
    computed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
