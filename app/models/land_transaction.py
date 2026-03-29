import uuid
from datetime import datetime
from sqlalchemy import String, Float, Integer, Boolean, JSON, DateTime, Text
from typing import Optional
from sqlalchemy.orm import Mapped, mapped_column
from app.core.database import Base


class LandTransaction(Base):
    """Historical land sale/transfer facts; Stage 1 reads and updates cleaning/derived columns."""

    __tablename__ = "land_transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    external_ref: Mapped[Optional[str]] = mapped_column(String(100))
    asset_type: Mapped[str] = mapped_column(String(50), nullable=False)
    region: Mapped[Optional[str]] = mapped_column(String(100))
    city: Mapped[Optional[str]] = mapped_column(String(100))
    district: Mapped[Optional[str]] = mapped_column(String(100))
    street: Mapped[Optional[str]] = mapped_column(String(200))
    latitude: Mapped[Optional[float]] = mapped_column(Float)
    longitude: Mapped[Optional[float]] = mapped_column(Float)
    zoning: Mapped[Optional[str]] = mapped_column(String(100))
    land_use: Mapped[Optional[str]] = mapped_column(String(100))
    area_sqm: Mapped[Optional[float]] = mapped_column(Float)
    frontage_m: Mapped[Optional[float]] = mapped_column(Float)
    depth_m: Mapped[Optional[float]] = mapped_column(Float)
    road_width_m: Mapped[Optional[float]] = mapped_column(Float)
    total_price: Mapped[Optional[float]] = mapped_column(Float)
    currency_code: Mapped[str] = mapped_column(String(10), default="SAR")
    standardized_total_price: Mapped[Optional[float]] = mapped_column(Float)
    price_per_sqm: Mapped[Optional[float]] = mapped_column(Float)
    transaction_date: Mapped[Optional[str]] = mapped_column(String(10))   # YYYY-MM-DD
    utilities_json: Mapped[Optional[dict]] = mapped_column(JSON)
    utility_count: Mapped[Optional[int]] = mapped_column(Integer)
    verification_status: Mapped[str] = mapped_column(String(50), default="unverified")
    data_source: Mapped[Optional[str]] = mapped_column(String(100))
    # True → excluded from benchmark pool (invalid / non-SAR / non-land). Outliers excluded separately.
    is_cleaned: Mapped[bool] = mapped_column(Boolean, default=False)
    is_outlier: Mapped[bool] = mapped_column(Boolean, default=False)
    area_category: Mapped[Optional[str]] = mapped_column(String(20))
    price_percentile: Mapped[Optional[float]] = mapped_column(Float)
    price_band: Mapped[Optional[str]] = mapped_column(String(20))
    city_road_median: Mapped[Optional[float]] = mapped_column(Float)
    city_ppsqm_median: Mapped[Optional[float]] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
