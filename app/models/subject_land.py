from datetime import datetime
from sqlalchemy import String, Integer, Float, JSON, DateTime
from typing import Optional
from sqlalchemy.orm import Mapped, mapped_column
from app.core.database import Base


class SubjectLand(Base):
    """Parcel under valuation (Stage 2); attributes mirror comparables for scoring."""

    __tablename__ = "subject_lands"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    subject_code: Mapped[Optional[str]] = mapped_column(String(100), unique=True)
    asset_type: Mapped[str] = mapped_column(String(50), default="Land")
    region: Mapped[Optional[str]] = mapped_column(String(100))
    city: Mapped[Optional[str]] = mapped_column(String(100))
    district: Mapped[Optional[str]] = mapped_column(String(100))
    street: Mapped[Optional[str]] = mapped_column(String(200))
    latitude: Mapped[Optional[float]] = mapped_column(Float)
    longitude: Mapped[Optional[float]] = mapped_column(Float)
    area_sqm: Mapped[Optional[float]] = mapped_column(Float)
    frontage_m: Mapped[Optional[float]] = mapped_column(Float)
    depth_m: Mapped[Optional[float]] = mapped_column(Float)
    zoning: Mapped[Optional[str]] = mapped_column(String(100))
    far: Mapped[Optional[float]] = mapped_column(Float)
    csr: Mapped[Optional[float]] = mapped_column(Float)
    utilities_json: Mapped[Optional[dict]] = mapped_column(JSON)
    distance_to_services_m: Mapped[Optional[float]] = mapped_column(Float)
    distance_to_main_road_m: Mapped[Optional[float]] = mapped_column(Float)
    commercial_grade: Mapped[Optional[str]] = mapped_column(String(10))
    administrative_grade: Mapped[Optional[str]] = mapped_column(String(10))
    subject_price_per_sqm: Mapped[Optional[float]] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
