from sqlalchemy import String, Integer, Float, JSON, UniqueConstraint
from typing import Optional
from sqlalchemy.orm import Mapped, mapped_column
from app.core.database import Base


class LocationLookup(Base):
    """Reference: district centroids and adjacency (for future location scoring)."""

    __tablename__ = "location_lookup"
    __table_args__ = (UniqueConstraint("city", "district"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    region: Mapped[Optional[str]] = mapped_column(String(100))
    city: Mapped[Optional[str]] = mapped_column(String(100))
    district: Mapped[Optional[str]] = mapped_column(String(100))
    adjacent_districts_json: Mapped[Optional[list]] = mapped_column(JSON)
    latitude_centroid: Mapped[Optional[float]] = mapped_column(Float)
    longitude_centroid: Mapped[Optional[float]] = mapped_column(Float)
