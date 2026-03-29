from sqlalchemy import String, Integer, Boolean, JSON
from typing import Optional
from sqlalchemy.orm import Mapped, mapped_column
from app.core.database import Base


class ZoningLookup(Base):
    """Reference: zoning codes, compatibility, development flags."""

    __tablename__ = "zoning_lookup"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    zoning_code: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    zoning_name: Mapped[Optional[str]] = mapped_column(String(200))
    permitted_use_json: Mapped[Optional[list]] = mapped_column(JSON)
    compatible_zoning_json: Mapped[Optional[list]] = mapped_column(JSON)
    is_high_value: Mapped[bool] = mapped_column(Boolean, default=False)
    allows_development: Mapped[bool] = mapped_column(Boolean, default=False)
