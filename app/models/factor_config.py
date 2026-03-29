from datetime import datetime
from sqlalchemy import String, Integer, Float, Boolean, JSON, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from app.core.database import Base


class FactorConfig(Base):
    """Weighted scoring factor definition for Stage 2 (key, weight, method, JSON params)."""

    __tablename__ = "factor_configs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    factor_key: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    factor_name: Mapped[str] = mapped_column(String(200), nullable=False)
    weight: Mapped[float] = mapped_column(Float, nullable=False)
    scoring_method: Mapped[str] = mapped_column(String(50), nullable=False)
    params_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    version: Mapped[int] = mapped_column(Integer, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
