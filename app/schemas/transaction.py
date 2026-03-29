"""API models for land transaction listing and detail."""
from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class TransactionBase(BaseModel):
    external_ref: Optional[str] = None
    asset_type: str = "Land"
    region: Optional[str] = None
    city: Optional[str] = None
    district: Optional[str] = None
    street: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    zoning: Optional[str] = None
    land_use: Optional[str] = None
    area_sqm: Optional[float] = None
    frontage_m: Optional[float] = None
    depth_m: Optional[float] = None
    road_width_m: Optional[float] = None
    total_price: Optional[float] = None
    currency_code: str = "SAR"
    transaction_date: Optional[str] = None
    utilities_json: Optional[dict] = None
    utility_count: Optional[int] = None
    verification_status: str = "unverified"
    data_source: Optional[str] = None


class TransactionResponse(TransactionBase):
    id: int
    standardized_total_price: Optional[float] = None
    price_per_sqm: Optional[float] = None
    is_cleaned: bool
    is_outlier: bool
    area_category: Optional[str] = None
    price_percentile: Optional[float] = None
    price_band: Optional[str] = None
    city_road_median: Optional[float] = None
    city_ppsqm_median: Optional[float] = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class TransactionListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[TransactionResponse]
