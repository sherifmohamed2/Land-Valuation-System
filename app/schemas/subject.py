"""Stage 2 subject land request/response shapes (routes not implemented)."""
from pydantic import BaseModel
from typing import Optional


class SubjectCreate(BaseModel):
    subject_code: Optional[str] = None
    asset_type: str = "Land"
    region: Optional[str] = None
    city: Optional[str] = None
    district: Optional[str] = None
    street: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    area_sqm: Optional[float] = None
    frontage_m: Optional[float] = None
    depth_m: Optional[float] = None
    zoning: Optional[str] = None
    far: Optional[float] = None
    csr: Optional[float] = None
    utilities_json: Optional[dict] = None
    distance_to_services_m: Optional[float] = None
    distance_to_main_road_m: Optional[float] = None
    commercial_grade: Optional[str] = None
    administrative_grade: Optional[str] = None


class SubjectResponse(SubjectCreate):
    id: int
    subject_price_per_sqm: Optional[float] = None

    model_config = {"from_attributes": True}
