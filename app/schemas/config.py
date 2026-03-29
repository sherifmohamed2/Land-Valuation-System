"""Factor configuration payloads for ``GET /configs/factors``."""
from pydantic import BaseModel
from typing import Optional


class FactorConfigSchema(BaseModel):
    id: int
    factor_key: str
    factor_name: str
    weight: float
    scoring_method: str
    params_json: dict
    is_active: bool
    version: int

    model_config = {"from_attributes": True}


class FactorConfigListResponse(BaseModel):
    total: int
    total_weight: float
    items: list[FactorConfigSchema]
