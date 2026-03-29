"""Pydantic shapes for benchmark run API responses (five slots + validation)."""
from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class BenchmarkResultSchema(BaseModel):
    benchmark_type: str
    land_transaction_id: Optional[int] = None
    city: Optional[str] = None
    district: Optional[str] = None
    zoning: Optional[str] = None
    area_sqm: Optional[float] = None
    road_width_m: Optional[float] = None
    price_per_sqm: Optional[float] = None
    selection_score: Optional[float] = None
    score_breakdown: Optional[dict] = None
    selection_method: Optional[str] = "rule_based"
    cluster_metadata: Optional[dict] = None
    assigned_valuation_method: str = "Market Method"
    validation_flags: Optional[dict] = None
    candidate_pool_size: Optional[int] = None

    model_config = {"from_attributes": True}


class ValidationResult(BaseModel):
    is_valid: bool
    benchmarks_found: int
    flags: list[str]
    warnings: list[str]


class BenchmarkRunResponse(BaseModel):
    run_uuid: str
    status: str
    trigger_type: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    total_candidates: Optional[int] = None
    clean_candidates: Optional[int] = None
    benchmarks_found: Optional[int] = None
    filter_config: Optional[dict] = None
    benchmarks: dict[str, Optional[BenchmarkResultSchema]]
    validation: Optional[ValidationResult] = None
    notes: Optional[str] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class BenchmarkRunSummary(BaseModel):
    id: int
    run_uuid: str
    status: str
    trigger_type: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    benchmarks_found: Optional[int] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class BenchmarkRunsListResponse(BaseModel):
    total: int
    items: list[BenchmarkRunSummary]
