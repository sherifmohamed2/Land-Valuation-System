"""
Map benchmark ORM rows to public API response models.

Keeps FastAPI routes thin. JSON columns may be str or already-parsed structures.
"""
from __future__ import annotations

import json
from typing import Any, Optional

from app.schemas.benchmark import (
    BenchmarkResultSchema,
    BenchmarkRunResponse,
    ValidationResult,
)

BENCHMARK_TYPE_ORDER = [
    "market_average",
    "prime",
    "secondary",
    "large_dev",
    "emerging",
]


def _parse_json(value: Any) -> Any:
    """Normalize JSON-ish DB values (same behavior as former route helper)."""
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return {}
    return {}


def build_benchmark_run_response(run, results) -> BenchmarkRunResponse:
    """
    Build ``BenchmarkRunResponse`` from a ``BenchmarkRun`` and its ``BenchmarkResult`` rows.
    """
    result_map = {r.benchmark_type: r for r in results}

    benchmarks: dict[str, Optional[BenchmarkResultSchema]] = {}
    for btype in BENCHMARK_TYPE_ORDER:
        r = result_map.get(btype)
        if r and r.land_transaction_id is not None:
            benchmarks[btype] = BenchmarkResultSchema(
                benchmark_type=btype,
                land_transaction_id=r.land_transaction_id,
                city=r.city,
                district=r.district,
                zoning=r.zoning,
                area_sqm=r.area_sqm,
                road_width_m=r.road_width_m,
                price_per_sqm=r.price_per_sqm,
                selection_score=r.selection_score,
                score_breakdown=_parse_json(r.score_breakdown_json),
                selection_method=r.selection_method or "rule_based",
                cluster_metadata=_parse_json(r.cluster_metadata_json) if r.cluster_metadata_json else None,
                assigned_valuation_method=r.assigned_valuation_method,
                validation_flags=_parse_json(r.validation_flags_json),
                candidate_pool_size=r.candidate_pool_size,
            )
        else:
            benchmarks[btype] = None

    vr_raw = _parse_json(run.validation_result_json) if run.validation_result_json else {}
    validation = (
        ValidationResult(
            is_valid=vr_raw.get("is_valid", False),
            benchmarks_found=vr_raw.get("benchmarks_found", 0),
            flags=vr_raw.get("flags", []),
            warnings=vr_raw.get("warnings", []),
        )
        if vr_raw
        else None
    )

    return BenchmarkRunResponse(
        run_uuid=run.run_uuid,
        status=run.status,
        trigger_type=run.trigger_type,
        started_at=run.started_at,
        completed_at=run.completed_at,
        total_candidates=run.total_candidates,
        clean_candidates=run.clean_candidates,
        benchmarks_found=run.benchmarks_found,
        filter_config=_parse_json(run.filter_config_json),
        benchmarks=benchmarks,
        validation=validation,
        notes=run.notes,
        created_at=run.created_at,
    )
