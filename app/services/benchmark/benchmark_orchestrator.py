"""
Stage 1 benchmark selection orchestration.

Pipeline order:
  1. Load all land transactions
  2. Normalize (sets ``is_cleaned``, derived price/area fields, city medians)
  3. Flag price outliers and assign ``price_percentile``
  4. Write cleaning columns back to the DB
  5. Run five selectors (in-memory pandas), then ``resolve_duplicate_districts``
  6. ``validate`` (flags, warnings, spread checks); merge resolution notes into warnings
  7. Persist ``BenchmarkRun`` + five ``BenchmarkResult`` rows (nullable slots)
  8. Audit log

The session is not committed here — the FastAPI ``get_db`` dependency commits after success.
"""

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Any
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.config import Settings
from app.models.benchmark_run import BenchmarkRun
from app.models.benchmark_result import BenchmarkResult
from app.models.district_price_trend import DistrictPriceTrend
from app.repositories.land_transaction_repo import LandTransactionRepository
from app.repositories.benchmark_repo import BenchmarkRepository
from app.services.ingestion.import_service import ImportService
from app.services.ingestion.normalization_service import NormalizationService
from app.services.ingestion.outlier_service import OutlierService
from app.services.benchmark.market_average_selector import MarketAverageSelector
from app.services.benchmark.prime_selector import PrimeSelector
from app.services.benchmark.secondary_selector import SecondarySelector
from app.services.benchmark.large_dev_selector import LargeDevSelector
from app.services.benchmark.emerging_selector import EmergingSelector
from app.services.benchmark.benchmark_validation_service import BenchmarkValidationService
from app.services.audit.audit_service import AuditService

logger = logging.getLogger(__name__)

BENCHMARK_TYPES = ["market_average", "prime", "secondary", "large_dev", "emerging"]


@dataclass
class BenchmarkRunResult:
    """In-memory summary returned to callers (e.g. scheduler) after a run completes."""

    run_uuid: str
    status: str
    trigger_type: str
    started_at: datetime
    completed_at: Optional[datetime]
    total_candidates: int
    clean_candidates: int
    benchmarks_found: int
    filter_config: dict
    benchmarks: dict[str, Optional[dict]]
    validation: dict
    notes: Optional[str]
    db_run_id: int
    created_at: datetime


async def _load_rising_districts(session: AsyncSession) -> set:
    """District names with ``trend_direction == 'rising'`` for the Emerging benchmark."""
    result = await session.execute(
        select(DistrictPriceTrend)
        .where(DistrictPriceTrend.trend_direction == "rising")
    )
    rows = result.scalars().all()
    return {r.district for r in rows if r.district}


async def run_benchmark_selection(
    session: AsyncSession,
    settings: Settings,
    trigger_type: str = "manual",
) -> BenchmarkRunResult:
    """
    Execute the full Stage 1 benchmark selection pipeline.

    Side effects: updates ``land_transactions`` cleaning fields; inserts run/results;
    appends an audit row. On failure, marks the run ``failed`` and re-raises.

    Args:
        session: Active async session (uncommitted until caller commits).
        settings: Thresholds and weights for selectors/validation.
        trigger_type: ``manual`` (API) or ``scheduled`` (job).

    Returns:
        ``BenchmarkRunResult`` including resolved benchmarks and validation dict.
    """
    tx_repo = LandTransactionRepository(session)
    bm_repo = BenchmarkRepository(session)
    audit_svc = AuditService(session)

    run_uuid = str(uuid.uuid4())
    started_at = datetime.utcnow()

    # --- Create run record in DB (status=running) ---
    bm_run = BenchmarkRun(
        run_uuid=run_uuid,
        run_name=f"benchmark_run_{started_at.strftime('%Y%m%d_%H%M%S')}",
        status="running",
        trigger_type=trigger_type,
        started_at=started_at,
        created_by="system",
        created_at=started_at,
    )
    bm_run = await bm_repo.create_run(bm_run)

    try:
        # --- Load transactions ---
        raw_transactions = await tx_repo.get_all_for_processing()
        total_candidates = len(raw_transactions)
        logger.info(f"[{run_uuid}] Loaded {total_candidates} transactions.")

        raw_dicts = [ImportService.to_dict(tx) for tx in raw_transactions]
        df = pd.DataFrame(raw_dicts)

        if df.empty:
            raise ValueError("No transactions available for benchmark selection.")

        # --- Normalize ---
        df = NormalizationService.normalize(df)

        # --- Flag outliers ---
        df = OutlierService.flag_outliers(df, settings)

        # --- Persist cleaning fields back to DB ---
        updates = []
        for _, row in df.iterrows():
            updates.append({
                "id": int(row["id"]),
                "is_cleaned": bool(row["is_cleaned"]),
                "is_outlier": bool(row.get("is_outlier", False)),
                "price_per_sqm": float(row["price_per_sqm"]) if pd.notna(row.get("price_per_sqm")) else None,
                "standardized_total_price": float(row["standardized_total_price"]) if pd.notna(row.get("standardized_total_price")) else None,
                "area_category": str(row["area_category"]) if pd.notna(row.get("area_category")) else None,
                "price_percentile": float(row["price_percentile"]) if pd.notna(row.get("price_percentile")) else None,
                "price_band": str(row["price_band"]) if pd.notna(row.get("price_band")) else None,
                "city_road_median": float(row["city_road_median"]) if pd.notna(row.get("city_road_median")) else None,
                "city_ppsqm_median": float(row["city_ppsqm_median"]) if pd.notna(row.get("city_ppsqm_median")) else None,
            })
        await tx_repo.bulk_update_cleaning_fields(updates)

        # Logged count only: ``is_cleaned`` / ``is_outlier`` exclude rows from selector pools.
        clean_df = df[(df["is_cleaned"] == False) & (df["is_outlier"] == False)]
        clean_candidates = len(clean_df)
        logger.info(f"[{run_uuid}] Clean candidates: {clean_candidates}/{total_candidates}")

        # --- Load rising districts ---
        rising_districts = await _load_rising_districts(session)

        # --- Run all 5 selectors ---
        selectors = {
            "market_average": MarketAverageSelector(settings),
            "prime": PrimeSelector(settings),
            "secondary": SecondarySelector(settings),
            "large_dev": LargeDevSelector(settings),
            "emerging": EmergingSelector(settings),
        }

        # Each selector reads the full ``df`` (same medians/percentiles as persisted).
        benchmarks: dict[str, Optional[dict]] = {}
        for btype, selector in selectors.items():
            if btype == "emerging":
                result = selector.select(df, rising_districts=rising_districts)
            else:
                result = selector.select(df)
            benchmarks[btype] = result
            logger.info(f"[{run_uuid}] {btype}: {'found' if result else 'NOT FOUND'}")

        # --- Resolve duplicate districts (re-pick lower-priority benchmarks when possible) ---
        validator = BenchmarkValidationService(settings)
        benchmarks, district_resolution_notes = validator.resolve_duplicate_districts(
            benchmarks, df, selectors, rising_districts
        )

        # --- Validate ---
        validation = validator.validate(benchmarks)
        validation["warnings"] = list(validation["warnings"]) + district_resolution_notes
        benchmarks_found = validation["benchmarks_found"]

        # --- Persist benchmark results ---
        filter_config = {
            "outlier_low_pct": settings.OUTLIER_LOW_PCT,
            "outlier_high_pct": settings.OUTLIER_HIGH_PCT,
            "b1_pct_range": [settings.B1_PCT_MIN, settings.B1_PCT_MAX],
            "b1_road_tolerance": settings.B1_ROAD_TOLERANCE,
            "b2_pct_min": settings.B2_PCT_MIN,
            "b2_prime_zonings": settings.B2_PRIME_ZONINGS,
            "b3_pct_max": settings.B3_PCT_MAX,
            "b4_iqr": [settings.B4_IQR_LOW, settings.B4_IQR_HIGH],
            "b4_price_percentile_range": [
                settings.B4_IQR_LOW * 100.0,
                settings.B4_IQR_HIGH * 100.0,
            ],
            "b4_dev_zonings": settings.B4_DEV_ZONINGS,
            "b5_months_lookback": settings.B5_MONTHS_LOOKBACK,
            "b5_pct_range": [settings.B5_PCT_MIN, settings.B5_PCT_MAX],
            "rising_districts": list(rising_districts),
        }

        completed_at = datetime.utcnow()
        for btype in BENCHMARK_TYPES:
            bm_data = benchmarks.get(btype)
            bm_result = BenchmarkResult(
                run_id=bm_run.id,
                land_transaction_id=bm_data["land_transaction_id"] if bm_data else None,
                benchmark_type=btype,
                city=bm_data.get("city") if bm_data else None,
                district=bm_data.get("district") if bm_data else None,
                zoning=bm_data.get("zoning") if bm_data else None,
                area_sqm=bm_data.get("area_sqm") if bm_data else None,
                road_width_m=bm_data.get("road_width_m") if bm_data else None,
                price_per_sqm=bm_data.get("price_per_sqm") if bm_data else None,
                selection_score=bm_data.get("selection_score") if bm_data else None,
                score_breakdown_json=bm_data.get("score_breakdown") if bm_data else None,
                validation_flags_json=validation,
                candidate_pool_size=bm_data.get("candidate_pool_size") if bm_data else None,
                created_at=completed_at,
            )
            await bm_repo.create_result(bm_result)

        # --- Update run record ---
        bm_run.status = "completed"
        bm_run.completed_at = completed_at
        bm_run.total_candidates = total_candidates
        bm_run.clean_candidates = clean_candidates
        bm_run.benchmarks_found = benchmarks_found
        bm_run.filter_config_json = filter_config
        bm_run.validation_result_json = validation
        await bm_repo.update_run(bm_run)

        # --- Write audit log ---
        await audit_svc.log(
            entity_type="benchmark_run",
            entity_id=bm_run.id,
            action="completed",
            after_data={
                "run_uuid": run_uuid,
                "benchmarks_found": benchmarks_found,
                "is_valid": validation["is_valid"],
                "flags": validation["flags"],
            },
        )

        return BenchmarkRunResult(
            run_uuid=run_uuid,
            status="completed",
            trigger_type=trigger_type,
            started_at=started_at,
            completed_at=completed_at,
            total_candidates=total_candidates,
            clean_candidates=clean_candidates,
            benchmarks_found=benchmarks_found,
            filter_config=filter_config,
            benchmarks=benchmarks,
            validation=validation,
            notes=None,
            db_run_id=bm_run.id,
            created_at=bm_run.created_at,
        )

    except Exception as exc:
        logger.exception(f"[{run_uuid}] Benchmark selection failed: {exc}")
        bm_run.status = "failed"
        bm_run.notes = str(exc)
        bm_run.completed_at = datetime.utcnow()
        await bm_repo.update_run(bm_run)
        raise
