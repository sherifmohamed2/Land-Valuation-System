"""
prime_selector.py

Benchmark 2 — Prime

Selection criteria:
  - is_cleaned = False AND is_outlier = False
  - price_percentile >= B2_PCT_MIN (default 75%)
  - zoning in B2_PRIME_ZONINGS (default: Commercial, Mixed Use, Residential-Prime)
  - road_width_m > city_road_median (strictly wider than city median road)

Tiebreaker: highest tiebreaker_score from BenchmarkScoringService.
"""

import logging
from typing import Optional, Set
import pandas as pd
from app.core.config import Settings
from app.services.benchmark.benchmark_scoring_service import BenchmarkScoringService

logger = logging.getLogger(__name__)


class PrimeSelector:
    """Benchmark 2 — high-end commercial / prime zoning with road wider than city median."""

    BENCHMARK_TYPE = "prime"

    def __init__(self, settings: Settings):
        self.settings = settings
        self.scorer = BenchmarkScoringService(settings)

    @staticmethod
    def _pick_ranked(
        scored: pd.DataFrame,
        exclude_ids: Optional[Set[int]],
        forbidden_districts: Optional[Set[str]],
    ) -> Optional[pd.Series]:
        if scored.empty:
            return None
        ex = exclude_ids or set()
        fb = forbidden_districts or set()
        ordered = scored.sort_values(by=["tiebreaker_score", "id"], ascending=[False, True])
        for _, row in ordered.iterrows():
            rid = int(row["id"])
            if rid in ex:
                continue
            d = row.get("district")
            if fb and d is not None and d in fb:
                continue
            return row
        return None

    def select(
        self,
        df: pd.DataFrame,
        exclude_ids: Optional[Set[int]] = None,
        forbidden_districts: Optional[Set[str]] = None,
    ) -> Optional[dict]:
        s = self.settings
        eligible = df[
            (df["is_cleaned"] == False)
            & (df["is_outlier"] == False)
            & df["price_percentile"].notna()
            & (df["price_percentile"] >= s.B2_PCT_MIN)
            & df["zoning"].isin(s.B2_PRIME_ZONINGS)
            & df["road_width_m"].notna()
            & df["city_road_median"].notna()
            & (df["road_width_m"] > df["city_road_median"])
        ].copy()

        if eligible.empty:
            logger.warning(f"{self.BENCHMARK_TYPE}: no candidates found.")
            return None

        logger.info(f"{self.BENCHMARK_TYPE}: {len(eligible)} eligible candidates, scoring...")
        city_median = eligible["city_ppsqm_median"].iloc[0] if "city_ppsqm_median" in eligible.columns else None
        scored = self.scorer.score_candidates(eligible, city_ppsqm_median=city_median)
        best = self._pick_ranked(scored, exclude_ids, forbidden_districts)
        if best is None:
            return None
        return self._to_result(best, len(eligible))

    def _to_result(self, row: pd.Series, pool_size: int) -> dict:
        return {
            "benchmark_type": self.BENCHMARK_TYPE,
            "land_transaction_id": int(row["id"]),
            "city": row.get("city"),
            "district": row.get("district"),
            "zoning": row.get("zoning"),
            "area_sqm": float(row["area_sqm"]) if pd.notna(row.get("area_sqm")) else None,
            "road_width_m": float(row["road_width_m"]) if pd.notna(row.get("road_width_m")) else None,
            "price_per_sqm": float(row["price_per_sqm"]) if pd.notna(row.get("price_per_sqm")) else None,
            "selection_score": float(row["tiebreaker_score"]),
            "score_breakdown": self.scorer.build_score_breakdown(row),
            "candidate_pool_size": pool_size,
        }
