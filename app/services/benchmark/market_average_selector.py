"""
market_average_selector.py

Benchmark 1 — Market Average

Selection criteria (all must be satisfied):
  - is_cleaned = False AND is_outlier = False
  - price_percentile between B1_PCT_MIN and B1_PCT_MAX (default 45–55%)
  - area_category = "Medium"
  - road_width_m within ±B1_ROAD_TOLERANCE of city_road_median

Tiebreaker: highest tiebreaker_score from BenchmarkScoringService.
"""

import logging
from typing import Optional, Set
import pandas as pd
from app.core.config import Settings
from app.services.benchmark.benchmark_scoring_service import BenchmarkScoringService

logger = logging.getLogger(__name__)


class MarketAverageSelector:
    """Benchmark 1 — mid-market parcel vs city road median (see module docstring)."""

    BENCHMARK_TYPE = "market_average"

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
        """
        Select the Market Average benchmark from the clean candidate pool.

        Returns a benchmark dict or None if no candidate satisfies all criteria.
        """
        s = self.settings
        eligible = df[
            (df["is_cleaned"] == False)
            & (df["is_outlier"] == False)
            & df["price_percentile"].notna()
            & (df["price_percentile"] >= s.B1_PCT_MIN)
            & (df["price_percentile"] <= s.B1_PCT_MAX)
            & (df["area_category"] == "Medium")
            & df["road_width_m"].notna()
            & df["city_road_median"].notna()
        ].copy()

        if eligible.empty:
            logger.warning(f"{self.BENCHMARK_TYPE}: no candidates found after primary filters.")
            return None

        # Road width tolerance filter
        tol = s.B1_ROAD_TOLERANCE
        road_ok = (
            (eligible["road_width_m"] >= eligible["city_road_median"] * (1 - tol))
            & (eligible["road_width_m"] <= eligible["city_road_median"] * (1 + tol))
        )
        eligible = eligible[road_ok]

        if eligible.empty:
            logger.warning(f"{self.BENCHMARK_TYPE}: no candidates after road width tolerance filter.")
            return None

        logger.info(f"{self.BENCHMARK_TYPE}: {len(eligible)} eligible candidates, scoring...")
        city_median = eligible["city_ppsqm_median"].iloc[0] if "city_ppsqm_median" in eligible.columns else None
        scored = self.scorer.score_candidates(eligible, city_ppsqm_median=city_median)
        best = self._pick_ranked(scored, exclude_ids, forbidden_districts)
        if best is None:
            return None

        return self._to_result(best, len(eligible))

    def _to_result(self, row: pd.Series, pool_size: int) -> dict:
        scorer = self.scorer
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
            "score_breakdown": scorer.build_score_breakdown(row),
            "candidate_pool_size": pool_size,
        }
