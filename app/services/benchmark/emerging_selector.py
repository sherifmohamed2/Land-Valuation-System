"""
emerging_selector.py

Benchmark 5 — Emerging Market

Selection criteria:
  - is_cleaned = False AND is_outlier = False
  - transaction_date within last B5_MONTHS_LOOKBACK months (default 12)
  - price_percentile between B5_PCT_MIN and B5_PCT_MAX (default 50–70%)
  - district must appear in rising_districts (from district_price_trends.trend_direction = "rising")

If rising_districts is empty or no row matches a rising district, returns None (no fallback).
"""

import logging
from datetime import date, timedelta
from typing import Optional, Set
import pandas as pd
from app.core.config import Settings
from app.services.benchmark.benchmark_scoring_service import BenchmarkScoringService

logger = logging.getLogger(__name__)


class EmergingSelector:
    """Benchmark 5 — recent deals in districts flagged as rising in trend data."""

    BENCHMARK_TYPE = "emerging"

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
        rising_districts: Optional[set] = None,
        exclude_ids: Optional[Set[int]] = None,
        forbidden_districts: Optional[Set[str]] = None,
    ) -> Optional[dict]:
        s = self.settings
        rising = rising_districts or set()

        if not rising:
            logger.warning(
                f"{self.BENCHMARK_TYPE}: no rising districts configured — cannot select emerging benchmark."
            )
            return None

        cutoff = date.today() - timedelta(days=s.B5_MONTHS_LOOKBACK * 30)
        cutoff_str = str(cutoff)

        eligible = df[
            (df["is_cleaned"] == False)
            & (df["is_outlier"] == False)
            & df["price_percentile"].notna()
            & (df["price_percentile"] >= s.B5_PCT_MIN)
            & (df["price_percentile"] <= s.B5_PCT_MAX)
            & df["transaction_date"].notna()
            & (df["transaction_date"] >= cutoff_str)
            & df["district"].isin(rising)
        ].copy()

        if eligible.empty:
            logger.warning(
                f"{self.BENCHMARK_TYPE}: no candidates in rising districts after date/percentile filters."
            )
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
