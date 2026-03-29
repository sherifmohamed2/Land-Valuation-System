"""
benchmark_scoring_service.py

Tiebreaker scoring for benchmark candidate selection.
When multiple records satisfy the same benchmark rule, this service scores them
on 4 dimensions and selects the highest-scoring candidate.

Scoring dimensions (weights must sum to 1.0 — enforced in settings):
  1. price_stability   (0.35) — inverse of price_per_sqm variability vs city median
  2. completeness      (0.25) — ratio of non-null important fields
  3. centrality        (0.20) — distance from district centroid (closer = higher score)
  4. recency           (0.20) — exponential decay from transaction_date

All weights read from settings — never hardcoded.
"""

import logging
import math
from datetime import datetime, date
from typing import Optional
import pandas as pd
import numpy as np
from app.core.config import Settings

logger = logging.getLogger(__name__)

COMPLETENESS_FIELDS = [
    "city", "district", "zoning", "area_sqm", "road_width_m",
    "price_per_sqm", "latitude", "longitude", "transaction_date"
]


class BenchmarkScoringService:
    """Computes ``tiebreaker_score`` and per-dimension breakdowns for candidate rows."""

    def __init__(self, settings: Settings):
        self.settings = settings

    def score_candidates(
        self,
        candidates: pd.DataFrame,
        city_ppsqm_median: Optional[float] = None,
        district_centroid: Optional[tuple[float, float]] = None,
    ) -> pd.DataFrame:
        """
        Score each candidate record on 4 dimensions.
        Returns candidates DataFrame with a new 'tiebreaker_score' column and
        individual dimension columns.
        """
        if candidates.empty:
            return candidates

        df = candidates.copy()

        # 1. Price stability score
        df["_score_price_stability"] = self._price_stability_score(df, city_ppsqm_median)

        # 2. Completeness score
        df["_score_completeness"] = self._completeness_score(df)

        # 3. Centrality score
        df["_score_centrality"] = self._centrality_score(df, district_centroid)

        # 4. Recency score
        df["_score_recency"] = self._recency_score(df)

        s = self.settings
        df["tiebreaker_score"] = (
            s.SCORE_WEIGHT_PRICE_STABILITY * df["_score_price_stability"]
            + s.SCORE_WEIGHT_COMPLETENESS   * df["_score_completeness"]
            + s.SCORE_WEIGHT_CENTRALITY     * df["_score_centrality"]
            + s.SCORE_WEIGHT_RECENCY        * df["_score_recency"]
        ).round(6)

        return df

    def _price_stability_score(self, df: pd.DataFrame, median: Optional[float]) -> pd.Series:
        if median is None or median == 0:
            return pd.Series(1.0, index=df.index)
        deviation = (df["price_per_sqm"] - median).abs() / median
        # Score: 1 at deviation=0, approaching 0 as deviation grows
        return (1 / (1 + deviation)).round(6)

    def _completeness_score(self, df: pd.DataFrame) -> pd.Series:
        present = df[COMPLETENESS_FIELDS].notna().sum(axis=1)
        return (present / len(COMPLETENESS_FIELDS)).round(6)

    def _centrality_score(
        self,
        df: pd.DataFrame,
        centroid: Optional[tuple[float, float]],
    ) -> pd.Series:
        if centroid is None:
            return pd.Series(1.0, index=df.index)
        lat_c, lng_c = centroid
        has_coords = df["latitude"].notna() & df["longitude"].notna()
        scores = pd.Series(0.5, index=df.index)  # default for missing coords
        if has_coords.any():
            # Simple Euclidean approximation in degrees
            dist = ((df.loc[has_coords, "latitude"] - lat_c) ** 2
                    + (df.loc[has_coords, "longitude"] - lng_c) ** 2) ** 0.5
            # Max possible ~0.2 degrees in typical city scope
            scores[has_coords] = (1 / (1 + dist * 100)).round(6)
        return scores

    def _recency_score(self, df: pd.DataFrame) -> pd.Series:
        today = date.today()
        decay_months = max(self.settings.SCORE_RECENCY_DECAY_MONTHS, 1)
        scores = pd.Series(0.0, index=df.index)
        for idx, row in df.iterrows():
            try:
                tx_date = date.fromisoformat(str(row["transaction_date"])[:10])
                months_ago = (today - tx_date).days / 30.0
                scores[idx] = round(math.exp(-months_ago / decay_months), 6)
            except Exception:
                scores[idx] = 0.0
        return scores

    def build_score_breakdown(self, row: pd.Series) -> dict:
        return {
            "price_stability": round(float(row.get("_score_price_stability", 0)), 4),
            "completeness": round(float(row.get("_score_completeness", 0)), 4),
            "centrality": round(float(row.get("_score_centrality", 0)), 4),
            "recency": round(float(row.get("_score_recency", 0)), 4),
            "total": round(float(row.get("tiebreaker_score", 0)), 6),
        }
