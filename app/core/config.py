"""
Environment-backed application settings (``pydantic-settings``).

Loads from ``.env`` and the process environment. Groups:
  - Outlier percentiles on ``price_per_sqm``
  - Benchmark rules B1–B5 (percentile bands, zonings, lookback)
  - Tiebreaker weights for benchmark tie resolution (must sum to 1.0)
  - Post-selection validation (min area / price spread)
  - Scheduler cron and placeholder threshold for future ingestion triggers
"""
from pydantic_settings import BaseSettings
from pydantic import model_validator
from typing import List


class Settings(BaseSettings):
    """All Stage 1 thresholds and infrastructure toggles; override via env / ``.env``."""

    DATABASE_URL: str = "sqlite+aiosqlite:///./data/land_valuation.db"
    APP_ENV: str = "development"
    LOG_LEVEL: str = "INFO"

    # Outlier removal
    OUTLIER_LOW_PCT: float = 0.05
    OUTLIER_HIGH_PCT: float = 0.95

    # Benchmark 1 — Market Average
    B1_PCT_MIN: float = 45.0
    B1_PCT_MAX: float = 55.0
    B1_ROAD_TOLERANCE: float = 0.20

    # Benchmark 2 — Prime
    B2_PCT_MIN: float = 75.0
    B2_PRIME_ZONINGS: List[str] = ["Commercial", "Mixed Use", "Residential-Prime"]

    # Benchmark 3 — Secondary
    B3_PCT_MAX: float = 25.0

    # Benchmark 4 — Large Development (IQR band applies to price_percentile, 0–100 scale)
    B4_IQR_LOW: float = 0.25
    B4_IQR_HIGH: float = 0.75
    B4_DEV_ZONINGS: List[str] = ["Commercial", "Mixed Use", "Industrial"]

    # Benchmark 5 — Emerging
    B5_MONTHS_LOOKBACK: int = 12
    B5_PCT_MIN: float = 50.0
    B5_PCT_MAX: float = 70.0

    # Tiebreaker scoring weights (must sum to 1.0)
    SCORE_WEIGHT_PRICE_STABILITY: float = 0.35
    SCORE_WEIGHT_COMPLETENESS: float = 0.25
    SCORE_WEIGHT_CENTRALITY: float = 0.20
    SCORE_WEIGHT_RECENCY: float = 0.20
    SCORE_RECENCY_DECAY_MONTHS: int = 36

    # Post-selection validation thresholds
    VALID_MIN_AREA_DIFF_PCT: float = 0.15
    VALID_MIN_PRICE_DIFF_PCT: float = 0.10

    # AI Benchmark Selection (Stage 1 — K-Means Clustering)
    USE_AI_BENCHMARK_SELECTION: bool = True
    AI_N_CLUSTERS: int = 5  # must match number of benchmark slot types

    # Scheduler
    BENCHMARK_REFRESH_CRON: str = "0 2 1 * *"
    BENCHMARK_NEW_RECORD_THRESHOLD: int = 50

    @model_validator(mode='after')
    def validate_scoring_weights(self):
        total = (self.SCORE_WEIGHT_PRICE_STABILITY + self.SCORE_WEIGHT_COMPLETENESS
                 + self.SCORE_WEIGHT_CENTRALITY + self.SCORE_WEIGHT_RECENCY)
        if abs(total - 1.0) > 0.001:
            raise ValueError(f"Scoring weights must sum to 1.0, got {total:.4f}")
        return self

    class Config:
        env_file = ".env"


settings = Settings()
