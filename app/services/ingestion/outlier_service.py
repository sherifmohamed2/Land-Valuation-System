"""
outlier_service.py

Stage 1 outlier detection.

Uses pandas percentile (numpy) to detect top/bottom outliers by price_per_sqm.
Records at or beyond the configured thresholds are flagged is_outlier=True.
Records are never deleted.

Thresholds come from settings:
  - OUTLIER_LOW_PCT  (default 0.05 → bottom 5%)
  - OUTLIER_HIGH_PCT (default 0.95 → top 5%)
"""

import logging
import pandas as pd
import numpy as np
from app.core.config import Settings

logger = logging.getLogger(__name__)


class OutlierService:
    """Flags extreme ``price_per_sqm`` rows; assigns percentile rank for downstream rules."""

    @staticmethod
    def flag_outliers(df: pd.DataFrame, settings: Settings) -> pd.DataFrame:
        """
        Flag outliers in price_per_sqm using configurable percentile bands.

        Only operates on records where is_cleaned=False and price_per_sqm is not null.

        Adds/updates:
          - is_outlier    (True → excluded from benchmark selection)
          - price_percentile (0–100 rank within valid pool)
          - price_band    ("bottom_{n}%", "middle", "top_{n}%")
        """
        df = df.copy()

        eligible = df["is_cleaned"] == False
        valid_prices = df.loc[eligible & df["price_per_sqm"].notna(), "price_per_sqm"]

        if valid_prices.empty:
            logger.warning("OutlierService: no valid price_per_sqm values — skipping outlier detection.")
            return df

        low_threshold = valid_prices.quantile(settings.OUTLIER_LOW_PCT)
        high_threshold = valid_prices.quantile(settings.OUTLIER_HIGH_PCT)

        logger.info(
            f"Outlier thresholds: low={low_threshold:.2f} (p{settings.OUTLIER_LOW_PCT*100:.0f}), "
            f"high={high_threshold:.2f} (p{settings.OUTLIER_HIGH_PCT*100:.0f})"
        )

        # Compute percentile rank for all eligible records
        pct_rank = valid_prices.rank(pct=True) * 100
        df.loc[pct_rank.index, "price_percentile"] = pct_rank.round(2)

        # Flag outliers
        mask_low = eligible & (df["price_per_sqm"] <= low_threshold)
        mask_high = eligible & (df["price_per_sqm"] >= high_threshold)
        df.loc[mask_low | mask_high, "is_outlier"] = True

        # Assign price_band labels
        low_pct_label = f"bottom_{int(settings.OUTLIER_LOW_PCT * 100)}pct"
        high_pct_label = f"top_{int((1 - settings.OUTLIER_HIGH_PCT) * 100)}pct"

        df.loc[mask_low, "price_band"] = low_pct_label
        df.loc[mask_high, "price_band"] = high_pct_label
        df.loc[eligible & ~mask_low & ~mask_high, "price_band"] = "middle"

        n_low = mask_low.sum()
        n_high = mask_high.sum()
        logger.info(f"Outlier detection: {n_low} low outliers, {n_high} high outliers flagged.")

        return df
