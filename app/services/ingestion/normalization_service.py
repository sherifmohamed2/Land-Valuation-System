"""
normalization_service.py

Stage 1 data cleaning and normalization.

Rules applied (in order):
1. Enforce asset_type == "Land"
2. Drop records with total_price <= 0 or area_sqm <= 0 (flag is_cleaned=True)
3. Enforce single currency (SAR) — log any non-SAR records and flag them
4. Compute price_per_sqm = total_price / area_sqm
5. Compute area_category (Small / Medium / Large / Very Large) based on quantiles
6. Compute city_road_median and city_ppsqm_median per city
"""

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

REQUIRED_CURRENCY = "SAR"

# Fixed sqm bins (not city-relative percentiles): [0,400)→Small, [400,1200)→Medium, etc.
AREA_CATEGORY_BREAKS = [0, 400, 1200, 3000, float("inf")]
AREA_CATEGORY_LABELS = ["Small", "Medium", "Large", "Very Large"]


class NormalizationService:
    """Stage 1 cleaning and derived columns on a transaction DataFrame."""

    @staticmethod
    def normalize(df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all normalization rules to the raw DataFrame.

        Returns a new DataFrame with added/updated columns:
          - is_cleaned    (True → excluded from benchmarking)
          - standardized_total_price
          - price_per_sqm
          - area_category
          - city_road_median
          - city_ppsqm_median
        """
        df = df.copy()
        original_count = len(df)

        # --- Rule 1: asset_type filter ---
        mask_non_land = df["asset_type"].str.strip().str.lower() != "land"
        if mask_non_land.any():
            count = mask_non_land.sum()
            logger.warning(f"Normalization: flagging {count} records where asset_type != 'Land'")
            df.loc[mask_non_land, "is_cleaned"] = True

        # --- Rule 2: drop invalid price and area ---
        mask_bad_price = df["total_price"].isna() | (df["total_price"] <= 0)
        mask_bad_area = df["area_sqm"].isna() | (df["area_sqm"] <= 0)
        mask_invalid = mask_bad_price | mask_bad_area
        if mask_invalid.any():
            count = mask_invalid.sum()
            logger.info(
                f"Normalization: flagging {count} records with invalid price or area "
                f"(null_price={mask_bad_price.sum()}, zero_area={mask_bad_area.sum()})"
            )
            df.loc[mask_invalid, "is_cleaned"] = True

        # --- Rule 3: currency consistency ---
        non_sar = df[~df["is_cleaned"] & (df["currency_code"] != REQUIRED_CURRENCY)]
        if not non_sar.empty:
            logger.warning(
                f"Normalization: {len(non_sar)} records with non-{REQUIRED_CURRENCY} currency — "
                "flagging for manual review. FX conversion not implemented (single-currency dataset assumed)."
            )
            df.loc[non_sar.index, "is_cleaned"] = True

        # --- Compute derived fields only for valid records ---
        valid = ~df["is_cleaned"]

        df.loc[valid, "standardized_total_price"] = df.loc[valid, "total_price"]

        df.loc[valid, "price_per_sqm"] = (
            df.loc[valid, "total_price"] / df.loc[valid, "area_sqm"]
        ).round(4)

        # --- area_category ---
        df.loc[valid, "area_category"] = pd.cut(
            df.loc[valid, "area_sqm"],
            bins=AREA_CATEGORY_BREAKS,
            labels=AREA_CATEGORY_LABELS,
            right=False,
        ).astype(str)
        df.loc[~valid, "area_category"] = None

        # --- City-level medians ---
        city_road_medians = (
            df[valid & df["road_width_m"].notna()]
            .groupby("city")["road_width_m"]
            .median()
        )
        city_ppsqm_medians = (
            df[valid & df["price_per_sqm"].notna()]
            .groupby("city")["price_per_sqm"]
            .median()
        )
        df.loc[valid, "city_road_median"] = df.loc[valid, "city"].map(city_road_medians)
        df.loc[valid, "city_ppsqm_median"] = df.loc[valid, "city"].map(city_ppsqm_medians)

        cleaned_count = df["is_cleaned"].sum()
        logger.info(
            f"Normalization complete: {original_count} in → "
            f"{original_count - cleaned_count} valid, {cleaned_count} flagged."
        )
        return df
