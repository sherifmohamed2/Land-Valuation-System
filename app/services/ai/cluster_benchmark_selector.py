"""
cluster_benchmark_selector.py — AI-Powered Stage 1 Benchmark Selection

Replaces the five fixed-rule selectors (MarketAverageSelector, PrimeSelector, etc.)
with a K-Means clustering approach. Instead of relying on hardcoded percentile
bands and zoning rules, this selector lets the data speak for itself:

  1. Build a feature matrix from the clean transaction pool.
  2. Standardize features (StandardScaler) to give each dimension equal weight.
  3. Run KMeans(n_clusters=5, random_state=42) to discover 5 market segments.
  4. For each cluster, pick the transaction *closest to its centroid* as the
     most representative benchmark candidate.
  5. Map the 5 clusters → the 5 benchmark slot names by sorting on mean
     price_per_sqm:
       - Lowest mean  → secondary
       - Mid-low mean → market_average
       - Middle mean  → large_dev  (largest mean area in mid group)
       - Mid-high     → emerging   (most recent in mid group)
       - Highest mean → prime
  6. Return the same ``dict[str, Optional[dict]]`` interface as the rule-based
     selectors, so the orchestrator requires zero structural changes.

Each returned benchmark dict carries three extra AI metadata keys:
  - ``selection_method``  : always ``"ai_kmeans"``
  - ``cluster_id``        : integer index of the cluster source
  - ``cluster_size``      : count of transactions in that cluster
  - ``centroid_distance`` : Euclidean distance from the centroid (lower = more central)
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer

from app.services.benchmark.benchmark_scoring_service import BenchmarkScoringService
from app.core.config import Settings

logger = logging.getLogger(__name__)

# Numeric and categorical raw features fed to the clustering algorithm
_NUMERIC_FEATURES = ["area_sqm", "road_width_m", "price_per_sqm", "price_percentile"]
_OPTION_FEATURES = ["frontage_m", "depth_m"]       # optional — included when present & non-null >50 %
_ZONING_DUMMIES = True                             # one-hot encode zoning column

BENCHMARK_TYPES = ["market_average", "prime", "secondary", "large_dev", "emerging"]


class ClusterBenchmarkSelector:
    """
    AI-powered Stage 1 selector using K-Means unsupervised clustering.

    Args:
        settings: Application settings (provides ``AI_N_CLUSTERS`` and scoring weights).
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.n_clusters: int = settings.AI_N_CLUSTERS
        self.scoring_svc = BenchmarkScoringService(settings)

    # ------------------------------------------------------------------
    # Public interface (matches existing selector dict contract)
    # ------------------------------------------------------------------

    def select(self, df: pd.DataFrame) -> dict[str, Optional[dict]]:
        """
        Run K-Means clustering on the *clean pool* of ``df`` and return a
        ``{benchmark_type: transaction_dict | None}`` mapping.

        Args:
            df: Full normalized + outlier-flagged DataFrame from the orchestrator.

        Returns:
            Dict with exactly the 5 benchmark slot keys; value is ``None`` when
            a slot could not be filled (e.g. not enough clean data).
        """
        clean_df = df[(df["is_cleaned"] == False) & (df["is_outlier"] == False)].copy()
        n = len(clean_df)

        if n < self.n_clusters:
            logger.warning(
                "ClusterBenchmarkSelector: only %d clean records — need at least %d. "
                "Returning all None slots.",
                n, self.n_clusters,
            )
            return {btype: None for btype in BENCHMARK_TYPES}

        # --- Build feature matrix ---
        feature_matrix, clean_df = self._build_features(clean_df)

        # --- Fit K-Means pipeline ---
        pipeline = Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ])
        X_scaled = pipeline.fit_transform(feature_matrix)
        km = KMeans(n_clusters=self.n_clusters, random_state=42, n_init=10)
        km.fit(X_scaled)

        clean_df = clean_df.copy()
        clean_df["_cluster"] = km.labels_

        # Centroid distances in scaled space
        centroids = km.cluster_centers_
        distances = np.linalg.norm(X_scaled - centroids[km.labels_], axis=1)
        clean_df["_centroid_dist"] = distances

        # --- Map clusters → slot names ---
        slot_map = self._map_clusters_to_slots(clean_df)
        logger.info(
            "ClusterBenchmarkSelector: cluster→slot mapping: %s",
            {v: k for k, v in slot_map.items()},
        )

        # --- Pick centroid-closest record per slot ---
        results: dict[str, Optional[dict]] = {}
        for slot_name, cluster_id in slot_map.items():
            cluster_rows = clean_df[clean_df["_cluster"] == cluster_id]
            if cluster_rows.empty:
                results[slot_name] = None
                continue

            # Among the cluster, pick the row closest to its centroid
            best_row = cluster_rows.loc[cluster_rows["_centroid_dist"].idxmin()]
            tx_dict = best_row.to_dict()

            tx_dict["selection_method"] = "ai_kmeans"
            if "land_transaction_id" not in tx_dict and "id" in tx_dict:
                tx_dict["land_transaction_id"] = int(tx_dict["id"])
            tx_dict["cluster_id"] = int(cluster_id)
            tx_dict["cluster_size"] = int(len(cluster_rows))
            tx_dict["centroid_distance"] = float(best_row["_centroid_dist"])
            tx_dict["candidate_pool_size"] = n

            # Tiebreaker score for auditability (same as rule-based path)
            candidates = self.scoring_svc.score_candidates(cluster_rows)
            best_row_id = best_row.get("id")
            
            # Find the score specifically for the row we selected (centroid-closest)
            scored_candidates = candidates[candidates["id"] == best_row_id]
            
            if not scored_candidates.empty:
                best_scored_row = scored_candidates.iloc[0]
                tx_dict["selection_score"] = float(best_scored_row.get("tiebreaker_score", 0.0))
                tx_dict["score_breakdown"] = self.scoring_svc.build_score_breakdown(best_scored_row)
            else:
                logger.warning(
                    "ClusterBenchmarkSelector: best_row ID %s missing from scored candidates for slot %s.",
                    best_row_id, slot_name
                )
                tx_dict["selection_score"] = None
                tx_dict["score_breakdown"] = {}

            results[slot_name] = tx_dict
            logger.info(
                "ClusterBenchmarkSelector: slot=%s cluster=%d size=%d tx_id=%s dist=%.4f",
                slot_name, cluster_id, len(cluster_rows),
                tx_dict.get("land_transaction_id"), best_row["_centroid_dist"],
            )

        return results

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_features(self, df: pd.DataFrame) -> tuple[np.ndarray, pd.DataFrame]:
        """
        Construct the numeric feature matrix for K-Means.

        Always includes: area_sqm, road_width_m, price_per_sqm, price_percentile.
        Optionally includes: frontage_m, depth_m (when >50% non-null in the pool).
        Optionally one-hot encodes: zoning (top-level categories only).

        Returns:
            Tuple of (raw feature array, df with valid rows only).
        """
        cols: list[str] = []

        for col in _NUMERIC_FEATURES:
            if col in df.columns:
                cols.append(col)

        for col in _OPTION_FEATURES:
            if col in df.columns and df[col].notna().mean() > 0.5:
                cols.append(col)

        feature_df = df[cols].copy()

        if _ZONING_DUMMIES and "zoning" in df.columns:
            zoning_dummies = pd.get_dummies(df["zoning"].fillna("Unknown"), prefix="z")
            feature_df = pd.concat([feature_df, zoning_dummies], axis=1)

        return feature_df.values.astype(float), df

    def _map_clusters_to_slots(self, df: pd.DataFrame) -> dict[str, int]:
        """
        Assign each cluster index to one of the 5 slot names.
        """
        # Safely parse dates to datetime to avoid median crash with strings/nulls
        df_copy = df.copy()
        df_copy["_transaction_date"] = pd.to_datetime(df_copy.get("transaction_date"), errors="coerce")
        # Fill missing dates with a very old date so they don't break median and go to the bottom of recency
        df_copy["_transaction_date"] = df_copy["_transaction_date"].fillna(pd.Timestamp("1970-01-01"))

        cluster_stats = (
            df_copy.groupby("_cluster")
            .agg(
                mean_ppsqm=("price_per_sqm", "mean"),
                mean_area=("area_sqm", "mean"),
                median_date=("_transaction_date", "median"),
            )
            .rename_axis("cluster_id")
            .reset_index()
            .sort_values("mean_ppsqm")
            .reset_index(drop=True)
        )

        sorted_ids = cluster_stats["cluster_id"].tolist()

        # Cheapest → secondary; priciest → prime
        secondary_id = int(sorted_ids[0])
        prime_id = int(sorted_ids[-1])
        mid_ids = sorted_ids[1:-1]  # exactly 3 for n_clusters=5

        mid_stats = cluster_stats[cluster_stats["cluster_id"].isin(mid_ids)].copy()

        # Largest area → large_dev
        large_dev_id = int(mid_stats.loc[mid_stats["mean_area"].idxmax(), "cluster_id"])
        mid_stats = mid_stats[mid_stats["cluster_id"] != large_dev_id]

        # Most recent → emerging (handle non-datetime median_date gracefully)
        emerging_id = int(mid_stats.loc[mid_stats["median_date"].idxmax(), "cluster_id"])
        mid_stats = mid_stats[mid_stats["cluster_id"] != emerging_id]

        # Remainder → market_average
        market_avg_id = int(mid_stats.iloc[0]["cluster_id"])

        return {
            "market_average": market_avg_id,
            "prime": prime_id,
            "secondary": secondary_id,
            "large_dev": large_dev_id,
            "emerging": emerging_id,
        }
