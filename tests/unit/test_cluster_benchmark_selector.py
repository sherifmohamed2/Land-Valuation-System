import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from app.core.config import Settings
from app.services.ai.cluster_benchmark_selector import ClusterBenchmarkSelector


@pytest.fixture
def mock_clean_df():
    """Returns a mock DataFrame representing 20 clean land transactions."""
    now = datetime.utcnow()
    data = []

    # Let's create 5 very distinct "clusters" of data so K-Means works deterministically
    cluster_templates = [
        {"area_sqm": 200, "price_per_sqm": 1000, "road_width_m": 10, "zoning": "Residential"},      # Secondary
        {"area_sqm": 600, "price_per_sqm": 2500, "road_width_m": 15, "zoning": "Residential"},      # Market Average
        {"area_sqm": 1000, "price_per_sqm": 5000, "road_width_m": 20, "zoning": "Commercial"},      # Prime
        {"area_sqm": 5000, "price_per_sqm": 2200, "road_width_m": 30, "zoning": "Industrial"},      # Large Dev
        {"area_sqm": 600, "price_per_sqm": 2800, "road_width_m": 15, "zoning": "Residential"},      # Emerging
    ]

    for cluster_idx, template in enumerate(cluster_templates):
        for i in range(5):  # 5 points per cluster = 25 total
            tx_date = now - timedelta(days=(cluster_idx * 30 + i * 5))
            # Emerging (cluster 4) gets very recent dates to guarantee it wins the emerging slot
            if cluster_idx == 4:
                tx_date = now - timedelta(days=i)

            data.append({
                "id": len(data) + 1,
                "land_transaction_id": len(data) + 1,
                "area_sqm": template["area_sqm"] + np.random.normal(0, 10),
                "price_per_sqm": template["price_per_sqm"] + np.random.normal(0, 50),
                "road_width_m": template["road_width_m"],
                "zoning": template["zoning"],
                "price_percentile": 50.0 + cluster_idx * 10,
                "city_ppsqm_median": 2500,
                "city": "Riyadh",
                "district": "Al Olaya",
                "latitude": 24.0,
                "longitude": 46.0,
                "is_cleaned": False,
                "is_outlier": False,
                "transaction_date": str(tx_date.date()) if i % 2 == 0 else None  # Mix strings and missing dates
            })

    return pd.DataFrame(data)


def test_cluster_benchmark_selector(mock_clean_df):
    settings = Settings(USE_AI_BENCHMARK_SELECTION=True, AI_N_CLUSTERS=5)
    selector = ClusterBenchmarkSelector(settings)

    results = selector.select(mock_clean_df)

    # Assert all 5 slots returned something
    assert len(results) == 5
    assert all(res is not None for res in results.values())

    # Assert AI Metadata and required fields are present
    for slot_name, res in results.items():
        assert res["selection_method"] == "ai_kmeans"
        assert "cluster_id" in res
        assert "cluster_size" in res
        assert "centroid_distance" in res
        assert res["cluster_size"] == 5  # Because we seeded exactly 5 per cluster
        
        # P1 fix verification: scores and ID matching
        assert "land_transaction_id" in res
        assert isinstance(res["land_transaction_id"], int)
        assert "selection_score" in res
        assert res["selection_score"] is not None
        assert "score_breakdown" in res
        assert isinstance(res["score_breakdown"], dict)
        assert "total" in res["score_breakdown"]
        
        # Verify the breakdown actually has dimensions
        assert "price_stability" in res["score_breakdown"]
        assert "completeness" in res["score_breakdown"]

    # Assert no duplicate properties were picked
    picked_ids = [res["id"] for res in results.values()]
    assert len(set(picked_ids)) == 5


def test_cluster_benchmark_selector_skips_when_not_enough_data():
    settings = Settings(USE_AI_BENCHMARK_SELECTION=True, AI_N_CLUSTERS=5)
    selector = ClusterBenchmarkSelector(settings)
    
    # Send only 3 rows
    tiny_df = pd.DataFrame([
        {"is_cleaned": False, "is_outlier": False},
        {"is_cleaned": False, "is_outlier": False},
        {"is_cleaned": False, "is_outlier": False},
    ])
    
    results = selector.select(tiny_df)
    
    # Should safely return dict of Nones
    assert len(results) == 5
    assert all(res is None for res in results.values())
