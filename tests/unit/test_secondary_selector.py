import pytest
from app.services.benchmark.secondary_selector import SecondarySelector
from app.core.config import Settings


@pytest.fixture
def settings():
    return Settings()


def test_secondary_selects_low_pct(sample_df, settings):
    selector = SecondarySelector(settings)
    result = selector.select(sample_df)
    assert result is not None
    assert result["benchmark_type"] == "secondary"


def test_secondary_returns_none_when_no_low_pct(settings):
    import pandas as pd
    df = pd.DataFrame([
        {"id": 1, "city": "Riyadh", "district": "D1", "zoning": "Residential",
         "area_sqm": 500.0, "road_width_m": 20.0, "price_per_sqm": 5000.0,
         "is_cleaned": False, "is_outlier": False, "price_percentile": 60.0,
         "area_category": "Medium", "city_road_median": 20.0, "city_ppsqm_median": 3500.0,
         "transaction_date": "2025-01-01", "latitude": 24.69, "longitude": 46.72,
         "total_price": 500*5000, "frontage_m": 20.0, "depth_m": 25.0},
    ])
    result = SecondarySelector(settings).select(df)
    assert result is None


def test_secondary_score_within_range(sample_df, settings):
    selector = SecondarySelector(settings)
    result = selector.select(sample_df)
    assert result is not None
    assert 0.0 <= result["selection_score"] <= 1.0


def test_secondary_requires_road_below_city_median(settings):
    import pandas as pd
    df = pd.DataFrame([
        {"id": 1, "city": "Dammam", "district": "D1", "zoning": "Residential",
         "area_sqm": 300.0, "road_width_m": 20.0, "price_per_sqm": 1200.0,
         "is_cleaned": False, "is_outlier": False, "price_percentile": 15.0,
         "area_category": "Small", "city_road_median": 20.0, "city_ppsqm_median": 3500.0,
         "transaction_date": "2025-01-01", "latitude": 26.44, "longitude": 50.11,
         "total_price": 300 * 1200, "frontage_m": 15.0, "depth_m": 20.0},
    ])
    assert SecondarySelector(settings).select(df) is None


def test_secondary_requires_small_or_medium_area(settings):
    import pandas as pd
    df = pd.DataFrame([
        {"id": 1, "city": "Dammam", "district": "D1", "zoning": "Residential",
         "area_sqm": 5000.0, "road_width_m": 8.0, "price_per_sqm": 1200.0,
         "is_cleaned": False, "is_outlier": False, "price_percentile": 15.0,
         "area_category": "Large", "city_road_median": 20.0, "city_ppsqm_median": 3500.0,
         "transaction_date": "2025-01-01", "latitude": 26.44, "longitude": 50.11,
         "total_price": 5000 * 1200, "frontage_m": 15.0, "depth_m": 20.0},
    ])
    assert SecondarySelector(settings).select(df) is None
