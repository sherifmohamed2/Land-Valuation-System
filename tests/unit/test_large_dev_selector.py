import pytest
from app.services.benchmark.large_dev_selector import LargeDevSelector
from app.core.config import Settings


@pytest.fixture
def settings():
    return Settings()


def test_large_dev_selects_dev_zoning(sample_df, settings):
    selector = LargeDevSelector(settings)
    result = selector.select(sample_df)
    assert result is not None
    assert result["benchmark_type"] == "large_dev"
    assert result["zoning"] in settings.B4_DEV_ZONINGS


def test_large_dev_returns_none_when_no_dev_zoning(settings):
    import pandas as pd
    df = pd.DataFrame([
        {"id": 1, "city": "Riyadh", "district": "D1", "zoning": "Agricultural",
         "area_sqm": 4000.0, "road_width_m": 25.0, "price_per_sqm": 1500.0,
         "is_cleaned": False, "is_outlier": False, "price_percentile": 50.0,
         "area_category": "Very Large", "city_road_median": 20.0, "city_ppsqm_median": 3500.0,
         "transaction_date": "2025-01-01", "latitude": 24.69, "longitude": 46.72,
         "total_price": 4000*1500, "frontage_m": 60.0, "depth_m": 80.0},
    ])
    result = LargeDevSelector(settings).select(df)
    assert result is None


def test_large_dev_has_price_per_sqm(sample_df, settings):
    selector = LargeDevSelector(settings)
    result = selector.select(sample_df)
    assert result is not None
    assert result["price_per_sqm"] is not None
    assert result["price_per_sqm"] > 0


def test_large_dev_requires_large_area_category(settings):
    import pandas as pd
    df = pd.DataFrame([
        {"id": 1, "city": "Riyadh", "district": "D1", "zoning": "Industrial",
         "area_sqm": 800.0, "road_width_m": 25.0, "price_per_sqm": 1900.0,
         "is_cleaned": False, "is_outlier": False, "price_percentile": 50.0,
         "area_category": "Medium", "city_road_median": 20.0, "city_ppsqm_median": 3500.0,
         "transaction_date": "2025-01-01", "latitude": 24.70, "longitude": 46.70,
         "total_price": 800 * 1900, "frontage_m": 60.0, "depth_m": 80.0},
    ])
    assert LargeDevSelector(settings).select(df) is None


def test_large_dev_requires_price_percentile_in_iqr_band(settings):
    import pandas as pd
    df = pd.DataFrame([
        {"id": 1, "city": "Riyadh", "district": "D1", "zoning": "Industrial",
         "area_sqm": 4200.0, "road_width_m": 25.0, "price_per_sqm": 1900.0,
         "is_cleaned": False, "is_outlier": False, "price_percentile": 10.0,
         "area_category": "Large", "city_road_median": 20.0, "city_ppsqm_median": 3500.0,
         "transaction_date": "2025-01-01", "latitude": 24.70, "longitude": 46.70,
         "total_price": 4200 * 1900, "frontage_m": 60.0, "depth_m": 80.0},
    ])
    assert LargeDevSelector(settings).select(df) is None
