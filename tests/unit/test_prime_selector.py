import pytest
from app.services.benchmark.prime_selector import PrimeSelector
from app.core.config import Settings


@pytest.fixture
def settings():
    return Settings()


def test_prime_selects_high_pct_commercial(sample_df, settings):
    selector = PrimeSelector(settings)
    result = selector.select(sample_df)
    assert result is not None
    assert result["benchmark_type"] == "prime"


def test_prime_zoning_is_prime(sample_df, settings):
    selector = PrimeSelector(settings)
    result = selector.select(sample_df)
    assert result is not None
    assert result["zoning"] in settings.B2_PRIME_ZONINGS


def test_prime_returns_none_when_no_prime_zoning(settings):
    import pandas as pd
    df = pd.DataFrame([
        {"id": 1, "city": "Riyadh", "district": "D1", "zoning": "Agricultural",
         "area_sqm": 800.0, "road_width_m": 20.0, "price_per_sqm": 9000.0,
         "is_cleaned": False, "is_outlier": False, "price_percentile": 80.0,
         "area_category": "Medium", "city_road_median": 20.0, "city_ppsqm_median": 3500.0,
         "transaction_date": "2025-01-01", "latitude": 24.69, "longitude": 46.72,
         "total_price": 800*9000, "frontage_m": 28.0, "depth_m": 32.0},
    ])
    result = PrimeSelector(settings).select(df)
    assert result is None


def test_prime_has_candidate_pool_size(sample_df, settings):
    selector = PrimeSelector(settings)
    result = selector.select(sample_df)
    assert result is not None
    assert result["candidate_pool_size"] >= 1


def test_prime_requires_road_wider_than_city_median(settings):
    import pandas as pd
    df = pd.DataFrame([
        {"id": 1, "city": "Jeddah", "district": "D1", "zoning": "Commercial",
         "area_sqm": 1200.0, "road_width_m": 20.0, "price_per_sqm": 9500.0,
         "is_cleaned": False, "is_outlier": False, "price_percentile": 80.0,
         "area_category": "Large", "city_road_median": 20.0, "city_ppsqm_median": 3500.0,
         "transaction_date": "2025-01-01", "latitude": 21.58, "longitude": 39.13,
         "total_price": 1200 * 9500, "frontage_m": 40.0, "depth_m": 50.0},
    ])
    assert PrimeSelector(settings).select(df) is None


def test_prime_selects_when_road_strictly_above_median(settings):
    import pandas as pd
    df = pd.DataFrame([
        {"id": 1, "city": "Jeddah", "district": "D1", "zoning": "Commercial",
         "area_sqm": 1200.0, "road_width_m": 25.0, "price_per_sqm": 9500.0,
         "is_cleaned": False, "is_outlier": False, "price_percentile": 80.0,
         "area_category": "Large", "city_road_median": 20.0, "city_ppsqm_median": 3500.0,
         "transaction_date": "2025-01-01", "latitude": 21.58, "longitude": 39.13,
         "total_price": 1200 * 9500, "frontage_m": 40.0, "depth_m": 50.0},
    ])
    r = PrimeSelector(settings).select(df)
    assert r is not None
    assert r["road_width_m"] > 20.0
