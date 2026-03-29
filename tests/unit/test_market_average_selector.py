import pytest
from app.services.benchmark.market_average_selector import MarketAverageSelector
from app.core.config import Settings


@pytest.fixture
def settings():
    return Settings()


def test_market_average_selects_medium_area_mid_pct(sample_df, settings):
    selector = MarketAverageSelector(settings)
    result = selector.select(sample_df)
    assert result is not None
    assert result["benchmark_type"] == "market_average"


def test_market_average_returns_score(sample_df, settings):
    selector = MarketAverageSelector(settings)
    result = selector.select(sample_df)
    assert result is not None
    assert result["selection_score"] > 0


def test_market_average_returns_none_when_no_medium(settings):
    import pandas as pd
    df = pd.DataFrame([
        {"id": 1, "city": "Riyadh", "district": "D1", "zoning": "Commercial",
         "area_sqm": 5000.0, "road_width_m": 20.0, "price_per_sqm": 3500.0,
         "is_cleaned": False, "is_outlier": False,
         "price_percentile": 50.0, "area_category": "Very Large",
         "city_road_median": 20.0, "city_ppsqm_median": 3500.0,
         "transaction_date": "2025-01-01", "latitude": 24.69, "longitude": 46.72,
         "total_price": 5000*3500, "frontage_m": 50.0, "depth_m": 60.0},
    ])
    result = selector = MarketAverageSelector(settings)
    result = selector.select(df)
    assert result is None


def test_market_average_has_score_breakdown(sample_df, settings):
    selector = MarketAverageSelector(settings)
    result = selector.select(sample_df)
    assert result is not None
    assert "score_breakdown" in result
    assert "total" in result["score_breakdown"]
