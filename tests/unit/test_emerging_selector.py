import pytest
from app.services.benchmark.emerging_selector import EmergingSelector
from app.core.config import Settings


@pytest.fixture
def settings():
    return Settings()


def test_emerging_selects_from_rising_district(sample_df, settings):
    selector = EmergingSelector(settings)
    result = selector.select(sample_df, rising_districts={"Diriyah"})
    assert result is not None
    assert result["benchmark_type"] == "emerging"
    assert result["district"] == "Diriyah"


def test_emerging_returns_none_when_no_rising_district(sample_df, settings):
    selector = EmergingSelector(settings)
    result = selector.select(sample_df, rising_districts=set())
    assert result is None


def test_emerging_respects_date_lookback(settings):
    import pandas as pd
    from datetime import date, timedelta
    old_date = str(date.today() - timedelta(days=400))
    df = pd.DataFrame([
        {"id": 40, "city": "Riyadh", "district": "Diriyah", "zoning": "Residential",
         "area_sqm": 700.0, "road_width_m": 20.0, "price_per_sqm": 4000.0,
         "is_cleaned": False, "is_outlier": False, "price_percentile": 60.0,
         "area_category": "Medium", "city_road_median": 20.0, "city_ppsqm_median": 3500.0,
         "transaction_date": old_date,
         "latitude": 24.73, "longitude": 46.58,
         "total_price": 700*4000, "frontage_m": 25.0, "depth_m": 30.0},
    ])
    result = EmergingSelector(settings).select(df, rising_districts={"Diriyah"})
    # Record is too old — should not be selected
    assert result is None
