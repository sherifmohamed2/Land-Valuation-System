import pytest
import pandas as pd
from app.services.ingestion.outlier_service import OutlierService
from app.core.config import Settings


@pytest.fixture
def settings():
    return Settings()


def make_df(prices: list[float]) -> pd.DataFrame:
    return pd.DataFrame([
        {"id": i, "price_per_sqm": p, "is_cleaned": False, "is_outlier": False}
        for i, p in enumerate(prices)
    ])


def test_outlier_flags_high_prices(settings):
    # Create 20 prices; top 5% (1 record) should be flagged
    prices = list(range(100, 2100, 100))  # 20 records: 100, 200, ..., 2000
    df = make_df(prices)
    result = OutlierService.flag_outliers(df, settings)
    high_outliers = result[result["is_outlier"] == True]
    assert len(high_outliers) >= 1


def test_outlier_flags_low_prices(settings):
    prices = list(range(100, 2100, 100))
    df = make_df(prices)
    result = OutlierService.flag_outliers(df, settings)
    low_outliers = result[(result["is_outlier"] == True) & (result["price_per_sqm"] <= 200)]
    assert len(low_outliers) >= 1


def test_outlier_assigns_price_band(settings):
    prices = list(range(100, 2100, 100))
    df = make_df(prices)
    result = OutlierService.flag_outliers(df, settings)
    middle = result[(result["is_outlier"] == False)]
    assert (middle["price_band"] == "middle").all()


def test_outlier_does_not_touch_cleaned_records(settings):
    df = pd.DataFrame([
        {"id": 0, "price_per_sqm": 99999.0, "is_cleaned": True, "is_outlier": False},
    ])
    result = OutlierService.flag_outliers(df, settings)
    # Cleaned records should not be flagged
    assert result.loc[0, "is_outlier"] == False


def test_outlier_assigns_percentile_ranks(settings):
    prices = [1000.0, 2000.0, 3000.0, 4000.0, 5000.0]
    df = make_df(prices)
    result = OutlierService.flag_outliers(df, settings)
    valid = result[result["is_cleaned"] == False]
    assert valid["price_percentile"].notna().any()
