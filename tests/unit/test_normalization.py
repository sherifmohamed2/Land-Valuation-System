import pytest
import pandas as pd
from app.services.ingestion.normalization_service import NormalizationService


def test_normalization_flags_null_price():
    df = pd.DataFrame([
        {"id": 1, "asset_type": "Land", "total_price": None, "area_sqm": 500.0,
         "currency_code": "SAR", "road_width_m": 20.0, "is_cleaned": False, "is_outlier": False, "city": "Riyadh"},
    ])
    result = NormalizationService.normalize(df)
    assert result.loc[0, "is_cleaned"] == True


def test_normalization_flags_zero_area():
    df = pd.DataFrame([
        {"id": 1, "asset_type": "Land", "total_price": 500000.0, "area_sqm": 0.0,
         "currency_code": "SAR", "road_width_m": 20.0, "is_cleaned": False, "is_outlier": False, "city": "Riyadh"},
    ])
    result = NormalizationService.normalize(df)
    assert result.loc[0, "is_cleaned"] == True


def test_normalization_computes_price_per_sqm():
    df = pd.DataFrame([
        {"id": 1, "asset_type": "Land", "total_price": 400000.0, "area_sqm": 200.0,
         "currency_code": "SAR", "road_width_m": 20.0, "is_cleaned": False, "is_outlier": False, "city": "Riyadh"},
    ])
    result = NormalizationService.normalize(df)
    assert result.loc[0, "is_cleaned"] == False
    assert abs(result.loc[0, "price_per_sqm"] - 2000.0) < 0.01


def test_normalization_assigns_area_category():
    df = pd.DataFrame([
        {"id": 1, "asset_type": "Land", "total_price": 100000.0, "area_sqm": 800.0,
         "currency_code": "SAR", "road_width_m": 20.0, "is_cleaned": False, "is_outlier": False, "city": "Riyadh"},
    ])
    result = NormalizationService.normalize(df)
    assert result.loc[0, "area_category"] == "Medium"


def test_normalization_flags_non_land_asset_type():
    df = pd.DataFrame([
        {"id": 1, "asset_type": "Building", "total_price": 500000.0, "area_sqm": 400.0,
         "currency_code": "SAR", "road_width_m": 20.0, "is_cleaned": False, "is_outlier": False, "city": "Riyadh"},
    ])
    result = NormalizationService.normalize(df)
    assert result.loc[0, "is_cleaned"] == True


def test_normalization_computes_city_medians():
    df = pd.DataFrame([
        {"id": 1, "asset_type": "Land", "total_price": 400000.0, "area_sqm": 200.0,
         "currency_code": "SAR", "road_width_m": 20.0, "is_cleaned": False, "is_outlier": False, "city": "Riyadh"},
        {"id": 2, "asset_type": "Land", "total_price": 800000.0, "area_sqm": 400.0,
         "currency_code": "SAR", "road_width_m": 30.0, "is_cleaned": False, "is_outlier": False, "city": "Riyadh"},
    ])
    result = NormalizationService.normalize(df)
    assert result.loc[0, "city_road_median"] == 25.0
    assert result.loc[1, "city_road_median"] == 25.0
