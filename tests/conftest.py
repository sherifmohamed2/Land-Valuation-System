import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


@pytest.fixture
def sample_df():
    """
    20-record DataFrame with controlled values covering all 5 benchmark rules.
    Used by unit tests — no database needed.
    """
    today = datetime.now().date()
    records = []

    # 4 records → Benchmark 1 (Market Average)
    for i in range(4):
        records.append({
            "id": i+1, "city": "Riyadh", "district": f"District-B1-{i}",
            "zoning": "Residential", "area_sqm": 800.0 + i*50,
            "road_width_m": 20.0, "price_per_sqm": 3500.0 + i*10,
            "transaction_date": str(today - timedelta(days=90)),
            "latitude": 24.69, "longitude": 46.72,
            "area_category": "Medium", "pct_rank": 50.0,
            "city_road_median": 20.0, "city_ppsqm_median": 3500.0,
            "total_price": (800.0 + i*50) * (3500.0 + i*10),
            "frontage_m": 28.0, "depth_m": 32.0,
            "is_cleaned": False, "is_outlier": False,
            "price_percentile": 50.0,
        })

    # 3 records → Benchmark 2 (Prime)
    for i in range(3):
        records.append({
            "id": 10+i, "city": "Jeddah", "district": f"District-B2-{i}",
            "zoning": "Commercial", "area_sqm": 1200.0 + i*100,
            "road_width_m": 35.0, "price_per_sqm": 9500.0 + i*100,
            "transaction_date": str(today - timedelta(days=180)),
            "latitude": 21.58, "longitude": 39.13,
            "area_category": "Large", "pct_rank": 80.0,
            "city_road_median": 20.0, "city_ppsqm_median": 3500.0,
            "total_price": (1200.0 + i*100) * (9500.0 + i*100),
            "frontage_m": 40.0, "depth_m": 50.0,
            "is_cleaned": False, "is_outlier": False,
            "price_percentile": 80.0,
        })

    # 3 records → Benchmark 3 (Secondary)
    for i in range(3):
        records.append({
            "id": 20+i, "city": "Dammam", "district": f"District-B3-{i}",
            "zoning": "Residential", "area_sqm": 300.0 + i*30,
            "road_width_m": 10.0, "price_per_sqm": 1200.0 + i*50,
            "transaction_date": str(today - timedelta(days=400)),
            "latitude": 26.44, "longitude": 50.11,
            "area_category": "Small", "pct_rank": 15.0,
            "city_road_median": 20.0, "city_ppsqm_median": 3500.0,
            "total_price": (300.0 + i*30) * (1200.0 + i*50),
            "frontage_m": 15.0, "depth_m": 20.0,
            "is_cleaned": False, "is_outlier": False,
            "price_percentile": 15.0,
        })

    # 3 records → Benchmark 4 (Large Dev)
    for i in range(3):
        records.append({
            "id": 30+i, "city": "Riyadh", "district": f"District-B4-{i}",
            "zoning": "Industrial", "area_sqm": 4200.0 + i*300,
            "road_width_m": 25.0, "price_per_sqm": 1900.0 + i*50,
            "transaction_date": str(today - timedelta(days=300)),
            "latitude": 24.70, "longitude": 46.70,
            "area_category": "Large", "pct_rank": 50.0,
            "city_road_median": 20.0, "city_ppsqm_median": 3500.0,
            "total_price": (4200.0 + i*300) * (1900.0 + i*50),
            "frontage_m": 60.0, "depth_m": 80.0,
            "is_cleaned": False, "is_outlier": False,
            "price_percentile": 50.0,
        })

    # 3 records → Benchmark 5 (Emerging — Diriyah district)
    for i in range(3):
        records.append({
            "id": 40+i, "city": "Riyadh", "district": "Diriyah",
            "zoning": "Residential", "area_sqm": 700.0 + i*100,
            "road_width_m": 20.0, "price_per_sqm": 4000.0 + i*100,
            "transaction_date": str(today - timedelta(days=30 + i*15)),
            "latitude": 24.73, "longitude": 46.58,
            "area_category": "Medium", "pct_rank": 60.0,
            "city_road_median": 20.0, "city_ppsqm_median": 3500.0,
            "total_price": (700.0 + i*100) * (4000.0 + i*100),
            "frontage_m": 25.0, "depth_m": 30.0,
            "is_cleaned": False, "is_outlier": False,
            "price_percentile": 60.0,
        })

    # 4 records that match no benchmark rule
    for i in range(4):
        records.append({
            "id": 50+i, "city": "Jeddah", "district": "Al Balad",
            "zoning": "Agricultural", "area_sqm": 100.0 + i*10,
            "road_width_m": 8.0, "price_per_sqm": 250.0,
            "transaction_date": str(today - timedelta(days=900)),
            "latitude": 21.50, "longitude": 39.19,
            "area_category": "Small", "pct_rank": 5.0,
            "city_road_median": 20.0, "city_ppsqm_median": 3500.0,
            "total_price": (100.0 + i*10) * 250.0,
            "frontage_m": 10.0, "depth_m": 10.0,
            "is_cleaned": False, "is_outlier": False,
            "price_percentile": 5.0,
        })

    return pd.DataFrame(records)
