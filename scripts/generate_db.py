"""
Regenerate committed SQLite + JSON fixtures under ``data/``.

Creates deterministic mock locations, transactions, trends, factor configs, etc.
Run from repo root: ``python scripts/generate_db.py``. Overwrites ``land_valuation.db``
and ``mock_data.json`` when executed.
"""
import json
import random
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker

fake = Faker("en_US")
random.seed(42)   # deterministic — same data every time

DB_PATH = Path("data/land_valuation.db")
JSON_PATH = Path("data/mock_data.json")
DB_PATH.parent.mkdir(exist_ok=True)

# === Reference data ===

LOCATIONS = {
    "Riyadh": {
        "districts": [
            {"name": "Al Olaya",  "lat": 24.6881, "lng": 46.7219, "adjacent": ["Al Rawdah", "Al Malaz"]},
            {"name": "Al Malaz",  "lat": 24.6543, "lng": 46.7710, "adjacent": ["Al Olaya", "Al Naseem"]},
            {"name": "Al Naseem", "lat": 24.6234, "lng": 46.8012, "adjacent": ["Al Malaz"]},
            {"name": "Al Rawdah", "lat": 24.7012, "lng": 46.6987, "adjacent": ["Al Olaya", "Diriyah"]},
            {"name": "Diriyah",   "lat": 24.7334, "lng": 46.5765, "adjacent": ["Al Rawdah"]},
        ]
    },
    "Jeddah": {
        "districts": [
            {"name": "Al Hamra",      "lat": 21.5765, "lng": 39.1234, "adjacent": ["Al Salamah", "Al Shati"]},
            {"name": "Al Salamah",    "lat": 21.5432, "lng": 39.1567, "adjacent": ["Al Hamra", "Al Faisaliyah"]},
            {"name": "Al Balad",      "lat": 21.4987, "lng": 39.1876, "adjacent": ["Al Salamah"]},
            {"name": "Al Shati",      "lat": 21.6123, "lng": 39.1045, "adjacent": ["Al Hamra"]},
            {"name": "Al Faisaliyah", "lat": 21.5234, "lng": 39.2012, "adjacent": ["Al Salamah"]},
        ]
    },
    "Dammam": {
        "districts": [
            {"name": "Al Faisaliyah", "lat": 26.4321, "lng": 50.1123, "adjacent": ["Al Nuzha"]},
            {"name": "Al Nuzha",      "lat": 26.4012, "lng": 50.0987, "adjacent": ["Al Faisaliyah", "Al Shula"]},
            {"name": "Al Shula",      "lat": 26.4567, "lng": 50.1456, "adjacent": ["Al Nuzha"]},
            {"name": "Al Muntazah",   "lat": 26.3876, "lng": 50.1234, "adjacent": ["Al Badia"]},
            {"name": "Al Badia",      "lat": 26.4234, "lng": 50.0765, "adjacent": ["Al Muntazah"]},
        ]
    },
}

ZONINGS = [
    {"code": "Residential",       "is_high_value": False, "allows_development": False},
    {"code": "Residential-Prime", "is_high_value": True,  "allows_development": False},
    {"code": "Commercial",        "is_high_value": True,  "allows_development": True},
    {"code": "Mixed Use",         "is_high_value": True,  "allows_development": True},
    {"code": "Industrial",        "is_high_value": False, "allows_development": True},
    {"code": "Agricultural",      "is_high_value": False, "allows_development": False},
]

PRICE_RANGES = {
    "Commercial":        (4000, 12000),
    "Mixed Use":         (3000, 9000),
    "Residential-Prime": (3000, 8000),
    "Residential":       (1500, 6000),
    "Industrial":        (800, 3000),
    "Agricultural":      (200, 1000),
}

ROAD_WIDTHS = [10, 15, 20, 25, 30, 40]

FACTOR_CONFIGS = [
    {"factor_key": "address",             "factor_name": "Address",                  "weight": 0.05,
     "scoring_method": "district_match",
     "params_json": {"same_district": 1.00, "same_city_diff_district": 0.60, "different_city": 0.00, "reject_if_different_city": True}},
    {"factor_key": "area",                "factor_name": "Area",                     "weight": 0.15,
     "scoring_method": "pct_diff_lookup",
     "params_json": {"thresholds": [{"max_diff_pct":5,"multiplier":1.00},{"max_diff_pct":10,"multiplier":0.80},{"max_diff_pct":20,"multiplier":0.60},{"max_diff_pct":30,"multiplier":0.40},{"max_diff_pct":50,"multiplier":0.20},{"max_diff_pct":999,"multiplier":0.00}]}},
    {"factor_key": "general_location",    "factor_name": "General Location",         "weight": 0.15,
     "scoring_method": "distance_km_lookup",
     "params_json": {"thresholds": [{"max_km":0.5,"multiplier":1.00},{"max_km":1.0,"multiplier":0.80},{"max_km":2.0,"multiplier":0.60},{"max_km":5.0,"multiplier":0.40},{"max_km":10.0,"multiplier":0.20},{"max_km":999,"multiplier":0.00}], "fallback_method": "district_match", "fallback_params": {"same_district":1.00,"adjacent_district":0.80,"same_city":0.60,"different_city":0.00}}},
    {"factor_key": "commercial_location", "factor_name": "Commercial Location",      "weight": 0.10,
     "scoring_method": "distance_m_lookup",
     "params_json": {"thresholds": [{"max_m":200,"multiplier":1.00},{"max_m":500,"multiplier":0.80},{"max_m":1000,"multiplier":0.60},{"max_m":2000,"multiplier":0.40},{"max_m":9999,"multiplier":0.20}],"unknown_multiplier":0.00}},
    {"factor_key": "admin_location",      "factor_name": "Administrative Location",  "weight": 0.10,
     "scoring_method": "distance_m_lookup",
     "params_json": {"thresholds": [{"max_m":200,"multiplier":1.00},{"max_m":500,"multiplier":0.80},{"max_m":1000,"multiplier":0.60},{"max_m":2000,"multiplier":0.40},{"max_m":9999,"multiplier":0.20}],"unknown_multiplier":0.00}},
    {"factor_key": "proximity_services",  "factor_name": "Proximity to Services",   "weight": 0.10,
     "scoring_method": "distance_m_lookup",
     "params_json": {"thresholds": [{"max_m":300,"multiplier":1.00},{"max_m":700,"multiplier":0.80},{"max_m":1500,"multiplier":0.60},{"max_m":3000,"multiplier":0.40},{"max_m":9999,"multiplier":0.20}],"unknown_multiplier":0.00}},
    {"factor_key": "proximity_main_road", "factor_name": "Proximity to Main Road",  "weight": 0.10,
     "scoring_method": "distance_m_lookup",
     "params_json": {"thresholds": [{"max_m":150,"multiplier":1.00},{"max_m":400,"multiplier":0.80},{"max_m":800,"multiplier":0.60},{"max_m":1500,"multiplier":0.40},{"max_m":9999,"multiplier":0.20}],"unknown_multiplier":0.00}},
    {"factor_key": "plot_proportions",    "factor_name": "Plot Proportions",         "weight": 0.10,
     "scoring_method": "ratio_lookup",
     "params_json": {"thresholds": [{"max_ratio":1.5,"multiplier":1.00},{"max_ratio":2.0,"multiplier":0.80},{"max_ratio":3.0,"multiplier":0.60},{"max_ratio":4.0,"multiplier":0.40},{"max_ratio":999,"multiplier":0.20}],"missing_dimensions_multiplier":0.00}},
    {"factor_key": "far_csr",             "factor_name": "Building Ratio / FAR / CSR","weight": 0.10,
     "scoring_method": "zoning_compatibility",
     "params_json": {"same_zoning":1.00,"same_permitted_use":0.70,"partially_compatible":0.40,"not_compatible":0.00}},
    {"factor_key": "utilities",           "factor_name": "Utilities Availability",   "weight": 0.05,
     "scoring_method": "count_ratio",
     "params_json": {"utility_keys":["electricity","water","sewerage","fiber","gas"],"total_count":5}},
]

# Validate factor weights sum to 1.0
total_weight = sum(f["weight"] for f in FACTOR_CONFIGS)
assert abs(total_weight - 1.0) < 0.001, f"Factor weights sum to {total_weight}, must be 1.0"


def generate_transactions():
    """Generate 121 deterministic land transaction records."""
    records = []
    today = datetime.now().date()

    city_list = list(LOCATIONS.keys())
    city_roads = {"Riyadh": 20, "Jeddah": 20, "Dammam": 20}

    # --- Controlled records to guarantee benchmark coverage ---

    # 4 records for Benchmark 1: pct ~50, medium area, road within ±20% of median (20m)
    for i in range(4):
        city = city_list[i % 3]
        district = LOCATIONS[city]["districts"][i % 5]
        base_ppsqm = PRICE_RANGES["Residential"][0] + (PRICE_RANGES["Residential"][1] - PRICE_RANGES["Residential"][0]) * 0.50
        area = 800 + i * 50  # medium range
        records.append({
            "id": None, "external_ref": f"B1-CTRL-{i+1}",
            "asset_type": "Land", "region": "Central",
            "city": city, "district": district["name"],
            "street": fake.street_name(), "latitude": district["lat"] + random.uniform(-0.01,0.01),
            "longitude": district["lng"] + random.uniform(-0.01,0.01),
            "zoning": "Residential", "land_use": "Residential",
            "area_sqm": round(area, 2), "frontage_m": round(area**0.5 * 0.8, 1), "depth_m": round(area**0.5 * 1.2, 1),
            "road_width_m": 20.0,
            "total_price": round(area * base_ppsqm, 2), "currency_code": "SAR",
            "standardized_total_price": None, "price_per_sqm": None,
            "transaction_date": str(today - timedelta(days=90 + i*30)),
            "utilities_json": {"electricity":True,"water":True,"sewerage":True,"fiber":False,"gas":False},
            "utility_count": 3, "verification_status": "verified", "data_source": "mock_controlled",
            "is_cleaned": False, "is_outlier": False,
            "area_category": None, "price_percentile": None, "price_band": None,
            "city_road_median": None, "city_ppsqm_median": None,
        })

    # 3 records for Benchmark 2: high pct, wide road, Commercial
    for i in range(3):
        city = city_list[i % 3]
        district = LOCATIONS[city]["districts"][1]
        base_ppsqm = PRICE_RANGES["Commercial"][0] + (PRICE_RANGES["Commercial"][1] - PRICE_RANGES["Commercial"][0]) * 0.85
        area = 1200 + i * 200
        records.append({
            "id": None, "external_ref": f"B2-CTRL-{i+1}",
            "asset_type": "Land", "region": "Central",
            "city": city, "district": district["name"],
            "street": fake.street_name(), "latitude": district["lat"] + random.uniform(-0.01,0.01),
            "longitude": district["lng"] + random.uniform(-0.01,0.01),
            "zoning": "Commercial", "land_use": "Commercial",
            "area_sqm": round(area, 2), "frontage_m": round(area**0.5, 1), "depth_m": round(area**0.5 * 1.5, 1),
            "road_width_m": 35.0,
            "total_price": round(area * base_ppsqm, 2), "currency_code": "SAR",
            "standardized_total_price": None, "price_per_sqm": None,
            "transaction_date": str(today - timedelta(days=180 + i*20)),
            "utilities_json": {"electricity":True,"water":True,"sewerage":True,"fiber":True,"gas":True},
            "utility_count": 5, "verification_status": "verified", "data_source": "mock_controlled",
            "is_cleaned": False, "is_outlier": False,
            "area_category": None, "price_percentile": None, "price_band": None,
            "city_road_median": None, "city_ppsqm_median": None,
        })

    # 3 records for Benchmark 3: low pct, narrow road, small area
    for i in range(3):
        city = city_list[i % 3]
        district = LOCATIONS[city]["districts"][2]
        base_ppsqm = PRICE_RANGES["Residential"][0] + (PRICE_RANGES["Residential"][1] - PRICE_RANGES["Residential"][0]) * 0.15
        area = 300 + i * 50
        records.append({
            "id": None, "external_ref": f"B3-CTRL-{i+1}",
            "asset_type": "Land", "region": "Peripheral",
            "city": city, "district": district["name"],
            "street": fake.street_name(), "latitude": district["lat"] + random.uniform(-0.01,0.01),
            "longitude": district["lng"] + random.uniform(-0.01,0.01),
            "zoning": "Residential", "land_use": "Residential",
            "area_sqm": round(area, 2), "frontage_m": round(area**0.5 * 0.7, 1), "depth_m": round(area**0.5 * 1.3, 1),
            "road_width_m": 10.0,
            "total_price": round(area * base_ppsqm, 2), "currency_code": "SAR",
            "standardized_total_price": None, "price_per_sqm": None,
            "transaction_date": str(today - timedelta(days=400 + i*30)),
            "utilities_json": {"electricity":True,"water":False,"sewerage":False,"fiber":False,"gas":False},
            "utility_count": 1, "verification_status": "unverified", "data_source": "mock_controlled",
            "is_cleaned": False, "is_outlier": False,
            "area_category": None, "price_percentile": None, "price_band": None,
            "city_road_median": None, "city_ppsqm_median": None,
        })

    # 3 records for Benchmark 4: large area, dev zoning, mid price
    for i in range(3):
        city = city_list[i % 3]
        district = LOCATIONS[city]["districts"][3]
        base_ppsqm = PRICE_RANGES["Industrial"][0] + (PRICE_RANGES["Industrial"][1] - PRICE_RANGES["Industrial"][0]) * 0.50
        area = 4200 + i * 300
        records.append({
            "id": None, "external_ref": f"B4-CTRL-{i+1}",
            "asset_type": "Land", "region": "Industrial Zone",
            "city": city, "district": district["name"],
            "street": fake.street_name(), "latitude": district["lat"] + random.uniform(-0.01,0.01),
            "longitude": district["lng"] + random.uniform(-0.01,0.01),
            "zoning": "Industrial", "land_use": "Industrial",
            "area_sqm": round(area, 2), "frontage_m": round(area**0.5, 1), "depth_m": round(area**0.5 * 2, 1),
            "road_width_m": 25.0,
            "total_price": round(area * base_ppsqm, 2), "currency_code": "SAR",
            "standardized_total_price": None, "price_per_sqm": None,
            "transaction_date": str(today - timedelta(days=300 + i*40)),
            "utilities_json": {"electricity":True,"water":True,"sewerage":True,"fiber":False,"gas":True},
            "utility_count": 4, "verification_status": "verified", "data_source": "mock_controlled",
            "is_cleaned": False, "is_outlier": False,
            "area_category": None, "price_percentile": None, "price_band": None,
            "city_road_median": None, "city_ppsqm_median": None,
        })

    # 3 records for Benchmark 5: recent, mid pct, in Diriyah (rising trend)
    for i in range(3):
        base_ppsqm = PRICE_RANGES["Residential"][0] + (PRICE_RANGES["Residential"][1] - PRICE_RANGES["Residential"][0]) * 0.60
        area = 700 + i * 100
        records.append({
            "id": None, "external_ref": f"B5-CTRL-{i+1}",
            "asset_type": "Land", "region": "Central",
            "city": "Riyadh", "district": "Diriyah",
            "street": fake.street_name(), "latitude": 24.7334 + random.uniform(-0.01,0.01),
            "longitude": 46.5765 + random.uniform(-0.01,0.01),
            "zoning": "Residential", "land_use": "Residential",
            "area_sqm": round(area, 2), "frontage_m": round(area**0.5 * 0.9, 1), "depth_m": round(area**0.5 * 1.1, 1),
            "road_width_m": 20.0,
            "total_price": round(area * base_ppsqm, 2), "currency_code": "SAR",
            "standardized_total_price": None, "price_per_sqm": None,
            "transaction_date": str(today - timedelta(days=30 + i*15)),
            "utilities_json": {"electricity":True,"water":True,"sewerage":True,"fiber":True,"gas":False},
            "utility_count": 4, "verification_status": "verified", "data_source": "mock_controlled",
            "is_cleaned": False, "is_outlier": False,
            "area_category": None, "price_percentile": None, "price_band": None,
            "city_road_median": None, "city_ppsqm_median": None,
        })

    # --- 80 random records across all cities/districts/zonings ---
    for i in range(80):
        city = random.choice(list(LOCATIONS.keys()))
        district_info = random.choice(LOCATIONS[city]["districts"])
        zoning = random.choice([z["code"] for z in ZONINGS])
        pmin, pmax = PRICE_RANGES[zoning]
        base_ppsqm = random.uniform(pmin, pmax)
        area = round(random.uniform(200, 3500), 2)
        road = random.choice(ROAD_WIDTHS)
        days_ago = random.randint(30, 1095)
        utilities = {
            "electricity": random.choice([True, False]),
            "water": random.choice([True, False]),
            "sewerage": random.choice([True, False]),
            "fiber": random.choice([True, False]),
            "gas": random.choice([True, False]),
        }
        records.append({
            "id": None, "external_ref": f"RND-{i+1:04d}",
            "asset_type": "Land", "region": "General",
            "city": city, "district": district_info["name"],
            "street": fake.street_name(),
            "latitude": round(district_info["lat"] + random.uniform(-0.05, 0.05), 6),
            "longitude": round(district_info["lng"] + random.uniform(-0.05, 0.05), 6),
            "zoning": zoning, "land_use": zoning,
            "area_sqm": area,
            "frontage_m": round(random.uniform(10, 80), 1),
            "depth_m": round(random.uniform(15, 100), 1),
            "road_width_m": float(road),
            "total_price": round(area * base_ppsqm, 2), "currency_code": "SAR",
            "standardized_total_price": None, "price_per_sqm": None,
            "transaction_date": str(today - timedelta(days=days_ago)),
            "utilities_json": utilities,
            "utility_count": sum(utilities.values()),
            "verification_status": random.choice(["verified", "unverified"]),
            "data_source": "mock_random",
            "is_cleaned": False, "is_outlier": False,
            "area_category": None, "price_percentile": None, "price_band": None,
            "city_road_median": None, "city_ppsqm_median": None,
        })

    # --- Intentionally dirty records ---

    # 8 records with NULL total_price
    for i in range(8):
        city = random.choice(list(LOCATIONS.keys()))
        district_info = random.choice(LOCATIONS[city]["districts"])
        records.append({
            "id": None, "external_ref": f"DIRTY-NULL-PRICE-{i+1}",
            "asset_type": "Land", "region": "General",
            "city": city, "district": district_info["name"],
            "street": fake.street_name(),
            "latitude": district_info["lat"], "longitude": district_info["lng"],
            "zoning": "Residential", "land_use": "Residential",
            "area_sqm": 500.0, "frontage_m": 20.0, "depth_m": 25.0, "road_width_m": 15.0,
            "total_price": None,
            "currency_code": "SAR", "standardized_total_price": None, "price_per_sqm": None,
            "transaction_date": str(today - timedelta(days=200)),
            "utilities_json": None, "utility_count": 0, "verification_status": "unverified",
            "data_source": "mock_dirty",
            "is_cleaned": False, "is_outlier": False,
            "area_category": None, "price_percentile": None, "price_band": None,
            "city_road_median": None, "city_ppsqm_median": None,
        })

    # 8 records with area_sqm = 0
    for i in range(8):
        city = random.choice(list(LOCATIONS.keys()))
        district_info = random.choice(LOCATIONS[city]["districts"])
        records.append({
            "id": None, "external_ref": f"DIRTY-ZERO-AREA-{i+1}",
            "asset_type": "Land", "region": "General",
            "city": city, "district": district_info["name"],
            "street": fake.street_name(),
            "latitude": district_info["lat"], "longitude": district_info["lng"],
            "zoning": "Residential", "land_use": "Residential",
            "area_sqm": 0.0,
            "frontage_m": 0.0, "depth_m": 0.0, "road_width_m": 15.0,
            "total_price": 500000.0, "currency_code": "SAR",
            "standardized_total_price": None, "price_per_sqm": None,
            "transaction_date": str(today - timedelta(days=300)),
            "utilities_json": None, "utility_count": 0, "verification_status": "unverified",
            "data_source": "mock_dirty",
            "is_cleaned": False, "is_outlier": False,
            "area_category": None, "price_percentile": None, "price_band": None,
            "city_road_median": None, "city_ppsqm_median": None,
        })

    # 5 extreme-high price outliers
    for i in range(5):
        city = random.choice(list(LOCATIONS.keys()))
        district_info = random.choice(LOCATIONS[city]["districts"])
        area = 800.0
        records.append({
            "id": None, "external_ref": f"DIRTY-OUTLIER-HIGH-{i+1}",
            "asset_type": "Land", "region": "General",
            "city": city, "district": district_info["name"],
            "street": fake.street_name(),
            "latitude": district_info["lat"], "longitude": district_info["lng"],
            "zoning": "Residential", "land_use": "Residential",
            "area_sqm": area, "frontage_m": 25.0, "depth_m": 32.0, "road_width_m": 20.0,
            "total_price": round(area * PRICE_RANGES["Residential"][1] * 15, 2),
            "currency_code": "SAR", "standardized_total_price": None, "price_per_sqm": None,
            "transaction_date": str(today - timedelta(days=150)),
            "utilities_json": {"electricity":True,"water":True,"sewerage":True,"fiber":True,"gas":True},
            "utility_count": 5, "verification_status": "unverified", "data_source": "mock_dirty",
            "is_cleaned": False, "is_outlier": False,
            "area_category": None, "price_percentile": None, "price_band": None,
            "city_road_median": None, "city_ppsqm_median": None,
        })

    # 5 extreme-low price outliers
    for i in range(5):
        city = random.choice(list(LOCATIONS.keys()))
        district_info = random.choice(LOCATIONS[city]["districts"])
        area = 800.0
        records.append({
            "id": None, "external_ref": f"DIRTY-OUTLIER-LOW-{i+1}",
            "asset_type": "Land", "region": "General",
            "city": city, "district": district_info["name"],
            "street": fake.street_name(),
            "latitude": district_info["lat"], "longitude": district_info["lng"],
            "zoning": "Residential", "land_use": "Residential",
            "area_sqm": area, "frontage_m": 25.0, "depth_m": 32.0, "road_width_m": 20.0,
            "total_price": round(area * PRICE_RANGES["Residential"][0] * 0.03, 2),
            "currency_code": "SAR", "standardized_total_price": None, "price_per_sqm": None,
            "transaction_date": str(today - timedelta(days=500)),
            "utilities_json": {"electricity":False,"water":False,"sewerage":False,"fiber":False,"gas":False},
            "utility_count": 0, "verification_status": "unverified", "data_source": "mock_dirty",
            "is_cleaned": False, "is_outlier": False,
            "area_category": None, "price_percentile": None, "price_band": None,
            "city_road_median": None, "city_ppsqm_median": None,
        })

    return records


def build_database():
    """Build the SQLite database and JSON export synchronously using sqlite3 directly."""
    print(f"Building database at {DB_PATH}...")

    if DB_PATH.exists():
        DB_PATH.unlink()
        print("  Removed existing database.")

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    c = conn.cursor()

    c.executescript("""
    CREATE TABLE IF NOT EXISTS land_transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        external_ref TEXT, asset_type TEXT NOT NULL, region TEXT,
        city TEXT, district TEXT, street TEXT, latitude REAL, longitude REAL,
        zoning TEXT, land_use TEXT, area_sqm REAL, frontage_m REAL, depth_m REAL,
        road_width_m REAL, total_price REAL, currency_code TEXT DEFAULT 'SAR',
        standardized_total_price REAL, price_per_sqm REAL,
        transaction_date TEXT, utilities_json TEXT, utility_count INTEGER,
        verification_status TEXT DEFAULT 'unverified', data_source TEXT,
        is_cleaned INTEGER DEFAULT 0, is_outlier INTEGER DEFAULT 0,
        area_category TEXT, price_percentile REAL, price_band TEXT,
        city_road_median REAL, city_ppsqm_median REAL,
        created_at TEXT, updated_at TEXT
    );

    CREATE TABLE IF NOT EXISTS benchmark_selection_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_uuid TEXT UNIQUE,
        run_name TEXT, status TEXT DEFAULT 'pending', trigger_type TEXT,
        started_at TEXT, completed_at TEXT,
        total_candidates INTEGER, clean_candidates INTEGER, benchmarks_found INTEGER,
        filter_config_json TEXT, validation_result_json TEXT, notes TEXT,
        created_by TEXT DEFAULT 'system', created_at TEXT
    );

    CREATE TABLE IF NOT EXISTS benchmark_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER REFERENCES benchmark_selection_runs(id),
        land_transaction_id INTEGER REFERENCES land_transactions(id),
        benchmark_type TEXT NOT NULL, city TEXT, district TEXT, zoning TEXT,
        area_sqm REAL, road_width_m REAL, price_per_sqm REAL,
        selection_score REAL, score_breakdown_json TEXT,
        assigned_valuation_method TEXT DEFAULT 'Market Method',
        validation_flags_json TEXT, candidate_pool_size INTEGER,
        created_at TEXT,
        UNIQUE(run_id, benchmark_type)
    );

    CREATE TABLE IF NOT EXISTS subject_lands (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        subject_code TEXT UNIQUE, asset_type TEXT DEFAULT 'Land',
        region TEXT, city TEXT, district TEXT, street TEXT,
        latitude REAL, longitude REAL, area_sqm REAL, frontage_m REAL, depth_m REAL,
        zoning TEXT, far REAL, csr REAL, utilities_json TEXT,
        distance_to_services_m REAL, distance_to_main_road_m REAL,
        commercial_grade TEXT, administrative_grade TEXT, subject_price_per_sqm REAL,
        created_at TEXT
    );

    CREATE TABLE IF NOT EXISTS valuation_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_uuid TEXT UNIQUE, subject_land_id INTEGER, benchmark_run_id INTEGER,
        status TEXT DEFAULT 'pending', comparables_evaluated INTEGER,
        comparables_accepted INTEGER, average_point_value REAL, target_points REAL,
        estimated_price_per_sqm REAL, estimated_total_value REAL,
        factor_config_version INTEGER, rule_version TEXT, created_at TEXT
    );

    CREATE TABLE IF NOT EXISTS valuation_factor_scores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        valuation_run_id INTEGER, asset_kind TEXT NOT NULL, asset_ref_id INTEGER NOT NULL,
        factor_key TEXT, weight REAL, multiplier REAL, score_points REAL,
        raw_inputs_snapshot_json TEXT, created_at TEXT
    );

    CREATE TABLE IF NOT EXISTS factor_configs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        factor_key TEXT UNIQUE NOT NULL, factor_name TEXT NOT NULL,
        weight REAL NOT NULL, scoring_method TEXT NOT NULL,
        params_json TEXT NOT NULL, is_active INTEGER DEFAULT 1,
        version INTEGER DEFAULT 1, created_at TEXT, updated_at TEXT
    );

    CREATE TABLE IF NOT EXISTS location_lookup (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        region TEXT, city TEXT, district TEXT,
        adjacent_districts_json TEXT,
        latitude_centroid REAL, longitude_centroid REAL,
        UNIQUE(city, district)
    );

    CREATE TABLE IF NOT EXISTS zoning_lookup (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        zoning_code TEXT UNIQUE NOT NULL, zoning_name TEXT,
        permitted_use_json TEXT, compatible_zoning_json TEXT,
        is_high_value INTEGER DEFAULT 0, allows_development INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS district_price_trends (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT, district TEXT, period_start TEXT, period_end TEXT,
        avg_price_per_sqm REAL, transaction_count INTEGER,
        trend_direction TEXT, computed_at TEXT,
        UNIQUE(city, district, period_start)
    );

    CREATE TABLE IF NOT EXISTS audit_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entity_type TEXT, entity_id INTEGER, action TEXT,
        before_json TEXT, after_json TEXT,
        performed_by TEXT DEFAULT 'system', performed_at TEXT
    );
    """)

    now = datetime.utcnow().isoformat()

    # Seed location_lookup
    for city, data in LOCATIONS.items():
        for d in data["districts"]:
            c.execute("""
                INSERT OR IGNORE INTO location_lookup (region, city, district, adjacent_districts_json, latitude_centroid, longitude_centroid)
                VALUES (?, ?, ?, ?, ?, ?)
            """, ("Central", city, d["name"], json.dumps(d["adjacent"]), d["lat"], d["lng"]))

    # Seed zoning_lookup
    for z in ZONINGS:
        c.execute("""
            INSERT OR IGNORE INTO zoning_lookup (zoning_code, zoning_name, permitted_use_json, compatible_zoning_json, is_high_value, allows_development)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (z["code"], z["code"], json.dumps([z["code"]]), json.dumps([z["code"]]),
              int(z["is_high_value"]), int(z["allows_development"])))

    # Seed factor_configs
    for f in FACTOR_CONFIGS:
        c.execute("""
            INSERT OR IGNORE INTO factor_configs (factor_key, factor_name, weight, scoring_method, params_json, is_active, version, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 1, 1, ?, ?)
        """, (f["factor_key"], f["factor_name"], f["weight"], f["scoring_method"],
              json.dumps(f["params_json"]), now, now))

    # Seed district_price_trends — mark Diriyah and Al Hamra as rising
    today_str = datetime.now().date().isoformat()
    for city, district in [("Riyadh", "Diriyah"), ("Jeddah", "Al Hamra")]:
        c.execute("""
            INSERT OR IGNORE INTO district_price_trends
            (city, district, period_start, period_end, avg_price_per_sqm, transaction_count, trend_direction, computed_at)
            VALUES (?, ?, ?, ?, ?, ?, 'rising', ?)
        """, (city, district, "2024-01-01", today_str, 4500.0, 12, now))

    # Insert land transactions
    records = generate_transactions()
    for rec in records:
        c.execute("""
            INSERT INTO land_transactions
            (external_ref, asset_type, region, city, district, street, latitude, longitude,
             zoning, land_use, area_sqm, frontage_m, depth_m, road_width_m,
             total_price, currency_code, standardized_total_price, price_per_sqm,
             transaction_date, utilities_json, utility_count, verification_status, data_source,
             is_cleaned, is_outlier, area_category, price_percentile, price_band,
             city_road_median, city_ppsqm_median, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            rec["external_ref"], rec["asset_type"], rec["region"], rec["city"],
            rec["district"], rec["street"], rec["latitude"], rec["longitude"],
            rec["zoning"], rec["land_use"], rec["area_sqm"], rec["frontage_m"],
            rec["depth_m"], rec["road_width_m"], rec["total_price"], rec["currency_code"],
            rec["standardized_total_price"], rec["price_per_sqm"], rec["transaction_date"],
            json.dumps(rec["utilities_json"]) if rec["utilities_json"] else None,
            rec["utility_count"], rec["verification_status"], rec["data_source"],
            int(rec["is_cleaned"]), int(rec["is_outlier"]),
            rec["area_category"], rec["price_percentile"], rec["price_band"],
            rec["city_road_median"], rec["city_ppsqm_median"], now, now
        ))

    conn.commit()

    # Count summary
    total = c.execute("SELECT COUNT(*) FROM land_transactions").fetchone()[0]
    print(f"  Inserted {total} land transactions")
    print(f"    Controlled (benchmark guarantee): {len([r for r in records if 'CTRL' in r['external_ref']])}")
    print(f"    Random: {len([r for r in records if 'RND' in r['external_ref']])}")
    print(f"    Dirty (null/zero/outlier): {len([r for r in records if 'DIRTY' in r['external_ref']])}")

    # Export to JSON
    rows = c.execute("SELECT * FROM land_transactions").fetchall()
    cols = [d[0] for d in c.description]
    export = {"metadata": {"generated_at": now, "total_records": total, "seed": 42},
              "transactions": [dict(zip(cols, row)) for row in rows]}
    JSON_PATH.write_text(json.dumps(export, indent=2, ensure_ascii=False))
    print(f"  Exported {JSON_PATH}")

    conn.close()
    print("Done. Commit data/land_valuation.db and data/mock_data.json to your repo.")


if __name__ == "__main__":
    build_database()
