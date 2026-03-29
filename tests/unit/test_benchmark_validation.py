import pytest
from app.services.benchmark.benchmark_validation_service import BenchmarkValidationService
from app.core.config import Settings


@pytest.fixture
def settings():
    return Settings()


@pytest.fixture
def validator(settings):
    return BenchmarkValidationService(settings)


def make_full_benchmarks():
    return {
        "market_average": {"city": "Riyadh", "district": "Al Olaya", "area_sqm": 800.0, "price_per_sqm": 3500.0, "zoning": "Residential"},
        "prime": {"city": "Jeddah", "district": "Al Hamra", "area_sqm": 1500.0, "price_per_sqm": 9000.0, "zoning": "Commercial"},
        "secondary": {"city": "Dammam", "district": "Al Nuzha", "area_sqm": 300.0, "price_per_sqm": 1200.0, "zoning": "Residential"},
        "large_dev": {"city": "Riyadh", "district": "Al Rawdah", "area_sqm": 4500.0, "price_per_sqm": 1900.0, "zoning": "Industrial"},
        "emerging": {"city": "Riyadh", "district": "Diriyah", "area_sqm": 700.0, "price_per_sqm": 4200.0, "zoning": "Residential"},
    }


def test_validation_passes_with_all_benchmarks(validator):
    result = validator.validate(make_full_benchmarks())
    assert result["is_valid"] == True
    assert result["benchmarks_found"] == 5


def test_validation_fails_with_no_benchmarks(validator):
    result = validator.validate({k: None for k in ["market_average", "prime", "secondary", "large_dev", "emerging"]})
    assert result["is_valid"] == False
    assert "NO_BENCHMARKS_SELECTED" in result["flags"][0]


def test_validation_flags_low_count(validator):
    bms = {k: None for k in ["market_average", "prime", "secondary", "large_dev", "emerging"]}
    bms["market_average"] = {"city": "Riyadh", "district": "D1", "area_sqm": 800.0, "price_per_sqm": 3500.0}
    bms["prime"] = {"city": "Jeddah", "district": "D2", "area_sqm": 1500.0, "price_per_sqm": 9000.0}
    result = validator.validate(bms)
    assert result["is_valid"] == False  # only 2/5 found
    assert result["benchmarks_found"] == 2


def test_validation_warns_district_overlap_unavoidable(validator):
    bms = make_full_benchmarks()
    bms["secondary"]["district"] = "Al Olaya"  # same as market_average
    result = validator.validate(bms)
    assert any("DISTRICT_OVERLAP_UNAVOIDABLE" in w for w in result["warnings"])


def test_resolve_duplicate_replaces_lower_priority(validator, sample_df):
    from app.core.config import Settings
    from app.services.benchmark.market_average_selector import MarketAverageSelector
    from app.services.benchmark.prime_selector import PrimeSelector
    from app.services.benchmark.secondary_selector import SecondarySelector
    from app.services.benchmark.large_dev_selector import LargeDevSelector
    from app.services.benchmark.emerging_selector import EmergingSelector

    settings = Settings()
    selectors = {
        "market_average": MarketAverageSelector(settings),
        "prime": PrimeSelector(settings),
        "secondary": SecondarySelector(settings),
        "large_dev": LargeDevSelector(settings),
        "emerging": EmergingSelector(settings),
    }
    bms = {
        "market_average": {"land_transaction_id": 1, "district": "Shared", "area_sqm": 800.0, "price_per_sqm": 3500.0},
        "prime": {"land_transaction_id": 10, "district": "Shared", "area_sqm": 1200.0, "price_per_sqm": 9500.0},
        "secondary": None,
        "large_dev": None,
        "emerging": None,
    }
    resolved, notes = validator.resolve_duplicate_districts(
        bms, sample_df, selectors, rising_districts={"Diriyah"}
    )
    districts = [
        resolved[k]["district"]
        for k in resolved
        if resolved[k] and resolved[k].get("district")
    ]
    assert len(districts) == len(set(districts))
    assert any("DISTRICT_RESOLVED" in n for n in notes)
