import pytest
import pandas as pd
from datetime import date, timedelta
from app.services.benchmark.benchmark_scoring_service import BenchmarkScoringService
from app.core.config import Settings


@pytest.fixture
def settings():
    return Settings()


@pytest.fixture
def scorer(settings):
    return BenchmarkScoringService(settings)


def make_candidate_df():
    return pd.DataFrame([
        {
            "id": 1, "city": "Riyadh", "district": "Al Olaya",
            "zoning": "Residential", "area_sqm": 800.0,
            "road_width_m": 20.0, "price_per_sqm": 3500.0,
            "transaction_date": str(date.today() - timedelta(days=60)),
            "latitude": 24.69, "longitude": 46.72,
            "is_cleaned": False, "is_outlier": False,
            "price_percentile": 50.0, "area_category": "Medium",
            "city_road_median": 20.0, "city_ppsqm_median": 3500.0,
            "total_price": 2800000, "frontage_m": 28.0, "depth_m": 32.0,
        }
    ])


def test_scoring_returns_tiebreaker_score(scorer):
    df = make_candidate_df()
    result = scorer.score_candidates(df)
    assert "tiebreaker_score" in result.columns
    assert result.loc[result.index[0], "tiebreaker_score"] > 0


def test_scoring_weight_breakdown_sums_to_score(scorer, settings):
    df = make_candidate_df()
    result = scorer.score_candidates(df)
    row = result.iloc[0]
    expected = (
        settings.SCORE_WEIGHT_PRICE_STABILITY * row["_score_price_stability"]
        + settings.SCORE_WEIGHT_COMPLETENESS * row["_score_completeness"]
        + settings.SCORE_WEIGHT_CENTRALITY * row["_score_centrality"]
        + settings.SCORE_WEIGHT_RECENCY * row["_score_recency"]
    )
    assert abs(row["tiebreaker_score"] - expected) < 1e-4


def test_scoring_empty_df_returns_empty(scorer):
    df = pd.DataFrame()
    result = scorer.score_candidates(df)
    assert result.empty


def test_build_score_breakdown_keys(scorer):
    df = make_candidate_df()
    result = scorer.score_candidates(df)
    row = result.iloc[0]
    breakdown = scorer.build_score_breakdown(row)
    assert set(breakdown.keys()) == {"price_stability", "completeness", "centrality", "recency", "total"}
