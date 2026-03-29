"""Stage 2 valuation run output design (not returned by API until implemented)."""
from pydantic import BaseModel
from typing import Optional


class ValuationRunSchema(BaseModel):
    """Stage 2 valuation run output schema — fully designed, not yet executed."""

    run_uuid: str
    subject_land_id: Optional[int] = None
    benchmark_run_id: Optional[int] = None
    status: str
    comparables_evaluated: Optional[int] = None
    comparables_accepted: Optional[int] = None
    average_point_value: Optional[float] = None
    target_points: Optional[float] = None
    estimated_price_per_sqm: Optional[float] = None
    estimated_total_value: Optional[float] = None

    model_config = {"from_attributes": True}


class ComparableOutputDesign(BaseModel):
    """
    Stage 2 comparable output design schema.

    Pricing equations:
      s_i = w_i * m_i           (factor score = weight × multiplier)
      P   = Σ s_i               (total points for a comparable)
      V_j = p_j / P_j           (point value for comparable j)
      V̄   = AVG(V_j)            (average point value across accepted comparables)
      p_target = V̄ * P_target   (estimated price/sqm for subject)
    """

    comparable_id: int
    eligibility_status: str  # "accepted" | "rejected"
    rejection_reason: Optional[str] = None
    factor_scores: dict  # {factor_key: {"weight": float, "multiplier": float, "points": float}}
    total_points: Optional[float] = None
    point_value: Optional[float] = None


class SubjectOutputDesign(BaseModel):
    """Stage 2 subject output design schema."""

    subject_id: int
    total_target_points: Optional[float] = None
    average_point_value: Optional[float] = None
    estimated_price_per_sqm: Optional[float] = None
    estimated_total_land_value: Optional[float] = None
