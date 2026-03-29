from __future__ import annotations
"""
comparable_filter_service.py  — STAGE 2 DESIGN (not yet executed)

Comparable Eligibility Filtering

Business rules for Stage 2 comparable selection:
  1. Must be in the same city as the subject land (enforced via factor "address")
  2. Must pass factor "address" — different-city comparables are rejected outright
  3. Must not be an outlier (is_outlier = False)
  4. Must have price_per_sqm, area_sqm, zoning (mandatory fields)
  5. Area tolerance: area_sqm within ±50% of subject area_sqm
  6. Zoning compatibility: must be in the compatible_zoning list of the subject's zoning

Rejection reasons are tracked per comparable for audit output.

Pricing equations (designed for Stage 2 execution):
  s_i = w_i * m_i          (score for factor i = weight × multiplier)
  P   = Σ s_i              (total points for comparable)
  V_j = p_j / P_j          (point value for comparable j)
  V̄   = AVG(V_j)           (average point value)
  p_target = V̄ * P_target  (estimated price/sqm for subject)

Input schema:
  - comparables: list of LandTransaction dicts (from transaction table)
  - subject: SubjectLand dict
  - factor_configs: list of FactorConfig dicts

Output schema (per comparable):
  - comparable_id: int
  - eligibility_status: "accepted" | "rejected"
  - rejection_reason: str | None
"""

from typing import Any


class ComparableFilterService:
    """
    Stage 2 design stub. Raises NotImplementedError at runtime.

    Full business logic is documented in the docstring above.
    Implementation is deferred to Stage 2 execution phase.
    """

    def filter_comparables(
        self,
        comparables: list[dict[str, Any]],
        subject: dict[str, Any],
        factor_configs: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Filter comparables for eligibility against a subject land parcel.

        Returns list of dicts with keys: comparable_id, eligibility_status, rejection_reason.
        """
        raise NotImplementedError(
            "ComparableFilterService.filter_comparables is a Stage 2 design stub. "
            "Stage 2 execution is not yet implemented."
        )
