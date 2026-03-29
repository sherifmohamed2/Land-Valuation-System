"""
factor_scoring_service.py  — STAGE 2 DESIGN (not yet executed)

Factor Scoring Engine

Evaluates each comparable against 10 configured factor keys.
For each factor, a multiplier (0.0–1.0) is computed using the scoring_method
defined in FactorConfig, then converted to score points:

  s_i = w_i * m_i

Where:
  w_i = factor weight (from factor_configs — must sum to 1.0)
  m_i = multiplier (computed from factor-specific logic below)

Supported scoring_method values:
  1. district_match       — multiplier by district/city match level
  2. pct_diff_lookup      — lookup table by % area difference
  3. distance_km_lookup   — lookup table by km distance (with optional fallback)
  4. distance_m_lookup    — lookup table by meter distance
  5. ratio_lookup         — lookup table by depth/frontage ratio
  6. zoning_compatibility — multiplier by zoning compatibility level
  7. count_ratio          — utility count ratio vs max possible

Input schema:
  - comparable: LandTransaction dict
  - subject: SubjectLand dict
  - factor_configs: list of FactorConfig dicts

Output schema (per comparable, per factor):
  - factor_key: str
  - weight: float
  - multiplier: float
  - score_points: float
  - raw_inputs_snapshot: dict
"""

from typing import Any


class FactorScoringService:
    """
    Stage 2 design stub. Raises NotImplementedError at runtime.

    Full scoring logic is documented in the docstring above.
    """

    def score_comparable(
        self,
        comparable: dict[str, Any],
        subject: dict[str, Any],
        factor_configs: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Score a single comparable against a subject across all configured factors.

        Returns list of factor score dicts.
        """
        raise NotImplementedError(
            "FactorScoringService.score_comparable is a Stage 2 design stub. "
            "Stage 2 execution is not yet implemented."
        )
