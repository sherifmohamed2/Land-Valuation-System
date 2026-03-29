from __future__ import annotations
"""
valuation_service.py  — STAGE 2 DESIGN (not yet executed)

Valuation Service

Orchestrates the full Stage 2 valuation workflow for a given subject land:
  1. Load subject land from DB
  2. Load most recent benchmark_run results
  3. Load all transactions as comparable candidates
  4. Filter comparables (ComparableFilterService)
  5. Score accepted comparables (FactorScoringService)
  6. Compute target points for subject (FactorScoringService)
  7. Run pricing engine (PricingEngine)
  8. Persist valuation_run and valuation_factor_scores records
  9. Write audit log

This service is the entry point for POST /api/v1/valuations/{subject_id}/run

Input schema:
  - subject_id: int
  - benchmark_run_id: int | None (defaults to latest completed run)

Output schema (see app/schemas/valuation.py):
  - ValuationRunSchema with all pricing results
"""

from typing import Any


class ValuationService:
    """
    Stage 2 design stub. Raises NotImplementedError at runtime.

    Full orchestration logic documented in the docstring above.
    """

    async def run_valuation(
        self,
        subject_id: int,
        benchmark_run_id: int | None = None,
    ) -> dict[str, Any]:
        """
        Execute a full Stage 2 valuation for a subject land parcel.

        Returns valuation result conforming to ValuationRunSchema.
        """
        raise NotImplementedError(
            "ValuationService.run_valuation is a Stage 2 design stub. "
            "Stage 2 execution is not yet implemented."
        )
