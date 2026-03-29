"""
Market-method valuation execution — reserved for Stage 2. All routes return 501.
"""
from fastapi import APIRouter
from app.core.exceptions import Stage2NotImplementedError

router = APIRouter()


@router.post("/valuations/{subject_id}/run")
async def run_valuation(subject_id: int):
    """Stage 2 — not yet implemented. Returns 501."""
    raise Stage2NotImplementedError()


@router.get("/valuations/{subject_id}/latest")
async def get_latest_valuation(subject_id: int):
    """Stage 2 — not yet implemented. Returns 501."""
    raise Stage2NotImplementedError()
