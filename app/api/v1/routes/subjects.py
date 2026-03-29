"""
Subject land CRUD — reserved for Stage 2. All routes return 501 until implemented.
"""
from fastapi import APIRouter
from app.core.exceptions import Stage2NotImplementedError

router = APIRouter()


@router.get("/subjects")
async def list_subjects():
    """Stage 2 — not yet implemented. Returns 501."""
    raise Stage2NotImplementedError()


@router.post("/subjects")
async def create_subject():
    """Stage 2 — not yet implemented. Returns 501."""
    raise Stage2NotImplementedError()


@router.get("/subjects/{subject_id}")
async def get_subject(subject_id: int):
    """Stage 2 — not yet implemented. Returns 501."""
    raise Stage2NotImplementedError()
