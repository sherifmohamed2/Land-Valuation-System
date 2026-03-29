"""
HTTP exception subclasses mapped to common API error shapes.

Use these from routes and services instead of raw ``HTTPException`` where a stable
semantic is needed (404 / 422 / 500 / Stage 2 not implemented).
"""
from fastapi import HTTPException, status


class NotFoundError(HTTPException):
    """404 — missing resource (e.g. no benchmark run, unknown transaction id)."""

    def __init__(self, detail: str = "Resource not found"):
        super().__init__(status_code=status.HTTP_404_NOT_FOUND, detail=detail)


class ValidationError(HTTPException):
    """422 — request or domain validation failed."""

    def __init__(self, detail: str = "Validation failed"):
        super().__init__(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=detail)


class ServiceError(HTTPException):
    """500 — unexpected failure in application logic."""

    def __init__(self, detail: str = "Internal service error"):
        super().__init__(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=detail)


class Stage2NotImplementedError(HTTPException):
    """501 — subject/valuation endpoints reserved until Stage 2 is wired."""

    def __init__(self):
        super().__init__(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail=(
                "Stage 2 (comparable scoring and valuation execution) is designed but not yet "
                "implemented in this phase. See app/services/valuation/ for the complete design."
            ),
        )
