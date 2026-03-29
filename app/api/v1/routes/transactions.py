"""Read-only access to land transaction rows (source data for Stage 1)."""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.repositories.land_transaction_repo import LandTransactionRepository
from app.schemas.transaction import TransactionResponse, TransactionListResponse

router = APIRouter()


@router.get("/transactions", response_model=TransactionListResponse)
async def list_transactions(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Browse all land transaction records in the seeded database."""
    repo = LandTransactionRepository(db)
    total, items = await repo.get_all(limit=limit, offset=offset)
    return TransactionListResponse(
        total=total,
        limit=limit,
        offset=offset,
        items=[TransactionResponse.model_validate(item) for item in items],
    )


@router.get("/transactions/{transaction_id}", response_model=TransactionResponse)
async def get_transaction(
    transaction_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get a single land transaction by ID."""
    from app.core.exceptions import NotFoundError
    repo = LandTransactionRepository(db)
    tx = await repo.get_by_id(transaction_id)
    if not tx:
        raise NotFoundError(f"Transaction {transaction_id} not found")
    return TransactionResponse.model_validate(tx)
