"""Stage 2 factor weights and metadata (read-only in this phase)."""
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.services.config.factor_config_service import FactorConfigService
from app.schemas.config import FactorConfigListResponse, FactorConfigSchema

router = APIRouter()


@router.get("/configs/factors", response_model=FactorConfigListResponse)
async def get_factor_configs(db: AsyncSession = Depends(get_db)):
    """
    List all Stage 2 factor configurations.

    These 10 factors define how comparable land parcels are scored during Stage 2
    valuation. Weights must sum to 1.0. Admin-configurable.
    """
    svc = FactorConfigService(db)
    configs = await svc.get_all()
    total_weight = round(sum(c.weight for c in configs), 6)
    return FactorConfigListResponse(
        total=len(configs),
        total_weight=total_weight,
        items=[FactorConfigSchema.model_validate(c) for c in configs],
    )
