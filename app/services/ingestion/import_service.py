"""ORM → dict bridge so Stage 1 can run pandas pipelines without raw SQL."""
import logging
from typing import Any
from app.models.land_transaction import LandTransaction

logger = logging.getLogger(__name__)


class ImportService:
    """
    Handles reading raw transaction records from the database for pipeline ingestion.
    In production this could be extended to accept CSV/JSON uploads or pull from an API feed.
    For Stage 1, transactions are already pre-loaded in the SQLite DB.
    """

    @staticmethod
    def to_dict(tx: LandTransaction) -> dict[str, Any]:
        """Convert a LandTransaction ORM row to a plain dict for pandas processing."""
        return {
            "id": tx.id,
            "external_ref": tx.external_ref,
            "asset_type": tx.asset_type,
            "region": tx.region,
            "city": tx.city,
            "district": tx.district,
            "street": tx.street,
            "latitude": tx.latitude,
            "longitude": tx.longitude,
            "zoning": tx.zoning,
            "land_use": tx.land_use,
            "area_sqm": tx.area_sqm,
            "frontage_m": tx.frontage_m,
            "depth_m": tx.depth_m,
            "road_width_m": tx.road_width_m,
            "total_price": tx.total_price,
            "currency_code": tx.currency_code,
            "standardized_total_price": tx.standardized_total_price,
            "price_per_sqm": tx.price_per_sqm,
            "transaction_date": tx.transaction_date,
            "utilities_json": tx.utilities_json,
            "utility_count": tx.utility_count,
            "verification_status": tx.verification_status,
            "data_source": tx.data_source,
            "is_cleaned": tx.is_cleaned,
            "is_outlier": tx.is_outlier,
            "area_category": tx.area_category,
            "price_percentile": tx.price_percentile,
            "price_band": tx.price_band,
            "city_road_median": tx.city_road_median,
            "city_ppsqm_median": tx.city_ppsqm_median,
        }
