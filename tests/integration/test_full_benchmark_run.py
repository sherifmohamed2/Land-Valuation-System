import pytest
import pytest_asyncio
import json
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import select
from app.core.database import Base
from app.core.config import Settings

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@pytest_asyncio.fixture
async def test_db():
    """Fresh in-memory DB for each test. Pre-seeded with controlled records."""
    engine = create_async_engine(TEST_DB_URL, connect_args={"check_same_thread": False})
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as session:
        await _seed_test_data(session)
        await session.commit()
        yield session
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


async def _seed_test_data(session: AsyncSession):
    """Insert minimum controlled records to guarantee each benchmark finds candidates."""
    from app.models.land_transaction import LandTransaction
    from app.models.district_price_trend import DistrictPriceTrend
    from datetime import date

    today = date.today()
    now = datetime.utcnow()

    transactions = [
        # B1: Market Average — Medium area, road=20; ppsqm chosen to land ~45–55 pct vs pool
        *[LandTransaction(
            external_ref=f"TEST-B1-{i}", asset_type="Land", region="Central",
            city="Riyadh", district=f"District-B1-{i}", zoning="Residential",
            area_sqm=800.0 + i*50, frontage_m=28.0, depth_m=32.0, road_width_m=20.0,
            total_price=(800.0 + i*50) * 3200.0, currency_code="SAR",
            transaction_date=str(today - timedelta(days=90+i*10)),
            utilities_json={"electricity": True, "water": True, "sewerage": True},
            utility_count=3, verification_status="verified", data_source="test",
            is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
        ) for i in range(4)],
        # High ppsqm Riyadh noise (pulls mid-tier percentiles into B1 band)
        *[LandTransaction(
            external_ref=f"TEST-RY-HI-{i}", asset_type="Land", region="Central",
            city="Riyadh", district=f"District-RY-HI-{i}", zoning="Residential",
            area_sqm=600.0, frontage_m=22.0, depth_m=28.0, road_width_m=18.0,
            total_price=600.0 * 7200.0, currency_code="SAR",
            transaction_date=str(today - timedelta(days=120+i*5)),
            utilities_json={"electricity": True}, utility_count=1,
            verification_status="verified", data_source="test",
            is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
        ) for i in range(3)],
        # B2: Prime — high pct, Commercial; stagger ppsqm so not all hit top outlier band
        *[LandTransaction(
            external_ref=f"TEST-B2-{i}", asset_type="Land", region="Central",
            city="Jeddah", district=f"District-B2-{i}", zoning="Commercial",
            area_sqm=1200.0 + i*100, frontage_m=40.0, depth_m=50.0,
            road_width_m=(24.0 + i * 8.0),
            total_price=(1200.0 + i*100) * (8200.0 + i * 600.0),
            currency_code="SAR",
            transaction_date=str(today - timedelta(days=180+i*10)),
            utilities_json={"electricity": True, "water": True, "sewerage": True, "fiber": True, "gas": True},
            utility_count=5, verification_status="verified", data_source="test",
            is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
        ) for i in range(3)],
        # Extra Jeddah land (pulls road median down so prime parcels stay strictly wider)
        LandTransaction(
            external_ref="TEST-JED-LOWROAD", asset_type="Land", region="Central",
            city="Jeddah", district="District-JED-NORM", zoning="Residential",
            area_sqm=500.0, frontage_m=20.0, depth_m=25.0, road_width_m=12.0,
            total_price=500.0 * 2200.0, currency_code="SAR",
            transaction_date=str(today - timedelta(days=60)),
            utilities_json={"electricity": True}, utility_count=1,
            verification_status="verified", data_source="test",
            is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
        ),
        LandTransaction(
            external_ref="TEST-JED-LOWROAD-2", asset_type="Land", region="Central",
            city="Jeddah", district="District-JED-NORM2", zoning="Residential",
            area_sqm=450.0, frontage_m=18.0, depth_m=22.0, road_width_m=10.0,
            total_price=450.0 * 2100.0, currency_code="SAR",
            transaction_date=str(today - timedelta(days=55)),
            utilities_json={"electricity": True}, utility_count=1,
            verification_status="verified", data_source="test",
            is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
        ),
        *[LandTransaction(
            external_ref=f"TEST-JED-HI-{i}", asset_type="Land", region="Central",
            city="Jeddah", district=f"District-JED-HI-{i}", zoning="Residential",
            area_sqm=400.0, frontage_m=18.0, depth_m=22.0, road_width_m=14.0,
            total_price=400.0 * 15500.0, currency_code="SAR",
            transaction_date=str(today - timedelta(days=40 + i)),
            utilities_json={"electricity": True}, utility_count=1,
            verification_status="verified", data_source="test",
            is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
        ) for i in range(4)],
        # B3: Secondary — low pct, narrow road; ppsqm above bottom outlier cutoff
        *[LandTransaction(
            external_ref=f"TEST-B3-{i}", asset_type="Land", region="Peripheral",
            city="Dammam", district=f"District-B3-{i}", zoning="Residential",
            area_sqm=300.0 + i*30, frontage_m=15.0, depth_m=20.0, road_width_m=10.0,
            total_price=(300.0 + i*30) * (1380.0 + i * 35.0),
            currency_code="SAR",
            transaction_date=str(today - timedelta(days=400+i*10)),
            utilities_json={"electricity": True}, utility_count=1,
            verification_status="unverified", data_source="test",
            is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
        ) for i in range(3)],
        # Wider-road Dammam parcels so city median road > 10 (secondary: road < median)
        LandTransaction(
            external_ref="TEST-DMM-WIDE", asset_type="Land", region="Peripheral",
            city="Dammam", district="District-DMM-WIDE", zoning="Residential",
            area_sqm=900.0, frontage_m=30.0, depth_m=35.0, road_width_m=22.0,
            total_price=900.0 * 2800.0, currency_code="SAR",
            transaction_date=str(today - timedelta(days=200)),
            utilities_json={"electricity": True, "water": True}, utility_count=2,
            verification_status="verified", data_source="test",
            is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
        ),
        LandTransaction(
            external_ref="TEST-DMM-MID", asset_type="Land", region="Peripheral",
            city="Dammam", district="District-DMM-MID", zoning="Residential",
            area_sqm=600.0, frontage_m=22.0, depth_m=28.0, road_width_m=20.0,
            total_price=600.0 * 2600.0, currency_code="SAR",
            transaction_date=str(today - timedelta(days=150)),
            utilities_json={"electricity": True}, utility_count=1,
            verification_status="verified", data_source="test",
            is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
        ),
        LandTransaction(
            external_ref="TEST-DMM-HI", asset_type="Land", region="Peripheral",
            city="Dammam", district="District-DMM-HI", zoning="Residential",
            area_sqm=700.0, frontage_m=25.0, depth_m=30.0, road_width_m=26.0,
            total_price=700.0 * 2700.0, currency_code="SAR",
            transaction_date=str(today - timedelta(days=120)),
            utilities_json={"electricity": True}, utility_count=1,
            verification_status="verified", data_source="test",
            is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
        ),
        # B4: Large Dev — Industrial, area_category Large (1200–3000 sqm band)
        *[LandTransaction(
            external_ref=f"TEST-B4-{i}", asset_type="Land", region="Industrial",
            city="Riyadh", district=f"District-B4-{i}", zoning="Industrial",
            area_sqm=2000.0 + i*250, frontage_m=45.0, depth_m=55.0, road_width_m=25.0,
            total_price=(2000.0 + i*250) * 3200.0, currency_code="SAR",
            transaction_date=str(today - timedelta(days=300+i*10)),
            utilities_json={"electricity": True, "water": True, "gas": True}, utility_count=3,
            verification_status="verified", data_source="test",
            is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
        ) for i in range(3)],
        # B5: Emerging — recent, Diriyah; ppsqm tuned to B5 percentile band (50–70)
        *[LandTransaction(
            external_ref=f"TEST-B5-{i}", asset_type="Land", region="Central",
            city="Riyadh", district="Diriyah", zoning="Residential",
            area_sqm=700.0 + i*100, frontage_m=25.0, depth_m=30.0, road_width_m=20.0,
            total_price=(700.0 + i*100) * 3450.0, currency_code="SAR",
            transaction_date=str(today - timedelta(days=30+i*10)),
            utilities_json={"electricity": True, "water": True, "sewerage": True, "fiber": True},
            utility_count=4, verification_status="verified", data_source="test",
            is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
        ) for i in range(3)],
    ]

    for tx in transactions:
        session.add(tx)
    await session.flush()

    # Add district price trend for Diriyah
    trend = DistrictPriceTrend(
        city="Riyadh", district="Diriyah",
        period_start="2024-01-01", period_end=str(today),
        avg_price_per_sqm=4500.0, transaction_count=10,
        trend_direction="rising", computed_at=now,
    )
    session.add(trend)
    await session.flush()


@pytest.mark.asyncio
async def test_full_pipeline_returns_benchmarks(test_db):
    from app.services.benchmark.benchmark_orchestrator import run_benchmark_selection
    settings = Settings()
    result = await run_benchmark_selection(test_db, settings)
    found = [k for k, v in result.benchmarks.items() if v is not None]
    assert len(found) >= 3, f"Expected at least 3 benchmarks, got {len(found)}: {found}"


@pytest.mark.asyncio
async def test_run_stored_in_db(test_db):
    from app.models.benchmark_run import BenchmarkRun
    from app.services.benchmark.benchmark_orchestrator import run_benchmark_selection
    settings = Settings()
    await run_benchmark_selection(test_db, settings)
    result = await test_db.execute(select(BenchmarkRun).where(BenchmarkRun.status == "completed"))
    runs = result.scalars().all()
    assert len(runs) >= 1


@pytest.mark.asyncio
async def test_benchmark_results_stored(test_db):
    from app.models.benchmark_result import BenchmarkResult
    from app.services.benchmark.benchmark_orchestrator import run_benchmark_selection
    settings = Settings()
    await run_benchmark_selection(test_db, settings)
    result = await test_db.execute(select(BenchmarkResult))
    results = result.scalars().all()
    assert len(results) >= 1


@pytest.mark.asyncio
async def test_audit_log_written(test_db):
    from app.models.audit_log import AuditLog
    from app.services.benchmark.benchmark_orchestrator import run_benchmark_selection
    settings = Settings()
    await run_benchmark_selection(test_db, settings)
    result = await test_db.execute(
        select(AuditLog).where(AuditLog.entity_type == "benchmark_run")
    )
    logs = result.scalars().all()
    assert len(logs) >= 1
