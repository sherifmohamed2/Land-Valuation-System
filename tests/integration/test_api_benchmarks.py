import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.core.database import Base, get_db
from app.main import app

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@pytest_asyncio.fixture
async def client():
    """Test HTTP client with isolated in-memory database."""
    engine = create_async_engine(TEST_DB_URL, connect_args={"check_same_thread": False})
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)

    # Seed minimal data
    async with session_maker() as session:
        from datetime import datetime, date, timedelta
        from app.models.land_transaction import LandTransaction
        from app.models.district_price_trend import DistrictPriceTrend
        from app.models.factor_config import FactorConfig
        import json

        today = date.today()
        now = datetime.utcnow()
        # Benchmark-friendly seed (aligned with test_full_benchmark_run tuning)
        for i in range(4):
            session.add(LandTransaction(
                external_ref=f"API-TEST-B1-{i}", asset_type="Land", city="Riyadh",
                district=f"D{i}", zoning="Residential", area_sqm=800.0+i*50,
                road_width_m=20.0, total_price=(800.0+i*50)*3200, currency_code="SAR",
                transaction_date=str(today - timedelta(days=90)),
                utilities_json={"electricity": True}, utility_count=1,
                is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
            ))
        for i in range(3):
            session.add(LandTransaction(
                external_ref=f"API-RY-HI-{i}", asset_type="Land", city="Riyadh",
                district=f"RYH{i}", zoning="Residential", area_sqm=600.0,
                road_width_m=18.0, total_price=600.0*7200, currency_code="SAR",
                transaction_date=str(today - timedelta(days=120+i*5)),
                utilities_json={"electricity": True}, utility_count=1,
                is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
            ))
        for i in range(3):
            session.add(LandTransaction(
                external_ref=f"API-TEST-B2-{i}", asset_type="Land", city="Jeddah",
                district=f"JD{i}", zoning="Commercial", area_sqm=1200.0+i*100,
                road_width_m=24.0 + i * 8.0,
                total_price=(1200.0+i*100)*(8200.0+i*600.0), currency_code="SAR",
                transaction_date=str(today - timedelta(days=180)),
                utilities_json={"electricity": True, "water": True}, utility_count=2,
                is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
            ))
        session.add(LandTransaction(
            external_ref="API-JED-NORM", asset_type="Land", city="Jeddah",
            district="JD-NORM", zoning="Residential", area_sqm=500.0,
            road_width_m=12.0, total_price=500.0*2200, currency_code="SAR",
            transaction_date=str(today - timedelta(days=60)),
            utilities_json={"electricity": True}, utility_count=1,
            is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
        ))
        session.add(LandTransaction(
            external_ref="API-JED-NORM2", asset_type="Land", city="Jeddah",
            district="JD-NORM2", zoning="Residential", area_sqm=450.0,
            road_width_m=10.0, total_price=450.0*2100, currency_code="SAR",
            transaction_date=str(today - timedelta(days=55)),
            utilities_json={"electricity": True}, utility_count=1,
            is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
        ))
        for i in range(4):
            session.add(LandTransaction(
                external_ref=f"API-JED-HI-{i}", asset_type="Land", city="Jeddah",
                district=f"JDH{i}", zoning="Residential", area_sqm=400.0,
                road_width_m=14.0, total_price=400.0*15500, currency_code="SAR",
                transaction_date=str(today - timedelta(days=40+i)),
                utilities_json={"electricity": True}, utility_count=1,
                is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
            ))
        for i in range(3):
            session.add(LandTransaction(
                external_ref=f"API-TEST-B3-{i}", asset_type="Land", city="Dammam",
                district=f"DM{i}", zoning="Residential", area_sqm=300.0+i*30,
                road_width_m=10.0, total_price=(300.0+i*30)*(1380.0+i*35.0), currency_code="SAR",
                transaction_date=str(today - timedelta(days=400)),
                utilities_json={"electricity": True}, utility_count=1,
                is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
            ))
        session.add(LandTransaction(
            external_ref="API-DMM-WIDE", asset_type="Land", city="Dammam",
            district="DM-WIDE", zoning="Residential", area_sqm=900.0,
            road_width_m=22.0, total_price=900.0*2800, currency_code="SAR",
            transaction_date=str(today - timedelta(days=200)),
            utilities_json={"electricity": True}, utility_count=1,
            is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
        ))
        session.add(LandTransaction(
            external_ref="API-DMM-MID", asset_type="Land", city="Dammam",
            district="DM-MID", zoning="Residential", area_sqm=600.0,
            road_width_m=20.0, total_price=600.0*2600, currency_code="SAR",
            transaction_date=str(today - timedelta(days=150)),
            utilities_json={"electricity": True}, utility_count=1,
            is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
        ))
        session.add(LandTransaction(
            external_ref="API-DMM-HI", asset_type="Land", city="Dammam",
            district="DM-HI", zoning="Residential", area_sqm=700.0,
            road_width_m=26.0, total_price=700.0*2700, currency_code="SAR",
            transaction_date=str(today - timedelta(days=120)),
            utilities_json={"electricity": True}, utility_count=1,
            is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
        ))
        for i in range(3):
            session.add(LandTransaction(
                external_ref=f"API-TEST-B4-{i}", asset_type="Land", city="Riyadh",
                district=f"IND{i}", zoning="Industrial", area_sqm=2000.0+i*250,
                road_width_m=25.0, total_price=(2000.0+i*250)*3200, currency_code="SAR",
                transaction_date=str(today - timedelta(days=300)),
                utilities_json={"electricity": True}, utility_count=1,
                is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
            ))
        for i in range(3):
            session.add(LandTransaction(
                external_ref=f"API-TEST-B5-{i}", asset_type="Land", city="Riyadh",
                district="Diriyah", zoning="Residential", area_sqm=700.0+i*100,
                road_width_m=20.0, total_price=(700.0+i*100)*3450, currency_code="SAR",
                transaction_date=str(today - timedelta(days=30+i*10)),
                utilities_json={"electricity": True}, utility_count=1,
                is_cleaned=False, is_outlier=False, created_at=now, updated_at=now,
            ))
        session.add(DistrictPriceTrend(
            city="Riyadh", district="Diriyah",
            period_start="2024-01-01", period_end=str(today),
            avg_price_per_sqm=4500.0, transaction_count=10,
            trend_direction="rising", computed_at=now,
        ))
        await session.commit()

    async def override_get_db():
        async with session_maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[get_db] = override_get_db

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.mark.asyncio
async def test_health_endpoint(client):
    response = await client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["stage1"] == "active"


@pytest.mark.asyncio
async def test_post_benchmarks_run_returns_201(client):
    response = await client.post("/api/v1/benchmarks/run")
    assert response.status_code == 201


@pytest.mark.asyncio
async def test_post_benchmarks_run_has_all_five_keys(client):
    response = await client.post("/api/v1/benchmarks/run")
    data = response.json()
    assert "benchmarks" in data
    for btype in ["market_average", "prime", "secondary", "large_dev", "emerging"]:
        assert btype in data["benchmarks"]


@pytest.mark.asyncio
async def test_post_benchmarks_run_has_validation(client):
    response = await client.post("/api/v1/benchmarks/run")
    data = response.json()
    assert "validation" in data
    assert "is_valid" in data["validation"]


@pytest.mark.asyncio
async def test_get_transactions(client):
    response = await client.get("/api/v1/transactions?limit=10")
    assert response.status_code == 200
    data = response.json()
    assert "items" in data
    assert "total" in data


@pytest.mark.asyncio
async def test_subjects_returns_501(client):
    response = await client.get("/api/v1/subjects")
    assert response.status_code == 501


@pytest.mark.asyncio
async def test_valuations_returns_501(client):
    response = await client.post("/api/v1/valuations/1/run")
    assert response.status_code == 501
