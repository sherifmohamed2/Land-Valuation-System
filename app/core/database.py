"""
Async SQLAlchemy engine and session factory for the app.

``get_db`` is the FastAPI dependency: one session per request, commit on success,
rollback on error. SQLite requires ``check_same_thread=False`` when using async.
"""
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from app.core.config import settings

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    connect_args={"check_same_thread": False}   # required for SQLite
)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


class Base(DeclarativeBase):
    pass


async def create_tables():
    """Create all tables if they don't exist. Safe to call on every startup."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_db():
    """Yield a request-scoped ``AsyncSession``; commits after the route succeeds."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
