"""
APScheduler integration: periodic benchmark selection refresh.

Currently only the cron trigger is wired (``BENCHMARK_REFRESH_CRON``). The setting
``BENCHMARK_NEW_RECORD_THRESHOLD`` is reserved for a future “enough new transactions” trigger.
"""

import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.core.config import settings

logger = logging.getLogger(__name__)


def create_scheduler(app) -> AsyncIOScheduler:
    """Build, configure, and start the async scheduler; returns the running instance."""
    scheduler = AsyncIOScheduler()

    async def scheduled_benchmark_refresh():
        logger.info("Scheduler: triggering scheduled benchmark refresh...")
        try:
            from app.core.database import AsyncSessionLocal
            from app.services.benchmark.benchmark_orchestrator import run_benchmark_selection
            async with AsyncSessionLocal() as session:
                result = await run_benchmark_selection(session, settings, trigger_type="scheduled")
                await session.commit()
                logger.info(
                    f"Scheduler: benchmark refresh completed. "
                    f"run_uuid={result.run_uuid}, found={result.benchmarks_found}/5"
                )
        except Exception as exc:
            logger.exception(f"Scheduler: benchmark refresh failed: {exc}")

    # Parse cron expression (format: "minute hour day month day_of_week")
    try:
        cron_parts = settings.BENCHMARK_REFRESH_CRON.split()
        if len(cron_parts) == 5:
            minute, hour, day, month, day_of_week = cron_parts
        else:
            minute, hour, day, month, day_of_week = "0", "2", "1", "*", "*"
    except Exception:
        minute, hour, day, month, day_of_week = "0", "2", "1", "*", "*"

    scheduler.add_job(
        scheduled_benchmark_refresh,
        trigger="cron",
        minute=minute,
        hour=hour,
        day=day,
        month=month,
        day_of_week=day_of_week,
        id="benchmark_refresh",
        replace_existing=True,
    )

    scheduler.start()
    logger.info(
        f"APScheduler started. Benchmark refresh cron: {settings.BENCHMARK_REFRESH_CRON}"
    )
    return scheduler
