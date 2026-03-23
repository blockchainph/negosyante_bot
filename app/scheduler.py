from __future__ import annotations

import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from telegram.ext import Application

from app.database import Database
from app.telegram_handler import format_revenue_summary, format_top_items_report


logger = logging.getLogger(__name__)


def build_scheduler(
    application: Application,
    database: Database,
    timezone_name: str,
    hour: int,
    minute: int,
) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone=timezone_name)
    scheduler.add_job(
        send_monthly_sales_summaries,
        trigger=CronTrigger(day="last", hour=hour, minute=minute, timezone=timezone_name),
        kwargs={"application": application, "database": database},
        id="monthly-sales-summary",
        replace_existing=True,
    )
    return scheduler


async def send_monthly_sales_summaries(application: Application, database: Database) -> None:
    user_ids = database.get_all_user_ids() if hasattr(database, "get_all_user_ids") else []
    for user_id in user_ids:
        try:
            summary = database.get_revenue_summary(user_id, "month", now=datetime.now(timezone.utc))
            top_items = database.get_top_selling_items(user_id, "month", now=datetime.now(timezone.utc))
            message = "End-of-month sales report\n" + format_revenue_summary(summary) + "\n\n" + format_top_items_report(top_items)
            await application.bot.send_message(chat_id=user_id, text=message)
        except Exception:
            logger.exception("Failed to send monthly sales summary to user %s", user_id)
