from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from telegram import Update

from app.claude_handler import ClaudeHandler
from app.config import get_settings
from app.database import Database
from app.scheduler import build_scheduler
from app.telegram_handler import build_application, post_init


logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

settings = get_settings()
database = Database(settings)
claude_handler = ClaudeHandler(settings)
telegram_app = build_application(settings.telegram_bot_token, database, claude_handler, settings)
scheduler = build_scheduler(
    application=telegram_app,
    database=database,
    timezone_name=settings.timezone,
    hour=settings.monthly_summary_hour,
    minute=settings.monthly_summary_minute,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required.")

    await telegram_app.initialize()
    await post_init(telegram_app)
    await telegram_app.start()

    if settings.webhook_url:
        await telegram_app.bot.set_webhook(url=settings.webhook_url)
        logger.info("Telegram webhook set to %s", settings.webhook_url)
    else:
        logger.warning("APP_BASE_URL is not set. Skipping automatic webhook registration.")

    scheduler.start()
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)
        await telegram_app.stop()
        await telegram_app.shutdown()


app = FastAPI(title="Telegram Sales Tracker", lifespan=lifespan)


@app.get("/")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/webhook")
@app.post("/webhook/{secret}")
async def telegram_webhook(request: Request, secret: str | None = None) -> dict[str, bool]:
    expected_secret = settings.telegram_webhook_secret
    if expected_secret and secret != expected_secret:
        raise HTTPException(status_code=403, detail="Invalid webhook secret.")

    payload = await request.json()
    update = Update.de_json(payload, telegram_app.bot)
    await telegram_app.process_update(update)
    return {"ok": True}
