import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass(slots=True)
class Settings:
    telegram_bot_token: str
    anthropic_api_key: str
    anthropic_model: str
    supabase_url: str
    supabase_key: str
    app_base_url: str | None
    telegram_webhook_secret: str | None
    timezone: str
    monthly_summary_hour: int
    monthly_summary_minute: int
    admin_telegram_user_ids: tuple[int, ...]

    @property
    def webhook_path(self) -> str:
        return f"/webhook/{self.telegram_webhook_secret}" if self.telegram_webhook_secret else "/webhook"

    @property
    def webhook_url(self) -> str | None:
        if not self.app_base_url:
            return None
        return f"{self.app_base_url.rstrip('/')}{self.webhook_path}"


def get_settings() -> Settings:
    admin_ids_raw = os.getenv("ADMIN_TELEGRAM_USER_IDS", "")
    return Settings(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        anthropic_api_key=os.getenv("ANTHROPIC_API_KEY", ""),
        anthropic_model=os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5"),
        supabase_url=os.getenv("SUPABASE_URL", ""),
        supabase_key=os.getenv("SUPABASE_KEY", ""),
        app_base_url=os.getenv("APP_BASE_URL"),
        telegram_webhook_secret=os.getenv("TELEGRAM_WEBHOOK_SECRET"),
        timezone=os.getenv("TIMEZONE", "Asia/Manila"),
        monthly_summary_hour=int(os.getenv("MONTHLY_SUMMARY_HOUR", "21")),
        monthly_summary_minute=int(os.getenv("MONTHLY_SUMMARY_MINUTE", "0")),
        admin_telegram_user_ids=tuple(
            int(value.strip())
            for value in admin_ids_raw.split(",")
            if value.strip()
        ),
    )
