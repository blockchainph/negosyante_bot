from __future__ import annotations

import logging
from datetime import datetime, timezone

from telegram import BotCommand, Update
from telegram.constants import ChatAction
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from app.claude_handler import ClaudeHandler
from app.config import Settings
from app.database import Database


logger = logging.getLogger(__name__)


def build_application(
    token: str,
    database: Database,
    claude_handler: ClaudeHandler,
    settings: Settings,
) -> Application:
    application = Application.builder().token(token).build()
    application.bot_data["db"] = database
    application.bot_data["claude"] = claude_handler
    application.bot_data["settings"] = settings

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("summary", summary_command))
    application.add_handler(CommandHandler("topitems", top_items_command))
    application.add_handler(CommandHandler("prices", prices_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))
    return application


async def post_init(application: Application) -> None:
    await application.bot.set_my_commands(
        [
            BotCommand("start", "See how to log sales"),
            BotCommand("help", "Show examples and features"),
            BotCommand("summary", "Get today's revenue summary"),
            BotCommand("topitems", "Show top-selling items this month"),
            BotCommand("prices", "Show your current price list"),
        ]
    )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = (
        "Send sales naturally, like:\n"
        "`3 coke 20, 2 lucky me 15, 1 soap 35`\n"
        "`sold 2 alaska 35 each and 1 bread 45`\n\n"
        "You can also ask:\n"
        "`sales summary today`\n"
        "`weekly revenue`\n"
        "`top selling items this month`\n"
        "`set price coke 20`\n"
        "`set stock coke 24 reorder 6`"
    )
    await update.effective_message.reply_text(message, parse_mode="Markdown")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = (
        "This bot records sari-sari store sales, keeps a price list per owner, and tracks revenue by day, week, and month.\n\n"
        "It can also show top-selling items and warn you when stock falls below your reorder level."
    )
    await update.effective_message.reply_text(message)


async def summary_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Database = context.application.bot_data["db"]
    user = update.effective_user
    summary = db.get_revenue_summary(user.id, "today")
    await update.effective_message.reply_text(format_revenue_summary(summary))


async def top_items_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Database = context.application.bot_data["db"]
    user = update.effective_user
    report = db.get_top_selling_items(user.id, "month")
    await update.effective_message.reply_text(format_top_items_report(report))


async def prices_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Database = context.application.bot_data["db"]
    user = update.effective_user
    price_list = db.get_price_list(user.id)
    await update.effective_message.reply_text(format_price_list(price_list))


async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Database = context.application.bot_data["db"]
    claude: ClaudeHandler = context.application.bot_data["claude"]
    message = update.effective_message
    user = update.effective_user
    if not message or not user or not message.text:
        return

    text = message.text.strip()
    await context.bot.send_chat_action(chat_id=message.chat_id, action=ChatAction.TYPING)

    try:
        parsed = await claude.parse_message(text)
    except Exception:
        logger.exception("Claude parsing failed")
        await message.reply_text("I had trouble reading that. Please try again in a moment.")
        return

    if parsed["needs_clarification"]:
        await message.reply_text(parsed["clarification_message"] or "I need a bit more detail before I record that sale.")
        return

    intent = parsed["intent"]
    if intent == "sale_record":
        prepared_items, missing_items = db.prepare_sale_items(user.id, parsed["line_items"])
        if missing_items:
            missing_text = ", ".join(missing_items)
            await message.reply_text(
                f"I still need prices for: {missing_text}. Set them first with messages like `set price coke 20`, or include the prices in the sale message.",
                parse_mode="Markdown",
            )
            return
        if not prepared_items:
            await message.reply_text("Please send at least one sale item, like `2 coke 20 and 1 bread 45`.")
            return

        total_amount = parsed.get("total_amount") or sum(item["line_total"] for item in prepared_items)
        transaction = db.save_sale(
            telegram_user_id=user.id,
            telegram_username=user.username,
            line_items=prepared_items,
            total_amount=total_amount,
            currency=parsed.get("currency") or "PHP",
            raw_message=text,
        )
        await message.reply_text(format_sale_saved_message(transaction))
        for warning in transaction.get("stock_warnings", []):
            await message.reply_text(format_low_stock_warning(warning))
        return

    if intent == "revenue_summary":
        summary = db.get_revenue_summary(user.id, parsed.get("period") or "today", now=datetime.now(timezone.utc))
        await message.reply_text(format_revenue_summary(summary))
        return

    if intent == "top_items":
        report = db.get_top_selling_items(user.id, parsed.get("period") or "month", now=datetime.now(timezone.utc))
        await message.reply_text(format_top_items_report(report))
        return

    if intent == "price_set":
        item_name = parsed.get("item_name")
        unit_price = parsed.get("unit_price")
        if not item_name or unit_price is None or unit_price <= 0:
            await message.reply_text("Please send a price like `set price coke 20`.")
            return
        item = db.upsert_price_list_item(
            telegram_user_id=user.id,
            item_name=item_name,
            unit_price=unit_price,
            currency=parsed.get("currency") or "PHP",
        )
        await message.reply_text(f"Saved price for {item['item_name']}: {format_money(item['unit_price'], item['currency'])}.")
        return

    if intent == "price_show":
        price_list = db.get_price_list(user.id)
        await message.reply_text(format_price_list(price_list))
        return

    if intent == "stock_update":
        item_name = parsed.get("item_name")
        stock_quantity = parsed.get("stock_quantity")
        reorder_level = parsed.get("reorder_level")
        if not item_name or (stock_quantity is None and reorder_level is None):
            await message.reply_text("Please send stock info like `set stock coke 24 reorder 6`.")
            return
        item = db.upsert_price_list_item(
            telegram_user_id=user.id,
            item_name=item_name,
            stock_quantity=stock_quantity,
            reorder_level=reorder_level,
            currency=parsed.get("currency") or "PHP",
        )
        await message.reply_text(format_stock_update_message(item))
        return

    await message.reply_text(
        "I can record sales, manage prices, track stock, and show revenue summaries. Try `3 coke 20, 1 bread 45` or `set price coke 20`."
    )


def format_revenue_summary(summary: dict[str, object]) -> str:
    return (
        f"Revenue summary for {summary['label']}\n"
        f"Total revenue: {format_money(summary['total'], summary['currency'])}\n"
        f"Transactions: {summary['count']}"
    )


def format_top_items_report(report: dict[str, object]) -> str:
    items = report["items"]
    if not items:
        return f"No sales recorded for {report['label']} yet."
    lines = [f"Top selling items for {report['label']}:"]
    for item in items:
        lines.append(
            f"- {item['item_name']}: {item['quantity']} sold, {format_money(item['revenue'], report['currency'])}"
        )
    return "\n".join(lines)


def format_price_list(price_list: list[dict[str, object]]) -> str:
    if not price_list:
        return "Your price list is empty. Add one with `set price coke 20`."
    lines = ["Current price list:"]
    for item in price_list[:20]:
        stock_text = ""
        if item.get("stock_quantity") is not None:
            stock_text = f", stock {int(item['stock_quantity'])}"
        if item.get("reorder_level") is not None:
            stock_text += f", reorder at {int(item['reorder_level'])}"
        lines.append(f"- {item['item_name']}: {format_money(item['unit_price'], item['currency'])}{stock_text}")
    return "\n".join(lines)


def format_sale_saved_message(transaction: dict[str, object]) -> str:
    lines = [f"Saved sale: {format_money(transaction['total_amount'], transaction['currency'])}", "Items:"]
    for item in transaction["line_items"]:
        lines.append(
            f"- {item['item_name']}: {int(item['quantity'])} x {format_money(item['unit_price'], transaction['currency'])} = {format_money(item['line_total'], transaction['currency'])}"
        )
    return "\n".join(lines)


def format_stock_update_message(item: dict[str, object]) -> str:
    parts = [f"Updated {item['item_name']}"]
    if item.get("unit_price") is not None:
        parts.append(f"price {format_money(item['unit_price'], item['currency'])}")
    if item.get("stock_quantity") is not None:
        parts.append(f"stock {int(item['stock_quantity'])}")
    if item.get("reorder_level") is not None:
        parts.append(f"reorder {int(item['reorder_level'])}")
    return "Saved: " + ", ".join(parts) + "."


def format_low_stock_warning(warning: dict[str, object]) -> str:
    return (
        f"Low stock warning: {warning['item_name']} is now at {warning['stock_quantity']} "
        f"which is at or below the reorder level of {warning['reorder_level']}."
    )


def format_money(amount: float | int | str, currency: str) -> str:
    symbol = "₱" if (currency or "").upper() == "PHP" else f"{currency.upper()} "
    return f"{symbol}{float(amount):,.2f}"
