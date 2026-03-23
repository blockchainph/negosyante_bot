from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

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
    application.add_handler(CommandHandler("stats", stats_command))
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
            BotCommand("stats", "Show admin usage stats"),
        ]
    )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await track_user_context(update, context, event_type="start_command")
    message = (
        "Created by @FerdieCPS\n\n"
        "Send sales naturally, like:\n"
        "`3 coke 20, 2 lucky me 15, 1 soap 35`\n"
        "`sold 2 alaska 35 each and 1 bread 45`\n\n"
        "You can also ask:\n"
        "`sales summary today`\n"
        "`weekly revenue`\n"
        "`top selling items this month`\n"
        "`set price coke 20`\n"
        "`set stock coke 24 reorder 6`\n"
        "`utang kay ana: 2 coke 20, 1 bread 45`\n"
        "`ana paid 100`\n"
        "`how much utang ni ana`"
    )
    await update.effective_message.reply_text(message, parse_mode="Markdown")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await track_user_context(update, context, event_type="help_command")
    message = (
        "This bot records sari-sari store sales, keeps a price list per owner, and tracks revenue by day, week, and month.\n\n"
        "It can also show top-selling items, track utang balances, and warn you when stock falls below your reorder level."
    )
    await update.effective_message.reply_text(message)


async def summary_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Database = context.application.bot_data["db"]
    user = update.effective_user
    await track_user_context(update, context, event_type="summary_command")
    summary = db.get_revenue_summary(user.id, "today")
    await update.effective_message.reply_text(format_revenue_summary(summary))


async def top_items_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Database = context.application.bot_data["db"]
    user = update.effective_user
    await track_user_context(update, context, event_type="top_items_command")
    report = db.get_top_selling_items(user.id, "month")
    await update.effective_message.reply_text(format_top_items_report(report))


async def prices_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Database = context.application.bot_data["db"]
    user = update.effective_user
    await track_user_context(update, context, event_type="prices_command")
    price_list = db.get_price_list(user.id)
    await update.effective_message.reply_text(format_price_list(price_list))


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Database = context.application.bot_data["db"]
    settings: Settings = context.application.bot_data["settings"]
    user = update.effective_user
    await track_user_context(update, context, event_type="stats_command")

    if not user or user.id not in settings.admin_telegram_user_ids:
        await update.effective_message.reply_text("This command is only available to bot admins.")
        return

    stats = db.get_usage_stats()
    await update.effective_message.reply_text(format_stats_message(stats))


async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Database = context.application.bot_data["db"]
    claude: ClaudeHandler = context.application.bot_data["claude"]
    message = update.effective_message
    user = update.effective_user
    if not message or not user or not message.text:
        return

    await track_user_context(update, context, event_type="message_received", message_text=message.text)
    text = message.text.strip()
    await context.bot.send_chat_action(chat_id=message.chat_id, action=ChatAction.TYPING)

    if await resume_pending_action(update, context, text):
        return

    try:
        parsed = await claude.parse_message(text)
    except Exception:
        logger.exception("Claude parsing failed")
        db.log_event(user.id, "claude_parse_error", message_text=text)
        await message.reply_text("I had trouble reading that. Please try again in a moment.")
        return

    if parsed["needs_clarification"]:
        pending_action = build_pending_action_from_parsed(parsed, text)
        if pending_action:
            context.user_data["pending_action"] = pending_action
        db.log_event(user.id, "clarification_requested", message_text=text, metadata={"intent": parsed["intent"]})
        await message.reply_text(parsed["clarification_message"] or "I need a bit more detail before I record that sale.")
        return

    intent = parsed["intent"]
    if intent == "sale_record":
        success = await process_sale_like_intent(
            update=update,
            context=context,
            parsed=parsed,
            raw_message=text,
            is_utang=False,
        )
        if success:
            context.user_data.pop("pending_action", None)
        return

    if intent == "utang_record":
        success = await process_sale_like_intent(
            update=update,
            context=context,
            parsed=parsed,
            raw_message=text,
            is_utang=True,
        )
        if success:
            context.user_data.pop("pending_action", None)
        return

    if intent == "payment_record":
        customer_name = parsed.get("customer_name")
        total_amount = parsed.get("total_amount")
        if not customer_name or total_amount is None or total_amount <= 0:
            await message.reply_text("Please send a payment like `ana paid 100`.")
            return
        payment = db.record_utang_payment(
            telegram_user_id=user.id,
            customer_name=customer_name,
            amount=total_amount,
            currency=parsed.get("currency") or "PHP",
        )
        db.log_event(user.id, "utang_payment_recorded", message_text=text, metadata={"customer_name": payment["customer_name"], "amount": float(payment["amount"])})
        await message.reply_text(format_utang_payment_message(payment))
        return

    if intent == "balance_query":
        customer_name = parsed.get("customer_name")
        if customer_name:
            balance = db.get_customer_balance(user.id, customer_name)
            db.log_event(user.id, "balance_query", message_text=text, metadata={"customer_name": customer_name})
            await message.reply_text(format_customer_balance(balance))
            return
        balances = db.get_all_balances(user.id)
        db.log_event(user.id, "balance_query", message_text=text)
        await message.reply_text(format_all_balances(balances))
        return

    if intent == "revenue_summary":
        summary = db.get_revenue_summary(user.id, parsed.get("period") or "today", now=datetime.now(timezone.utc))
        db.log_event(user.id, "revenue_summary_requested", message_text=text, metadata={"period": parsed.get("period") or "today"})
        await message.reply_text(format_revenue_summary(summary))
        return

    if intent == "top_items":
        report = db.get_top_selling_items(user.id, parsed.get("period") or "month", now=datetime.now(timezone.utc))
        db.log_event(user.id, "top_items_requested", message_text=text, metadata={"period": parsed.get("period") or "month"})
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
        db.log_event(user.id, "price_set", message_text=text, metadata={"item_name": item["item_name"], "unit_price": float(item["unit_price"])})
        await message.reply_text(f"Saved price for {item['item_name']}: {format_money(item['unit_price'], item['currency'])}.")
        return

    if intent == "price_show":
        price_list = db.get_price_list(user.id)
        db.log_event(user.id, "price_list_viewed", message_text=text)
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
        db.log_event(
            user.id,
            "stock_updated",
            message_text=text,
            metadata={"item_name": item["item_name"], "stock_quantity": item.get("stock_quantity"), "reorder_level": item.get("reorder_level")},
        )
        await message.reply_text(format_stock_update_message(item))
        return

    db.log_event(user.id, "unknown_message", message_text=text)
    await message.reply_text(
        "I can record sales, manage prices, track stock, and handle utang. Try `3 coke 20, 1 bread 45`, `utang kay ana: 2 coke 20`, or `ana paid 100`."
    )


async def process_sale_like_intent(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    parsed: dict[str, Any],
    raw_message: str,
    *,
    is_utang: bool,
) -> bool:
    db: Database = context.application.bot_data["db"]
    message = update.effective_message
    user = update.effective_user
    if not message or not user:
        return False

    customer_name = parsed.get("customer_name")
    if is_utang and not customer_name:
        await message.reply_text("Please tell me who the utang is for, like `utang kay ana: 2 coke 20`.")
        return False

    prepared_items, missing_items = db.prepare_sale_items(user.id, parsed["line_items"])
    if missing_items:
        context.user_data["pending_action"] = {
            "type": "missing_price",
            "intent": "utang_record" if is_utang else "sale_record",
            "customer_name": customer_name,
            "line_items": parsed["line_items"],
            "currency": parsed.get("currency") or "PHP",
            "raw_message": raw_message,
            "missing_items": missing_items,
        }
        missing_text = ", ".join(missing_items)
        if len(missing_items) == 1:
            await message.reply_text(
                f"What is the unit price of {missing_items[0]}? You can reply with just the amount, like `2 pesos`."
            )
        else:
            await message.reply_text(
                f"I still need prices for: {missing_text}. Reply one at a time like `odong 2` or set them with `set price odong 2`.",
                parse_mode="Markdown",
            )
        return False

    if not prepared_items:
        await message.reply_text(
            "Please send at least one utang item, like `utang kay ana: 2 coke 20`."
            if is_utang
            else "Please send at least one sale item, like `2 coke 20 and 1 bread 45`."
        )
        return False

    total_amount = parsed.get("total_amount") or sum(item["line_total"] for item in prepared_items)
    if is_utang:
        sale = db.save_utang_sale(
            telegram_user_id=user.id,
            telegram_username=user.username,
            customer_name=customer_name,
            line_items=prepared_items,
            total_amount=total_amount,
            currency=parsed.get("currency") or "PHP",
            raw_message=raw_message,
        )
        db.log_event(
            user.id,
            "utang_recorded",
            message_text=raw_message,
            metadata={"customer_name": sale["customer_name"], "total_amount": float(sale["total_amount"])},
        )
        await message.reply_text(format_utang_sale_message(sale))
        for warning in sale.get("stock_warnings", []):
            await message.reply_text(format_low_stock_warning(warning))
    else:
        transaction = db.save_sale(
            telegram_user_id=user.id,
            telegram_username=user.username,
            line_items=prepared_items,
            total_amount=total_amount,
            currency=parsed.get("currency") or "PHP",
            raw_message=raw_message,
        )
        db.log_event(
            user.id,
            "sale_recorded",
            message_text=raw_message,
            metadata={"total_amount": float(transaction["total_amount"])},
        )
        await message.reply_text(format_sale_saved_message(transaction))
        for warning in transaction.get("stock_warnings", []):
            await message.reply_text(format_low_stock_warning(warning))
    return True


async def resume_pending_action(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
) -> bool:
    pending = context.user_data.get("pending_action")
    if not pending:
        return False

    pending_type = pending.get("type")
    if pending_type == "summary_period":
        return await resolve_summary_period_follow_up(update, context, text, pending)
    if pending_type == "missing_price":
        return await resolve_missing_price_follow_up(update, context, text, pending)
    return False


def build_pending_action_from_parsed(parsed: dict[str, Any], raw_message: str) -> dict[str, Any] | None:
    if parsed["intent"] == "revenue_summary":
        return {"type": "summary_period", "raw_message": raw_message}
    if parsed["intent"] in {"sale_record", "utang_record"} and parsed.get("line_items"):
        return {
            "type": "missing_price",
            "intent": parsed["intent"],
            "customer_name": parsed.get("customer_name"),
            "line_items": parsed["line_items"],
            "currency": parsed.get("currency") or "PHP",
            "raw_message": raw_message,
        }
    return None


async def resolve_summary_period_follow_up(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    pending: dict[str, Any],
) -> bool:
    db: Database = context.application.bot_data["db"]
    message = update.effective_message
    user = update.effective_user
    if not message or not user:
        return False

    period = detect_period(text)
    if not period:
        if text.strip().lower() in {"yes", "y", "ok", "okay", "sige"}:
            period = "today"
        else:
            await message.reply_text("Please reply with `today`, `week`, or `month`.")
            return True

    context.user_data.pop("pending_action", None)
    summary = db.get_revenue_summary(user.id, period, now=datetime.now(timezone.utc))
    db.log_event(user.id, "revenue_summary_requested", message_text=pending.get("raw_message"), metadata={"period": period, "follow_up": True})
    await message.reply_text(format_revenue_summary(summary))
    return True


async def resolve_missing_price_follow_up(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    pending: dict[str, Any],
) -> bool:
    message = update.effective_message
    if not message:
        return False

    line_items = [dict(item) for item in pending.get("line_items", [])]
    unresolved = [
        item for item in line_items
        if item.get("unit_price") in (None, "", 0) and item.get("line_total") in (None, "", 0)
    ]
    if not unresolved:
        context.user_data.pop("pending_action", None)
        return False

    target_item = choose_pending_item(unresolved, text)
    amount = extract_first_amount(text)
    if not target_item or amount is None or amount <= 0:
        if len(unresolved) == 1:
            await message.reply_text(f"Reply with the price for {unresolved[0]['item_name']}, like `2 pesos`.")
        else:
            names = ", ".join(item["item_name"] for item in unresolved)
            await message.reply_text(f"I still need prices for: {names}. Reply one at a time like `odong 2`.")
        return True

    for item in line_items:
        if item["item_name"] == target_item["item_name"]:
            quantity = float(item.get("quantity") or 1)
            item["unit_price"] = amount
            item["line_total"] = round(quantity * amount, 2)
            break

    parsed = {
        "intent": pending["intent"],
        "line_items": line_items,
        "total_amount": sum(float(item.get("line_total") or 0) for item in line_items),
        "customer_name": pending.get("customer_name"),
        "currency": pending.get("currency") or "PHP",
    }

    unresolved_after = [
        item for item in line_items
        if item.get("unit_price") in (None, "", 0) and item.get("line_total") in (None, "", 0)
    ]
    if unresolved_after:
        context.user_data["pending_action"] = {
            **pending,
            "line_items": line_items,
            "missing_items": [item["item_name"] for item in unresolved_after],
        }
    else:
        context.user_data.pop("pending_action", None)

    success = await process_sale_like_intent(
        update=update,
        context=context,
        parsed=parsed,
        raw_message=f"{pending.get('raw_message', '')} | follow-up: {text}",
        is_utang=pending["intent"] == "utang_record",
    )
    if success:
        context.user_data.pop("pending_action", None)
    return True


def detect_period(text: str) -> str | None:
    lowered = text.lower()
    if "today" in lowered or "daily" in lowered:
        return "today"
    if "week" in lowered or "weekly" in lowered:
        return "week"
    if "month" in lowered or "monthly" in lowered:
        return "month"
    return None


def extract_first_amount(text: str) -> float | None:
    match = re.search(r"(\d+(?:\.\d+)?)", text.replace(",", ""))
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def choose_pending_item(unresolved: list[dict[str, Any]], text: str) -> dict[str, Any] | None:
    lowered = text.lower()
    for item in unresolved:
        if item["item_name"].lower() in lowered:
            return item
    if len(unresolved) == 1:
        return unresolved[0]
    return None


async def track_user_context(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    event_type: str,
    message_text: str | None = None,
) -> None:
    db: Database = context.application.bot_data["db"]
    user = update.effective_user
    if not user:
        return
    db.upsert_user(
        telegram_user_id=user.id,
        telegram_username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
    )
    db.log_event(
        telegram_user_id=user.id,
        event_type=event_type,
        message_text=message_text,
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


def format_utang_sale_message(sale: dict[str, object]) -> str:
    lines = [
        f"Saved utang for {sale['customer_name']}: {format_money(sale['total_amount'], sale['currency'])}",
        "Items:",
    ]
    for item in sale["line_items"]:
        lines.append(
            f"- {item['item_name']}: {int(item['quantity'])} x {format_money(item['unit_price'], sale['currency'])} = {format_money(item['line_total'], sale['currency'])}"
        )
    lines.append(f"Remaining balance: {format_money(sale['remaining_balance'], sale['currency'])}")
    return "\n".join(lines)


def format_utang_payment_message(payment: dict[str, object]) -> str:
    return (
        f"Recorded payment from {payment['customer_name']}: {format_money(payment['amount'], payment['currency'])}\n"
        f"Remaining balance: {format_money(payment['remaining_balance'], payment['currency'])}"
    )


def format_customer_balance(balance: dict[str, object]) -> str:
    return (
        f"{balance['customer_name']}'s balance\n"
        f"Total utang: {format_money(balance['total_sales'], balance['currency'])}\n"
        f"Total paid: {format_money(balance['total_payments'], balance['currency'])}\n"
        f"Remaining: {format_money(balance['balance'], balance['currency'])}"
    )


def format_all_balances(balances: list[dict[str, object]]) -> str:
    if not balances:
        return "No unpaid utang balances right now."
    lines = ["Unpaid utang balances:"]
    for balance in balances[:10]:
        lines.append(f"- {balance['customer_name']}: {format_money(balance['balance'], balance['currency'])}")
    return "\n".join(lines)


def format_money(amount: float | int | str, currency: str) -> str:
    symbol = "₱" if (currency or "").upper() == "PHP" else f"{currency.upper()} "
    return f"{symbol}{float(amount):,.2f}"


def format_stats_message(stats: dict[str, object]) -> str:
    lines = [
        "Bot usage stats",
        f"Total users: {stats['total_users']}",
        f"Active this week: {stats['active_users_this_week']}",
        f"Active this month: {stats['active_users_this_month']}",
        f"Sales recorded: {stats['sales_count']} ({format_money(stats['sales_revenue'], 'PHP')})",
        f"Utang sales: {stats['utang_sales_count']} ({format_money(stats['utang_revenue'], 'PHP')})",
        f"Customers with unpaid balance: {stats['customers_with_balance']}",
        f"Unpaid utang total: {format_money(stats['unpaid_utang_total'], 'PHP')}",
        f"Tracked events: {stats['total_events']}",
        "",
        "Event breakdown:",
    ]

    if stats["event_counts"]:
        for event_type, count in sorted(stats["event_counts"].items(), key=lambda item: item[1], reverse=True):
            lines.append(f"- {event_type}: {count}")
    else:
        lines.append("- No events yet")

    if stats["recent_users"]:
        lines.extend(["", "Recent users:"])
        for user in stats["recent_users"]:
            label = user.get("telegram_username") or user.get("first_name") or str(user["telegram_user_id"])
            lines.append(f"- {label} ({user['telegram_user_id']})")

    return "\n".join(lines)
