from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

from supabase import Client, create_client

from app.config import Settings


class Database:
    def __init__(self, settings: Settings) -> None:
        if not settings.supabase_url or not settings.supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY are required.")
        self.client: Client = create_client(settings.supabase_url, settings.supabase_key)
        self.local_timezone = ZoneInfo(settings.timezone)
        self.sales_table = "sales_transactions"
        self.line_items_table = "sales_line_items"
        self.price_list_table = "price_list_items"
        self.customers_table = "customers"
        self.utang_sales_table = "utang_sales"
        self.utang_line_items_table = "utang_line_items"
        self.utang_payments_table = "utang_payments"
        self.users_table = "users"
        self.events_table = "events"

    def upsert_user(
        self,
        telegram_user_id: int,
        telegram_username: str | None,
        first_name: str | None,
        last_name: str | None,
    ) -> None:
        payload = {
            "telegram_user_id": telegram_user_id,
            "telegram_username": telegram_username,
            "first_name": first_name,
            "last_name": last_name,
            "last_seen_at": datetime.now(timezone.utc).isoformat(),
        }
        self.client.table(self.users_table).upsert(payload, on_conflict="telegram_user_id").execute()

    def log_event(
        self,
        telegram_user_id: int,
        event_type: str,
        message_text: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        payload = {
            "telegram_user_id": telegram_user_id,
            "event_type": event_type,
            "message_text": message_text,
            "metadata": metadata or {},
        }
        self.client.table(self.events_table).insert(payload).execute()

    def save_sale(
        self,
        telegram_user_id: int,
        telegram_username: str | None,
        line_items: list[dict[str, Any]],
        total_amount: float,
        currency: str,
        raw_message: str | None = None,
    ) -> dict[str, Any]:
        transaction_payload = {
            "telegram_user_id": telegram_user_id,
            "telegram_username": telegram_username,
            "total_amount": round(float(total_amount), 2),
            "currency": currency or "PHP",
            "raw_message": raw_message,
        }
        transaction = self.client.table(self.sales_table).insert(transaction_payload).execute().data[0]

        prepared_items: list[dict[str, Any]] = []
        for item in line_items:
            prepared_items.append(
                {
                    "sales_transaction_id": transaction["id"],
                    "item_name": item["item_name"],
                    "quantity": int(item["quantity"]),
                    "unit_price": round(float(item["unit_price"]), 2),
                    "line_total": round(float(item["line_total"]), 2),
                }
            )
        self.client.table(self.line_items_table).insert(prepared_items).execute()

        stock_warnings = self._decrement_stock_and_collect_warnings(telegram_user_id, line_items)
        transaction["line_items"] = prepared_items
        transaction["stock_warnings"] = stock_warnings
        return transaction

    def get_or_create_customer(self, telegram_user_id: int, customer_name: str) -> dict[str, Any]:
        normalized_name = customer_name.strip().lower()
        existing = (
            self.client.table(self.customers_table)
            .select("*")
            .eq("telegram_user_id", telegram_user_id)
            .eq("customer_name", normalized_name)
            .limit(1)
            .execute()
        )
        if existing.data:
            return existing.data[0]
        payload = {
            "telegram_user_id": telegram_user_id,
            "customer_name": normalized_name,
        }
        return self.client.table(self.customers_table).insert(payload).execute().data[0]

    def save_utang_sale(
        self,
        telegram_user_id: int,
        telegram_username: str | None,
        customer_name: str,
        line_items: list[dict[str, Any]],
        total_amount: float,
        currency: str,
        raw_message: str | None = None,
    ) -> dict[str, Any]:
        customer = self.get_or_create_customer(telegram_user_id, customer_name)
        sale_payload = {
            "telegram_user_id": telegram_user_id,
            "telegram_username": telegram_username,
            "customer_id": customer["id"],
            "customer_name": customer["customer_name"],
            "total_amount": round(float(total_amount), 2),
            "currency": currency or "PHP",
            "raw_message": raw_message,
        }
        sale = self.client.table(self.utang_sales_table).insert(sale_payload).execute().data[0]
        prepared_items: list[dict[str, Any]] = []
        for item in line_items:
            prepared_items.append(
                {
                    "utang_sale_id": sale["id"],
                    "item_name": item["item_name"],
                    "quantity": int(item["quantity"]),
                    "unit_price": round(float(item["unit_price"]), 2),
                    "line_total": round(float(item["line_total"]), 2),
                }
            )
        self.client.table(self.utang_line_items_table).insert(prepared_items).execute()
        stock_warnings = self._decrement_stock_and_collect_warnings(telegram_user_id, line_items)
        sale["line_items"] = prepared_items
        sale["customer"] = customer
        sale["stock_warnings"] = stock_warnings
        sale["remaining_balance"] = self.get_customer_balance(telegram_user_id, customer["customer_name"])["balance"]
        return sale

    def record_utang_payment(
        self,
        telegram_user_id: int,
        customer_name: str,
        amount: float,
        currency: str = "PHP",
    ) -> dict[str, Any]:
        customer = self.get_or_create_customer(telegram_user_id, customer_name)
        payload = {
            "telegram_user_id": telegram_user_id,
            "customer_id": customer["id"],
            "customer_name": customer["customer_name"],
            "amount": round(float(amount), 2),
            "currency": currency or "PHP",
        }
        payment = self.client.table(self.utang_payments_table).insert(payload).execute().data[0]
        payment["remaining_balance"] = self.get_customer_balance(telegram_user_id, customer["customer_name"])["balance"]
        return payment

    def get_customer_balance(
        self,
        telegram_user_id: int,
        customer_name: str,
    ) -> dict[str, Any]:
        normalized_name = customer_name.strip().lower()
        sales = (
            self.client.table(self.utang_sales_table)
            .select("total_amount,currency")
            .eq("telegram_user_id", telegram_user_id)
            .eq("customer_name", normalized_name)
            .execute()
        ).data or []
        payments = (
            self.client.table(self.utang_payments_table)
            .select("amount,currency")
            .eq("telegram_user_id", telegram_user_id)
            .eq("customer_name", normalized_name)
            .execute()
        ).data or []
        total_sales = sum(self._safe_amount(row.get("total_amount")) for row in sales)
        total_payments = sum(self._safe_amount(row.get("amount")) for row in payments)
        currency = sales[0]["currency"] if sales else payments[0]["currency"] if payments else "PHP"
        return {
            "customer_name": normalized_name,
            "balance": round(total_sales - total_payments, 2),
            "total_sales": round(total_sales, 2),
            "total_payments": round(total_payments, 2),
            "currency": currency,
        }

    def get_all_balances(self, telegram_user_id: int) -> list[dict[str, Any]]:
        customers = (
            self.client.table(self.customers_table)
            .select("customer_name")
            .eq("telegram_user_id", telegram_user_id)
            .order("customer_name", desc=False)
            .execute()
        ).data or []
        balances: list[dict[str, Any]] = []
        for customer in customers:
            balance = self.get_customer_balance(telegram_user_id, customer["customer_name"])
            if balance["balance"] > 0:
                balances.append(balance)
        return sorted(balances, key=lambda row: row["balance"], reverse=True)

    def upsert_price_list_item(
        self,
        telegram_user_id: int,
        item_name: str,
        unit_price: float | None = None,
        stock_quantity: float | None = None,
        reorder_level: float | None = None,
        currency: str = "PHP",
    ) -> dict[str, Any]:
        existing = self.get_price_list_item(telegram_user_id, item_name)
        payload = {
            "telegram_user_id": telegram_user_id,
            "item_name": item_name.strip().lower(),
            "unit_price": round(float(unit_price), 2) if unit_price is not None else existing.get("unit_price") if existing else None,
            "stock_quantity": int(stock_quantity) if stock_quantity is not None else existing.get("stock_quantity") if existing else None,
            "reorder_level": int(reorder_level) if reorder_level is not None else existing.get("reorder_level") if existing else None,
            "currency": currency or (existing.get("currency") if existing else "PHP"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        response = self.client.table(self.price_list_table).upsert(
            payload,
            on_conflict="telegram_user_id,item_name",
        ).execute()
        return response.data[0]

    def get_price_list_item(self, telegram_user_id: int, item_name: str) -> dict[str, Any] | None:
        response = (
            self.client.table(self.price_list_table)
            .select("*")
            .eq("telegram_user_id", telegram_user_id)
            .eq("item_name", item_name.strip().lower())
            .limit(1)
            .execute()
        )
        return response.data[0] if response.data else None

    def get_price_list(self, telegram_user_id: int) -> list[dict[str, Any]]:
        response = (
            self.client.table(self.price_list_table)
            .select("*")
            .eq("telegram_user_id", telegram_user_id)
            .order("item_name", desc=False)
            .execute()
        )
        return response.data or []

    def get_all_user_ids(self) -> list[int]:
        users_response = self.client.table(self.users_table).select("telegram_user_id").execute()
        transactions_response = self.client.table(self.sales_table).select("telegram_user_id").execute()
        prices_response = self.client.table(self.price_list_table).select("telegram_user_id").execute()
        user_ids = {row["telegram_user_id"] for row in transactions_response.data or []}
        user_ids.update({row["telegram_user_id"] for row in prices_response.data or []})
        user_ids.update({row["telegram_user_id"] for row in users_response.data or []})
        return sorted(user_ids)

    def get_usage_stats(self, now: datetime | None = None) -> dict[str, Any]:
        current = now or datetime.now(timezone.utc)
        week_start, _, _ = self._resolve_period("week", current)
        month_start, _, _ = self._resolve_period("month", current)

        users_response = self.client.table(self.users_table).select("*", count="exact").execute()
        events_response = self.client.table(self.events_table).select("*", count="exact").execute()
        sales_response = self.client.table(self.sales_table).select("total_amount", count="exact").execute()
        utang_sales_response = self.client.table(self.utang_sales_table).select("total_amount", count="exact").execute()
        active_week_response = (
            self.client.table(self.users_table)
            .select("telegram_user_id", count="exact")
            .gte("last_seen_at", week_start.isoformat())
            .execute()
        )
        active_month_response = (
            self.client.table(self.users_table)
            .select("telegram_user_id", count="exact")
            .gte("last_seen_at", month_start.isoformat())
            .execute()
        )
        event_breakdown_response = (
            self.client.table(self.events_table)
            .select("event_type")
            .order("created_at", desc=True)
            .limit(5000)
            .execute()
        )

        event_counts: dict[str, int] = {}
        for row in event_breakdown_response.data or []:
            event_type = row.get("event_type", "unknown")
            event_counts[event_type] = event_counts.get(event_type, 0) + 1

        total_sales_amount = sum(self._safe_amount(row.get("total_amount")) for row in sales_response.data or [])
        total_utang_amount = sum(self._safe_amount(row.get("total_amount")) for row in utang_sales_response.data or [])
        unpaid_balances = self._calculate_total_unpaid_balance()

        recent_users_response = (
            self.client.table(self.users_table)
            .select("telegram_user_id, telegram_username, first_name, last_seen_at")
            .order("last_seen_at", desc=True)
            .limit(5)
            .execute()
        )

        return {
            "total_users": users_response.count or 0,
            "total_events": events_response.count or 0,
            "active_users_this_week": active_week_response.count or 0,
            "active_users_this_month": active_month_response.count or 0,
            "event_counts": event_counts,
            "recent_users": recent_users_response.data or [],
            "sales_count": sales_response.count or 0,
            "sales_revenue": round(total_sales_amount, 2),
            "utang_sales_count": utang_sales_response.count or 0,
            "utang_revenue": round(total_utang_amount, 2),
            "unpaid_utang_total": round(unpaid_balances["total_balance"], 2),
            "customers_with_balance": unpaid_balances["customer_count"],
        }

    def prepare_sale_items(
        self,
        telegram_user_id: int,
        line_items: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[str]]:
        prepared: list[dict[str, Any]] = []
        missing_items: list[str] = []
        for raw_item in line_items:
            item_name = (raw_item.get("item_name") or "").strip()
            if not item_name:
                continue
            quantity = int(raw_item.get("quantity") or 1)
            unit_price = raw_item.get("unit_price")
            line_total = raw_item.get("line_total")

            price_record = self.get_price_list_item(telegram_user_id, item_name)
            if unit_price is None and price_record and price_record.get("unit_price") is not None:
                unit_price = float(price_record["unit_price"])
            if line_total is None and unit_price is not None:
                line_total = round(quantity * float(unit_price), 2)
            if unit_price is None and line_total is not None and quantity > 0:
                unit_price = round(float(line_total) / quantity, 2)

            if unit_price is None or line_total is None:
                missing_items.append(item_name)
                continue

            prepared.append(
                {
                    "item_name": item_name.strip().lower(),
                    "quantity": quantity,
                    "unit_price": float(unit_price),
                    "line_total": float(line_total),
                }
            )
        return prepared, missing_items

    def get_revenue_summary(
        self,
        telegram_user_id: int,
        period: str,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        start_date, end_date, label = self._resolve_period(period, now or datetime.now(timezone.utc))
        transactions = self._get_sales_between(telegram_user_id, start_date, end_date)
        total = sum(self._safe_amount(row.get("total_amount")) for row in transactions)
        return {
            "label": label,
            "period": period,
            "count": len(transactions),
            "total": round(total, 2),
            "currency": transactions[0]["currency"] if transactions else "PHP",
        }

    def get_top_selling_items(
        self,
        telegram_user_id: int,
        period: str,
        now: datetime | None = None,
        limit: int = 5,
    ) -> dict[str, Any]:
        start_date, end_date, label = self._resolve_period(period, now or datetime.now(timezone.utc))
        transactions = self._get_sales_between(telegram_user_id, start_date, end_date)
        transaction_ids = [row["id"] for row in transactions]
        if not transaction_ids:
            return {"label": label, "period": period, "items": [], "currency": "PHP"}

        items_response = (
            self.client.table(self.line_items_table)
            .select("*")
            .in_("sales_transaction_id", transaction_ids)
            .execute()
        )
        totals: dict[str, dict[str, float]] = {}
        for item in items_response.data or []:
            name = item["item_name"]
            entry = totals.setdefault(name, {"quantity": 0.0, "revenue": 0.0})
            entry["quantity"] += self._safe_amount(item.get("quantity"))
            entry["revenue"] += self._safe_amount(item.get("line_total"))

        sorted_items = sorted(totals.items(), key=lambda pair: pair[1]["revenue"], reverse=True)[:limit]
        return {
            "label": label,
            "period": period,
            "currency": transactions[0]["currency"] if transactions else "PHP",
            "items": [
                {
                    "item_name": name,
                    "quantity": int(values["quantity"]),
                    "revenue": round(values["revenue"], 2),
                }
                for name, values in sorted_items
            ],
        }

    def _decrement_stock_and_collect_warnings(
        self,
        telegram_user_id: int,
        line_items: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        warnings: list[dict[str, Any]] = []
        for item in line_items:
            price_record = self.get_price_list_item(telegram_user_id, item["item_name"])
            if not price_record or price_record.get("stock_quantity") is None:
                continue
            new_stock = int(price_record["stock_quantity"]) - int(item["quantity"])
            updated = self.upsert_price_list_item(
                telegram_user_id=telegram_user_id,
                item_name=item["item_name"],
                unit_price=float(price_record["unit_price"]) if price_record.get("unit_price") is not None else None,
                stock_quantity=max(new_stock, 0),
                reorder_level=price_record.get("reorder_level"),
                currency=price_record.get("currency") or "PHP",
            )
            reorder_level = updated.get("reorder_level")
            if reorder_level is not None and updated.get("stock_quantity") is not None and int(updated["stock_quantity"]) <= int(reorder_level):
                warnings.append(
                    {
                        "item_name": updated["item_name"],
                        "stock_quantity": int(updated["stock_quantity"]),
                        "reorder_level": int(reorder_level),
                    }
                )
        return warnings

    def _get_sales_between(
        self,
        telegram_user_id: int,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict[str, Any]]:
        response = (
            self.client.table(self.sales_table)
            .select("*")
            .eq("telegram_user_id", telegram_user_id)
            .gte("created_at", start_date.isoformat())
            .lte("created_at", end_date.isoformat())
            .order("created_at", desc=False)
            .execute()
        )
        return response.data or []

    def _calculate_total_unpaid_balance(self) -> dict[str, Any]:
        customers = self.client.table(self.customers_table).select("telegram_user_id, customer_name").execute().data or []
        total_balance = 0.0
        customer_count = 0
        for customer in customers:
            balance = self.get_customer_balance(customer["telegram_user_id"], customer["customer_name"])
            if balance["balance"] > 0:
                total_balance += balance["balance"]
                customer_count += 1
        return {"total_balance": total_balance, "customer_count": customer_count}

    def _resolve_period(self, period: str, now: datetime) -> tuple[datetime, datetime, str]:
        period_name = (period or "today").lower()
        base_local = now.astimezone(self.local_timezone)

        if period_name == "today":
            start_local = datetime(base_local.year, base_local.month, base_local.day, tzinfo=self.local_timezone)
            end_local = start_local + timedelta(days=1) - timedelta(seconds=1)
            return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc), "today"

        if period_name == "week":
            day_start_local = datetime(base_local.year, base_local.month, base_local.day, tzinfo=self.local_timezone)
            start_local = day_start_local - timedelta(days=base_local.weekday())
            end_local = start_local + timedelta(days=7) - timedelta(seconds=1)
            return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc), "this week"

        if period_name == "month":
            start_local = datetime(base_local.year, base_local.month, 1, tzinfo=self.local_timezone)
            if base_local.month == 12:
                next_month = datetime(base_local.year + 1, 1, 1, tzinfo=self.local_timezone)
            else:
                next_month = datetime(base_local.year, base_local.month + 1, 1, tzinfo=self.local_timezone)
            end_local = next_month - timedelta(seconds=1)
            return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc), "this month"

        raise ValueError(f"Unsupported period: {period}")

    @staticmethod
    def _safe_amount(value: Any) -> float:
        try:
            return float(Decimal(str(value)))
        except (InvalidOperation, TypeError, ValueError):
            return 0.0
