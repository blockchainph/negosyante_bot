from __future__ import annotations

import json
from json import JSONDecodeError
from textwrap import dedent
from typing import Any

from anthropic import AsyncAnthropic

from app.config import Settings


class ClaudeHandler:
    def __init__(self, settings: Settings) -> None:
        if not settings.anthropic_api_key:
            raise ValueError("ANTHROPIC_API_KEY is required.")
        self.client = AsyncAnthropic(api_key=settings.anthropic_api_key)
        self.model = settings.anthropic_model

    async def parse_message(self, message_text: str) -> dict[str, Any]:
        system_prompt = dedent(
            """
            You are a sales parsing engine for a sari-sari store Telegram bot.
            Return JSON only. No markdown. No prose. No code fences.

            Supported intents:
            - sale_record: user is logging one sale with one or more line items
            - utang_record: user is logging a credit sale for a named customer
            - payment_record: user is logging a payment toward utang
            - balance_query: user wants a customer's remaining utang or a list of unpaid balances
            - revenue_summary: user wants daily, weekly, or monthly sales summary
            - top_items: user wants top-selling items
            - price_set: user wants to set or update the price of an item
            - price_show: user wants to view the current price list
            - stock_update: user wants to set stock quantity or reorder level
            - unknown: not enough information or not sales-related

            Rules:
            - Default currency is PHP unless another clear currency is stated.
            - For sale_record, line_items may contain multiple items.
            - Each line item should include:
              item_name, quantity, unit_price, line_total.
            - If quantity is missing, default to 1.
            - If unit_price is unknown but line_total is clear, keep unit_price null.
            - If line_total is unknown but quantity and unit_price are known, line_total can be inferred by the app.
            - For utang_record, include customer_name and line_items.
            - For payment_record, include customer_name and total_amount.
            - For balance_query, include customer_name when the user asks about one person only.
            - For summary requests, set period to one of: today, week, month.
            - If period is unclear, default to today for revenue_summary and month for top_items.
            - For price_set, use item_name plus unit_price.
            - For stock_update, use item_name and stock_quantity and/or reorder_level.
            - Keep clarification_message short and helpful.

            Return this exact JSON shape:
            {
              "intent": "sale_record|utang_record|payment_record|balance_query|revenue_summary|top_items|price_set|price_show|stock_update|unknown",
              "line_items": [
                {
                  "item_name": "string",
                  "quantity": 1,
                  "unit_price": 0,
                  "line_total": 0
                }
              ],
              "total_amount": 0,
              "customer_name": "string or null",
              "item_name": "string or null",
              "unit_price": 0,
              "stock_quantity": 0,
              "reorder_level": 0,
              "currency": "PHP",
              "period": "today|week|month|null",
              "needs_clarification": false,
              "clarification_message": "string or null"
            }
            """
        ).strip()

        response = await self.client.messages.create(
            model=self.model,
            max_tokens=600,
            system=system_prompt,
            messages=[{"role": "user", "content": message_text}],
        )
        content = "".join(
            block.text for block in response.content if getattr(block, "type", None) == "text"
        ).strip()
        parsed = self._load_json(content)
        return self._normalize_result(parsed)

    def _normalize_result(self, result: dict[str, Any]) -> dict[str, Any]:
        intent = (result.get("intent") or "unknown").lower()
        period = (result.get("period") or "").lower() or None
        if period not in {"today", "week", "month", None}:
            period = "today"

        line_items: list[dict[str, Any]] = []
        for item in result.get("line_items") or []:
            if not isinstance(item, dict):
                continue
            line_items.append(
                {
                    "item_name": self._clean_string(item.get("item_name")),
                    "quantity": self._safe_number(item.get("quantity"), default=1),
                    "unit_price": self._safe_number(item.get("unit_price")),
                    "line_total": self._safe_number(item.get("line_total")),
                }
            )

        return {
            "intent": intent
            if intent in {"sale_record", "utang_record", "payment_record", "balance_query", "revenue_summary", "top_items", "price_set", "price_show", "stock_update", "unknown"}
            else "unknown",
            "line_items": line_items,
            "total_amount": self._safe_number(result.get("total_amount")),
            "customer_name": self._clean_string(result.get("customer_name")),
            "item_name": self._clean_string(result.get("item_name")),
            "unit_price": self._safe_number(result.get("unit_price")),
            "stock_quantity": self._safe_number(result.get("stock_quantity")),
            "reorder_level": self._safe_number(result.get("reorder_level")),
            "currency": (result.get("currency") or "PHP").upper(),
            "period": period,
            "needs_clarification": bool(result.get("needs_clarification")),
            "clarification_message": self._clean_string(result.get("clarification_message")),
        }

    @staticmethod
    def _load_json(content: str) -> dict[str, Any]:
        try:
            return json.loads(content)
        except JSONDecodeError:
            start = content.find("{")
            end = content.rfind("}")
            if start == -1 or end == -1 or end <= start:
                raise
            return json.loads(content[start : end + 1])

    @staticmethod
    def _clean_string(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        cleaned = value.strip()
        return cleaned or None

    @staticmethod
    def _safe_number(value: Any, default: float | None = None) -> float | None:
        try:
            if value is None or value == "":
                return default
            return float(value)
        except (TypeError, ValueError):
            return default
