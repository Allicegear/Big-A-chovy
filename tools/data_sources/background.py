"""Optional market-background composition for reports and the workbench."""

from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from typing import Any

from .calendar import TradingCalendarService
from .sentiment import EastmoneySentimentSource


BEIJING = timezone(timedelta(hours=8))


def build_market_background(
    data_date: str | None = None,
    *,
    calendar_service: TradingCalendarService | None = None,
    sentiment_source: EastmoneySentimentSource | None = None,
    include_sentiment: bool = True,
) -> dict[str, Any]:
    """Return independently degradable calendar and sentiment evidence."""
    if data_date is None:
        data_date = datetime.now(BEIJING).date().isoformat()
    background: dict[str, Any] = {
        "data_date": data_date,
        "note": "市场情绪与事件仅作背景证据，不改变现有评分、门禁或真实仓权限",
    }
    calendar_result = (calendar_service or TradingCalendarService()).is_open(data_date)
    background["calendar"] = calendar_result.to_dict()
    if include_sentiment:
        sentiment_result = (sentiment_source or EastmoneySentimentSource()).fetch(data_date)
        background["sentiment"] = sentiment_result.to_dict()
    return background

