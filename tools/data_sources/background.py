"""Optional market-background composition for reports and the workbench."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from datetime import datetime, timezone, timedelta
import time
from typing import Any
import urllib.request

from .calendar import TradingCalendarService
from .http import HTTPClient
from .sentiment import EastmoneySentimentSource


BEIJING = timezone(timedelta(hours=8))
BACKGROUND_DEFAULT_BUDGET_SECONDS = 8.0


def project_http_client() -> HTTPClient:
    """Build the shared data-source client with measured path + TLS policy."""
    try:
        import network_path
        import tls_context

        proxy = network_path.best_proxy_url()
        handlers = [urllib.request.HTTPSHandler(context=tls_context.build_context())]
        handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy} if proxy else {}))
        return HTTPClient(opener=urllib.request.build_opener(*handlers))
    except Exception:
        # CLI use outside the dashboard still gets verified default TLS; the
        # dashboard path above reuses its measured direct/proxy choice.
        return HTTPClient()


def build_market_background(
    data_date: str | None = None,
    *,
    calendar_service: TradingCalendarService | None = None,
    sentiment_source: EastmoneySentimentSource | None = None,
    include_sentiment: bool = True,
    budget_seconds: float = BACKGROUND_DEFAULT_BUDGET_SECONDS,
) -> dict[str, Any]:
    """Return independently degradable background evidence within a hard budget."""
    if data_date is None:
        data_date = datetime.now(BEIJING).date().isoformat()
    background: dict[str, Any] = {
        "data_date": data_date,
        "note": "市场情绪与事件仅作背景证据，不改变现有评分、门禁或真实仓权限",
    }
    shared_client = None
    if calendar_service is None or (include_sentiment and sentiment_source is None):
        shared_client = project_http_client()
    calendar_service = calendar_service or TradingCalendarService(client=shared_client, request_timeout=max(0.5, float(budget_seconds) / 3))
    sentiment_source = sentiment_source or EastmoneySentimentSource(client=shared_client, request_timeout=max(0.5, float(budget_seconds) / 3))

    tasks: dict[str, Any] = {}
    pool = ThreadPoolExecutor(max_workers=2)
    try:
        tasks["calendar"] = pool.submit(calendar_service.is_open, data_date)
        if include_sentiment:
            tasks["sentiment"] = pool.submit(sentiment_source.fetch, data_date)
        deadline = time.monotonic() + max(0.5, float(budget_seconds))
        for name, future in tasks.items():
            remaining = max(0.0, deadline - time.monotonic())
            try:
                result = future.result(timeout=remaining)
                background[name] = result.to_dict()
            except FutureTimeout:
                background[name] = {"status": "unavailable", "error": {"code": "background_budget_exceeded", "message": "市场背景查询超过总预算"}, "warnings": ["背景查询已限时，不影响核心筛选结果"]}
            except Exception as exc:
                background[name] = {"status": "unavailable", "error": {"code": "background_source_error", "message": f"{type(exc).__name__}: {exc}"}, "warnings": ["背景源失败，不影响核心筛选结果"]}
    finally:
        # Do not wait for a stalled provider after the budget has expired.  The
        # worker is bounded by the HTTP client's timeout and is independent of
        # the screening lock.
        pool.shutdown(wait=False, cancel_futures=True)
    return background
