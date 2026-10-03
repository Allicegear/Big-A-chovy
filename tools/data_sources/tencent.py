"""Tencent quote snapshot and approximately three-second tick adapter."""

from __future__ import annotations

from datetime import datetime, timezone
import re
import time
from typing import Any, Iterable

from .cache import JsonCache
from .contracts import Result, ResultStatus, result_empty, result_error, result_ok
from .http import HTTPClient, HTTPClientError
from .symbols import SecuritySymbol, SymbolError, normalize_security


TICK_URL = "https://stock.gtimg.cn/data/index.php"
QUOTE_URL = "https://qt.gtimg.cn/q="
MAX_PAGES = 300
SESSION_END = "15:00:59"
TICK_SOURCE = "tencent_ticks"


def _number(value: Any, field: str) -> float:
    if value in (None, "", "-", "--"):
        raise ValueError(f"腾讯分笔字段 {field} 缺失")
    try:
        number = float(str(value).replace(",", ""))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"腾讯分笔字段 {field} 不是数字: {value!r}") from exc
    if number != number or number in (float("inf"), float("-inf")):
        raise ValueError(f"腾讯分笔字段 {field} 非有限数值")
    return number


def _time_seconds(value: str) -> int:
    if re.fullmatch(r"\d{6}", value):
        value = f"{value[:2]}:{value[2:4]}:{value[4:]}"
    if not re.fullmatch(r"\d{2}:\d{2}:\d{2}", value):
        raise ValueError(f"分笔时间格式异常: {value!r}")
    h, m, s = (int(x) for x in value.split(":"))
    if h > 23 or m > 59 or s > 59:
        raise ValueError(f"分笔时间无效: {value!r}")
    return h * 3600 + m * 60 + s


def parse_snapshot(text: str, symbol: str) -> dict[str, Any]:
    """Parse ``qt.gtimg.cn`` snapshot without silently accepting another code."""
    match = re.search(rf'v_{re.escape(symbol)}="([^"]*)"', text)
    if not match:
        if "v_pv_none_match" in text:
            raise ValueError(f"腾讯没有 {symbol} 这个代码")
        raise RuntimeError(f"腾讯行情快照 {symbol} 未返回预期变量")
    fields = match.group(1).split("~")
    if len(fields) < 36 or not re.fullmatch(r"\d{14}", fields[30] or ""):
        raise RuntimeError(f"腾讯行情快照 {symbol} 字段数/时间字段异常: {len(fields)}")
    parts = (fields[35] or "").split("/")
    if len(parts) != 3:
        raise RuntimeError(f"腾讯行情快照 {symbol} 的价量额字段异常: {fields[35]!r}")
    return {
        "symbol": symbol,
        "code": symbol[2:],
        "name": fields[1],
        "data_date": f"{fields[30][:4]}-{fields[30][4:6]}-{fields[30][6:8]}",
        "as_of": fields[30][8:],
        "price": _number(fields[3], "price"),
        "amount": _number(parts[2], "amount"),
    }


def parse_tick_page(text: str, symbol: str, page: int) -> list[dict[str, Any]] | None:
    """Parse one 70-row page; ``None`` means the provider's end marker."""
    text = text.strip()
    if not text:
        return None
    match = re.fullmatch(rf'v_detail_data_{re.escape(symbol)}=\[(\d+),"([^"]*)"\];?', text)
    if not match or int(match.group(1)) != page:
        raise RuntimeError(f"腾讯逐笔 {symbol} 第 {page} 页返回结构异常: {text[:100]!r}")
    payload = match.group(2)
    if not payload:
        return None
    rows: list[dict[str, Any]] = []
    for raw in payload.split("|"):
        fields = raw.split("/")
        if len(fields) != 7:
            raise RuntimeError(f"腾讯逐笔字段数改变: {raw!r}")
        seq_raw, clock, price, change, volume, amount, side = fields
        _time_seconds(clock)
        if side not in {"B", "S", "M"}:
            raise RuntimeError(f"腾讯逐笔方向未知: {side!r}")
        try:
            seq = int(seq_raw)
        except ValueError as exc:
            raise RuntimeError(f"腾讯逐笔序号不是整数: {seq_raw!r}") from exc
        rows.append({
            "seq": seq,
            "time": clock,
            "price": _number(price, "price"),
            "change": _number(change, "change"),
            "volume": _number(volume, "volume_hand"),
            "amount": _number(amount, "amount_yuan"),
            "side": side,
        })
    return rows


def aggregate_ticks(rows: Iterable[dict[str, Any]], *, window_minutes: int = 5, as_of: str | None = None) -> dict[str, Any]:
    """Aggregate B/S/M ticks in continuous auction time only.

    The returned ratio is ``B amount / S amount``.  A zero sell amount is
    represented by ``None`` rather than infinity so it cannot accidentally
    satisfy a trading gate.
    """
    prepared = []
    for row in rows:
        clock = str(row.get("time") or "")
        seconds = _time_seconds(clock)
        if 9 * 3600 + 30 * 60 <= seconds <= 15 * 3600 + 59:
            prepared.append((seconds, row))
    prepared.sort(key=lambda item: item[0])
    if not prepared:
        return {
            "window_minutes": window_minutes,
            "as_of": as_of,
            "row_count": 0,
            "coverage_minutes": 0.0,
            "buy_amount": 0.0,
            "sell_amount": 0.0,
            "neutral_amount": 0.0,
            "net_amount": 0.0,
            "buy_sell_ratio": None,
            "data_sufficient": False,
            "reason": "连续竞价窗口没有有效分笔",
        }
    latest = max(_time_seconds(as_of), prepared[-1][0]) if as_of else prepared[-1][0]
    cutoff = latest - max(1, int(window_minutes)) * 60
    selected = [row for seconds, row in prepared if cutoff <= seconds <= latest]
    buy = sum(float(row.get("amount") or 0) for row in selected if row.get("side") == "B")
    sell = sum(float(row.get("amount") or 0) for row in selected if row.get("side") == "S")
    neutral = sum(float(row.get("amount") or 0) for row in selected if row.get("side") == "M")
    selected_times = [_time_seconds(str(row["time"])) for row in selected]
    coverage = ((max(selected_times) - min(selected_times)) / 60.0) if len(selected_times) > 1 else 0.0
    return {
        "window_minutes": int(window_minutes),
        "as_of": as_of or selected[-1].get("time"),
        "row_count": len(selected),
        "coverage_minutes": round(coverage, 2),
        "buy_amount": round(buy, 2),
        "sell_amount": round(sell, 2),
        "neutral_amount": round(neutral, 2),
        "net_amount": round(buy - sell, 2),
        "buy_sell_ratio": round(buy / sell, 4) if sell > 0 else None,
        "data_sufficient": coverage >= min(float(window_minutes), 1.0),
        "reason": "" if coverage >= min(float(window_minutes), 1.0) else "窗口覆盖不足",
    }


class TencentTickSource:
    """Fetch and incrementally cache Tencent tick pages."""

    def __init__(self, client: HTTPClient | None = None, *, cache: JsonCache | None = None, sleep_seconds: float = 0.1):
        self.client = client or HTTPClient()
        self.cache = cache or JsonCache("tencent_ticks")
        self.sleep_seconds = max(0.0, float(sleep_seconds))

    def _snapshot(self, symbol: SecuritySymbol) -> dict[str, Any]:
        response = self.client.get(
            QUOTE_URL + symbol.tencent,
            headers={"Referer": "https://gu.qq.com/", "Accept": "text/plain"},
            retries=1,
        )
        return parse_snapshot(response.text, symbol.tencent)

    def _page(self, symbol: SecuritySymbol, page: int) -> list[dict[str, Any]] | None:
        response = self.client.get(
            TICK_URL,
            params={"appn": "detail", "action": "data", "c": symbol.tencent, "p": page},
            headers={"Referer": "https://gu.qq.com/", "Accept": "text/plain"},
            retries=0,
        )
        return parse_tick_page(response.text, symbol.tencent, page)

    @staticmethod
    def _merge_rows(base: list[dict[str, Any]], incoming: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        by_seq = {int(row["seq"]): row for row in base}
        for row in incoming:
            by_seq[int(row["seq"])] = row
        return [by_seq[key] for key in sorted(by_seq)]

    def fetch(self, code: str, *, max_pages: int = MAX_PAGES, verify_amount: bool = True, force: bool = False) -> Result:
        try:
            symbol = normalize_security(code)
            if symbol.market == "bj":
                return result_error(ResultStatus.UNSUPPORTED, source=TICK_SOURCE, source_url=TICK_URL, code="unsupported_market", message="腾讯分笔不支持北交所")
            snapshot = self._snapshot(symbol)
            cache_key = self.cache.key({"symbol": symbol.tencent})
            entry = None if force else self.cache.get(cache_key, allow_stale=True)
            cached = entry.value if entry else None
            if not isinstance(cached, dict) or cached.get("data_date") != snapshot["data_date"]:
                cached = None
            base_rows = list(cached.get("rows") or []) if cached else []
            next_page = int(cached.get("next_page") or 0) if cached else 0
            # Re-read the boundary page so an intraday page that grew since the
            # last call is merged without downloading the entire day again.
            start_page = max(0, next_page - 1) if cached else 0
            # Keep all completed pages before the boundary.  The first row of
            # the page being re-read is normally ``start_page * 70 + 1``;
            # retaining the preceding page's final sequence avoids inventing a
            # gap when page sizes are exactly 70.
            rows = [row for row in base_rows if int(row.get("seq", -1)) <= start_page * 70] if cached else []
            missing_seq: list[int] = []
            complete = False
            page = start_page
            for _ in range(max(1, int(max_pages))):
                page_rows = self._page(symbol, page)
                if page_rows is None:
                    complete = True
                    next_page = page
                    break
                rows = self._merge_rows(rows, page_rows)
                page += 1
                if self.sleep_seconds:
                    time.sleep(self.sleep_seconds)
            else:
                return result_error(ResultStatus.PARTIAL, source=TICK_SOURCE, source_url=TICK_URL, code="page_limit", message=f"翻页超过 {max_pages} 页，结果不完整", data={"rows": rows})

            if not rows:
                status = ResultStatus.EMPTY if snapshot["as_of"] < "092500" else ResultStatus.PARTIAL
                return Result(
                    status=status,
                    data={"code": symbol.code, "symbol": symbol.tencent, "rows": [], "windows": {}},
                    source=TICK_SOURCE,
                    source_url=TICK_URL,
                    data_date=snapshot["data_date"],
                    as_of=snapshot["as_of"],
                    freshness="fresh",
                    warnings=["腾讯快照有成交额但分笔为空" if status == ResultStatus.PARTIAL else "尚未撮合"],
                    request_count=self.client.request_count,
                )

            # Validate sequence and time order.  Gaps after the continuous
            # auction are recorded, not treated as a missing live transaction.
            ordered = sorted(rows, key=lambda row: int(row["seq"]))
            warnings: list[str] = []
            seen: set[int] = set()
            last_time = ""
            expected = int(ordered[0]["seq"])
            for row in ordered:
                seq = int(row["seq"])
                if seq in seen:
                    warnings.append(f"重复序号 {seq}")
                seen.add(seq)
                if seq > expected:
                    gap = list(range(expected, seq))
                    if str(row["time"]) <= SESSION_END:
                        warnings.append(f"连续竞价缺序号 {gap[0]}–{gap[-1]}")
                    else:
                        missing_seq.extend(gap)
                if last_time and _time_seconds(str(row["time"])) < _time_seconds(last_time):
                    warnings.append("分笔时间倒退")
                expected = seq + 1
                last_time = str(row["time"])

            continuous = [row for row in ordered if "09:30:00" <= str(row["time"]) <= SESSION_END]
            continuous_amount = sum(float(row.get("amount") or 0) for row in continuous)
            if verify_amount and snapshot["amount"] > 0 and snapshot["as_of"] >= "15:01:00":
                tolerance = snapshot["amount"] * 0.001 + 1000
                if abs(continuous_amount - snapshot["amount"]) > tolerance:
                    warnings.append(
                        f"连续竞价成交额 {continuous_amount:.0f} 与快照 {snapshot['amount']:.0f} 不符，分笔可能不完整"
                    )

            windows = {str(minutes): aggregate_ticks(ordered, window_minutes=minutes, as_of=snapshot["as_of"]) for minutes in (5, 15)}
            data = {
                "code": symbol.code,
                "symbol": symbol.tencent,
                "name": snapshot.get("name", ""),
                "data_date": snapshot["data_date"],
                "as_of": snapshot["as_of"],
                "rows": ordered,
                "row_count": len(ordered),
                "continuous_amount": round(continuous_amount, 2),
                "snapshot_amount": snapshot["amount"],
                "missing_seq": sorted(set(missing_seq)),
                "windows": windows,
                "note": "腾讯约3秒聚合分笔，不是交易所 Level-2 原始逐笔委托/成交",
            }
            self.cache.set(cache_key, {"data_date": snapshot["data_date"], "rows": ordered, "next_page": next_page, "complete": complete}, ttl=24 * 3600, data_date=snapshot["data_date"])
            if any("连续竞价缺序号" in warning or "时间倒退" in warning for warning in warnings):
                status = ResultStatus.PARTIAL
            else:
                status = ResultStatus.OK
            return Result(
                status=status,
                data=data,
                source=TICK_SOURCE,
                source_url=TICK_URL,
                data_date=snapshot["data_date"],
                as_of=snapshot["as_of"],
                freshness="fresh",
                warnings=warnings,
                request_count=self.client.request_count,
                cache={"hit": bool(cached), "incremental": bool(cached), "next_page": next_page},
            )
        except SymbolError as exc:
            return result_error(ResultStatus.UNSUPPORTED, source=TICK_SOURCE, source_url=TICK_URL, code="invalid_security", message=str(exc))
        except HTTPClientError as exc:
            return result_error(ResultStatus.UNAVAILABLE, source=TICK_SOURCE, source_url=TICK_URL, code=exc.code, message=str(exc), retryable=exc.retryable)
        except (ValueError, RuntimeError) as exc:
            return result_error(ResultStatus.PARTIAL, source=TICK_SOURCE, source_url=TICK_URL, code="parse_or_integrity_error", message=str(exc), retryable=True)
