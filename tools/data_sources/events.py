"""Structured, on-demand event evidence from Eastmoney data-center feeds."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
from typing import Any

from .cache import JsonCache
from .contracts import Result, ResultStatus, result_error, result_ok
from .http import HTTPClient, HTTPClientError
from .symbols import SymbolError, normalize_security, validate_ymd


DATACENTER_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
EVENT_SOURCE = "eastmoney_events"
BEIJING = timezone(timedelta(hours=8))

EVENT_TYPES = {
    "unlock": {
        "name": "解禁",
        "report": "RPT_LIFT_STAGE",
        "date_fields": ("NOTICE_DATE", "ANN_DATE", "FREE_DATE", "free_date", "DATE"),
        "effective_fields": ("FREE_DATE", "free_date", "DATE"),
        "title_fields": ("FREE_SHARES_TYPE", "LIMITED_STOCK_TYPE"),
        "shares_fields": ("FREE_SHARES", "ABLE_FREE_SHARES"),
        "ratio_fields": ("FREE_RATIO",),
    },
    "holder_trade": {
        "name": "股东增减持",
        "report": "RPT_HOLDER_INCREASE_DECREASE",
        "date_fields": ("NOTICE_DATE", "END_DATE", "TRADE_DATE"),
        "effective_fields": ("START_DATE", "END_DATE", "TRADE_DATE"),
        "title_fields": ("CHANGE_TYPE", "HOLDER_NAME", "CHANGE_REASON"),
        "shares_fields": ("CHANGE_SHARES", "HOLD_NUM", "HOLD_NUM_CHANGE"),
        "ratio_fields": ("CHANGE_RATIO", "HOLD_RATIO"),
    },
    "earnings_forecast": {
        "name": "业绩预告",
        "report": "RPT_PUBLIC_OP_NEWPREDICT",
        "date_fields": ("NOTICE_DATE",),
        "effective_fields": ("REPORT_DATE",),
        "title_fields": ("PREDICT_FINANCE", "PREDICT_TYPE"),
        "amount_fields": ("PREDICT_AMT_LOWER", "PREDICT_AMT_UPPER"),
        "ratio_fields": ("ADD_AMP_LOWER", "ADD_AMP_UPPER"),
    },
    "buyback": {
        "name": "回购",
        "report": "RPT_SHARE_HOLDER_REPURCHASE",
        "date_fields": ("NOTICE_DATE", "ANN_DATE"),
        "effective_fields": ("PLAN_END_DATE", "EXECUTE_DATE"),
        "title_fields": ("REPURPOSE", "STATUS", "PLAN_PROGRESS"),
        "shares_fields": ("REPURCHASE_NUM", "REPURCHASE_SHARES"),
        "amount_fields": ("REPURCHASE_AMOUNT", "AMOUNT"),
        "ratio_fields": ("REPURCHASE_RATIO",),
    },
    "pledge": {
        "name": "股权质押",
        "report": "RPT_PLEDGE_STATISTICS",
        "date_fields": ("NOTICE_DATE", "PLEDGE_DATE", "END_DATE"),
        "effective_fields": ("PLEDGE_DATE", "END_DATE"),
        "title_fields": ("PLEDGE_STATUS", "PLEDGEE", "HOLDER_NAME"),
        "shares_fields": ("PLEDGE_NUM", "PLEDGE_SHARES"),
        "ratio_fields": ("PLEDGE_RATIO",),
    },
}


def _first(row: Mapping[str, Any], fields: Iterable[str]) -> Any:
    for field in fields:
        if field in row and row[field] not in (None, "", "-"):
            return row[field]
    return None


def _date_or_none(value: Any) -> str | None:
    if value in (None, "", "-"):
        return None
    raw = str(value).strip()
    if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
        return raw[:10]
    if len(raw) >= 8 and raw[:8].isdigit():
        try:
            return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8])).isoformat()
        except ValueError:
            return None
    return raw[:10] if len(raw) >= 10 else raw


def _number_or_none(value: Any) -> float | None:
    if value in (None, "", "-"):
        return None
    try:
        result = float(str(value).replace(",", "").replace("%", ""))
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _rows_from_payload(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    if not isinstance(payload, Mapping):
        raise ValueError("事件响应不是对象")
    result = payload.get("result")
    data = result.get("data") if isinstance(result, Mapping) else payload.get("data")
    if data is None:
        return [] if isinstance(result, Mapping) and result.get("count") == 0 else (_ for _ in ()).throw(ValueError("事件响应缺少 result.data"))
    if not isinstance(data, list):
        raise ValueError("事件 result.data 不是列表")
    if not all(isinstance(item, Mapping) for item in data):
        raise ValueError("事件结果含非对象记录")
    return list(data)


def normalize_event_rows(rows: Iterable[Mapping[str, Any]], *, event_type: str, code: str, source_url: str = DATACENTER_URL) -> list[dict[str, Any]]:
    """Normalize source rows without interpreting them as a trade signal."""
    config = EVENT_TYPES.get(event_type)
    if config is None:
        raise ValueError(f"未知事件类型: {event_type}")
    output: list[dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise ValueError("事件记录不是对象")
        returned_code = str(_first(raw, ("SECURITY_CODE", "SECUCODE", "code", "CODE")) or "").strip()
        if returned_code and returned_code.zfill(6) != code:
            raise ValueError(f"事件接口返回其他证券: {returned_code}")
        notice_date = _date_or_none(_first(raw, config.get("date_fields", ())))
        effective_date = _date_or_none(_first(raw, config.get("effective_fields", ())))
        title = _first(raw, config.get("title_fields", ()))
        row = {
            "event_type": event_type,
            "event_type_name": config["name"],
            "code": code,
            "name": str(_first(raw, ("SECURITY_NAME_ABBR", "SECURITY_NAME", "name", "NAME")) or ""),
            "notice_date": notice_date,
            "effective_date": effective_date,
            "title": str(title or ""),
            "shares": _number_or_none(_first(raw, config.get("shares_fields", ()))),
            "shares_unit": "source_native" if config.get("shares_fields") else None,
            "amount": _number_or_none(_first(raw, config.get("amount_fields", ()))),
            "amount_unit": "source_native" if config.get("amount_fields") else None,
            "ratio": _number_or_none(_first(raw, config.get("ratio_fields", ()))),
            "ratio_unit": "source_native_percent_or_fraction" if config.get("ratio_fields") else None,
            "status": str(_first(raw, ("STATUS", "status", "PLAN_STATUS", "PLEDGE_STATUS")) or ""),
            "source": EVENT_SOURCE,
            "source_url": source_url,
            "raw": dict(raw),
        }
        row["evidence_key"] = hashlib.sha256(json.dumps({key: row.get(key) for key in ("event_type", "code", "notice_date", "effective_date", "title", "shares", "amount")}, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()[:16]
        output.append(row)
    return output


class EastmoneyEventSource:
    def __init__(self, client: HTTPClient | None = None, *, cache: JsonCache | None = None, cache_ttl: int = 24 * 3600):
        self.client = client or HTTPClient()
        self.cache = cache or JsonCache("eastmoney_events")
        self.cache_ttl = max(300, int(cache_ttl))

    def _fetch_one(self, code: str, event_type: str, *, start: str | None, end: str | None, limit: int) -> Result:
        config = EVENT_TYPES[event_type]
        filters = [f'(SECURITY_CODE="{code}")']
        date_field = config.get("date_fields", (None,))[0]
        if start and end and date_field:
            filters.append(f"({date_field}>='{start}')({date_field}<='{end}')")
        filter_value = "".join(filters)
        params = {
            "reportName": config["report"],
            "columns": "ALL",
            "source": "WEB",
            "client": "WEB",
            "filter": filter_value,
            "pageNumber": "1",
            "pageSize": str(max(1, min(int(limit), 500))),
            "sortColumns": date_field or "NOTICE_DATE",
            "sortTypes": "-1",
        }
        try:
            response = self.client.get(DATACENTER_URL, params=params, headers={"Referer": "https://data.eastmoney.com/"}, retries=1)
            rows = normalize_event_rows(_rows_from_payload(response.json()), event_type=event_type, code=code, source_url=response.url)
            return Result(status=ResultStatus.OK if rows else ResultStatus.EMPTY, data=rows, source=EVENT_SOURCE, source_url=response.url, freshness="fresh", request_count=self.client.request_count)
        except HTTPClientError as exc:
            return result_error(ResultStatus.UNAVAILABLE, source=EVENT_SOURCE, source_url=DATACENTER_URL, code=exc.code, message=str(exc), retryable=exc.retryable)
        except (TypeError, ValueError, KeyError) as exc:
            return result_error(ResultStatus.UNAVAILABLE, source=EVENT_SOURCE, source_url=DATACENTER_URL, code="malformed_response", message=str(exc), retryable=True)

    def fetch(self, code: str, *, event_types: Iterable[str] | None = None, as_of: str | None = None, forward_days: int = 90, force: bool = False, limit: int = 100) -> Result:
        try:
            symbol = normalize_security(code)
            if as_of is None:
                as_of = datetime.now(BEIJING).date().isoformat()
            as_of = validate_ymd(as_of)
        except (SymbolError, ValueError) as exc:
            return result_error(ResultStatus.UNSUPPORTED, source=EVENT_SOURCE, source_url=DATACENTER_URL, code="invalid_request", message=str(exc))
        selected = list(event_types or EVENT_TYPES)
        unknown = [item for item in selected if item not in EVENT_TYPES]
        if unknown:
            return result_error(ResultStatus.UNSUPPORTED, source=EVENT_SOURCE, source_url=DATACENTER_URL, code="unsupported_event_type", message=f"未知事件类型: {', '.join(unknown)}")
        end = (date.fromisoformat(as_of) + timedelta(days=max(0, int(forward_days)))).isoformat()
        key = self.cache.key({"code": symbol.code, "types": selected, "as_of": as_of, "forward_days": forward_days, "limit": limit})
        if not force:
            cached = self.cache.get(key, ttl=self.cache_ttl)
            if cached and isinstance(cached.value, dict):
                return Result(status=ResultStatus.OK if cached.value.get("rows") else ResultStatus.EMPTY, data=cached.value, source=EVENT_SOURCE, source_url=DATACENTER_URL, data_date=as_of, freshness="cached", cache={"hit": True})
        all_rows: list[dict[str, Any]] = []
        statuses: dict[str, str] = {}
        errors: list[str] = []
        for event_type in selected:
            result = self._fetch_one(symbol.code, event_type, start=as_of, end=end if event_type == "unlock" else None, limit=limit)
            statuses[event_type] = result.status
            if result.status in {ResultStatus.OK.value, ResultStatus.EMPTY.value}:
                all_rows.extend(result.data or [])
            else:
                errors.append(f"{event_type}:{(result.error or {}).get('message', result.status)}")
        unique: dict[str, dict[str, Any]] = {}
        for row in all_rows:
            unique[row["evidence_key"]] = row
        data = {"code": symbol.code, "as_of": as_of, "forward_days": forward_days, "rows": sorted(unique.values(), key=lambda row: (row.get("effective_date") or "9999-99-99", row.get("notice_date") or "9999-99-99")), "status_by_type": statuses, "coverage_note": "事件源按类型独立请求；金额/股数保留源原生单位，未知不填0"}
        if not all_rows and errors and len(errors) == len(selected):
            return result_error(ResultStatus.UNAVAILABLE, source=EVENT_SOURCE, source_url=DATACENTER_URL, code="all_event_types_failed", message="事件源均不可用", warnings=errors, retryable=True)
        self.cache.set(key, data, ttl=self.cache_ttl, data_date=as_of)
        status = ResultStatus.PARTIAL if errors else ResultStatus.OK if all_rows else ResultStatus.EMPTY
        return Result(status=status, data=data, source=EVENT_SOURCE, source_url=DATACENTER_URL, data_date=as_of, freshness="fresh", warnings=errors, cache={"hit": False}, request_count=self.client.request_count)
