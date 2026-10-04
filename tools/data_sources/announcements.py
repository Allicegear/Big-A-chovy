"""Announcement evidence and fail-closed primary/fallback orchestration."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any

from .cninfo import CNInfoAnnouncementSource
from .contracts import Result, ResultStatus, result_empty, result_error, result_ok
from tools.rule_config import RULE_CONFIG


ANNOUNCEMENT_SOURCE = "announcement_evidence"


def _announcement_config() -> dict[str, list[str]]:
    """Read the single announcement policy owned by ``rule_config``."""
    config = RULE_CONFIG["risk"]["announcement"]
    return {
        "hard_keywords": list(config["hard_keywords"]),
        "watch_keywords": list(config["watch_keywords"]),
        "ignore_keywords": list(config["ignore_keywords"]),
    }


def classify_announcement_titles(titles: Iterable[str]) -> dict[str, list[str]]:
    """Classify titles using the same policy as the production screener."""
    config = _announcement_config()
    result = {"avoid": [], "watch_risk": [], "other": []}
    for value in titles:
        title = str(value or "").strip()
        if not title or any(word in title for word in config["ignore_keywords"]):
            continue
        bucket = "other"
        if any(word in title for word in config["hard_keywords"]):
            bucket = "avoid"
        elif any(word in title for word in config["watch_keywords"]):
            bucket = "watch_risk"
        result[bucket].append(title)
    return result


def classify_announcement_risk(titles: Iterable[str]) -> dict[str, Any]:
    """Return the canonical risk result used by both evidence and screening."""
    config = _announcement_config()
    normalized = [str(value or "").strip() for value in titles if str(value or "").strip()]
    filtered = [title for title in normalized if not any(word in title for word in config["ignore_keywords"])]
    classification = classify_announcement_titles(filtered)
    hard = sorted({word for title in filtered for word in config["hard_keywords"] if word in title})
    watch = sorted({word for title in filtered for word in config["watch_keywords"] if word in title})
    if hard:
        risk = RULE_CONFIG["risk"]["statuses"]["avoid"]
    elif watch:
        risk = RULE_CONFIG["risk"]["statuses"]["watch_risk"]
    else:
        risk = RULE_CONFIG["risk"]["statuses"]["clean"]
    return {
        "announcement_risk": risk,
        "announcement_keywords": hard or watch,
        "announcement_titles": filtered[:3],
        "classification": classification,
    }


def _business_failure(payload: Mapping[str, Any]) -> str | None:
    """Find business-error envelopes before accepting an empty list."""
    if "success" in payload:
        success = payload.get("success")
        if success not in (True, 1, "1", "true", "True", "ok", "OK"):
            return f"业务 success={success!r}"
    if "code" in payload and payload.get("code") not in (None, "", 0, "0", 200, "200"):
        return f"业务 code={payload.get('code')!r}"
    if "data" in payload and payload.get("data") is None:
        return "业务 data=null"
    for key in ("data", "result"):
        nested = payload.get(key)
        if isinstance(nested, Mapping):
            failure = _business_failure(nested)
            if failure:
                return failure
    return None


def _validate_row_shape(rows: list[Any], *, code: str | None = None) -> list[dict[str, Any]]:
    """Require every non-empty announcement row to carry a usable title."""
    title_keys = ("title", "announcementTitle", "noticeTitle", "notice_title", "art_title", "artTitle", "TITLE")
    code_keys = ("SECURITY_CODE", "securityCode", "stockCode", "secCode", "security_code")
    normalized: list[dict[str, Any]] = []
    for item in rows:
        if isinstance(item, str):
            title = item.strip()
            if not title:
                raise ValueError("公告记录标题为空")
            normalized.append({"title": title})
            continue
        if not isinstance(item, Mapping):
            raise ValueError("公告记录不是对象")
        title = next((str(item.get(key) or "").strip() for key in title_keys if str(item.get(key) or "").strip()), "")
        if not title:
            raise ValueError("公告非空页缺少可解析标题")
        if code:
            for key in code_keys:
                value = str(item.get(key) or "").strip()
                if value and value.split(".", 1)[0].zfill(6) != code:
                    raise ValueError(f"公告返回其他证券: {value}")
        row = dict(item)
        row["title"] = title
        normalized.append(row)
    return normalized


def _validate_page_count(rows: list[Any], total: Any, page_size: int) -> None:
    if total in (None, ""):
        return
    try:
        total_int = int(total)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"公告 total 不是非负整数: {total!r}") from exc
    if total_int < 0:
        raise ValueError(f"公告 total 为负数: {total_int}")
    expected = min(total_int, max(1, int(page_size)))
    if len(rows) != expected:
        raise ValueError(f"公告页不完整: total={total_int}, page_size={page_size}, rows={len(rows)}")


def _primary_container(value: Any) -> tuple[list[Any], Mapping[str, Any]] | None:
    """Extract rows from either a normalized result or provider nesting."""
    if isinstance(value, list):
        return value, {}
    if not isinstance(value, Mapping):
        return None
    for key in ("rows", "list", "announcements"):
        if isinstance(value.get(key), list):
            return value[key], value
    for key in ("data", "result"):
        nested = value.get(key)
        found = _primary_container(nested)
        if found is not None:
            rows, metadata = found
            merged = dict(value)
            merged.update(metadata)
            return rows, merged
    return None


def _primary_rows(value: Any) -> tuple[list[dict[str, Any]], str]:
    if isinstance(value, Result):
        if value.status in {ResultStatus.OK.value, ResultStatus.EMPTY.value} and isinstance(value.data, list):
            return _validate_row_shape(value.data), value.source_url
        raise ValueError(value.error.get("message") if isinstance(value.error, Mapping) else "主公告源不可用")
    if isinstance(value, Mapping):
        failure = _business_failure(value)
        if failure:
            raise ValueError(f"主公告源{failure}")
        found = _primary_container(value)
        if found is None:
            raise ValueError("主公告源缺少公告列表")
        rows, metadata = found
        total = next((metadata.get(key) for key in ("_provider_total", "total", "totalCount", "totalAnnouncement", "count") if metadata.get(key) is not None), None)
        page_size = int(metadata.get("_requested_page_size") or metadata.get("page_size") or 30)
        _validate_page_count(rows, total, page_size)
        return _validate_row_shape(rows), str(value.get("source_url") or metadata.get("source_url") or "")
    if isinstance(value, list):
        return _validate_row_shape(value), ""
    raise ValueError("主公告源返回结构异常")


def fetch_announcement_evidence(
    code: str,
    *,
    primary: Callable[[], Any] | None = None,
    fallback: CNInfoAnnouncementSource | None = None,
    fallback_factory: Callable[[], CNInfoAnnouncementSource] | None = None,
    page_size: int = 30,
) -> Result:
    """Try the current primary source, then CNINFO, preserving failure state."""
    primary_error: str | None = None
    if primary is not None:
        try:
            rows, source_url = _primary_rows(primary())
            normalized = []
            for row in rows:
                title = str(row.get("title") or row.get("announcementTitle") or "").strip()
                if not title:
                    continue
                item = dict(row)
                item["title"] = title
                item.setdefault("source", "primary_announcement")
                normalized.append(item)
            status = ResultStatus.OK if normalized else ResultStatus.EMPTY
            risk = classify_announcement_risk(item["title"] for item in normalized)
            return Result(
                status=status,
                data={"rows": normalized, "classification": risk["classification"], "announcement_risk": risk["announcement_risk"], "announcement_keywords": risk["announcement_keywords"]},
                source="primary_announcement",
                source_url=source_url,
                freshness="fresh",
            )
        except Exception as exc:  # primary errors are evidence for fallback, not clean state
            primary_error = str(exc)
    fallback = fallback_factory() if fallback_factory is not None else fallback or CNInfoAnnouncementSource()
    result = fallback.fetch(code, page_size=page_size)
    if result.status in {ResultStatus.OK.value, ResultStatus.EMPTY.value}:
        rows = result.data if isinstance(result.data, list) else []
        risk = classify_announcement_risk(item.get("title", "") for item in rows if isinstance(item, Mapping))
        result.data = {"rows": rows, "classification": risk["classification"], "announcement_risk": risk["announcement_risk"], "announcement_keywords": risk["announcement_keywords"]}
        result.source = f"{result.source}:fallback"
        if primary_error:
            result.warnings.append(f"主公告源失败，已切换巨潮: {primary_error}")
        return result
    message = "主公告源与巨潮公告均不可用"
    if primary_error:
        message += f"；主源: {primary_error}"
    if isinstance(result.error, Mapping) and result.error.get("message"):
        message += f"；巨潮: {result.error['message']}"
    return result_error(ResultStatus.UNAVAILABLE, source=ANNOUNCEMENT_SOURCE, source_url=result.source_url, code="all_announcement_sources_failed", message=message, retryable=True)
