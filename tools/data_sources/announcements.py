"""Announcement evidence and fail-closed primary/fallback orchestration."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any

from .cninfo import CNInfoAnnouncementSource
from .contracts import Result, ResultStatus, result_empty, result_error, result_ok


ANNOUNCEMENT_SOURCE = "announcement_evidence"


def classify_announcement_titles(titles: Iterable[str]) -> dict[str, list[str]]:
    """Classify titles for evidence display; this does not decide a trade."""
    avoid_words = ("立案", "处罚", "退市", "重大违法", "风险警示", "暂停上市")
    watch_words = ("减持", "问询", "监管", "诉讼", "质押", "业绩预告", "回购")
    result = {"avoid": [], "watch_risk": [], "other": []}
    for value in titles:
        title = str(value or "").strip()
        if not title:
            continue
        bucket = "other"
        if any(word in title for word in avoid_words):
            bucket = "avoid"
        elif any(word in title for word in watch_words):
            bucket = "watch_risk"
        result[bucket].append(title)
    return result


def _primary_rows(value: Any) -> tuple[list[dict[str, Any]], str]:
    if isinstance(value, Result):
        if value.status in {ResultStatus.OK.value, ResultStatus.EMPTY.value} and isinstance(value.data, list):
            return value.data, value.source_url
        raise ValueError(value.error.get("message") if isinstance(value.error, Mapping) else "主公告源不可用")
    if isinstance(value, Mapping):
        if "rows" in value:
            rows = value.get("rows")
        elif "announcements" in value:
            rows = value.get("announcements")
        else:
            rows = value.get("data")
        if not isinstance(rows, list):
            raise ValueError("主公告源缺少公告列表")
        return [dict(item) if isinstance(item, Mapping) else {"title": str(item)} for item in rows], str(value.get("source_url") or "")
    if isinstance(value, list):
        return [dict(item) if isinstance(item, Mapping) else {"title": str(item)} for item in value], ""
    raise ValueError("主公告源返回结构异常")


def fetch_announcement_evidence(
    code: str,
    *,
    primary: Callable[[], Any] | None = None,
    fallback: CNInfoAnnouncementSource | None = None,
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
            return Result(
                status=status,
                data={"rows": normalized, "classification": classify_announcement_titles(item["title"] for item in normalized)},
                source="primary_announcement",
                source_url=source_url,
                freshness="fresh",
            )
        except Exception as exc:  # primary errors are evidence for fallback, not clean state
            primary_error = str(exc)
    fallback = fallback or CNInfoAnnouncementSource()
    result = fallback.fetch(code, page_size=page_size)
    if result.status in {ResultStatus.OK.value, ResultStatus.EMPTY.value}:
        rows = result.data if isinstance(result.data, list) else []
        result.data = {"rows": rows, "classification": classify_announcement_titles(item.get("title", "") for item in rows if isinstance(item, Mapping))}
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
