"""公告风险：东财公告抓取（巨潮兜底）、7 日风险缓存、按栏目挂载风险与池门禁。

风险语义（clean / watch_risk / avoid / unknown）只读 screening.risk；取数失败一律
fail-closed 为 unknown。缓存文件路径来自 runtime_paths（随 A_SHARE_STATE_DIR 隔离）。
_ANNOUNCEMENT_RISK_CACHE 是本模块用 global 重新绑定的进程内缓存，核心门面不导出。
"""
from __future__ import annotations

import concurrent.futures as futures
import json
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

SCRIPT_DIR = Path(__file__).resolve().parents[2]
_PROJECT_ROOT = SCRIPT_DIR.parent.parent
for _p in (str(_PROJECT_ROOT), str(SCRIPT_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)
from tools.rule_config import RULE_CONFIG  # noqa: E402
from tools.data_sources.announcements import (  # noqa: E402
    classify_announcement_risk as _canonical_announcement_risk,
    extract_primary_announcement_container,
    extract_primary_announcement_total,
    fetch_announcement_evidence,
    validate_primary_announcement_page,
)
from tools.data_sources.cninfo import CNInfoAnnouncementSource  # noqa: E402
from tools.data_sources.http import project_http_client  # noqa: E402

import runtime_paths  # noqa: E402  运行状态路径唯一来源
import state_commit  # noqa: E402  原子写入
from screening.net import fetch_json  # noqa: E402
from screening.risk import (  # noqa: E402
    RISK_AVOID,
    RISK_CHECKED_VALUES,
    RISK_UNKNOWN,
    RISK_WATCH,
    _row_risk_status,
    append_risk_text,
)

RISK_CONFIG = RULE_CONFIG["risk"]

ANNOUNCEMENT_URL = "https://np-anotice-stock.eastmoney.com/api/security/ann"

ANNOUNCEMENT_CACHE_FILE = runtime_paths.state_file(".announcement_risk_cache.json")
_ANNOUNCEMENT_RISK_CACHE: Optional[Dict[str, Dict[str, Any]]] = None
ANNOUNCEMENT_CACHE_TTL_SECONDS = 7 * 24 * 60 * 60
ANNOUNCEMENT_REQUEST_TIMEOUT_SECONDS = 4

ANNOUNCEMENT_CONFIG = RISK_CONFIG["announcement"]
HARD_ANNOUNCEMENT_KEYWORDS = list(ANNOUNCEMENT_CONFIG["hard_keywords"])
WATCH_ANNOUNCEMENT_KEYWORDS = list(ANNOUNCEMENT_CONFIG["watch_keywords"])
ANNOUNCEMENT_IGNORE_KEYWORDS = list(ANNOUNCEMENT_CONFIG["ignore_keywords"])


def collect_announcement_titles(obj: Any) -> List[str]:
    titles: List[str] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            lower = str(key).lower()
            if isinstance(value, str) and ("title" in lower or "announcement" in lower or "notice" in lower):
                title = value.strip()
                if title:
                    titles.append(title)
            else:
                titles.extend(collect_announcement_titles(value))
    elif isinstance(obj, list):
        for item in obj:
            titles.extend(collect_announcement_titles(item))
    seen = set()
    deduped = []
    for title in titles:
        if title not in seen:
            seen.add(title)
            deduped.append(title)
    return deduped


def _announcement_container(value: Any) -> tuple[list[Any], list[Any]] | None:
    """Find rows while retaining the original metadata at every envelope level."""
    return extract_primary_announcement_container(value)


def _announcement_business_failure(value: Any) -> str | None:
    """Reject an error envelope at any data/result nesting level."""
    if not isinstance(value, dict):
        return None
    if "error" in value and value.get("error") not in (None, "", False, 0):
        return f"error={value.get('error')!r}"
    if "success" in value and value.get("success") not in (True, 1, "1", "true", "True", "ok", "OK"):
        return f"success={value.get('success')!r}"
    if "code" in value and value.get("code") not in (None, "", 0, "0", 200, "200"):
        return f"code={value.get('code')!r}"
    if "data" in value and value.get("data") is None:
        return "data=null"
    for key in ("data", "result"):
        nested = value.get(key)
        if isinstance(nested, dict):
            failure = _announcement_business_failure(nested)
            if failure:
                return failure
    return None


def _has_announcement_container(value: Any) -> bool:
    """Return whether an announcement response contains a list-shaped payload.

    An HTTP 200 response is not enough evidence that Eastmoney returned a
    valid empty list.  The endpoint has returned business-error envelopes with
    ``data: null`` and empty objects; those must reach the CNINFO fallback.
    """
    return _announcement_container(value) is not None


def _announcement_row_title(row: Any) -> str:
    if isinstance(row, str):
        return row.strip()
    if not isinstance(row, dict):
        return ""
    for key in ("title", "announcementTitle", "noticeTitle", "notice_title", "art_title", "artTitle", "TITLE"):
        title = str(row.get(key) or "").strip()
        if title:
            return title
    return ""


def _announcement_total(metadata_layers: Any) -> Any:
    """Read only fields that mean total matches; ``count`` is page-local."""
    if isinstance(metadata_layers, dict):
        metadata_layers = [metadata_layers]
    return extract_primary_announcement_total(metadata_layers or [])


def _announcement_total_from_response(data: Any) -> Any:
    if isinstance(data, dict):
        found = _announcement_container(data)
        return _announcement_total(found[1] if found is not None else [data])
    return None


def _validate_announcement_page_count(rows: List[Any], total: Any, page_size: int) -> None:
    metadata = [] if total in (None, "") else [{"_provider_total": total}]
    validate_primary_announcement_page(rows, metadata, page_size)


def _validate_primary_announcement_response(data: Any, *, code: str, page_size: int) -> list[dict[str, Any]]:
    """Validate Eastmoney's business envelope before title extraction."""
    if isinstance(data, list):
        rows = data
        metadata_layers: list[Any] = []
    else:
        if not isinstance(data, dict):
            raise RuntimeError("东财公告响应不是对象")
        failure = _announcement_business_failure(data)
        if failure:
            raise RuntimeError(f"东财公告业务失败: {failure}")
        found = _announcement_container(data)
        if found is None:
            raise RuntimeError("东财公告响应缺少公告列表容器")
        rows, metadata_layers = found
    validate_primary_announcement_page(rows, metadata_layers, page_size)
    normalized: list[dict[str, Any]] = []
    for row in rows:
        title = _announcement_row_title(row)
        if not title:
            raise RuntimeError("东财公告非空页缺少可解析标题")
        if isinstance(row, dict):
            returned_code = str(next((row.get(key) for key in ("SECURITY_CODE", "securityCode", "stockCode", "secCode", "security_code") if row.get(key)), "")).strip()
            if returned_code and returned_code.split(".", 1)[0].zfill(6) != str(code).zfill(6):
                raise RuntimeError(f"东财公告返回其他证券: {returned_code}")
            normalized.append({**row, "title": title})
        else:
            normalized.append({"title": title})
    return normalized


def fetch_announcements(code: str, page_size: int = 8) -> List[str]:
    """Return announcement titles while preserving the existing risk API.

    Eastmoney remains the primary source because it is already part of the
    screening path.  A structurally valid empty response stays empty; network
    or schema failures try CNINFO.  If both sources fail an exception is
    raised so ``attach_announcement_risks`` keeps the last-known avoid or
    marks the stock unknown instead of upgrading it to clean.
    """
    def _primary() -> Dict[str, Any]:
        data = fetch_json(ANNOUNCEMENT_URL, {
            "sr": -1,
            "page_size": page_size,
            "page_index": 1,
            "ann_type": "A",
            "client_source": "web",
            "stock_list": code,
        }, timeout=ANNOUNCEMENT_REQUEST_TIMEOUT_SECONDS, retries=0)
        rows = _validate_primary_announcement_response(data, code=code, page_size=page_size)
        return {
            "rows": rows[:page_size],
            "source_url": ANNOUNCEMENT_URL,
            "_provider_total": _announcement_total_from_response(data),
            "_requested_page_size": page_size,
        }

    evidence = fetch_announcement_evidence(
        code,
        primary=_primary,
        fallback_factory=lambda: CNInfoAnnouncementSource(client=project_http_client()),
        page_size=page_size,
    )
    if evidence.status not in {"ok", "empty"}:
        error = evidence.error if isinstance(evidence.error, dict) else {}
        raise RuntimeError(error.get("message") or "公告源不可用")
    payload = evidence.data if isinstance(evidence.data, dict) else {}
    return [str(row.get("title")) for row in (payload.get("rows") or []) if isinstance(row, dict) and row.get("title")][:page_size]


def classify_announcement_risk(titles: List[str]) -> Dict[str, Any]:
    canonical = _canonical_announcement_risk(titles)
    return {key: canonical[key] for key in ("announcement_risk", "announcement_keywords", "announcement_titles")}


def _load_announcement_risk_cache() -> Dict[str, Dict[str, Any]]:
    global _ANNOUNCEMENT_RISK_CACHE
    if _ANNOUNCEMENT_RISK_CACHE is not None:
        return _ANNOUNCEMENT_RISK_CACHE
    try:
        payload = json.loads(ANNOUNCEMENT_CACHE_FILE.read_text(encoding="utf-8"))
        _ANNOUNCEMENT_RISK_CACHE = payload if isinstance(payload, dict) else {}
    except Exception:
        _ANNOUNCEMENT_RISK_CACHE = {}
    return _ANNOUNCEMENT_RISK_CACHE


def _save_announcement_risk_cache(cache: Dict[str, Dict[str, Any]]) -> None:
    try:
        # 原子写入：轮次被中断/进程被杀时不留下半截 JSON（读取侧会整体丢弃坏文件）。
        state_commit.atomic_write_text(
            ANNOUNCEMENT_CACHE_FILE, json.dumps(cache, ensure_ascii=False, indent=2)
        )
    except Exception:
        # A cache write failure must never turn a valid screening result into
        # a hard error. The in-memory cache still protects this run.
        pass


# 参与公告核验的栏目：公告查询范围 = 这些栏目里出现的全部代码（去重）。
# 不含 trend_diagnostics：那是仅供解释的近似命中行，不能把它变成一人一次请求。
ANNOUNCEMENT_SECTIONS = (
    "strict_ultra", "trend_observation", "strict_trend", "dual_pool",
    "capital_rank", "dual_pool_raw", "low_ultra", "low_trend", "watchlist",
    "pre_intersection",   # 准交集表必须与其它表读到同一份风险结果
    "negative_super_observations",  # 负超单观察行展示的公告状态须与其它表同源
)


def attach_announcement_risks(
    result: Dict[str, Any],
    page_size: int,
    workers: int,
    risk_cache: Optional[Dict[str, Dict[str, Any]]] = None,
    progress_callback: Optional[Callable[[int, int, str, str, str], None]] = None,
) -> List[str]:
    rows: List[Dict[str, Any]] = []
    # Diagnostics can contain the full pre-truncation ultra candidate set.
    # They are for explanation only, so they must not turn one checkbox into
    # one network request per near-miss stock. Check only visible/actionable
    # sections; matching diagnostic rows are annotated from this same map.
    for section in ANNOUNCEMENT_SECTIONS:
        rows.extend(result.get(section) or [])
    codes = sorted({str(r.get("code")) for r in rows if r.get("code")})
    result["announcement_total_count"] = len(codes)
    if not codes:
        result["announcement_check_available"] = True
        result["announcement_unknown_codes"] = []
        result["announcement_cached_count"] = 0
        result["announcement_requested_count"] = 0
        return []

    cache = risk_cache if risk_cache is not None else _load_announcement_risk_cache()
    ann_map: Dict[str, Dict[str, Any]] = {}
    errors: List[str] = []
    cache_changed = False
    now = time.time()
    pending_codes: List[str] = []
    for code in codes:
        cached = cache.get(code) or {}
        cached_status = cached.get("status")
        checked_at = cached.get("checked_at")
        try:
            cache_fresh = checked_at is not None and now - float(checked_at) <= ANNOUNCEMENT_CACHE_TTL_SECONDS
        except (TypeError, ValueError):
            cache_fresh = False
        if cache_fresh and cached_status in RISK_CHECKED_VALUES:
            ann_map[code] = {
                "announcement_risk": cached_status,
                "risk_status": cached_status,
                "announcement_keywords": cached.get("keywords") or [],
                "announcement_titles": cached.get("titles") or [],
                "announcement_check": "cached",
                "announcement_risk_source": "cache",
            }
        else:
            pending_codes.append(code)

    total = len(codes)
    completed = 0

    def report_progress(code: str, status: str, source: str) -> None:
        nonlocal completed
        completed += 1
        if progress_callback is not None:
            try:
                progress_callback(completed, total, code, status, source)
            except Exception:
                pass

    for code in codes:
        if code in ann_map:
            report_progress(code, ann_map[code]["risk_status"], "cache")

    with futures.ThreadPoolExecutor(max_workers=max(1, min(workers, 12))) as pool:
        future_map = {pool.submit(fetch_announcements, code, page_size): code for code in pending_codes}
        for fut in futures.as_completed(future_map):
            code = future_map[fut]
            try:
                classified = classify_announcement_risk(fut.result())
                status = classified["announcement_risk"]
                ann_map[code] = {
                    **classified,
                    "risk_status": status,
                    "announcement_check": "checked",
                    "announcement_risk_source": "current",
                }
                cache[code] = {
                    "status": status,
                    "keywords": classified.get("announcement_keywords") or [],
                    "titles": classified.get("announcement_titles") or [],
                    "checked_at": time.time(),
                }
                cache_changed = True
                report_progress(code, status, "current")
            except Exception:
                errors.append(code)
                cached = cache.get(code) or {}
                cached_status = cached.get("status")
                # A stale last-known avoid is still safer than silently
                # downgrading a known risk to unknown when the source fails.
                # TTL controls re-query skipping, not risk erasure.
                if cached_status == RISK_AVOID:
                    ann_map[code] = {
                        "announcement_risk": RISK_AVOID,
                        "risk_status": RISK_AVOID,
                        "announcement_keywords": cached.get("keywords") or [],
                        "announcement_titles": cached.get("titles") or [],
                        "announcement_check": "unavailable",
                        "announcement_risk_source": "last-known",
                    }
                else:
                    ann_map[code] = {
                        "announcement_risk": RISK_UNKNOWN,
                        "risk_status": RISK_UNKNOWN,
                        "announcement_keywords": [],
                        "announcement_titles": [],
                        "announcement_check": "unavailable",
                        "announcement_risk_source": "last-known" if cached_status else "none",
                        "last_known_risk": cached_status or "",
                    }
                report_progress(code, ann_map[code]["risk_status"], ann_map[code]["announcement_risk_source"])

    result["announcement_cached_count"] = len(codes) - len(pending_codes)
    result["announcement_requested_count"] = len(pending_codes)

    # Visible diagnostics share the checked status when their code is also a
    # visible candidate; otherwise they remain unknown without causing I/O.
    rows_to_update = list(rows)
    for row in result.get("trend_diagnostics") or []:
        if str(row.get("code")) in ann_map:
            rows_to_update.append(row)

    for row in rows_to_update:
        info = ann_map.get(str(row.get("code")), {
            "announcement_risk": RISK_UNKNOWN,
            "risk_status": RISK_UNKNOWN,
            "announcement_keywords": [],
            "announcement_titles": [],
            "announcement_check": "unavailable",
            "announcement_risk_source": "none",
        })
        row.update(info)
        status = _row_risk_status(row)
        if status == RISK_AVOID:
            row["risk"] = append_risk_text(str(row.get("risk") or "无"), "公告硬风险")
            if row.get("class") in ("A", "B"):
                row["class"] = "C"
        elif status == RISK_WATCH:
            row["risk"] = append_risk_text(str(row.get("risk") or "无"), "公告观察风险")
        elif status == RISK_UNKNOWN:
            row["risk"] = append_risk_text(str(row.get("risk") or "无"), "公告检查不可用")
    # 统一按代码索引的公告风险映射：所有模块（含状态机）从这里读取，
    # 不得各自重新查询或重新解析。缺失一律 unknown（fail-closed）。
    result["announcement_risk_map"] = {
        code: info.get("risk_status", RISK_UNKNOWN) for code, info in ann_map.items()
    }
    result["announcement_errors"] = sorted(set(errors))
    result["announcement_check_available"] = not errors
    result["announcement_unknown_codes"] = sorted({
        str(row.get("code"))
        for row in rows
        if _row_risk_status(row) == RISK_UNKNOWN
    })
    if risk_cache is None and cache_changed:
        _save_announcement_risk_cache(cache)
    return errors


def apply_announcement_pool_gates(result: Dict[str, Any]) -> None:
    """Apply fail-closed announcement gates after every screening run."""
    unknown_codes = {
        str(row.get("code"))
        for section in (
            "strict_ultra", "trend_observation", "strict_trend", "dual_pool", "dual_pool_raw",
            "pre_intersection", "capital_rank", "trend_diagnostics", "low_ultra", "low_trend", "watchlist",
        )
        for row in (result.get(section) or [])
        if _row_risk_status(row) == RISK_UNKNOWN
    }
    result["announcement_unknown_codes"] = sorted(
        set(result.get("announcement_unknown_codes") or []) | unknown_codes
    )
    if result.get("announcement_errors"):
        result["announcement_check_available"] = False

    # Keep the raw ultra pool visible for auditability. Unknown and avoid
    # statuses are not allowed to upgrade into a tradeable recommendation.
    result["trend_observation"] = [
        row for row in (result.get("trend_observation") or [])
        if _row_risk_status(row) != RISK_AVOID
    ]
    for section in ("strict_trend", "dual_pool", "capital_rank"):
        result[section] = [
            row for row in (result.get(section) or [])
            if _row_risk_status(row) not in {RISK_AVOID, RISK_UNKNOWN}
        ]
    for section in ("low_ultra", "low_trend"):
        for row in result.get(section) or []:
            status = _row_risk_status(row)
            if status == RISK_AVOID:
                row["risk"] = append_risk_text(str(row.get("risk") or "无"), "公告硬风险")
                row["class"] = "C"
            elif status == RISK_UNKNOWN:
                row["risk"] = append_risk_text(str(row.get("risk") or "无"), "公告检查不可用")
                row["class"] = "C"
    for row in result.get("trend_diagnostics") or []:
        status = _row_risk_status(row)
        if status == RISK_AVOID:
            row["upgrade_status"] = "公告avoid，禁止升级"
        elif status == RISK_UNKNOWN:
            row["upgrade_status"] = "公告unknown，数据不足，禁止升级"
