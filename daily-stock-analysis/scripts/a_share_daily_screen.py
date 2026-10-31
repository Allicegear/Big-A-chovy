#!/usr/bin/env python3
"""A-share daily screening helper for daily-stock-analysis.

This script fetches current A-share snapshots and daily K lines, then emits
structured Markdown or JSON for Codex/Hermes analysis. It is intentionally a
query and classification tool, not an auto-trading tool.
"""
from __future__ import annotations

import argparse
import concurrent.futures as futures
import csv
import json
import math
import os
import random
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

SCRIPT_DIR = Path(__file__).resolve().parent
# The screener is also run directly as a script.  Add the project root before
# importing the shared executable-parameter registry so both direct execution
# and test/module imports use the same configuration.
PROJECT_ROOT = SCRIPT_DIR.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from tools.rule_config import RULE_CONFIG, get_rule_config, hhmm_to_minutes  # noqa: E402
from tools.data_sources.announcements import (  # noqa: E402
    classify_announcement_risk as _canonical_announcement_risk,
    extract_primary_announcement_container,
    extract_primary_announcement_total,
    fetch_announcement_evidence,
    validate_primary_announcement_page,
)
from tools.data_sources.cninfo import CNInfoAnnouncementSource  # noqa: E402
from tools.data_sources.background import build_market_background  # noqa: E402
from tools.data_sources.http import project_http_client  # noqa: E402

if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
import tencent_kline  # noqa: E402
import runtime_paths  # noqa: E402  运行状态路径唯一来源（A_SHARE_STATE_DIR 可定向到临时目录）
import state_commit  # noqa: E402  回合状态提交门（超时轮不得提交运行状态）
import tls_context  # noqa: E402  TLS 校验上下文唯一来源（默认校验证书）

RISK_CONFIG = RULE_CONFIG["risk"]
SCREENING_CONFIG = RULE_CONFIG["screening"]
EXECUTION_CONFIG = RULE_CONFIG["execution"]
# 公告风险状态常量的唯一来源是 screening/risk.py
from screening.risk import (  # noqa: E402,F401
    RISK_AVOID,
    RISK_CHECKED_VALUES,
    RISK_CLEAN,
    RISK_STATUS_VALUES,
    RISK_STATUSES,
    RISK_UNKNOWN,
    RISK_WATCH,
)

TZ = ZoneInfo("Asia/Shanghai")
# 网络核心（会话、主机熔断、东财冷却、代理与网络模式、fetch_json）已拆到
# screening/net.py；这里重新导出，既有调用方式不变。注意 NETWORK_MODE 由
# set_network_mode 改写 net 模块里的值，门面上的同名属性只是导入时的副本。
import network_path  # noqa: E402,F401  与 screening.net 中是同一个模块对象
from screening.net import (  # noqa: E402,F401
    NETWORK_MODE,
    REQUESTS_DIRECT_SESSION,
    REQUESTS_SESSION,
    SSL_CONTEXT,
    UA,
    NetworkUnavailable,
    _EM_COOLDOWN_BACKOFF,
    _EM_COOLDOWN_FILE,
    _HOST_COOLDOWN,
    _HOST_FAILURES,
    _em_clear_cooldown,
    _em_in_cooldown,
    _em_read_cooldown,
    _em_set_cooldown,
    _mark_host_failed,
    _mark_host_ok,
    _rank_urls,
    _system_proxy_url,
    build_url_opener,
    fetch_json,
    format_network_failure,
    requests,
    set_network_mode,
)

# 当日 1 分钟累计资金流序列。本地快照够不着 5/15 分钟基准时用它兜底，
# 只走 push2 系主机的 /webguest 路由（push2his 无 /webguest，standards 路由已下线）。
EM_FFLOW_MINUTE_URLS = [
    "https://push2.eastmoney.com/webguest/api/qt/stock/fflow/kline/get",
    "https://82.push2.eastmoney.com/webguest/api/qt/stock/fflow/kline/get",
]
# Historical daily capital flow is served by Eastmoney's daykline endpoint.
# Unlike the webguest intraday endpoint, push2his returns the date series.
EM_FFLOW_DAILY_URLS = [
    "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get",
    "https://1.push2his.eastmoney.com/api/qt/stock/fflow/daykline/get",
    "https://82.push2his.eastmoney.com/api/qt/stock/fflow/daykline/get",
]
LOW_OPEN_DAILY_FLOW_SESSIONS = 20
LOW_OPEN_DAILY_FLOW_MAX_CODES_PER_ROUND = 20
LOW_OPEN_DAILY_FLOW_CACHE_SECONDS = 12 * 60 * 60

DECISION_CASH = "cash"
DECISION_OBSERVE = "observe"
DECISION_LOW_ABSORB = "low_absorb_only"
DECISION_TRIAL = "trial_entry_allowed"
DECISION_AVOID = RISK_AVOID

TIER_STRICT = "strict"
TIER_HOPEFUL = "hopeful"
TIER_AVOID = RISK_AVOID

# 硬黑名单：框架明确记载的死亡螺旋/股灾级教训案例，无论资金面如何都不进入候选
# 新增教训需附带日期和根因，避免黑名单无限膨胀
HARD_BLACKLIST: Dict[str, str] = dict(RISK_CONFIG["hard_blacklist"])

# 软降权规则：以下条件任一满足 → 从低吸候选剔除（不标avoid，直接不出现）
# 1. 20日累计涨幅 ≥30% 且 近5日主力净占比持续<0（高位派发信号）
# 2. 硬黑名单命中
# 本轮警告列表与各数据源模块共用同一对象，定义在 screening/market_warnings.py
from screening.market_warnings import MARKET_WARNINGS  # noqa: E402,F401


# 数据模型、数值格式化与交易板分类已拆到 screening/model.py；这里重新导出，
# ``screen.Enriched`` / ``screen.board_of`` 等既有调用方式不变。
from screening.model import (  # noqa: E402,F401
    BOARD_CHINEXT,
    BOARD_LABELS,
    BOARD_MAIN,
    BOARD_ORDER,
    BOARD_STAR,
    BOARD_UNKNOWN,
    Enriched,
    _flow_number,
    _fmt,
    amount_yi,
    avg,
    board_of,
    current_status,
    is_a_share_row,
    is_number,
    normalize_boards,
    normalize_row,
    pct,
    secid_for,
    tencent_code,
    valid_board_row,
    valid_main_board,
)


# 行情快照、大盘指数与行业板块取数已拆到 screening/sources/market.py；这里重新导出。
# MARKET_FETCH_STATUS / SECTOR_BOARDS_STATUS 是同一个字典对象（只原地更新）。
from screening.sources.market import (  # noqa: E402,F401
    CLIST_FIELDS,
    CLIST_STARTUP_URLS,
    CLIST_URLS,
    EASTMONEY_UT,
    INDEX_SECIDS,
    INDEX_URLS,
    MARKET_FETCH_STATUS,
    SECTOR_BOARDS_STATUS,
    SECTOR_BOARD_FIELDS,
    SECTOR_BOARD_MAX_PAGES,
    SECTOR_BOARD_PAGE_SIZE,
    SECTOR_BOARD_SORT_FIELD,
    SINA_MARKET_URL,
    SINA_MAX_PAGES,
    SINA_PAGE_SIZE,
    _fetch_sector_page,
    _normalize_sina_row,
    _sector_board_params,
    _sina_number,
    fetch_indices,
    fetch_market,
    fetch_sector_indices,
    fetch_sina_market,
    get_market_fetch_status,
)


# 公告抓取、风险缓存、风险挂载与池门禁已拆到 screening/sources/announcements.py；
# 这里重新导出。进程内缓存 _ANNOUNCEMENT_RISK_CACHE（global 重新绑定）不导出。
from screening.sources.announcements import (  # noqa: E402,F401
    ANNOUNCEMENT_CACHE_FILE,
    ANNOUNCEMENT_CACHE_TTL_SECONDS,
    ANNOUNCEMENT_CONFIG,
    ANNOUNCEMENT_IGNORE_KEYWORDS,
    ANNOUNCEMENT_REQUEST_TIMEOUT_SECONDS,
    ANNOUNCEMENT_SECTIONS,
    ANNOUNCEMENT_URL,
    HARD_ANNOUNCEMENT_KEYWORDS,
    WATCH_ANNOUNCEMENT_KEYWORDS,
    _announcement_business_failure,
    _announcement_container,
    _announcement_row_title,
    _announcement_total,
    _announcement_total_from_response,
    _has_announcement_container,
    _load_announcement_risk_cache,
    _save_announcement_risk_cache,
    _validate_announcement_page_count,
    _validate_primary_announcement_response,
    apply_announcement_pool_gates,
    attach_announcement_risks,
    classify_announcement_risk,
    collect_announcement_titles,
    fetch_announcements,
)


# 公告风险政策（_row_risk_status、一票否决判定、风险标注）已拆到 screening/risk.py；
# 这里重新导出，既有调用方式不变。
from screening.risk import (  # noqa: E402,F401
    RISK_HARD_VETO,
    _row_risk_status,
    announcement_label,
    append_risk_text,
    risk_allows_state_qualification,
    risk_blocks_formal_qualification,
    risk_note_text,
)


# 日 K 取数链与 enrich / enrich_all 已拆到 screening/sources/kline.py；这里重新导出。
# 腾讯熔断计数等 global 标量不导出（门面上只会是导入时的旧副本），请用 kline 模块属性。
from screening.sources.kline import (  # noqa: E402,F401
    EM_KLINE_FIELDS1,
    EM_KLINE_FIELDS2,
    EM_KLINE_URL,
    EM_KLINE_URLS,
    KLINE_MIN_BARS,
    KLINE_QFQ_DEVIATION_LIMIT,
    KLINE_SOURCE_LABELS,
    SINA_KLINE_URL,
    TENCENT_KLINE_COOLDOWN_SECONDS,
    TENCENT_KLINE_FAIL_STREAK_LIMIT,
    TENCENT_KLINE_URL,
    TENCENT_KLINE_URLS,
    _kline_fallback_codes,
    _kline_mismatch_codes,
    _kline_reference,
    _note_sina_fallback_code,
    _note_sina_kline_fallback,
    _note_tencent_kline_result,
    _parse_sina_kline,
    _parse_tencent_qfq_kline,
    _record_kline_reference,
    _tencent_kline_ready,
    enrich,
    enrich_all,
    fetch_kline,
    kline_source_summary,
    parse_k_rows,
)


# 候选池规则（共振、严格超短/趋势、趋势观察、低吸分级与排序）已拆到
# screening/rules.py；这里重新导出，既有调用方式不变。
from screening.rules import (  # noqa: E402,F401
    _trend_quality_checks,
    has_resonance,
    is_after_tail_risk,
    low_trend_class,
    low_trend_output_eligible,
    low_trend_sort_key,
    low_ultra_class,
    low_ultra_output_eligible,
    low_ultra_sort_key,
    sector_stats,
    strict_trend,
    strict_ultra,
    trend_condition_diagnosis,
    trend_confirm_relaxed,
    trend_observation,
)


def _should_exclude_from_low_absorb(
    e: "Enriched", flow_history: Dict[str, List[Dict[str, Any]]]
) -> Tuple[bool, str]:
    """框架教训落地：硬黑名单 + 高位派发降权。返回 (是否排除, 原因)。

    进入筛选轮时（round_series_date 已设置）快照不足会用东财分钟序列核验；否则只按快照判断。
    """
    exclusion_cfg = RISK_CONFIG["low_absorb_exclusion"]
    # 硬黑名单：框架明确记载的教训案例
    if e.code in HARD_BLACKLIST:
        return True, HARD_BLACKLIST[e.code]
    # 软降权：5日累计涨幅≥12% + 近3+次快照主力净占比持续≤0 → 高位派发信号
    if (
        e.five_ret >= float(exclusion_cfg["five_day_return_min_ratio"])
        and e.main_pct <= float(exclusion_cfg["main_pct_max_inclusive"])
    ):
        lookback = int(exclusion_cfg["history_lookback_snapshots"])
        need = int(exclusion_cfg["negative_main_snapshots_min"])
        hist = (flow_history or {}).get(e.code) or []
        if len(hist) >= lookback:
            neg_count = sum(1 for h in hist[-lookback:] if h.get("main_net", 0) <= 0)
        else:
            # 2026-09-26：快照不足时用分钟序列等间隔采样核验（口径对齐：数累计值是否为负）；
            # 仍无法核验则只记数，不改变"不排除"的既有行为，并在报告里提示待核验。
            neg_count = None
            date = round_series_date()
            if date:
                neg_count = flow_negative_samples_from_minutes(
                    _flow_minute_series_for(e.code, date), lookback
                )
            if neg_count is None:
                _note_low_absorb_unverified()
                return False, ""
        if neg_count >= need:
            return True, (
                f"高位派发降权：5日涨{e.five_ret*100:.1f}%+"
                f"近{lookback}期{neg_count}期主力净额≤0"
            )
    return False, ""


# ── 低开洗盘模块（低开≥2% + 高于昨收 + 均价线上 + 当日/20日主力净流入为正）──
def fetch_daily_main_flow_history(
    code: str, expected_date: str, *, allow_request: bool = True
) -> Dict[str, Any]:
    """Fetch 20 completed daily main-flow bars preceding the current market date.

    Intraday cumulative snapshots are deliberately not accepted here. The
    current session is checked separately from ``e.main_net``.
    """
    code = str(code or "")
    expected_date = str(expected_date or "")[:10]
    try:
        datetime.strptime(expected_date, "%Y-%m-%d")
    except ValueError:
        return {"status": "unavailable", "sessions": 0, "persistent_net": None}
    cache_key = (code, expected_date)
    now = time.time()
    with _LOW_OPEN_DAILY_FLOW_CACHE_LOCK:
        cached = _LOW_OPEN_DAILY_FLOW_CACHE.get(cache_key)
        if cached:
            ttl = LOW_OPEN_DAILY_FLOW_CACHE_SECONDS if cached[1].get("status") == "complete" else 120
            if now - cached[0] <= ttl:
                return {**cached[1], "_request_made": False}
    if not allow_request:
        return {"status": "unavailable", "sessions": 0, "persistent_net": None,
                "reason": "本轮历史资金查询额度已用完", "_request_made": False}

    params = {
        "secid": secid_for(code),
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f56",
        "klt": "101",
        "lmt": str(LOW_OPEN_DAILY_FLOW_SESSIONS + 20),
        "beg": "0",
        "end": "20500101",
    }
    last_error: Optional[Exception] = None
    best_result: Optional[Dict[str, Any]] = None
    for url in _rank_urls(EM_FFLOW_DAILY_URLS):
        _pace_flow_minute_request()
        try:
            payload = fetch_json(url, params, timeout=6)
        except Exception as exc:
            last_error = exc
            _mark_host_failed(url)
            continue
        data = payload.get("data") if isinstance(payload, dict) else None
        rows = data.get("klines") if isinstance(data, dict) else None
        if not isinstance(rows, list):
            rows = []
        by_date: Dict[str, float] = {}
        for line in rows:
            parts = str(line).split(",")
            if len(parts) < 2:
                continue
            bar_date = parts[0].strip()[:10]
            try:
                datetime.strptime(bar_date, "%Y-%m-%d")
                main_net = float(parts[1])
            except (TypeError, ValueError):
                continue
            if bar_date < expected_date and math.isfinite(main_net):
                by_date[bar_date] = main_net
        latest = sorted(by_date.items())[-LOW_OPEN_DAILY_FLOW_SESSIONS:]
        result: Dict[str, Any] = {
            "status": "complete" if len(latest) == LOW_OPEN_DAILY_FLOW_SESSIONS else "insufficient",
            "sessions": len(latest),
            "persistent_net": round(sum(value for _, value in latest), 2)
            if len(latest) == LOW_OPEN_DAILY_FLOW_SESSIONS else None,
            "first_date": latest[0][0] if latest else None,
            "last_date": latest[-1][0] if latest else None,
            "source": "eastmoney_push2his_daykline",
        }
        _mark_host_ok(url)
        if result["status"] == "complete":
            best_result = result
            break
        if best_result is None or result["sessions"] > best_result["sessions"]:
            best_result = result

    if best_result is None:
        best_result = {
            "status": "unavailable", "sessions": 0, "persistent_net": None,
            "reason": str(last_error) if last_error else "历史资金接口无有效数据",
        }
    with _LOW_OPEN_DAILY_FLOW_CACHE_LOCK:
        _LOW_OPEN_DAILY_FLOW_CACHE[cache_key] = (now, dict(best_result))
    return {**best_result, "_request_made": True}


def _low_open_wash_current_conditions(e: "Enriched") -> bool:
    """Check low-open, red versus previous close, VWAP, and current-day flow."""
    if e is None or not all(
        is_number(value) and math.isfinite(float(value))
        for value in (e.open, e.prev_close, e.price, e.main_net)
    ):
        return False
    wash_cfg = SCREENING_CONFIG["low_open_wash"]
    return bool(
        e.prev_close > 0
        and e.open <= e.prev_close * float(wash_cfg["open_max_prev_close_multiplier"])
        and e.price > e.prev_close
        and e.price_above_vwap
        and e.main_net > 0
    )


def _qualifies_low_open_wash(e: "Enriched", daily_flow: Optional[Dict[str, Any]]) -> bool:
    """Require complete 20-session historical evidence; no intraday fallback."""
    return bool(
        _low_open_wash_current_conditions(e)
        and daily_flow
        and daily_flow.get("status") == "complete"
        and is_number(daily_flow.get("persistent_net"))
        and float(daily_flow["persistent_net"]) > 0
    )


def low_open_wash_rows(
    enriched: List["Enriched"], expected_date: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Return qualified and pending low-open observations with honest 20-day status."""
    out: List[Dict[str, Any]] = []
    requested_codes: set[str] = set()
    market_date = str(expected_date or "")[:10]
    for e in enriched:
        if not _low_open_wash_current_conditions(e):
            continue
        asof = market_date or str(getattr(e, "kdate", "") or "")[:10]
        can_request = e.code in requested_codes or len(requested_codes) < LOW_OPEN_DAILY_FLOW_MAX_CODES_PER_ROUND
        daily_flow = fetch_daily_main_flow_history(e.code, asof, allow_request=can_request)
        if daily_flow.get("_request_made"):
            requested_codes.add(e.code)
        if daily_flow.get("status") == "complete" and not _qualifies_low_open_wash(e, daily_flow):
            continue

        d = dict(asdict(e))
        d["low_open_pct"] = round((e.open - e.prev_close) / e.prev_close * 100, 2)
        d["persistent_net"] = daily_flow.get("persistent_net")
        d["persistent_flow_sessions"] = daily_flow.get("sessions", 0)
        d["persistent_flow_status"] = (
            "20日累计为正，条件通过" if _qualifies_low_open_wash(e, daily_flow)
            else "20日历史数据不足，未通过"
            if daily_flow.get("status") == "insufficient"
            else "20日历史数据不可用，未通过"
        )
        d["wash_qualified"] = _qualifies_low_open_wash(e, daily_flow)
        d["persistent_flow_asof"] = daily_flow.get("last_date")
        out.append(d)
    return out


def market_summary(
    rows: List[Dict[str, Any]],
    provider_total: Optional[int] = None,
    fetch_status: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Summarise A-share breadth without treating missing fields as flat."""
    a_share_rows = [r for r in rows if is_a_share_row(r)]
    valid_change = [r for r in a_share_rows if is_number(r.get("f3"))]
    invalid_change = len(a_share_rows) - len(valid_change)
    main_board_prefixes = tuple(str(prefix) for prefix in SCREENING_CONFIG["resonance"]["main_board_prefixes"])
    main = [r for r in valid_change if str(r.get("f12", "")).startswith(main_board_prefixes)]
    adv = sum(1 for r in valid_change if r["f3"] > 0)
    dec = sum(1 for r in valid_change if r["f3"] < 0)
    flat = sum(1 for r in valid_change if r["f3"] == 0)
    provider_complete = (
        bool(fetch_status.get("complete"))
        if fetch_status and fetch_status.get("complete") is not None
        else provider_total is None or provider_total <= len(rows)
    )
    quality_reasons: List[str] = []
    fallback_source = fetch_status.get("source") == "sina_fallback" if fetch_status else False
    if fallback_source:
        quality_reasons.append("备用行情未提供可校验总数、兼容量比、主力资金和行业字段")
    elif fetch_status and fetch_status.get("complete") is False:
        quality_reasons.append(
            "行情全量快照不完整"
            f"（{fetch_status.get('received_pages', 0)}/{fetch_status.get('expected_pages', '?')}页）"
        )
    if not provider_complete and not fallback_source:
        quality_reasons.append(f"服务端总数{provider_total}，本次仅取得{len(rows)}条")
    snapshot_cfg = SCREENING_CONFIG["market_snapshot"]
    if (
        a_share_rows
        and invalid_change / len(a_share_rows) > float(snapshot_cfg["invalid_change_ratio_max_exclusive"])
    ):
        quality_reasons.append(
            f"A股涨跌幅缺失{invalid_change}条，超过{float(snapshot_cfg['invalid_change_ratio_max_exclusive'])*100:g}%"
        )
    if valid_change and dec == 0:
        quality_reasons.append("下跌数为0，疑似仅取得涨幅排序的部分样本")
    degraded = bool(quality_reasons)
    return {
        "adv": adv,
        "dec": dec,
        "flat": flat,
        "total_rows": len(a_share_rows),
        "raw_total_rows": len(rows),
        "provider_total": provider_total,
        "valid_change": len(valid_change),
        "invalid_change": invalid_change,
        "breadth": adv / len(valid_change) if valid_change else None,
        "main_limit_up": sum(1 for r in main if r["f3"] >= float(snapshot_cfg["limit_up_change_min_inclusive"])),
        "main_limit_down": sum(1 for r in main if r["f3"] <= float(snapshot_cfg["limit_down_change_max_inclusive"])),
        "degraded": degraded,
        "resonance_usable": not degraded,
        "quality_reason": "；".join(quality_reasons),
    }


def filter_prefetch(
    rows: List[Dict[str, Any]],
    modes: List[str],
    boards: Any = (BOARD_MAIN,),
) -> List[Dict[str, Any]]:
    """按模块与交易板预筛候选。

    ``boards`` 默认只含沪深主板，因此默认输出与历史基线逐行一致。所选的创业板／
    科创板行与主板行**同样进入正式链路**（资金增量与状态 → 超短池／趋势池 →
    双池交集 → 资金优选／低吸／明日观察池 → 状态机），使用同一套门槛与排名：
    本文只负责按范围放行，不放宽也不收紧任何策略条件。未选的交易板在这里就被
    挡掉，正式池因此不会混入未选交易板的股票。
    """
    ultra_cfg = SCREENING_CONFIG["strict_ultra"]
    trend_cfg = SCREENING_CONFIG["strict_trend"]
    observation_cfg = SCREENING_CONFIG["trend_observation"]
    valid = [r for r in rows if valid_board_row(r, boards)]
    has_all = "all" in modes
    has_strict = has_all or "strict" in modes
    has_low = has_all or "low" in modes
    has_wl = has_all or "watchlist" in modes
    selected = []
    for r in valid:
        price = r["f2"]
        amount = r["f6"]
        chg = r["f3"]
        if has_strict:
            ultra_prefilter = (
                float(ultra_cfg["price_min_inclusive"]) <= price <= float(ultra_cfg["price_max_inclusive"])
                and r["f21"] < float(ultra_cfg["float_mv_max_exclusive"])
                and r["f8"] > float(ultra_cfg["turnover_min_exclusive"])
                and is_number(r.get("f10")) and r["f10"] > float(ultra_cfg["volume_ratio_min_exclusive"])
                and amount > float(ultra_cfg["amount_min_exclusive"])
                and float(ultra_cfg["change_min_inclusive"]) <= chg <= float(ultra_cfg["change_max_inclusive"])
            )
            confirmation_prefilter = (
                float(trend_cfg["price_min_inclusive"]) <= price <= float(trend_cfg["price_max_inclusive"])
                and float(trend_cfg["float_mv_min_inclusive"]) <= r["f21"] <= float(trend_cfg["float_mv_max_inclusive"])
                and float(trend_cfg["turnover_min_inclusive"]) <= r["f8"] <= float(trend_cfg["turnover_max_inclusive"])
                and float(trend_cfg["change_min_inclusive"]) <= chg <= float(trend_cfg["change_max_inclusive"])
            )
            observation_prefilter = (
                float(observation_cfg["price_min_inclusive"]) <= price <= float(observation_cfg["price_max_inclusive"])
                and float(observation_cfg["float_mv_min_inclusive"]) <= r["f21"] <= float(observation_cfg["float_mv_max_inclusive"])
                and amount > float(observation_cfg["amount_min_exclusive"])
                and float(observation_cfg["turnover_min_inclusive"]) <= r["f8"] <= float(observation_cfg["turnover_max_inclusive"])
                and float(observation_cfg["change_min_inclusive"]) <= chg <= float(observation_cfg["change_max_inclusive"])
            )
            if ultra_prefilter or confirmation_prefilter or observation_prefilter:
                selected.append(r)
                continue
        if has_low or has_wl:
            if (
                float(ultra_cfg["price_min_inclusive"]) <= price <= float(SCREENING_CONFIG["watchlist"]["price_max_inclusive"])
                and amount > float(ultra_cfg["amount_min_exclusive"])
                and float(SCREENING_CONFIG["low_absorb"]["prefetch_change_min_inclusive"]) <= chg
                <= float(SCREENING_CONFIG["low_absorb"]["prefetch_change_max_inclusive"])
            ):
                selected.append(r)
    dedup = {r["f12"]: r for r in selected}
    return list(dedup.values())


# ── snapshot & flow classification ──────────────────────────────────────

FLOW_SNAPSHOT_PATH = runtime_paths.state_file("flow_snapshot.json")


def load_flow_history() -> Dict[str, List[Dict[str, Any]]]:
    """Load flow snapshot history. Returns {code: [{main_net, ..., ts}, ...]}."""
    try:
        if FLOW_SNAPSHOT_PATH.exists():
            data = json.loads(FLOW_SNAPSHOT_PATH.read_text(encoding="utf-8"))
            # Support legacy single-snapshot format: convert to list
            if data and not isinstance(next(iter(data.values()), None), list):
                converted = {}
                for code, entry in data.items():
                    converted[code] = [entry]
                return converted
            return data
    except Exception:
        pass
    return {}


def save_flow_history(
    enriched: List[Enriched],
    history: Dict[str, List[Dict[str, Any]]],
    commit: Optional[state_commit.RoundCommit] = None,
) -> None:
    """Save current query's capital flow data, appending to history. Keep last 30 minutes.

    ``commit`` 非空时只暂存（超时轮会被丢弃），为空时立即原子落盘（CLI/单测行为）。
    """
    import time as _t
    now = _t.time()
    flow_cfg = SCREENING_CONFIG["flow"]
    cutoff = now - float(flow_cfg["history_retention_seconds"])
    for e in enriched:
        if e.code not in history:
            history[e.code] = []
        history[e.code].append({
            "main_net": e.main_net,
            "super_net": e.super_net,
            "big_net": e.big_net,
            "small_net": e.small_net,
            "amount": e.amount,
            "volume": e.volume,
            "ts": now,
        })
        # Prune entries older than 30 minutes
        history[e.code] = [h for h in history[e.code] if h.get("ts", 0) > cutoff]
        # Also remove the placeholder entry we added at the start
    # Remove codes with no valid entries
    history = {k: v for k, v in history.items() if v}
    state_commit.write_or_stage(
        commit, FLOW_SNAPSHOT_PATH, json.dumps(history, ensure_ascii=False)
    )


def apply_flow_increments(enriched: List[Enriched], history: Dict[str, List[Dict[str, Any]]]) -> None:
    """Calculate 5-min and 15-min main force increments from historical snapshots.

    5-min increment: current main_net minus the snapshot entry closest to ~5 minutes ago.
    15-min increment: current main_net minus the snapshot entry closest to ~15 minutes ago.
    If no suitable baseline is found, set to NaN (displayed as '基准不足').
    """
    import time as _t
    now = _t.time()
    flow_cfg = SCREENING_CONFIG["flow"]
    baseline_5m_cfg = flow_cfg["baseline_5m"]
    baseline_15m_cfg = flow_cfg["baseline_15m"]
    for e in enriched:
        e.flow_baseline_source = ""
        entries = history.get(e.code, [])
        if not entries:
            e.flow_5m_inc = float("nan")
            e.flow_15m_inc = float("nan")
            # 无快照历史的行也要给出同口径代理值，避免沿用 enrich 阶段的临时值
            e.buy_ratio = compute_buy_ratio(e, flow_cfg)
            continue

        # Find best baseline for 5-min window (3–7 minutes ago, prefer closest to 5min)
        baseline_5m = None
        best_diff_5m = 999999
        for h in entries:
            age = now - h.get("ts", 0)
            if float(baseline_5m_cfg["min_age_seconds"]) <= age <= float(baseline_5m_cfg["max_age_seconds"]):
                diff = abs(age - float(baseline_5m_cfg["target_age_seconds"]))
                if diff < best_diff_5m:
                    best_diff_5m = diff
                    baseline_5m = h

        # Find best baseline for 15-min window (10–20 minutes ago, prefer closest to 15min)
        baseline_15m = None
        best_diff_15m = 999999
        for h in entries:
            age = now - h.get("ts", 0)
            if float(baseline_15m_cfg["min_age_seconds"]) <= age <= float(baseline_15m_cfg["max_age_seconds"]):
                diff = abs(age - float(baseline_15m_cfg["target_age_seconds"]))
                if diff < best_diff_15m:
                    best_diff_15m = diff
                    baseline_15m = h

        if baseline_5m:
            e.flow_5m_inc = e.main_net - baseline_5m.get("main_net", 0)
        else:
            e.flow_5m_inc = float("nan")

        if baseline_15m:
            e.flow_15m_inc = e.main_net - baseline_15m.get("main_net", 0)
        else:
            e.flow_15m_inc = float("nan")

        if baseline_5m or baseline_15m:
            e.flow_baseline_source = "snapshot"

        # 主买比已在循环开头统一计算；此处保留顺序注释：
        # ── 量能快照差分（与"5分钟主力资金增量"同源，零额外查询）──
        # 当前累计额/量 减 历史快照 = 间隔增量；相对过去 N 根均值 = 量能倍数
        sorted_entries = sorted(entries, key=lambda h: h.get("ts", 0))
        latest_hist = sorted_entries[-1]
        recent_amt_delta = e.amount - latest_hist.get("amount", 0)
        recent_vol_delta = e.volume - latest_hist.get("volume", 0)

        # 5分钟额/量增量：取 ~5分钟前的基线快照
        base_vol = None
        best_d = 999999
        for h in sorted_entries:
            age = now - h.get("ts", 0)
            if float(baseline_5m_cfg["min_age_seconds"]) <= age <= float(baseline_5m_cfg["max_age_seconds"]):
                d = abs(age - float(baseline_5m_cfg["target_age_seconds"]))
                if d < best_d:
                    best_d = d
                    base_vol = h
        if base_vol:
            e.amount_5m_inc = e.amount - base_vol.get("amount", 0)
            e.volume_5m_inc = e.volume - base_vol.get("volume", 0)
        else:
            e.amount_5m_inc = float("nan")
            e.volume_5m_inc = float("nan")

        # 相对过去 N 根均量倍数 = 最近一根间隔额增量 / 过去 N 根间隔额增量均值
        deltas = []
        for i in range(1, len(sorted_entries)):
            d = sorted_entries[i].get("amount", 0) - sorted_entries[i - 1].get("amount", 0)
            if d > 0:
                deltas.append(d)
        if deltas and recent_amt_delta > 0:
            N = min(int(flow_cfg["history_delta_lookback"]), len(deltas))
            past = deltas[-N:]
            past_avg = sum(past) / len(past)
            e.vol_ratio_vs_hist = (recent_amt_delta / past_avg) if past_avg > 0 else float("nan")
        else:
            e.vol_ratio_vs_hist = float("nan")

        e.vol_surge = (
            not (isinstance(e.vol_ratio_vs_hist, float) and math.isnan(e.vol_ratio_vs_hist))
            and e.vol_ratio_vs_hist >= float(flow_cfg["volume_surge_ratio_min"])
            and recent_amt_delta > 0
        )

        # ── 主买比（代理值）：由主力净占比与 5 分钟增量推导，非真实分笔；缺成交额时为 NaN ──
        e.buy_ratio = compute_buy_ratio(e, flow_cfg)


def compute_buy_ratio(e: Enriched, flow_cfg: Dict[str, Any]) -> float:
    """盘口推升代理值：由主力净占比与 5 分钟增量推导，不是真实分笔统计。

    框架核验项要求“分笔连续主动买入”；此处只给出可排序的代理值，
    真实分笔（stock/details/get）成功接入前，不得把该值当作分笔证据。
    成交额或主力净额缺失时返回 NaN（未知），交由门槛拒绝，不做缺数据放行——
    2026-09-26 修正：原实现在 amount<=0 时按主力净额正负硬编码返回 1.6/0.8，
    其中 1.6 可直接满足 >=1.5 的主买比硬门槛，与“严禁缺数据放行”冲突。
    """
    if not (is_number(e.main_net) and e.amount > 0):
        return float("nan")
    denominator_floor = float(flow_cfg["buy_ratio_denominator_floor"])
    surge_cap = float(flow_cfg["buy_ratio_surge_cap"])
    surge_scale = float(flow_cfg["buy_ratio_surge_scale"])
    base_ratio = (100.0 + e.main_pct) / max(denominator_floor, 100.0 - e.main_pct)
    if e.super_net > 0 and e.big_net > 0 and is_number(e.flow_5m_inc) and e.flow_5m_inc > 0:
        surge_factor = min(surge_cap, (e.flow_5m_inc / e.amount) * surge_scale)
        return round(base_ratio + surge_factor, 2)
    return round(base_ratio, 2)


# ── 分钟资金流兜底基准 ────────────────────────────────────────────────
# 本地快照差分零额外请求，但需要同一天已经跑过至少两轮才有基准：冷启动、断点重启、
# 隔日首轮都会让 5/15 分钟增量变成“基准不足”，而这两个值是 A/B 类资金门槛的判定项，
# 缺失时门槛无法评估。兜底方案是直接拉东财当日 1 分钟累计资金流序列（一次请求 240 根），
# 用序列两个端点作差得到增量。请求有界：每轮上限 + 最小间隔 + 短 TTL 缓存。
FLOW_MINUTE_TTL_SECONDS = 60.0         # 同一标的序列的缓存时长
FLOW_MINUTE_MIN_INTERVAL_SECONDS = 0.4  # 相邻请求最小间隔，避免突发
FLOW_MINUTE_MAX_CODES_PER_ROUND = 20    # 每轮最多为多少只补 5/15 分钟基准
# 安全核验（高位派发、coalition 连续性）专用额度：不与基准补齐抢名额，
# 否则补齐按主力净额取前 20 只后，安全类判定会全部退回"无法核验"。
FLOW_MINUTE_SAFETY_RESERVE = 5
_FLOW_MINUTE_CACHE: Dict[str, Any] = {}
_LOW_OPEN_DAILY_FLOW_CACHE: Dict[Tuple[str, str], Tuple[float, Dict[str, Any]]] = {}
_LOW_OPEN_DAILY_FLOW_CACHE_LOCK = threading.Lock()
_FLOW_MINUTE_LOCK = threading.Lock()
_FLOW_MINUTE_LAST_REQUEST_AT = 0.0
# 每轮的请求预算：由 fetch_flow_minutes 统一扣减，新增调用点不会绕过上限
_FLOW_MINUTE_ROUND_CODES: set = set()
_FLOW_MINUTE_ROUND_LOCK = threading.Lock()
_FLOW_MINUTE_ROUND_EXPECTED_DATE: Optional[str] = None


def reset_flow_minute_round(expected_date: Optional[str] = None) -> None:
    """每轮筛选开始前调用：重置分钟序列请求预算与"无法核验"计数（已取到的缓存保留）。

    expected_date 为本轮行情快照交易日，同时作为"允许用分钟序列兜底核验"的开关：
    未进入筛选轮的调用（含单测）保持 None，不会隐式联网。
    """
    global _LOW_ABSORB_UNVERIFIED_COUNT, _FLOW_MINUTE_ROUND_EXPECTED_DATE
    with _FLOW_MINUTE_ROUND_LOCK:
        _FLOW_MINUTE_ROUND_CODES.clear()
    _LOW_ABSORB_UNVERIFIED_COUNT = 0
    _FLOW_MINUTE_ROUND_EXPECTED_DATE = expected_date


def round_series_date() -> Optional[str]:
    """本轮行情快照交易日；None 表示未进入筛选轮（禁用分钟序列兜底）。"""
    return _FLOW_MINUTE_ROUND_EXPECTED_DATE


_LOW_ABSORB_UNVERIFIED_COUNT = 0


def _note_low_absorb_unverified() -> None:
    global _LOW_ABSORB_UNVERIFIED_COUNT
    _LOW_ABSORB_UNVERIFIED_COUNT += 1


def low_absorb_unverified_count() -> int:
    """本轮满足高位条件、但既无快照又无分钟序列、无法核验派发信号的只数。"""
    return _LOW_ABSORB_UNVERIFIED_COUNT


def _flow_minute_budget_ok(code: str) -> bool:
    """本轮是否还能为这只股票请求分钟序列。已在缓存或本轮请求过的直接放行。

    总上限 = 基准补齐额度 + 安全核验保留额度；补齐函数自身已按基准额度切片，
    因此这里放宽的额度实际留给低位吸高位派发/coalition 连续性这类核验。
    """
    with _FLOW_MINUTE_ROUND_LOCK:
        if code in _FLOW_MINUTE_ROUND_CODES or code in _FLOW_MINUTE_CACHE:
            return True
        if len(_FLOW_MINUTE_ROUND_CODES) >= (
            FLOW_MINUTE_MAX_CODES_PER_ROUND + FLOW_MINUTE_SAFETY_RESERVE
        ):
            return False
        _FLOW_MINUTE_ROUND_CODES.add(code)
        return True


def _contiguous_tail_start(series: List[Tuple[str, ...]]) -> int:
    """返回最后一段"每分钟连续"的起点索引。

    行情分钟序列在午休（11:30 → 13:01）和接口缺 bar 处会出现时间缺口。按记录条数取窗口
    （如 series[-6]）会跨越缺口：2026-09-26 实测在 13:03 计算时 [-6] 取到 11:28，
    把约 95 分钟的累计变化当成"5 分钟增量"。因此所有窗口计算只在最后一段连续分钟里进行，
    缺口之前的数据一律不参与；样本不足就返回"无法核验"，不猜。
    """
    if not series:
        return 0
    start = len(series) - 1
    while start > 0:
        prev = _minutes_of_hhmm(series[start - 1][0])
        cur = _minutes_of_hhmm(series[start][0])
        if prev is None or cur is None or cur - prev != 1:
            break
        start -= 1
    return start


def _column_values(tail: List[Tuple[str, ...]], column: int) -> List[Optional[float]]:
    """按时间位置取出某一列的取值，缺值保留为 None。

    2026-09-26 修正：不能先过滤掉非数值再按条数取窗口——某根 bar 该列缺值时会压缩位置，
    让相隔 6 分钟的端点被当成 5 分钟增量。缺值必须占位，让用到它的窗口自己判定不可用。
    """
    out: List[Optional[float]] = []
    for row in tail:
        value = row[column] if len(row) > column else None
        out.append(float(value) if is_number(value) else None)
    return out


def flow_window_deltas(
    series: List[Tuple[str, float, float]], window: int = 5, column: int = 1
) -> List[float]:
    """把当日累计序列切成每 window 分钟的增量序列；越靠后越新。样本不足返回空列表。

    只在最后一段连续分钟里计算（见 `_contiguous_tail_start`），午休/缺 bar 不跨越。
    窗口端点若缺值，该窗口记 NaN（不可用）；中间某根缺值不影响跨度差——
    端点是各自独立取到的累计值，时间跨度已由连续性检查保证。
    column: 1=主力净额，2=超大单净额（对应 f52 / f56）。
    """
    tail = series[_contiguous_tail_start(series):]
    values = _column_values(tail, column)
    if len(values) <= window:
        return []
    deltas: List[float] = []
    for i in range(window, len(values)):
        current, base = values[i], values[i - window]
        if current is None or base is None:
            deltas.append(float("nan"))
        else:
            deltas.append(current - base)
    return deltas


def flow_continuity_from_minutes(
    series: List[Tuple[str, float, float]], periods: int,
    window_minutes: int = 5, column: int = 1,
) -> Optional[bool]:
    """用分钟序列判断最近 periods 个窗口是否未衰减。

    未衰减 = 每个窗口增量非负（等价于累计值不下降，与快照口径一致）。
    样本不足、跨午休导致连续段不够长、或窗口内端点缺值，统一返回 None（无法核验）——
    不返回 False，避免把"缺数据"当成"已衰减"。
    """
    deltas = flow_window_deltas(series, window_minutes, column)
    if len(deltas) < periods:
        return None
    recent = deltas[-periods:]
    if any(not is_number(delta) for delta in recent):
        return None
    return all(delta >= 0 for delta in recent)


def flow_negative_samples_from_minutes(
    series: List[Tuple[str, float, float]], samples: int, spacing_minutes: int = 5
) -> Optional[int]:
    """最近 samples 个等间隔采样点里，累计主力净额 ≤ 0 的个数。

    只在最后一段连续分钟里按固定间隔采样（保证采样点之间真的隔了 spacing 分钟），
    对齐快照口径：数的是累计值是否为负，不是窗口增量。
    采样点缺值即视为无法核验（这里数的是取值本身，缺了不能跳过也不能当 0）。
    """
    tail = series[_contiguous_tail_start(series):]
    values = _column_values(tail, 1)
    if not values:
        return None
    points = values[::-1][::spacing_minutes]
    if len(points) < samples:
        return None
    recent = points[:samples]
    if any(value is None for value in recent):
        return None
    return sum(1 for value in recent if value <= 0)


def _flow_minute_series_for(code: str, expected_date: Optional[str]) -> List[Tuple[str, float, float]]:
    """取分钟序列，失败返回空列表（不抛异常，调用方按"无法核验"处理）。"""
    try:
        return fetch_flow_minutes(code, expected_date=expected_date)
    except Exception:
        return []


def _pace_flow_minute_request() -> None:
    global _FLOW_MINUTE_LAST_REQUEST_AT
    with _FLOW_MINUTE_LOCK:
        now = time.monotonic()
        wait = FLOW_MINUTE_MIN_INTERVAL_SECONDS - (now - _FLOW_MINUTE_LAST_REQUEST_AT)
        if wait > 0:
            time.sleep(wait)
        _FLOW_MINUTE_LAST_REQUEST_AT = time.monotonic()


def _is_intraday_now(ts: Optional[datetime] = None) -> bool:
    """当前是否在连续竞价时段内（含 09:15 盘前与 15:05 收盘缓冲）。"""
    moment = ts or datetime.now(TZ)
    if moment.weekday() >= 5:
        return False
    minutes = moment.hour * 60 + moment.minute
    return (9 * 60 + 15) <= minutes <= (15 * 60 + 5)


def _minutes_of_hhmm(hhmm: str) -> Optional[int]:
    parts = str(hhmm).split(":")
    if len(parts) != 2:
        return None
    try:
        return int(parts[0]) * 60 + int(parts[1])
    except ValueError:
        return None


def fetch_flow_minutes(code: str, expected_date: Optional[str] = None) -> List[Tuple[str, float]]:
    """拉当日 1 分钟累计主力净额序列，返回 [(HH:MM, 累计主力净额), ...]。

    expected_date 为本次行情快照所属交易日（YYYY-MM-DD）；bar 日期与之不符即视为过期序列。
    盘中另加 10 分钟新鲜度校验。校验不过一律返回空，交由上层保持“基准不足”，不猜、不补零。
    """
    now = time.time()
    cached = _FLOW_MINUTE_CACHE.get(code)
    if cached and now - float(cached[0]) <= FLOW_MINUTE_TTL_SECONDS:
        return cached[1]
    if not _flow_minute_budget_ok(code):
        return []   # 本轮预算用完：保持"无基准/无法核验"，不额外发请求

    params = {
        "secid": secid_for(code),
        "fields1": "f1,f2,f3,f7",
        # f52=主力净额，f56=超大单净额（coalition 连续性要同时核验两者）
        "fields2": "f51,f52,f56",
        "klt": "1",
        "lmt": "300",
    }
    moment = datetime.now(TZ)
    expected = expected_date or moment.strftime("%Y-%m-%d")
    now_minutes = moment.hour * 60 + moment.minute
    last_error: Optional[Exception] = None
    for url in _rank_urls(EM_FFLOW_MINUTE_URLS):
        _pace_flow_minute_request()
        try:
            data = fetch_json(url, params, timeout=6)
        except Exception as exc:
            last_error = exc
            _mark_host_failed(url)
            continue
        _mark_host_ok(url)
        rows = ((data or {}).get("data") or {}).get("klines") or []
        series: List[Tuple[str, float, float]] = []
        last_date = ""
        for line in rows:
            parts = str(line).split(",")
            if len(parts) < 2:
                continue
            stamp = parts[0].strip()
            hhmm = stamp[-5:] if len(stamp) >= 5 else stamp
            if " " in stamp:
                last_date = stamp.split(" ")[0]
            try:
                main_net = float(parts[1])
            except (TypeError, ValueError):
                continue
            try:
                super_net = float(parts[2]) if len(parts) > 2 else float("nan")
            except (TypeError, ValueError):
                super_net = float("nan")
            series.append((hhmm, main_net, super_net))
        if not series:
            continue
        if last_date and last_date != expected:
            continue
        if _is_intraday_now(moment):
            bar_minutes = _minutes_of_hhmm(series[-1][0])
            if bar_minutes is None or (now_minutes - bar_minutes) > 10:
                continue
        _FLOW_MINUTE_CACHE[code] = (now, series)
        return series
    if last_error is not None:
        raise last_error
    return []


def flow_increments_from_minutes(series: List[Tuple[str, float]]) -> Tuple[float, float]:
    """用累计序列端点差得到 5/15 分钟增量；连续样本不足返回 NaN（显示为基准不足）。

    只在最后一段连续分钟（见 `_contiguous_tail_start`）里取端点：跨午休或跨缺 bar 的
    端点差不是"5/15 分钟增量"，宁可返回 NaN 交给门槛拒绝，也不能给出虚高/虚低的数字。
    """
    tail = series[_contiguous_tail_start(series):]
    if len(tail) < 2:
        return float("nan"), float("nan")
    latest = tail[-1][1]
    inc_5m = latest - tail[-6][1] if len(tail) >= 6 else float("nan")
    inc_15m = latest - tail[-16][1] if len(tail) >= 16 else float("nan")
    return inc_5m, inc_15m


def fill_flow_increments_from_fflow(enriched: List[Enriched], expected_date: Optional[str] = None) -> int:
    """快照基准缺失时用分钟序列补齐 5/15 分钟增量，返回补齐的只数。

    只补缺项，不覆盖已有快照基准；单只失败保持 NaN，绝不写成 0。
    expected_date 为本次行情快照交易日，用于拒绝隔日序列。
    """
    targets = [
        e for e in enriched
        if not is_number(e.flow_5m_inc) or not is_number(e.flow_15m_inc)
    ]
    if not targets:
        return 0
    targets.sort(key=lambda e: e.main_net if is_number(e.main_net) else 0.0, reverse=True)

    flow_cfg = SCREENING_CONFIG["flow"]
    filled = 0
    for e in targets[:FLOW_MINUTE_MAX_CODES_PER_ROUND]:
        try:
            series = fetch_flow_minutes(e.code, expected_date=expected_date)
        except Exception:
            continue
        inc_5m, inc_15m = flow_increments_from_minutes(series)
        changed = False
        if not is_number(e.flow_5m_inc) and is_number(inc_5m):
            e.flow_5m_inc = inc_5m
            changed = True
        if not is_number(e.flow_15m_inc) and is_number(inc_15m):
            e.flow_15m_inc = inc_15m
            changed = True
        if changed:
            e.flow_baseline_source = "fflow"
            e.buy_ratio = compute_buy_ratio(e, flow_cfg)
            filled += 1
    return filled


def flow_veto_reason(e: Enriched) -> str:
    """框架一票否决：超大单为负。返回标记文本，未触发返回空串。

    2026-09-26 补：原先该否决只在诊断工具 tools/scan_reports.py 里执行，引擎负责发现但
    行上没有任何标记，只能靠人去看"超大单"那一列，漏看就绕过了否决。现在引擎直接打标，
    并在报告顶部汇总列出，与公告风险的标注方式一致。
    """
    if not RULE_CONFIG["dominance"].get("negative_super_veto"):
        return ""
    super_net = getattr(e, "super_net", None)
    if is_number(super_net) and float(super_net) < 0:
        return "超大单为负·一票否决"
    return ""


# 区间分位标注档位（纯展示，不参与门槛）：现价处于当日区间 ≥80% 记偏高，≥90% 记贴顶。
# 阈值依据：2026-09-24 全量报告 42 个候选行的区间分位中位数 76%、≥70% 的占 86%——
# 70% 那条线几乎每行都触发、没有区分度，因此上调为两档（≥80% 占 21%、≥90% 占 7%）。
# 若以后要把它变成"不追高"的硬门槛，请先按框架维护纪律搬进 tools/rule_config.py 并记录。
# Markdown 渲染（render_markdown、表格与单元格格式化）已拆到 screening/render.py；
# 这里重新导出，既有调用方式不变。
from screening.render import (  # noqa: E402,F401
    RANGE_POSITION_TOP_PCT,
    RANGE_POSITION_WARN_PCT,
    flow_amount_str,
    flow_status_cell,
    flow_veto_suffix,
    markdown_table,
    range_position_cell,
    render_markdown,
    shadow_badge_text,
)


def capital_data_label(e: Enriched, has_5m: bool) -> str:
    """数据完整度标签：区分快照基准、分钟序列兜底基准、无基准三种来源。"""
    if not has_5m:
        return "仅当前快照"
    return "含分钟序列" if e.flow_baseline_source == "fflow" else "含连续快照"


def is_negative_super(e: Any) -> bool:
    """是否属于「超大单为负」样本：必须是有限数值且为负。

    缺失、NaN、±inf 一律不算负值样本，也不得视为通过门槛。
    """
    super_net = getattr(e, "super_net", None)
    if not is_number(super_net):
        return False
    value = float(super_net)
    return math.isfinite(value) and value < 0


def count_negative_super_observations(enriched: List["Enriched"]) -> int:
    """只统计数量、不构造观察行、不触发任何公告查询。

    严格模式（负超单观察未开启）用它报告“影响数量”，避免为隐藏列表白跑公告接口。
    """
    return sum(1 for e in enriched if is_negative_super(e))


def build_negative_super_observations(
    enriched: List["Enriched"],
    stats: Dict[str, Dict[str, Any]],
    flow_history: Optional[Dict[str, List[Dict[str, Any]]]],
    ts,
) -> List[Dict[str, Any]]:
    """负超单观察：独立的观察列表，不进入任何真实仓/模拟仓资格。

    与生产否决标记同源（``super_net < 0``），但这里只如实展示事实与卡点：
      - 只收 ``is_negative_super`` 的行；缺失或非数值一律不算负值样本。
      - 逐项列出未通过的门槛（超大单为负一票否决、非 absolute 主导），
        不伪造 absolute/coalition 标签，也不写成“仅差一个条件即可买”。
    """
    rows: List[Dict[str, Any]] = []
    data_time = ts.strftime("%Y-%m-%d %H:%M:%S") if ts is not None else ""
    for e in enriched:
        if not is_negative_super(e):
            continue
        dom_type, dom_label = evaluate_dominance_type(e, flow_history)
        blockers: List[str] = []
        if e.flow_veto:
            blockers.append(f"{e.flow_veto}（框架一票否决）")
        if dom_type != "absolute":
            blockers.append(f"主导标签 {dom_label}，不满足 absolute（真实仓资格）")
        rows.append({
            "code": e.code,
            "name": e.name,
            "data_time": data_time,
            "price": e.price,
            "change": e.change,
            "turnover": e.turnover,
            "amount": e.amount,
            "industry": e.industry,
            "super_net": e.super_net,
            "main_net": e.main_net,
            "big_net": e.big_net,
            "main_pct": e.main_pct,
            "flow_5m_inc": e.flow_5m_inc,
            "vwap_state": e.vwap_state,
            "price_above_vwap": e.price_above_vwap,
            "resonance": "是" if has_resonance(e, stats) else "否",
            "flow_status": e.flow_status,
            "flow_veto": e.flow_veto,
            "risk_status": e.risk_status,
            "dominance_type": dom_type,
            "dominance_label": dom_label,
            "blockers": blockers,
            # 报告落盘后由只读判定器回填影子徽标；未回填一律视为“未完成判定”。
            "shadow_badge": None,
        })
    rows.sort(key=lambda r: r["super_net"])
    return rows


def classify_flow(e: Enriched, stats: Dict[str, Dict[str, Any]], has_snapshot: bool) -> str:
    """Classify capital flow status for an enriched stock.

    Returns one of: 有效流入, 疑似流入, 价量背离, 疑似派发, 数据不足
    """
    if not has_snapshot and not e.flow_baseline_source:
        return "数据不足"

    # Check conditions
    flow_cfg = SCREENING_CONFIG["flow"]
    classification_cfg = flow_cfg["classification"]
    alignment_ratio = float(classification_cfg["alignment_ratio_max_exclusive"])
    main_positive = e.main_net > 0
    super_big_aligned = (e.super_net > 0 and e.big_net > 0) or (
        e.super_net > 0 and abs(e.big_net) < abs(e.super_net) * alignment_ratio
    ) or (e.big_net > 0 and abs(e.super_net) < abs(e.big_net) * alignment_ratio)
    price_rising = e.change > float(classification_cfg["price_rising_min_exclusive"])
    above_vwap = e.price_above_vwap
    has_5m = not (isinstance(e.flow_5m_inc, float) and math.isnan(e.flow_5m_inc))
    flow_increasing = has_5m and e.flow_5m_inc > 0

    # 疑似派发: small orders in, large orders out, stock at high position
    small_in = e.small_net > 0
    big_out = e.super_net < 0 and e.big_net < 0
    at_high = e.dist60 < float(classification_cfg["at_high_dist60_max_exclusive"]) if is_number(e.dist60) else False

    if small_in and big_out and at_high:
        return "疑似派发"

    # 价量背离: capital/volume increasing but price not rising or below VWAP
    if main_positive and (not price_rising or not above_vwap) and flow_increasing:
        return "价量背离"
    if (
        e.main_pct > float(classification_cfg["divergence_main_pct_min_exclusive"])
        and e.change < float(classification_cfg["divergence_change_max_exclusive"])
    ):
        return "价量背离"

    # 有效流入: main force positive + sustained, super+big aligned, price up, above VWAP
    if main_positive and super_big_aligned and price_rising and above_vwap:
        if flow_increasing or e.main_pct > float(classification_cfg["effective_main_pct_min_exclusive"]):
            return "有效流入"

    # 疑似流入: main force positive but some conditions not met
    if main_positive:
        return "疑似流入"

    # Default: 数据不足 (no clear signal)
    return "数据不足"


def evaluate_dominance_type(
    e: Any, flow_history: Optional[Dict[str, List[Dict[str, Any]]]] = None
) -> Tuple[str, str]:
    """
    超大单主导双轨判定（来源：选股框架.md 8/21 权威定版）：
    返回: (dominance_type, display_label)
    - dominance_type: 'absolute' | 'coalition' | 'none'
    - display_label: '✓(绝对)' | '✓(合力)' | '✗'

    条件 A ｜ 绝对主导 (absolute):
      超大单 > 0 且 超大单 / 主力净额 >= 50%
    条件 B ｜ 合力主导 (coalition):
      超大单 >= 2000万 且 大单 > 0 且 20% <= 超大单/主力 < 50% 且 主力 >= 5000万
      且 5分钟增量 >= 1000万 且 最近2次快照主力与超大单均未衰减 且 主买比（代理值） >= 1.5
      【严禁缺数据放行：历史不足2期或主买比缺失/小于1.5一律拒绝】
    """
    super_net = getattr(e, "super_net", 0)
    super_net = float(super_net) if isinstance(super_net, (int, float)) else 0.0
    main_net = getattr(e, "main_net", 0)
    main_net = float(main_net) if isinstance(main_net, (int, float)) else 0.0
    big_net = getattr(e, "big_net", 0)
    big_net = float(big_net) if isinstance(big_net, (int, float)) else (main_net - super_net)
    flow_5m = getattr(e, "flow_5m_inc", 0)
    flow_5m = float(flow_5m) if isinstance(flow_5m, (int, float)) else 0.0
    code = getattr(e, "code", "")
    buy_ratio = getattr(e, "buy_ratio", None)
    buy_ratio = float(buy_ratio) if (isinstance(buy_ratio, (int, float)) and not math.isnan(buy_ratio)) else None

    dominance_cfg = RULE_CONFIG["dominance"]
    absolute_cfg = dominance_cfg["absolute"]
    coalition_cfg = dominance_cfg["coalition"]
    none_label = dominance_cfg["none_label"]

    # 条件 A：绝对主导
    absolute_ratio = float(absolute_cfg["min_super_ratio"])
    if (
        super_net > float(absolute_cfg["min_super_net"])
        and main_net > 0
        and super_net >= main_net * absolute_ratio
    ):
        return "absolute", absolute_cfg["label"]

    # 条件 B：合力主导（严格互斥：20% <= super_ratio < 50% 且 严禁缺数据放行）
    super_ratio = (super_net / main_net) if main_net > 0 else 0.0
    is_coalition_amounts = (
        super_net >= float(coalition_cfg["min_super_net"])
        and big_net > float(coalition_cfg["min_big_net"])
        and float(coalition_cfg["min_super_ratio"]) <= super_ratio < float(coalition_cfg["max_super_ratio_exclusive"])
        and main_net >= float(coalition_cfg["min_main_net"])
        and flow_5m >= float(coalition_cfg["min_flow_5m"])
    )

    if is_coalition_amounts:
        # 硬门槛 1: 主买比（代理值，非真实分笔）必须存在且 >= 1.5（严禁 None/缺数据放行）
        if buy_ratio is None or buy_ratio < float(coalition_cfg["min_buy_ratio"]):
            return "none", none_label

        # 硬门槛 2: 必须有连续至少 2 期且主力与超大单均未衰减（严禁缺历史放行）
        min_snaps = int(coalition_cfg["min_history_snapshots"])
        snaps = (flow_history or {}).get(code) or []
        if len(snaps) >= min_snaps:
            prev_main = snaps[-2].get("main_net", 0)
            prev_super = snaps[-2].get("super_net", 0)
            keep_ratio = 1.0 - float(coalition_cfg["max_decay_pct"]) / 100.0
            if main_net < prev_main * keep_ratio or super_net < prev_super * keep_ratio:
                return "none", none_label
        else:
            # 2026-09-26：本地快照不足时用东财分钟序列核验"连续未衰减"（口径等价：
            # 累计值不下降 ⟺ 窗口增量非负）。主力(f52)与超大单(f56)两列都要通过，
            # 任一列样本不足即视为无法核验，按原规则拒绝——不靠单列放松门槛。
            date = round_series_date()
            series = _flow_minute_series_for(code, date) if date else []
            main_ok = flow_continuity_from_minutes(series, min_snaps, column=1)
            super_ok = flow_continuity_from_minutes(series, min_snaps, column=2)
            if main_ok is not True or super_ok is not True:
                return "none", none_label

        return "coalition", coalition_cfg["label"]

    return "none", none_label


def rank_capital_candidates(
    candidates: List[Enriched], stats: Dict[str, Dict[str, Any]],
    flow_history: Optional[Dict[str, List[Dict[str, Any]]]] = None
) -> List[Dict[str, Any]]:
    """Rank strict-pool candidates by formal flow evidence; annotate sector experiments separately.

    - 主线板块协同分只作为实验字段，不参与正式评分、评级或排序
    - 板块锚点条件: 成交额>20亿 且 主力净额>0 且 超大单主导(A或B) 且 VWAP上方 且 高位回落<2.0%
    - 板块共振条件: 至少3只共振，或锚点外另有2只共振（即锚点>=1 且 共振>=3 或 锚点外>=2）
    - 赋分条件: clean 且 主力>5% 且 VWAP上方 且 超单主导 且 回落<2%
    - 缺少5分钟基准时，正式评级标记未确认
    - 板块内归一化排序兼顾 5分钟净额 与 5分钟净额/成交额
    """
    sector_cfg = SCREENING_CONFIG["sector_boost"]
    score_cfg = SCREENING_CONFIG["capital_rank"]
    # 1. 扫描板块锚点与共振
    sector_anchors: Dict[str, List[Enriched]] = {}
    sector_resonance_counts: Dict[str, int] = {}
    for e in candidates:
        ind = e.industry or ""
        if not ind:
            continue
        if has_resonance(e, stats):
            sector_resonance_counts[ind] = sector_resonance_counts.get(ind, 0) + 1
        dom_type, _ = evaluate_dominance_type(e, flow_history)
        is_anchor = (
            e.amount >= float(sector_cfg["anchor_amount_min"]) and
            e.main_net > 0 and
            dom_type in ("absolute", "coalition") and
            e.price_above_vwap and
            e.high_pull < float(sector_cfg["anchor_high_pullback_max_exclusive"])
        )
        if is_anchor:
            sector_anchors.setdefault(ind, []).append(e)

    ranked: List[Dict[str, Any]] = []
    for e in candidates:
        has_5m = is_number(e.flow_5m_inc)
        quality = stats.get("__meta__") or {}
        resonance_usable = quality.get("resonance_usable", True)
        resonance = has_resonance(e, stats)
        dom_type, dom_label = evaluate_dominance_type(e, flow_history)

        # 0-100 score: current relative flow 55, persistence 20,
        # price confirmation 15, sector confirmation 10.
        main_points = max(
            0.0,
            min(
                float(score_cfg["score_main_max_points"]),
                e.main_pct / float(score_cfg["score_main_pct_scale"]) * float(score_cfg["score_main_max_points"]),
            ),
        )
        super_points = max(
            0.0,
            min(
                float(score_cfg["score_super_max_points"]),
                e.super_pct / float(score_cfg["score_super_pct_scale"]) * float(score_cfg["score_super_max_points"]),
            ),
        )
        persistence_points = 0.0
        if has_5m:
            increment_pct = e.flow_5m_inc / e.amount * 100 if e.amount > 0 else 0.0
            persistence_points = max(
                0.0,
                min(
                    float(score_cfg["score_persistence_max_points"]),
                    increment_pct / float(score_cfg["score_persistence_pct_scale"])
                    * float(score_cfg["score_persistence_max_points"]),
                ),
            )

        if e.price_above_vwap and e.change > float(score_cfg["score_price_change_min_exclusive"]):
            price_points = float(score_cfg["score_price_positive_points"])
        elif e.price_above_vwap:
            price_points = float(score_cfg["score_price_nonpositive_points"])
        else:
            price_points = 0.0
        sector_points = float(score_cfg["score_resonance_points"]) if resonance else 0.0

        # 板块协同加分器 (sector_boost)
        sector_boost = 0.0
        b_preferred = False
        ind = e.industry or ""
        anchors_cnt = len(sector_anchors.get(ind, []))
        has_anchor = anchors_cnt >= 1
        res_cnt = sector_resonance_counts.get(ind, 0)
        non_anchor_res_cnt = max(0, res_cnt - anchors_cnt)
        sector_active = has_anchor and (
            res_cnt >= int(sector_cfg["resonance_total_min"])
            or non_anchor_res_cnt >= int(sector_cfg["non_anchor_resonance_min"])
        )

        risk_status = _row_risk_status(e)
        is_clean = (risk_status == RISK_CLEAN)

        if (
            sector_active
            and is_clean
            and e.main_pct > float(sector_cfg["stock_main_pct_min_exclusive"])
            and e.price_above_vwap
            and dom_type in ("absolute", "coalition")
            and e.high_pull < float(sector_cfg["anchor_high_pullback_max_exclusive"])
        ):
            sector_boost = float(sector_cfg["boost_points"])
            b_preferred = True

        # The formal capital score and rank exclude the unvalidated sector experiment.
        score = main_points + super_points + persistence_points + price_points + sector_points

        reasons: List[str] = []
        if getattr(e, "flow_veto", ""):
            reasons.append(f"❌{e.flow_veto}")
        if e.main_pct > 0:
            reasons.append(f"主力净占比{e.main_pct:.1f}%")
        else:
            reasons.append("主力净流出")
        if dom_type == "absolute":
            reasons.append("超大单绝对主导")
        elif dom_type == "coalition":
            reasons.append("游资机构合力主升")
        if has_5m and e.flow_5m_inc > 0:
            reasons.append("5分钟持续流入")
        if e.price_above_vwap:
            reasons.append("均价线上方")
        if resonance:
            reasons.append("板块共振")
        elif not resonance_usable:
            reasons.append("板块共振数据质量降级，未加分")
        if sector_boost > 0:
            reasons.append(f"主线板块协同实验(+{int(sector_boost)}分，未计入正式评分)")

        if e.flow_status == "疑似派发":
            score -= float(score_cfg["penalty_distribution"])
            reasons.append("疑似派发")
        elif e.flow_status == "价量背离":
            score -= float(score_cfg["penalty_divergence"])
            reasons.append("价量背离")
        if e.main_net < 0:
            score -= float(score_cfg["penalty_main_out"])
        if e.high_pull > float(score_cfg["penalty_high_pull_free_max_inclusive"]):
            score -= min(
                float(score_cfg["penalty_high_pull_max_points"]),
                (e.high_pull - float(score_cfg["penalty_high_pull_free_max_inclusive"]))
                * float(score_cfg["penalty_high_pull_scale"]),
            )
            reasons.append("高位回落偏大")

        score = round(max(0.0, min(100.0, score)), 1)
        if e.flow_status == "疑似派发" or score < float(score_cfg["capital_class_c_score_max_exclusive"]):
            capital_class_candidate = "资金C类"
        elif score >= float(score_cfg["capital_class_a_score_min_inclusive"]) and e.main_net > 0 and e.price_above_vwap:
            capital_class_candidate = "资金A类"
        else:
            capital_class_candidate = "资金B类"
        capital_class_confirmed = has_5m
        capital_class = capital_class_candidate if capital_class_confirmed else "评级未确认"
        if not capital_class_confirmed:
            reasons.append("缺少5分钟基准，正式评级未确认")
        sector_experiment_score = round(min(100.0, score + sector_boost), 1)

        # 板块内归一化弹性得分（兼顾 5分净额与 5分/成交额）
        norm_score = score
        if has_5m and e.amount > 0:
            norm_score += min(
                float(score_cfg["normalized_flow_max_points"]),
                (e.flow_5m_inc / e.amount) * float(score_cfg["normalized_flow_scale"]),
            )

        ranked.append({
            **asdict(e),
            "capital_score": score,
            "capital_class": capital_class,
            "capital_class_candidate": capital_class_candidate,
            "capital_class_confirmed": capital_class_confirmed,
            "sector_experiment_score": sector_experiment_score,
            "capital_data": capital_data_label(e, has_5m),
            "capital_reason": "；".join(reasons),
            "resonance": "是" if resonance else ("数据质量降级" if not resonance_usable else "否"),
            "dominance_type": dom_type,
            "dominance_label": dom_label,
            "sector_boost": sector_boost,
            "b_preferred": b_preferred,
            "_norm_score": norm_score,
        })
    return sorted(ranked, key=lambda r: (-r["_norm_score"], -r["capital_score"], r["code"]))


# 双池交集状态机（配置/相位/准交集/状态推进/持久化）已拆到 screening/intersection.py；
# 这里重新导出，既有调用方式不变。状态文件路径请改 intersection 模块上的属性。
from screening.intersection import (  # noqa: E402,F401
    DEFAULT_INTERSECTION_CONFIG,
    INTERSECTION_CALIBRATION_FILE,
    INTERSECTION_CONFIG_VERSION,
    INTERSECTION_STATE_FILE,
    PHASE_ENTRY,
    PHASE_EXPIRED,
    PHASE_INVALID,
    PHASE_LABELS,
    PHASE_LATCHED,
    PHASE_LATE,
    PHASE_OBSERVING,
    PHASE_ORDER,
    PHASE_PRE,
    PHASE_RETEST_READY,
    PHASE_WAIT_RETEST,
    _LEGACY_PHASE_MAP,
    _canonical_phase,
    _clock_minutes,
    _config_is_safe,
    _format_clock,
    _intersection_rejection_reasons,
    _late_filter_reasons,
    _minute_freshness,
    _parse_intersection_datetime,
    _pre_gates,
    _predict_trigger_price,
    _pullback_zone,
    compute_pre_intersection,
    evaluate_intersection_states,
    load_intersection_calibration,
    load_intersection_state,
    resolve_intersection_config,
    resolve_market_mode,
    save_intersection_state,
)


def _sanitize_for_json(obj: Any) -> Any:
    """Recursively replace NaN/Inf with None for JSON serialization."""
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


WATCHLIST_BREAKOUT_STATE_PATH = runtime_paths.state_file("watchlist_breakout_state.json")


def load_watchlist_breakout_state() -> Dict[str, Dict[str, Any]]:
    """加载观察池突破状态机跨快照持久化数据。"""
    try:
        if WATCHLIST_BREAKOUT_STATE_PATH.exists():
            data = json.loads(WATCHLIST_BREAKOUT_STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def save_watchlist_breakout_state(
    items: Dict[str, Dict[str, Any]],
    date_str: str,
    commit: Optional[state_commit.RoundCommit] = None,
) -> None:
    """持久化保存观察池突破状态机（临时文件 + os.replace 原子写入）。"""
    payload = {"date": date_str, "items": items}
    state_commit.write_or_stage(
        commit, WATCHLIST_BREAKOUT_STATE_PATH, json.dumps(payload, ensure_ascii=False, indent=2)
    )


def _breakout_snapshot_epoch(value: Any) -> Optional[float]:
    """Normalize a source quote timestamp or persisted ISO timestamp to epoch seconds."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        return number if math.isfinite(number) and number > 0 else None
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).strip())
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=TZ)
    return parsed.timestamp()


def evaluate_watchlist_breakout_states(
    watchlist_items: List[Dict[str, Any]],
    enriched_by_code: Dict[str, Enriched],
    stats: Dict[str, Dict[str, Any]],
    flow_history: Optional[Dict[str, List[Dict[str, Any]]]],
    previous_state: Dict[str, Dict[str, Any]],
    now: datetime,
    risk_map: Optional[Dict[str, str]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    明日观察池突破升级状态机（来源：选股框架.md 8/21 权威定版）：
    流转路径: WATCHING → TRIGGERED → CONFIRMED → B_BREAKOUT → A_STRICT
    - 跨交易日合并：将昨日已持久化的观察池标的与今日新候选求并集，确保昨日标的在次日开盘能无缝流转
    - 09:30–09:40 原则上只观察（不可直升 B_BREAKOUT/A_STRICT）
    - 至少 2 个来源行情时间递增的快照确认；重复消费同一行情不计数
    - avoid 一票否决
    - 超过追高禁区、跌回触发价或 5 分钟转负立即降级重置
    """
    breakout_cfg = RULE_CONFIG["screening"]["breakout"]
    now_text = now.strftime("%Y-%m-%d %H:%M:%S")
    now_minutes = now.hour * 60 + now.minute
    is_morning_observe = (
        _clock_minutes(str(breakout_cfg["morning_observe_start"]))
        <= now_minutes
        < _clock_minutes(str(breakout_cfg["morning_observe_end"]))
    )
    next_state: Dict[str, Dict[str, Any]] = {}
    evaluated_watchlist: List[Dict[str, Any]] = []

    # 1. 跨日观察池合并：将昨日持久化标的与今日新候选合并（昨日标的保留其触发价基准）
    merged_items_map: Dict[str, Dict[str, Any]] = {}
    for p_code, p_item in (previous_state or {}).items():
        if p_code:
            merged_items_map[p_code] = {
                "code": p_code,
                "name": p_item.get("name", p_code),
                "trigger": p_item.get("trigger_price", p_item.get("trigger", 0)),
                "buy_zone": p_item.get("buy_zone", "-"),
                "invalid": p_item.get("invalid", 0),
                "no_chase": p_item.get(
                    "no_chase",
                    f">{float(p_item.get('trigger_price', 0)) * float(breakout_cfg['no_chase_multiplier']):.2f}不追",
                ),
                "industry": p_item.get("industry", "-"),
                "structure": p_item.get("structure", "观察池追踪"),
                "reason": p_item.get("reason", "昨日观察池延续"),
            }
    for item in watchlist_items:
        code = item.get("code", "")
        if code:
            merged_items_map[code] = {**merged_items_map.get(code, {}), **item}

    for code, item in merged_items_map.items():
        trigger_price = float(item.get("trigger", 0))
        no_chase_str = str(item.get("no_chase", ""))
        m_chase = re.search(r">([\d.]+)不追", no_chase_str)
        no_chase_price = (
            float(m_chase.group(1))
            if m_chase
            else trigger_price * float(breakout_cfg["no_chase_multiplier"])
        )

        e = enriched_by_code.get(code)
        prev = (previous_state or {}).get(code) or {}
        prev_phase = prev.get("phase", "WATCHING")
        confirm_count = prev.get("confirm_count", 0)
        source_snapshot_id = _breakout_snapshot_epoch(getattr(e, "timestamp", None)) if e else None
        previous_snapshot_id = _breakout_snapshot_epoch(prev.get("last_snapshot_id"))
        last_snapshot_id = previous_snapshot_id

        p_val = getattr(e, "price", None)
        cur_price = float(p_val) if isinstance(p_val, (int, float)) else float(item.get("price", 0))
        f5_val = getattr(e, "flow_5m_inc", None)
        flow_5m = float(f5_val) if isinstance(f5_val, (int, float)) else 0.0
        above_vwap = bool(getattr(e, "price_above_vwap", False))
        dom_type, dom_label = evaluate_dominance_type(e, flow_history) if e else ("none", "✗")
        res = has_resonance(e, stats) if e else False

        # 公告风控解析（优先从 risk_map 获取，次选 _row_risk_status）
        risk = RISK_UNKNOWN
        if risk_map and code in risk_map:
            risk = risk_map[code]
        elif e:
            risk = _row_risk_status(e)
        else:
            risk = _row_risk_status(item)

        # 状态机判定
        cur_phase = "WATCHING"
        breakout_class = "WATCHING"
        status_note = "处于触发价下方观察"

        if risk == RISK_AVOID:
            cur_phase = "INVALID"
            breakout_class = "INVALID"
            confirm_count = 0
            status_note = f"公告风控 {RISK_AVOID} 否决"
        elif risk == RISK_UNKNOWN:
            # 公告 unknown 与 avoid 同为一票否决语义：可以继续展示“数据不足”的观察信息，
            # 但不得输出 TRIGGERED/CONFIRMED/B_BREAKOUT/A_STRICT 等看似升级的状态
            # （选股框架.md：公告 unknown 禁止正式买入建议）。
            confirm_count = 0
            cur_phase = "WATCHING"
            breakout_class = "WATCHING"
            status_note = "公告检查不可用（unknown），仅观察不升级"
        elif cur_price > no_chase_price:
            cur_phase = "OVER_CHASE"
            breakout_class = "WATCHING"
            confirm_count = 0
            status_note = f"超过追高禁区({cur_price:.2f} > {no_chase_price:.2f})"
        elif cur_price < trigger_price:
            # 跌回触发价下方立即降级重置
            confirm_count = 0
            cur_phase = "WATCHING"
            breakout_class = "WATCHING"
            status_note = "处于触发价下方观察"
        elif flow_5m < 0:
            # 5分钟增量转负立即降级重置
            confirm_count = 0
            cur_phase = "WATCHING"
            breakout_class = "WATCHING"
            status_note = f"5分钟增量转负({flow_5m/10000:.0f}万 < 0)立即降级"
        elif cur_price >= trigger_price:
            # 价格突破触发价且 5分增量 >= 500万
            if flow_5m >= float(breakout_cfg["flow_5m_min"]):
                if prev_phase in ("TRIGGERED", "CONFIRMED", "B_BREAKOUT", "A_STRICT"):
                    # Only a newer source quote is an additional confirmation.
                    snapshot_note = ""
                    if (
                        source_snapshot_id is not None
                        and previous_snapshot_id is not None
                        and source_snapshot_id > previous_snapshot_id
                    ):
                        confirm_count += 1
                        last_snapshot_id = source_snapshot_id
                    elif previous_snapshot_id is None and source_snapshot_id is not None:
                        # Legacy state had no source ID. Establish a baseline,
                        # and discard unprovable legacy confirmations. This is at
                        # most the first source-time-backed confirmation.
                        confirm_count = 1
                        last_snapshot_id = source_snapshot_id
                        snapshot_note = "已记录行情时间基准，本轮不增加确认"
                    elif source_snapshot_id is None:
                        if previous_snapshot_id is None:
                            confirm_count = 0
                        snapshot_note = "缺少行情来源时间，确认次数不变"
                    else:
                        snapshot_note = "重复或回退的行情快照，确认次数不变"
                    # 突破确认门槛：站稳 >= 2 期 + VWAP 上方 + 超单主导 (A或B) + 板块共振
                    if (
                        confirm_count >= int(breakout_cfg["confirmations_min"])
                        and above_vwap
                        and dom_type in ("absolute", "coalition")
                        and res
                    ):
                        cur_phase = "CONFIRMED"
                        if is_morning_observe:
                            breakout_class = "CONFIRMED"
                            status_note = f"09:30-09:40 观察期锁定(已站稳{confirm_count}期·板块共振)"
                        else:
                            # 09:40 后根据资金强度评定 A_STRICT 或 B_BREAKOUT
                            mp_val = getattr(e, "main_pct", 0) if e else 0
                            main_pct = float(mp_val) if isinstance(mp_val, (int, float)) else 0.0
                            hp_val = getattr(e, "high_pull", 0) if e else 0
                            high_pull = float(hp_val) if isinstance(hp_val, (int, float)) else 0.0
                            br_val = getattr(e, "buy_ratio", None) if e else None
                            buy_ratio = float(br_val) if (isinstance(br_val, (int, float)) and not math.isnan(br_val)) else None

                            # A_STRICT 严格要求：绝对主导 + clean + 主力>=5% + 回落<1.5% + 主买比（代理值）>=1.5（严禁缺数据放行）
                            is_a_strict = (
                                dom_type == "absolute" and
                                risk == RISK_CLEAN and
                                main_pct >= float(breakout_cfg["a_main_pct_min"]) and
                                high_pull < float(breakout_cfg["a_high_pullback_max_exclusive"]) and
                                buy_ratio is not None and
                                buy_ratio >= float(RULE_CONFIG["dominance"]["coalition"]["min_buy_ratio"])
                            )
                            if is_a_strict:
                                cur_phase = "A_STRICT"
                                breakout_class = "A_STRICT"
                                status_note = f"A类突破主升·站稳{confirm_count}期·超单绝对主导·板块共振·低回落"
                            else:
                                cur_phase = "B_BREAKOUT"
                                breakout_class = "B_BREAKOUT"
                                status_note = f"B类突破候选·已站稳{confirm_count}期·超单主导·板块共振"
                    else:
                        cur_phase = "TRIGGERED"
                        breakout_class = "TRIGGERED"
                        missing_notes = []
                        if not above_vwap: missing_notes.append("未站上VWAP")
                        if dom_type not in ("absolute", "coalition"): missing_notes.append("超单未主导")
                        if not res: missing_notes.append("板块无共振")
                        status_note = f"突破触发·等待确认({confirm_count}/2期, 待满足:{','.join(missing_notes)})"
                        if snapshot_note:
                            status_note += f"；{snapshot_note}"
                else:
                    if source_snapshot_id is None:
                        cur_phase = "WATCHING"
                        breakout_class = "WATCHING"
                        confirm_count = 0
                        status_note = "缺少行情来源时间，不能记录首次确认"
                    elif (
                        previous_snapshot_id is not None
                        and source_snapshot_id <= previous_snapshot_id
                    ):
                        cur_phase = "WATCHING"
                        breakout_class = "WATCHING"
                        confirm_count = 0
                        status_note = "重复或回退的行情快照，不能记录首次确认"
                    else:
                        cur_phase = "TRIGGERED"
                        breakout_class = "TRIGGERED"
                        confirm_count = 1
                        last_snapshot_id = source_snapshot_id
                        status_note = "初次放量突破触发价"
            else:
                cur_phase = "WATCHING"
                breakout_class = "WATCHING"
                confirm_count = 0
                status_note = f"突破但5分量能不足({flow_5m/10000:.0f}万 < 500万)"

        # Persist every newest source quote ID, including snapshots that fail a
        # gate, so replaying that same quote cannot later become a new trigger.
        if (
            source_snapshot_id is not None
            and (previous_snapshot_id is None or source_snapshot_id > previous_snapshot_id)
        ):
            last_snapshot_id = source_snapshot_id

        # 更新状态持久化
        item_state = {
            "code": code,
            "name": item.get("name"),
            "phase": cur_phase,
            "breakout_class": breakout_class,
            "confirm_count": confirm_count,
            "trigger_price": trigger_price,
            "no_chase_price": no_chase_price,
            "buy_zone": item.get("buy_zone", "-"),
            "invalid": item.get("invalid", 0),
            "no_chase": item.get("no_chase", ""),
            "industry": item.get("industry", "-"),
            "structure": item.get("structure", ""),
            "reason": item.get("reason", ""),
            "last_price": cur_price,
            "flow_5m": flow_5m,
            "dominance_type": dom_type,
            "above_vwap": above_vwap,
            "risk_status": risk,
            "status_note": status_note,
            "last_updated": now_text,
            "last_snapshot_id": last_snapshot_id,
        }
        next_state[code] = item_state

        item_copy = dict(item)
        item_copy.update({
            "price": cur_price,
            "change": getattr(e, "change", item.get("change", 0)) if e else item.get("change", 0),
            "breakout_phase": cur_phase,
            "breakout_class": breakout_class,
            "confirm_count": confirm_count,
            "dominance_label": dom_label,
            "risk_status": risk,
            "status_note": status_note,
        })
        evaluated_watchlist.append(item_copy)

    return evaluated_watchlist, next_state


def build_watchlist(items: List[Enriched], stats: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    breakout_cfg = RULE_CONFIG["screening"]["breakout"]
    cfg = SCREENING_CONFIG["watchlist"]
    rows = []
    for e in items:
        if not (
            float(cfg["price_min_inclusive"]) <= e.price <= float(cfg["price_max_inclusive"])
            and float(cfg["float_mv_min_inclusive"]) <= e.float_mv <= float(cfg["float_mv_max_inclusive"])
        ):
            continue
        if not (e.price >= e.ma10 or e.price >= float(cfg["ma10_repair_min_multiplier"]) * e.ma10):
            continue
        if e.five_ret > float(cfg["five_ret_max_inclusive"]) or e.dist60 > float(cfg["dist60_max_inclusive"]):
            continue
        if not has_resonance(e, stats):
            continue
        if e.change > float(cfg["change_max_exclusive"]) or e.turnover > float(cfg["turnover_max_exclusive"]):
            continue
        trigger = max(e.prior_high, e.high)
        buy_low = max(
            e.ma5 * float(cfg["buy_low_ma5_multiplier"]),
            e.ma10 * float(cfg["buy_low_ma10_multiplier"]),
        )
        buy_high = max(
            e.ma5 * float(cfg["buy_high_ma5_multiplier"]),
            e.ma10 * float(cfg["buy_high_ma10_multiplier"]),
        )
        invalid = min(
            e.ma10 * float(cfg["invalid_ma10_multiplier"]),
            e.prior_low * float(cfg["invalid_prior_low_multiplier"]),
        )
        structure = "趋势低吸" if e.price > e.ma5 > e.ma10 and e.ma10 >= e.ma20 else "突破前观察"
        score = (
            100
            - abs(e.change - float(cfg["score_change_target"])) * float(cfg["score_change_scale"])
            - max(0, e.five_ret * 100 - float(cfg["score_five_ret_free_pct"])) * float(cfg["score_five_ret_scale"])
            - max(0, e.dist60 * 100 - float(cfg["score_dist60_free_pct"])) * float(cfg["score_dist60_scale"])
        )
        rows.append({
            "score": round(score, 1),
            "code": e.code,
            "name": e.name,
            "price": e.price,
            "change": e.change,
            "range_pct": e.range_pct,
            "industry": e.industry,
            "structure": structure,
            "trigger": round(trigger, 2),
            "buy_zone": f"{buy_low:.2f}-{buy_high:.2f}",
            "invalid": round(invalid, 2),
            "no_chase": f">{trigger * float(breakout_cfg['no_chase_multiplier']):.2f}不追",
            "reason": f"{e.industry}; 5日{e.five_ret*100:.1f}%; 距60高{e.dist60*100:.1f}%",
        })
    return sorted(rows, key=lambda r: r["score"], reverse=True)[: int(cfg["max_rows"])]



# 需要在每行标注交易板的正式栏目（与公告检查的可见栏目清单一致，另含双池明细与状态机）
BOARD_STAMPED_SECTIONS = (
    "strict_ultra", "trend_observation", "strict_trend", "dual_pool", "dual_pool_raw",
    "capital_rank", "low_ultra", "low_trend", "watchlist", "pre_intersection",
    "intersection_states", "negative_super_observations",
)


def stamp_board_fields(result: Dict[str, Any]) -> None:
    """给各正式栏目每一行补上「交易板」字段（由代码判定，不依赖行内已有值）。

    创业板/科创板加入正式筛选后，同一张表里会同时出现三板的股票，
    因此每行都必须能看出它属于哪个交易板；用 setdefault 保留既有值，
    便于历史快照按原样渲染。
    """
    for section in BOARD_STAMPED_SECTIONS:
        for row in result.get(section) or []:
            if not isinstance(row, dict):
                continue
            board = board_of(row.get("code") or row.get("f12"))
            row.setdefault("board", board)
            row.setdefault("board_label", BOARD_LABELS.get(board, board))


def board_scope_note(status: str, selected: Any, candidate_total: int) -> str:
    """本轮范围的结论说明：必须能区分「未符合条件」与「数据不可用」。

    ``candidate_total`` 是本轮所选交易板范围内各候选池**去重后的候选股票数**
    （CLI 与实时引擎同一口径）。数据完整且为 0 时才是「没有符合现有门槛的候选」；
    数据降级/不完整时无论是否为 0 都只能报「本轮结果不完整」。
    """
    boards = normalize_boards(selected)
    label = " + ".join(BOARD_LABELS.get(board, board) for board in boards)
    if status != "ok":
        reason = {"degraded": "行情降级（备用源，字段不完整）", "incomplete": "行情快照不完整"}.get(status, status)
        return f"筛选范围：{label}；{reason}，本轮结果不完整——无候选不等于「未符合条件」。"
    if candidate_total == 0:
        return f"筛选范围：{label}；数据完整，本轮该范围内没有符合现有门槛的候选（正常筛选结果）。"
    return f"筛选范围：{label}；数据完整，本轮范围内产生 {candidate_total} 条候选。"


def load_holding_codes() -> set:
    """读取本机持仓文件里的股票代码。

    只用于「取消某个交易板后，已持仓股票的监控不能停」这一条；不参与任何买入判定。
    文件结构可能变化，因此递归找 6 位代码，而不是依赖固定字段名。
    """
    path = SCRIPT_DIR / "holdings.json"
    codes: set = set()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return codes

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)
        elif isinstance(node, str):
            text = node.strip()
            if text.isdigit() and len(text) == 6:
                codes.add(text)

    walk(payload)
    return codes


def prune_state_by_boards(
    items: Dict[str, Any], boards: Any = (BOARD_MAIN,), keep_codes: Any = ()
) -> Dict[str, Any]:
    """把**不在所选交易板内**的旧候选从连续状态里剔除。

    切换交易板范围后，旧范围留下的锁存/等待回踩/突破状态不能继续生效（否则等于绕过了
    本次范围设置）；但已持仓股票必须保留——监控不能因为取消某个交易板而停止。
    """
    selected = set(normalize_boards(boards))
    keep = {str(code) for code in (keep_codes or ())}
    return {
        str(code): item
        for code, item in (items or {}).items()
        if str(code) in keep or board_of(code) in selected
    }


def count_by_board(rows: List[Any]) -> Dict[str, int]:
    """按板别计数；命中不了的归入 unknown（用于报告/meta 的口径记录）。"""
    counts = {board: 0 for board in BOARD_ORDER}
    counts[BOARD_UNKNOWN] = 0
    for item in rows:
        code = item.get("f12") if isinstance(item, dict) else getattr(item, "code", None)
        counts[board_of(code)] = counts.get(board_of(code), 0) + 1
    return counts


def board_scope_status_for(
    fallback_snapshot: bool, fetch_status: Optional[Dict[str, Any]] = None
) -> str:
    """本轮交易板范围结果的可用性判定（CLI 与引擎共用同一口径，避免各自缺省成 ok）。

    - ``degraded``：行情走了新浪备用源（字段降级）；
    - ``incomplete``：快照分页不完整；
    - ``ok``：可产出所选交易板的正式筛选结果。

    降级/不完整时结果不可用，报告要区分「未符合条件」与「数据不可用」。
    """
    if fallback_snapshot:
        return "degraded"
    if (fetch_status or {}).get("complete") is False:
        return "incomplete"
    return "ok"



# ══════════════════════════════════════════════════════════════════════════
# 共享筛选核心
#
# CLI（本文件 main）与看板/工作台（realtime_engine.run_screening）都调用
# run_screening_core。规则计算、公告门禁、资金排名、两条状态机只在这里跑一次，
# 避免两套实现漂移。入口差异（网络模式、K 线缓存、进度回调、分钟量能、温度计）
# 通过 ScreeningHooks 显式注入，不靠调用方身份分支，也不 monkey-patch 全局函数。
#
# ``top`` 只影响本函数最后的展示裁剪；完整候选集合、公告核验范围、双池交集、
# 准交集、观察池突破与交集状态推进一律读全量内部集合。
# ══════════════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class ScreeningParams:
    """一次筛选的机器参数（CLI、看板、工作台共用同一套）。"""

    modes: frozenset
    workers: int = 6
    top: int = 10
    skip_announcements: bool = False
    skip_capital_ranking: bool = False
    network_mode: str = "auto"
    announcement_page_size: int = 8
    enabled_boards: Tuple[str, ...] = (BOARD_MAIN,)


@dataclass
class ScreeningHooks:
    """入口差异注入点。

    - ``state_commit``：运行状态提交门。非空时资金基准/交集状态/观察池突破状态
      只暂存，由调度器在确认本轮有效后统一提交；超时/失败的一轮不得落盘。
    - ``fetch_kline_fn``：K 线取数（看板注入带缓存与限速的实现）。
    - ``build_extras``：公告核验前追加的看板专有列表（如负超单观察），其行会被
      同一份公告查询覆盖。
    - ``minute_map_provider``：状态机分钟线输入（看板专有）。市场环境分级在核心内
      统一计算，不再由入口注入。
    """

    announce_progress: Optional[Callable[[int, int, str, str, str], None]] = None
    state_commit: Optional[state_commit.RoundCommit] = None
    fetch_kline_fn: Optional[Callable[..., Any]] = None
    source_suffix: str = ""
    extra_meta: Any = None                      # dict 或返回 dict 的可调用对象
    build_extras: Optional[Callable[..., Dict[str, Any]]] = None
    save_kline_cache: Optional[Callable[[], None]] = None
    minute_map_provider: Optional[Callable[..., Dict[str, Any]]] = None
    on_enriched: Optional[Callable[[Dict[str, "Enriched"]], None]] = None
    snapshot_id: Optional[str] = None
    log: Optional[Callable[[str], None]] = None

    def log_line(self, message: str) -> None:
        if self.log is not None:
            try:
                self.log(message)
            except Exception:
                pass


def _display_rows(rows: Optional[List[Dict[str, Any]]], top: int) -> List[Dict[str, Any]]:
    """展示截取：只改变可见行数，不改变任何上游计算结果。"""
    if not rows:
        return []
    return list(rows[: max(0, int(top))])


def run_screening_core(
    params: ScreeningParams,
    hooks: Optional[ScreeningHooks] = None,
) -> Dict[str, Any]:
    """执行一次完整筛选并返回核心结果。

    阶段固定为：行情与质量判定 → 丰富数据 → 资金基准/否决 → 完整候选池 →
    公告核验及风险映射 → 资金排名 → 观察池突破 → 双池交集/准交集 →
    交集状态机 → 展示裁剪。看板专有的分钟量能、温度计、进出场建议由调用方附加。

    行情不可用时抛 ``NetworkUnavailable``，由调用方决定输出形式。
    """
    hooks = hooks or ScreeningHooks()
    modes = set(params.modes)
    if "all" in modes:
        modes = {"strict", "low", "watchlist"}
    if not modes:
        modes = {"strict"}

    set_network_mode(params.network_mode)
    MARKET_WARNINGS.clear()

    t0 = time.time()
    market, total = fetch_market()
    market_fetch_status = get_market_fetch_status()
    fallback_snapshot = market_fetch_status.get("source") == "sina_fallback"
    hooks.log_line(f"[计时] 行情分页: {time.time() - t0:.1f}s  ({len(market)} 条)")

    if fallback_snapshot:
        indices_raw: List[Dict[str, Any]] = []
        sector_boards: List[Dict[str, Any]] = []
        MARKET_WARNINGS.append("备用行情模式跳过东方财富指数和板块接口，避免主源故障导致二次等待")
    else:
        indices_raw = fetch_indices()
        sector_boards = fetch_sector_indices()

    boards_in_use = normalize_boards(params.enabled_boards)
    mode_list = ["all"] if modes == {"strict", "low", "watchlist"} else sorted(modes)
    prefetch = filter_prefetch(market, mode_list, boards=boards_in_use)
    enriched, errors = enrich_all(prefetch, max(1, params.workers), hooks.fetch_kline_fn)
    hooks.log_line(f"[计时] K线增强: {time.time() - t0:.1f}s  ({len(enriched)} 只)")
    if hooks.on_enriched is not None:
        hooks.on_enriched({e.code: e for e in enriched})
    if hooks.save_kline_cache is not None:
        hooks.save_kline_cache()

    if fallback_snapshot:
        MARKET_WARNINGS.append(
            "备用行情缺少兼容量比、主力资金和行业字段：趋势池仅供观察；"
            "超短池、双池交集、资金优选和低吸买点不会输出。"
        )
    breadth = market_summary(market, total, market_fetch_status)
    stats = sector_stats(market, breadth, boards=boards_in_use)

    # --- capital flow: load history, compute increments, classify ---
    flow_history = load_flow_history()
    has_snapshot = bool(flow_history)
    latest_ts = max([r.get("f124") or 0 for r in market] or [0])
    ts = datetime.fromtimestamp(latest_ts, TZ) if latest_ts else datetime.now(TZ)
    reset_flow_minute_round(ts.strftime("%Y-%m-%d"))
    apply_flow_increments(enriched, flow_history)
    # 快照基准缺失时（冷启动/断点重启/隔日首轮）用东财分钟序列补齐，否则资金门槛无法评估。
    flow_minute_filled = fill_flow_increments_from_fflow(
        enriched, expected_date=ts.strftime("%Y-%m-%d")
    )
    if flow_minute_filled:
        MARKET_WARNINGS.append(
            f"本地快照无可用基准，已用东财当日累计分钟序列为 {flow_minute_filled} 只候选补齐 5/15 分钟增量"
            "（来源 eastmoney_fflow_minutes）；分钟序列不足 15 根时 15 分钟增量仍记基准不足。"
        )
    for e in enriched:
        e.flow_veto = flow_veto_reason(e)
        e.flow_status = classify_flow(e, stats, has_snapshot)
    vetoed_codes = sorted({e.code for e in enriched if e.flow_veto})
    if vetoed_codes:
        MARKET_WARNINGS.append(
            "超大单为负（框架一票否决，禁止正式买入建议）：" + "、".join(vetoed_codes)
        )
    save_flow_history(enriched, flow_history, hooks.state_commit)
    hooks.log_line(
        f"[计时] 资金流向: 历史{'有' if has_snapshot else '无'} ({len(flow_history)} 只)"
        f"；分钟序列补齐 {flow_minute_filled} 只"
    )

    after_1420 = is_after_tail_risk(ts)

    # ── 完整候选池：排序后保留全量，top 只在最后裁剪展示 ──────────────
    strict_ultra_all = (
        []
        if fallback_snapshot
        else sorted([e for e in enriched if strict_ultra(e)], key=lambda e: e.change, reverse=True)
    )
    trend_observation_all = sorted(
        [e for e in enriched if trend_observation(e)],
        key=lambda e: (not strict_trend(e), -e.change, e.dist60),
    )
    strict_trend_all = sorted(
        [e for e in enriched if strict_trend(e)], key=lambda e: e.change, reverse=True
    )

    strict_ultra_all_rows: List[Dict[str, Any]] = [
        {**asdict(e), "resonance": "是" if has_resonance(e, stats) else "否"}
        for e in strict_ultra_all
    ] if "strict" in modes else []
    trend_observation_all_rows: List[Dict[str, Any]] = [
        {
            **asdict(e),
            "ma_state": "MA5/10/20上方" if e.price > e.ma5 and e.price > e.ma10 and e.price > e.ma20 else "MA20上方，短均线修复中",
        }
        for e in trend_observation_all
    ] if "strict" in modes else []
    strict_trend_all_rows: List[Dict[str, Any]] = [
        asdict(e) for e in strict_trend_all
    ] if "strict" in modes else []
    trend_diagnostics = [
        trend_condition_diagnosis(e, stats) for e in strict_ultra_all
    ] if "strict" in modes else []

    low_ultra_all_rows: List[Dict[str, Any]] = []
    low_trend_all_rows: List[Dict[str, Any]] = []
    if "low" in modes and not fallback_snapshot:
        for e in enriched:
            exclude, _reason = _should_exclude_from_low_absorb(e, flow_history)
            if exclude:
                continue  # 硬黑名单或高位派发降权，不进入低吸候选
            cls, tags, score = low_ultra_class(e, stats, after_1420)
            dom_type, dom_label = evaluate_dominance_type(e, flow_history)
            if low_ultra_output_eligible(e, cls):
                low_ultra_all_rows.append({
                    **asdict(e),
                    "class": cls,
                    "risk": "/".join(tags) if tags else "无",
                    "score": score,
                    "resonance": "是" if has_resonance(e, stats) else "否",
                    "dominance_type": dom_type,
                    "dominance_label": dom_label,
                    "super_lead": dom_label,
                })
            cls2, tags2, score2 = low_trend_class(e, stats, after_1420)
            if low_trend_output_eligible(e, cls2):
                low_trend_all_rows.append({
                    **asdict(e),
                    "class": cls2,
                    "risk": "/".join(tags2) if tags2 else "无",
                    "score": score2,
                    "ma_state": f"MA5/10/20上方,5日{'上行' if e.ma5 > e.prev_ma5 else '未上行'},10日{'走平上行' if e.ma10 >= e.prev_ma10 else '下行'}",
                    "dominance_type": dom_type,
                    "dominance_label": dom_label,
                    "super_lead": dom_label,
                })
        unverified = low_absorb_unverified_count()
        if unverified:
            MARKET_WARNINGS.append(
                f"低吸高位派发核验：{unverified} 只满足高位条件但既无本地快照也无分钟序列，"
                "派发信号未能核验、按未触发处理，建议人工复查。"
            )
        low_ultra_all_rows = sorted(low_ultra_all_rows, key=low_ultra_sort_key)
        low_trend_all_rows = sorted(low_trend_all_rows, key=low_trend_sort_key)

    raw_watchlist = build_watchlist(enriched, stats) if "watchlist" in modes else []

    ultra_codes = {r["code"] for r in strict_ultra_all_rows}
    observation_codes = {r["code"] for r in trend_observation_all_rows}
    confirmation_codes = {r["code"] for r in strict_trend_all_rows}
    relaxed_confirm_codes = {e.code for e in enriched if trend_confirm_relaxed(e)}
    enriched_by_code_for_pool = {e.code: e for e in enriched}

    # 双池交集基准：超短池 ∩ 趋势确认池(strict_trend)。交集即启动事件。
    board_by_code_for_pool = {b["name"]: b for b in sector_boards if b.get("name")}
    dual_pool_rows = [
        {
            **r,
            "resonance": "是" if has_resonance(enriched_by_code_for_pool[r["code"]], stats) else "否",
            "sector_change": (board_by_code_for_pool.get(r.get("industry"), {}) or {}).get("change"),
        }
        for r in strict_ultra_all_rows
        if r["code"] in relaxed_confirm_codes
    ]
    dual_pool_raw_rows = [dict(r) for r in dual_pool_rows]

    relevant_industries: set = set()
    for r in strict_ultra_all_rows + trend_observation_all_rows + strict_trend_all_rows:
        ind = r.get("industry", "")
        if ind and ind != "-":
            relevant_industries.add(ind)
    board_by_name = {b["name"]: b for b in sector_boards}
    sector_indices: List[Dict[str, Any]] = []
    for ind in sorted(relevant_industries):
        board = board_by_name.get(ind)
        if not board:
            for bn, bd in board_by_name.items():
                if ind in bn or bn in ind:
                    board = bd
                    break
        if board:
            sector_indices.append({
                "name": board["name"],
                "change": board["change"],
                "price": board["price"],
                "up_count": board["up_count"],
                "down_count": board["down_count"],
                "turnover": board["turnover"],
                "source": "筛选",
            })

    intersection_config, intersection_config_meta = resolve_intersection_config(
        load_intersection_calibration()
    )
    intersection_runtime_config = {
        **intersection_config,
        "version": intersection_config_meta["version"],
        "source": intersection_config_meta["source"],
    }

    diag_by_code = {d["code"]: d for d in trend_diagnostics} if "strict" in modes else {}
    pre_intersection_rows = (
        compute_pre_intersection(
            strict_ultra_all_rows, relaxed_confirm_codes, diag_by_code, intersection_runtime_config
        )
        if "strict" in modes else []
    )

    # 范围结论：区分「未符合条件」与「数据不可用」（与引擎共用同一判定）。
    # 候选数读**完整**内部集合，不能被 top 截短（否则 top 小的时候会误报"无候选"）。
    board_scope_status = board_scope_status_for(fallback_snapshot, market_fetch_status)
    board_scope_candidates = len({
        str(row.get("code"))
        for rows in (
            strict_ultra_all_rows, trend_observation_all_rows, strict_trend_all_rows,
            low_ultra_all_rows, low_trend_all_rows, raw_watchlist,
        )
        for row in (rows or [])
        if row.get("code")
    })

    extra_meta = hooks.extra_meta
    if callable(extra_meta):
        extra_meta = extra_meta()
    result: Dict[str, Any] = {
        "meta": {
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "status": current_status(ts),
            "source": (
                "新浪财经实时备用快照（字段降级）"
                if fallback_snapshot else "东方财富push2实时/快照"
            ) + " + " + kline_source_summary(enriched) + hooks.source_suffix,
            "total_rows": len(market),
            "provider_total": total,
            "market_fetch_complete": market_fetch_status.get("complete"),
            "prefetch_rows": len(prefetch),
            "enriched_rows": len(enriched),
            "elapsed_seconds": round(time.time() - t0, 1),
            "market_data_degraded": fallback_snapshot,
            # 交易板范围：本轮实际筛选范围（只决定筛哪些股票供观察，不授予买入资格）
            "enabled_boards": list(boards_in_use),
            "enabled_boards_label": " + ".join(BOARD_LABELS.get(b, b) for b in boards_in_use),
            "board_counts": {
                "market": count_by_board(market),
                "prefetch": count_by_board(prefetch),
                "strict_rows": count_by_board(enriched),
            },
            **(extra_meta or {}),
        },
        "breadth": breadth,
        "market_fetch_status": market_fetch_status,
        "indices": [{"code": x.get("f12"), "name": x.get("f14"), "price": x.get("f2"), "change": x.get("f3")} for x in indices_raw],
        "errors": errors,
        "warnings": MARKET_WARNINGS,
        "announcement_errors": [],
        "board_scope_status": board_scope_status,
        "board_scope_candidates": board_scope_candidates,
        "board_scope_note": board_scope_note(board_scope_status, boards_in_use, board_scope_candidates),
        "strict_enabled": "strict" in modes,
        # 以下候选池在公告核验期间保持全量；展示裁剪在函数末尾统一进行。
        "strict_ultra": strict_ultra_all_rows,
        "trend_observation": trend_observation_all_rows,
        "strict_trend": strict_trend_all_rows,
        "trend_diagnostics": trend_diagnostics,
        "dual_pool": dual_pool_rows,
        "dual_pool_raw": dual_pool_raw_rows,
        "pre_intersection": pre_intersection_rows,
        "intersection_states": [],
        "intersection_config": intersection_runtime_config,
        "intersection_config_meta": intersection_config_meta,
        "capital_rank": [],
        "low_ultra": low_ultra_all_rows,
        "low_trend": low_trend_all_rows,
        "watchlist": raw_watchlist,
        "sector_indices": sector_indices,
        "has_snapshot": has_snapshot,
        "flow_minute_filled": flow_minute_filled,
        "low_open_wash": low_open_wash_rows(enriched, expected_date=ts.strftime("%Y-%m-%d")),
    }

    # 看板专有列表（负超单观察等）在公告核验前并入，确保它们与正式池读到同一份风险结果。
    if hooks.build_extras is not None:
        extras = hooks.build_extras(
            enriched=enriched,
            stats=stats,
            flow_history=flow_history,
            ts=ts,
            fallback_snapshot=fallback_snapshot,
            market_fetch_status=market_fetch_status,
        )
        if extras:
            result.update(extras)

    # --- flow detail for key candidates (strict top 5 + low A-class top 5 + holdings) ---
    detail_codes: set = set()
    flow_detail: List[Dict[str, Any]] = []
    for e in (strict_ultra_all[:5] + trend_observation_all[:5] + strict_trend_all[:5]):
        if e.code not in detail_codes:
            detail_codes.add(e.code)
            flow_detail.append(asdict(e))
    for r in low_ultra_all_rows if "low" in modes else []:
        if r.get("class") == "A" and r["code"] not in detail_codes:
            detail_codes.add(r["code"])
            flow_detail.append(r)
            if len([d for d in flow_detail if d.get("class") == "A"]) >= 5:
                break
    holdings_file = SCRIPT_DIR / "holdings.json"
    enriched_by_code = {e.code: e for e in enriched}
    try:
        if holdings_file.exists():
            holdings_data = json.loads(holdings_file.read_text(encoding="utf-8"))
            for h in holdings_data:
                code = h.get("code", "")
                if code and code not in detail_codes:
                    detail_codes.add(code)
                    e = enriched_by_code.get(code)
                    if e:
                        d = asdict(e)
                        d["_holding"] = True
                        flow_detail.append(d)
    except Exception:
        pass
    result["flow_detail"] = flow_detail

    # --- 公告风控查询与统一门槛处理 ---
    if not params.skip_announcements:
        t_ann_start = time.time()
        result["announcement_errors"] = attach_announcement_risks(
            result,
            params.announcement_page_size,
            params.workers,
            progress_callback=hooks.announce_progress,
        )
        apply_announcement_pool_gates(result)
        hooks.log_line(f"[计时] 公告检查: {time.time() - t_ann_start:.1f}s")
    else:
        result["announcement_check_available"] = False
        result["announcement_unknown_codes"] = sorted({
            str(row.get("code"))
            for section in ANNOUNCEMENT_SECTIONS
            for row in (result.get(section) or [])
            if _row_risk_status(row) == RISK_UNKNOWN
        })
        apply_announcement_pool_gates(result)

    # 汇总统一公告风控字典。公告查询结果是唯一权威来源；其它池子只
    # 用来补齐查询结果缺失的代码，且缺失状态一律 unknown（fail-closed）。
    risk_map: Dict[str, str] = {
        str(code): (status if isinstance(status, str) and status in RISK_STATUS_VALUES else RISK_UNKNOWN)
        for code, status in (result.get("announcement_risk_map") or {}).items()
        if code
    }
    for section in ANNOUNCEMENT_SECTIONS:
        for row in (result.get(section) or []):
            code = str(row.get("code") or "")
            if code:
                risk_map.setdefault(code, _row_risk_status(row))

    # 将确切的公告风控状态同步赋给 Enriched 实例（资金排名读它判 clean 加分）。
    for e in enriched:
        e.risk_status = risk_map.get(e.code, RISK_UNKNOWN)

    # --- 在公告检查之后统一执行主力资金优选排序（确保 clean 硬门槛生效） ---
    # 候选集合读**完整**内部池；avoid/unknown 一票否决不进排名，watch_risk 保留。
    strict_candidate_codes = {
        r["code"]
        for r in (result.get("strict_ultra") or []) + (result.get("trend_observation") or [])
        if not risk_blocks_formal_qualification(_row_risk_status(r))
    }
    capital_rank_full = [] if params.skip_capital_ranking or fallback_snapshot else rank_capital_candidates(
        [e for e in enriched if e.code in strict_candidate_codes], stats, flow_history
    )
    for row in capital_rank_full:
        in_ultra = row["code"] in ultra_codes
        in_observation = row["code"] in observation_codes
        in_confirmation = row["code"] in confirmation_codes
        if in_ultra and in_observation:
            row["pool_source"] = "双池交集 + 趋势确认" if in_confirmation else "双池交集"
        elif in_ultra:
            row["pool_source"] = "超短池"
        else:
            row["pool_source"] = "趋势确认池" if in_confirmation else "趋势观察池"
    result["capital_rank"] = capital_rank_full
    # capital_rank 是公告查询完成后新生成的派生池，必须再次走同一套
    # fail-closed 门槛，避免 avoid/unknown 通过重新排名重新出现。
    apply_announcement_pool_gates(result)

    # 观察池构建与突破状态机评估（在公告检查之后执行，接入真实风控）
    # 持仓代码只用于状态剔除时保留监控（取消交易板不等于停止监控已持仓）
    holding_codes = load_holding_codes()
    wl_state_payload = load_watchlist_breakout_state()
    prev_wl_items = prune_state_by_boards(
        wl_state_payload.get("items") or {}, boards_in_use, holding_codes
    )
    is_today = (wl_state_payload.get("date") == ts.strftime("%Y-%m-%d"))
    if not is_today and prev_wl_items:
        # 跨日：保留昨日设定的触发价基准，重置日内确认计数为0
        for _code, itm in prev_wl_items.items():
            itm["confirm_count"] = 0
            itm["phase"] = "WATCHING"
            itm["breakout_class"] = "WATCHING"
    watchlist_evaluated, next_wl_items = evaluate_watchlist_breakout_states(
        raw_watchlist, enriched_by_code_for_pool, stats, flow_history, prev_wl_items, ts, risk_map
    )
    result["watchlist"] = watchlist_evaluated
    save_watchlist_breakout_state(next_wl_items, ts.strftime("%Y-%m-%d"), hooks.state_commit)

    if result.get("strict_enabled"):
        state_payload = load_intersection_state()
        previous_items = prune_state_by_boards(
            state_payload.get("items") or {}, boards_in_use, holding_codes
        )
        if state_payload.get("date") != ts.strftime("%Y-%m-%d"):
            previous_items = {}
        minute_map: Dict[str, Dict[str, Any]] = {}
        if hooks.minute_map_provider is not None and not fallback_snapshot:
            try:
                minute_map = hooks.minute_map_provider(result, previous_items) or {}
            except Exception as exc:  # noqa: BLE001
                result.setdefault("warnings", []).append(f"状态机分钟线拉取失败：{exc}")
        # 市场环境分级（宽度/指数极端）在核心内统一计算：CLI 与看板必须用同一
        # 口径判定 NORMAL/LIGHT/DOWNGRADE/CASH，CASH 一律禁止新开仓。
        market_context = resolve_market_mode(
            breadth, result.get("indices") or [], intersection_runtime_config
        )
        result["market_context"] = market_context
        state_rows, next_items = evaluate_intersection_states(
            result.get("dual_pool_raw") or [],
            result.get("pre_intersection") or [],
            previous_items,
            ts,
            intersection_runtime_config,
            snapshot_id=hooks.snapshot_id or ts.strftime("%Y-%m-%d %H:%M:%S"),
            risk_map=risk_map,
            minute_map=minute_map,
            market_context=market_context,
        )
        result["intersection_states"] = state_rows
        if minute_map:
            result["minute_fetch_log"] = minute_map
        save_intersection_state(next_items, ts.strftime("%Y-%m-%d"), hooks.state_commit)

    # ── 展示裁剪：top 至此才生效，只改变可见行数 ──────────────────────
    result["strict_ultra"] = _display_rows(result.get("strict_ultra"), params.top)
    result["trend_observation"] = _display_rows(result.get("trend_observation"), params.top)
    result["strict_trend"] = _display_rows(result.get("strict_trend"), params.top)
    result["low_ultra"] = _display_rows(result.get("low_ultra"), max(params.top, 15))
    result["low_trend"] = _display_rows(result.get("low_trend"), max(params.top, 15))
    result["capital_rank"] = _display_rows(result.get("capital_rank"), params.top)

    # 每行标注交易板（三板同表时必须可辨）。放在最后：资金优选、交集状态机、
    # 观察池突破升级后的观察池都是在此之后才生成的派生栏目，早标注会漏掉它们。
    stamp_board_fields(result)
    result["meta"]["elapsed_seconds"] = round(time.time() - t0, 1)
    hooks.log_line(f"[计时] 总计: {result['meta']['elapsed_seconds']:.1f}s")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="A-share screening query helper for daily-stock-analysis")
    parser.add_argument("--mode", nargs="+", choices=["all", "strict", "low", "watchlist"], default=["strict"], help="screening modules to output (default: original dual screen)")
    parser.add_argument("--format", choices=["md", "json"], default="md", help="output format")
    parser.add_argument("--workers", type=int, default=6, help="concurrent K-line workers")
    parser.add_argument("--top", type=int, default=10, help="max display rows per section")
    parser.add_argument("--save", type=Path, help="also save output to file")
    parser.add_argument("--skip-announcements", action="store_true", help="skip latest-announcement risk check")
    parser.add_argument("--skip-capital-ranking", action="store_true", help="skip capital-flow secondary ranking")
    parser.add_argument("--announcement-page-size", type=int, default=8, help="latest announcement rows per stock")
    parser.add_argument("--network-mode", choices=["auto", "direct", "proxy"], default="auto",
                        help="network mode: auto (measured fastest) / direct / proxy")
    parser.add_argument("--boards", nargs="+", choices=list(BOARD_ORDER), default=[BOARD_MAIN],
                        help="交易板范围（默认 main=仅沪深主板）。可任意非空组合；所选交易板"
                             "统一参与完整筛选（同一门槛/排名/状态机）。未勾选的交易板不进入新增候选。"
                             "本参数只决定筛选范围，不授予任何买入资格。")
    args = parser.parse_args()

    modes = set(args.mode)
    if "all" in modes:
        modes = {"strict", "low", "watchlist"}

    params = ScreeningParams(
        modes=frozenset(modes),
        workers=args.workers,
        top=args.top,
        skip_announcements=args.skip_announcements,
        skip_capital_ranking=args.skip_capital_ranking,
        network_mode=args.network_mode,
        announcement_page_size=args.announcement_page_size,
        enabled_boards=tuple(args.boards),
    )

    def announcement_progress(done: int, total: int, code: str, status: str, source: str) -> None:
        source_label = "缓存" if source == "cache" else "查询"
        print(f"[公告] {done}/{total} {code} {source_label}={status}", file=sys.stderr, flush=True)

    hooks = ScreeningHooks(
        announce_progress=announcement_progress,
        source_suffix="" if args.skip_announcements else " + 东方财富公告",
        log=lambda message: print(message, file=sys.stderr),
    )

    try:
        result = run_screening_core(params, hooks)
    except NetworkUnavailable as exc:
        print(format_network_failure(exc), file=sys.stderr)
        return 2

    # 背景数据在核心筛选、公告门禁和状态机完成后按独立缓存请求；任何失败只
    # 影响报告顶部证据，不改变候选池、排序或状态提交语义。
    try:
        result["market_background"] = build_market_background(
            (result.get("meta") or {}).get("timestamp", "")[:10] or None
        )
    except Exception as exc:
        result.setdefault("warnings", []).append(f"市场背景查询失败：{type(exc).__name__}: {exc}")

    if args.format == "json":
        output = json.dumps(_sanitize_for_json(result), ensure_ascii=False, indent=2)
    else:
        output = render_markdown(result)
    if args.save:
        args.save.parent.mkdir(parents=True, exist_ok=True)
        args.save.write_text(output, encoding="utf-8")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
