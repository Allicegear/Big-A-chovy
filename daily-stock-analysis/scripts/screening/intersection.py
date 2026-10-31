"""双池交集状态机（default-v2）：交集配置/标定、相位、准交集门槛、状态推进与持久化。

状态文件路径来自 runtime_paths（随 A_SHARE_STATE_DIR 隔离）；读写由核心
run_screening_core 调用，经 state_commit 回合提交门落盘。
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

SCRIPT_DIR = Path(__file__).resolve().parents[1]
_PROJECT_ROOT = SCRIPT_DIR.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
from tools.rule_config import RULE_CONFIG, get_rule_config  # noqa: E402

import runtime_paths  # noqa: E402  运行状态路径唯一来源
import state_commit  # noqa: E402  回合状态提交门
from screening.model import is_number  # noqa: E402
from screening.risk import (  # noqa: E402
    RISK_CLEAN,
    RISK_UNKNOWN,
    _row_risk_status,
    risk_allows_state_qualification,
    risk_blocks_formal_qualification,
    risk_note_text,
)

SCREENING_CONFIG = RULE_CONFIG["screening"]
TZ = ZoneInfo("Asia/Shanghai")

# The raw dual-pool intersection remains an audit signal.  This second layer
# tracks whether that signal is new, confirmed, late, or actually eligible
# for a new position.  It never reads holdings.json or uses simulated trades.
INTERSECTION_STATE_FILE = runtime_paths.state_file("intersection_state.json")
INTERSECTION_CALIBRATION_FILE = SCRIPT_DIR / "intersection_calibration.json"
DEFAULT_INTERSECTION_CONFIG = get_rule_config()["intersection"]
INTERSECTION_CONFIG_VERSION = "default-v2"

# ── default-v2 状态机相位（英文为规范值，中文仅供展示）──────────────
PHASE_OBSERVING = "OBSERVING"
PHASE_PRE = "PRE_INTERSECTION"
PHASE_LATCHED = "INTERSECTION_LATCHED"
PHASE_WAIT_RETEST = "WAIT_RETEST"
PHASE_RETEST_READY = "RETEST_READY"
PHASE_ENTRY = "ENTRY_ELIGIBLE"
PHASE_LATE = "LATE_INTERSECTION"
PHASE_INVALID = "INVALID"
PHASE_EXPIRED = "EXPIRED"

PHASE_LABELS = {
    PHASE_OBSERVING: "观察中",
    PHASE_PRE: "准交集",
    PHASE_LATCHED: "首次交集",
    PHASE_WAIT_RETEST: "等待回踩",
    PHASE_RETEST_READY: "回踩确认",
    PHASE_ENTRY: "可新开仓",
    PHASE_LATE: "迟到交集",
    PHASE_INVALID: "失效",
    PHASE_EXPIRED: "已过期",
}

# 严格推进顺序：同一轮快照最多前进一级，禁止跳级
PHASE_ORDER = [
    PHASE_OBSERVING, PHASE_PRE, PHASE_LATCHED,
    PHASE_WAIT_RETEST, PHASE_RETEST_READY, PHASE_ENTRY,
]


def _clock_minutes(value: str) -> int:
    hour, minute = (int(part) for part in value.split(":", 1))
    return hour * 60 + minute


def _format_clock(value: str) -> str:
    hour, minute = (int(part) for part in value.split(":", 1))
    return f"{hour:02d}:{minute:02d}"


def _config_is_safe(config: Dict[str, Any]) -> bool:
    try:
        confirmation = int(config["confirmation_snapshots"])
        morning = _clock_minutes(str(config["morning_cutoff"]))
        change = float(config["overheat_change_pct"])
        turnover = float(config["overheat_turnover_pct"])
        age = int(config["signal_age_window_minutes"])
        latch = int(config.get("intersection_latch_minutes", 15))
        pb_min = float(config.get("pullback_min_pct", 0.5))
        pb_max = float(config.get("pullback_max_pct", 1.5))
        pb_vol = float(config.get("pullback_vol_ratio", 0.7))
        late_chg = float(config.get("late_change_pct", 4.6))
        late_vwap = float(config.get("late_vwap_dist_pct", 1.2))
        late_to = float(config.get("late_turnover_pct", 7.0))
        recover = float(config.get("pullback_recover_pct", 0.4))
    except (KeyError, TypeError, ValueError):
        return False
    return (
        2 <= confirmation <= 3
        and 10 * 60 + 45 <= morning <= 11 * 60 + 15
        and 3 <= change <= 5
        and 6 <= turnover <= 10
        and 20 <= age <= 60
        and 5 <= latch <= 30
        and 0.2 <= pb_min <= 1.0
        and 1.0 <= pb_max <= 3.0
        and 0.3 <= pb_vol <= 0.9
        and 3.0 <= late_chg <= 6.0
        and 0.5 <= late_vwap <= 2.5
        and 4.0 <= late_to <= 12.0
        and 0.1 <= recover <= 1.5
        and _clock_minutes(str(config.get("afternoon_start", "13:05"))) == _clock_minutes("13:05")
        and _clock_minutes(str(config.get("afternoon_buy_deadline", "14:20"))) == _clock_minutes("14:20")
    )


def resolve_intersection_config(calibration: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Resolve defaults or a validated automatic calibration candidate.

    Calibration is intentionally conservative: it needs 20 completed T+1/T+3
    outcomes, an in-range candidate, and a metric better than the baseline.
    Otherwise the default configuration is returned without user tuning.
    """
    default = dict(DEFAULT_INTERSECTION_CONFIG)
    if not isinstance(calibration, dict):
        return default, {"version": INTERSECTION_CONFIG_VERSION, "source": "default", "reason": "样本不足，使用默认参数"}
    samples = calibration.get("samples") or []
    # Auto-calibration requires one completed T+1 and T+3 result for each of
    # 20 distinct trading days. A single aggregate return is not sufficient.
    outcomes: List[Dict[str, Any]] = []
    trading_days: set[str] = set()
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        trade_day = sample.get("trade_date") or sample.get("date") or sample.get("session_date")
        t1_return = sample.get("t1_return", sample.get("t1"))
        t3_return = sample.get("t3_return", sample.get("t3"))
        if trade_day and is_number(t1_return) and is_number(t3_return):
            day_text = str(trade_day)
            trading_days.add(day_text)
            outcomes.append(sample)
    candidate = calibration.get("candidate")
    if len(trading_days) < 20 or not isinstance(candidate, dict):
        return default, {"version": INTERSECTION_CONFIG_VERSION, "source": "default", "reason": "样本不足，使用默认参数"}
    merged = {**default, **candidate}
    if not _config_is_safe(merged):
        return default, {"version": INTERSECTION_CONFIG_VERSION, "source": "default", "reason": "候选参数超出安全范围，回退默认参数"}
    baseline_metric = calibration.get("baseline_metric")
    candidate_metric = calibration.get("candidate_metric")
    if not is_number(baseline_metric) or not is_number(candidate_metric) or candidate_metric <= baseline_metric:
        return default, {"version": INTERSECTION_CONFIG_VERSION, "source": "default", "reason": "表现未改善或缺少对照，回退默认参数"}
    version = str(calibration.get("version") or "auto-v1")
    return merged, {"version": version, "source": "auto", "reason": "至少20个已完成T+1/T+3样本且表现改善"}


def load_intersection_calibration() -> Optional[Dict[str, Any]]:
    try:
        payload = json.loads(INTERSECTION_CALIBRATION_FILE.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


# 旧版中文相位 → default-v2 规范相位（状态文件迁移用）
_LEGACY_PHASE_MAP = {
    "准交集": PHASE_PRE,
    "等待转强": PHASE_OBSERVING,
    "观察中": PHASE_OBSERVING,
    "首次交集": PHASE_LATCHED,
    "等待回踩": PHASE_WAIT_RETEST,
    "回踩确认": PHASE_RETEST_READY,
    "可试错": PHASE_RETEST_READY,
    "可新开仓": PHASE_ENTRY,
    "迟到交集": PHASE_LATE,
    "失效": PHASE_INVALID,
    "已过期": PHASE_EXPIRED,
    "信号消失": PHASE_EXPIRED,
}


def _canonical_phase(value: Any) -> str:
    text = str(value or "")
    if text in PHASE_LABELS:
        return text
    return _LEGACY_PHASE_MAP.get(text, PHASE_OBSERVING)


def load_intersection_state() -> Dict[str, Any]:
    """读取按交易日持久化的交集状态；程序重启后未过期锁存信号必须恢复。"""
    try:
        payload = json.loads(INTERSECTION_STATE_FILE.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and isinstance(payload.get("items"), dict):
            for item in payload["items"].values():
                if isinstance(item, dict):
                    item["phase"] = _canonical_phase(item.get("phase"))
            return payload
    except Exception:
        pass
    return {"date": "", "items": {}}


def save_intersection_state(
    items: Dict[str, Dict[str, Any]],
    date_text: str,
    commit: Optional[state_commit.RoundCommit] = None,
) -> None:
    """按交易日原子写入（tmp + os.replace），进程被杀不产生半截文件。

    ``commit`` 非空时只暂存：看板超时/失败的一轮不得提交交集状态。
    """
    for item in items.values():
        if isinstance(item, dict):
            item.setdefault("trade_date", date_text)
    state_commit.write_or_stage(
        commit,
        INTERSECTION_STATE_FILE,
        json.dumps({"date": date_text, "items": items}, ensure_ascii=False, indent=2),
    )


def _parse_intersection_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
    except (TypeError, ValueError):
        return None


def _intersection_rejection_reasons(row: Dict[str, Any]) -> List[str]:
    reasons: List[str] = []

    flow_status = str(row.get("flow_status") or "")
    if row.get("flow_veto"):
        reasons.append(str(row["flow_veto"]))
    if flow_status in {"疑似派发", "价量背离"}:
        reasons.append("主力资金反向")
    main_net = row.get("main_net")
    if not is_number(main_net):
        reasons.append("主力资金数据不足")
    elif main_net < 0:
        reasons.append("主力资金反向")
    elif main_net == 0 and flow_status in {"", "数据不足"}:
        reasons.append("主力资金数据不足")

    for field, label, missing_label in (
        ("flow_5m_inc", "5分钟资金反向", "5分钟资金数据不足"),
        ("flow_15m_inc", "15分钟资金反向", "15分钟资金数据不足"),
    ):
        value = row.get(field)
        if not is_number(value):
            reasons.append(missing_label)
        elif value < 0:
            reasons.append(label)
    if flow_status == "数据不足":
        reasons.append("资金数据不足")

    if row.get("price_above_vwap") is False or row.get("vwap_state") != "均价线上方":
        reasons.append("跌破均价线")

    resonance = row.get("resonance")
    if resonance in (None, "", "数据质量降级"):
        reasons.append("数据不足")
    elif resonance != "是":
        reasons.append("无板块共振")

    risk = _row_risk_status(row)
    if risk_blocks_formal_qualification(risk):
        reasons.append(f"公告风险否决({risk})")

    deduped: List[str] = []
    for reason in reasons:
        if reason not in deduped:
            deduped.append(reason)
    return deduped


def _predict_trigger_price(row: Dict[str, Any], missing: Optional[str]) -> Optional[float]:
    """Estimate the price at which the single missing strict-trend condition clears.

    Returns None when the missing condition is structural (e.g. turnover,
    volume, 5-day return) and cannot be expressed as a single trigger price.
    """
    if not missing:
        return None
    prev_close = row.get("prev_close")
    ma5 = row.get("ma5")
    ma10 = row.get("ma10")
    ma20 = row.get("ma20")
    trigger_multiplier = float(SCREENING_CONFIG["strict_trend"]["preintersection_trigger_multiplier"])
    if missing == "当日涨幅" and is_number(prev_close) and prev_close > 0:
        change_min = float(SCREENING_CONFIG["strict_trend"]["change_min_inclusive"])
        return round(prev_close * (1 + change_min / 100), 2)
    if missing == "价格相对MA5" and is_number(ma5) and ma5 > 0:
        return round(ma5 * trigger_multiplier, 2)
    if missing == "价格相对MA10" and is_number(ma10) and ma10 > 0:
        return round(ma10 * trigger_multiplier, 2)
    if missing == "价格相对MA20" and is_number(ma20) and ma20 > 0:
        return round(ma20 * trigger_multiplier, 2)
    # 距60日高点 / 换手率 / 成交额 / 量能 / 5日涨幅 / MA20斜率 / 价格区间 / 流通市值：
    # 结构性条件，无法用单一触发价表达
    return None


def _pre_gates(row: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Evaluate the four 准交集 quality gates from a current snapshot row.

    统一生成 gate_failures：失败原因描述"为什么没过"（含实际数值），
    绝不把满足态文案（如"5分钟资金为正"）当失败原因输出。
    """
    gates: Dict[str, Any] = {}
    gate_failures: List[str] = []

    main_net = row.get("main_net")
    main_pct = row.get("main_pct")
    if cfg.get("pre_gate_main_net", True):
        if not is_number(main_net):
            main_ok = False
            gate_failures.append("主力净占比缺失")
        elif main_net <= 0:
            main_ok = False
            if is_number(main_pct):
                gate_failures.append(f"主力净占比未为正({main_pct:.1f}%)")
            else:
                gate_failures.append(f"主力净额未为正({main_net / 1e4:+.0f}万)")
        else:
            main_ok = True
    else:
        main_ok = True

    fund_5m = row.get("flow_5m_inc")
    if cfg.get("pre_gate_flow_5m", True):
        if not is_number(fund_5m):
            flow_ok = False
            gate_failures.append("5分钟资金基准不足")
        elif fund_5m <= 0:
            flow_ok = False
            gate_failures.append(f"5分钟资金未为正({fund_5m / 1e4:+.0f}万)")
        else:
            flow_ok = True
    else:
        flow_ok = True

    above_vwap = row.get("price_above_vwap") is True
    if cfg.get("pre_gate_above_vwap", True):
        vwap_ok = above_vwap
        if not vwap_ok:
            gate_failures.append("价格位于均价线下")
    else:
        vwap_ok = True

    sector_resonance = row.get("resonance") == "是"
    # 板块共振已降为参考项（加权不否决），不再作为准交集门槛
    # 仍保留 resonance 值供评分/展示使用
    res_ok = True
    _ = sector_resonance  # 保留取值供下游使用，避免 lint 警告

    gates["main_net"] = main_ok
    gates["flow_5m"] = flow_ok
    gates["above_vwap"] = vwap_ok
    gates["resonance"] = res_ok
    gates["all"] = not gate_failures
    # funds gates (main+5m) used to distinguish 等待转强 from 观察中
    gates["funds_ok"] = main_ok and flow_ok
    gates["failures"] = gate_failures
    return gates


def _late_filter_reasons(row: Dict[str, Any], cfg: Dict[str, Any]) -> List[str]:
    """迟到过滤：首次交集时任一满足即标记迟到交集，不产生买点。"""
    reasons: List[str] = []
    change = row.get("change")
    vwap = row.get("vwap")
    price = row.get("price")
    turnover = row.get("turnover")
    high_pull = row.get("high_pull")
    vol_ratio = row.get("volume_ratio")
    if is_number(change) and change >= float(cfg["late_change_pct"]):
        reasons.append(f"涨幅过热({change:.1f}%≥{cfg['late_change_pct']}%)")
    if is_number(vwap) and is_number(price) and vwap > 0:
        dist = (price - vwap) / vwap * 100
        if dist >= float(cfg["late_vwap_dist_pct"]):
            reasons.append(f"距VWAP过远({dist:.1f}%≥{cfg['late_vwap_dist_pct']}%)")
    if is_number(turnover) and turnover >= float(cfg["late_turnover_pct"]):
        reasons.append(f"换手过热({turnover:.1f}%≥{cfg['late_turnover_pct']}%)")
    if is_number(high_pull) and high_pull >= float(cfg["late_high_pull_pct"]):
        reasons.append(f"高位回撤({high_pull:.1f}%≥{cfg['late_high_pull_pct']}%)")
    # 单根脉冲大阳线：涨得多、几乎无回撤、巨量
    if (
        is_number(change) and change >= float(cfg["late_pulse_change_pct"])
        and is_number(high_pull) and high_pull <= float(cfg["late_pulse_high_pull_pct"])
        and is_number(vol_ratio) and vol_ratio >= float(cfg["late_pulse_vol_ratio"])
    ):
        reasons.append("单根脉冲大阳线")
    if row.get("resonance") != "是":
        reasons.append("板块无共振")
    return reasons


def _pullback_zone(trigger_price: Optional[float], cfg: Dict[str, Any]) -> str:
    if not is_number(trigger_price) or trigger_price <= 0:
        return "-"
    lo = trigger_price * (1 - float(cfg["pullback_max_pct"]) / 100)
    hi = trigger_price * (1 - float(cfg["pullback_min_pct"]) / 100)
    return f"{lo:.2f}–{hi:.2f}"


def resolve_market_mode(
    breadth: Optional[Dict[str, Any]],
    indices: Optional[List[Dict[str, Any]]] = None,
    cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """市场环境分级：宽度 → NORMAL/LIGHT/DOWNGRADE/CASH；指数极端时至少降一级。

    宽度 = 上涨家数 / 有效涨跌家数 × 100。数据缺失按 CASH（fail-closed）。
    """
    c = {**DEFAULT_INTERSECTION_CONFIG, **(cfg or {})}
    adv = (breadth or {}).get("adv")
    valid = (breadth or {}).get("valid_change")
    breadth_pct: Optional[float] = None
    if is_number(adv) and is_number(valid) and valid > 0:
        breadth_pct = adv / valid * 100
    reasons: List[str] = []
    if breadth_pct is None:
        mode = "CASH"
        reasons.append("市场宽度数据缺失，按空仓环境处理")
    elif breadth_pct >= float(c["market_breadth_normal"]):
        mode = "NORMAL"
    elif breadth_pct >= float(c["market_breadth_light"]):
        mode = "LIGHT"
    elif breadth_pct >= float(c["market_breadth_downgrade"]):
        mode = "DOWNGRADE"
    else:
        mode = "CASH"
    if breadth_pct is not None:
        reasons.append(f"市场宽度{breadth_pct:.1f}%")
    # 指数极端条件（如创业板跌幅≤-5%）：环境至少降一级
    order = ["NORMAL", "LIGHT", "DOWNGRADE", "CASH"]
    extreme_codes = {str(x) for x in (c.get("index_extreme_codes") or [])}
    threshold = float(c["index_extreme_change_pct"])
    for idx in indices or []:
        code = str(idx.get("code") or "")
        change = idx.get("change")
        if code in extreme_codes and is_number(change) and change <= threshold:
            pos = order.index(mode)
            if pos < len(order) - 1:
                mode = order[pos + 1]
            reasons.append(
                f"指数极端({idx.get('name') or code} {change:.1f}%≤{threshold}%)，环境降一级"
            )
            break
    return {
        "market_mode": mode,
        "breadth_pct": round(breadth_pct, 1) if breadth_pct is not None else None,
        "reason": "；".join(reasons),
    }


def _minute_freshness(
    minute_info: Optional[Dict[str, Any]], cfg: Dict[str, Any]
) -> Tuple[bool, Optional[float], str, str]:
    """分钟K新鲜度：返回 (fresh, age_seconds, status, block_reason)。

    过期/缺失/失败的数据不得用于回踩确认，也不得当成资金流出。
    """
    limit = float(cfg.get("minute_fresh_seconds", 180))
    if not minute_info:
        return False, None, "fetch_failed", "分钟K缺失"
    age = minute_info.get("age_seconds")
    status = str(minute_info.get("status") or "")
    if status == "fetch_failed" and not is_number(age):
        return False, None, "fetch_failed", "分钟K获取失败"
    if not is_number(age):
        return False, None, "fetch_failed", "分钟K无有效时间戳"
    if status != "fresh":
        if status == "stale" and age > limit:
            return False, float(age), "stale", f"分钟K过期({age:.0f}秒)"
        return False, float(age), status or "unknown", "分钟K来源时间未验证"
    if age < 0:
        return False, float(age), "stale", "分钟K来源时间晚于当前时间"
    if age > limit:
        return False, float(age), "stale", f"分钟K过期({age:.0f}秒)"
    return True, float(age), "fresh", ""


def compute_pre_intersection(
    strict_ultra_rows: List[Dict[str, Any]],
    confirmation_codes: set,
    diag_by_code: Dict[str, Dict[str, Any]],
    cfg: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """准交集预警：超短池 + 距趋势确认(质量条件)仅差1项 + 前置门槛通过（相位由 evaluate 决定）。

    前置门槛为 主力净额 / 5分钟资金 / 均价线上；板块共振已降为参考项，不单独否决。
    缺失条件与"差一项"判定直接用行内字段（价格相对MA/MA20斜率/当日涨幅/5日涨幅/
    距60高/换手/成交/量能），不含价格区间与流通市值（已由超短池约束），避免错误
    排除哈药(~5.8)等低价强势股。候选行携带 preintersection_missing 与 trigger_price。
    """
    strict_cfg = SCREENING_CONFIG["strict_trend"]
    out: List[Dict[str, Any]] = []
    for r in strict_ultra_rows:
        code = str(r.get("code"))
        if code in confirmation_codes:
            continue  # 已进入正式交集，不算准交集
        checks = [
            ("价格相对MA5", is_number(r.get("ma5")) and r["price"] > r["ma5"]),
            ("价格相对MA10", is_number(r.get("ma10")) and r["price"] > r["ma10"]),
            ("价格相对MA20", is_number(r.get("ma20")) and r["price"] > r["ma20"]),
            ("MA20斜率", is_number(r.get("prev_ma20")) and r["ma20"] > r["prev_ma20"]),
            (
                "当日涨幅",
                is_number(r.get("change"))
                and float(strict_cfg["change_min_inclusive"]) <= r["change"] <= float(strict_cfg["change_max_inclusive"]),
            ),
            (
                "5日涨幅",
                is_number(r.get("five_ret")) and r["five_ret"] <= float(strict_cfg["five_ret_max_inclusive"]),
            ),
            (
                "距60日高点",
                is_number(r.get("dist60")) and r["dist60"] <= float(strict_cfg["dist60_max_inclusive"]),
            ),
            (
                "换手率",
                is_number(r.get("turnover"))
                and float(strict_cfg["turnover_min_inclusive"]) <= r["turnover"] <= float(strict_cfg["turnover_max_inclusive"]),
            ),
            (
                "成交额",
                is_number(r.get("amount")) and r["amount"] > float(strict_cfg["amount_min_exclusive"]),
            ),
            (
                "量能相对5日",
                is_number(r.get("vol_vs_avg5")) and r["vol_vs_avg5"] <= float(strict_cfg["vol_vs_avg5_max_inclusive"]),
            ),
        ]
        failures = [name for name, ok in checks if not ok]
        trend_missing_count = len(failures)
        if trend_missing_count != 1:
            continue  # 距趋势确认不止差1项
        missing = failures[0]
        row = dict(r)
        gates = _pre_gates(r, cfg)
        gate_failures = list(gates.get("failures") or [])
        # 准交集硬门槛：差1项 且 无任何 gate_failures。
        # 有任何 gate_failures 时只能进入"观察中·门槛未过"，不得进入准交集。
        pre_intersection_eligible = trend_missing_count == 1 and not gate_failures
        if pre_intersection_eligible:
            phase = "准交集"
        elif gates.get("funds_ok") and not gates.get("above_vwap"):
            phase = "等待转强"   # 资金+共振已确认，仅价格低于VWAP（仍属观察，不预警买点）
        else:
            phase = "观察中"     # 前置门槛未过，不进入预警
        risk = _row_risk_status(r)
        row["intersection_phase"] = phase
        row["pre_intersection_eligible"] = pre_intersection_eligible
        row["preintersection_missing"] = missing
        row["trigger_price"] = _predict_trigger_price(r, missing)
        row["_gates"] = gates
        row["gate_failures"] = gate_failures
        row["gate_failure_text"] = "；".join(gate_failures) if gate_failures else "全部通过"
        row["risk_note"] = (
            risk_note_text(risk) if (phase in ("准交集", "等待转强") and risk != RISK_CLEAN) else ""
        )
        out.append(row)
    return out


def evaluate_intersection_states(
    intersection_rows: List[Dict[str, Any]],
    pre_rows: List[Dict[str, Any]],
    previous_state: Dict[str, Dict[str, Any]],
    now: datetime,
    config: Optional[Dict[str, Any]] = None,
    snapshot_id: Optional[str] = None,
    risk_map: Optional[Dict[str, str]] = None,
    minute_map: Optional[Dict[str, Dict[str, Any]]] = None,
    market_context: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """default-v2 严格顺序状态机。

    OBSERVING → PRE_INTERSECTION → INTERSECTION_LATCHED → WAIT_RETEST
      → RETEST_READY → ENTRY_ELIGIBLE
    任意阶段 → INVALID；任意锁存阶段 → EXPIRED；首次交集过热 → LATE_INTERSECTION。
    禁止同一轮快照跳级（每快照最多前进一级）。

    - snapshot_id：独立快照标识（报告数据时间/行情批次），连续确认按不同
      snapshot_id 计数，同一份数据不得重复计数。
    - risk_map：按代码索引的统一公告风险，状态机不重新查询/解析风险。
    - minute_map：code -> {age_seconds,status,last_bar_at,close_5m,vwap_5m,vol_5m}；
      分钟K过期(>minute_fresh_seconds)是回踩确认硬否决，但不推断资金流出。
    - market_context：{market_mode, breadth_pct, reason}；CASH 一律禁止新开仓。
    每个快照只读取"当前可见字段 + 已持久化历史"，不使用任何未来数据。
    """
    if now.tzinfo is None:
        now = now.replace(tzinfo=TZ)
    cfg = {**DEFAULT_INTERSECTION_CONFIG, **(config or {})}
    now_text = now.strftime("%Y-%m-%d %H:%M:%S")
    date_text = now.strftime("%Y-%m-%d")
    latch_window = int(cfg["intersection_latch_minutes"])
    pre_need = max(1, int(cfg.get("pre_confirm_snapshots", 2)))
    retest_need = max(1, int(cfg.get("retest_confirm_snapshots", 2)))
    deadline_seconds = _clock_minutes(str(cfg["afternoon_buy_deadline"])) * 60
    now_seconds = now.hour * 3600 + now.minute * 60 + now.second
    past_deadline = now_seconds > deadline_seconds

    snap_id = str(snapshot_id or now_text)
    risk_map = risk_map or {}
    minute_map = minute_map or {}
    mctx = market_context or {}
    market_mode = str(mctx.get("market_mode") or "NORMAL")
    breadth_pct = mctx.get("breadth_pct")

    current_inter: Dict[str, Dict[str, Any]] = {str(r.get("code")): r for r in intersection_rows}
    current_pre: Dict[str, Dict[str, Any]] = {str(r.get("code")): r for r in pre_rows}

    next_state: Dict[str, Dict[str, Any]] = {
        code: dict(value) for code, value in (previous_state or {}).items()
    }
    results: List[Dict[str, Any]] = []

    LATCHED_FAMILY = {PHASE_LATCHED, PHASE_WAIT_RETEST, PHASE_RETEST_READY, PHASE_ENTRY, PHASE_LATE}

    def _risk_for(code: str, row: Dict[str, Any]) -> str:
        # 需求2：所有模块从同一份按代码索引的公告风险结果读取；缺失=unknown
        if code in risk_map:
            status = str(risk_map[code])
            return status if status else RISK_UNKNOWN
        return _row_risk_status(row)

    def _entry_allowed(risk: str, flow_veto: str = "") -> Tuple[bool, str]:
        """新开仓资格 = 公告非一票否决 + 超大单非负 + 未过14:20 + 市场环境非CASH。

        框架一票否决有两条在这台状态机上生效：公告 avoid/unknown，以及超大单为负。
        watch_risk 仅减分，不阻断状态资格（见 选股框架.md「一、信号与建仓门禁」）。
        DOWNGRADE 环境的附加条件（clean+共振+回踩确认）已是 ENTRY_ELIGIBLE
        的必要条件；CASH 一律禁止。
        """
        if risk_blocks_formal_qualification(risk):
            return False, f"公告风险否决({risk})"
        if flow_veto:
            return False, f"{flow_veto}（框架一票否决）"
        if past_deadline:
            return False, "已过新开仓截止，仅供明日观察"
        if market_mode == "CASH":
            return False, f"市场环境CASH(宽度{breadth_pct}%)，暂停新开仓"
        return True, ""

    def _count_confirm(item: Dict[str, Any], count_key: str, snap_key: str, passed: bool) -> int:
        """连续确认按不同快照计数：同一 snapshot_id 不重复计数（需求6）。"""
        if item.get(snap_key) == snap_id:
            return int(item.get(count_key) or 0)
        item[snap_key] = snap_id
        item[count_key] = (int(item.get(count_key) or 0) + 1) if passed else 0
        return item[count_key]

    def _expires_text(base: datetime) -> str:
        return (base + timedelta(minutes=latch_window)).strftime("%Y-%m-%d %H:%M:%S")

    def _emit(row: Dict[str, Any], item: Dict[str, Any], display_label: Optional[str] = None) -> None:
        code = str(row.get("code"))
        phase_code = _canonical_phase(item.get("phase"))
        label = display_label or PHASE_LABELS.get(phase_code, phase_code)
        risk = item.get("risk_status") or _risk_for(code, row)
        first = _parse_intersection_datetime(item.get("first_intersection_at")) or now
        age = round(max(0.0, (now - first).total_seconds() / 60), 1)
        minute_info = minute_map.get(code)
        fresh, m_age, m_status, _m_block = _minute_freshness(minute_info, cfg)
        flow_veto = str(row.get("flow_veto") or "")
        allowed, block = _entry_allowed(risk, flow_veto)
        entry_blocks: List[str] = []
        gate_failures = row.get("gate_failures") or []
        if isinstance(gate_failures, str):
            gate_failures = [gate_failures]
        gate_failures = [str(value) for value in gate_failures if str(value)]
        if phase_code == PHASE_ENTRY:
            if code not in current_inter:
                entry_blocks.append("本轮未出现在双池交集")
            else:
                current_gates = _pre_gates(current_inter[code], cfg)
                gate_failures.extend(
                    str(value) for value in (current_gates.get("failures") or []) if str(value)
                )
            gate_failures = list(dict.fromkeys(gate_failures))
            entry_blocks.extend(gate_failures)
            if not fresh:
                entry_blocks.append(f"分钟K证据{m_status}")
            if not allowed:
                entry_blocks.append(block)
        eligible = phase_code == PHASE_ENTRY and not entry_blocks
        entry_block = "；".join(dict.fromkeys(entry_blocks))
        if phase_code != PHASE_ENTRY:
            entry_block = item.get("entry_block_reason") or ("" if allowed else block)
        row.update({
            "intersection_phase": label,
            "phase_code": phase_code,
            "intersection_state": label,            # 兼容旧前端字段
            "signal_age_minutes": age,
            "signal_age": f"{age:.1f}分钟",
            "first_pre_at": item.get("first_pre_at"),
            "first_intersection_at": item.get("first_intersection_at"),
            "preintersection_at": item.get("first_pre_at") or item.get("preintersection_at"),
            "preintersection_missing": item.get("preintersection_missing") or row.get("preintersection_missing"),
            "trigger_price": item.get("trigger_price") if item.get("trigger_price") is not None else row.get("trigger_price"),
            "trigger_vwap": item.get("trigger_vwap"),
            "trigger_high": item.get("trigger_high"),
            "trigger_flow_5m": item.get("trigger_flow_5m"),
            "trigger_sector_strength": item.get("trigger_sector_strength"),
            "expires_at": item.get("expires_at"),
            "consecutive_confirmations": int(item.get("consecutive_confirmations") or 0),
            "pullback_pct": item.get("pullback_pct"),
            "late_flag": bool(item.get("late_flag")),
            "late_reasons": item.get("late_reasons") or [],
            "late_reason": "；".join(item.get("late_reasons") or []) or "无",
            "invalid_reason": item.get("invalid_reason"),
            "failure_reason": item.get("invalid_reason") or item.get("failure_reason") or "-",
            "data_block_reason": item.get("data_block_reason") or "",
            "pullback_zone": _pullback_zone(item.get("trigger_high") or item.get("trigger_price"), cfg),
            "actionable": eligible,
            "new_open_eligible": eligible,
            "new_entry_allowed": allowed,
            "entry_block_reason": entry_block,
            "risk_note": risk_note_text(risk),
            "announcement_risk": risk,
            "risk_status": risk,
            "minute_data_fresh": fresh,
            "minute_data_stale": m_status != "fresh",
            "minute_status": m_status,
            "minute_age_seconds": m_age,
            "minute_last_bar_at": (minute_info or {}).get("last_bar_at"),
            "market_mode": market_mode,
            "market_breadth_pct": breadth_pct,
            "past_entry_deadline": past_deadline,
            "deadline_note": "已过新开仓截止，仅供明日观察" if past_deadline else "",
            "gate_failures": gate_failures,
            "gate_failure_text": (
                "；".join(gate_failures) or "全部通过"
                if phase_code == PHASE_ENTRY
                else row.get("gate_failure_text") or "全部通过"
            ),
            "intersection_config_version": str(cfg.get("version", INTERSECTION_CONFIG_VERSION)),
            "intersection_config_source": str(cfg.get("source", "default")),
        })
        results.append(row)

    def _step_retest(item: Dict[str, Any], row: Dict[str, Any], code: str, risk: str) -> None:
        """已锁存(非迟到)信号的严格顺序推进：LATCHED→WAIT_RETEST→RETEST_READY→ENTRY_ELIGIBLE。

        每快照最多前进一级；失效条件命中立即 INVALID。
        分钟K过期时不得推进到 RETEST_READY/ENTRY_ELIGIBLE，也不推断资金流出。
        """
        phase = _canonical_phase(item.get("phase"))
        price = row.get("price")
        vwap = row.get("vwap")
        flow5 = row.get("flow_5m_inc")
        flow15 = row.get("flow_15m_inc")
        resonance = row.get("resonance") == "是"

        # 更新触发后高点（回踩幅度基准）
        trigger_high = item.get("trigger_high") or item.get("trigger_price")
        for candidate_high in (row.get("high"), price):
            if is_number(candidate_high) and is_number(trigger_high):
                trigger_high = max(trigger_high, candidate_high)
            elif is_number(candidate_high) and trigger_high is None:
                trigger_high = candidate_high
        item["trigger_high"] = trigger_high

        minute_info = minute_map.get(code)
        fresh, _m_age, m_status, m_block = _minute_freshness(minute_info, cfg)
        close_5m = (minute_info or {}).get("close_5m")
        vwap_5m = (minute_info or {}).get("vwap_5m")
        vol_5m = (minute_info or {}).get("vol_5m")
        impulse = item.get("impulse_5m_volume")
        if not is_number(impulse) and is_number(vol_5m) and fresh:
            item["impulse_5m_volume"] = vol_5m   # 迟补启动量基准（首次可用的新鲜分钟量）
            impulse = vol_5m

        pullback_pct = None
        if is_number(trigger_high) and trigger_high > 0 and is_number(price):
            pullback_pct = (trigger_high - price) / trigger_high * 100
        item["pullback_pct"] = round(pullback_pct, 2) if pullback_pct is not None else None

        # ── 立即失效条件（需求7）──
        below_vwap_snap = (row.get("price_above_vwap") is False) or (
            is_number(vwap) and is_number(price) and vwap > 0 and price < vwap
        )
        # 分钟K新鲜时用 close_5m/vwap_5m 判定；过期/缺失时退回快照VWAP，
        # 绝不把分钟数据缺失当成资金流出。
        if fresh and is_number(close_5m) and is_number(vwap_5m):
            below_vwap = close_5m < vwap_5m
        else:
            below_vwap = below_vwap_snap
        if below_vwap and is_number(flow5) and flow5 <= 0:
            item["phase"] = PHASE_INVALID
            item["invalid_reason"] = "跌破VWAP且5分钟资金转负"
            return
        if pullback_pct is not None and pullback_pct > float(cfg["pullback_max_pct"]):
            item["phase"] = PHASE_INVALID
            item["invalid_reason"] = f"回撤超限({pullback_pct:.1f}%>{cfg['pullback_max_pct']}%)"
            return
        if not resonance:
            item["phase"] = PHASE_INVALID
            item["invalid_reason"] = "板块共振消失"
            return

        # 首次交集只记录启动事件：下一快照才进入等待回踩（禁止同轮跳级）
        if phase == PHASE_LATCHED:
            item["phase"] = PHASE_WAIT_RETEST
            item["consecutive_confirmations"] = 0
            item["last_confirm_snapshot_id"] = None
            return

        # ── 回踩确认条件（需求3/7）──
        volume_contracted = (
            is_number(vol_5m) and is_number(impulse) and impulse > 0
            and vol_5m <= impulse * float(cfg["pullback_vol_ratio"])
        )
        retest_structure_ok = (
            pullback_pct is not None
            and float(cfg["pullback_min_pct"]) <= pullback_pct <= float(cfg["pullback_max_pct"])
            and volume_contracted
            and fresh                      # 分钟K过期 → 硬否决回踩确认
        )
        retest_confirmed = (
            retest_structure_ok
            and is_number(close_5m) and is_number(vwap_5m) and close_5m >= vwap_5m
            and is_number(flow5) and flow5 > 0
            and is_number(flow15) and flow15 > 0
            and resonance
            and risk_allows_state_qualification(risk)   # avoid/unknown 一票否决；watch_risk 仅减分
        )
        if not fresh:
            retest_confirmed = False
            item["data_block_reason"] = m_block
        else:
            item["data_block_reason"] = ""
        item["retest_confirmed"] = bool(retest_confirmed)

        count = _count_confirm(item, "consecutive_confirmations", "last_confirm_snapshot_id", retest_confirmed)

        if phase == PHASE_WAIT_RETEST:
            if retest_confirmed and count >= 1:
                item["phase"] = PHASE_RETEST_READY   # 第1次确认
            return
        if phase == PHASE_RETEST_READY:
            if not retest_confirmed:
                item["phase"] = PHASE_WAIT_RETEST    # 确认中断，回到等待并重新计数
                return
            if count >= retest_need:                 # 连续第2次确认（不同快照）
                allowed, block = _entry_allowed(risk, str(row.get("flow_veto") or ""))
                if allowed and not past_deadline:    # 两次确认都必须在14:20前完成
                    item["phase"] = PHASE_ENTRY
                    item["entry_confirmed_at"] = now_text
                    item["entry_block_reason"] = ""
                else:
                    item["entry_block_reason"] = block
            return
        if phase == PHASE_ENTRY:
            # 维持资格需条件持续成立；结构失守但未触发失效 → 退回等待回踩
            if not retest_confirmed:
                item["phase"] = PHASE_WAIT_RETEST
                item["invalid_reason"] = None
            return

    def _new_item(code: str) -> Dict[str, Any]:
        return {
            "trade_date": date_text,
            "code": code,
            "phase": PHASE_OBSERVING,
            "first_pre_at": None,
            "first_intersection_at": None,
            "trigger_price": None,
            "trigger_vwap": None,
            "trigger_high": None,
            "expires_at": None,
            "consecutive_confirmations": 0,
            "last_snapshot_at": None,
            "last_minute_bar_at": None,
            "invalid_reason": None,
        }

    # ── 1) 当前正式交集行（锁存事件 + 迟到过滤 + 严格顺序推进）──
    for code, row in current_inter.items():
        old = next_state.get(code)
        item = dict(old) if old else _new_item(code)
        cur_phase = _canonical_phase(item.get("phase"))
        item["trade_date"] = date_text
        item["active"] = True
        item["last_seen_at"] = now_text
        item["last_snapshot_at"] = now_text
        item["name"] = row.get("name") or item.get("name")
        risk = _risk_for(code, row)
        item["risk_status"] = risk
        minute_info = minute_map.get(code)
        if (minute_info or {}).get("last_bar_at"):
            item["last_minute_bar_at"] = minute_info["last_bar_at"]

        if cur_phase == PHASE_EXPIRED:
            # 过期后重新出现在交集：视为全新启动事件
            item = _new_item(code)
            item["active"] = True
            item["last_seen_at"] = now_text
            item["last_snapshot_at"] = now_text
            item["name"] = row.get("name")
            item["risk_status"] = risk
            cur_phase = PHASE_OBSERVING

        if cur_phase == PHASE_INVALID:
            # 当日失效信号不复活，不产生买点
            _emit(row, item)
            next_state[code] = item
            continue

        if not item.get("first_intersection_at"):
            # 首次交集事件：锁存触发信息 + 迟到过滤（只记录启动，不产生买点）
            item["first_intersection_at"] = now_text
            item["trigger_price"] = row.get("price")
            item["trigger_vwap"] = row.get("vwap")
            item["trigger_high"] = row.get("high") if is_number(row.get("high")) else row.get("price")
            item["trigger_flow_5m"] = row.get("flow_5m_inc")
            item["trigger_sector_strength"] = row.get("resonance")
            item["impulse_5m_volume"] = (minute_info or {}).get("vol_5m")
            item["expires_at"] = _expires_text(now)
            late_reasons = _late_filter_reasons(row, cfg)
            if late_reasons:
                item["late_flag"] = True
                item["late_reasons"] = late_reasons
                item["phase"] = PHASE_LATE          # 首次交集过热 → 迟到交集
            else:
                item["late_flag"] = False
                item["late_reasons"] = []
                item["phase"] = PHASE_LATCHED       # 启动事件，不直接产生买点
                item["consecutive_confirmations"] = 0
                item["last_confirm_snapshot_id"] = None
        else:
            item["expires_at"] = _expires_text(now)  # 仍在交集内，刷新锁存期
            if item.get("late_flag"):
                item["phase"] = PHASE_LATE
            else:
                _step_retest(item, row, code, risk)
        _emit(row, item)
        next_state[code] = item

    # ── 2) 准交集候选（未同时在正式交集）──
    for code, row in current_pre.items():
        if code in current_inter:
            continue
        old = next_state.get(code)
        item = dict(old) if old else _new_item(code)
        cur_phase = _canonical_phase(item.get("phase"))
        item["trade_date"] = date_text
        item["active"] = True
        item["last_seen_at"] = now_text
        item["last_snapshot_at"] = now_text
        item["name"] = row.get("name") or item.get("name")
        risk = _risk_for(code, row)
        item["risk_status"] = risk

        # 已锁存过交集的股票掉回准交集候选：保持锁存相位，检查过期
        if item.get("first_intersection_at") and cur_phase in LATCHED_FAMILY | {PHASE_INVALID}:
            expires = _parse_intersection_datetime(item.get("expires_at"))
            if cur_phase != PHASE_INVALID and expires and now > expires:
                item["phase"] = PHASE_EXPIRED
                item["invalid_reason"] = item.get("invalid_reason") or "锁存期满"
                item["active"] = False
            _emit(row, item)
            next_state[code] = item
            continue

        gate_failures = row.get("gate_failures") or []
        eligible = bool(row.get("pre_intersection_eligible"))
        count = _count_confirm(item, "pre_confirmations", "last_pre_snapshot_id", eligible)

        display: Optional[str] = None
        if eligible and count >= pre_need:
            if not item.get("first_pre_at"):
                item["first_pre_at"] = now_text
                item["preintersection_at"] = now_text
            item["phase"] = PHASE_PRE
            item["preintersection_missing"] = row.get("preintersection_missing")
            if item.get("trigger_price") is None:
                item["trigger_price"] = row.get("trigger_price")
            item["invalid_reason"] = None
            item["pre_note"] = ""
        elif eligible:
            item["phase"] = PHASE_OBSERVING
            item["pre_note"] = f"准交集候选·连续确认{count}/{pre_need}"
            display = "观察中"
        else:
            # 有任何 gate_failures：只能"观察中·门槛未过"，不得进入准交集
            if cur_phase == PHASE_PRE:
                item["pre_exit_reason"] = "；".join(gate_failures) or "门槛回落"
                item["pre_exit_at"] = now_text
            item["phase"] = PHASE_OBSERVING
            item["pre_note"] = "观察中·门槛未过" if gate_failures else "观察中"
            # 等待转强：资金+共振已过、仅VWAP下（仍属观察，不预警买点）
            row_label = row.get("intersection_phase")
            display = row_label if row_label == "等待转强" else "观察中"
        row["pre_note"] = item.get("pre_note") or ""
        row["pre_exit_reason"] = item.get("pre_exit_reason") or ""
        _emit(row, item, display)
        next_state[code] = item

    # ── 3) 本快照掉出候选的锁存信号：latch 窗口内保留，超期 EXPIRED ──
    for code, item in list(next_state.items()):
        if code in current_inter or code in current_pre:
            continue
        cur_phase = _canonical_phase(item.get("phase"))
        if not item.get("first_intersection_at"):
            # 从未锁存：准交集/观察状态掉出候选 → 回到观察，重置确认计数
            if cur_phase == PHASE_PRE:
                item["phase"] = PHASE_OBSERVING
                item["pre_exit_reason"] = "跌出准交集候选"
                item["pre_exit_at"] = now_text
            item["pre_confirmations"] = 0
            item["active"] = False
            continue
        expires = _parse_intersection_datetime(item.get("expires_at"))
        first = _parse_intersection_datetime(item.get("first_intersection_at"))
        deadline_dt = expires or (first + timedelta(minutes=latch_window) if first else None)
        if deadline_dt and now > deadline_dt:
            if cur_phase not in (PHASE_INVALID, PHASE_EXPIRED):
                item["phase"] = PHASE_EXPIRED
                item["invalid_reason"] = item.get("invalid_reason") or "锁存期满"
            item["active"] = False
            continue
        # 仍在锁存窗口内：保留最后相位（无当前快照，不推进、不失效）
        synth = {
            "code": code,
            "name": item.get("name") or code,
            "price": item.get("trigger_price"),
            "change": None,
            "risk_status": item.get("risk_status") or RISK_UNKNOWN,
            "latched_hold": True,
        }
        _emit(synth, item)

    return results, next_state
