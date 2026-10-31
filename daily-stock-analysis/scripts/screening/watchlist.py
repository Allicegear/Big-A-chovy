"""明日观察池：构建观察池行（触发价 / 低吸区 / 失效 / 不追高），以及观察池突破
升级状态机（WATCHING → TRIGGERED → CONFIRMED，跨快照持久化）。

状态文件路径来自 runtime_paths（随 A_SHARE_STATE_DIR 隔离），写入经 state_commit 回合
提交门；读写由核心 run_screening_core 调用。主导资格读 screening.capital，公告风险读
screening.risk，不另立口径。
"""
from __future__ import annotations

import json
import math
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

SCRIPT_DIR = Path(__file__).resolve().parents[1]
_PROJECT_ROOT = SCRIPT_DIR.parent.parent
for _p in (str(_PROJECT_ROOT), str(SCRIPT_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)
from tools.rule_config import RULE_CONFIG  # noqa: E402

import runtime_paths  # noqa: E402  运行状态路径唯一来源
import state_commit  # noqa: E402  回合状态提交门
from screening.capital import evaluate_dominance_type  # noqa: E402
from screening.intersection import _clock_minutes  # noqa: E402
from screening.model import Enriched  # noqa: E402
from screening.risk import RISK_AVOID, RISK_CLEAN, RISK_UNKNOWN, _row_risk_status  # noqa: E402
from screening.rules import has_resonance  # noqa: E402

SCREENING_CONFIG = RULE_CONFIG["screening"]
TZ = ZoneInfo("Asia/Shanghai")

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
