"""资金规则：低吸硬黑名单与高位派发降权、低开洗盘、超大单一票否决、资金分类、
超大单主导判定（absolute / coalition）、资金排名与负超单观察。

只读 tools.rule_config 的参数与传入的行情/资金数据；需要的分钟序列与 20 日资金
日线由 screening.sources.flow 提供（每轮预算、缓存与限速都在那里）。
"""
from __future__ import annotations

import math
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
from tools.rule_config import RULE_CONFIG  # noqa: E402

from screening.model import Enriched, is_number  # noqa: E402
from screening.risk import RISK_CLEAN, _row_risk_status  # noqa: E402
from screening.rules import has_resonance  # noqa: E402
from screening.sources.flow import (  # noqa: E402
    LOW_OPEN_DAILY_FLOW_MAX_CODES_PER_ROUND,
    _flow_minute_series_for,
    _note_low_absorb_unverified,
    fetch_daily_main_flow_history,
    flow_continuity_from_minutes,
    flow_negative_samples_from_minutes,
    round_series_date,
)

RISK_CONFIG = RULE_CONFIG["risk"]
SCREENING_CONFIG = RULE_CONFIG["screening"]

# 硬黑名单：框架明确记载的死亡螺旋/股灾级教训案例，无论资金面如何都不进入候选
# 新增教训需附带日期和根因，避免黑名单无限膨胀
HARD_BLACKLIST: Dict[str, str] = dict(RISK_CONFIG["hard_blacklist"])

# 软降权规则：以下条件任一满足 → 从低吸候选剔除（不标avoid，直接不出现）
# 1. 20日累计涨幅 ≥30% 且 近5日主力净占比持续<0（高位派发信号）
# 2. 硬黑名单命中


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
