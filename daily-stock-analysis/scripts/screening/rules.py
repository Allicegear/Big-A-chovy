"""候选池规则：行业共振、严格超短/趋势、趋势观察、低吸 A/B/C 分级与排序。

只读 ``tools.rule_config`` 的参数与传入的行情/统计，不联网、不读写运行状态。
"""
from __future__ import annotations

import sys
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
from tools.rule_config import RULE_CONFIG, hhmm_to_minutes  # noqa: E402

from screening.model import (  # noqa: E402
    BOARD_MAIN,
    Enriched,
    amount_yi,
    board_of,
    is_a_share_row,
    is_number,
    normalize_boards,
    pct,
)

SCREENING_CONFIG = RULE_CONFIG["screening"]
EXECUTION_CONFIG = RULE_CONFIG["execution"]


def sector_stats(
    rows: List[Dict[str, Any]],
    breadth: Optional[Dict[str, Any]] = None,
    boards: Any = (BOARD_MAIN,),
) -> Dict[str, Dict[str, Any]]:
    """行业统计。

    口径（2026-09-30 起）：
    - **行业上涨比例、平均涨幅**（``adv`` / ``sum`` / ``n``）继续用全市场 A 股背景，
      只按行业聚合，不随交易板范围变化；
    - **行业强势股数量**（``strong``）只统计**本轮所选交易板**内的股票，因此同行业
      可以跨主板、创业板、科创板形成共振；默认只选主板时与历史口径逐值一致。
    - 注意：``resonance_cfg["main_board_prefixes"]`` **不在这里使用**——它仍服务于
      `market_summary` 的「主板涨停／跌停」统计，两个用途必须分开（否则扩大交易板
      范围会悄悄改变"主板涨跌停"的分母）。
    """
    resonance_cfg = SCREENING_CONFIG["resonance"]
    selected = set(normalize_boards(boards))
    stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"n": 0, "adv": 0, "strong": 0, "sum": 0.0})
    for row in rows:
        if not is_a_share_row(row) or not is_number(row.get("f3")):
            continue
        sec = row.get("f100") or "-"
        stats[sec]["n"] += 1
        stats[sec]["sum"] += row.get("f3") or 0.0
        if row.get("f3", 0) > 0:
            stats[sec]["adv"] += 1
        if (
            board_of(row.get("f12")) in selected
            and row.get("f3", 0) >= float(resonance_cfg["strong_change_min_inclusive"])
            and is_number(row.get("f6")) and row.get("f6") >= float(resonance_cfg["strong_amount_min_inclusive"])
        ):
            stats[sec]["strong"] += 1
    quality = breadth or {}
    stats["__meta__"] = {
        "resonance_usable": bool(quality.get("resonance_usable", True)),
        "quality_reason": quality.get("quality_reason", ""),
        "strong_scope_boards": list(selected),
    }
    return stats


def has_resonance(e: Enriched, stats: Dict[str, Dict[str, Any]]) -> bool:
    resonance_cfg = SCREENING_CONFIG["resonance"]
    meta = stats.get("__meta__") or {}
    if not meta.get("resonance_usable", True):
        return False
    sec = stats.get(e.industry) or {}
    if sec.get("strong", 0) < int(resonance_cfg["strong_candidates_min"]):
        return False
    # Also require sector-wide bullish: either ≥30% of stocks advancing,
    # or the sector's average change is positive.  This prevents a bearish
    # sector (e.g. 18↑/162↓) from passing resonance on the strength of a
    # couple of outliers.
    n = sec.get("n", 0)
    adv = sec.get("adv", 0)
    avg_change = sec.get("sum", 0) / n if n > 0 else 0.0
    adv_ratio = adv / n if n > 0 else 0.0
    return (
        adv_ratio >= float(resonance_cfg["advancing_ratio_min_inclusive"])
        or avg_change > float(resonance_cfg["average_change_min_exclusive"])
    )


def strict_ultra(e: Enriched) -> bool:
    cfg = SCREENING_CONFIG["strict_ultra"]
    return (
        is_number(e.volume_ratio)
        and float(cfg["price_min_inclusive"]) <= e.price <= float(cfg["price_max_inclusive"])
        and e.float_mv < float(cfg["float_mv_max_exclusive"])
        and e.turnover > float(cfg["turnover_min_exclusive"])
        and e.volume_ratio > float(cfg["volume_ratio_min_exclusive"])
        and e.amount > float(cfg["amount_min_exclusive"])
        and float(cfg["change_min_inclusive"]) <= e.change <= float(cfg["change_max_inclusive"])
        and e.adj_close > e.ma5
        and e.five_ret <= float(cfg["five_ret_max_inclusive"])
        and e.cur_to_high <= float(cfg["cur_to_high_max_inclusive"])
    )


def strict_trend(e: Enriched) -> bool:
    cfg = SCREENING_CONFIG["strict_trend"]
    return (
        float(cfg["price_min_inclusive"]) <= e.price <= float(cfg["price_max_inclusive"])
        and float(cfg["float_mv_min_inclusive"]) <= e.float_mv <= float(cfg["float_mv_max_inclusive"])
        and e.price > e.ma5 and e.price > e.ma10 and e.price > e.ma20 and e.ma20 > e.prev_ma20
        and e.dist60 <= float(cfg["dist60_max_inclusive"])
        and float(cfg["turnover_min_inclusive"]) <= e.turnover <= float(cfg["turnover_max_inclusive"])
        and e.vol_vs_avg5 <= float(cfg["vol_vs_avg5_max_inclusive"])
        and float(cfg["change_min_inclusive"]) <= e.change <= float(cfg["change_max_inclusive"])
        and e.five_ret <= float(cfg["five_ret_max_inclusive"])
    )


def trend_confirm_relaxed(e: Enriched) -> bool:
    """趋势确认的质量条件（不含价格区间/流通市值）。

    超短池已约束价格 5–30 与流通市值上限，趋势侧复检价格区间属冗余且会
    错误排除哈药(~5.8)等低价强势股。交集状态机用此判定"正式交集"，与
    strict_trend 相比只去掉价格区间、流通市值两项。
    """
    cfg = SCREENING_CONFIG["strict_trend"]
    return (
        e.price > e.ma5 and e.price > e.ma10 and e.price > e.ma20
        and e.ma20 > e.prev_ma20
        and float(cfg["change_min_inclusive"]) <= e.change <= float(cfg["change_max_inclusive"])
        and e.five_ret <= float(cfg["five_ret_max_inclusive"])
        and e.dist60 <= float(cfg["dist60_max_inclusive"])
        and float(cfg["turnover_min_inclusive"]) <= e.turnover <= float(cfg["turnover_max_inclusive"])
        and e.amount > float(cfg["amount_min_exclusive"])
        and e.vol_vs_avg5 <= float(cfg["vol_vs_avg5_max_inclusive"])
    )


def _trend_quality_checks(e: Enriched) -> List[tuple]:
    """趋势质量条件清单（不含价格区间/流通市值），供准交集"差一项"判定。"""
    cfg = SCREENING_CONFIG["strict_trend"]
    return [
        ("价格相对MA5", e.price > e.ma5),
        ("价格相对MA10", e.price > e.ma10),
        ("价格相对MA20", e.price > e.ma20),
        ("MA20斜率", e.ma20 > e.prev_ma20),
        ("当日涨幅", float(cfg["change_min_inclusive"]) <= e.change <= float(cfg["change_max_inclusive"])),
        ("5日涨幅", e.five_ret <= float(cfg["five_ret_max_inclusive"])),
        ("距60日高点", e.dist60 <= float(cfg["dist60_max_inclusive"])),
        ("换手率", float(cfg["turnover_min_inclusive"]) <= e.turnover <= float(cfg["turnover_max_inclusive"])),
        ("成交额", e.amount > float(cfg["amount_min_exclusive"])),
        ("量能相对5日", e.vol_vs_avg5 <= float(cfg["vol_vs_avg5_max_inclusive"])),
    ]


def trend_observation(e: Enriched) -> bool:
    """Identify intact/recovering trends without diluting trend confirmation.

    This pool deliberately admits modest red-to-green repair days, but it
    retains the MA20 trend, liquidity and overheating safeguards.  Strict
    confirmation remains a separate, unchanged rule and is promoted into this
    pool so that confirmation is always an upgrade rather than a parallel
    classification.
    """
    if strict_trend(e):
        return True
    cfg = SCREENING_CONFIG["trend_observation"]
    return (
        float(cfg["price_min_inclusive"]) <= e.price <= float(cfg["price_max_inclusive"])
        and float(cfg["float_mv_min_inclusive"]) <= e.float_mv <= float(cfg["float_mv_max_inclusive"])
        and e.amount > float(cfg["amount_min_exclusive"])
        and e.price >= e.ma20
        and e.ma20 > e.prev_ma20
        and e.price >= e.ma5 * float(cfg["price_ma5_min_multiplier"])
        and e.ma5 >= e.ma10 * float(cfg["ma5_ma10_min_multiplier"])
        and float(cfg["change_min_inclusive"]) <= e.change <= float(cfg["change_max_inclusive"])
        and float(cfg["turnover_min_inclusive"]) <= e.turnover <= float(cfg["turnover_max_inclusive"])
        and e.vol_vs_avg5 <= float(cfg["vol_vs_avg5_max_inclusive"])
        and e.five_ret <= float(cfg["five_ret_max_inclusive"])
        and e.dist60 <= float(cfg["dist60_max_inclusive"])
    )


def trend_condition_diagnosis(e: Enriched, stats: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
    """Explain every strict confirmation condition for an ultra candidate."""
    strict_cfg = SCREENING_CONFIG["strict_trend"]
    observation_cfg = SCREENING_CONFIG["trend_observation"]
    checks = [
        ("价格区间", float(strict_cfg["price_min_inclusive"]) <= e.price <= float(strict_cfg["price_max_inclusive"])),
        ("流通市值", float(strict_cfg["float_mv_min_inclusive"]) <= e.float_mv <= float(strict_cfg["float_mv_max_inclusive"])),
        ("价格相对MA5", e.price > e.ma5),
        ("价格相对MA10", e.price > e.ma10),
        ("价格相对MA20", e.price > e.ma20),
        ("MA20斜率", e.ma20 > e.prev_ma20),
        ("当日涨幅", float(strict_cfg["change_min_inclusive"]) <= e.change <= float(strict_cfg["change_max_inclusive"])),
        ("5日涨幅", e.five_ret <= float(strict_cfg["five_ret_max_inclusive"])),
        ("距60日高点", e.dist60 <= float(strict_cfg["dist60_max_inclusive"])),
        ("换手率", float(strict_cfg["turnover_min_inclusive"]) <= e.turnover <= float(strict_cfg["turnover_max_inclusive"])),
        ("成交额", e.amount > float(strict_cfg["amount_min_exclusive"])),
        ("量能相对5日", e.vol_vs_avg5 <= float(strict_cfg["vol_vs_avg5_max_inclusive"])),
    ]
    failures = [name for name, passed in checks if not passed]
    observation_checks = [
        ("价格区间", float(observation_cfg["price_min_inclusive"]) <= e.price <= float(observation_cfg["price_max_inclusive"])),
        ("流通市值", float(observation_cfg["float_mv_min_inclusive"]) <= e.float_mv <= float(observation_cfg["float_mv_max_inclusive"])),
        ("成交额", e.amount > float(observation_cfg["amount_min_exclusive"])),
        ("价格相对MA20", e.price >= e.ma20),
        ("MA20斜率", e.ma20 > e.prev_ma20),
        (
            "MA5/MA10未明显转弱",
            e.price >= e.ma5 * float(observation_cfg["price_ma5_min_multiplier"])
            and e.ma5 >= e.ma10 * float(observation_cfg["ma5_ma10_min_multiplier"]),
        ),
        (
            "当日涨幅",
            float(observation_cfg["change_min_inclusive"]) <= e.change <= float(observation_cfg["change_max_inclusive"]),
        ),
        (
            "换手率",
            float(observation_cfg["turnover_min_inclusive"]) <= e.turnover <= float(observation_cfg["turnover_max_inclusive"]),
        ),
        ("量能相对5日", e.vol_vs_avg5 <= float(observation_cfg["vol_vs_avg5_max_inclusive"])),
        ("5日涨幅", e.five_ret <= float(observation_cfg["five_ret_max_inclusive"])),
        ("距60日高点", e.dist60 <= float(observation_cfg["dist60_max_inclusive"])),
    ]
    observation_failures = [name for name, passed in observation_checks if not passed]
    # Strict confirmation is explicitly an upgrade of observation.  Keep that
    # invariant even where the observation rule uses a separate MA5/MA10
    # repair guard that the stricter confirmation rule does not need.
    if strict_trend(e):
        observation_failures = []
    quality = (stats or {}).get("__meta__") or {}
    if stats is None:
        sector_condition = "未计算"
    elif not quality.get("resonance_usable", True):
        sector_condition = "数据质量降级，不参与加分"
    else:
        sector_condition = "板块共振" if has_resonance(e, stats) else "板块未共振"
    return {
        **asdict(e),
        "ma_state": "MA5/10/20上方" if e.price > e.ma5 and e.price > e.ma10 and e.price > e.ma20 else "均线未全部站上",
        "ma20_slope": "上行" if e.ma20 > e.prev_ma20 else "未上行",
        "day_change": pct(e.change),
        "five_day": pct(e.five_ret * 100),
        "dist60": pct(e.dist60 * 100),
        "turnover_amount": f"{e.turnover:.2f}% / {amount_yi(e.amount)}",
        "sector_condition": sector_condition,
        "first_failure": failures[0] if failures else "无",
        "all_failures": "、".join(failures) if failures else "无",
        "near_match": len(failures) == 1,
        "observation_first_failure": observation_failures[0] if observation_failures else "无",
        "observation_all_failures": "、".join(observation_failures) if observation_failures else "无",
        "trend_observation": not observation_failures,
        "trend_confirmation": not failures,
        "upgrade_status": "趋势确认" if not failures else ("趋势观察" if not observation_failures else "未进入趋势观察"),
    }


def low_ultra_class(e: Enriched, stats: Dict[str, Dict[str, Any]], after_1420: bool) -> Tuple[str, List[str], float]:
    cfg = SCREENING_CONFIG["low_ultra"]
    resonance = has_resonance(e, stats)
    tags: List[str] = []
    if e.high_pull <= float(cfg["tag_high_pull_max_inclusive"]):
        tags.append("追高风险")
    if e.high_pull > float(cfg["tag_high_pull_risk_max_exclusive"]):
        tags.append("冲高回落风险")
    if e.change > float(cfg["tag_tail_change_min_exclusive"]) and after_1420:
        tags.append("尾盘追高风险")
    if (
        e.turnover > float(cfg["stagnation_turnover_min_exclusive"])
        and e.amount > float(cfg["stagnation_amount_min_exclusive"])
        and e.change < float(cfg["stagnation_change_max_exclusive"])
    ):
        tags.append("巨量滞涨")
    if e.vwap_state == "均价线下方":
        tags.append("均价线下方")
    if not resonance:
        tags.append("板块共振不足")
    hard_c = (
        e.change > float(cfg["hard_change_max_exclusive"])
        or e.turnover > float(cfg["hard_turnover_max_exclusive"])
        or e.volume_ratio > float(cfg["hard_volume_ratio_max_exclusive"])
        or "巨量滞涨" in tags
        or e.vwap_state == "均价线下方"
    )
    if hard_c:
        cls = "C"
    elif (
        float(cfg["a_change_min_inclusive"]) <= e.change <= float(cfg["a_change_max_inclusive"])
        and float(cfg["a_turnover_min_inclusive"]) <= e.turnover <= float(cfg["a_turnover_max_inclusive"])
        and float(cfg["a_volume_ratio_min_inclusive"]) <= e.volume_ratio <= float(cfg["a_volume_ratio_max_inclusive"])
        and e.amount > float(cfg["a_amount_min_exclusive"])
        and e.price > e.ma5
        and e.high_pull <= float(cfg["a_high_pull_max_inclusive"])
        and e.vwap_state == "均价线上方"
        and resonance
    ):
        cls = "A"
    elif (
        float(cfg["b_change_min_inclusive"]) <= e.change <= float(cfg["b_change_max_inclusive"])
        and e.turnover <= float(cfg["b_turnover_max_inclusive"])
        and e.volume_ratio <= float(cfg["b_volume_ratio_max_inclusive"])
        and e.price > e.ma5
    ):
        cls = "B"
    else:
        cls = "C"
    score = 0.0
    score += max(0, float(cfg["score_change_max"]) - abs(e.change - float(cfg["score_change_target"])) * float(cfg["score_change_scale"]))
    score += max(0, float(cfg["score_turnover_max"]) - abs(e.turnover - float(cfg["score_turnover_target"])) * float(cfg["score_turnover_scale"]))
    score += float(cfg["score_amount_good_points"]) if float(cfg["score_amount_good_min_inclusive"]) <= e.amount <= float(cfg["score_amount_good_max_inclusive"]) else float(cfg["score_amount_other_points"])
    score += float(cfg["score_vwap_points"]) if e.vwap_state == "均价线上方" else 0
    score += float(cfg["score_high_pull_good_points"]) if e.high_pull <= float(cfg["score_high_pull_good_max_inclusive"]) else float(cfg["score_high_pull_other_points"])
    score += float(cfg["score_resonance_points"]) if resonance else 0
    score += float(cfg["score_ma5_good_points"]) if e.price > e.ma5 and e.price / e.ma5 - 1 <= float(cfg["score_ma5_distance_max_inclusive"]) else float(cfg["score_ma5_other_points"])
    return cls, tags, round(score, 1)


def low_trend_class(e: Enriched, stats: Dict[str, Dict[str, Any]], after_1420: bool) -> Tuple[str, List[str], float]:
    cfg = SCREENING_CONFIG["low_trend"]
    resonance = has_resonance(e, stats)
    tags: List[str] = []
    if e.change > float(cfg["tag_hold_change_min_exclusive"]):
        tags.append("持仓区/止盈区")
    elif e.change > float(cfg["tag_observation_change_min_exclusive"]):
        tags.append("趋势观察池")
    if e.high_pull <= float(cfg["tag_high_pull_max_inclusive"]):
        tags.append("追高风险")
    if after_1420 and e.change > float(cfg["tag_tail_change_min_exclusive"]):
        tags.append("尾盘追高风险")
    if e.vwap_state == "均价线下方":
        tags.append("放量滞涨风险")
    if not resonance:
        tags.append("板块共振不足")
    base = (
        e.price > e.ma5 and e.price > e.ma10 and e.price > e.ma20
        and e.ma5 > e.prev_ma5 and e.ma10 >= e.prev_ma10
        and e.amount > float(cfg["base_amount_min_exclusive"])
    )
    hard_c = (
        e.change > float(cfg["hard_change_max_exclusive"])
        or e.turnover > float(cfg["hard_turnover_max_exclusive"])
        or e.ma20_dist > float(cfg["hard_ma20_dist_max_exclusive"])
        or e.five_ret > float(cfg["hard_five_ret_max_inclusive"])
    )
    if hard_c:
        cls = "C"
    elif (
        base
        and float(cfg["a_change_min_inclusive"]) <= e.change <= float(cfg["a_change_max_inclusive"])
        and float(cfg["a_turnover_min_inclusive"]) <= e.turnover <= float(cfg["a_turnover_max_inclusive"])
        and e.high_pull > float(cfg["a_high_pull_min_exclusive"])
        and "尾盘追高风险" not in tags
        and resonance
    ):
        cls = "A"
    elif (
        base
        and float(cfg["b_change_min_inclusive"]) <= e.change <= float(cfg["b_change_max_inclusive"])
        and e.turnover <= float(cfg["b_turnover_max_inclusive"])
    ):
        cls = "B"
    else:
        cls = "C"
    score = 0.0
    score += max(0, float(cfg["score_change_max"]) - abs(e.change - float(cfg["score_change_target"])) * float(cfg["score_change_scale"]))
    score += float(cfg["score_turnover_good_points"]) if float(cfg["score_turnover_min_inclusive"]) <= e.turnover <= float(cfg["score_turnover_max_inclusive"]) else float(cfg["score_turnover_other_points"])
    score += float(cfg["score_amount_good_points"]) if float(cfg["score_amount_good_min_inclusive"]) <= e.amount <= float(cfg["score_amount_good_max_inclusive"]) else float(cfg["score_amount_other_points"])
    score += float(cfg["score_slope_good_points"]) if e.ma5 > e.prev_ma5 and e.ma10 >= e.prev_ma10 else float(cfg["score_slope_other_points"])
    score += max(0, float(cfg["score_ma20_max_points"]) - e.ma20_dist * float(cfg["score_ma20_scale"]))
    score += float(cfg["score_resonance_points"]) if resonance else 0
    score += float(cfg["score_high_pull_good_points"]) if e.high_pull <= float(cfg["score_high_pull_good_max_inclusive"]) else float(cfg["score_high_pull_other_points"])
    return cls, tags, round(score, 1)


def low_ultra_output_eligible(e: Enriched, cls: str) -> bool:
    """低吸超短报表保留条件，供 CLI 与实时引擎共用。"""
    cfg = SCREENING_CONFIG["low_ultra"]
    return cls != "C" or (
        e.change >= float(cfg["output_change_min_inclusive"])
        and (
            e.turnover > float(cfg["hard_turnover_max_exclusive"])
            or e.change > float(cfg["hard_change_max_exclusive"])
            or e.volume_ratio > float(cfg["hard_volume_ratio_max_exclusive"])
        )
    )


def low_trend_output_eligible(e: Enriched, cls: str) -> bool:
    """低吸趋势报表保留条件，供 CLI 与实时引擎共用。"""
    cfg = SCREENING_CONFIG["low_trend"]
    return cls != "C" or (
        e.change >= float(cfg["output_change_min_inclusive"])
        and (
            e.change > float(cfg["hard_change_max_exclusive"])
            or e.turnover > float(cfg["hard_turnover_max_exclusive"])
            or e.ma20_dist > float(cfg["hard_ma20_dist_max_exclusive"])
        )
    )


def low_ultra_sort_key(row: Dict[str, Any]) -> Tuple[Any, ...]:
    cfg = SCREENING_CONFIG["low_ultra"]
    return (
        {"A": 0, "B": 1, "C": 2}.get(row["class"], 99),
        -row["score"],
        abs(row["change"] - float(cfg["score_change_target"])),
    )


def low_trend_sort_key(row: Dict[str, Any]) -> Tuple[Any, ...]:
    cfg = SCREENING_CONFIG["low_trend"]
    return (
        {"A": 0, "B": 1, "C": 2}.get(row["class"], 99),
        -row["score"],
        abs(row["change"] - float(cfg["score_change_target"])),
    )


def is_after_tail_risk(timestamp: datetime) -> bool:
    """判断是否进入执行配置登记的尾盘风控时段。"""
    start = EXECUTION_CONFIG["time_windows"]["tail_risk"]["start"]
    return timestamp.hour * 60 + timestamp.minute >= hhmm_to_minutes(start)
