"""Markdown 报告渲染：表格与单元格格式化、render_markdown。

只把核心结果字典排版成报告文本，不计算、不联网、不读写运行状态。
报告格式由 tools/report_parser.py 等下游按表头解析，改动须保持表头与栏目不变。
"""
from __future__ import annotations

import math
from collections import Counter
from typing import Any, Dict, List, Optional

from screening.intersection import DEFAULT_INTERSECTION_CONFIG, INTERSECTION_CONFIG_VERSION
from screening.model import BOARD_MAIN, _fmt, amount_yi, is_number, pct
from screening.risk import announcement_label

# 区间分位标注档位（纯展示，不参与门槛）：现价处于当日区间 ≥80% 记偏高，≥90% 记贴顶。
# 阈值依据：2026-09-24 全量报告 42 个候选行的区间分位中位数 76%、≥70% 的占 86%——
# 70% 那条线几乎每行都触发、没有区分度，因此上调为两档（≥80% 占 21%、≥90% 占 7%）。
# 若以后要把它变成"不追高"的硬门槛，请先按框架维护纪律搬进 tools/rule_config.py 并记录。
RANGE_POSITION_WARN_PCT = 80.0
RANGE_POSITION_TOP_PCT = 90.0


def range_position_cell(row: Dict[str, Any]) -> str:
    """把现价在当日区间的分位渲染成报告单元格：≥80% 标 ⚠，≥90% 标 ⚠顶。"""
    value = row.get("range_pct")
    if not is_number(value):
        return "-"
    pct = float(value)
    if pct >= RANGE_POSITION_TOP_PCT:
        return f"{pct:.0f}%⚠顶"
    if pct >= RANGE_POSITION_WARN_PCT:
        return f"{pct:.0f}%⚠"
    return f"{pct:.0f}%"


def flow_veto_suffix(row: Dict[str, Any]) -> str:
    """超大单为负的一票否决标记后缀；未触发返回空串。"""
    return f"；❌{row['flow_veto']}" if row.get("flow_veto") else ""


def shadow_badge_text(badge: Optional[Dict[str, Any]]) -> str:
    """负超单观察行的影子徽标文案。

    徽标由报告落盘后的只读判定器（tools/detect_divergence_leader）回填；
    未回填、历史不足或该股不在判定器的低吸表中，一律显示“未完成判定”，
    不得显示成“不符合”。
    """
    if not isinstance(badge, dict):
        return "未完成判定"
    status = badge.get("status")
    if status == "triggered":
        when = badge.get("trigger_time") or "-"
        return f"今日已触发影子条件 · 触发时间 {when}"
    if status == "not_triggered":
        return "未触发（判定完成）"
    return "未完成判定"


def flow_status_cell(row: Dict[str, Any]) -> str:
    """资金状态单元格：拼上一票否决标记。

    2026-09-26 补：引擎只在顶部告警与资金追踪明细里体现否决，池表与低吸表的行上看不出来，
    而这几张表正是挑标的的地方。现在统一走这里，保证每条候选行都带标记。
    """
    return f"{row.get('flow_status', '') or ''}{flow_veto_suffix(row)}"


def flow_amount_str(value: float) -> str:
    """Format capital flow amount in human-readable form. NaN → 基准不足."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "基准不足"
    abs_val = abs(value)
    sign = "+" if value >= 0 else ""
    if abs_val >= 100_000_000:
        return f"{sign}{value / 100_000_000:.2f}亿"
    if abs_val >= 10_000:
        return f"{sign}{value / 10_000:.0f}万"
    return f"{sign}{value:.0f}"


def markdown_table(headers: List[str], rows: List[List[Any]]) -> str:
    if not rows:
        return "无"
    align = ["---"] + ["---:" if h in {"涨幅", "换手率", "成交额", "量比", "高位回落", "近5日", "距20日线", "评分", "涨跌", "涨跌%", "主力净占比", "5分钟增量", "主力净额", "超大单", "大单", "中单", "小单", "15分钟增量", "板块内候选", "区间分位"} else "---" for h in headers[1:]]
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(align) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(x) for x in row) + " |")
    return "\n".join(lines)


def render_markdown(result: Dict[str, Any]) -> str:
    lines: List[str] = []
    meta = result["meta"]
    lines.append(f"数据时间：{meta['timestamp']}，状态：{meta['status']}，耗时 {meta.get('elapsed_seconds', '?')}s。来源：{meta['source']}。")
    background = result.get("market_background") or {}
    calendar_info = background.get("calendar") if isinstance(background, dict) else None
    if isinstance(calendar_info, dict):
        calendar_status = calendar_info.get("status")
        calendar_data = calendar_info.get("data") or {}
        if calendar_status == "ok" and isinstance(calendar_data, dict):
            day_label = "交易日" if calendar_data.get("is_open") else "休市日"
            lines.append(f"交易日历：{day_label}（深交所官方整月日历，数据日 {calendar_info.get('data_date') or background.get('data_date') or '-'}）。")
        else:
            lines.append("交易日历：待确认（官方整月日历不可用或未发布；未据此推进状态机/T+1）。")
    sentiment_info = background.get("sentiment") if isinstance(background, dict) else None
    if isinstance(sentiment_info, dict):
        metrics = ((sentiment_info.get("data") or {}).get("metrics") if isinstance(sentiment_info.get("data"), dict) else None) or {}
        if sentiment_info.get("status") in {"ok", "partial"} and metrics:
            lines.append(
                "市场情绪（背景摘要，仅统计已取范围）："
                f"涨停 {metrics.get('limit_up_count', 0)} / 炸板 {metrics.get('broken_count', 0)} / 跌停 {metrics.get('limit_down_count', 0)}；"
                f"炸板率 {metrics.get('break_rate') if metrics.get('break_rate') is not None else '不适用'}%；"
                f"最高连板 {metrics.get('max_streak') or '未取到'}；"
                f"昨涨停今日平均 {metrics.get('yesterday_limit_up_today_average_change_pct') if metrics.get('yesterday_limit_up_today_average_change_pct') is not None else '不适用'}%。"
                f"范围={metrics.get('scope', '未知')}，时点={metrics.get('as_of') or '-'}。"
            )
        else:
            lines.append("市场情绪：不可用/待确认；不把失败当作空池，也不改变现有评分和权限。")
    # 交易板范围：报告必须能追溯本轮实际筛了什么范围，避免把扩展板沉默地混进主板结论。
    if meta.get("enabled_boards_label"):
        lines.append(
            f"筛选范围（交易板）：{meta['enabled_boards_label']}"
            + ("（默认，仅沪深主板）" if list(meta.get("enabled_boards") or []) == [BOARD_MAIN] else "")
            + "；所选交易板统一参与正式筛选（同一门槛、同一排名、同一条状态机）。"
        )
    # 范围结论在结果顶层（引擎/CLI 同位置），旧结果可能把它放在 meta 里。
    scope_note = result.get("board_scope_note") or meta.get("board_scope_note")
    if scope_note:
        lines.append(scope_note)
    b = result["breadth"]
    fetch_status = result.get("market_fetch_status") or {}
    if fetch_status.get("source") == "sina_fallback":
        lines.append(
            "行情快照：已切换新浪备用源。该源缺少兼容量比、主力资金和行业字段；"
            "趋势结果仅供观察，超短池、双池交集、资金优选和低吸买点均已关闭。"
        )
    elif fetch_status.get("complete") is False:
        lines.append(
            "行情快照：不完整（"
            f"已取 {fetch_status.get('received_pages', '?')}/{fetch_status.get('expected_pages', '?')} 页，"
            f"失败页 {','.join(map(str, fetch_status.get('failed_pages') or [])) or '无'}）。"
        )
    provider_total_label = b.get("provider_total") if b.get("provider_total") is not None else "不可校验"
    adv = b.get("adv", 0)
    dec = b.get("dec", 0)
    flat = b.get("flat", 0)
    valid_chg = b.get("valid_change", adv + dec + flat)
    lines.append(
        "市场宽度："
        f"A股样本 {b.get('total_rows', '?')} / 原始服务端 {provider_total_label}；"
        f"有效涨跌 {valid_chg}；"
        f"上涨 {adv} / 下跌 {dec} / 平盘 {flat} / 无效 {b.get('invalid_change', 0)}；"
        f"主板涨停 {b.get('main_limit_up', 0)}，跌停 {b.get('main_limit_down', 0)}。"
    )
    if b.get("degraded"):
        lines.append(f"数据质量降级：{b.get('quality_reason') or '市场宽度不完整'}；板块共振不参与加分或筛选。")
    idx = "；".join(f"{x['name']} {x['price']} ({x['change']}%)" for x in result["indices"])
    lines.append(f"指数：{idx}")
    if result.get("errors"):
        lines.append(f"K线失败：{len(result['errors'])} 只；结果已按可验证数据输出。")
    if result.get("announcement_check_available") is False:
        unknown_count = len(result.get("announcement_unknown_codes") or [])
        lines.append(
            f'公告检查不可用：候选 {result.get("announcement_total_count", "?")} 只；'
            f'缓存 {result.get("announcement_cached_count", 0)} 只；'
            f'本次请求 {result.get("announcement_requested_count", "?")} 只；'
            f'{len(result.get("announcement_errors") or [])} 只请求失败；unknown {unknown_count} 只，'
            '仅显示观察/数据不足，未进入双池交集、趋势确认和资金优选。'
        )
    elif result.get("announcement_errors"):
        lines.append(f"公告失败：{len(result['announcement_errors'])} 只；相关个股标记为 unknown。")
    elif "announcement_total_count" in result:
        lines.append(
            f'公告检查：候选 {result.get("announcement_total_count", 0)} 只；'
            f'缓存命中 {result.get("announcement_cached_count", 0)} 只；'
            f'本次请求 {result.get("announcement_requested_count", 0)} 只。'
        )
    for warning in result.get("warnings") or []:
        lines.append(f"警告：{warning}")
    if result.get("strict_enabled"):
        raw_dual_pool = result.get("dual_pool_raw") if "dual_pool_raw" in result else result.get("dual_pool") or []
        state_rows = result.get("intersection_states") or []
        eligible_count = sum(1 for row in state_rows if row.get("new_open_eligible"))
        lines.append(
            "双池运行："
            f"超短池 {len(result.get('strict_ultra') or [])} 只；"
            f"趋势观察池 {len(result.get('trend_observation') or [])} 只；"
            f"趋势确认池 {len(result.get('strict_trend') or [])} 只；"
            f"交集 {len(raw_dual_pool)} 只。"
        )
        lines.append(f"交集状态机：可新开仓 {eligible_count} 只；其余为观察、过热或拒绝状态。")

    if result.get("capital_rank"):
        rows = [[
            r["capital_class"], r.get("pool_source", ""), r["code"], r["name"], r.get("board_label", ""), _fmt(r.get("price")),
            pct(r.get("change")), range_position_cell(r), f"{_fmt(r.get('capital_score'), 1)}",
            flow_amount_str(r.get("main_net", 0)), f"{r.get('main_pct', 0):.1f}%",
            flow_amount_str(r.get("super_net", 0)),
            flow_amount_str(r.get("flow_5m_inc", float('nan'))),
            r.get("vwap_state", ""), r.get("resonance", "否"),
            r.get("capital_class_candidate", ""), _fmt(r.get("sector_experiment_score"), 1),
            r.get("capital_data", ""), r.get("capital_reason", ""),
        ] for r in result["capital_rank"]]
        lines += ["", "## 主力资金优选（候选池二次排序）", markdown_table(
            ["资金类", "原始来源", "代码", "名称", "交易板", "现价", "涨幅", "区间分位", "正式评分", "主力净额",
             "主力净占比", "超大单", "5分钟增量", "均价线", "板块共振",
             "候选评级", "板块实验分（不计正式评级）", "数据完整度", "评分依据"], rows)]

    raw_dual_pool = result.get("dual_pool_raw") if "dual_pool_raw" in result else result.get("dual_pool") or []
    if raw_dual_pool:
        rows = [[r["code"], r["name"], r.get("board_label", ""), _fmt(r.get("price")), pct(r["change"]), range_position_cell(r), pct(r["turnover"]), amount_yi(r["amount"]), r["industry"], f"{r.get('main_pct',0):.1f}%", flow_status_cell(r), announcement_label(r)] for r in raw_dual_pool]
        lines += ["", "## 双池交集（超短池 ∩ 趋势确认池，启动事件）", "注：交集仅代表启动确认，不是买点；真正买点由交集后的缩量回踩产生（见下方状态机）。", markdown_table(["代码", "名称", "交易板", "现价", "涨幅", "区间分位", "换手率", "成交额", "板块", "主力净占比", "资金状态", "公告风险"], rows)]
    elif "dual_pool" in result:
        lines += ["", "## 双池交集（超短池 ∩ 趋势确认池，启动事件）", "无"]

    if result.get("strict_enabled"):
        state_rows = result.get("intersection_states") or []
        pre_rows_md = result.get("pre_intersection") or []
        config = result.get("intersection_config") or {}
        config_meta = result.get("intersection_config_meta") or {}
        version = config.get("version") or config_meta.get("version") or INTERSECTION_CONFIG_VERSION
        source = config.get("source") or config_meta.get("source") or "default"
        lines += [
            "",
            "## 交集四阶段状态机（准交集 → 首次交集锁存 → 等待回踩 → 回踩确认/失效）",
            (
                f"参数版本：{version}；来源：{source}；确认快照 {config.get('confirmation_snapshots', DEFAULT_INTERSECTION_CONFIG['confirmation_snapshots'])} 次；"
                f"上午截止 {config.get('morning_cutoff', DEFAULT_INTERSECTION_CONFIG['morning_cutoff'])}；"
                f"午后买点截止 {config.get('afternoon_buy_deadline', DEFAULT_INTERSECTION_CONFIG['afternoon_buy_deadline'])}；"
                f"信号锁存 {config.get('intersection_latch_minutes', DEFAULT_INTERSECTION_CONFIG['intersection_latch_minutes'])} 分钟；"
                f"迟到涨幅阈值 {config.get('late_change_pct', DEFAULT_INTERSECTION_CONFIG['late_change_pct'])}%。"
            ),
        ]
        # ── 准交集预警 ──
        if pre_rows_md:
            pre_table = [[
                r.get("code", ""), r.get("name", ""), r.get("board_label", ""), f"{r.get('price', 0):.2f}", pct(r.get("change")),
                r.get("intersection_phase", ""),
                r.get("preintersection_missing", "-"),
                (f"{r['trigger_price']:.2f}" if is_number(r.get("trigger_price")) else "—"),
                f"{r.get('main_pct', 0):.1f}%", flow_status_cell(r), r.get("resonance", "否"),
                r.get("risk_note") or announcement_label(r),
            ] for r in pre_rows_md if r.get("intersection_phase") in ("准交集", "等待转强")]
            if pre_table:
                lines += ["", "### 【准交集预警】", "距趋势确认仅差1项，并通过主力净额、5分钟资金和均价线前置检查；板块共振在此阶段用于参考，不单独否决。准交集仍是观察信号。公告 avoid/unknown 一票否决（不给新开仓资格），watch_risk 仅减分。", markdown_table(
                    ["代码", "名称", "交易板", "现价", "涨幅", "相位", "缺失条件", "预计触发价", "主力净占比", "资金状态", "板块共振", "公告风险"], pre_table)]
            # 被前置门槛排除的标的：透明输出未通过门槛，便于核对
            excluded_table = [[
                r.get("code", ""), r.get("name", ""), r.get("board_label", ""), f"{r.get('price', 0):.2f}", pct(r.get("change")),
                r.get("preintersection_missing", "-"),
                r.get("gate_failure_text", "-"),
            ] for r in pre_rows_md if r.get("intersection_phase") == "观察中"]
            if excluded_table:
                lines += ["", "### 【观察中·门槛未过】", "趋势条件只差1项，但前置门槛（主力净额/5分钟资金/均价线）未全过，不进入准交集预警。板块共振不单独否决。", markdown_table(
                    ["代码", "名称", "交易板", "现价", "涨幅", "缺失趋势条件", "未通过前置门槛"], excluded_table)]
        # ── 已触发·等待回踩 ──
        active_states = [r for r in state_rows if r.get("late_flag") is not True and r.get("intersection_phase") in ("首次交集", "等待回踩", "回踩确认", "可试错")]
        if active_states:
            act_table = [[
                r.get("code", ""), r.get("name", ""), r.get("board_label", ""), r.get("first_intersection_at", ""),
                (f"{r['trigger_price']:.2f}" if is_number(r.get("trigger_price")) else "—"),
                (f"{r['trigger_vwap']:.2f}" if is_number(r.get("trigger_vwap")) else "—"),
                flow_amount_str(r.get("trigger_flow_5m")) if is_number(r.get("trigger_flow_5m")) else "—",
                r.get("pullback_zone", "-"), r.get("intersection_phase", ""),
                "有效" if r.get("failure_reason") in (None, "-") else "失效",
                r.get("failure_reason", "-"),
            ] for r in active_states]
            lines += ["", "### 【已触发·等待回踩】", "交集后不追，等待缩量回踩至回踩区且5分资金仍正，方为买点。", markdown_table(
                ["代码", "名称", "交易板", "交集时间", "触发价", "当时VWAP", "5分资金", "回踩观察区", "相位", "有效性", "失效原因"], act_table)]
        # ── 迟到交集 ──
        late_states = [r for r in state_rows if r.get("late_flag") is True]
        if late_states:
            late_table = [[
                r.get("code", ""), r.get("name", ""), r.get("board_label", ""), pct(r.get("change")),
                "；".join(r.get("late_reasons", []) or []) or "无",
            ] for r in late_states]
            lines += ["", "### 【迟到交集·不追】", "首次交集即过热/无共振，已标记迟到，不提供买点。", markdown_table(
                ["代码", "名称", "交易板", "涨幅", "迟到原因"], late_table)]
        if not (pre_rows_md or active_states or late_states):
            lines.append("无（当前无准交集预警，也无正式交集；不为了制造信号而放宽原池条件）")

    if result.get("strict_enabled"):
        rows = [[r["code"], r["name"], r.get("board_label", ""), _fmt(r.get("price")), pct(r["change"]), pct(r["turnover"]), amount_yi(r["amount"]), _fmt(r.get("volume_ratio")), r["industry"], f"{r.get('main_pct',0):.1f}%", flow_amount_str(r.get("flow_5m_inc",0)), flow_status_cell(r), announcement_label(r)] for r in result["strict_ultra"]]
        content = markdown_table(["代码", "名称", "交易板", "现价", "涨幅", "换手率", "成交额", "量比", "板块", "主力净占比", "5分钟增量", "资金状态", "公告风险"], rows)
        if not rows:
            content = "无（当前没有满足超短池硬条件的标的）"
        lines += ["", "## 超短池", content]
    if result.get("strict_enabled"):
        rows = [[r["code"], r["name"], r.get("board_label", ""), _fmt(r.get("price")), pct(r["change"]), pct(r["turnover"]), amount_yi(r["amount"]), r["industry"], r.get("ma_state", ""), f"{r.get('main_pct',0):.1f}%", flow_amount_str(r.get("flow_5m_inc",0)), flow_status_cell(r), announcement_label(r)] for r in result.get("trend_observation") or []]
        content = markdown_table(["代码", "名称", "交易板", "现价", "涨幅", "换手率", "成交额", "板块", "均线状态", "主力净占比", "5分钟增量", "资金状态", "公告风险"], rows)
        if not rows:
            content = "无（当前没有满足趋势观察条件的标的）\n\n说明：不以放宽趋势确认来凑数。"
        lines += ["", "## 趋势观察池", content]
    if result.get("strict_enabled"):
        rows = [[r["code"], r["name"], r.get("board_label", ""), _fmt(r.get("price")), pct(r["change"]), pct(r["turnover"]), amount_yi(r["amount"]), r["industry"], f"{r.get('main_pct',0):.1f}%", flow_amount_str(r.get("flow_5m_inc",0)), flow_status_cell(r), announcement_label(r)] for r in result["strict_trend"]]
        content = markdown_table(["代码", "名称", "交易板", "现价", "涨幅", "换手率", "成交额", "板块", "主力净占比", "5分钟增量", "资金状态", "公告风险"], rows)
        if not rows:
            content = "无（当前没有满足趋势确认硬条件的标的）"
        lines += ["", "## 趋势确认池", content]
    if result.get("strict_enabled"):
        diagnostics = result.get("trend_diagnostics") or []
        near_match = sum(1 for row in diagnostics if row.get("near_match"))
        lines += ["", "## 超短候选的趋势条件淘汰诊断", f"趋势确认诊断：超短候选 {len(diagnostics)} 只，近似命中/只差一项 {near_match} 只。“观察淘汰”与“确认淘汰”分别对应两套规则。"]
        diag_rows = [[
            r.get("code", ""), r.get("name", ""), r.get("ma_state", ""), r.get("ma20_slope", ""),
            r.get("day_change", ""), r.get("five_day", ""), r.get("dist60", ""),
            r.get("turnover_amount", ""), r.get("sector_condition", ""), announcement_label(r),
            r.get("observation_first_failure", ""), r.get("observation_all_failures", ""),
            r.get("first_failure", ""), r.get("all_failures", ""), r.get("upgrade_status", ""),
        ] for r in diagnostics]
        lines.append(markdown_table(
            ["代码", "名称", "均线", "MA20斜率", "当日涨幅", "5日涨幅", "距60高", "换手/成交额", "板块条件", "公告风险", "观察首个淘汰", "观察全部淘汰", "确认首个淘汰", "确认全部淘汰", "结论"], diag_rows
        ))
    if result.get("low_ultra"):
        ind_counts = Counter(r.get("industry", "") for r in result["low_ultra"])
        rows = [[
            r["class"], r["code"], r["name"], r.get("board_label", ""), _fmt(r.get("price")), pct(r["change"]), range_position_cell(r), pct(r["turnover"]),
            amount_yi(r["amount"]), _fmt(r.get("volume_ratio")), r["industry"], ind_counts.get(r.get("industry", ""), 0),
            r["resonance"], f"{_fmt(r.get('high_pull'))}pct", r["vwap_state"], f"{r.get('main_pct',0):.1f}%",
            flow_amount_str(r.get("super_net", 0)),
            r.get("dominance_label") or r.get("super_lead") or "未计算",
            flow_amount_str(r.get("flow_5m_inc", 0)), flow_status_cell(r), r["risk"], announcement_label(r),
        ] for r in result["low_ultra"]]
        lines += ["", "## 低吸超短线 A/B/C", markdown_table(["类", "代码", "名称", "交易板", "现价", "涨幅", "区间分位", "换手率", "成交额", "量比", "板块", "板块内候选", "共振", "高位回落", "均价线", "主力净占比", "超大单", "超单主导", "5分钟增量", "资金状态", "风险", "公告风险"], rows)]
    if result.get("low_trend"):
        rows = [[r["class"], r["code"], r["name"], r.get("board_label", ""), _fmt(r.get("price")), pct(r["change"]), range_position_cell(r), pct(r["turnover"]), amount_yi(r["amount"]), r["industry"], r["ma_state"], pct((r.get("five_ret") or 0) * 100), pct((r.get("ma20_dist") or 0) * 100), f"{_fmt(r.get('high_pull'))}pct", f"{r.get('main_pct',0):.1f}%", flow_amount_str(r.get("flow_5m_inc",0)), flow_status_cell(r), r["risk"], announcement_label(r)] for r in result["low_trend"]]
        lines += ["", "## 低吸短线趋势 A/B/C", markdown_table(["类", "代码", "名称", "交易板", "现价", "涨幅", "区间分位", "换手率", "成交额", "板块", "均线状态", "近5日", "距20日线", "高位回落", "主力净占比", "5分钟增量", "资金状态", "风险", "公告风险"], rows)]
    if result.get("low_open_wash"):
        rows = [[
            r["code"], r["name"],
            _fmt(r.get("open")), _fmt(r.get("prev_close")),
            f"{r.get('low_open_pct', 0):.2f}%",
            f"{_fmt(r.get('price'))} / {pct(r.get('change'))}",
            f"{r.get('main_pct', 0):.1f}%",
            flow_amount_str(r.get("flow_5m_inc", 0)),
            r.get("vwap_state", ""),
            flow_amount_str(r.get("persistent_net", 0)),
            r.get("persistent_flow_status", "未核验"),
            r.get("industry", ""),
            announcement_label(r),
        ] for r in result["low_open_wash"]]
        lines += ["", "## 低开洗盘（实验性观察补充 · 不计入主/次级信号 · 永不自动出手）",
                   "> 仅进观察，不作为独立开仓信号。条件：低开≥2%、现价高于昨收并站上均价线、当日主力净流入为正，且此前最近20个已完整结算交易日累计主力净流入为正。历史数据不足的行仅作待核验观察，不通过该条件；盘中快照不充当20日数据。",
                   markdown_table(["代码", "名称", "今开", "昨收", "低开%", "现价/涨幅", "主力净占比", "5分钟增量", "均价线", "20日累计净流入", "20日验证", "板块", "公告风险"], rows)]
    # 历史快照兼容：旧结果带独立「扩展交易板观察」列表时按原口径原样渲染，
    # **不把旧结果重新解释成完整筛选结果**。新结果不再有这一节——三板都进正式栏目。
    if "extended_board_observations" in result:
        legacy_status = result.get("extended_board_status", "ok")
        legacy_rows = result.get("extended_board_observations") or []
        lines += ["", "## 扩展交易板观察（历史口径 · 该快照生成时两板仅观察）",
                  "> ⚠️ 本快照由旧口径生成：创业板/科创板当时只进观察列表、不参与正式池。"
                  "当前设置已是「所选交易板统一参与正式筛选」，勿按本表判断候选强弱。"]
        if legacy_status != "ok":
            lines.append("> ⚠️ 该快照当轮数据不可用，未产出观察列表。")
        elif not legacy_rows:
            lines.append("无")
        else:
            lines.append(markdown_table(
                ["交易板", "代码", "名称", "现价", "涨幅", "换手率", "成交额", "量比"],
                [[r.get("board_label", ""), r.get("code", ""), r.get("name", ""),
                  _fmt(r.get("price")), f"{_fmt(r.get('change'))}%",
                  f"{_fmt(r.get('turnover'))}%", flow_amount_str(r.get("amount")),
                  _fmt(r.get("vol_ratio"), 2)] for r in legacy_rows]))

    if meta.get("negative_super_view") == "observe":
        neg_status = result.get("negative_super_status", "ok")
        neg_rows = result.get("negative_super_observations") or []
        lines += ["", "## 负超单观察（独立列表 · 仅观察 · 真实仓一票否决不变）"]
        if neg_status != "ok":
            reason = {"degraded": "行情降级（新浪备用源）", "incomplete": "行情快照不完整"}.get(
                neg_status, neg_status)
            lines.append(
                f"> ⚠️ {reason}，本轮未产出负超单观察列表；列表为空不代表“今天没有负超单标的”。"
            )
        elif not neg_rows:
            lines.append("无")
        else:
            rows = [[
                r.get("code", ""), r.get("name", ""), r.get("data_time", ""),
                flow_amount_str(r.get("super_net", 0)),
                flow_amount_str(r.get("main_net", 0)),
                flow_amount_str(r.get("big_net", 0)),
                flow_amount_str(r.get("flow_5m_inc", float("nan"))),
                f"{_fmt(r.get('price'))} / {r.get('vwap_state', '')}",
                r.get("resonance", "否"),
                r.get("dominance_label", "✗"),
                r.get("flow_status", ""),
                announcement_label(r),
                "；".join(r.get("blockers") or []),
                shadow_badge_text(r.get("shadow_badge")),
            ] for r in neg_rows]
            lines.append("> ⚠️ 仅观察，真实仓一票否决不变；逐项未通过门槛如实列出，不得当作“仅差一个条件即可买”。")
            lines.append(markdown_table(
                ["代码", "名称", "数据时间", "超大单", "主力净额", "大单净额", "5分钟增量",
                 "现价/均价线", "板块共振", "生产主导标签", "资金状态", "公告风险",
                 "未通过门槛", "影子徽标"], rows))
    if result.get("watchlist"):
        # 观察池突破状态机只在 CLI 路径运行；实时看板链路不产出 breakout_phase。
        # 未评估时如实标「未评估 / —」，不默认 WATCHING、0次、✗（那等于伪造状态）。
        breakout_evaluated = any(r.get("breakout_phase") for r in result["watchlist"])
        rows = [[
            r["code"], r["name"], r.get("board_label", ""), _fmt(r.get("price")), pct(r.get("change")), range_position_cell(r), r["industry"], r["structure"],
            r["trigger"], r.get("breakout_phase") or "未评估",
            (f"{r['confirm_count']}次" if r.get("confirm_count") is not None else "—"),
            r.get("dominance_label") or "—", r["buy_zone"], r["invalid"], r["no_chase"],
            (r.get("status_note", "") or "") + flow_veto_suffix(r), announcement_label(r)
        ] for r in result["watchlist"]]
        watchlist_title = (
            "## 明日观察池（含突破升级状态机）" if breakout_evaluated else "## 明日观察池"
        )
        lines += ["", watchlist_title, markdown_table(["代码", "名称", "交易板", "当前价", "涨幅", "区间分位", "板块", "结构", "触发价", "突破状态", "确认次数", "超单主导", "低吸区", "失效", "追高禁区", "状态说明", "公告风险"], rows)]
    if result.get("sector_indices"):
        sectors = sorted(result["sector_indices"], key=lambda s: s["change"], reverse=True)
        rows = [[s["name"], pct(s["change"]), f"{s['price']:.2f}" if s.get("price") else "-",
                 f"{s['up_count']}↑{s['down_count']}↓",
                 s.get("source", "")] for s in sectors]
        lines += ["", "## 相关板块指数", markdown_table(["板块", "涨跌%", "现价", "涨/跌", "来源"], rows)]
    if result.get("flow_detail"):
        details = result["flow_detail"]
        rows = []
        for r in details:
            vwap_label = "上方" if r.get("price_above_vwap", True) else "下方"
            rows.append([
                r.get("code", ""),
                r.get("name", ""),
                flow_amount_str(r.get("main_net", 0)),
                f"{r.get('main_pct', 0):.1f}%",
                flow_amount_str(r.get("super_net", 0)),
                flow_amount_str(r.get("big_net", 0)),
                flow_amount_str(r.get("mid_net", 0)),
                flow_amount_str(r.get("small_net", 0)),
                flow_amount_str(r.get("flow_5m_inc", 0)),
                flow_amount_str(r.get("flow_15m_inc", 0)),
                vwap_label,
                r.get("industry", ""),
                flow_status_cell(r),
            ])
        lines += ["", "## 重点候选资金追踪", markdown_table(
            ["代码", "名称", "主力净额", "主力净占比", "超大单", "大单", "中单", "小单",
             "5分钟增量", "15分钟增量", "均价线", "板块", "结论"], rows)]
    lines.append("\n备注：脚本只负责查询和分层；最终买卖、仓位、止损需要按 trading-rules 再分析。")
    return "\n".join(lines)
