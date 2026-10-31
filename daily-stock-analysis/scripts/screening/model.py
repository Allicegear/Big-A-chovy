"""行情行的数据模型、数值格式化与交易板分类（无网络、无运行状态）。"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List

from screening.risk import RISK_UNKNOWN  # 风险状态唯一来源


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not math.isnan(value)


def _flow_number(value: Any) -> float | int:
    """Normalize optional Eastmoney capital-flow fields.

    Eastmoney occasionally returns ``-``/``--`` instead of a number during
    the first minutes after the open. Keep missing flow fail-closed as zero,
    but never let the sentinel string leak into arithmetic.
    """
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return 0
        return value
    if value in (None, "", "-", "--"):
        return 0
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0
    return 0 if math.isnan(parsed) else parsed


def _fmt(value: Any, nd: int = 2, default: str = "—") -> str:
    """None/NaN-safe numeric formatter for markdown tables.

    Avoids `unsupported format string passed to NoneType.__format__`
    crashes when an enriched row is missing price/volume_ratio/etc.
    """
    if isinstance(value, (int, float)) and not (isinstance(value, float) and math.isnan(value)):
        return f"{value:.{nd}f}"
    return default


def avg(values: Iterable[float]) -> float:
    vals = list(values)
    return sum(vals) / len(vals) if vals else float("nan")


def pct(value: float) -> str:
    return f"{_fmt(value, 2)}%"


def amount_yi(value: float) -> str:
    if not is_number(value):
        return "—"
    return f"{value / 100_000_000:.2f}亿"


def secid_for(code: str) -> str:
    return ("1." if code.startswith("6") else "0.") + code


def tencent_code(code: str) -> str:
    return ("sh" if code.startswith("6") else "sz") + code


def current_status(ts: datetime) -> str:
    if ts.weekday() >= 5:
        return "非交易日/最新快照"
    if ((ts.hour == 9 and ts.minute >= 30) or (10 <= ts.hour < 11) or (ts.hour == 11 and ts.minute <= 30) or (13 <= ts.hour < 15)):
        return "盘中"
    return "非交易时段/最新快照"


@dataclass
class Enriched:
    code: str
    name: str
    price: float
    change: float
    turnover: float
    amount: float
    volume_ratio: float
    high: float
    low: float
    open: float
    prev_close: float
    total_mv: float
    float_mv: float
    industry: str
    timestamp: int
    volume: float
    kdate: str
    k_source: str
    adj_close: float
    ma5: float
    ma10: float
    ma20: float
    prev_ma5: float
    prev_ma10: float
    prev_ma20: float
    five_ret: float
    dist60: float
    ma20_dist: float
    high_pull: float
    cur_to_high: float
    vol_vs_avg5: float
    vwap: float
    vwap_state: str
    prior_high: float
    prior_low: float
    # capital flow
    main_net: float = 0
    main_pct: float = 0
    super_net: float = 0
    super_pct: float = 0
    big_net: float = 0
    big_pct: float = 0
    mid_net: float = 0
    mid_pct: float = 0
    small_net: float = 0
    small_pct: float = 0
    flow_5m_inc: float = 0
    flow_15m_inc: float = 0
    # 增量基准来源："snapshot"=本地快照累积，"fflow"=东财当日累计分钟序列；空串=无基准
    flow_baseline_source: str = ""
    amount_5m_inc: float = 0
    volume_5m_inc: float = 0
    vol_ratio_vs_hist: float = 0
    vol_surge: bool = False
    price_above_vwap: bool = True
    flow_status: str = "数据不足"
    # 框架一票否决标记：超大单为负（禁止正式买入建议）。空串表示未触发。
    flow_veto: str = ""
    buy_ratio: float = float("nan")
    risk_status: str = RISK_UNKNOWN
    # 现价在当日区间的位置（0%=最低，100%=最高）；NaN=当日无振幅（如一字板）。
    # 只用于报告标注，不参与任何门槛判断（2026-09-26 加入）。
    range_pct: float = float("nan")


def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "code": str(row.get("f12", "")),
        "name": str(row.get("f14", "")),
        "price": row.get("f2"),
        "change": row.get("f3"),
        "change_abs": row.get("f4"),
        "volume": row.get("f5"),
        "amount": row.get("f6"),
        "amplitude": row.get("f7"),
        "turnover": row.get("f8"),
        "volume_ratio": row.get("f10"),
        "high": row.get("f15"),
        "low": row.get("f16"),
        "open": row.get("f17"),
        "prev_close": row.get("f18"),
        "total_mv": row.get("f20"),
        "float_mv": row.get("f21"),
        "industry": row.get("f100") or "-",
        "timestamp": row.get("f124") or 0,
        "main_net": _flow_number(row.get("f62")),
        "main_pct": _flow_number(row.get("f184")),
        "super_net": _flow_number(row.get("f66")),
        "super_pct": _flow_number(row.get("f69")),
        "big_net": _flow_number(row.get("f72")),
        "big_pct": _flow_number(row.get("f75")),
        "mid_net": _flow_number(row.get("f78")),
        "mid_pct": _flow_number(row.get("f81")),
        "small_net": _flow_number(row.get("f84")),
        "small_pct": _flow_number(row.get("f87")),
    }


# ── 交易板分类 ───────────────────────────────────────────────────────
# 取值与 dashboard_settings.BOARD_* 一致，但在此独立定义：筛选引擎与 CLI 都要用，
# 不该反向依赖看板配置模块。这里的“板”是交易板（上市板块），与按行业计算的
# 「板块共振」是两个概念。
BOARD_MAIN = "main"
BOARD_CHINEXT = "chinext"
BOARD_STAR = "star"
BOARD_UNKNOWN = "unknown"
BOARD_ORDER = (BOARD_MAIN, BOARD_CHINEXT, BOARD_STAR)
BOARD_LABELS = {
    BOARD_MAIN: "沪深主板",
    BOARD_CHINEXT: "创业板",
    BOARD_STAR: "科创板",
    BOARD_UNKNOWN: "未知板",
}


def board_of(code: Any) -> str:
    """按代码前缀判定交易板；不确定的一律 unknown，由调用方排除。

    沪深主板 60/00（含 001/002/003）、创业板 300/301、科创板 688/689。
    北交所（43/83/87/920 等）与其它代码暂不在选项内 → unknown。
    """
    text = str(code or "").strip()
    if text.startswith(("60", "00")):
        return BOARD_MAIN
    if text.startswith(("300", "301")):
        return BOARD_CHINEXT
    if text.startswith(("688", "689")):
        return BOARD_STAR
    return BOARD_UNKNOWN


def normalize_boards(boards: Any) -> List[str]:
    """把外部传入的交易板集合归一为固定顺序、去重、剔除未知值。

    非法输入（空、未知值、类型错误）一律回退 ``["main"]``：失败时取**最窄**范围，
    而不是悄悄放宽到全部交易板。
    """
    if isinstance(boards, str):
        boards = [boards]
    if not isinstance(boards, (list, tuple, set)):
        return [BOARD_MAIN]
    chosen = {str(item) for item in boards}
    ordered = [board for board in BOARD_ORDER if board in chosen]
    return ordered or [BOARD_MAIN]


def valid_board_row(row: Dict[str, Any], boards: Any = (BOARD_MAIN,)) -> bool:
    """所选交易板内、且字段齐全的非 ST/退市行情行。"""
    if board_of(row.get("f12")) not in set(normalize_boards(boards)):
        return False
    name = str(row.get("f14", ""))
    if "ST" in name.upper() or name.startswith("*") or "退" in name:
        return False
    required = ["f2", "f3", "f5", "f6", "f8", "f10", "f15", "f18", "f21"]
    # The independent backup does not expose a compatible real-time volume
    # ratio.  Keep its trend-observation data available, but never synthesize
    # a ratio for strict ultra-short or intersection buy signals.
    if row.get("_source") == "sina_fallback":
        required.remove("f10")
    return all(row.get(k) not in (None, "-") for k in required)


def valid_main_board(row: Dict[str, Any]) -> bool:
    """兼容旧调用点：等价于只筛沪深主板。"""
    return valid_board_row(row, (BOARD_MAIN,))


def is_a_share_row(row: Dict[str, Any]) -> bool:
    """Keep mixed clist products out of A-share breadth calculations."""
    return str(row.get("f12", "")).startswith(("60", "00", "30", "68"))
