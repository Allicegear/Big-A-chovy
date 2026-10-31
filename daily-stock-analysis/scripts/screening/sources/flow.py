"""资金数据源：本地资金快照历史（5/15 分钟增量）与东财当日 1 分钟累计资金序列兜底。

快照文件路径来自 runtime_paths（随 A_SHARE_STATE_DIR 隔离），写入经 state_commit 回合
提交门。分钟序列的每轮预算 / 限速 / 缓存是本模块的进程级状态；其中用 global 重新绑定
的标量（_FLOW_MINUTE_ROUND_EXPECTED_DATE、_FLOW_MINUTE_LAST_REQUEST_AT、
_LOW_ABSORB_UNVERIFIED_COUNT）核心门面不导出，读取请用本模块属性或对应函数。
"""
from __future__ import annotations

import json
import math
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

SCRIPT_DIR = Path(__file__).resolve().parents[2]
_PROJECT_ROOT = SCRIPT_DIR.parent.parent
for _p in (str(_PROJECT_ROOT), str(SCRIPT_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)
from tools.rule_config import RULE_CONFIG  # noqa: E402

import runtime_paths  # noqa: E402  运行状态路径唯一来源
import state_commit  # noqa: E402  回合状态提交门
from screening.model import Enriched, is_number, secid_for  # noqa: E402
from screening.net import _mark_host_failed, _mark_host_ok, _rank_urls, fetch_json  # noqa: E402

SCREENING_CONFIG = RULE_CONFIG["screening"]
TZ = ZoneInfo("Asia/Shanghai")

# 当日 1 分钟累计资金流序列。本地快照够不着 5/15 分钟基准时用它兜底，
# 只走 push2 系主机的 /webguest 路由（push2his 无 /webguest，standards 路由已下线）。
EM_FFLOW_MINUTE_URLS = [
    "https://push2.eastmoney.com/webguest/api/qt/stock/fflow/kline/get",
    "https://82.push2.eastmoney.com/webguest/api/qt/stock/fflow/kline/get",
]


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
