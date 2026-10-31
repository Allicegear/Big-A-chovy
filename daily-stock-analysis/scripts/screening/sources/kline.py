"""日 K 取数链（腾讯前复权 → 东财 push2his 前复权 → 新浪不复权）与行情行增强 enrich。

腾讯熔断计数、新浪降级与口径核对是本模块的进程级状态（global 重新绑定），
核心门面**不**重新导出这三个标量，读取或复位请直接使用本模块属性。
"""
from __future__ import annotations

import concurrent.futures as futures
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parents[2]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
import runtime_paths  # noqa: E402,F401
import tencent_kline  # noqa: E402  腾讯日 K 主机列表单一来源
from screening.market_warnings import MARKET_WARNINGS  # noqa: E402
from screening.model import Enriched, avg, is_number, normalize_row, secid_for  # noqa: E402
from screening.net import _mark_host_failed, _mark_host_ok, _rank_urls, fetch_json  # noqa: E402

# 腾讯日 K 主机列表收口在 tencent_kline 模块（引擎 / 看板 / 命令行工具共用一份），
# 单一来源避免"改一处漏三处"；WAF 实测记录见该模块 docstring。
TENCENT_KLINE_URLS = list(tencent_kline.TENCENT_KLINE_URLS)
TENCENT_KLINE_URL = TENCENT_KLINE_URLS[0]  # 兼容旧调用点
SINA_KLINE_URL = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
# 东财日 K（push2his）——第二档备源。2026-09-25 实测该路径被断而停用，2026-09-28 复测已恢复：
# 前复权（与腾讯同日收盘差 ≤0.2%）、含当日 bar，覆盖沪深主板/创业板/科创板/北交所。
# 与腾讯**非同厂商**，所以作为并列的第二档：2026-09-28 腾讯整链连续失败 47 分钟时，
# 若有这一档就不必整轮吃新浪不复权数据（实测池子被压掉约一半）。
EM_KLINE_URLS: Tuple[str, ...] = (
    "https://push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://1.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://82.push2his.eastmoney.com/api/qt/stock/kline/get",
)
EM_KLINE_URL = EM_KLINE_URLS[0]  # 兼容旧调用点与文档引用
# 东财 kline 的 fields2=f51..f61 列序与腾讯 qfqday 前 6 列一致（date,open,close,high,low,vol），
# 因此两档可共用 parse_k_rows，不必各写一个解析器。
EM_KLINE_FIELDS1 = "f1,f2,f3,f4,f5,f6"
EM_KLINE_FIELDS2 = "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"


def parse_k_rows(rows: List[Any]) -> List[Dict[str, float]]:
    out: List[Dict[str, float]] = []
    for p in rows:
        if isinstance(p, str):
            parts = p.split(",")
        else:
            parts = p
        if len(parts) < 6:
            continue
        out.append({
            "date": str(parts[0]),
            "open": float(parts[1]),
            "close": float(parts[2]),
            "high": float(parts[3]),
            "low": float(parts[4]),
            "vol": float(parts[5]),
        })
    return out


def _parse_sina_kline(data: Any) -> List[Dict[str, float]]:
    """Parse Sina K-line response: list of {day, open, high, low, close, volume}."""
    rows: List[Dict[str, float]] = []
    if not isinstance(data, list):
        return rows
    for item in data:
        try:
            rows.append({
                "date": str(item.get("day") or item.get("date") or "")[:10],
                "close": float(item["close"]),
                "open": float(item["open"]),
                "high": float(item["high"]),
                "low": float(item["low"]),
                "vol": float(item.get("volume", 0)),
            })
        except (KeyError, ValueError, TypeError):
            continue
    return rows


def _parse_tencent_qfq_kline(data: Any, symbol: str) -> List[Dict[str, float]]:
    """Parse Tencent's qfqday array into the screening K-line shape."""
    body = ((data or {}).get("data") or {}).get(symbol) or {}
    return parse_k_rows(body.get("qfqday") or [])


# ── 腾讯日 K 失败熔断 ────────────────────────────────────────────────
# 腾讯日 K（web.ifzq.gtimg.cn）会返回 WAF 拦截页而不是 JSON：2026-09-26 实测在代理与直连下
# 都返回 waf.tencent.com/501page.html，同一时间腾讯实时行情（qt.gtimg.cn）正常，
# 属于接口级拦截而非网络不通。若不做熔断，一轮 76 只候选会逐只重试并叠加多路径失败，
# 把一次拦截放大成上百次请求。连续失败达阈值即暂停该主机一段时间，期间直接走新浪降级源。
TENCENT_KLINE_FAIL_STREAK_LIMIT = 3
TENCENT_KLINE_COOLDOWN_SECONDS = 600.0
_tencent_kline_fail_streak = 0
_tencent_kline_blocked_until = 0.0
_sina_kline_fallback_warned = False


def _tencent_kline_ready() -> bool:
    return time.monotonic() >= _tencent_kline_blocked_until


def _note_tencent_kline_result(ok: bool) -> None:
    global _tencent_kline_fail_streak, _tencent_kline_blocked_until
    if ok:
        _tencent_kline_fail_streak = 0
        _tencent_kline_blocked_until = 0.0
        return
    _tencent_kline_fail_streak += 1
    if _tencent_kline_fail_streak >= TENCENT_KLINE_FAIL_STREAK_LIMIT:
        _tencent_kline_fail_streak = 0
        _tencent_kline_blocked_until = time.monotonic() + TENCENT_KLINE_COOLDOWN_SECONDS
        MARKET_WARNINGS.append(
            f"腾讯日 K 连续失败 {TENCENT_KLINE_FAIL_STREAK_LIMIT} 次，暂停 "
            f"{int(TENCENT_KLINE_COOLDOWN_SECONDS)} 秒（实测为 WAF 拦截页，非网络不通）；"
            "期间日 K 由新浪降级源提供，前复权口径可能不一致。"
        )


def _note_sina_kline_fallback() -> None:
    """新浪降级的一次性 stderr 提示（报告内的逐轮标注见 kline_source_summary）。"""
    global _sina_kline_fallback_warned
    if _sina_kline_fallback_warned:
        return
    _sina_kline_fallback_warned = True
    print("[kline] Sina fallback in use (unadjusted, may miss today's bar)", file=sys.stderr)


# ── 日 K 降级：口径自检与逐轮标注 ────────────────────────────────────
# 旧实现只在第一次落到新浪时往报告里塞一条警告（_sina_kline_fallback_warned 一次性），
# 2026-09-28 实测：连续 15 轮走新浪、107 份报告里只有 1 份带警告，频率被严重低估。
# 现在改为按轮汇总：每轮出报告时结算一次，并标注有多少只与前复权基准不一致。
KLINE_MIN_BARS = 65
# 同一交易日收盘与前复权基准的偏差上限；超过即视为复权口径不一致
KLINE_QFQ_DEVIATION_LIMIT = 0.01
# 前复权基准：code → (最新交易日, 收盘)。由腾讯/东财两档的成功结果维护，
# 供降级源比对——不复权价比前复权价低 1.4%~2.7%（2026-09-28 实测），必须显式指出。
_kline_reference: Dict[str, Tuple[str, float]] = {}
_kline_fallback_codes: set = set()
_kline_mismatch_codes: set = set()


def _record_kline_reference(code: str, bars: List[Dict[str, float]]) -> None:
    """用成功的前复权结果更新口径基准。"""
    if bars:
        _kline_reference[str(code)] = (str(bars[-1].get("date") or ""), float(bars[-1].get("close") or 0.0))


def _note_sina_fallback_code(code: str, bars: List[Dict[str, float]]) -> None:
    """登记一只走新浪降级的个股，并和基准比对日期/口径（用于逐轮标注）。"""
    _kline_fallback_codes.add(str(code))
    reference = _kline_reference.get(str(code))
    if not reference or not bars:
        return
    ref_date, ref_close = reference
    last = bars[-1]
    last_date = str(last.get("date") or "")
    last_close = float(last.get("close") or 0.0)
    date_mismatch = bool(ref_date) and last_date != ref_date
    deviation = abs(last_close / ref_close - 1) if ref_close else 0.0
    if date_mismatch or deviation > KLINE_QFQ_DEVIATION_LIMIT:
        _kline_mismatch_codes.add(str(code))


KLINE_SOURCE_LABELS = {
    "tencent_qfq": "腾讯前复权日K",
    "sina_daily": "新浪日K(可能不复权)",
    "eastmoney_qfq": "东财前复权日K",
}


def kline_source_summary(enriched: List[Enriched]) -> str:
    """按本轮实际用到的日 K 来源生成报告标签，并逐轮结算降级标注。

    2026-09-26 修正：原先写死“腾讯/东方财富日K”，但腾讯被 WAF 拦截时会整轮降级到新浪、
    东财日 K 又已下线，写死的来源会与实际不符，掩盖均线口径变化。
    2026-09-28 追加：降级警告改为**逐轮**输出（旧实现每进程只报一次，导致 15 轮降级里
    只有 1 份报告带警告），并点名有多少只与前复权基准的日期/收盘不一致。
    """
    seen: List[str] = []
    for e in enriched:
        source = getattr(e, "k_source", "") or ""
        if source and source not in seen:
            seen.append(source)
    fallback_count = len(_kline_fallback_codes)
    if fallback_count:
        mismatch = len(_kline_mismatch_codes)
        detail = (
            f"，其中 {mismatch} 只与前复权基准的日期或收盘不一致"
            f"（偏差>{KLINE_QFQ_DEVIATION_LIMIT * 100:.0f}%；均线类判据不可比）"
            if mismatch else ""
        )
        MARKET_WARNINGS.append(
            f"日K降级：本轮 {fallback_count} 只来自新浪（不复权、可能缺当日 bar）{detail}；"
            "均线与趋势确认池口径需按 source 字段复核，勿与本轮之外的数据直接比较。"
        )
    _kline_fallback_codes.clear()
    _kline_mismatch_codes.clear()
    if not seen:
        return "日K来源未记录"
    return " + ".join(KLINE_SOURCE_LABELS.get(s, s) for s in seen)


def fetch_kline(code: str, limit: int = 90) -> Tuple[List[Dict[str, float]], str]:
    """取前复权日 K：腾讯（主）→ 东财（备，独立厂商）→ 新浪（末档，不复权，逐轮标注）。

    三档都要求根数达标；前两档成功即登记口径基准，新浪命中时与该基准比对日期与收盘，
    不一致的个股计入 _kline_mismatch_codes，由 kline_source_summary 逐轮报出，
    避免不复权数据静默进入均线类判据（超短池 adj_close>ma5、趋势确认池全部均线条件）。
    """
    min_bars = min(KLINE_MIN_BARS, limit)
    # 1. 腾讯 qfq daily bars（主源，多主机故障转移 + 失败熔断）。
    if _tencent_kline_ready():
        normalized = str(code).strip().lower()
        symbol = normalized if normalized.startswith(("sh", "sz")) else ("sh" if normalized.startswith(("6", "9")) else "sz") + normalized
        all_hosts_failed = True
        for url in _rank_urls(TENCENT_KLINE_URLS):
            try:
                data = fetch_json(url, {
                    "param": f"{symbol},day,,,{limit},qfq",
                }, timeout=5, retries=1)
            except Exception:
                _mark_host_failed(url)
                continue
            all_hosts_failed = False
            try:
                parsed = _parse_tencent_qfq_kline(data, symbol)
            except (AttributeError, TypeError, ValueError):
                # 上游形状异常（拦截页/结构变更）按该主机失败处理，继续下一档，不要整只崩掉
                parsed = []
            if len(parsed) >= min_bars:
                _mark_host_ok(url)
                _note_tencent_kline_result(True)
                _record_kline_reference(code, parsed)
                return parsed, "tencent_qfq"
            _mark_host_failed(url)
        # 全部主机不可达/被拦才计入熔断；主机可达但样本不足（如次新股）不算故障
        _note_tencent_kline_result(True if not all_hosts_failed else False)
    # 2. 东财 push2his 前复权（并列备源，与腾讯非同厂商）。列序与腾讯 qfqday 前 6 列一致，
    #    因此共用 parse_k_rows。2026-09-25 该路径曾被断而停用，09-28 复测恢复。
    for url in _rank_urls(EM_KLINE_URLS):
        try:
            data = fetch_json(url, {
                "secid": secid_for(code),
                "klt": 101,          # 日线
                "fqt": 1,            # 前复权
                "lmt": limit,
                "end": "20500101",
                "fields1": EM_KLINE_FIELDS1,
                "fields2": EM_KLINE_FIELDS2,
            }, timeout=5, retries=1)
        except Exception:
            _mark_host_failed(url)
            continue
        # 形状容错：非预期结构（列表/文案/WAF 页）按该主机失败处理，继续下沉，不要整只崩掉
        try:
            body = data.get("data") if isinstance(data, dict) else None
            klines = body.get("klines") if isinstance(body, dict) else None
            parsed = parse_k_rows(klines) if isinstance(klines, list) else []
        except (AttributeError, TypeError, ValueError):
            parsed = []
        if len(parsed) >= min_bars:
            _mark_host_ok(url)
            _record_kline_reference(code, parsed)
            return parsed, "eastmoney_qfq"
        _mark_host_failed(url)
    # 3. 新浪（末档；该端点返回不复权历史价、且可能缺当日 bar，前复权均线会出现口径跳变）。
    try:
        symbol = ("sh" if code.startswith("6") else "sz") + code
        data = fetch_json(SINA_KLINE_URL, {
            "symbol": symbol, "scale": 240, "ma": "no", "datalen": limit,
        }, timeout=5, retries=1)
        parsed = _parse_sina_kline(data)
        if len(parsed) >= min_bars:
            _note_sina_kline_fallback()
            _note_sina_fallback_code(code, parsed)
            return parsed, "sina_daily"
    except Exception:
        pass
    raise RuntimeError(f"all kline sources failed for {code}")


def enrich(
    row: Dict[str, Any],
    fetch_kline_fn: Optional[Callable[..., Tuple[List[Dict[str, float]], str]]] = None,
) -> Optional[Enriched]:
    r = normalize_row(row)
    rows, source = (fetch_kline_fn or fetch_kline)(r["code"], 90)
    if len(rows) < 65:
        return None
    closes = [x["close"] for x in rows]
    highs = [x["high"] for x in rows]
    lows = [x["low"] for x in rows]
    vols = [x["vol"] for x in rows]
    ma5 = avg(closes[-5:])
    ma10 = avg(closes[-10:])
    ma20 = avg(closes[-20:])
    prev_ma5 = avg(closes[-6:-1])
    prev_ma10 = avg(closes[-11:-1])
    prev_ma20 = avg(closes[-21:-1])
    latest = closes[-1]
    high_chg = (r["high"] / r["prev_close"] - 1) * 100 if r["prev_close"] else float("nan")
    high_pull = max(0.0, high_chg - r["change"]) if is_number(high_chg) else float("nan")
    vwap = r["amount"] / (r["volume"] * 100) if r["volume"] else float("nan")
    vwap_state = "均价线上方" if is_number(vwap) and r["price"] >= vwap else ("均价线下方" if is_number(vwap) else "无法验证")
    prior_high = highs[-2] if len(highs) >= 2 else float("nan")
    prior_low = lows[-2] if len(lows) >= 2 else float("nan")
    price_above_vwap = is_number(vwap) and r["price"] >= vwap
    return Enriched(
        code=r["code"], name=r["name"], price=r["price"], change=r["change"], turnover=r["turnover"],
        amount=r["amount"], volume_ratio=r["volume_ratio"], high=r["high"], low=r["low"], open=r["open"],
        prev_close=r["prev_close"], total_mv=r["total_mv"], float_mv=r["float_mv"], industry=r["industry"],
        timestamp=r["timestamp"], volume=r["volume"], kdate=rows[-1]["date"], k_source=source, adj_close=latest,
        ma5=ma5, ma10=ma10, ma20=ma20, prev_ma5=prev_ma5, prev_ma10=prev_ma10, prev_ma20=prev_ma20,
        five_ret=latest / closes[-6] - 1 if len(closes) >= 6 and closes[-6] else float("nan"),
        dist60=max(highs[-60:]) / latest - 1 if latest else float("nan"),
        ma20_dist=r["price"] / ma20 - 1 if ma20 else float("nan"),
        high_pull=high_pull,
        cur_to_high=r["high"] / r["price"] - 1 if r["price"] else float("nan"),
        # 现价在当日区间的位置：0%=当日最低，100%=当日最高。只标注、不做门槛，
        # 用于把"是不是买在半山腰"变成可见数字（2026-09-26 加入）。
        range_pct=(
            (r["price"] - r["low"]) / (r["high"] - r["low"]) * 100
            if r["high"] > r["low"] else float("nan")
        ),
        vol_vs_avg5=r["volume"] / avg(vols[-6:-1]) if avg(vols[-6:-1]) else float("nan"),
        vwap=vwap,
        vwap_state=vwap_state,
        prior_high=prior_high,
        prior_low=prior_low,
        main_net=r.get("main_net", 0), main_pct=r.get("main_pct", 0),
        super_net=r.get("super_net", 0), super_pct=r.get("super_pct", 0),
        big_net=r.get("big_net", 0), big_pct=r.get("big_pct", 0),
        mid_net=r.get("mid_net", 0), mid_pct=r.get("mid_pct", 0),
        small_net=r.get("small_net", 0), small_pct=r.get("small_pct", 0),
        price_above_vwap=price_above_vwap,
        buy_ratio=round((100.0 + float(r.get("main_pct", 0))) / max(1.0, 100.0 - float(r.get("main_pct", 0))), 2) if is_number(r.get("main_pct")) else float("nan"),
    )


def enrich_all(
    rows: List[Dict[str, Any]],
    workers: int,
    fetch_kline_fn: Optional[Callable[..., Tuple[List[Dict[str, float]], str]]] = None,
) -> Tuple[List[Enriched], List[str]]:
    # 结果按输入顺序返回，而不是线程完成顺序：下游（如低开洗盘每轮 20 只的
    # 历史资金查询额度）按列表先后分配，完成顺序会让同一份行情每次结果不同。
    out: List[Enriched] = []
    errors: List[str] = []
    with futures.ThreadPoolExecutor(max_workers=workers) as pool:
        submitted = [(pool.submit(enrich, r, fetch_kline_fn), str(r.get("f12"))) for r in rows]
        for fut, code in submitted:
            try:
                item = fut.result()
                if item:
                    out.append(item)
                else:
                    errors.append(code)
            except Exception:
                errors.append(code)
    return out, errors
