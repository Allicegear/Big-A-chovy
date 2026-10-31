"""行情快照（东财 clist 分页 + 新浪独立备源）、大盘指数与行业板块列表的取数。

完整性证据写进 MARKET_FETCH_STATUS / SECTOR_BOARDS_STATUS（只原地更新，不重新绑定），
数据质量问题追加到共享的 MARKET_WARNINGS；网络请求统一经 screening.net.fetch_json。
"""
from __future__ import annotations

import concurrent.futures as futures
import math
import random
import time
from typing import Any, Dict, List, Optional, Tuple

from screening.market_warnings import MARKET_WARNINGS
from screening.model import is_number
from screening.net import (
    NetworkUnavailable,
    _em_clear_cooldown,
    _em_in_cooldown,
    _em_set_cooldown,
    _mark_host_ok,
    _rank_urls,
    fetch_json,
)

EASTMONEY_UT = "bd1d9ddb04089700cf9c27f6f7426281"

# 2026-09-24: 当前网络路径下标准 clist/ulist 入口失败；push2、82/72/83.push2 的
# /webguest 入口返回有效数据。push2delay/webguest 未通过探测，因此不列为候选。
CLIST_URLS = [
    "https://push2.eastmoney.com/webguest/api/qt/clist/get",
    "https://82.push2.eastmoney.com/webguest/api/qt/clist/get",
    "https://72.push2.eastmoney.com/webguest/api/qt/clist/get",
    "https://83.push2.eastmoney.com/webguest/api/qt/clist/get",
]
# Keep startup discovery on the same webguest route used by subsequent pages.
CLIST_STARTUP_URLS = [
    "https://push2.eastmoney.com/webguest/api/qt/clist/get",
    "https://82.push2.eastmoney.com/webguest/api/qt/clist/get",
]
SINA_MARKET_URL = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
SINA_PAGE_SIZE = 100
SINA_MAX_PAGES = 70
INDEX_URLS = [
    "https://push2.eastmoney.com/webguest/api/qt/ulist.np/get",
    "https://82.push2.eastmoney.com/webguest/api/qt/ulist.np/get",
    "https://72.push2.eastmoney.com/webguest/api/qt/ulist.np/get",
    "https://83.push2.eastmoney.com/webguest/api/qt/ulist.np/get",
]

CLIST_FIELDS = "f12,f14,f2,f3,f4,f5,f6,f7,f8,f10,f15,f16,f17,f18,f20,f21,f100,f124,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87"
INDEX_SECIDS = "1.000001,0.399001,1.000300,0.399006"

MARKET_FETCH_STATUS: Dict[str, Any] = {
    "source": "eastmoney_push2",
    "complete": None,
    "expected_pages": 0,
    "received_pages": 0,
    "failed_pages": [],
    "provider_total": None,
    "retrieved_rows": 0,
}


def get_market_fetch_status() -> Dict[str, Any]:
    """Return a copy so reports cannot accidentally mutate fetch evidence."""
    return {
        **MARKET_FETCH_STATUS,
        "failed_pages": list(MARKET_FETCH_STATUS.get("failed_pages") or []),
    }


def _sina_number(value: Any) -> Optional[float]:
    if value in (None, "", "-", "--"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_sina_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Map the independent Sina snapshot to the fields the screener knows.

    Sina does not provide Eastmoney's real-time volume ratio, capital-flow, or
    industry fields in this endpoint.  Those values intentionally remain
    missing instead of being guessed from unrelated data.
    """
    symbol = str(row.get("symbol") or "")
    code = str(row.get("code") or "")
    if symbol.startswith(("sh", "sz")):
        code = symbol[2:]
    if not code:
        return None

    price = _sina_number(row.get("trade"))
    prev_close = _sina_number(row.get("settlement"))
    high = _sina_number(row.get("high"))
    low = _sina_number(row.get("low"))
    open_price = _sina_number(row.get("open"))
    volume = _sina_number(row.get("volume"))
    amount = _sina_number(row.get("amount"))
    turnover = _sina_number(row.get("turnoverratio"))
    total_mv = _sina_number(row.get("mktcap"))
    float_mv = _sina_number(row.get("nmc"))
    change = _sina_number(row.get("changepercent"))
    if price is None or prev_close is None:
        return None

    return {
        "f12": code,
        "f14": str(row.get("name") or ""),
        "f2": price,
        "f3": change,
        "f4": _sina_number(row.get("pricechange")),
        # Sina volumes are shares while Eastmoney's f5 is hands.
        "f5": volume / 100 if volume is not None else None,
        "f6": amount,
        "f7": _sina_number(row.get("amplitude")),
        "f8": turnover,
        "f10": None,
        "f15": high,
        "f16": low,
        "f17": open_price,
        "f18": prev_close,
        "f20": total_mv * 10_000 if total_mv is not None else None,
        "f21": float_mv * 10_000 if float_mv is not None else None,
        "f100": "-",
        "f124": 0,
        "_source": "sina_fallback",
    }


def fetch_sina_market() -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """Fetch a broad A-share fallback snapshot from Sina in bounded batches.

    There is no authoritative total in this endpoint.  The caller must treat
    this as a data-quality downgrade even when all pages appear available.
    """
    def _fetch_page(page: int) -> Tuple[int, List[Dict[str, Any]]]:
        data = fetch_json(SINA_MARKET_URL, {
            "page": page,
            "num": SINA_PAGE_SIZE,
            "sort": "symbol",
            "asc": 1,
            "node": "hs_a",
            "_s_r_a": "page",
        })
        if not isinstance(data, list):
            raise RuntimeError("新浪行情返回格式异常")
        return page, data

    raw_pages: Dict[int, List[Dict[str, Any]]] = {}
    first_error: Optional[Exception] = None
    reached_end = False
    for batch_start in range(1, SINA_MAX_PAGES + 1, 10):
        pages = list(range(batch_start, min(batch_start + 10, SINA_MAX_PAGES + 1)))
        with futures.ThreadPoolExecutor(max_workers=len(pages)) as pool:
            pending = {pool.submit(_fetch_page, page): page for page in pages}
            for future in futures.as_completed(pending):
                page = pending[future]
                try:
                    _, data = future.result()
                    raw_pages[page] = data
                except Exception as exc:
                    first_error = first_error or exc

        if not raw_pages and first_error is not None:
            raise first_error
        if any(len(raw_pages.get(page, [])) < SINA_PAGE_SIZE for page in pages):
            reached_end = True
            break

    if not raw_pages:
        raise first_error or RuntimeError("新浪备用行情未返回数据")

    rows: List[Dict[str, Any]] = []
    for page in sorted(raw_pages):
        for raw in raw_pages[page]:
            normalized = _normalize_sina_row(raw)
            if normalized is not None:
                rows.append(normalized)

    if not reached_end:
        MARKET_WARNINGS.append("新浪备用行情达到分页上限，当前仅为局部快照")
    return rows, len(rows)


def fetch_market() -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """Fetch a verifiable market snapshot instead of silently accepting pages.

    Eastmoney returns clist data in the requested sort order.  A failed tail
    page therefore used to look like a valid all-riser market when ``fid=f3``
    was used.  We fetch in stable code order, retry missing pages with lower
    concurrency, and expose completion evidence to callers.
    """
    MARKET_FETCH_STATUS.update({
        "source": "eastmoney_push2",
        "complete": None,
        "expected_pages": 0,
        "received_pages": 0,
        "failed_pages": [],
        "provider_total": None,
        "retrieved_rows": 0,
    })
    rows: List[Dict[str, Any]] = []
    total: Optional[int] = None
    # Eastmoney's clist endpoint currently caps this route at 100 rows even
    # when a larger ``pz`` is requested.  Requesting the real cap keeps the
    # expected page count aligned with what the server actually returns.
    page_size = 100

    def _make_params(page: int) -> Dict[str, Any]:
        return {
            "pn": page, "pz": page_size, "po": 1, "np": 1,
            "ut": EASTMONEY_UT, "fltt": 2, "invt": 2, "fid": "f12",
            "fs": "m:1+t:2,m:0+t:6,m:0+t:80,m:1+t:23",  # 已去掉 m:0+t:81(北交所+新三板)：剔除场外层、减少约55%全市场拉取量
            "fields": CLIST_FIELDS,
        }

    # --- page 1: discover total ------------------------------------------
    last_error: Optional[Exception] = None

    def _fetch_first_page() -> Optional[dict]:
        """首页决定全量，失败即坠入降级；每 host 重试 2 次抗击瞬时抖动。"""
        nonlocal last_error
        for url in _rank_urls(CLIST_STARTUP_URLS):
            for attempt in range(2):
                try:
                    data = fetch_json(url, _make_params(1))
                    if data.get("data") is not None:
                        return data
                    break  # 该 host 返回空 data，换下一 host
                except Exception as exc:
                    last_error = exc
                    if attempt == 0:
                        time.sleep(0.3)
        return None

    first_data = None
    in_cooldown = _em_in_cooldown()
    if in_cooldown:
        MARKET_WARNINGS.append("东方财富处于冷却期，但本次仍会尝试主源（若成功则自动恢复）")
    # 首页失败会直接坠入新浪降级并把池子清空，因此值得多试几轮。
    # cooldown 内试 2 轮，正常试 4 轮，每轮每 host 重试 2 次，轮间递增退避。
    max_rounds = 2 if in_cooldown else 4
    for startup_round in range(max_rounds):
        first_data = _fetch_first_page()
        if first_data is not None:
            _em_clear_cooldown()
            MARKET_WARNINGS.clear()  # cooldown/降级相关提示已不适用
            break
        if startup_round < max_rounds - 1:
            time.sleep(1.5 * (startup_round + 1))
    if first_data is None:
        _em_set_cooldown()
        try:
            fallback_rows, fallback_total = fetch_sina_market()
        except Exception:
            if isinstance(last_error, NetworkUnavailable):
                raise last_error
            raise RuntimeError(f"行情主源和备用源均失败：{last_error}")
        MARKET_FETCH_STATUS.update({
            "source": "sina_fallback",
            "complete": False,
            "expected_pages": 0,
            "received_pages": 0,
            "failed_pages": [],
            # This endpoint has no authoritative total.  Do not promote the
            # retrieved count into a fake provider total.
            "provider_total": None,
            "retrieved_rows": len(fallback_rows),
        })
        MARKET_WARNINGS.append(
            "东方财富实时列表不可用，已切换新浪备用行情；量比、主力资金和行业字段缺失，"
            "市场宽度、板块共振及严格超短/交集买点已禁用"
        )
        return fallback_rows, None

    _em_clear_cooldown()  # East Money is back; reset cooldown
    body = first_data.get("data") or {}
    diff = body.get("diff") or []
    total = body.get("total")
    MARKET_FETCH_STATUS["provider_total"] = total
    if diff:
        rows.extend(diff)

    if not isinstance(total, int) or total < 0:
        MARKET_FETCH_STATUS.update({
            "complete": False,
            "expected_pages": 1,
            "received_pages": 1 if diff else 0,
            "failed_pages": [] if diff else [1],
            "retrieved_rows": len(rows),
        })
        MARKET_WARNINGS.append("行情服务端未返回可校验总数，当前仅为局部快照；市场宽度和板块共振已禁用")
        return rows, total

    if not diff and total > 0:
        MARKET_FETCH_STATUS.update({
            "complete": False,
            "expected_pages": math.ceil(total / page_size),
            "received_pages": 0,
            "failed_pages": [1],
            "retrieved_rows": 0,
        })
        MARKET_WARNINGS.append("行情第一页为空，当前仅为局部快照；市场宽度和板块共振已禁用")
        return rows, total

    expected_pages = max(1, math.ceil(total / page_size))
    page_rows: Dict[int, List[Dict[str, Any]]] = {1: diff} if diff else {}

    def _fetch_page(pn: int) -> Tuple[int, List[Dict[str, Any]]]:
        # 随机抖动避免固定间隔被识别为爬虫
        time.sleep(random.uniform(0.3, 0.8))
        for url in _rank_urls(CLIST_URLS):
            # 单 host 失败重试一次，抗击瞬时抖动/限流（之前任一异常即跳下一 host）
            for attempt in range(2):
                try:
                    data = fetch_json(url, _make_params(pn))
                    bd = (data.get("data") or {}).get("diff") or []
                    if bd:
                        return pn, bd
                    break  # 该 host 返回空 diff，换下一个 host 试试
                except Exception:
                    if attempt == 0:
                        time.sleep(0.3)
        return pn, []

    def _collect_pages(pages: List[int], workers: int) -> List[int]:
        missing: List[int] = []
        if not pages:
            return missing
        with futures.ThreadPoolExecutor(max_workers=max(1, min(workers, len(pages)))) as pool:
            futs = {pool.submit(_fetch_page, pn): pn for pn in pages}
            for fut in futures.as_completed(futs):
                pn, bd = fut.result()
                if bd:
                    page_rows[pn] = bd
                else:
                    missing.append(pn)
        return sorted(missing)

    # Two concurrent workers cap burst rate below provider's threshold.
    # Three workers previously triggered persistent rate limiting (2026-08 batch).
    missing_pages = _collect_pages(list(range(2, expected_pages + 1)), workers=2)
    # 失败页重试 5 轮（指数退避 0.5/1/2/4/8s），并发随轮次递减。
    # 交易时段东财接口偶发限流会让个别页失败，多轮退避能显著减少「快照不完整」。
    for retry_round in range(5):
        if not missing_pages:
            break
        time.sleep(0.5 * (2 ** retry_round))
        missing_pages = _collect_pages(missing_pages, workers=max(1, 3 - (retry_round // 2)))

    for pn in sorted(page_rows):
        if pn != 1:
            rows.extend(page_rows[pn])

    received_pages = len(page_rows)
    complete = not missing_pages and received_pages == expected_pages and len(rows) >= total
    MARKET_FETCH_STATUS.update({
        "complete": complete,
        "expected_pages": expected_pages,
        "received_pages": received_pages,
        "failed_pages": missing_pages,
        "provider_total": total,
        "retrieved_rows": len(rows),
    })
    if not complete:
        failed_text = ",".join(map(str, missing_pages)) if missing_pages else "无"
        MARKET_WARNINGS.append(
            "行情全量快照不完整："
            f"应取 {expected_pages} 页，已取 {received_pages} 页，失败页 {failed_text}，"
            f"服务端 {total} 条 / 实得 {len(rows)} 条；当前仅为局部快照，"
            "市场宽度和板块共振已禁用"
        )

    # 密集分页请求后冷却，避免紧跟的指数/板块/K线请求触发东财限流
    time.sleep(2.0 + random.uniform(0, 1.0))
    return rows, total


def fetch_indices() -> List[Dict[str, Any]]:
    params = {
        "fltt": 2,
        "invt": 2,
        "fields": "f12,f14,f2,f3,f4,f6,f104,f105,f106,f124",
        "secids": INDEX_SECIDS,
        "ut": EASTMONEY_UT,
    }
    for url in _rank_urls(INDEX_URLS):
        try:
            data = fetch_json(url, params)
            _mark_host_ok(url)
            return (data.get("data") or {}).get("diff") or []
        except Exception:
            continue
    MARKET_WARNINGS.append("所有指数接口失败，指数数据缺失")
    return []


SECTOR_BOARD_PAGE_SIZE = 100
# 安全上限：total 异常时不至于无限翻页
SECTOR_BOARD_MAX_PAGES = 20
# 翻页排序键必须是**盘中不变**的字段。原用 f3（涨跌幅）实时排序：
# 2026-09-28 实测同一页隔 6 秒取两次只重合 92/100，首行代码都会变。5 页串行取数
# 期间板块在页间移动 → 有的板块被取到两次（去重后计数变少）、有的整轮漏掉。
# 上游页大小上限是 100（pz=600 仍只回 100 行），所以只能靠固定排序键消除漂移；
# f12（板块代码）实测两次取数 100/100 一致。
SECTOR_BOARD_SORT_FIELD = "f12"
SECTOR_BOARD_FIELDS = "f12,f14,f2,f3,f4,f8,f104,f105,f124"
# 板块列表的完整性状态（与 MARKET_FETCH_STATUS 同一用途，供报告与诊断读取）
SECTOR_BOARDS_STATUS: Dict[str, Any] = {
    "complete": None, "provider_total": None, "expected_pages": 0,
    "received_pages": 0, "failed_pages": [], "retrieved_rows": 0,
    "raw_rows": 0, "duplicate_rows": 0, "blank_code_rows": 0, "missing_rows": 0,
    "sort_field": SECTOR_BOARD_SORT_FIELD,
}


def _sector_board_params(page: int) -> Dict[str, Any]:
    return {
        "pn": page,
        "pz": SECTOR_BOARD_PAGE_SIZE,
        "po": 1,
        "np": 1,
        "fltt": 2,
        "invt": 2,
        "fid": SECTOR_BOARD_SORT_FIELD,
        "fs": "m:90+t:2",
        "fields": SECTOR_BOARD_FIELDS,
        "ut": EASTMONEY_UT,
    }


def _fetch_sector_page(page: int) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """取一页板块原始行，并返回服务端 total。所有主机都失败时抛异常。"""
    last_error: Optional[Exception] = None
    for url in _rank_urls(CLIST_URLS):
        try:
            data = fetch_json(url, _sector_board_params(page))
        except Exception as exc:
            last_error = exc
            continue
        body = (data or {}).get("data") or {}
        return list(body.get("diff") or []), body.get("total")
    raise last_error or RuntimeError("板块分页取数失败")


def fetch_sector_indices() -> List[Dict[str, Any]]:
    """拉全部行业板块：按服务端 total 分页，失败页重试一次，并校验完整性。

    2026-09-27 修正（同类工具调研发现）：
    - 原实现固定最多 5 页且**不读 total**——板块数超过 500 时会被静默截断；
    - 某页在所有主机都失败时会直接 break，悄悄返回部分板块，调用方无从察觉。
    现在按 total 推算期望页数（带安全上限），失败页重试一次，并把「页数/行数是否符合预期」
    写进 `SECTOR_BOARDS_STATUS` 与报告警告——不完整必须可见，不能静默。

    2026-09-28 追加修正（盘中 total=496/实收 491 且无失败页）：
    - 翻页排序键从 f3（实时涨跌幅）改为 f12（板块代码），消除页间漂移；
    - 去重键从板块名改为板块代码——同名板块不会被互相顶掉，缺代码的行单独计数；
    - 完整性同时记录原始行数/重复行/缺失行，警告写清差异构成，避免下次只剩
      「实收 N」而看不出是重复、漂移还是真丢。
    """
    rows_by_page: Dict[int, List[Dict[str, Any]]] = {}
    total: Optional[int] = None
    try:
        rows_by_page[1], total = _fetch_sector_page(1)
    except Exception:
        MARKET_WARNINGS.append("板块指数查询失败：首屏未取到，板块列表与板块指数区块将为空")
        SECTOR_BOARDS_STATUS.update({
            "complete": False, "provider_total": None, "expected_pages": 1,
            "received_pages": 0, "failed_pages": [1], "retrieved_rows": 0,
            "raw_rows": 0, "duplicate_rows": 0, "blank_code_rows": 0, "missing_rows": 0,
            "sort_field": SECTOR_BOARD_SORT_FIELD,
        })
        return []

    expected_pages = 1
    if is_number(total) and int(total) > 0:
        expected_pages = max(1, math.ceil(int(total) / SECTOR_BOARD_PAGE_SIZE))
        if expected_pages > SECTOR_BOARD_MAX_PAGES:
            MARKET_WARNINGS.append(
                f"板块 total={int(total)} 推算需 {expected_pages} 页，超过安全上限 "
                f"{SECTOR_BOARD_MAX_PAGES} 页；按上限取数并标记不完整"
            )
            expected_pages = SECTOR_BOARD_MAX_PAGES

    failed_pages: List[int] = []
    for page in range(2, expected_pages + 1):
        try:
            rows_by_page[page], _ = _fetch_sector_page(page)
        except Exception:
            failed_pages.append(page)
    for page in list(failed_pages):     # 失败页重试一次，与全市场分页同样处理
        try:
            rows_by_page[page], _ = _fetch_sector_page(page)
            failed_pages.remove(page)
        except Exception:
            continue

    all_boards: List[Dict[str, Any]] = []
    seen_codes: set = set()
    raw_rows = 0
    duplicate_rows = 0
    blank_code_rows = 0
    for page in sorted(rows_by_page):
        for b in rows_by_page[page]:
            raw_rows += 1
            code = str(b.get("f12") or "").strip()
            name = b.get("f14", "")
            if not code:
                # 代码是去重键，也是与自选/共振映射的键；缺代码的行不能算有效板块
                blank_code_rows += 1
                continue
            if code in seen_codes:
                duplicate_rows += 1
                continue
            seen_codes.add(code)
            all_boards.append({
                "code": code,
                "name": name,
                "price": b.get("f2"),
                "change": b.get("f3"),
                "turnover": b.get("f8"),
                "up_count": b.get("f104"),
                "down_count": b.get("f105"),
            })

    provider_total = int(total) if is_number(total) else None
    missing_rows = max(0, provider_total - len(all_boards)) if provider_total is not None else 0
    complete = (not failed_pages) and (provider_total is None or len(all_boards) >= provider_total)
    SECTOR_BOARDS_STATUS.update({
        "complete": complete,
        "provider_total": provider_total,
        "expected_pages": expected_pages,
        "received_pages": len(rows_by_page),
        "failed_pages": failed_pages,
        "retrieved_rows": len(all_boards),
        "raw_rows": raw_rows,
        "duplicate_rows": duplicate_rows,
        "blank_code_rows": blank_code_rows,
        "missing_rows": missing_rows,
        "sort_field": SECTOR_BOARD_SORT_FIELD,
    })
    if not complete:
        # 差异构成写清楚，否则下次只看到「实收 N」无法判断是漂移、重复还是真丢
        detail: List[str] = []
        if failed_pages:
            detail.append(f"失败页 {sorted(failed_pages)}")
        if missing_rows:
            detail.append(f"较 total 少 {missing_rows} 个")
        if duplicate_rows:
            detail.append(f"重复行 {duplicate_rows}")
        if blank_code_rows:
            detail.append(f"缺板块代码 {blank_code_rows} 行")
        MARKET_WARNINGS.append(
            f"板块列表不完整：服务端 total={provider_total if provider_total is not None else '未知'}，"
            f"实收 {len(all_boards)} 个板块（原始 {raw_rows} 行"
            + (f"；{'、'.join(detail)}" if detail else "")
            + "）；板块指数区块与双池交集的板块涨跌可能缺失，按不完整数据处理"
        )
    return all_boards
