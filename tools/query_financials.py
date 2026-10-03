#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only financial profile query.

Market multiples and disclosed financial statements are different evidence
families. This command keeps them separate: a positive dynamic PE is not used
to infer that a company is profitable when the latest disclosed EPS or net
profit is unavailable.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.data_sources.contracts import Result  # noqa: E402
from tools.data_sources.http import HTTPClient, HTTPClientError  # noqa: E402
from tools.data_sources.sina import SinaFinancialSource  # noqa: E402
from tools.data_sources.symbols import normalize_security  # noqa: E402
from tools.data_sources.tencent import parse_snapshot  # noqa: E402


EASTMONEY_QUOTE_URL = "https://push2.eastmoney.com/webguest/api/qt/stock/get"
TENCENT_QUOTE_URL = "https://qt.gtimg.cn/q="


def normalize_code_clean(code: str) -> str:
    """Compatibility helper returning the six-digit code when present."""
    match = re.search(r"\d{6}", str(code).strip().upper())
    return match.group(0) if match else str(code).strip()


def get_secid(code: str) -> str:
    return normalize_security(code).secid


def get_tsym(code: str) -> str:
    return normalize_security(code).tencent


def _number(value: Any, *, scale: float = 1.0) -> Optional[float]:
    if value in (None, "", "-", "--", "N/A", "null"):
        return None
    try:
        number = float(str(value).replace(",", "").replace("%", "")) / scale
    except (TypeError, ValueError):
        return None
    return number if number == number and number not in (float("inf"), float("-inf")) else None


def _eastmoney_snapshot(client: HTTPClient, symbol: Any) -> tuple[dict[str, Any], str | None]:
    try:
        response = client.get(
            EASTMONEY_QUOTE_URL,
            params={
                "secid": symbol.secid,
                "fields": "f57,f58,f43,f59,f162,f163,f164,f167,f173,f183,f184,f185,f186,f187",
            },
            retries=1,
        )
        payload = response.json()
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            raise ValueError("东财财务快照缺少 data")
        return data, None
    except (HTTPClientError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return {}, str(exc)


def _tencent_snapshot(client: HTTPClient, symbol: Any) -> tuple[dict[str, Any], str | None]:
    try:
        response = client.get(TENCENT_QUOTE_URL + symbol.tencent, headers={"Accept": "text/plain"}, retries=1)
        return parse_snapshot(response.text, symbol.tencent), None
    except (HTTPClientError, TypeError, ValueError, RuntimeError) as exc:
        return {}, str(exc)


def _first_row_value(row: dict[str, Any], names: tuple[str, ...]) -> Optional[float]:
    for name in names:
        for key, value in row.items():
            if key == name or name in str(key):
                parsed = _number(value)
                if parsed is not None:
                    return parsed
    return None


def _disclosed_financials(result: Result) -> dict[str, Any]:
    rows = result.data if isinstance(result.data, list) else []
    if not rows:
        return {
            "report_period": None,
            "eps_disclosed": None,
            "net_profit_disclosed": None,
            "revenue_disclosed": None,
            "revenue_growth_disclosed": None,
            "net_profit_growth_disclosed": None,
            "profit_evidence_status": "unknown",
            "profit_basis": "未取到最新利润表中的归母净利润或每股收益",
        }
    row = rows[0]
    eps = _first_row_value(row, ("基本每股收益", "稀释每股收益", "每股收益", "EPS"))
    net_profit = _first_row_value(row, ("归属于母公司所有者的净利润", "归属于母公司股东的净利润", "归母净利润", "净利润"))
    revenue = _first_row_value(row, ("营业总收入", "营业收入", "主营业务收入"))
    revenue_growth = _first_row_value(row, ("营业收入同比", "营业总收入同比"))
    profit_growth = _first_row_value(row, ("归属于母公司所有者的净利润_同比", "净利润_同比"))
    if (eps is not None and eps > 0) or (net_profit is not None and net_profit > 0):
        status = "profit"
        basis = "最新披露利润表为正"
    elif (eps is not None and eps < 0) or (net_profit is not None and net_profit < 0):
        status = "loss"
        basis = "最新披露利润表为负"
    elif eps is not None or net_profit is not None:
        status = "break_even"
        basis = "最新披露利润表为零或接近盈亏平衡"
    else:
        status = "unknown"
        basis = "最新披露利润表未提供可解析的归母净利润或每股收益"
    return {
        "report_period": row.get("report_period") or row.get("报告期"),
        "eps_disclosed": eps,
        "net_profit_disclosed": net_profit,
        "revenue_disclosed": revenue,
        "revenue_growth_disclosed": revenue_growth,
        "net_profit_growth_disclosed": profit_growth,
        "profit_evidence_status": status,
        "profit_basis": basis,
    }


def _ytd(symbol: Any, price: float) -> tuple[Optional[float], Optional[str], str]:
    """Use the existing qfq K-line adapter for a non-trading YTD statistic."""
    ytd_pct: Optional[float] = None
    ytd_error: Optional[str] = None
    ytd_basis = ""
    try:
        scripts_dir = ROOT / "daily-stock-analysis" / "scripts"
        if str(scripts_dir) not in sys.path:
            sys.path.insert(0, str(scripts_dir))
        import tencent_kline

        year = datetime.now().year
        payload, _ = tencent_kline.fetch_kline_json(
            symbol.tencent,
            300,
            start=f"{year - 1}-12-01",
            end=f"{year}-12-31",
            require_qfq=True,
            timeout=6,
        )
        days = tencent_kline.kline_rows(payload, symbol.tencent, require_qfq=True)
        previous = [row for row in days if str(row[0]) < f"{year}-01-01"]
        current = [row for row in days if str(row[0]) >= f"{year}-01-01"]
        if previous:
            base = float(previous[-1][2])
            ytd_basis = f"{previous[-1][0]} 收盘"
        elif current:
            base = float(current[0][1])
            ytd_basis = f"{current[0][0]} 开盘（次新股，无去年数据）"
        else:
            base = 0.0
        if base > 0:
            last_close = price if price > 0 else float((current or days)[-1][2])
            ytd_pct = round((last_close - base) / base * 100.0, 2)
        else:
            ytd_error = "腾讯日K未返回可用于 YTD 的数据"
    except Exception as exc:
        ytd_error = f"腾讯日K不可用（{type(exc).__name__}）"
    return ytd_pct, ytd_error, ytd_basis


def query_financial_profile(code: str, *, client: HTTPClient | None = None) -> Dict[str, Any]:
    symbol = normalize_security(code)
    http = client or HTTPClient()
    em, em_error = _eastmoney_snapshot(http, symbol)
    tq, tq_error = _tencent_snapshot(http, symbol)
    name = str(em.get("f58") or tq.get("name") or "")
    price = _number(em.get("f43"), scale=100.0) or _number(tq.get("price")) or 0.0
    pe_dynamic = _number(em.get("f162"), scale=100.0)
    pe_ttm = _number(em.get("f164"), scale=100.0)
    pb = _number(em.get("f167"), scale=100.0)
    if pe_dynamic is None:
        pe_dynamic = _number(tq.get("pe"))
    snapshot_eps = _number(em.get("f187"), scale=100.0)
    snapshot_revenue = _number(em.get("f183"), scale=100_000_000.0)
    snapshot_revenue_growth = _number(em.get("f184"))
    snapshot_net_profit_growth = _number(em.get("f186"))

    sina = SinaFinancialSource(client=http).fetch_reports(symbol.code, report_type="lrb", limit=8)
    disclosed = _disclosed_financials(sina)
    eps = disclosed["eps_disclosed"]
    evidence_status = disclosed["profit_evidence_status"]
    if evidence_status == "loss":
        fin_status, fin_color = "亏损 🔴", "red"
    elif evidence_status == "profit":
        fin_status, fin_color = "盈利 🟢", "green"
    elif evidence_status == "break_even":
        fin_status, fin_color = "微利/平衡 🟡", "yellow"
    else:
        fin_status, fin_color = "待披露/未知 ⚪", "gray"

    if fin_color == "red" or (pe_dynamic is not None and pe_dynamic < 0):
        safety_advice = "❌ 亏损股(不宜重仓)"
    elif fin_color == "green" and pe_dynamic is not None and 0 < pe_dynamic < 60:
        safety_advice = "✅ 稳健盈利(安全)"
    elif fin_color == "green" and pe_dynamic is not None and pe_dynamic >= 60:
        safety_advice = "⚠️ 盈利但高估值"
    elif evidence_status == "unknown" and pe_dynamic is not None and pe_dynamic > 0:
        safety_advice = "⚪ 动态PE为正，但实际披露盈利待核验"
    else:
        safety_advice = "⚪ 正常观察"

    ytd_pct, ytd_error, ytd_basis = _ytd(symbol, price)
    return {
        "code": symbol.code,
        "market": symbol.market,
        "name": name,
        "price": price,
        "pe": pe_dynamic,
        "pe_dynamic": pe_dynamic,
        "pe_ttm": pe_ttm,
        "pb": pb,
        "eps": eps,
        "eps_disclosed": eps,
        "eps_snapshot": snapshot_eps,
        "eps_source": "sina_disclosed_report" if eps is not None else "unavailable",
        "revenue_yi": disclosed["revenue_disclosed"] / 100_000_000 if disclosed["revenue_disclosed"] is not None else snapshot_revenue,
        "revenue_growth": disclosed["revenue_growth_disclosed"] if disclosed["revenue_growth_disclosed"] is not None else snapshot_revenue_growth,
        "net_profit_disclosed": disclosed["net_profit_disclosed"],
        "net_profit_growth": disclosed["net_profit_growth_disclosed"] if disclosed["net_profit_growth_disclosed"] is not None else snapshot_net_profit_growth,
        "report_period": disclosed["report_period"],
        "profit_evidence_status": evidence_status,
        "profit_basis": disclosed["profit_basis"],
        "fin_status": fin_status,
        "fin_color": fin_color,
        "safety_advice": safety_advice,
        "ytd_pct": ytd_pct,
        "ytd_error": ytd_error,
        "ytd_basis": ytd_basis,
        "sources": {
            "market_snapshot": {"source": "eastmoney_quote" if not em_error else "tencent_quote" if not tq_error else "unavailable", "error": em_error or tq_error},
            "disclosed_report": sina.to_dict(),
        },
        "warnings": [
            "动态PE/TTM PE属于估值快照，不替代最新披露利润表",
            "财务数据用于证据核验，不自动授予交易权限",
        ],
    }


def print_summary_table(results: List[Dict[str, Any]]) -> None:
    print("\n" + "=" * 108)
    print(f"{'代码':<8} {'名称':<8} {'现价':>7} {'披露业绩':<12} {'披露EPS':>11} {'动态PE':>8} {'TTM PE':>8} {'YTD':>10} {'基本面建议':<24}")
    print("-" * 108)
    for row in results:
        price = f"{row['price']:.2f}" if row.get("price") else "-"
        eps = f"{row['eps_disclosed']:+.3f}元" if row.get("eps_disclosed") is not None else "-"
        pe = f"{row['pe_dynamic']:.1f}" if row.get("pe_dynamic") is not None else "-"
        ttm = f"{row['pe_ttm']:.1f}" if row.get("pe_ttm") is not None else "-"
        ytd = f"{row['ytd_pct']:+.2f}%" if row.get("ytd_pct") is not None else "-"
        print(f"{row['code']:<8} {row['name']:<8} {price:>7} {row['fin_status']:<12} {eps:>11} {pe:>8} {ttm:>8} {ytd:>10} {row['safety_advice']:<24}")
    print("=" * 108)
    for row in results:
        if row.get("ytd_error"):
            print(f"⚠ {row['code']} {row['name']}：YTD 未取到 —— {row['ytd_error']}；按框架列为待核验项。")
        if row.get("profit_evidence_status") == "unknown":
            print(f"⚠ {row['code']}：{row['profit_basis']}；不能以正动态PE推断盈利。")
    print()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="股票披露财务与估值证据查询")
    parser.add_argument("codes", nargs="+", help="股票代码列表")
    parser.add_argument("--json", action="store_true", dest="json_mode", help="输出 JSON")
    args = parser.parse_args(argv)
    rows: list[dict[str, Any]] = []
    for raw in args.codes:
        for code in (part.strip() for part in raw.split(",")):
            if code:
                try:
                    rows.append(query_financial_profile(code))
                except Exception as exc:
                    rows.append({"code": normalize_code_clean(code), "name": "", "fin_status": "待披露/未知 ⚪", "safety_advice": "⚪ 正常观察", "error": str(exc)})
    if args.json_mode:
        print(json.dumps(rows, ensure_ascii=False, indent=2))
    else:
        print_summary_table(rows)
    return 0 if all("error" not in row for row in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())

