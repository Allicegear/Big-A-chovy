"""本轮行情/数据质量警告（报告顶部"warnings"）。

各数据源模块与核心流水线共用**同一个列表对象**：只做 append / clear / extend，
从不重新绑定，因此 ``from screening.market_warnings import MARKET_WARNINGS`` 拿到的
始终是同一份。整理为每轮上下文对象属于 P3（RunContext），此处只做原样搬移。
"""
from __future__ import annotations

from typing import List

MARKET_WARNINGS: List[str] = []
