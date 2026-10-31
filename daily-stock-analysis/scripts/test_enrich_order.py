#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""enrich_all 必须按输入顺序返回，与线程完成顺序无关。

下游按列表先后分配有限额度（低开洗盘每轮 20 只历史资金查询），完成顺序会让
同一份行情每次得到不同结果。
"""

import time
import unittest
from unittest import mock

import a_share_daily_screen as screen
from screening.sources import kline


class EnrichAllOrderTest(unittest.TestCase):
    def test_results_follow_input_order_not_completion_order(self):
        rows = [{"f12": f"{600000 + i}"} for i in range(8)]

        def slow_first(row, fetch_kline_fn=None):
            # 越靠前越慢完成：完成顺序与输入顺序正好相反
            index = int(row["f12"]) - 600000
            time.sleep(0.02 * (8 - index))
            return None if index == 3 else row["f12"]

        # enrich_all 在 screening.sources.kline 里调用 enrich
        with mock.patch.object(kline, "enrich", side_effect=slow_first):
            out, errors = screen.enrich_all(rows, workers=8)

        self.assertEqual(out, [r["f12"] for r in rows if r["f12"] != "600003"])
        self.assertEqual(errors, ["600003"])


if __name__ == "__main__":
    unittest.main()
