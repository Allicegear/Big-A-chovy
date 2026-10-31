#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""兼容门面：a_share_daily_screen 重新导出的名字必须就是 screening 子模块里的同一对象。

防止门面里再出现同名副本（两份实现会悄悄分叉），也让依赖 ``screen.X`` 的
调用方与测试确知自己拿到的是真实实现。
"""

import unittest

import a_share_daily_screen as screen
from screening import model

MODEL_EXPORTS = (
    "BOARD_CHINEXT", "BOARD_LABELS", "BOARD_MAIN", "BOARD_ORDER", "BOARD_STAR", "BOARD_UNKNOWN",
    "Enriched", "_flow_number", "_fmt", "amount_yi", "avg", "board_of", "current_status",
    "is_a_share_row", "is_number", "normalize_boards", "normalize_row", "pct", "secid_for",
    "tencent_code", "valid_board_row", "valid_main_board",
)


class FacadeIdentityTest(unittest.TestCase):
    def test_model_names_are_reexported_not_copied(self):
        for name in MODEL_EXPORTS:
            with self.subTest(name=name):
                self.assertIs(getattr(screen, name), getattr(model, name))

    def test_enriched_default_risk_matches_screen(self):
        self.assertEqual(model.RISK_UNKNOWN, screen.RISK_UNKNOWN)


if __name__ == "__main__":
    unittest.main()
