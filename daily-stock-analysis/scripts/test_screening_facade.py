#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""兼容门面：a_share_daily_screen 重新导出的名字必须就是 screening 子模块里的同一对象。

防止门面里再出现同名副本（两份实现会悄悄分叉），也让依赖 ``screen.X`` 的
调用方与测试确知自己拿到的是真实实现。
"""

import re
import unittest
from pathlib import Path

import a_share_daily_screen as screen
from screening import intersection, model, risk, rules

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

MODEL_EXPORTS = (
    "BOARD_CHINEXT", "BOARD_LABELS", "BOARD_MAIN", "BOARD_ORDER", "BOARD_STAR", "BOARD_UNKNOWN",
    "Enriched", "_flow_number", "_fmt", "amount_yi", "avg", "board_of", "current_status",
    "is_a_share_row", "is_number", "normalize_boards", "normalize_row", "pct", "secid_for",
    "tencent_code", "valid_board_row", "valid_main_board",
)

RULES_EXPORTS = (
    "_trend_quality_checks", "has_resonance", "is_after_tail_risk", "low_trend_class",
    "low_trend_output_eligible", "low_trend_sort_key", "low_ultra_class",
    "low_ultra_output_eligible", "low_ultra_sort_key", "sector_stats", "strict_trend",
    "strict_ultra", "trend_condition_diagnosis", "trend_confirm_relaxed", "trend_observation",
)


RISK_EXPORTS = (
    "RISK_AVOID", "RISK_CHECKED_VALUES", "RISK_CLEAN", "RISK_STATUS_VALUES", "RISK_STATUSES",
    "RISK_UNKNOWN", "RISK_WATCH", "RISK_HARD_VETO", "_row_risk_status", "announcement_label",
    "append_risk_text", "risk_allows_state_qualification", "risk_blocks_formal_qualification",
    "risk_note_text",
)


INTERSECTION_EXPORTS = (
    "DEFAULT_INTERSECTION_CONFIG", "INTERSECTION_CALIBRATION_FILE", "INTERSECTION_CONFIG_VERSION",
    "INTERSECTION_STATE_FILE", "PHASE_ENTRY", "PHASE_EXPIRED", "PHASE_INVALID", "PHASE_LABELS",
    "PHASE_LATCHED", "PHASE_LATE", "PHASE_OBSERVING", "PHASE_ORDER", "PHASE_PRE",
    "PHASE_RETEST_READY", "PHASE_WAIT_RETEST", "_LEGACY_PHASE_MAP", "_canonical_phase",
    "_clock_minutes", "_config_is_safe", "_format_clock", "_intersection_rejection_reasons",
    "_late_filter_reasons", "_minute_freshness", "_parse_intersection_datetime", "_pre_gates",
    "_predict_trigger_price", "_pullback_zone", "compute_pre_intersection",
    "evaluate_intersection_states", "load_intersection_calibration", "load_intersection_state",
    "resolve_intersection_config", "resolve_market_mode", "save_intersection_state",
)

# 已拆到子模块、由子模块内部读取的状态路径：在门面上赋值或 patch 不会生效，
# 测试会以为自己隔离了状态，实际写的是默认位置。
MOVED_STATE_PATHS = ("INTERSECTION_STATE_FILE", "INTERSECTION_CALIBRATION_FILE")


class FacadeIdentityTest(unittest.TestCase):
    def test_intersection_names_are_reexported_not_copied(self):
        for name in INTERSECTION_EXPORTS:
            with self.subTest(name=name):
                self.assertIs(getattr(screen, name), getattr(intersection, name))

    def test_calibration_file_still_lives_beside_the_scripts(self):
        self.assertEqual(intersection.INTERSECTION_CALIBRATION_FILE,
                         screen.SCRIPT_DIR / "intersection_calibration.json")

    def test_no_code_redirects_moved_state_paths_through_the_facade(self):
        alias = r"(?:screen|s|a_share_daily_screen)"
        patterns = [
            re.compile(rf"\b{alias}\.({'|'.join(MOVED_STATE_PATHS)})\s*=(?!=)"),
            re.compile(rf"patch\.object\(\s*{alias}\s*,\s*[\"']({'|'.join(MOVED_STATE_PATHS)})[\"']"),
            re.compile(rf"patch\(\s*[\"']a_share_daily_screen\.({'|'.join(MOVED_STATE_PATHS)})[\"']"),
        ]
        offenders = []
        for path in sorted(list(SCRIPT_DIR.glob("*.py")) + list((PROJECT_ROOT / "tools").glob("*.py"))):
            if path.name == Path(__file__).name:
                continue
            for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if any(p.search(line) for p in patterns):
                    offenders.append(f"{path.name}:{lineno}: {line.strip()}")
        self.assertEqual(offenders, [], "请改 screening.intersection 模块上的属性")

    def test_risk_names_are_reexported_not_copied(self):
        for name in RISK_EXPORTS:
            with self.subTest(name=name):
                self.assertIs(getattr(screen, name), getattr(risk, name))

    def test_model_names_are_reexported_not_copied(self):
        for name in MODEL_EXPORTS:
            with self.subTest(name=name):
                self.assertIs(getattr(screen, name), getattr(model, name))

    def test_rules_names_are_reexported_not_copied(self):
        for name in RULES_EXPORTS:
            with self.subTest(name=name):
                self.assertIs(getattr(screen, name), getattr(rules, name))

    def test_rule_parameters_have_a_single_source(self):
        # 规则模块与核心读同一份参数对象，不是各自的拷贝
        self.assertIs(rules.SCREENING_CONFIG, screen.SCREENING_CONFIG)
        self.assertIs(rules.EXECUTION_CONFIG, screen.EXECUTION_CONFIG)

    def test_enriched_default_risk_matches_screen(self):
        self.assertIs(model.RISK_UNKNOWN, risk.RISK_UNKNOWN)


if __name__ == "__main__":
    unittest.main()
