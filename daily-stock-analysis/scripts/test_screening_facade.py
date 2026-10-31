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
from screening import intersection, market_warnings, model, net, render, risk, rules
from screening.sources import kline

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

NET_EXPORTS = (
    "NETWORK_MODE", "REQUESTS_DIRECT_SESSION", "REQUESTS_SESSION", "SSL_CONTEXT", "UA",
    "NetworkUnavailable", "_EM_COOLDOWN_BACKOFF", "_EM_COOLDOWN_FILE", "_HOST_COOLDOWN",
    "_HOST_FAILURES", "_em_clear_cooldown", "_em_in_cooldown", "_em_read_cooldown",
    "_em_set_cooldown", "_mark_host_failed", "_mark_host_ok", "_rank_urls", "_system_proxy_url",
    "build_url_opener", "fetch_json", "format_network_failure", "requests", "set_network_mode",
    "network_path",
)

# 禁止经门面重定向的名字：读取它们的代码已全部在子模块里，在门面上赋值或 patch
# 只改了门面的副本，被测代码看不到——测试会以为自己隔离了状态或替换了依赖，实际
# 没有（状态写到默认位置、走了真实网络分支）。
# 规则：某个名字的**所有读取方**都已拆进子模块时才加进来。调用方仍在门面里的函数
# （例如 fetch_json：fetch_market 等还在核心）在门面上 patch 仍然生效，暂不加入，
# 待其调用方拆走的那一步再加。
FACADE_REDIRECT_FORBIDDEN = {
    "screening.intersection": ("INTERSECTION_STATE_FILE", "INTERSECTION_CALIBRATION_FILE"),
    "screening.net": (
        "NETWORK_MODE", "requests", "REQUESTS_SESSION", "REQUESTS_DIRECT_SESSION", "UA",
        "SSL_CONTEXT", "_HOST_FAILURES", "_HOST_COOLDOWN", "_EM_COOLDOWN_FILE",
        "_EM_COOLDOWN_BACKOFF", "_em_read_cooldown", "_system_proxy_url", "build_url_opener",
    ),
    # fetch_kline（引擎、shadow_tracker 经门面读取）、enrich_all / kline_source_summary
    # （核心流水线调用）仍有门面读取方，不在此列。
    "screening.sources.kline": (
        "_tencent_kline_fail_streak", "_tencent_kline_blocked_until", "_sina_kline_fallback_warned",
        "_kline_reference", "_kline_fallback_codes", "_kline_mismatch_codes",
        "TENCENT_KLINE_FAIL_STREAK_LIMIT", "TENCENT_KLINE_COOLDOWN_SECONDS", "KLINE_MIN_BARS",
        "KLINE_QFQ_DEVIATION_LIMIT", "KLINE_SOURCE_LABELS", "TENCENT_KLINE_URLS", "TENCENT_KLINE_URL",
        "SINA_KLINE_URL", "EM_KLINE_URLS", "EM_KLINE_URL", "EM_KLINE_FIELDS1", "EM_KLINE_FIELDS2",
        "parse_k_rows", "_parse_sina_kline", "_parse_tencent_qfq_kline", "_tencent_kline_ready",
        "_note_tencent_kline_result", "_note_sina_kline_fallback", "_record_kline_reference",
        "_note_sina_fallback_code", "enrich",
    ),
}

KLINE_EXPORTS = (
    "EM_KLINE_FIELDS1", "EM_KLINE_FIELDS2", "EM_KLINE_URL", "EM_KLINE_URLS", "KLINE_MIN_BARS",
    "KLINE_QFQ_DEVIATION_LIMIT", "KLINE_SOURCE_LABELS", "SINA_KLINE_URL",
    "TENCENT_KLINE_COOLDOWN_SECONDS", "TENCENT_KLINE_FAIL_STREAK_LIMIT", "TENCENT_KLINE_URL",
    "TENCENT_KLINE_URLS", "_kline_fallback_codes", "_kline_mismatch_codes", "_kline_reference",
    "_note_sina_fallback_code", "_note_sina_kline_fallback", "_note_tencent_kline_result",
    "_parse_sina_kline", "_parse_tencent_qfq_kline", "_record_kline_reference",
    "_tencent_kline_ready", "enrich", "enrich_all", "fetch_kline", "kline_source_summary",
    "parse_k_rows",
)

# 子模块用 global 改写的标量：门面若导出，只会是导入时的旧副本（读到的永远是初值，
# 断言会"恒真"）。门面不导出它们，误读会直接 AttributeError。
REBOUND_SCALARS_NOT_EXPORTED = {
    "screening.sources.kline": (
        "_tencent_kline_fail_streak", "_tencent_kline_blocked_until", "_sina_kline_fallback_warned",
    ),
}


RENDER_EXPORTS = (
    "RANGE_POSITION_TOP_PCT", "RANGE_POSITION_WARN_PCT", "flow_amount_str", "flow_status_cell",
    "flow_veto_suffix", "markdown_table", "range_position_cell", "render_markdown",
    "shadow_badge_text",
)


class FacadeIdentityTest(unittest.TestCase):
    def test_kline_names_are_reexported_not_copied(self):
        for name in KLINE_EXPORTS:
            with self.subTest(name=name):
                self.assertIs(getattr(screen, name), getattr(kline, name))

    def test_rebound_scalars_are_not_exported_by_the_facade(self):
        import importlib
        for mod, names in REBOUND_SCALARS_NOT_EXPORTED.items():
            module = importlib.import_module(mod)
            for name in names:
                with self.subTest(name=name):
                    self.assertTrue(hasattr(module, name))
                    self.assertFalse(hasattr(screen, name), "门面导出会变成导入时的旧副本")

    def test_market_warnings_is_one_shared_list(self):
        self.assertIs(screen.MARKET_WARNINGS, market_warnings.MARKET_WARNINGS)
        self.assertIs(kline.MARKET_WARNINGS, market_warnings.MARKET_WARNINGS)

    def test_net_names_are_reexported_not_copied(self):
        for name in NET_EXPORTS:
            with self.subTest(name=name):
                self.assertIs(getattr(screen, name), getattr(net, name))

    def test_render_names_are_reexported_not_copied(self):
        for name in RENDER_EXPORTS:
            with self.subTest(name=name):
                self.assertIs(getattr(screen, name), getattr(render, name))

    def test_intersection_names_are_reexported_not_copied(self):
        for name in INTERSECTION_EXPORTS:
            with self.subTest(name=name):
                self.assertIs(getattr(screen, name), getattr(intersection, name))

    def test_calibration_file_still_lives_beside_the_scripts(self):
        self.assertEqual(intersection.INTERSECTION_CALIBRATION_FILE,
                         screen.SCRIPT_DIR / "intersection_calibration.json")

    def test_no_code_redirects_moved_names_through_the_facade(self):
        alias = r"(?:screen|s|em|a_share_daily_screen)"
        owner = {name: mod for mod, names in FACADE_REDIRECT_FORBIDDEN.items() for name in names}
        names = "|".join(re.escape(n) for n in owner)
        patterns = [
            re.compile(rf"\b{alias}\.({names})\s*=(?!=)"),
            re.compile(rf"patch\.object\(\s*{alias}\s*,\s*[\"']({names})[\"']"),
            re.compile(rf"patch\(\s*[\"']a_share_daily_screen\.({names})[\"']"),
        ]
        offenders = []
        for path in sorted(list(SCRIPT_DIR.glob("*.py")) + list((PROJECT_ROOT / "tools").glob("*.py"))):
            if path.name == Path(__file__).name:
                continue
            for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                for p in patterns:
                    m = p.search(line)
                    if m:
                        offenders.append(f"{path.name}:{lineno}: {line.strip()}  → 改到 {owner[m.group(1)]}")
        self.assertEqual(offenders, [], "这些名字须在拥有它的子模块上重定向")

    def test_forbidden_names_really_live_in_their_module(self):
        # 防止名单本身过时：列出的每个名字都必须是对应子模块的属性
        import importlib
        for mod, names in FACADE_REDIRECT_FORBIDDEN.items():
            module = importlib.import_module(mod)
            for name in names:
                with self.subTest(module=mod, name=name):
                    self.assertTrue(hasattr(module, name))

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
