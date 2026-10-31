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
from screening import (
    capital, intersection, market_warnings, model, net, render, risk, rules, watchlist,
)
from screening.sources import announcements, flow, kline, market

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
    "screening.intersection": (
        "INTERSECTION_STATE_FILE", "INTERSECTION_CALIBRATION_FILE",
        # 观察池突破状态机拆走后，门面里已无读取方（交集与观察池两个模块各自导入）
        "_clock_minutes",
    ),
    # load/save_watchlist_breakout_state、evaluate_watchlist_breakout_states、build_watchlist
    # 由核心流水线调用，不在此列。
    "screening.watchlist": ("WATCHLIST_BREAKOUT_STATE_PATH", "_breakout_snapshot_epoch"),
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
    # fetch_market / fetch_indices / fetch_sector_indices / get_market_fetch_status（核心
    # 流水线调用）、SINA_MARKET_URL / _normalize_sina_row（看板经门面读取）仍有门面读取方。
    "screening.sources.market": (
        "EASTMONEY_UT", "CLIST_URLS", "CLIST_STARTUP_URLS", "CLIST_FIELDS", "INDEX_URLS",
        "INDEX_SECIDS", "SINA_PAGE_SIZE", "SINA_MAX_PAGES", "SECTOR_BOARD_PAGE_SIZE",
        "SECTOR_BOARD_MAX_PAGES", "SECTOR_BOARD_SORT_FIELD", "SECTOR_BOARD_FIELDS",
        "MARKET_FETCH_STATUS", "SECTOR_BOARDS_STATUS", "_sina_number", "fetch_sina_market",
        "_sector_board_params", "_fetch_sector_page",
    ),
    # attach_announcement_risks / apply_announcement_pool_gates / ANNOUNCEMENT_SECTIONS
    # （核心流水线使用）仍有门面读取方。tools.data_sources 的几个导入名在门面里仍有绑定
    # 但已无人使用，patch 门面上的它们不会影响公告取数，一并禁止。
    "screening.sources.announcements": (
        "ANNOUNCEMENT_URL", "ANNOUNCEMENT_CACHE_FILE", "_ANNOUNCEMENT_RISK_CACHE",
        "ANNOUNCEMENT_CACHE_TTL_SECONDS", "ANNOUNCEMENT_REQUEST_TIMEOUT_SECONDS",
        "ANNOUNCEMENT_CONFIG", "HARD_ANNOUNCEMENT_KEYWORDS", "WATCH_ANNOUNCEMENT_KEYWORDS",
        "ANNOUNCEMENT_IGNORE_KEYWORDS", "collect_announcement_titles", "_announcement_container",
        "_announcement_business_failure", "_has_announcement_container", "_announcement_row_title",
        "_announcement_total", "_announcement_total_from_response",
        "_validate_announcement_page_count", "_validate_primary_announcement_response",
        "fetch_announcements", "classify_announcement_risk", "_load_announcement_risk_cache",
        "_save_announcement_risk_cache", "CNInfoAnnouncementSource", "project_http_client",
        "fetch_announcement_evidence", "extract_primary_announcement_container",
        "extract_primary_announcement_total", "validate_primary_announcement_page",
        "_canonical_announcement_risk",
    ),
    # 流水线仍经门面调用的不在此列：load/save_flow_history、apply_flow_increments、
    # fill_flow_increments_from_fflow、reset_flow_minute_round、low_absorb_unverified_count。
    # （F2 起低吸排除 / 低开洗盘 / 主导判定已移入 capital，其余分钟序列与日线名字不再有门面读取方。）
    "screening.sources.flow": (
        "FLOW_SNAPSHOT_PATH", "EM_FFLOW_MINUTE_URLS", "FLOW_MINUTE_TTL_SECONDS",
        "FLOW_MINUTE_MIN_INTERVAL_SECONDS", "FLOW_MINUTE_MAX_CODES_PER_ROUND",
        "FLOW_MINUTE_SAFETY_RESERVE", "_FLOW_MINUTE_CACHE", "_FLOW_MINUTE_LOCK",
        "_FLOW_MINUTE_ROUND_CODES", "_FLOW_MINUTE_ROUND_LOCK", "_FLOW_MINUTE_ROUND_EXPECTED_DATE",
        "_FLOW_MINUTE_LAST_REQUEST_AT", "_LOW_ABSORB_UNVERIFIED_COUNT", "_flow_minute_budget_ok",
        "_contiguous_tail_start", "_column_values", "flow_window_deltas", "_is_intraday_now",
        "_minutes_of_hhmm", "fetch_flow_minutes", "flow_increments_from_minutes",
        "compute_buy_ratio",
        # F2 追加：调用方已全部移入 capital / flow
        "_pace_flow_minute_request", "round_series_date", "_flow_minute_series_for",
        "_note_low_absorb_unverified", "flow_continuity_from_minutes",
        "flow_negative_samples_from_minutes", "_LOW_OPEN_DAILY_FLOW_CACHE",
        "_LOW_OPEN_DAILY_FLOW_CACHE_LOCK", "EM_FFLOW_DAILY_URLS", "LOW_OPEN_DAILY_FLOW_SESSIONS",
        "LOW_OPEN_DAILY_FLOW_MAX_CODES_PER_ROUND", "LOW_OPEN_DAILY_FLOW_CACHE_SECONDS",
        "fetch_daily_main_flow_history",
    ),
    # 流水线 / 交集与突破状态机 / 看板引擎经门面调用的不在此列：_should_exclude_from_low_absorb、
    # classify_flow、evaluate_dominance_type、flow_veto_reason、low_open_wash_rows、
    # rank_capital_candidates、build/count_negative_super_observations。
    # 说明：fetch_json 门面里已无调用方，但看板引擎 / 看板探测 / verify_em_webguest 仍经
    # screen.fetch_json 调用，在门面上 patch 仍然生效，因此不加入。
    "screening.capital": (
        "HARD_BLACKLIST", "_low_open_wash_current_conditions", "_qualifies_low_open_wash",
        "capital_data_label", "is_negative_super",
    ),
}

DAILY_FLOW_EXPORTS = (
    "EM_FFLOW_DAILY_URLS", "LOW_OPEN_DAILY_FLOW_CACHE_SECONDS",
    "LOW_OPEN_DAILY_FLOW_MAX_CODES_PER_ROUND", "LOW_OPEN_DAILY_FLOW_SESSIONS",
    "fetch_daily_main_flow_history",
)

CAPITAL_EXPORTS = (
    "HARD_BLACKLIST", "_low_open_wash_current_conditions", "_qualifies_low_open_wash",
    "_should_exclude_from_low_absorb", "build_negative_super_observations", "capital_data_label",
    "classify_flow", "count_negative_super_observations", "evaluate_dominance_type",
    "flow_veto_reason", "is_negative_super", "low_open_wash_rows", "rank_capital_candidates",
)

FLOW_EXPORTS = (
    "EM_FFLOW_MINUTE_URLS", "FLOW_MINUTE_MAX_CODES_PER_ROUND", "FLOW_MINUTE_MIN_INTERVAL_SECONDS",
    "FLOW_MINUTE_SAFETY_RESERVE", "FLOW_MINUTE_TTL_SECONDS", "FLOW_SNAPSHOT_PATH",
    "_FLOW_MINUTE_CACHE", "_FLOW_MINUTE_LOCK", "_FLOW_MINUTE_ROUND_CODES", "_FLOW_MINUTE_ROUND_LOCK",
    "_LOW_OPEN_DAILY_FLOW_CACHE", "_LOW_OPEN_DAILY_FLOW_CACHE_LOCK", "_column_values",
    "_contiguous_tail_start", "_flow_minute_budget_ok", "_flow_minute_series_for",
    "_is_intraday_now", "_minutes_of_hhmm", "_note_low_absorb_unverified",
    "_pace_flow_minute_request", "apply_flow_increments", "compute_buy_ratio", "fetch_flow_minutes",
    "fill_flow_increments_from_fflow", "flow_continuity_from_minutes",
    "flow_increments_from_minutes", "flow_negative_samples_from_minutes", "flow_window_deltas",
    "load_flow_history", "low_absorb_unverified_count", "reset_flow_minute_round",
    "round_series_date", "save_flow_history",
)

ANNOUNCEMENT_EXPORTS = (
    "ANNOUNCEMENT_CACHE_FILE", "ANNOUNCEMENT_CACHE_TTL_SECONDS", "ANNOUNCEMENT_CONFIG",
    "ANNOUNCEMENT_IGNORE_KEYWORDS", "ANNOUNCEMENT_REQUEST_TIMEOUT_SECONDS",
    "ANNOUNCEMENT_SECTIONS", "ANNOUNCEMENT_URL", "HARD_ANNOUNCEMENT_KEYWORDS",
    "WATCH_ANNOUNCEMENT_KEYWORDS", "_announcement_business_failure", "_announcement_container",
    "_announcement_row_title", "_announcement_total", "_announcement_total_from_response",
    "_has_announcement_container", "_load_announcement_risk_cache",
    "_save_announcement_risk_cache", "_validate_announcement_page_count",
    "_validate_primary_announcement_response", "apply_announcement_pool_gates",
    "attach_announcement_risks", "classify_announcement_risk", "collect_announcement_titles",
    "fetch_announcements",
)

MARKET_EXPORTS = (
    "CLIST_FIELDS", "CLIST_STARTUP_URLS", "CLIST_URLS", "EASTMONEY_UT", "INDEX_SECIDS",
    "INDEX_URLS", "MARKET_FETCH_STATUS", "SECTOR_BOARDS_STATUS", "SECTOR_BOARD_FIELDS",
    "SECTOR_BOARD_MAX_PAGES", "SECTOR_BOARD_PAGE_SIZE", "SECTOR_BOARD_SORT_FIELD",
    "SINA_MARKET_URL", "SINA_MAX_PAGES", "SINA_PAGE_SIZE", "_fetch_sector_page",
    "_normalize_sina_row", "_sector_board_params", "_sina_number", "fetch_indices",
    "fetch_market", "fetch_sector_indices", "fetch_sina_market", "get_market_fetch_status",
)

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
    "screening.sources.announcements": ("_ANNOUNCEMENT_RISK_CACHE",),
    "screening.sources.flow": (
        "_FLOW_MINUTE_ROUND_EXPECTED_DATE", "_FLOW_MINUTE_LAST_REQUEST_AT",
        "_LOW_ABSORB_UNVERIFIED_COUNT",
    ),
}


RENDER_EXPORTS = (
    "RANGE_POSITION_TOP_PCT", "RANGE_POSITION_WARN_PCT", "flow_amount_str", "flow_status_cell",
    "flow_veto_suffix", "markdown_table", "range_position_cell", "render_markdown",
    "shadow_badge_text",
)


WATCHLIST_EXPORTS = (
    "WATCHLIST_BREAKOUT_STATE_PATH", "_breakout_snapshot_epoch", "build_watchlist",
    "evaluate_watchlist_breakout_states", "load_watchlist_breakout_state",
    "save_watchlist_breakout_state",
)


class FacadeIdentityTest(unittest.TestCase):
    def test_watchlist_names_are_reexported_not_copied(self):
        for name in WATCHLIST_EXPORTS:
            with self.subTest(name=name):
                self.assertIs(getattr(screen, name), getattr(watchlist, name))

    def test_watchlist_config_and_clock_have_a_single_source(self):
        self.assertIs(watchlist.SCREENING_CONFIG, screen.SCREENING_CONFIG)
        self.assertIs(watchlist.TZ, screen.TZ)

    def test_capital_names_are_reexported_not_copied(self):
        for name in CAPITAL_EXPORTS:
            with self.subTest(name=name):
                self.assertIs(getattr(screen, name), getattr(capital, name))

    def test_capital_config_has_a_single_source(self):
        self.assertIs(capital.RISK_CONFIG, screen.RISK_CONFIG)
        self.assertIs(capital.SCREENING_CONFIG, screen.SCREENING_CONFIG)

    def test_daily_flow_names_are_reexported_not_copied(self):
        for name in DAILY_FLOW_EXPORTS:
            with self.subTest(name=name):
                self.assertIs(getattr(screen, name), getattr(flow, name))

    def test_flow_names_are_reexported_not_copied(self):
        for name in FLOW_EXPORTS:
            with self.subTest(name=name):
                self.assertIs(getattr(screen, name), getattr(flow, name))

    def test_flow_config_and_clock_have_a_single_source(self):
        self.assertIs(flow.SCREENING_CONFIG, screen.SCREENING_CONFIG)
        self.assertIs(flow.TZ, screen.TZ)

    def test_announcement_names_are_reexported_not_copied(self):
        for name in ANNOUNCEMENT_EXPORTS:
            with self.subTest(name=name):
                self.assertIs(getattr(screen, name), getattr(announcements, name))

    def test_announcement_risk_config_has_a_single_source(self):
        self.assertIs(announcements.RISK_CONFIG, screen.RISK_CONFIG)

    def test_market_names_are_reexported_not_copied(self):
        for name in MARKET_EXPORTS:
            with self.subTest(name=name):
                self.assertIs(getattr(screen, name), getattr(market, name))

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
