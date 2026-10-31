"""公告风险状态与风险政策（唯一语义，依据 选股框架.md「一、信号与建仓门禁」）。

clean：正常；watch_risk：仅减分，不阻断正式资格；avoid/unknown：一票否决。
状态机、准交集、观察池突破、资金排名与输出标签都读这里，不各自另立口径。
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
from tools.rule_config import RULE_CONFIG  # noqa: E402

RISK_CONFIG = RULE_CONFIG["risk"]
RISK_STATUSES = RISK_CONFIG["statuses"]
RISK_CLEAN = RISK_STATUSES["clean"]
RISK_WATCH = RISK_STATUSES["watch_risk"]
RISK_AVOID = RISK_STATUSES["avoid"]
RISK_UNKNOWN = RISK_STATUSES["unknown"]
RISK_STATUS_VALUES = frozenset(RISK_STATUSES.values())
RISK_CHECKED_VALUES = frozenset({RISK_CLEAN, RISK_WATCH, RISK_AVOID})


def _row_risk_status(row: Any) -> str:
    """Normalize announcement risk without treating missing as clean. Supports Dict and Enriched objects."""
    if row is None:
        return RISK_UNKNOWN
    if isinstance(row, dict):
        status = row.get("risk_status")
        if status in RISK_STATUS_VALUES:
            return status
        legacy = row.get("announcement_risk")
        if legacy in RISK_CHECKED_VALUES:
            return legacy
        return RISK_UNKNOWN
    status = getattr(row, "risk_status", None)
    if status in RISK_STATUS_VALUES:
        return status
    legacy = getattr(row, "announcement_risk", None)
    if legacy in RISK_CHECKED_VALUES:
        return legacy
    return RISK_UNKNOWN


def announcement_label(row: Dict[str, Any]) -> str:
    risk = _row_risk_status(row)
    keywords = row.get("announcement_keywords") or []
    if keywords:
        return f"{risk}({','.join(keywords[:3])})"
    if risk == RISK_UNKNOWN:
        return f"{RISK_UNKNOWN}(公告检查不可用)"
    return risk


# ── 公告风险政策（唯一语义，依据 选股框架.md「一、信号与建仓门禁」）──────────
# clean：正常；watch_risk：仅减分，不阻断正式资格；avoid/unknown：一票否决。
# 所有模块（双池交集状态机、准交集、观察池突破、资金排名、输出标签）都必须读这里，
# 不得各自把“非 clean”当成硬否决——那是把软风险和框架一票否决混为一谈。
RISK_HARD_VETO = frozenset({RISK_AVOID, RISK_UNKNOWN})


def risk_blocks_formal_qualification(risk: Any) -> bool:
    """avoid/unknown 一票否决；clean/watch_risk 不因“非 clean”被否决。"""
    return str(risk or RISK_UNKNOWN) in RISK_HARD_VETO


def risk_allows_state_qualification(risk: Any) -> bool:
    """状态资格（可否推进到 ENTRY_ELIGIBLE 等）只看是否触及一票否决。"""
    return not risk_blocks_formal_qualification(risk)


def risk_note_text(risk: Any) -> str:
    """人类可读风险标注：软风险与一票否决必须分开表述。"""
    status = str(risk or RISK_UNKNOWN)
    if status == RISK_CLEAN:
        return ""
    if status == RISK_WATCH:
        return f"公告{RISK_WATCH}（仅减分，非一票否决）"
    return f"公告风险否决({status})"


def append_risk_text(existing: str, extra: str) -> str:
    if not extra:
        return existing
    if not existing or existing == "无":
        return extra
    if extra in existing:
        return existing
    return f"{existing}/{extra}"
