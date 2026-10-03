"""Bounded in-memory and atomic JSON cache helpers.

New cache files follow ``A_SHARE_STATE_DIR`` when set.  With no override they
live under the historical scripts directory and remain ignored by the project.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import threading
import time
from typing import Any


def cache_root() -> Path:
    override = os.environ.get("A_SHARE_STATE_DIR", "").strip()
    if override:
        return Path(override).expanduser()
    return Path(__file__).resolve().parents[2] / "daily-stock-analysis" / "scripts"


def cache_path(namespace: str) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in namespace).strip("_") or "data"
    return cache_root() / f".a_share_data_{safe}.json"


def atomic_write_json(path: Path, payload: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, allow_nan=False), encoding="utf-8")
    os.replace(tmp, path)


@dataclass
class CacheEntry:
    value: Any
    stored_at: float
    expires_at: float | None = None
    data_date: str | None = None


class JsonCache:
    """A tiny process-safe JSON cache with atomic writes and stale visibility."""

    def __init__(self, namespace: str, *, path: Path | None = None):
        self.path = path or cache_path(namespace)
        self._lock = threading.RLock()
        self._loaded = False
        self._payload: dict[str, Any] = {}

    def _load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                self._payload = payload
        except (OSError, ValueError, TypeError):
            self._payload = {}

    @staticmethod
    def key(value: Any) -> str:
        raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def get(self, key: str, *, ttl: float | None = None, now: float | None = None, allow_stale: bool = False) -> CacheEntry | None:
        with self._lock:
            self._load()
            entry = self._payload.get(key)
            if not isinstance(entry, dict) or "value" not in entry:
                return None
            stored = float(entry.get("stored_at") or 0)
            expires = entry.get("expires_at")
            if ttl is not None:
                expires = stored + float(ttl)
            current = time.time() if now is None else float(now)
            fresh = expires is None or current <= float(expires)
            if not fresh and not allow_stale:
                return None
            return CacheEntry(entry.get("value"), stored, float(expires) if expires is not None else None, entry.get("data_date"))

    def set(self, key: str, value: Any, *, ttl: float | None = None, data_date: str | None = None) -> None:
        with self._lock:
            self._load()
            stored = time.time()
            self._payload[key] = {
                "value": value,
                "stored_at": stored,
                "expires_at": stored + float(ttl) if ttl is not None else None,
                "data_date": data_date,
            }
            atomic_write_json(self.path, self._payload)

    def delete(self, key: str) -> None:
        with self._lock:
            self._load()
            if key in self._payload:
                self._payload.pop(key, None)
                atomic_write_json(self.path, self._payload)

    def stats(self) -> dict[str, Any]:
        with self._lock:
            self._load()
            return {"path": str(self.path), "entries": len(self._payload)}

