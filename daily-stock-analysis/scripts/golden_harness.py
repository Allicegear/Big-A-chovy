#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""金标准（golden master）录制 / 回放：重构期间证明筛选输出逐字节不变。

只用于本地重构验证，不参与生产。数据（录制的行情响应、种子状态、基线输出）
默认写在 ``<项目根>/.golden_local/``，该目录只在本机，不提交 Git（含真实行情
与本机运行状态）。可用 ``GOLDEN_DATA_DIR`` 改位置。

工作方式
--------
1. ``record``：真实联网跑一组场景，把两条 HTTP 边界的全部请求/响应录成 cassette：
   - ``a_share_daily_screen.fetch_json``（东财/新浪/腾讯行情、K 线、资金、公告）
   - ``tools.data_sources.http.HTTPClient.request``（巨潮公告兜底、市场背景）
2. ``replay``：断网（socket 级拦截）后按 cassette 回放同一组场景，输出每轮结果
   （JSON / Markdown）与场景结束时的运行状态文件。
3. ``compare``：逐字节比较两次 replay 的输出目录。

确定性：录制与回放都冻结墙钟（``time.time`` 与各模块的 ``datetime.now``）到同一
时刻 T0，``time.monotonic`` 保持真实（等待/超时逻辑只用它），``time.sleep`` 截短
以加速。每个场景在独立子进程运行，因为 ``A_SHARE_STATE_DIR`` 在导入时读取，
模块级缓存也不能跨场景泄漏。

隔离：每个场景都把 ``A_SHARE_STATE_DIR`` / ``A_SHARE_REPORT_DIR`` /
``A_SHARE_SHADOW_DATA_DIR`` 指向临时目录；本工具从不读写真实运行状态
（种子状态只从 ``.golden_local/seed_state`` 复制）。

用法（项目根目录执行）::

    python daily-stock-analysis/scripts/golden_harness.py seed --from daily-stock-analysis/scripts
    python daily-stock-analysis/scripts/golden_harness.py record --t0 2026-10-30T14:30
    python daily-stock-analysis/scripts/golden_harness.py replay --out baseline
    python daily-stock-analysis/scripts/golden_harness.py replay --out candidate
    python daily-stock-analysis/scripts/golden_harness.py compare baseline candidate
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import datetime as _dt
import io
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from unittest import mock
from zoneinfo import ZoneInfo

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
TZ = ZoneInfo("Asia/Shanghai")

DATA_DIR = Path(os.environ.get("GOLDEN_DATA_DIR") or (PROJECT_ROOT / ".golden_local"))
CASSETTE_PATH = DATA_DIR / "cassette.json"
SEED_DIR = DATA_DIR / "seed_state"
RUNS_DIR = DATA_DIR / "runs"

# 跟随 A_SHARE_STATE_DIR 的运行状态文件（见 runtime_paths.py）；种子与场景结束快照都只取这些。
STATE_FILES = (
    "flow_snapshot.json",
    "intersection_state.json",
    "watchlist_breakout_state.json",
    ".kline_cache.json",
    ".announcement_risk_cache.json",
    "last_valid_result.json",
)

_COMMON = ["--mode", "all", "--top", "15"]

# init: empty=空状态（冷启动） / warm=从 seed_state 复制（盘中续跑）
SCENARIOS: Dict[str, Dict[str, Any]] = {
    "cli_cold_all_json": {"init": "empty", "rounds": [("cli", _COMMON + ["--format", "json"])]},
    "cli_cold_all_md": {"init": "empty", "rounds": [("cli", _COMMON + ["--format", "md"])]},
    "cli_cold_boards3_md": {
        "init": "empty",
        "rounds": [("cli", _COMMON + ["--format", "md", "--boards", "main", "chinext", "star"])],
    },
    "cli_cold_skip_json": {
        "init": "empty",
        "rounds": [("cli", _COMMON + ["--format", "json", "--skip-announcements", "--skip-capital-ranking"])],
    },
    "cli_warm_2rounds_json": {
        "init": "warm",
        "rounds": [("cli", _COMMON + ["--format", "json"]), ("cli", _COMMON + ["--format", "md"])],
    },
    "engine_warm_2rounds": {
        "init": "warm",
        "rounds": [("engine", {"modes": ["all"], "top": 15}), ("engine", {"modes": ["all"], "top": 15})],
    },
}


# ══════════════════════════════════════════════════════════════════════════
# 冻结时钟
# ══════════════════════════════════════════════════════════════════════════

def _make_frozen_datetime(t0: float):
    real = _dt.datetime

    class FrozenDatetime(real):  # type: ignore[misc, valid-type]
        @classmethod
        def now(cls, tz=None):
            return real.fromtimestamp(t0, tz)

        @classmethod
        def today(cls):
            return real.fromtimestamp(t0)

        @classmethod
        def utcnow(cls):
            return real.fromtimestamp(t0, _dt.timezone.utc).replace(tzinfo=None)

    return FrozenDatetime


@contextlib.contextmanager
def frozen_clock(t0: float):
    """冻结墙钟：time.time 与所有已加载模块里的 ``datetime`` 类名引用。

    只替换 ``模块.datetime is datetime.datetime`` 的属性（``from datetime import
    datetime`` 的写法）；``import datetime`` 的模块通过替换 ``datetime.datetime``
    本身不可行（C 类型），因此额外替换 ``datetime`` 模块属性下的类。拆分后新增的
    模块只要已导入就会被覆盖，无需改本工具。
    """
    frozen = _make_frozen_datetime(t0)
    real_sleep = time.sleep
    patches = [
        mock.patch("time.time", lambda: t0),
        mock.patch("time.sleep", lambda s: real_sleep(min(max(float(s), 0.0), 0.002))),
    ]
    swapped: List[Tuple[Any, str]] = []
    for mod in list(sys.modules.values()):
        if mod is None or mod is _dt:
            continue
        try:
            if getattr(mod, "datetime", None) is _dt.datetime:
                setattr(mod, "datetime", frozen)
                swapped.append((mod, "datetime"))
        except Exception:
            continue
    for p in patches:
        p.start()
    try:
        yield
    finally:
        for p in reversed(patches):
            p.stop()
        for mod, attr in swapped:
            setattr(mod, attr, _dt.datetime)


# ══════════════════════════════════════════════════════════════════════════
# cassette
# ══════════════════════════════════════════════════════════════════════════

def _canon(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


class Cassette:
    """key → 按调用顺序排列的响应列表；回放时同 key 依次取，用尽后重复最后一条。"""

    def __init__(self, data: Optional[Dict[str, Any]] = None) -> None:
        data = data or {}
        self.meta: Dict[str, Any] = dict(data.get("meta") or {})
        self.entries: Dict[str, List[Any]] = {k: list(v) for k, v in (data.get("entries") or {}).items()}
        self._cursor: Dict[str, int] = {}
        self._lock = threading.Lock()
        self.misses: List[str] = []

    def add(self, key: str, entry: Any) -> None:
        with self._lock:
            self.entries.setdefault(key, []).append(entry)

    def take(self, key: str) -> Any:
        with self._lock:
            seq = self.entries.get(key)
            if not seq:
                self.misses.append(key)
                return None
            i = self._cursor.get(key, 0)
            self._cursor[key] = i + 1
            return seq[min(i, len(seq) - 1)]

    def to_json(self) -> Dict[str, Any]:
        return {"meta": self.meta, "entries": self.entries}


def _fetch_json_key(url: str, params: Any) -> str:
    return "fetch_json " + url + " " + _canon(params)


def _http_key(method: str, url: str, params: Any, data: Any) -> str:
    if isinstance(data, bytes):
        data = data.decode("utf-8", errors="replace")
    return f"http {method.upper()} {url} {_canon(params or {})} {_canon(data)}"


def _install_seams(cassette: Cassette, mode: str) -> List[Any]:
    """在两条 HTTP 边界上录制或回放。返回已启动的 patch 列表。"""
    import a_share_daily_screen as screen
    from tools.data_sources import http as ds_http

    real_fetch_json = screen.fetch_json
    real_request = ds_http.HTTPClient.request

    def fetch_json(url, params, timeout=6, retries=0, *, deadline=None):
        key = _fetch_json_key(url, params)
        if mode == "record":
            try:
                result = real_fetch_json(url, params, timeout, retries, deadline=deadline)
            except screen.NetworkUnavailable as exc:
                cassette.add(key, {"error": "NetworkUnavailable", "url": exc.url, "errors": exc.errors})
                raise
            cassette.add(key, {"ok": result})
            return result
        # 真实 fetch_json 成功/失败时会更新主机健康度（_rank_urls 据此排列备用主机），
        # 回放必须保留这一副作用，否则备用主机尝试顺序与录制时不同。
        entry = cassette.take(key)
        if entry is None:
            screen._mark_host_failed(url)
            raise screen.NetworkUnavailable(url, {"golden": "cassette miss"})
        if "error" in entry:
            screen._mark_host_failed(url)
            raise screen.NetworkUnavailable(entry.get("url", url), dict(entry.get("errors") or {}))
        screen._mark_host_ok(url)
        return json.loads(json.dumps(entry["ok"]))  # 深拷贝，防止调用方改写缓存的响应

    def request(self, method, url, *, params=None, data=None, headers=None, timeout=10.0, retries=1):
        key = _http_key(method, url, params, data)
        if mode == "record":
            try:
                resp = real_request(self, method, url, params=params, data=data, headers=headers,
                                    timeout=timeout, retries=retries)
            except ds_http.HTTPClientError as exc:
                cassette.add(key, {"error": str(exc), "code": exc.code, "status": exc.status,
                                   "retryable": exc.retryable})
                raise
            cassette.add(key, {"status": resp.status, "url": resp.url,
                               "body": base64.b64encode(resp.body).decode("ascii"),
                               "headers": dict(resp.headers)})
            return resp
        entry = cassette.take(key)
        if entry is None:
            raise ds_http.HTTPClientError("golden: cassette miss", code="network_error", retryable=False)
        if "error" in entry:
            raise ds_http.HTTPClientError(entry["error"], code=entry.get("code") or "network_error",
                                          status=entry.get("status"), retryable=False)
        self.request_count += 1
        return ds_http.HTTPResponse(entry["status"], entry["url"], base64.b64decode(entry["body"]),
                                    dict(entry.get("headers") or {}), 0.0)

    # fetch_json 随核心拆分被多个子模块各自 import（screening.net、screening.sources.*）。
    # 逐个替换所有指向真实函数的模块属性，拆分新模块后无需改本工具；断网兜底保证漏网即报错。
    patches = [
        mock.patch.object(mod, "fetch_json", fetch_json)
        for mod in list(sys.modules.values())
        if mod is not None and getattr(mod, "fetch_json", None) is real_fetch_json
    ]
    if not patches:
        raise RuntimeError("golden: 没找到任何引用真实 fetch_json 的模块")
    patches.append(mock.patch.object(ds_http.HTTPClient, "request", request))
    if mode == "replay":
        import network_path

        direct = [{"label": "直连", "proxy": None, "latency_ms": 10.0, "score": 10.0,
                   "degraded": [], "endpoints": {}}]
        patches.append(mock.patch.object(network_path, "probe_paths", lambda *a, **k: list(direct)))

        def _no_network(*args, **kwargs):
            raise OSError("golden replay: live network blocked")

        patches.append(mock.patch.object(socket.socket, "connect", _no_network))
        patches.append(mock.patch.object(socket, "create_connection", _no_network))
    for p in patches:
        p.start()
    return patches


# ══════════════════════════════════════════════════════════════════════════
# 单场景（子进程内执行）
# ══════════════════════════════════════════════════════════════════════════

def _run_cli_round(argv: List[str]) -> Tuple[str, str]:
    import a_share_daily_screen as screen

    fmt = argv[argv.index("--format") + 1]
    buf, err = io.StringIO(), io.StringIO()
    with mock.patch.object(sys, "argv", ["a_share_daily_screen.py"] + argv), \
            contextlib.redirect_stdout(buf), contextlib.redirect_stderr(err):
        code = screen.main()
    if code != 0:
        raise SystemExit(f"main() exit {code}: {err.getvalue()[-1500:]}")
    return buf.getvalue(), fmt


def _run_engine_round(kwargs: Dict[str, Any]) -> Tuple[str, str]:
    import realtime_engine as engine

    kw = dict(kwargs)
    kw["modes"] = set(kw.get("modes") or ["strict"])
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        result = engine.run_screening(**kw)
    return json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), "json"


def run_scenario_in_process(name: str, mode: str, cassette_path: Path, out_dir: Path, t0: float) -> None:
    sys.path.insert(0, str(SCRIPT_DIR))
    sys.path.insert(0, str(PROJECT_ROOT))
    if mode == "replay":
        # 每个场景只回放自己录到的响应：合并后同一请求会有多个场景的响应，
        # 并发线程取到第几条取决于完成顺序，回放就不再确定。
        stored = json.loads(cassette_path.read_text(encoding="utf-8"))
        cassette = Cassette({"entries": stored["scenarios"][name]})
    else:
        cassette = Cassette()
    spec = SCENARIOS[name]

    # 先导入全部被测模块，frozen_clock 才能覆盖它们模块级的 ``datetime`` 引用
    import a_share_daily_screen  # noqa: F401
    import realtime_engine  # noqa: F401
    with frozen_clock(t0):
        patches = _install_seams(cassette, mode)
        try:
            for i, (kind, arg) in enumerate(spec["rounds"], start=1):
                text, ext = _run_cli_round(arg) if kind == "cli" else _run_engine_round(arg)
                (out_dir / f"round{i}.{ext}").write_text(text, encoding="utf-8")
        finally:
            for p in reversed(patches):
                p.stop()

    state_dir = Path(os.environ["A_SHARE_STATE_DIR"])
    snap = out_dir / "state_after"
    snap.mkdir(parents=True, exist_ok=True)
    for fn in STATE_FILES:
        src = state_dir / fn
        if src.exists():
            shutil.copy2(src, snap / fn)

    if mode == "record":
        # 子进程各自录制一段，由父进程合并
        (out_dir / "cassette.part.json").write_text(json.dumps(cassette.to_json(), ensure_ascii=False),
                                                    encoding="utf-8")
    else:
        (out_dir / "misses.json").write_text(json.dumps(cassette.misses, ensure_ascii=False, indent=2),
                                             encoding="utf-8")


# ══════════════════════════════════════════════════════════════════════════
# 父进程：编排
# ══════════════════════════════════════════════════════════════════════════

def _spawn(name: str, mode: str, out_dir: Path, t0: float) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix=f"golden_{name}_"))
    state = tmp / "state"
    state.mkdir()
    if SCENARIOS[name]["init"] == "warm":
        if not SEED_DIR.exists():
            raise SystemExit(f"缺少种子状态 {SEED_DIR}，先运行 seed")
        for fn in STATE_FILES:
            if (SEED_DIR / fn).exists():
                shutil.copy2(SEED_DIR / fn, state / fn)
    env = dict(os.environ)
    env.update({
        "A_SHARE_STATE_DIR": str(state),
        "A_SHARE_REPORT_DIR": str(tmp / "reports"),
        "A_SHARE_SHADOW_DATA_DIR": str(tmp / "shadow"),
        "PYTHONIOENCODING": "utf-8",
        "PYTHONHASHSEED": "0",
    })
    cmd = [sys.executable, str(Path(__file__).resolve()), "_scenario", name, mode, str(out_dir), str(t0)]
    proc = subprocess.run(cmd, env=env, cwd=str(PROJECT_ROOT), capture_output=True, text=True,
                          encoding="utf-8", errors="replace")
    (out_dir / "stderr.log").write_text(proc.stdout + "\n" + proc.stderr, encoding="utf-8")
    shutil.rmtree(tmp, ignore_errors=True)
    return proc.returncode


def _parse_t0(text: str) -> float:
    return _dt.datetime.fromisoformat(text).replace(tzinfo=TZ).timestamp()


def cmd_seed(src: Path) -> None:
    SEED_DIR.mkdir(parents=True, exist_ok=True)
    copied = []
    for fn in STATE_FILES:
        if (src / fn).exists():
            shutil.copy2(src / fn, SEED_DIR / fn)
            copied.append(fn)
    print(f"种子状态已复制到 {SEED_DIR}：{', '.join(copied) or '（无）'}")


def _assemble_cassette(rec_root: Path, names: List[str], meta: Dict[str, Any]) -> None:
    """把各场景子进程录到的片段按场景存成一个 cassette 文件。"""
    scenarios: Dict[str, Any] = {}
    for name in names:
        part = rec_root / name / "cassette.part.json"
        if part.exists():
            scenarios[name] = json.loads(part.read_text(encoding="utf-8"))["entries"]
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CASSETTE_PATH.write_text(json.dumps({"meta": meta, "scenarios": scenarios}, ensure_ascii=False),
                             encoding="utf-8")
    total = sum(len(v) for v in scenarios.values())
    print(f"[record] cassette: {len(scenarios)} 个场景 / {total} 个请求键 → {CASSETTE_PATH}")


def cmd_record(t0_text: str, names: List[str]) -> None:
    t0 = _parse_t0(t0_text)
    rec_root = RUNS_DIR / "_record"
    if rec_root.exists():
        shutil.rmtree(rec_root)
    failed = []
    for name in names:
        started = time.monotonic()
        rc = _spawn(name, "record", rec_root / name, t0)
        print(f"[record] {name}: rc={rc} {time.monotonic() - started:.0f}s", flush=True)
        if rc != 0:
            failed.append(name)
    meta = {"t0": t0, "t0_text": t0_text, "recorded_wall": _dt.datetime.now(TZ).isoformat()}
    _assemble_cassette(rec_root, names, meta)
    if failed:
        raise SystemExit(f"录制失败的场景：{failed}（见 {rec_root}/<场景>/stderr.log）")


def cmd_replay(label: str, names: List[str]) -> None:
    if not CASSETTE_PATH.exists():
        raise SystemExit(f"缺少 cassette：{CASSETTE_PATH}，先运行 record")
    if "scenarios" not in json.loads(CASSETTE_PATH.read_text(encoding="utf-8")):
        raise SystemExit("cassette 是旧的合并格式，请重新 record")
    t0 = float(json.loads(CASSETTE_PATH.read_text(encoding="utf-8"))["meta"]["t0"])
    out_root = RUNS_DIR / label
    if out_root.exists():
        shutil.rmtree(out_root)
    failed = []
    for name in names:
        started = time.monotonic()
        rc = _spawn(name, "replay", out_root / name, t0)
        misses = out_root / name / "misses.json"
        n_miss = len(json.loads(misses.read_text(encoding="utf-8"))) if misses.exists() else -1
        print(f"[replay] {name}: rc={rc} misses={n_miss} {time.monotonic() - started:.1f}s", flush=True)
        # 未录制请求按"网络不可用"回放，不算失败：录制时 K 线多线程并发，腾讯源
        # 熔断时机取决于线程完成顺序，个别代码回放时会走与录制不同的源。
        # misses.json 本身参与 compare，请求集合相对基线的任何变化都会暴露。
        if rc != 0:
            failed.append(name)
    if failed:
        raise SystemExit(f"回放失败的场景：{failed}（见 {out_root}/<场景>/stderr.log）")


def _same_content(pa: Path, pb: Path) -> bool:
    """Markdown 等文本逐字节比较；JSON 解析后比较。

    JSON 对象的键顺序不参与比较：公告核验并发执行，缓存/结果字典按线程完成
    先后插入，生产中本就不固定（两次回放实测仅此差异）。列表顺序仍须一致。
    """
    raw_a, raw_b = pa.read_bytes(), pb.read_bytes()
    if raw_a == raw_b:
        return True
    if pa.suffix != ".json":
        return False
    try:
        return json.loads(raw_a) == json.loads(raw_b)
    except ValueError:
        return False


def _json_paths_diff(a: Any, b: Any, path: str, out: List[str]) -> None:
    if type(a) is not type(b):
        out.append(f"{path}: 类型 {type(a).__name__} | {type(b).__name__}")
    elif isinstance(a, dict):
        for k in sorted(set(a) | set(b), key=str):
            if a.get(k) != b.get(k):
                _json_paths_diff(a.get(k), b.get(k), f"{path}.{k}", out)
    elif isinstance(a, list):
        if len(a) != len(b):
            out.append(f"{path}: 长度 {len(a)} | {len(b)}")
        for i, (x, y) in enumerate(zip(a, b)):
            if x != y:
                _json_paths_diff(x, y, f"{path}[{i}]", out)
    elif a != b:
        out.append(f"{path}: {str(a)[:100]} | {str(b)[:100]}")


def cmd_diff(a: str, b: str, rel: str, limit: int) -> None:
    """逐路径列出一个 JSON 输出文件在两次运行间的差异（排查用）。"""
    out: List[str] = []
    _json_paths_diff(json.loads((RUNS_DIR / a / rel).read_text(encoding="utf-8")),
                     json.loads((RUNS_DIR / b / rel).read_text(encoding="utf-8")), "", out)
    print(f"{rel}：{len(out)} 处差异")
    for line in out[:limit]:
        print("  " + line)


def cmd_compare(a: str, b: str) -> int:
    root_a, root_b = RUNS_DIR / a, RUNS_DIR / b
    diffs = []
    files_a = {p.relative_to(root_a) for p in root_a.rglob("*") if p.is_file() and p.name != "stderr.log"}
    files_b = {p.relative_to(root_b) for p in root_b.rglob("*") if p.is_file() and p.name != "stderr.log"}
    for rel in sorted(files_a | files_b):
        if rel not in files_a or rel not in files_b:
            diffs.append(f"仅存在于一侧：{rel}")
        elif not _same_content(root_a / rel, root_b / rel):
            diffs.append(f"内容不同：{rel}")
    print(f"比较 {a} ↔ {b}：{len(files_a | files_b)} 个文件，{len(diffs)} 处差异")
    for d in diffs:
        print("  " + d)
    return 1 if diffs else 0


def main() -> int:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")  # Windows GBK 控制台
        except (AttributeError, ValueError):
            pass
    if len(sys.argv) >= 2 and sys.argv[1] == "_scenario":
        _, _, name, mode, out_dir, t0 = sys.argv
        run_scenario_in_process(name, mode, CASSETTE_PATH, Path(out_dir), float(t0))
        return 0
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("seed", help="从指定目录复制运行状态作为 warm 场景种子（只读源目录）")
    s.add_argument("--from", dest="src", type=Path, required=True)
    r = sub.add_parser("record", help="联网录制全部场景")
    r.add_argument("--t0", required=True, help="冻结时刻（上海时间），如 2026-10-30T14:30")
    r.add_argument("--only", nargs="*", default=None)
    p = sub.add_parser("replay", help="断网回放全部场景")
    p.add_argument("--out", required=True)
    p.add_argument("--only", nargs="*", default=None)
    c = sub.add_parser("compare", help="逐字节比较两次回放")
    c.add_argument("a")
    c.add_argument("b")
    d = sub.add_parser("diff", help="逐路径列出某个 JSON 输出的差异")
    d.add_argument("a")
    d.add_argument("b")
    d.add_argument("file", help="相对场景目录的路径，如 cli_cold_all_json/round1.json")
    d.add_argument("--limit", type=int, default=30)
    args = ap.parse_args()
    names = list(getattr(args, "only", None) or SCENARIOS)
    if args.cmd == "seed":
        cmd_seed(args.src)
    elif args.cmd == "record":
        cmd_record(args.t0, names)
    elif args.cmd == "replay":
        cmd_replay(args.out, names)
    elif args.cmd == "diff":
        cmd_diff(args.a, args.b, args.file, args.limit)
    else:
        return cmd_compare(args.a, args.b)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
