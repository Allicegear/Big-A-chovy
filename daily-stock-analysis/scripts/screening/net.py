"""网络核心：requests 会话、主机熔断、东财冷却、代理与网络模式、fetch_json。

行情、K 线、资金、公告等数据源都经 fetch_json 取数；路径择优委托 network_path，
TLS 校验来自 tls_context，冷却标记文件路径来自 runtime_paths（随 A_SHARE_STATE_DIR 隔离）。
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Tuple

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
import runtime_paths  # noqa: E402  运行状态路径唯一来源
import tls_context  # noqa: E402  TLS 校验上下文唯一来源

try:
    import requests
    requests.packages.urllib3.disable_warnings()
    REQUESTS_SESSION = requests.Session()
    REQUESTS_SESSION.trust_env = True
    REQUESTS_DIRECT_SESSION = requests.Session()
    REQUESTS_DIRECT_SESSION.trust_env = False
except Exception:  # keep stdlib fallback for minimal Python envs
    requests = None
    REQUESTS_SESSION = None
    REQUESTS_DIRECT_SESSION = None

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X) daily-stock-analysis/1.0"
# 默认做完整证书校验（实测所有行情主机证书链有效）。需要自签 CA 时配置
# A_SHARE_CA_BUNDLE 而不是关掉校验；证书失败按数据不可用处理。
SSL_CONTEXT = tls_context.build_context()
import network_path  # 多路径实测延迟择优（直连+候选代理端口），软件无关

# ── Per-host circuit breaker ──────────────────────────────────────────
# Maps hostname -> timestamp of last failure.  Hosts that failed within
# _HOST_COOLDOWN seconds are moved to the end of the URL list so the
# fetcher tries healthy hosts first.
_HOST_FAILURES: Dict[str, float] = {}
_HOST_COOLDOWN = 120  # seconds to keep a host deprioritised


def _rank_urls(urls: List[str]) -> List[str]:
    """Return *urls* sorted so that recently-failed hosts come last."""
    now = time.time()
    def _key(u: str) -> int:
        host = u.split("/")[2]
        fail_ts = _HOST_FAILURES.get(host, 0)
        return 0 if (now - fail_ts) > _HOST_COOLDOWN else 1
    return sorted(urls, key=_key)


def _mark_host_failed(url: str) -> None:
    host = url.split("/")[2]
    _HOST_FAILURES[host] = time.time()


def _mark_host_ok(url: str) -> None:
    host = url.split("/")[2]
    _HOST_FAILURES.pop(host, None)


# ── Persistent East Money cooldown ────────────────────────────────────
# When ALL hosts fail in a run, record the timestamp.  Subsequent runs
# within _EM_COOLDOWN seconds skip East Money entirely and go straight
# to the Sina fallback, saving ~30 s of wasted retry time.
_EM_COOLDOWN_FILE = runtime_paths.state_file(".em_cooldown")
# 指数退避：第1次封 120s → 第2次 300s → 第3次起 600s
_EM_COOLDOWN_BACKOFF = [120, 300, 600]

def _em_read_cooldown() -> Tuple[float, int]:
    """Return (timestamp, consecutive_failures) from cooldown file."""
    try:
        if _EM_COOLDOWN_FILE.exists():
            data = _EM_COOLDOWN_FILE.read_text().strip().split("\n")
            ts = float(data[0])
            count = int(data[1]) if len(data) > 1 else 1
            return ts, count
    except (ValueError, OSError):
        pass
    return 0.0, 0


def _em_in_cooldown() -> bool:
    ts, count = _em_read_cooldown()
    if ts == 0.0:
        return False
    idx = min(count - 1, len(_EM_COOLDOWN_BACKOFF) - 1)
    backoff = _EM_COOLDOWN_BACKOFF[idx]
    return (time.time() - ts) < backoff


def _em_set_cooldown() -> None:
    """Record a failed EM run with exponential backoff counter."""
    try:
        ts = time.time()
        _, prev_count = _em_read_cooldown()
        # If previous cooldown already expired, reset counter; else increment
        if prev_count > 0 and (ts - _em_read_cooldown()[0]) > _EM_COOLDOWN_BACKOFF[min(prev_count - 1, len(_EM_COOLDOWN_BACKOFF) - 1)]:
            count = 1  # previous ban expired, fresh start
        else:
            count = min(prev_count + 1, 99)
        _EM_COOLDOWN_FILE.write_text(f"{ts}\n{count}")
    except OSError:
        pass


def _em_clear_cooldown() -> None:
    """Remove cooldown file — call after a successful EM request."""
    try:
        _EM_COOLDOWN_FILE.unlink(missing_ok=True)
    except OSError:
        pass


NETWORK_MODE = "auto"


class NetworkUnavailable(RuntimeError):
    """Raised when none of the configured network paths can fetch data."""

    def __init__(self, url: str, errors: Dict[str, str]):
        self.url = url
        self.errors = errors
        super().__init__(f"network unavailable: {url}")


def set_network_mode(mode: str) -> None:
    global NETWORK_MODE
    NETWORK_MODE = mode


def format_network_failure(error: NetworkUnavailable) -> str:
    lines = [
        "网络连接失败：无法连接行情服务，本次未生成新的筛选结果。",
        "已尝试的连接方式：",
    ]
    for label, detail in error.errors.items():
        lines.append(f"- {label}：{detail}")
    lines.extend([
        "已自动尝试独立备用行情源。若仍失败，可在浏览器确认行情页能否打开后重试；",
        "如直连受限，请在 daily-stock-analysis/scripts/proxy_ports.json 配置可用的本机 HTTP 代理端口后重试；",
    ])
    return "\n".join(lines)


def _system_proxy_url():
    """proxy 模式下从环境变量 / scutil 实时读代理地址（不固化，避免死代理拖累）。"""
    import subprocess
    p = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
    if p:
        return p
    try:
        out = subprocess.run(["scutil", "--proxy"], capture_output=True, text=True, timeout=5)
        if out.returncode == 0:
            import re
            txt = out.stdout or ""
            m = re.search(r"HTTPSProxy\s*:\s*([\d.]+)", txt)
            pm = re.search(r"HTTPSPort\s*:\s*(\d+)", txt)
            if m and pm:
                return f"http://{m.group(1)}:{pm.group(1)}"
    except Exception:
        pass
    return ""


def build_url_opener(network_mode: str):
    handlers = [urllib.request.HTTPSHandler(context=SSL_CONTEXT)]
    if network_mode == "direct":
        handlers.append(urllib.request.ProxyHandler({}))
    elif network_mode == "proxy":
        ph = _system_proxy_url()
        handlers.append(urllib.request.ProxyHandler({"http": ph, "https": ph} if ph else {}))
    else:
        # auto：实测最快的路径（直连或某个候选代理端口），不依赖系统代理设置
        paths = network_path.best_paths()
        if paths:
            proxy = paths[0]["proxy"]
            handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy} if proxy else {}))
        else:
            ph = _system_proxy_url()
            handlers.append(urllib.request.ProxyHandler({"http": ph, "https": ph} if ph else {}))
    return urllib.request.build_opener(*handlers)


def fetch_json(
    url: str,
    params: Dict[str, Any],
    timeout: int = 6,
    retries: int = 0,
    *,
    deadline: float | None = None,
) -> Any:
    errors: Dict[str, str] = {}
    independent_host = "sina.com.cn" in url
    if requests is not None:
        if NETWORK_MODE == "direct":
            sessions = [("直连", REQUESTS_DIRECT_SESSION)]
        elif NETWORK_MODE == "proxy":
            sessions = [("系统代理", REQUESTS_SESSION)]
        elif independent_host:
            # 东财探测失败只说明东财不可用，不能因此把所有候选路径都从新浪
            # 备用源的探测/实际抓取中排除。
            sessions = network_path.ordered_independent_sessions(
                REQUESTS_DIRECT_SESSION,
                deadline=deadline,
            )
        else:
            # auto：实测所有路径（直连+各候选代理端口）延迟，最快优先，不依赖系统代理设置
            sessions = network_path.ordered_sessions(REQUESTS_DIRECT_SESSION)

    for attempt in range(retries + 1):
        try:
            if requests is not None:
                headers = {
                    "User-Agent": UA,
                    "Referer": "https://finance.sina.com.cn/" if "sina.com.cn" in url else "https://quote.eastmoney.com/center/gridlist.html",
                    "Accept": "application/json,text/plain,*/*",
                    "Accept-Language": "zh-CN,zh;q=0.9",
                }
                for label, session in sessions:
                    remaining = deadline - time.monotonic() if deadline is not None else timeout
                    if remaining <= 0:
                        errors[label] = "deadline exceeded"
                        break
                    try:
                        resp = session.get(url, params=params, headers=headers, timeout=min(timeout, remaining),
                                           verify=tls_context.requests_verify())
                        resp.raise_for_status()
                        _mark_host_ok(url)
                        return resp.json()
                    except Exception as exc:
                        detail = " ".join(str(exc).split())[:220]
                        errors[label] = f"{type(exc).__name__}: {detail}"
                        # RemoteDisconnected = stale pooled connection; reset pool
                        if "RemoteDisconnected" in detail or "ConnectionReset" in detail:
                            try:
                                session.close()
                            except Exception:
                                pass
                _mark_host_failed(url)
                raise NetworkUnavailable(url, errors)
            query = urllib.parse.urlencode(params)
            request_url = f"{url}?{query}"
            req = urllib.request.Request(request_url, headers={"User-Agent": UA})
            if independent_host and NETWORK_MODE == "auto":
                paths = network_path.independent_path_candidates(deadline=deadline)
                for label, proxy in paths:
                    remaining = deadline - time.monotonic() if deadline is not None else timeout
                    if remaining <= 0:
                        errors[label] = "deadline exceeded"
                        break
                    handlers = [urllib.request.HTTPSHandler(context=SSL_CONTEXT)]
                    handlers.append(urllib.request.ProxyHandler(
                        {"http": proxy, "https": proxy} if proxy else {}
                    ))
                    opener = urllib.request.build_opener(*handlers)
                    try:
                        with opener.open(req, timeout=min(timeout, remaining)) as resp:
                            data = resp.read().decode("utf-8", errors="replace")
                        result = json.loads(data)
                        _mark_host_ok(url)
                        return result
                    except Exception as exc:
                        errors[label] = f"{type(exc).__name__}: {' '.join(str(exc).split())[:220]}"
            else:
                opener = build_url_opener(NETWORK_MODE)
                remaining = deadline - time.monotonic() if deadline is not None else timeout
                if remaining <= 0:
                    raise TimeoutError("deadline exceeded")
                with opener.open(req, timeout=min(timeout, remaining)) as resp:
                    data = resp.read().decode("utf-8", errors="replace")
                result = json.loads(data)
                _mark_host_ok(url)
                return result
            _mark_host_failed(url)
            raise NetworkUnavailable(url, errors)
        except NetworkUnavailable:
            pass
        except Exception as exc:  # network providers are not fully stable
            detail = " ".join(str(exc).split())[:220]
            errors["直连" if NETWORK_MODE == "direct" else "系统代理"] = f"{type(exc).__name__}: {detail}"
        if attempt < retries:
            time.sleep(0.5 * (attempt + 1))
    # 所有路径都失败：丢弃测速缓存，下一次请求重新实测各路径
    network_path.invalidate()
    raise NetworkUnavailable(url, errors)
