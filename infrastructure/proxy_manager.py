# proxy_manager.py, target: Windows 11 / Linux
# Full proxy lifecycle: SOCKS5/HTTP/HTTPS, per-account assignment, health checking,
# rotation policies, latency tracking, ban detection, persistence.

from __future__ import annotations

import asyncio
import enum
import hashlib
import ipaddress
import json
import logging
import random
import re
import socket
import statistics
import time
import urllib.parse
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Callable, Iterator, Optional

import aiohttp

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class ProxyProtocol(enum.Enum):
    SOCKS5  = "socks5"
    HTTP    = "http"
    HTTPS   = "https"
    SOCKS4  = "socks4"


class ProxyStatus(enum.Enum):
    UNKNOWN   = "unknown"
    HEALTHY   = "healthy"
    DEGRADED  = "degraded"   # high latency / partial failures
    DEAD      = "dead"       # failed health check
    BANNED    = "banned"     # detected as banned by Telegram
    COOLDOWN  = "cooldown"   # temporarily excluded, retry later
    RESERVED  = "reserved"   # pinned to a specific account


class RotationPolicy(enum.Enum):
    ROUND_ROBIN      = "round_robin"
    LEAST_USED       = "least_used"
    LOWEST_LATENCY   = "lowest_latency"
    RANDOM           = "random"
    STICKY           = "sticky"       # keep same proxy per account


class BanSignal(enum.Enum):
    FLOOD_WAIT_EXTREME  = "flood_wait_extreme"   # >3600 s FloodWait → likely IP ban
    PEER_FLOOD          = "peer_flood"
    AUTHORIZATION_ERROR = "authorization_error"
    NETWORK_TIMEOUT     = "network_timeout"
    CONNECTION_REFUSED  = "connection_refused"
    TG_CONNECT_ERROR    = "tg_connect_error"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ProxyCredential:
    username: str = ""
    password: str = ""

    def has_auth(self) -> bool:
        return bool(self.username)


@dataclass
class ProxyEntry:
    host: str
    port: int
    protocol: ProxyProtocol       = ProxyProtocol.SOCKS5
    credential: ProxyCredential   = field(default_factory=ProxyCredential)
    label: str                    = ""
    region: str                   = ""                 # e.g. "US", "TH", "NL"
    source: str                   = ""                 # e.g. filename or API tag

    # runtime state (not persisted as config, persisted as state)
    status: ProxyStatus           = ProxyStatus.UNKNOWN
    latency_ms_history: deque     = field(default_factory=lambda: deque(maxlen=20))
    fail_count: int               = 0
    success_count: int            = 0
    ban_signals: list[BanSignal]  = field(default_factory=list)
    last_check_ts: float          = 0.0
    cooldown_until: float         = 0.0
    assigned_to: list[str]        = field(default_factory=list)   # phone numbers
    use_count: int                = 0
    created_at: float             = field(default_factory=time.time)

    # --- derived ---
    @property
    def proxy_id(self) -> str:
        raw = f"{self.protocol.value}://{self.host}:{self.port}"
        return hashlib.sha1(raw.encode()).hexdigest()[:12]

    @property
    def url(self) -> str:
        cred = self.credential
        if cred.has_auth():
            auth = f"{cred.username}:{cred.password}@"
        else:
            auth = ""
        return f"{self.protocol.value}://{auth}{self.host}:{self.port}"

    @property
    def latency_avg_ms(self) -> float:
        if not self.latency_ms_history:
            return float("inf")
        return statistics.mean(self.latency_ms_history)

    @property
    def latency_p95_ms(self) -> float:
        h = sorted(self.latency_ms_history)
        if not h:
            return float("inf")
        idx = max(0, int(len(h) * 0.95) - 1)
        return h[idx]

    @property
    def is_available(self) -> bool:
        if self.status in (ProxyStatus.DEAD, ProxyStatus.BANNED):
            return False
        if self.status == ProxyStatus.COOLDOWN and time.time() < self.cooldown_until:
            return False
        return True

    def record_latency(self, ms: float) -> None:
        self.latency_ms_history.append(ms)

    def record_success(self) -> None:
        self.success_count += 1
        self.fail_count = max(0, self.fail_count - 1)
        if self.status in (ProxyStatus.DEGRADED, ProxyStatus.COOLDOWN, ProxyStatus.UNKNOWN):
            self.status = ProxyStatus.HEALTHY

    def record_failure(self, threshold: int = 5) -> None:
        self.fail_count += 1
        if self.fail_count >= threshold:
            self.status = ProxyStatus.DEAD
        elif self.fail_count >= 2:
            self.status = ProxyStatus.DEGRADED

    def add_ban_signal(self, signal: BanSignal, ban_threshold: int = 2) -> bool:
        self.ban_signals.append(signal)
        recent = [s for s in self.ban_signals[-5:]]
        if len(recent) >= ban_threshold:
            self.status = ProxyStatus.BANNED
            return True
        return False

    def set_cooldown(self, seconds: float) -> None:
        self.cooldown_until = time.time() + seconds
        self.status = ProxyStatus.COOLDOWN

    def to_dict(self) -> dict:
        return {
            "proxy_id": self.proxy_id,
            "host": self.host,
            "port": self.port,
            "protocol": self.protocol.value,
            "username": self.credential.username,
            "password": self.credential.password,
            "label": self.label,
            "region": self.region,
            "source": self.source,
            "status": self.status.value,
            "fail_count": self.fail_count,
            "success_count": self.success_count,
            "latency_avg_ms": round(self.latency_avg_ms, 1) if self.latency_avg_ms != float("inf") else None,
            "last_check_ts": self.last_check_ts,
            "cooldown_until": self.cooldown_until,
            "assigned_to": self.assigned_to,
            "use_count": self.use_count,
            "ban_signals": [s.value for s in self.ban_signals],
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ProxyEntry":
        cred = ProxyCredential(
            username=d.get("username", ""),
            password=d.get("password", ""),
        )
        e = cls(
            host=d["host"],
            port=int(d["port"]),
            protocol=ProxyProtocol(d.get("protocol", "socks5")),
            credential=cred,
            label=d.get("label", ""),
            region=d.get("region", ""),
            source=d.get("source", ""),
        )
        e.status        = ProxyStatus(d.get("status", "unknown"))
        e.fail_count    = d.get("fail_count", 0)
        e.success_count = d.get("success_count", 0)
        e.last_check_ts = d.get("last_check_ts", 0.0)
        e.cooldown_until = d.get("cooldown_until", 0.0)
        e.assigned_to   = d.get("assigned_to", [])
        e.use_count     = d.get("use_count", 0)
        e.ban_signals   = [BanSignal(s) for s in d.get("ban_signals", []) if s in BanSignal._value2member_map_]
        return e


@dataclass
class HealthCheckResult:
    proxy_id: str
    success: bool
    latency_ms: float
    error: str = ""
    checked_at: float = field(default_factory=time.time)


@dataclass
class ProxyAssignment:
    phone: str
    proxy: ProxyEntry
    assigned_at: float = field(default_factory=time.time)
    sticky: bool = False


@dataclass
class PoolStats:
    total: int
    healthy: int
    degraded: int
    dead: int
    banned: int
    cooldown: int
    unknown: int
    avg_latency_ms: float
    total_uses: int
    assignments: int


# ---------------------------------------------------------------------------
# Parser — load from text / list
# ---------------------------------------------------------------------------

_PROXY_PATTERNS = [
    # socks5://user:pass@host:port
    re.compile(
        r"^(?P<proto>socks5|socks4|http|https)://"
        r"(?:(?P<user>[^:@]+):(?P<pass>[^@]+)@)?"
        r"(?P<host>[^:]+):(?P<port>\d+)$",
        re.I,
    ),
    # host:port:user:pass
    re.compile(
        r"^(?P<host>[^:]+):(?P<port>\d+):(?P<user>[^:]*):(?P<pass>.*)$"
    ),
    # host:port
    re.compile(r"^(?P<host>[^:]+):(?P<port>\d+)$"),
]


def parse_proxy_line(line: str, default_protocol: ProxyProtocol = ProxyProtocol.SOCKS5) -> Optional[ProxyEntry]:
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    for pat in _PROXY_PATTERNS:
        m = pat.match(line)
        if not m:
            continue
        g = m.groupdict()
        proto = ProxyProtocol(g.get("proto") or default_protocol.value)
        try:
            ipaddress.ip_address(g["host"])   # validate if IP
        except ValueError:
            pass   # domain is fine
        cred = ProxyCredential(
            username=g.get("user") or "",
            password=g.get("pass") or "",
        )
        return ProxyEntry(
            host=g["host"],
            port=int(g["port"]),
            protocol=proto,
            credential=cred,
        )
    return None


def load_proxies_from_file(path: str | Path,
                            default_protocol: ProxyProtocol = ProxyProtocol.SOCKS5,
                            source_tag: str = "") -> list[ProxyEntry]:
    path = Path(path)
    entries: list[ProxyEntry] = []
    seen: set[str] = set()
    with open(path, encoding="utf-8") as fh:
        for ln in fh:
            e = parse_proxy_line(ln, default_protocol)
            if e and e.proxy_id not in seen:
                e.source = source_tag or path.name
                entries.append(e)
                seen.add(e.proxy_id)
    logger.info("Loaded %d proxies from %s", len(entries), path)
    return entries


def load_proxies_from_list(raw: list[str],
                             default_protocol: ProxyProtocol = ProxyProtocol.SOCKS5) -> list[ProxyEntry]:
    out: list[ProxyEntry] = []
    seen: set[str] = set()
    for line in raw:
        e = parse_proxy_line(line, default_protocol)
        if e and e.proxy_id not in seen:
            out.append(e)
            seen.add(e.proxy_id)
    return out


# ---------------------------------------------------------------------------
# Health checker
# ---------------------------------------------------------------------------

TEST_URLS = [
    "https://api.telegram.org",
    "https://core.telegram.org",
    "https://httpbin.org/ip",          # fallback
]

TIMEOUT_SECS = 10.0


async def _check_proxy_http(proxy: ProxyEntry,
                              test_url: str = TEST_URLS[0],
                              timeout: float = TIMEOUT_SECS) -> HealthCheckResult:
    """HTTP GET through the proxy, measure latency."""
    t0 = time.monotonic()
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as sess:
            async with sess.get(
                test_url,
                proxy=proxy.url,
                timeout=aiohttp.ClientTimeout(total=timeout),
                allow_redirects=True,
            ) as resp:
                await resp.read()
                latency = (time.monotonic() - t0) * 1000
                ok = resp.status < 500
                return HealthCheckResult(
                    proxy_id=proxy.proxy_id,
                    success=ok,
                    latency_ms=latency,
                    error="" if ok else f"HTTP {resp.status}",
                )
    except asyncio.TimeoutError:
        return HealthCheckResult(proxy.proxy_id, False, (time.monotonic()-t0)*1000, "timeout")
    except aiohttp.ClientProxyConnectionError as exc:
        return HealthCheckResult(proxy.proxy_id, False, (time.monotonic()-t0)*1000, f"proxy_conn: {exc}")
    except Exception as exc:
        return HealthCheckResult(proxy.proxy_id, False, (time.monotonic()-t0)*1000, str(exc))


async def _check_proxy_tcp(proxy: ProxyEntry,
                             timeout: float = TIMEOUT_SECS) -> HealthCheckResult:
    """Raw TCP connect to proxy host:port."""
    t0 = time.monotonic()
    try:
        fut = asyncio.open_connection(proxy.host, proxy.port)
        reader, writer = await asyncio.wait_for(fut, timeout=timeout)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        latency = (time.monotonic() - t0) * 1000
        return HealthCheckResult(proxy.proxy_id, True, latency)
    except asyncio.TimeoutError:
        return HealthCheckResult(proxy.proxy_id, False, (time.monotonic()-t0)*1000, "timeout")
    except OSError as exc:
        return HealthCheckResult(proxy.proxy_id, False, (time.monotonic()-t0)*1000, str(exc))


class HealthChecker:
    """Runs periodic or on-demand health checks for a pool."""

    def __init__(self,
                 concurrency: int = 20,
                 check_interval_sec: float = 300.0,
                 failure_threshold: int = 3,
                 latency_degraded_ms: float = 3000.0,
                 test_url: str = TEST_URLS[0]):
        self.concurrency         = concurrency
        self.check_interval_sec  = check_interval_sec
        self.failure_threshold   = failure_threshold
        self.latency_degraded_ms = latency_degraded_ms
        self.test_url            = test_url
        self._sem                = asyncio.Semaphore(concurrency)
        self._running            = False
        self._task: Optional[asyncio.Task] = None

    async def check_one(self, proxy: ProxyEntry) -> HealthCheckResult:
        async with self._sem:
            if proxy.protocol in (ProxyProtocol.HTTP, ProxyProtocol.HTTPS):
                res = await _check_proxy_http(proxy, self.test_url)
            else:
                # For SOCKS we use HTTP through it; fall back to TCP connect
                res = await _check_proxy_http(proxy, self.test_url)
                if not res.success:
                    res = await _check_proxy_tcp(proxy)

            proxy.last_check_ts = time.time()
            if res.success:
                proxy.record_latency(res.latency_ms)
                proxy.record_success()
                if res.latency_ms > self.latency_degraded_ms:
                    proxy.status = ProxyStatus.DEGRADED
            else:
                proxy.record_failure(self.failure_threshold)

            return res

    async def check_all(self, proxies: list[ProxyEntry]) -> list[HealthCheckResult]:
        return list(await asyncio.gather(*[self.check_one(p) for p in proxies]))

    async def _loop(self, get_proxies: Callable[[], list[ProxyEntry]]) -> None:
        while self._running:
            proxies = get_proxies()
            if proxies:
                results = await self.check_all(proxies)
                healthy = sum(1 for r in results if r.success)
                logger.info("Health sweep: %d/%d healthy", healthy, len(results))
            await asyncio.sleep(self.check_interval_sec)

    def start(self, get_proxies: Callable[[], list[ProxyEntry]]) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(get_proxies))

    def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()


# ---------------------------------------------------------------------------
# Rotation strategies
# ---------------------------------------------------------------------------

class _RotationStrategy:
    def pick(self, candidates: list[ProxyEntry]) -> Optional[ProxyEntry]:
        raise NotImplementedError


class _RoundRobin(_RotationStrategy):
    def __init__(self):
        self._idx = 0

    def pick(self, candidates: list[ProxyEntry]) -> Optional[ProxyEntry]:
        if not candidates:
            return None
        proxy = candidates[self._idx % len(candidates)]
        self._idx += 1
        return proxy


class _LeastUsed(_RotationStrategy):
    def pick(self, candidates: list[ProxyEntry]) -> Optional[ProxyEntry]:
        if not candidates:
            return None
        return min(candidates, key=lambda p: p.use_count)


class _LowestLatency(_RotationStrategy):
    def pick(self, candidates: list[ProxyEntry]) -> Optional[ProxyEntry]:
        if not candidates:
            return None
        return min(candidates, key=lambda p: p.latency_avg_ms)


class _Random(_RotationStrategy):
    def pick(self, candidates: list[ProxyEntry]) -> Optional[ProxyEntry]:
        return random.choice(candidates) if candidates else None


_STRATEGY_MAP: dict[RotationPolicy, _RotationStrategy] = {
    RotationPolicy.ROUND_ROBIN:    _RoundRobin(),
    RotationPolicy.LEAST_USED:     _LeastUsed(),
    RotationPolicy.LOWEST_LATENCY: _LowestLatency(),
    RotationPolicy.RANDOM:         _Random(),
}


# ---------------------------------------------------------------------------
# Proxy store (JSON persistence)
# ---------------------------------------------------------------------------

class ProxyStore:
    """Thread-safe JSON-backed store for proxy state."""

    def __init__(self, path: str | Path):
        self._path = Path(path)
        self._lock = asyncio.Lock()
        self._data: dict[str, dict] = {}   # proxy_id → state dict

    async def load(self) -> None:
        if self._path.exists():
            async with self._lock:
                try:
                    self._data = json.loads(self._path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    self._data = {}

    async def save(self, entries: list[ProxyEntry]) -> None:
        async with self._lock:
            for e in entries:
                self._data[e.proxy_id] = e.to_dict()
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(json.dumps(self._data, indent=2, ensure_ascii=False), encoding="utf-8")

    def apply_state(self, entries: list[ProxyEntry]) -> None:
        """Overlay persisted state onto freshly loaded proxy entries."""
        for e in entries:
            saved = self._data.get(e.proxy_id)
            if not saved:
                continue
            e.status         = ProxyStatus(saved.get("status", e.status.value))
            e.fail_count     = saved.get("fail_count", 0)
            e.success_count  = saved.get("success_count", 0)
            e.last_check_ts  = saved.get("last_check_ts", 0.0)
            e.cooldown_until = saved.get("cooldown_until", 0.0)
            e.assigned_to    = saved.get("assigned_to", [])
            e.use_count      = saved.get("use_count", 0)
            e.ban_signals    = [
                BanSignal(s)
                for s in saved.get("ban_signals", [])
                if s in BanSignal._value2member_map_
            ]


# ---------------------------------------------------------------------------
# Main proxy manager
# ---------------------------------------------------------------------------

class ProxyManager:
    """
    Central proxy pool for Bootsusers.

    Features
    --------
    - Load from file or list, de-dup by proxy_id
    - Per-account sticky assignment with override
    - Multiple rotation strategies
    - Async health checking loop
    - Ban signal detection → automatic status downgrade
    - JSON state persistence
    - Region filtering
    - Cooldown / circuit-breaker integration
    """

    def __init__(self,
                 store_path: str | Path = "data/proxies.json",
                 rotation_policy: RotationPolicy = RotationPolicy.LEAST_USED,
                 health_check_interval: float = 300.0,
                 failure_threshold: int = 3,
                 latency_degraded_ms: float = 3000.0,
                 ban_signal_threshold: int = 2,
                 cooldown_on_ban_sec: float = 3600.0,
                 auto_check: bool = True):

        self._entries: dict[str, ProxyEntry] = {}     # proxy_id → entry
        self._assignments: dict[str, str] = {}        # phone → proxy_id
        self._policy         = rotation_policy
        self._strategy       = _STRATEGY_MAP[rotation_policy]
        self._store          = ProxyStore(store_path)
        self._ban_threshold  = ban_signal_threshold
        self._cooldown_secs  = cooldown_on_ban_sec
        self._auto_check     = auto_check
        self._lock           = asyncio.Lock()
        self._running        = False
        self._persist_task: Optional[asyncio.Task] = None

        self._checker = HealthChecker(
            concurrency=20,
            check_interval_sec=health_check_interval,
            failure_threshold=failure_threshold,
            latency_degraded_ms=latency_degraded_ms,
        )

        # event callbacks: (event_name, data_dict) → None
        self._callbacks: list[Callable[[str, dict], None]] = []

    # --- lifecycle ---

    async def start(self) -> None:
        await self._store.load()
        async with self._lock:
            self._store.apply_state(list(self._entries.values()))
        if self._auto_check:
            self._checker.start(self.all_proxies)
        self._running = True
        self._persist_task = asyncio.create_task(self._persist_loop())
        logger.info("ProxyManager started — pool size %d", len(self._entries))

    async def stop(self) -> None:
        self._running = False
        self._checker.stop()
        if self._persist_task:
            self._persist_task.cancel()
        await self._store.save(list(self._entries.values()))

    async def _persist_loop(self, interval: float = 60.0) -> None:
        while self._running:
            await asyncio.sleep(interval)
            await self._store.save(list(self._entries.values()))

    # --- loading ---

    async def add_proxies(self, entries: list[ProxyEntry]) -> int:
        added = 0
        async with self._lock:
            for e in entries:
                if e.proxy_id not in self._entries:
                    self._entries[e.proxy_id] = e
                    added += 1
        self._store.apply_state(entries)
        logger.info("Added %d new proxies (total: %d)", added, len(self._entries))
        return added

    async def load_from_file(self,
                              path: str | Path,
                              protocol: ProxyProtocol = ProxyProtocol.SOCKS5) -> int:
        entries = load_proxies_from_file(path, protocol)
        return await self.add_proxies(entries)

    async def load_from_list(self,
                              lines: list[str],
                              protocol: ProxyProtocol = ProxyProtocol.SOCKS5) -> int:
        entries = load_proxies_from_list(lines, protocol)
        return await self.add_proxies(entries)

    # --- query ---

    def all_proxies(self) -> list[ProxyEntry]:
        return list(self._entries.values())

    def available_proxies(self, region: str = "") -> list[ProxyEntry]:
        out = [p for p in self._entries.values() if p.is_available]
        if region:
            out = [p for p in out if p.region.upper() == region.upper()]
        return out

    def get_proxy(self, proxy_id: str) -> Optional[ProxyEntry]:
        return self._entries.get(proxy_id)

    # --- assignment ---

    async def get_or_assign(self,
                             phone: str,
                             *,
                             region: str = "",
                             force_new: bool = False) -> Optional[ProxyEntry]:
        """Return the proxy assigned to this phone. Assign one if none."""
        async with self._lock:
            if not force_new and phone in self._assignments:
                pid = self._assignments[phone]
                proxy = self._entries.get(pid)
                if proxy and proxy.is_available:
                    return proxy
                # assigned proxy is dead/banned; fall through and re-assign

            candidates = [p for p in self._entries.values()
                          if p.is_available and
                          (not region or p.region.upper() == region.upper())]
            if not candidates:
                return None

            proxy = self._strategy.pick(candidates)
            if proxy:
                self._assignments[phone] = proxy.proxy_id
                if phone not in proxy.assigned_to:
                    proxy.assigned_to.append(phone)
                proxy.use_count += 1
            return proxy

    async def release_assignment(self, phone: str) -> None:
        async with self._lock:
            pid = self._assignments.pop(phone, None)
            if pid:
                proxy = self._entries.get(pid)
                if proxy and phone in proxy.assigned_to:
                    proxy.assigned_to.remove(phone)

    async def force_assign(self, phone: str, proxy_id: str) -> bool:
        async with self._lock:
            proxy = self._entries.get(proxy_id)
            if not proxy:
                return False
            # remove from old
            old_pid = self._assignments.get(phone)
            if old_pid and old_pid != proxy_id:
                old = self._entries.get(old_pid)
                if old and phone in old.assigned_to:
                    old.assigned_to.remove(phone)
            self._assignments[phone] = proxy_id
            if phone not in proxy.assigned_to:
                proxy.assigned_to.append(phone)
            return True

    # --- reporting & feedback ---

    async def report_success(self, proxy_id: str, latency_ms: float) -> None:
        async with self._lock:
            proxy = self._entries.get(proxy_id)
            if proxy:
                proxy.record_success()
                proxy.record_latency(latency_ms)

    async def report_failure(self, proxy_id: str, error: str = "") -> None:
        async with self._lock:
            proxy = self._entries.get(proxy_id)
            if proxy:
                proxy.record_failure(3)
                logger.debug("Proxy %s failure: %s", proxy_id, error)

    async def report_ban_signal(self,
                                 proxy_id: str,
                                 signal: BanSignal,
                                 phone: str = "") -> bool:
        """
        Returns True if the proxy was marked BANNED as a result.
        """
        async with self._lock:
            proxy = self._entries.get(proxy_id)
            if not proxy:
                return False
            became_banned = proxy.add_ban_signal(signal, self._ban_threshold)
            if became_banned:
                logger.warning("Proxy %s BANNED (signal: %s, phone: %s)",
                               proxy_id, signal.value, phone)
                self._emit("proxy_banned", {"proxy_id": proxy_id,
                                            "signal": signal.value,
                                            "phone": phone})
                # re-assign the phone
                if phone and self._assignments.get(phone) == proxy_id:
                    self._assignments.pop(phone, None)
                    proxy.assigned_to = [p for p in proxy.assigned_to if p != phone]
            return became_banned

    async def set_cooldown(self, proxy_id: str, seconds: float) -> None:
        async with self._lock:
            proxy = self._entries.get(proxy_id)
            if proxy:
                proxy.set_cooldown(seconds)

    async def reset_proxy(self, proxy_id: str) -> bool:
        async with self._lock:
            proxy = self._entries.get(proxy_id)
            if not proxy:
                return False
            proxy.status       = ProxyStatus.UNKNOWN
            proxy.fail_count   = 0
            proxy.ban_signals  = []
            proxy.cooldown_until = 0.0
            return True

    async def remove_proxy(self, proxy_id: str) -> bool:
        async with self._lock:
            proxy = self._entries.pop(proxy_id, None)
            if not proxy:
                return False
            for phone in list(proxy.assigned_to):
                self._assignments.pop(phone, None)
            return True

    # --- health check on demand ---

    async def check_proxy(self, proxy_id: str) -> Optional[HealthCheckResult]:
        proxy = self._entries.get(proxy_id)
        if not proxy:
            return None
        return await self._checker.check_one(proxy)

    async def check_all(self) -> list[HealthCheckResult]:
        return await self._checker.check_all(list(self._entries.values()))

    # --- policy ---

    def set_rotation_policy(self, policy: RotationPolicy) -> None:
        self._policy   = policy
        self._strategy = _STRATEGY_MAP[policy]

    # --- stats ---

    def pool_stats(self) -> PoolStats:
        entries = list(self._entries.values())
        latencies = [p.latency_avg_ms for p in entries if p.latency_avg_ms != float("inf")]
        return PoolStats(
            total    = len(entries),
            healthy  = sum(1 for p in entries if p.status == ProxyStatus.HEALTHY),
            degraded = sum(1 for p in entries if p.status == ProxyStatus.DEGRADED),
            dead     = sum(1 for p in entries if p.status == ProxyStatus.DEAD),
            banned   = sum(1 for p in entries if p.status == ProxyStatus.BANNED),
            cooldown = sum(1 for p in entries if p.status == ProxyStatus.COOLDOWN),
            unknown  = sum(1 for p in entries if p.status == ProxyStatus.UNKNOWN),
            avg_latency_ms = statistics.mean(latencies) if latencies else 0.0,
            total_uses     = sum(p.use_count for p in entries),
            assignments    = len(self._assignments),
        )

    def report(self) -> dict:
        stats = self.pool_stats()
        return {
            "pool": asdict(stats),
            "assignments": {phone: pid for phone, pid in self._assignments.items()},
            "proxies": [p.to_dict() for p in self._entries.values()],
        }

    # --- events ---

    def on_event(self, cb: Callable[[str, dict], None]) -> None:
        self._callbacks.append(cb)

    def _emit(self, name: str, data: dict) -> None:
        for cb in self._callbacks:
            try:
                cb(name, data)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Multi-pool manager (e.g., different pools per region)
# ---------------------------------------------------------------------------

class MultiPoolManager:
    """
    Wraps several ProxyManager instances, one per region (or pool label).
    Falls back to global pool if region-specific pool is empty.
    """

    def __init__(self, store_dir: str | Path = "data/proxies"):
        self._dir  = Path(store_dir)
        self._pools: dict[str, ProxyManager] = {}
        self._global = ProxyManager(
            store_path=self._dir / "global.json",
            rotation_policy=RotationPolicy.LEAST_USED,
        )

    async def start(self) -> None:
        await self._global.start()
        for mgr in self._pools.values():
            await mgr.start()

    async def stop(self) -> None:
        await self._global.stop()
        for mgr in self._pools.values():
            await mgr.stop()

    def pool(self, region: str) -> Optional[ProxyManager]:
        return self._pools.get(region.upper())

    async def add_pool(self, region: str, proxies: list[ProxyEntry]) -> ProxyManager:
        region = region.upper()
        if region not in self._pools:
            self._pools[region] = ProxyManager(
                store_path=self._dir / f"{region.lower()}.json",
                rotation_policy=RotationPolicy.LEAST_USED,
            )
            await self._pools[region].start()
        for e in proxies:
            e.region = region
        await self._pools[region].add_proxies(proxies)
        return self._pools[region]

    async def get_or_assign(self,
                             phone: str,
                             *,
                             region: str = "",
                             force_new: bool = False) -> Optional[ProxyEntry]:
        if region and region.upper() in self._pools:
            proxy = await self._pools[region.upper()].get_or_assign(
                phone, region=region, force_new=force_new
            )
            if proxy:
                return proxy
        # fallback to global
        return await self._global.get_or_assign(phone, region=region, force_new=force_new)

    def combined_stats(self) -> dict:
        out = {"global": self._global.pool_stats()}
        for region, mgr in self._pools.items():
            out[region] = mgr.pool_stats()
        return out


# ---------------------------------------------------------------------------
# Convenience builders
# ---------------------------------------------------------------------------

async def build_proxy_manager(
    proxy_file: Optional[str | Path] = None,
    proxy_list: Optional[list[str]] = None,
    store_path: str | Path = "data/proxies.json",
    protocol: ProxyProtocol = ProxyProtocol.SOCKS5,
    rotation_policy: RotationPolicy = RotationPolicy.LEAST_USED,
    auto_check: bool = True,
) -> ProxyManager:
    """One-call builder: load proxies, start manager, return ready instance."""
    mgr = ProxyManager(
        store_path=store_path,
        rotation_policy=rotation_policy,
        auto_check=auto_check,
    )
    await mgr.start()
    if proxy_file:
        await mgr.load_from_file(proxy_file, protocol)
    if proxy_list:
        await mgr.load_from_list(proxy_list, protocol)
    return mgr


async def assign_proxies_to_accounts(
    manager: ProxyManager,
    phones: list[str],
    region: str = "",
) -> dict[str, Optional[ProxyEntry]]:
    """Bulk assign: return phone → ProxyEntry mapping."""
    result: dict[str, Optional[ProxyEntry]] = {}
    for phone in phones:
        result[phone] = await manager.get_or_assign(phone, region=region)
    return result


def proxy_to_telethon_dict(proxy: ProxyEntry) -> dict:
    """
    Convert a ProxyEntry to Telethon's proxy parameter format.
    Telethon expects: (socks.SOCKS5, host, port, True, username, password)
    but accepts a dict too via python_socks.
    """
    proto_map = {
        ProxyProtocol.SOCKS5: 2,   # socks.SOCKS5
        ProxyProtocol.SOCKS4: 1,
        ProxyProtocol.HTTP:   3,
        ProxyProtocol.HTTPS:  3,
    }
    d: dict = {
        "proxy_type": proto_map.get(proxy.protocol, 2),
        "addr": proxy.host,
        "port": proxy.port,
        "rdns": True,
    }
    if proxy.credential.has_auth():
        d["username"] = proxy.credential.username
        d["password"] = proxy.credential.password
    return d


def build_telethon_proxies(
    assignments: dict[str, Optional[ProxyEntry]]
) -> dict[str, Optional[dict]]:
    """Map phone → telethon proxy dict for use in TelegramClient(proxy=...)."""
    return {
        phone: proxy_to_telethon_dict(proxy) if proxy else None
        for phone, proxy in assignments.items()
    }


# ---------------------------------------------------------------------------
# Proxy scanner — test a batch of untrusted proxies before adding to pool
# ---------------------------------------------------------------------------

class ProxyScanner:
    """
    Validate a list of raw proxy strings before ingesting them.
    Runs concurrent health checks and returns only the passing ones.
    """

    def __init__(self,
                 concurrency: int = 50,
                 timeout: float = 8.0,
                 min_pass_rate: float = 0.0):
        self.concurrency   = concurrency
        self.timeout       = timeout
        self.min_pass_rate = min_pass_rate
        self._checker      = HealthChecker(concurrency=concurrency)

    async def scan(self,
                   raw_lines: list[str],
                   protocol: ProxyProtocol = ProxyProtocol.SOCKS5,
                   *,
                   progress_cb: Optional[Callable[[int, int], None]] = None
                   ) -> tuple[list[ProxyEntry], list[ProxyEntry]]:
        """
        Returns (working, dead) lists of ProxyEntry.
        progress_cb(done, total) called after each batch.
        """
        entries = load_proxies_from_list(raw_lines, protocol)
        total   = len(entries)
        done    = 0
        sem     = asyncio.Semaphore(self.concurrency)
        working: list[ProxyEntry] = []
        dead:    list[ProxyEntry] = []

        async def _one(e: ProxyEntry) -> None:
            nonlocal done
            async with sem:
                res = await self._checker.check_one(e)
                if res.success:
                    working.append(e)
                else:
                    dead.append(e)
                done += 1
                if progress_cb:
                    progress_cb(done, total)

        await asyncio.gather(*[_one(e) for e in entries])
        logger.info("Scan complete: %d/%d working", len(working), total)
        return working, dead

    async def scan_file(self,
                        path: str | Path,
                        protocol: ProxyProtocol = ProxyProtocol.SOCKS5,
                        out_file: Optional[str | Path] = None
                        ) -> tuple[list[ProxyEntry], list[ProxyEntry]]:
        with open(path, encoding="utf-8") as fh:
            lines = fh.readlines()
        working, dead = await self.scan(lines, protocol)
        if out_file:
            Path(out_file).write_text(
                "\n".join(e.url for e in working),
                encoding="utf-8"
            )
        return working, dead


# ---------------------------------------------------------------------------
# Ban detector — integrates with Telethon error stream
# ---------------------------------------------------------------------------

class BanDetector:
    """
    Receives Telethon exceptions and maps them to BanSignals.
    Call detect(exc, proxy_id, phone) after catching an error from TelegramClient.
    """

    _FLOOD_BAN_THRESHOLD_SEC = 3600   # FloodWait > 1h → likely IP ban

    def __init__(self, manager: ProxyManager):
        self._mgr = manager

    async def detect(self,
                     exc: Exception,
                     proxy_id: Optional[str],
                     phone: str) -> Optional[BanSignal]:
        if proxy_id is None:
            return None
        signal = self._classify(exc)
        if signal:
            became_banned = await self._mgr.report_ban_signal(proxy_id, signal, phone)
            if became_banned:
                logger.warning("BanDetector: proxy %s banned via signal %s", proxy_id, signal.value)
        return signal

    def _classify(self, exc: Exception) -> Optional[BanSignal]:
        name = type(exc).__name__
        msg  = str(exc).lower()

        if "floodwaiterror" in name or "floodwait" in name:
            # extract seconds
            m = re.search(r"(\d+)", msg)
            if m and int(m.group(1)) > self._FLOOD_BAN_THRESHOLD_SEC:
                return BanSignal.FLOOD_WAIT_EXTREME
            return None

        if "peerflood" in name:
            return BanSignal.PEER_FLOOD

        if "authkey" in name or "unauthorized" in name:
            return BanSignal.AUTHORIZATION_ERROR

        if any(k in msg for k in ("connection refused", "connection reset", "econnrefused")):
            return BanSignal.CONNECTION_REFUSED

        if "timeout" in msg or "timed out" in msg:
            return BanSignal.NETWORK_TIMEOUT

        if "connectionerror" in name or "rpccallconflict" in name:
            return BanSignal.TG_CONNECT_ERROR

        return None


# ---------------------------------------------------------------------------
# Export helpers
# ---------------------------------------------------------------------------

def export_working_proxies(manager: ProxyManager,
                            path: str | Path,
                            status_filter: Optional[list[ProxyStatus]] = None) -> int:
    """Write working proxies to file, one URL per line."""
    path = Path(path)
    statuses = status_filter or [ProxyStatus.HEALTHY, ProxyStatus.UNKNOWN]
    entries = [p for p in manager.all_proxies() if p.status in statuses]
    path.write_text("\n".join(e.url for e in entries), encoding="utf-8")
    return len(entries)