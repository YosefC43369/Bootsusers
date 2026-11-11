# account_manager.py
# Shared account management layer for auto_join.py and auto_react.py
# Provides: Account, ProxyConfig, SessionState, AccountPool, AccountManager
# Integrates with: infrastructure.ProxyManager, infrastructure.RateController, infrastructure.FloodGuard

from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import logging
import os
import random
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
)

from telethon import TelegramClient
from telethon.errors import (
    AuthKeyError,
    AuthKeyUnregisteredError,
    FloodWaitError,
    PhoneNumberBannedError,
    SessionPasswordNeededError,
    UserDeactivatedBanError,
    UserDeactivatedError,
)
from telethon.network import ConnectionTcpMTProxyRandomizedIntermediate

if TYPE_CHECKING:
    from infrastructure import FloodGuard, ProxyManager, RateController

# ---------------------------------------------------------------------------
# Module logger
# ---------------------------------------------------------------------------
logger = logging.getLogger("account_manager")

# ---------------------------------------------------------------------------
# Default config constants (overridable at AccountManager init)
# ---------------------------------------------------------------------------
DEFAULT_ACCOUNTS_CSV = "Telegram_70_000_000_[YOSCRYPT].csv"
API_ID: int = 34674448
API_HASH: str = "7098ee8e75f3c435f808ee9410a2f05b"

CONNECT_TIMEOUT_SEC: float = 30.0
HEALTH_CHECK_INTERVAL_SEC: float = 60.0
RECONNECT_COOLDOWN_SEC: float = 10.0
MAX_RECONNECT_ATTEMPTS: int = 5
SESSION_DIR: str = "sessions"
STATE_FILE: str = "data/account_states.json"


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class AccountStatus(Enum):
    """Runtime lifecycle status of an account."""

    UNKNOWN = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    DISCONNECTED = auto()
    BANNED = auto()
    FLOOD_WAIT = auto()
    AUTH_FAILED = auto()
    DEACTIVATED = auto()


class HealthStatus(Enum):
    """Result of a single health probe."""

    OK = auto()
    FLOOD = auto()
    AUTH_ERROR = auto()
    BANNED = auto()
    DEACTIVATED = auto()
    TIMEOUT = auto()
    NETWORK_ERROR = auto()


class RotationStrategy(Enum):
    """How AccountPool picks the next account."""

    ROUND_ROBIN = auto()
    RANDOM = auto()
    LEAST_RECENTLY_USED = auto()
    LEAST_FLOOD_RATE = auto()


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ProxyConfig:
    """SOCKS5 / MTProto proxy settings for a Telethon client."""

    host: str = ""
    port: int = 0
    username: str = ""
    password: str = ""
    proxy_type: str = "socks5"  # "socks5" | "socks4" | "http" | "mtproto"
    secret: str = ""  # MTProto only

    @property
    def is_set(self) -> bool:
        return bool(self.host and self.port)

    def to_telethon(self) -> Optional[tuple]:
        """Return the proxy tuple Telethon expects, or None."""
        if not self.is_set:
            return None
        if self.proxy_type in ("socks5", "socks4"):
            import socks  # PySocks

            proxy_map = {"socks5": socks.SOCKS5, "socks4": socks.SOCKS4}
            return (
                proxy_map[self.proxy_type],
                self.host,
                self.port,
                True,
                self.username or None,
                self.password or None,
            )
        if self.proxy_type == "http":
            return (self.host, self.port, self.username, self.password)
        if self.proxy_type == "mtproto":
            return (
                self.host,
                self.port,
                self.secret,
            )
        return None

    @classmethod
    def from_dict(cls, d: dict) -> "ProxyConfig":
        return cls(
            host=d.get("host", ""),
            port=int(d.get("port", 0)),
            username=d.get("username", ""),
            password=d.get("password", ""),
            proxy_type=d.get("proxy_type", "socks5"),
            secret=d.get("secret", ""),
        )

    def to_dict(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "password": self.password,
            "proxy_type": self.proxy_type,
            "secret": self.secret,
        }

    @property
    def proxy_id(self) -> str:
        raw = f"{self.proxy_type}:{self.host}:{self.port}:{self.username}"
        return hashlib.sha1(raw.encode()).hexdigest()[:12]


@dataclass
class AccountMeta:
    """Persistent metadata stored between sessions."""

    phone: str
    total_joins: int = 0
    total_reactions: int = 0
    total_floods: int = 0
    ban_count: int = 0
    last_active: float = 0.0
    last_flood_at: float = 0.0
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "phone": self.phone,
            "total_joins": self.total_joins,
            "total_reactions": self.total_reactions,
            "total_floods": self.total_floods,
            "ban_count": self.ban_count,
            "last_active": self.last_active,
            "last_flood_at": self.last_flood_at,
            "tags": self.tags,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "AccountMeta":
        return cls(
            phone=d["phone"],
            total_joins=d.get("total_joins", 0),
            total_reactions=d.get("total_reactions", 0),
            total_floods=d.get("total_floods", 0),
            ban_count=d.get("ban_count", 0),
            last_active=d.get("last_active", 0.0),
            last_flood_at=d.get("last_flood_at", 0.0),
            tags=d.get("tags", []),
        )


@dataclass
class SessionState:
    """Runtime state of a connected account — not persisted."""

    status: AccountStatus = AccountStatus.UNKNOWN
    health: HealthStatus = HealthStatus.OK
    reconnect_attempts: int = 0
    last_health_check: float = 0.0
    flood_until: float = 0.0           # epoch time when flood expires
    connect_time: float = 0.0
    last_error: str = ""

    @property
    def is_flood_active(self) -> bool:
        return time.monotonic() < self.flood_until

    @property
    def flood_remaining(self) -> float:
        return max(0.0, self.flood_until - time.monotonic())

    @property
    def is_usable(self) -> bool:
        return (
            self.status == AccountStatus.CONNECTED
            and not self.is_flood_active
            and self.health not in (HealthStatus.BANNED, HealthStatus.DEACTIVATED)
        )


@dataclass
class Account:
    """
    One Telegram account.  Central shared data class used by both
    auto_join.py and auto_react.py (and any future module).
    """

    phone: str
    api_id: int = API_ID
    api_hash: str = API_HASH
    proxy: ProxyConfig = field(default_factory=ProxyConfig)
    session_name: str = ""  # auto-derived if empty
    meta: AccountMeta = field(default_factory=lambda: AccountMeta(phone=""))
    state: SessionState = field(default_factory=SessionState)
    client: Optional[TelegramClient] = field(default=None, repr=False, compare=False)
    _lock: asyncio.Lock = field(
        default_factory=asyncio.Lock, init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if not self.session_name:
            self.session_name = self.phone.replace("+", "").replace(" ", "")
        if not self.meta.phone:
            self.meta.phone = self.phone

    @property
    def session_path(self) -> str:
        return os.path.join(SESSION_DIR, self.session_name)

    def build_client(self) -> TelegramClient:
        """Construct a fresh TelegramClient for this account."""
        proxy_tuple = self.proxy.to_telethon()
        kwargs: dict[str, Any] = {
            "session": self.session_path,
            "api_id": self.api_id,
            "api_hash": self.api_hash,
            "timeout": CONNECT_TIMEOUT_SEC,
            "connection_retries": 3,
            "retry_delay": 5,
            "device_model": _random_device(),
            "system_version": _random_system(),
            "app_version": "9.3.3",
            "lang_code": "en",
            "system_lang_code": "en-US",
        }
        if proxy_tuple:
            if self.proxy.proxy_type == "mtproto":
                kwargs["connection"] = ConnectionTcpMTProxyRandomizedIntermediate
                kwargs["proxy"] = proxy_tuple
            else:
                kwargs["proxy"] = proxy_tuple
        return TelegramClient(**kwargs)

    def to_dict(self) -> dict:
        return {
            "phone": self.phone,
            "api_id": self.api_id,
            "api_hash": self.api_hash,
            "proxy": self.proxy.to_dict(),
            "session_name": self.session_name,
            "meta": self.meta.to_dict(),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Account":
        acc = cls(
            phone=d["phone"],
            api_id=d.get("api_id", API_ID),
            api_hash=d.get("api_hash", API_HASH),
            proxy=ProxyConfig.from_dict(d.get("proxy", {})),
            session_name=d.get("session_name", ""),
        )
        if "meta" in d:
            acc.meta = AccountMeta.from_dict(d["meta"])
        return acc


# ---------------------------------------------------------------------------
# CSV loader
# ---------------------------------------------------------------------------


def load_accounts(
    csv_path: str = DEFAULT_ACCOUNTS_CSV,
    proxy_list: Optional[List[ProxyConfig]] = None,
    limit: Optional[int] = None,
    shuffle: bool = False,
) -> List[Account]:
    """
    Load accounts from the project's CSV file.

    Expected columns (order-independent, header row required):
        phone, [api_id], [api_hash], [proxy_host], [proxy_port],
        [proxy_user], [proxy_pass], [proxy_type], [session_name]

    If proxy_list is provided, accounts are assigned proxies round-robin,
    ignoring any proxy columns in the CSV.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Accounts CSV not found: {csv_path}")

    accounts: List[Account] = []

    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for i, row in enumerate(reader):
            if limit is not None and i >= limit:
                break

            phone = row.get("phone", "").strip()
            if not phone:
                logger.warning("Row %d: missing phone, skipped", i + 1)
                continue

            api_id = int(row.get("api_id", API_ID) or API_ID)
            api_hash = str(row.get("api_hash", API_HASH) or API_HASH).strip()
            session_name = row.get("session_name", "").strip()

            # Proxy resolution: explicit list wins over CSV columns
            if proxy_list:
                proxy = proxy_list[i % len(proxy_list)]
            else:
                h = row.get("proxy_host", "").strip()
                p = row.get("proxy_port", "0").strip()
                proxy = ProxyConfig(
                    host=h,
                    port=int(p) if p.isdigit() else 0,
                    username=row.get("proxy_user", "").strip(),
                    password=row.get("proxy_pass", "").strip(),
                    proxy_type=row.get("proxy_type", "socks5").strip().lower(),
                )

            acc = Account(
                phone=phone,
                api_id=api_id,
                api_hash=api_hash,
                proxy=proxy,
                session_name=session_name,
            )
            accounts.append(acc)

    if shuffle:
        random.shuffle(accounts)

    logger.info("Loaded %d accounts from %s", len(accounts), csv_path)
    return accounts


# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------


def _random_device() -> str:
    models = [
        "Samsung Galaxy S23",
        "Google Pixel 8",
        "OnePlus 11",
        "Xiaomi 13 Pro",
        "OPPO Find X6",
        "Redmi Note 12",
        "Realme GT 3",
        "Nokia G60",
    ]
    return random.choice(models)


def _random_system() -> str:
    versions = [
        "Android 13",
        "Android 14",
        "Android 12L",
        "Android 13.1",
    ]
    return random.choice(versions)


async def connect_account(
    acc: Account,
    *,
    flood_guard: Optional["FloodGuard"] = None,
) -> bool:
    """
    Connect a single account.  Returns True on success.

    Handles:
    - Building a fresh TelegramClient
    - FloodWaitError on connect
    - AuthKey errors → marks DEACTIVATED
    - PhoneNumberBanned / UserDeactivated → marks BANNED
    """
    async with acc._lock:
        if acc.state.status == AccountStatus.CONNECTED and acc.client is not None:
            if await acc.client.is_user_authorized():
                return True

        acc.state.status = AccountStatus.CONNECTING
        acc.state.last_error = ""

        try:
            os.makedirs(SESSION_DIR, exist_ok=True)
            client = acc.build_client()
            await client.connect()

            if not await client.is_user_authorized():
                logger.warning("%s: not authorized — session may be expired", acc.phone)
                acc.state.status = AccountStatus.AUTH_FAILED
                acc.state.last_error = "not authorized"
                await client.disconnect()
                return False

            acc.client = client
            acc.state.status = AccountStatus.CONNECTED
            acc.state.reconnect_attempts = 0
            acc.state.connect_time = time.monotonic()
            acc.meta.last_active = time.time()
            logger.info("%s: connected", acc.phone)

            if flood_guard:
                await flood_guard.register(acc.phone)

            return True

        except FloodWaitError as e:
            wait = e.seconds
            logger.warning("%s: FloodWait %ds on connect", acc.phone, wait)
            acc.state.status = AccountStatus.FLOOD_WAIT
            acc.state.flood_until = time.monotonic() + wait
            acc.state.last_error = f"FloodWait {wait}s"
            acc.meta.total_floods += 1
            acc.meta.last_flood_at = time.time()
            if flood_guard:
                await flood_guard.record_flood(acc.phone, wait, source="connect")
            return False

        except (
            AuthKeyError,
            AuthKeyUnregisteredError,
            SessionPasswordNeededError,
        ) as e:
            logger.error("%s: auth error — %s", acc.phone, e)
            acc.state.status = AccountStatus.AUTH_FAILED
            acc.state.last_error = str(e)
            return False

        except (PhoneNumberBannedError, UserDeactivatedBanError, UserDeactivatedError) as e:
            logger.error("%s: account banned/deactivated — %s", acc.phone, e)
            acc.state.status = AccountStatus.BANNED
            acc.state.health = HealthStatus.BANNED
            acc.state.last_error = str(e)
            acc.meta.ban_count += 1
            return False

        except Exception as e:
            logger.error("%s: connect error — %s", acc.phone, e)
            acc.state.status = AccountStatus.DISCONNECTED
            acc.state.last_error = str(e)
            return False


async def disconnect_account(acc: Account) -> None:
    """Cleanly disconnect a single account."""
    async with acc._lock:
        if acc.client is not None:
            try:
                await acc.client.disconnect()
            except Exception:
                pass
            acc.client = None
        acc.state.status = AccountStatus.DISCONNECTED


async def reconnect_account(
    acc: Account,
    *,
    flood_guard: Optional["FloodGuard"] = None,
    max_attempts: int = MAX_RECONNECT_ATTEMPTS,
    cooldown: float = RECONNECT_COOLDOWN_SEC,
) -> bool:
    """
    Attempt to reconnect a disconnected account with exponential backoff.
    Returns True when connection is re-established.
    """
    if acc.state.status in (AccountStatus.BANNED, AccountStatus.DEACTIVATED):
        logger.warning("%s: skipping reconnect — permanent failure", acc.phone)
        return False

    for attempt in range(1, max_attempts + 1):
        wait = cooldown * (2 ** (attempt - 1)) + random.uniform(0, 2)
        logger.info(
            "%s: reconnect attempt %d/%d (wait %.1fs)",
            acc.phone, attempt, max_attempts, wait
        )
        await asyncio.sleep(wait)

        await disconnect_account(acc)
        ok = await connect_account(acc, flood_guard=flood_guard)
        if ok:
            acc.state.reconnect_attempts = attempt
            return True

    logger.error("%s: all %d reconnect attempts failed", acc.phone, max_attempts)
    acc.state.reconnect_attempts = max_attempts
    return False


# ---------------------------------------------------------------------------
# Bulk connection
# ---------------------------------------------------------------------------


async def connect_all(
    accounts: List[Account],
    *,
    concurrency: int = 5,
    flood_guard: Optional["FloodGuard"] = None,
    on_connected: Optional[Callable[[Account], None]] = None,
    on_failed: Optional[Callable[[Account], None]] = None,
) -> Tuple[List[Account], List[Account]]:
    """
    Connect all accounts with bounded concurrency.

    Returns (connected_list, failed_list).
    """
    sem = asyncio.Semaphore(concurrency)
    connected: List[Account] = []
    failed: List[Account] = []
    _lock = asyncio.Lock()

    async def _connect_one(acc: Account) -> None:
        async with sem:
            ok = await connect_account(acc, flood_guard=flood_guard)
            async with _lock:
                if ok:
                    connected.append(acc)
                    if on_connected:
                        on_connected(acc)
                else:
                    failed.append(acc)
                    if on_failed:
                        on_failed(acc)

    await asyncio.gather(*[_connect_one(a) for a in accounts])
    logger.info(
        "connect_all: %d connected, %d failed out of %d total",
        len(connected), len(failed), len(accounts),
    )
    return connected, failed


async def disconnect_all(accounts: List[Account]) -> None:
    """Disconnect all accounts concurrently."""
    await asyncio.gather(*[disconnect_account(a) for a in accounts])
    logger.info("disconnect_all: %d accounts disconnected", len(accounts))


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


async def health_check(
    acc: Account,
    *,
    flood_guard: Optional["FloodGuard"] = None,
) -> HealthStatus:
    """
    Run a lightweight health probe on a connected account.

    Uses GetMeRequest — smallest possible RPC call.
    Updates acc.state.health and acc.state.last_health_check.
    """
    from telethon.tl.functions.users import GetFullUserRequest

    if acc.client is None or not acc.client.is_connected():
        acc.state.health = HealthStatus.NETWORK_ERROR
        acc.state.last_health_check = time.monotonic()
        return HealthStatus.NETWORK_ERROR

    try:
        me = await asyncio.wait_for(acc.client.get_me(), timeout=15.0)
        if me is None:
            acc.state.health = HealthStatus.AUTH_ERROR
        else:
            acc.state.health = HealthStatus.OK
            acc.meta.last_active = time.time()

    except FloodWaitError as e:
        wait = e.seconds
        logger.warning("%s: health check flood %ds", acc.phone, wait)
        acc.state.health = HealthStatus.FLOOD
        acc.state.flood_until = time.monotonic() + wait
        acc.meta.total_floods += 1
        acc.meta.last_flood_at = time.time()
        if flood_guard:
            await flood_guard.record_flood(acc.phone, wait, source="health_check")

    except (PhoneNumberBannedError, UserDeactivatedBanError, UserDeactivatedError):
        acc.state.health = HealthStatus.BANNED
        acc.state.status = AccountStatus.BANNED
        acc.meta.ban_count += 1

    except asyncio.TimeoutError:
        acc.state.health = HealthStatus.TIMEOUT

    except Exception as e:
        logger.debug("%s: health probe error: %s", acc.phone, e)
        acc.state.health = HealthStatus.NETWORK_ERROR

    acc.state.last_health_check = time.monotonic()
    return acc.state.health


# ---------------------------------------------------------------------------
# Account pool
# ---------------------------------------------------------------------------


class AccountPool:
    """
    Thread-safe account pool with pluggable rotation strategy.

    Maintains a hot set of usable accounts and a cold set of
    temporarily unavailable ones (flood / disconnected).
    """

    def __init__(
        self,
        accounts: List[Account],
        strategy: RotationStrategy = RotationStrategy.ROUND_ROBIN,
    ) -> None:
        self._all: List[Account] = list(accounts)
        self._strategy = strategy
        self._rr_index: int = 0
        self._lock = asyncio.Lock()

    @property
    def all_accounts(self) -> List[Account]:
        return list(self._all)

    @property
    def connected(self) -> List[Account]:
        return [a for a in self._all if a.state.status == AccountStatus.CONNECTED]

    @property
    def usable(self) -> List[Account]:
        return [a for a in self._all if a.state.is_usable]

    @property
    def flood_blocked(self) -> List[Account]:
        return [a for a in self._all if a.state.is_flood_active]

    @property
    def banned(self) -> List[Account]:
        return [a for a in self._all if a.state.status == AccountStatus.BANNED]

    @property
    def disconnected(self) -> List[Account]:
        return [a for a in self._all if a.state.status == AccountStatus.DISCONNECTED]

    async def next(self) -> Optional[Account]:
        """Pick the next usable account per the rotation strategy."""
        async with self._lock:
            pool = self.usable
            if not pool:
                return None

            if self._strategy == RotationStrategy.ROUND_ROBIN:
                acc = pool[self._rr_index % len(pool)]
                self._rr_index += 1
                return acc

            if self._strategy == RotationStrategy.RANDOM:
                return random.choice(pool)

            if self._strategy == RotationStrategy.LEAST_RECENTLY_USED:
                return min(pool, key=lambda a: a.meta.last_active)

            if self._strategy == RotationStrategy.LEAST_FLOOD_RATE:
                return min(pool, key=lambda a: a.meta.total_floods)

            return pool[0]

    async def next_batch(self, n: int) -> List[Account]:
        """Pick up to n usable accounts without repetition."""
        async with self._lock:
            pool = self.usable
            if not pool:
                return []
            count = min(n, len(pool))
            if self._strategy == RotationStrategy.RANDOM:
                return random.sample(pool, count)
            # round-robin slice
            start = self._rr_index % len(pool)
            self._rr_index += count
            indices = [(start + i) % len(pool) for i in range(count)]
            return [pool[i] for i in indices]

    def add(self, acc: Account) -> None:
        if acc not in self._all:
            self._all.append(acc)

    def remove(self, phone: str) -> None:
        self._all = [a for a in self._all if a.phone != phone]

    def by_phone(self, phone: str) -> Optional[Account]:
        for a in self._all:
            if a.phone == phone:
                return a
        return None

    def stats(self) -> dict:
        total = len(self._all)
        return {
            "total": total,
            "usable": len(self.usable),
            "connected": len(self.connected),
            "flood_blocked": len(self.flood_blocked),
            "banned": len(self.banned),
            "disconnected": len(self.disconnected),
        }


# ---------------------------------------------------------------------------
# Health check loop
# ---------------------------------------------------------------------------


class HealthCheckLoop:
    """
    Background coroutine that periodically health-probes all connected
    accounts and triggers reconnect for unhealthy ones.
    """

    def __init__(
        self,
        pool: AccountPool,
        *,
        interval: float = HEALTH_CHECK_INTERVAL_SEC,
        flood_guard: Optional["FloodGuard"] = None,
        on_ban: Optional[Callable[[Account], None]] = None,
        on_recover: Optional[Callable[[Account], None]] = None,
        on_flood: Optional[Callable[[Account, float], None]] = None,
    ) -> None:
        self._pool = pool
        self._interval = interval
        self._flood_guard = flood_guard
        self._on_ban = on_ban
        self._on_recover = on_recover
        self._on_flood = on_flood
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="health_check_loop")
        logger.info("HealthCheckLoop started (interval=%.1fs)", self._interval)

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("HealthCheckLoop stopped")

    async def _loop(self) -> None:
        while self._running:
            try:
                await self._run_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("HealthCheckLoop error: %s", e)
            await asyncio.sleep(self._interval)

    async def _run_once(self) -> None:
        accounts = self._pool.connected
        if not accounts:
            return

        logger.debug("Health check: probing %d accounts", len(accounts))
        sem = asyncio.Semaphore(5)

        async def _probe(acc: Account) -> None:
            async with sem:
                was_ok = acc.state.health == HealthStatus.OK
                status = await health_check(acc, flood_guard=self._flood_guard)

                if status == HealthStatus.OK and not was_ok:
                    logger.info("%s: recovered (health OK)", acc.phone)
                    if self._on_recover:
                        self._on_recover(acc)

                elif status == HealthStatus.BANNED:
                    logger.warning("%s: banned — removing from pool", acc.phone)
                    if self._on_ban:
                        self._on_ban(acc)

                elif status == HealthStatus.FLOOD:
                    remaining = acc.state.flood_remaining
                    logger.warning(
                        "%s: flood active, %.1fs remaining", acc.phone, remaining
                    )
                    if self._on_flood:
                        self._on_flood(acc, remaining)

                elif status in (HealthStatus.TIMEOUT, HealthStatus.NETWORK_ERROR):
                    logger.warning(
                        "%s: unhealthy (%s) — triggering reconnect",
                        acc.phone, status.name
                    )
                    asyncio.create_task(
                        reconnect_account(acc, flood_guard=self._flood_guard)
                    )

        await asyncio.gather(*[_probe(a) for a in accounts])


# ---------------------------------------------------------------------------
# Proxy assignment
# ---------------------------------------------------------------------------


async def assign_proxies_from_manager(
    accounts: List[Account],
    proxy_manager: "ProxyManager",
) -> int:
    """
    Pull one proxy per account from ProxyManager and wire it into
    the account's ProxyConfig.  Returns the number of accounts updated.
    """
    updated = 0
    for acc in accounts:
        entry = await proxy_manager.acquire()
        if entry is None:
            logger.warning("%s: no proxy available from ProxyManager", acc.phone)
            continue
        acc.proxy = ProxyConfig(
            host=entry.host,
            port=entry.port,
            username=entry.username,
            password=entry.password,
            proxy_type=entry.protocol.value.lower(),
        )
        updated += 1
        logger.debug(
            "%s: assigned proxy %s:%d (%s)",
            acc.phone, entry.host, entry.port, entry.protocol.value
        )
    return updated


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------


def save_account_states(accounts: List[Account], path: str = STATE_FILE) -> None:
    """Persist account metadata (not runtime state) to JSON."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    data = {a.phone: a.meta.to_dict() for a in accounts}
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
    os.replace(tmp, path)
    logger.debug("Account states saved to %s", path)


def load_account_states(accounts: List[Account], path: str = STATE_FILE) -> int:
    """Restore persisted metadata into loaded accounts. Returns count restored."""
    if not os.path.exists(path):
        return 0
    with open(path, encoding="utf-8") as fh:
        data: dict = json.load(fh)
    restored = 0
    for acc in accounts:
        if acc.phone in data:
            acc.meta = AccountMeta.from_dict(data[acc.phone])
            restored += 1
    logger.info("Restored metadata for %d/%d accounts", restored, len(accounts))
    return restored


# ---------------------------------------------------------------------------
# AccountManager — top-level orchestrator
# ---------------------------------------------------------------------------


class AccountManager:
    """
    Top-level orchestrator.

    Usage
    -----
    mgr = AccountManager.from_csv("accounts.csv")
    await mgr.start()
    # ... use mgr.pool, mgr.acquire(), etc.
    await mgr.stop()

    Or as an async context manager:

    async with AccountManager.from_csv("accounts.csv") as mgr:
        acc = await mgr.acquire()
        ...
    """

    def __init__(
        self,
        accounts: List[Account],
        *,
        flood_guard: Optional["FloodGuard"] = None,
        proxy_manager: Optional["ProxyManager"] = None,
        rate_controller: Optional["RateController"] = None,
        concurrency: int = 5,
        health_interval: float = HEALTH_CHECK_INTERVAL_SEC,
        strategy: RotationStrategy = RotationStrategy.ROUND_ROBIN,
        state_file: str = STATE_FILE,
        auto_save: bool = True,
        auto_reconnect: bool = True,
    ) -> None:
        self._raw_accounts = accounts
        self.flood_guard = flood_guard
        self.proxy_manager = proxy_manager
        self.rate_controller = rate_controller
        self._concurrency = concurrency
        self._health_interval = health_interval
        self._strategy = strategy
        self._state_file = state_file
        self._auto_save = auto_save
        self._auto_reconnect = auto_reconnect

        self.pool = AccountPool(accounts, strategy=strategy)
        self._health_loop: Optional[HealthCheckLoop] = None
        self._running = False

        # Event callbacks
        self._on_ban_callbacks: List[Callable[[Account], None]] = []
        self._on_recover_callbacks: List[Callable[[Account], None]] = []
        self._on_flood_callbacks: List[Callable[[Account, float], None]] = []
        self._on_connect_callbacks: List[Callable[[Account], None]] = []
        self._on_disconnect_callbacks: List[Callable[[Account], None]] = []

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_csv(
        cls,
        csv_path: str = DEFAULT_ACCOUNTS_CSV,
        *,
        limit: Optional[int] = None,
        shuffle: bool = False,
        **kwargs,
    ) -> "AccountManager":
        accounts = load_accounts(csv_path, limit=limit, shuffle=shuffle)
        return cls(accounts, **kwargs)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Load states, optionally assign proxies, connect all, start health loop."""
        if self._running:
            return

        # Restore persisted metadata
        load_account_states(self._raw_accounts, self._state_file)

        # Assign proxies from ProxyManager if one is wired in
        if self.proxy_manager:
            count = await assign_proxies_from_manager(
                self._raw_accounts, self.proxy_manager
            )
            logger.info("Assigned proxies to %d accounts via ProxyManager", count)

        # Register all phones with FloodGuard
        if self.flood_guard:
            await self.flood_guard.register_many(
                [a.phone for a in self._raw_accounts]
            )

        # Connect
        connected, failed = await connect_all(
            self._raw_accounts,
            concurrency=self._concurrency,
            flood_guard=self.flood_guard,
            on_connected=self._fire_connect,
            on_failed=self._fire_disconnect,
        )
        logger.info(
            "AccountManager.start: %d/%d connected",
            len(connected), len(self._raw_accounts),
        )

        # Health check loop
        self._health_loop = HealthCheckLoop(
            self.pool,
            interval=self._health_interval,
            flood_guard=self.flood_guard,
            on_ban=self._fire_ban,
            on_recover=self._fire_recover,
            on_flood=self._fire_flood,
        )
        await self._health_loop.start()
        self._running = True

    async def stop(self) -> None:
        """Stop health loop, disconnect all, save states."""
        if not self._running:
            return
        self._running = False

        if self._health_loop:
            await self._health_loop.stop()
            self._health_loop = None

        await disconnect_all(self._raw_accounts)

        if self._auto_save:
            save_account_states(self._raw_accounts, self._state_file)

    async def __aenter__(self) -> "AccountManager":
        await self.start()
        return self

    async def __aexit__(self, *_) -> None:
        await self.stop()

    # ------------------------------------------------------------------
    # Account acquisition
    # ------------------------------------------------------------------

    async def acquire(
        self,
        *,
        wait_timeout: float = 60.0,
        poll_interval: float = 2.0,
    ) -> Optional[Account]:
        """
        Return the next usable account per the pool's rotation strategy.
        Blocks up to wait_timeout seconds if no account is currently usable.
        Returns None on timeout.
        """
        deadline = time.monotonic() + wait_timeout
        while time.monotonic() < deadline:
            acc = await self.pool.next()
            if acc is not None:
                return acc
            await asyncio.sleep(poll_interval)
        logger.warning("acquire: timed out after %.1fs", wait_timeout)
        return None

    async def acquire_many(self, n: int) -> List[Account]:
        """Return up to n usable accounts immediately (no blocking)."""
        return await self.pool.next_batch(n)

    async def mark_flood(
        self,
        acc: Account,
        wait_seconds: int,
        source: str = "",
    ) -> None:
        """
        Record a FloodWait against an account, updating both the account
        state and the FloodGuard (if wired).
        """
        acc.state.status = AccountStatus.FLOOD_WAIT
        acc.state.flood_until = time.monotonic() + wait_seconds
        acc.meta.total_floods += 1
        acc.meta.last_flood_at = time.time()
        if self.flood_guard:
            await self.flood_guard.record_flood(acc.phone, wait_seconds, source=source)
        logger.warning(
            "%s: FloodWait %ds (source=%s)", acc.phone, wait_seconds, source or "?"
        )

    async def mark_success(self, acc: Account) -> None:
        """Record a successful operation — clears flood if applicable."""
        acc.meta.last_active = time.time()
        if acc.state.is_flood_active:
            # Flood is still running; don't clear it
            return
        if acc.state.status == AccountStatus.FLOOD_WAIT:
            acc.state.status = AccountStatus.CONNECTED
        if self.flood_guard:
            await self.flood_guard.record_success(acc.phone)

    async def mark_banned(self, acc: Account) -> None:
        """Mark an account as banned."""
        acc.state.status = AccountStatus.BANNED
        acc.state.health = HealthStatus.BANNED
        acc.meta.ban_count += 1
        logger.error("%s: marked as BANNED", acc.phone)
        self._fire_ban(acc)

    # ------------------------------------------------------------------
    # Rate control integration
    # ------------------------------------------------------------------

    async def check_rate(
        self,
        acc: Account,
        action: str,
        *,
        wait: bool = True,
    ) -> bool:
        """
        Check rate limit for an action via RateController.
        If wait=True, blocks until allowed.  Returns True when allowed.
        """
        if not self.rate_controller:
            return True

        try:
            from infrastructure.rate_controller import ActionType, Priority
            # Map string action to ActionType
            action_map = {
                "join": ActionType.JOIN,
                "react": ActionType.REACT,
                "message": ActionType.MESSAGE,
                "search": ActionType.SEARCH,
            }
            action_type = action_map.get(action.lower())
            if action_type is None:
                return True  # unknown action — pass through

            result = await self.rate_controller.check(
                acc.phone, action_type, priority=Priority.NORMAL
            )
            if result.allowed:
                return True
            if wait and result.wait_seconds > 0:
                logger.debug(
                    "%s: rate limit wait %.1fs for %s",
                    acc.phone, result.wait_seconds, action
                )
                await asyncio.sleep(result.wait_seconds)
                return True
            return result.allowed

        except Exception as e:
            logger.debug("check_rate error: %s", e)
            return True  # fail open

    # ------------------------------------------------------------------
    # Warmup check (ported from auto_join.py, generalized)
    # ------------------------------------------------------------------

    async def warmup_check(self, acc: Account) -> bool:
        """
        Lightweight warmup probe: fetch dialogs to warm the session.
        Returns True if account passes warmup.
        """
        if acc.client is None or not acc.client.is_connected():
            return False
        try:
            async for _ in acc.client.iter_dialogs(limit=5):
                break
            return True
        except FloodWaitError as e:
            await self.mark_flood(acc, e.seconds, source="warmup")
            return False
        except Exception as e:
            logger.warning("%s: warmup failed: %s", acc.phone, e)
            return False

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def report(self) -> dict:
        """Return a full status snapshot."""
        pool_stats = self.pool.stats()
        per_account = []
        for acc in self._raw_accounts:
            per_account.append(
                {
                    "phone": acc.phone,
                    "status": acc.state.status.name,
                    "health": acc.state.health.name,
                    "flood_remaining": round(acc.state.flood_remaining, 1),
                    "total_joins": acc.meta.total_joins,
                    "total_reactions": acc.meta.total_reactions,
                    "total_floods": acc.meta.total_floods,
                    "ban_count": acc.meta.ban_count,
                    "last_active": acc.meta.last_active,
                    "proxy": acc.proxy.proxy_id if acc.proxy.is_set else "none",
                }
            )
        return {
            "pool": pool_stats,
            "accounts": per_account,
        }

    def print_report(self) -> None:
        """Print a human-readable status report to stdout."""
        r = self.report()
        pool = r["pool"]
        print(
            f"\n{'='*60}\n"
            f"AccountManager Report\n"
            f"{'='*60}\n"
            f"  Total:       {pool['total']}\n"
            f"  Usable:      {pool['usable']}\n"
            f"  Connected:   {pool['connected']}\n"
            f"  Flood:       {pool['flood_blocked']}\n"
            f"  Banned:      {pool['banned']}\n"
            f"  Disconnected:{pool['disconnected']}\n"
            f"{'='*60}"
        )
        for a in r["accounts"]:
            fl = f" (flood {a['flood_remaining']}s)" if a["flood_remaining"] > 0 else ""
            print(
                f"  {a['phone']:<18} {a['status']:<15} {a['health']:<15}"
                f" joins={a['total_joins']} reacts={a['total_reactions']}"
                f" floods={a['total_floods']}{fl}"
            )

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------

    def on_ban(self, cb: Callable[[Account], None]) -> None:
        self._on_ban_callbacks.append(cb)

    def on_recover(self, cb: Callable[[Account], None]) -> None:
        self._on_recover_callbacks.append(cb)

    def on_flood(self, cb: Callable[[Account, float], None]) -> None:
        self._on_flood_callbacks.append(cb)

    def on_connect(self, cb: Callable[[Account], None]) -> None:
        self._on_connect_callbacks.append(cb)

    def on_disconnect(self, cb: Callable[[Account], None]) -> None:
        self._on_disconnect_callbacks.append(cb)

    def _fire_ban(self, acc: Account) -> None:
        for cb in self._on_ban_callbacks:
            try:
                cb(acc)
            except Exception:
                pass

    def _fire_recover(self, acc: Account) -> None:
        for cb in self._on_recover_callbacks:
            try:
                cb(acc)
            except Exception:
                pass

    def _fire_flood(self, acc: Account, remaining: float) -> None:
        for cb in self._on_flood_callbacks:
            try:
                cb(acc, remaining)
            except Exception:
                pass

    def _fire_connect(self, acc: Account) -> None:
        for cb in self._on_connect_callbacks:
            try:
                cb(acc)
            except Exception:
                pass

    def _fire_disconnect(self, acc: Account) -> None:
        for cb in self._on_disconnect_callbacks:
            try:
                cb(acc)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Convenience: build from infrastructure components
# ---------------------------------------------------------------------------


async def build_account_manager(
    csv_path: str = DEFAULT_ACCOUNTS_CSV,
    *,
    flood_guard: Optional["FloodGuard"] = None,
    proxy_manager: Optional["ProxyManager"] = None,
    rate_controller: Optional["RateController"] = None,
    concurrency: int = 5,
    health_interval: float = HEALTH_CHECK_INTERVAL_SEC,
    strategy: RotationStrategy = RotationStrategy.ROUND_ROBIN,
    limit: Optional[int] = None,
    shuffle: bool = False,
    auto_start: bool = True,
) -> AccountManager:
    """
    One-shot factory used by auto_join.py and auto_react.py.

    Example
    -------
    from infrastructure import FloodGuard, ProxyManager, RateController
    from account_manager import build_account_manager

    async def main():
        flood_guard = ...
        mgr = await build_account_manager(
            flood_guard=flood_guard,
            concurrency=5,
        )
        async with mgr:
            acc = await mgr.acquire()
            # pass acc.client to join/react code
    """
    accounts = load_accounts(csv_path, limit=limit, shuffle=shuffle)
    mgr = AccountManager(
        accounts,
        flood_guard=flood_guard,
        proxy_manager=proxy_manager,
        rate_controller=rate_controller,
        concurrency=concurrency,
        health_interval=health_interval,
        strategy=strategy,
    )
    if auto_start:
        await mgr.start()
    return mgr


# ---------------------------------------------------------------------------
# Integration shims for auto_join.py and auto_react.py
# ---------------------------------------------------------------------------


async def get_joined_accounts(
    manager: AccountManager,
    channel_username: str,
) -> List[Account]:
    """
    Return subset of usable accounts that are already in `channel_username`.
    Used by auto_react.py to build its reactors list.
    """
    result: List[Account] = []
    sem = asyncio.Semaphore(5)

    async def _check(acc: Account) -> None:
        async with sem:
            if acc.client is None:
                return
            try:
                entity = await acc.client.get_entity(channel_username)
                if entity:
                    result.append(acc)
            except Exception:
                pass

    await asyncio.gather(*[_check(a) for a in manager.pool.usable])
    return result


async def wait_all_flood_clear(
    manager: AccountManager,
    *,
    timeout: float = 600.0,
    poll: float = 5.0,
) -> bool:
    """
    Block until all accounts in the pool are flood-clear,
    or until timeout seconds pass.  Returns True when all clear.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        blocked = manager.pool.flood_blocked
        if not blocked:
            return True
        max_remaining = max(a.state.flood_remaining for a in blocked)
        logger.info(
            "Waiting for flood to clear: %d accounts, max %.1fs",
            len(blocked), max_remaining
        )
        await asyncio.sleep(min(poll, max_remaining + 1))
    return False


# ---------------------------------------------------------------------------
# CSV result writer (shared between auto_join and auto_react)
# ---------------------------------------------------------------------------


def write_results_csv(
    results: Iterable[dict],
    path: str,
    fieldnames: Optional[List[str]] = None,
) -> int:
    """
    Write a list of result dicts to a CSV.

    Each dict can have different keys — the union of all keys is used
    as the header when fieldnames is not supplied.

    Returns the number of rows written.
    """
    rows = list(results)
    if not rows:
        return 0

    if fieldnames is None:
        fieldnames = list({k for row in rows for k in row.keys()})
        fieldnames.sort()

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Wrote %d rows to %s", len(rows), path)
    return len(rows)


# ---------------------------------------------------------------------------
# Human-like delay helper (shared)
# ---------------------------------------------------------------------------


async def human_delay(
    min_sec: float = 1.0,
    max_sec: float = 5.0,
    *,
    mu: Optional[float] = None,
    sigma: float = 0.5,
) -> None:
    """
    Gaussian-distributed delay clamped to [min_sec, max_sec].
    If mu is None, it defaults to the midpoint.
    """
    if mu is None:
        mu = (min_sec + max_sec) / 2.0
    delay = random.gauss(mu, sigma)
    delay = max(min_sec, min(max_sec, delay))
    await asyncio.sleep(delay)


# ---------------------------------------------------------------------------
# Public API surface
# ---------------------------------------------------------------------------

__all__ = [
    # Enums
    "AccountStatus",
    "HealthStatus",
    "RotationStrategy",
    # Data classes
    "ProxyConfig",
    "AccountMeta",
    "SessionState",
    "Account",
    # Loaders
    "load_accounts",
    "load_account_states",
    "save_account_states",
    # Connection
    "connect_account",
    "disconnect_account",
    "reconnect_account",
    "connect_all",
    "disconnect_all",
    # Health
    "health_check",
    "HealthCheckLoop",
    # Pool
    "AccountPool",
    # Manager
    "AccountManager",
    "build_account_manager",
    # Utilities
    "assign_proxies_from_manager",
    "get_joined_accounts",
    "wait_all_flood_clear",
    "write_results_csv",
    "human_delay",
    # Constants
    "API_ID",
    "API_HASH",
    "DEFAULT_ACCOUNTS_CSV",
]