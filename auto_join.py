# auto_join.py

import asyncio
import csv
import random
import time
import logging
import threading
import statistics
from account_manager import build_account_manager
from dataclasses import dataclass, field
from collections import defaultdict
from typing import Optional, Literal
from enum import Enum, auto

from telethon import TelegramClient
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.errors import UserAlreadyParticipantError, InviteHashExpiredError

# ─── CONFIG ───────────────────────────────────────────────────────────────────

API_ID   = 34674448
API_HASH = "7098ee8e75f3c435f808ee9410a2f05b"

# use either a public username/link  e.g. "t.me/example"  or  "example"
# for private invite links use the hash only: "AbCdEfGhIj..."
TARGET_CHANNEL = "https://t.me/+gupMRZzM0RZkZDll"

JOIN_DELAY_MIN = 2.0   # seconds between each join (avoid flood wait)
JOIN_DELAY_MAX = 8.0

# ─── CSV FOLDER ───────────────────────────────────────────────────────────────
# Drop any number of CSV files into this folder.
# auto_join.py will read ALL of them at startup — no need to specify filenames.
# Expected CSV columns: phone, session_name[, proxy_type, proxy_host, proxy_port, proxy_user, proxy_pass]
ACCOUNTS_CSV_DIR = "accounts"   # folder path, relative to this script or absolute
# ──────────────────────────────────────────────────────────────────────────────

mgr = await build_account_manager(flood_guard=flood_guard, concurrency=3)
async with mgr:
    acc = await mgr.acquire()
    # acc.client ready use


# ─── ENUMS ────────────────────────────────────────────────────────────────────

class JoinStatus(Enum):
    PENDING        = auto()
    JOINED         = auto()
    ALREADY_IN     = auto()
    BANNED         = auto()
    INVITE_EXPIRED = auto()
    FLOOD_WAIT     = auto()
    FAILED         = auto()
    SKIPPED        = auto()   # unhealthy / rate-limited before attempt


class ChannelKind(Enum):
    PUBLIC_USERNAME = auto()   # @username or bare "username"
    INVITE_HASH     = auto()   # t.me/+HASH or t.me/joinchat/HASH
    NUMERIC_ID      = auto()   # integer channel ID


# ─── JOIN RESULT ──────────────────────────────────────────────────────────────

@dataclass
class JoinResult:
    phone:   str
    target:  str
    status:  JoinStatus
    detail:  str  = ""
    ts:      float = field(default_factory=time.time)

    def to_csv_row(self) -> dict:
        return {
            "phone":   self.phone,
            "target":  self.target,
            "status":  self.status.name,
            "detail":  self.detail,
            "ts":      self.ts,
        }


# ─── RATE LIMITER ─────────────────────────────────────────────────────────────

class RateLimiter:
    """
    Rolling-window rate limiter.
    max_per_hour  — joins allowed per 60-minute window.
    min_gap_sec   — minimum seconds between consecutive joins.
    """

    def __init__(self, max_per_hour: int = 10, min_gap_sec: float = 30.0):
        self.max_per_hour = max_per_hour
        self.min_gap_sec  = min_gap_sec
        self._ts: list[float] = []
        self._lock = threading.Lock()

    def _purge(self, now: float) -> None:
        cutoff = now - 3600.0
        self._ts = [t for t in self._ts if t >= cutoff]

    def allowed(self) -> bool:
        now = time.monotonic()
        with self._lock:
            self._purge(now)
            if len(self._ts) >= self.max_per_hour:
                return False
            if self._ts and (now - self._ts[-1]) < self.min_gap_sec:
                return False
            return True

    def record(self) -> None:
        with self._lock:
            self._ts.append(time.monotonic())

    def seconds_until_allowed(self) -> float:
        now = time.monotonic()
        with self._lock:
            self._purge(now)
            wait_gap = wait_quota = 0.0
            if self._ts:
                wait_gap = max(0.0, self._ts[-1] + self.min_gap_sec - now)
            if len(self._ts) >= self.max_per_hour:
                wait_quota = max(0.0, self._ts[0] + 3600.0 - now)
            return max(wait_gap, wait_quota)


# ─── PROXY CONFIG ─────────────────────────────────────────────────────────────

@dataclass
class ProxyConfig:
    proxy_type: str = ""
    host:       str = ""
    port:       int = 0
    username:   str = ""
    password:   str = ""

    def to_telethon(self) -> Optional[tuple]:
        if not self.proxy_type:
            return None
        import socks
        ptype = socks.SOCKS5 if self.proxy_type.lower() == "socks5" else socks.HTTP
        return (ptype, self.host, self.port, True,
                self.username or None, self.password or None)


# ─── ACCOUNT ──────────────────────────────────────────────────────────────────

@dataclass
class Account:
    phone:   str
    session: str
    proxy:   ProxyConfig               = field(default_factory=lambda: ProxyConfig())
    limiter: RateLimiter               = field(default_factory=RateLimiter)
    client:  Optional[TelegramClient]  = field(default=None, repr=False)

    _failures: int = field(default=0, init=False, repr=False)
    MAX_FAIL:  int = field(default=5,  init=False, repr=False)

    def healthy(self) -> bool:
        return self._failures < self.MAX_FAIL

    def mark_ok(self)   -> None: self._failures = 0
    def mark_fail(self) -> None: self._failures += 1


# ─── CONFIG (rate) ────────────────────────────────────────────────────────────
MAX_JOINS_PER_HOUR  = 8      # Telegram bans hard on rapid join — keep this low
MIN_JOIN_GAP_SEC    = 30.0   # minimum seconds between joins per account
GLOBAL_CONCURRENCY  = 3      # max accounts joining simultaneously
DRY_RUN             = False  # True → simulate everything, never call JoinChannel
# ─────────────────────────────────────────────────────────────────────────────


def _load_accounts_from_file(path: str) -> list[Account]:
    """Read one CSV file and return a list of Account objects."""
    accounts = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            proxy = ProxyConfig(
                proxy_type = row.get("proxy_type", "").strip(),
                host       = row.get("proxy_host", "").strip(),
                port       = int(row["proxy_port"]) if row.get("proxy_port", "").strip() else 0,
                username   = row.get("proxy_user", "").strip(),
                password   = row.get("proxy_pass", "").strip(),
            )
            accounts.append(Account(
                phone   = row["phone"].strip(),
                session = row["session_name"].strip(),
                proxy   = proxy,
                limiter = RateLimiter(MAX_JOINS_PER_HOUR, MIN_JOIN_GAP_SEC),
            ))
    return accounts


def load_accounts(folder: str = ACCOUNTS_CSV_DIR) -> list[Account]:
    """
    Read every *.csv file inside *folder* and merge into one account list.
    Duplicate phone numbers (same phone in multiple files) are de-duplicated —
    the first occurrence wins.

    Args:
        folder: Path to the directory that contains account CSV files.
                Create the folder yourself and drop any number of .csv files in.
                Sub-directories are ignored; only top-level *.csv files are read.

    Returns:
        Flat list of Account objects ready for the join worker pool.
    """
    import os, glob

    folder_path = os.path.abspath(folder)
    if not os.path.isdir(folder_path):
        raise FileNotFoundError(
            f"[load_accounts] CSV folder not found: {folder_path!r}\n"
            f"Create the folder and place your .csv account files inside it."
        )

    csv_files = sorted(glob.glob(os.path.join(folder_path, "*.csv")))
    if not csv_files:
        raise FileNotFoundError(
            f"[load_accounts] No .csv files found in: {folder_path!r}\n"
            f"Drop at least one account CSV file into that folder."
        )

    all_accounts: list[Account] = []
    seen_phones: set[str] = set()
    total_dupes = 0

    for csv_path in csv_files:
        try:
            batch = _load_accounts_from_file(csv_path)
        except Exception as e:
            logging.warning("[load_accounts] Skipping %s — %s", csv_path, e)
            continue

        for acc in batch:
            if acc.phone in seen_phones:
                total_dupes += 1
                continue
            seen_phones.add(acc.phone)
            all_accounts.append(acc)

        logging.info(
            "[load_accounts] %s → %d accounts (cumulative: %d)",
            os.path.basename(csv_path), len(batch), len(all_accounts),
        )

    if total_dupes:
        logging.warning(
            "[load_accounts] %d duplicate phone(s) removed across files", total_dupes
        )

    logging.info(
        "[load_accounts] Done. %d total accounts loaded from %d file(s) in %r",
        len(all_accounts), len(csv_files), folder_path,
    )
    return all_accounts


# ─── CHANNEL TYPE RESOLVER ────────────────────────────────────────────────────

def resolve_channel_kind(target: str) -> tuple[ChannelKind, str]:
    """
    Returns (kind, normalised_value).

    Rules:
      t.me/+HASH  or  t.me/joinchat/HASH  →  INVITE_HASH, raw hash
      purely numeric string               →  NUMERIC_ID,  as-is
      anything else                       →  PUBLIC_USERNAME, stripped @
    """
    t = target.strip()

    # private invite links
    for prefix in ("https://t.me/+", "http://t.me/+",
                   "https://t.me/joinchat/", "http://t.me/joinchat/"):
        if t.startswith(prefix):
            return ChannelKind.INVITE_HASH, t[len(prefix):]

    # bare +HASH shorthand
    if t.startswith("+") and len(t) > 5 and not t[1:].isdigit():
        return ChannelKind.INVITE_HASH, t[1:]

    # numeric channel ID
    if t.lstrip("-").isdigit():
        return ChannelKind.NUMERIC_ID, t

    # public username — strip leading @ or t.me/
    for prefix in ("https://t.me/", "http://t.me/", "@"):
        if t.startswith(prefix):
            t = t[len(prefix):]
    return ChannelKind.PUBLIC_USERNAME, t


async def fetch_channel_info(client: TelegramClient, target: str) -> dict:
    """
    Returns a dict with channel metadata before joining.
    Keys: title, username, member_count, kind (broadcast/megagroup/group), id
    Returns {} on failure.
    """
    from telethon.tl.functions.channels import GetFullChannelRequest
    from telethon.tl.types import Channel, Chat

    try:
        entity = await client.get_entity(target)
        full   = await client(GetFullChannelRequest(entity))
        ch     = full.chats[0]

        kind_str = "broadcast" if getattr(ch, "broadcast", False) \
              else "megagroup" if getattr(ch, "megagroup", False) \
              else "group"

        return {
            "title":        getattr(ch, "title", ""),
            "username":     getattr(ch, "username", ""),
            "member_count": getattr(full.full_chat, "participants_count", 0),
            "kind":         kind_str,
            "id":           ch.id,
        }
    except Exception as e:
        log.warning(f"fetch_channel_info failed for {target!r}: {e}")
        return {}


# ─── HUMAN-DELAY SIMULATOR ────────────────────────────────────────────────────

def human_delay(base_min: float, base_max: float) -> float:
    """
    Returns a delay sampled from a Gaussian distribution clamped to
    [base_min, base_max * 2].  Mean = midpoint, sigma = range / 4.
    Looks more organic than pure uniform random.
    """
    mid   = (base_min + base_max) / 2
    sigma = (base_max - base_min) / 4
    raw   = random.gauss(mid, sigma)
    return max(base_min, min(base_max * 2, raw))


# ─── STRUCTURED LOGGER ────────────────────────────────────────────────────────

logging.basicConfig(
    level    = logging.INFO,
    format   = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt  = "%Y-%m-%d %H:%M:%S",
    handlers = [
        logging.StreamHandler(),
        logging.FileHandler("auto_join.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("auto_join")


# ─── ACCOUNT WARM-UP CHECK ────────────────────────────────────────────────────

async def warmup_check(acc: Account) -> bool:
    """
    Before joining, verify the account is live:
      1. get_me() — confirms the session is authenticated.
      2. check that the account is not restricted/banned via me.restricted.
    Returns True if the account passes, False if it should be skipped.
    """
    try:
        me = await asyncio.wait_for(acc.client.get_me(), timeout=15.0)
        if me is None:
            log.warning(f"[{acc.phone}] get_me() returned None")
            acc.mark_fail()
            return False
        if getattr(me, "restricted", False):
            log.warning(f"[{acc.phone}] account restricted — skipping")
            acc._failures = acc.MAX_FAIL
            return False
        acc.mark_ok()
        return True
    except Exception as e:
        log.warning(f"[{acc.phone}] warmup check failed: {e}")
        acc.mark_fail()
        return False


# ─── RETRY JOIN (single channel, single account) ──────────────────────────────

RETRY_MAX       = 3          # maximum attempts per (account, channel) pair
RETRY_BASE_WAIT = 5.0        # base seconds for exponential backoff


async def retry_join(
    acc:    Account,
    target: str,
    kind:   ChannelKind,
) -> JoinResult:
    """
    Attempt to join `target` from `acc` with exponential backoff on transient
    errors.  Respects rate-limiter and dry-run mode.

    Returns a JoinResult capturing the final outcome.
    """
    from telethon.errors import (
        FloodWaitError,
        UserAlreadyParticipantError,
        InviteHashExpiredError,
        UserDeactivatedBanError,
        ChannelPrivateError,
        ChatWriteForbiddenError,
    )

    if not acc.healthy():
        log.info(f"[{acc.phone}] skipped {target!r} — unhealthy")
        return JoinResult(acc.phone, target, JoinStatus.SKIPPED, "unhealthy")

    if not acc.limiter.allowed():
        wait = acc.limiter.seconds_until_allowed()
        log.info(f"[{acc.phone}] rate-limited — {wait:.0f}s until next join allowed")
        return JoinResult(acc.phone, target, JoinStatus.SKIPPED,
                          f"rate-limited ({wait:.0f}s)")

    if DRY_RUN:
        log.info(f"[DRY-RUN] {acc.phone} would join {target!r} ({kind.name})")
        return JoinResult(acc.phone, target, JoinStatus.JOINED, "dry-run")

    for attempt in range(1, RETRY_MAX + 1):
        try:
            if kind == ChannelKind.INVITE_HASH:
                await acc.client(ImportChatInviteRequest(target))
            else:
                await acc.client(JoinChannelRequest(target))

            acc.limiter.record()
            acc.mark_ok()
            log.info(f"[{acc.phone}] joined {target!r} (attempt {attempt})")
            return JoinResult(acc.phone, target, JoinStatus.JOINED,
                              f"attempt {attempt}")

        except UserAlreadyParticipantError:
            log.info(f"[{acc.phone}] already in {target!r}")
            return JoinResult(acc.phone, target, JoinStatus.ALREADY_IN)

        except InviteHashExpiredError:
            log.warning(f"[{acc.phone}] invite expired: {target!r}")
            return JoinResult(acc.phone, target, JoinStatus.INVITE_EXPIRED,
                              "hash expired")

        except UserDeactivatedBanError:
            log.error(f"[{acc.phone}] BANNED")
            acc._failures = acc.MAX_FAIL
            return JoinResult(acc.phone, target, JoinStatus.BANNED, "deactivated")

        except (ChannelPrivateError, ChatWriteForbiddenError) as e:
            log.warning(f"[{acc.phone}] access denied: {e}")
            return JoinResult(acc.phone, target, JoinStatus.FAILED, str(e))

        except FloodWaitError as e:
            wait = e.seconds + random.uniform(2, 8)
            log.warning(f"[{acc.phone}] FloodWait {e.seconds}s — sleeping {wait:.0f}s "
                        f"(attempt {attempt}/{RETRY_MAX})")
            if attempt < RETRY_MAX:
                await asyncio.sleep(wait)
            else:
                return JoinResult(acc.phone, target, JoinStatus.FLOOD_WAIT,
                                  f"gave up after {RETRY_MAX} attempts")

        except Exception as e:
            backoff = RETRY_BASE_WAIT * (2 ** (attempt - 1)) + random.uniform(0, 3)
            log.warning(f"[{acc.phone}] error (attempt {attempt}): {e} — "
                        f"retry in {backoff:.1f}s")
            acc.mark_fail()
            if attempt < RETRY_MAX:
                await asyncio.sleep(backoff)
            else:
                return JoinResult(acc.phone, target, JoinStatus.FAILED, str(e))

    return JoinResult(acc.phone, target, JoinStatus.FAILED, "exhausted retries")
    
    
# ─── CONNECT HELPERS ──────────────────────────────────────────────────────────

async def connect_account(acc: Account) -> bool:
    try:
        proxy = acc.proxy.to_telethon()
        acc.client = TelegramClient(acc.session, API_ID, API_HASH, proxy=proxy)
        await acc.client.start(phone=acc.phone)
        acc.mark_ok()
        tag = f" via {acc.proxy.proxy_type}:{acc.proxy.host}" if acc.proxy.proxy_type else ""
        log.info(f"connected: {acc.phone}{tag}")
        return True
    except Exception as e:
        acc.mark_fail()
        log.error(f"connect failed {acc.phone}: {e}")
        return False


async def connect_all(accounts: list[Account]) -> None:
    for acc in accounts:
        await connect_account(acc)


HEALTH_CHECK_INTERVAL = 90.0   # seconds between sweeps

async def health_check_loop(accounts: list[Account]) -> None:
    """Background task — pings each account, reconnects dead sessions."""
    while True:
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)
        for acc in accounts:
            if acc.client is None:
                continue
            try:
                await asyncio.wait_for(acc.client.get_me(), timeout=15.0)
                was_dead = not acc.healthy()
                acc.mark_ok()
                if was_dead:
                    log.info(f"[{acc.phone}] session recovered")
            except Exception as e:
                acc.mark_fail()
                log.warning(f"[{acc.phone}] health ping failed "
                            f"(streak={acc._failures}): {e}")
                if not acc.healthy():
                    log.info(f"[{acc.phone}] reconnecting ...")
                    try:
                        await acc.client.disconnect()
                    except Exception:
                        pass
                    ok = await connect_account(acc)
                    if not ok:
                        log.error(f"[{acc.phone}] reconnect failed — marked unhealthy")
                        
                    
# ─── BATCH JOIN ───────────────────────────────────────────────────────────────

async def batch_join(
    accounts:  list[Account],
    targets:   list[str],
    semaphore: asyncio.Semaphore,
) -> list[JoinResult]:
    """
    Join every target in `targets` for every account in `accounts`.
    Uses a semaphore to cap simultaneous active joins at GLOBAL_CONCURRENCY.
    Each account runs a warmup check before its first join in this batch.
    """
    all_results: list[JoinResult] = []
    warmup_cache: dict[str, bool] = {}   # phone → passed warmup?

    async def _join_one(acc: Account, target: str, kind: ChannelKind) -> JoinResult:
        async with semaphore:
            # warmup once per account per batch run
            if acc.phone not in warmup_cache:
                passed = await warmup_check(acc)
                warmup_cache[acc.phone] = passed
            if not warmup_cache[acc.phone]:
                return JoinResult(acc.phone, target, JoinStatus.SKIPPED,
                                  "failed warmup")
            result = await retry_join(acc, target, kind)
            # stagger between accounts inside the semaphore window
            delay = human_delay(JOIN_DELAY_MIN, JOIN_DELAY_MAX)
            await asyncio.sleep(delay)
            return result

    resolved = [(t, *resolve_channel_kind(t)) for t in targets]
    # resolved = [(original_target, kind, normalised_value), ...]

    tasks = []
    for acc in accounts:
        for original, kind, normalised in resolved:
            tasks.append(_join_one(acc, normalised, kind))

    results = await asyncio.gather(*tasks, return_exceptions=False)
    all_results.extend(results)
    return all_results


# ─── SUMMARY REPORT ───────────────────────────────────────────────────────────

def print_summary(results: list[JoinResult]) -> None:
    """Print a formatted table of join results grouped by status."""
    from collections import Counter

    total   = len(results)
    counter: Counter[JoinStatus] = Counter(r.status for r in results)

    # per-account breakdown
    by_phone: dict[str, Counter] = defaultdict(Counter)
    for r in results:
        by_phone[r.phone][r.status] += 1

    print("\n" + "─" * 60)
    print(f"{'SUMMARY':^60}")
    print("─" * 60)
    print(f"  total attempts : {total}")
    for status, count in sorted(counter.items(), key=lambda x: x[1], reverse=True):
        bar = "█" * min(count, 40)
        print(f"  {status.name:<18} {count:>4}  {bar}")

    print("\n  per-account breakdown:")
    print(f"  {'phone':<18} {'joined':>7} {'already':>7} "
          f"{'skipped':>8} {'failed':>7} {'banned':>7}")
    print("  " + "─" * 58)
    for phone, cnt in sorted(by_phone.items()):
        print(f"  {phone:<18} "
              f"{cnt[JoinStatus.JOINED]:>7} "
              f"{cnt[JoinStatus.ALREADY_IN]:>7} "
              f"{cnt[JoinStatus.SKIPPED]:>8} "
              f"{cnt[JoinStatus.FAILED]:>7} "
              f"{cnt[JoinStatus.BANNED]:>7}")
    print("─" * 60 + "\n")


def write_csv_results(results: list[JoinResult], path: str = "join_results.csv") -> None:
    """Export all JoinResult entries to a CSV file."""
    if not results:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["phone", "target", "status",
                                               "detail", "ts"])
        writer.writeheader()
        for r in results:
            writer.writerow(r.to_csv_row())
    log.info(f"results written to {path}")
                        
                        
# ─── BATCH JOIN ───────────────────────────────────────────────────────────────

async def batch_join(
    accounts:  list[Account],
    targets:   list[str],
    semaphore: asyncio.Semaphore,
) -> list[JoinResult]:
    """
    Join every target in `targets` for every account in `accounts`.
    Uses a semaphore to cap simultaneous active joins at GLOBAL_CONCURRENCY.
    Each account runs a warmup check before its first join in this batch.
    """
    all_results: list[JoinResult] = []
    warmup_cache: dict[str, bool] = {}   # phone → passed warmup?

    async def _join_one(acc: Account, target: str, kind: ChannelKind) -> JoinResult:
        async with semaphore:
            # warmup once per account per batch run
            if acc.phone not in warmup_cache:
                passed = await warmup_check(acc)
                warmup_cache[acc.phone] = passed
            if not warmup_cache[acc.phone]:
                return JoinResult(acc.phone, target, JoinStatus.SKIPPED,
                                  "failed warmup")
            result = await retry_join(acc, target, kind)
            # stagger between accounts inside the semaphore window
            delay = human_delay(JOIN_DELAY_MIN, JOIN_DELAY_MAX)
            await asyncio.sleep(delay)
            return result

    resolved = [(t, *resolve_channel_kind(t)) for t in targets]
    # resolved = [(original_target, kind, normalised_value), ...]

    tasks = []
    for acc in accounts:
        for original, kind, normalised in resolved:
            tasks.append(_join_one(acc, normalised, kind))

    results = await asyncio.gather(*tasks, return_exceptions=False)
    all_results.extend(results)
    return all_results


# ─── SUMMARY REPORT ───────────────────────────────────────────────────────────

def print_summary(results: list[JoinResult]) -> None:
    """Print a formatted table of join results grouped by status."""
    from collections import Counter

    total   = len(results)
    counter: Counter[JoinStatus] = Counter(r.status for r in results)

    # per-account breakdown
    by_phone: dict[str, Counter] = defaultdict(Counter)
    for r in results:
        by_phone[r.phone][r.status] += 1

    print("\n" + "─" * 60)
    print(f"{'SUMMARY':^60}")
    print("─" * 60)
    print(f"  total attempts : {total}")
    for status, count in sorted(counter.items(), key=lambda x: x[1], reverse=True):
        bar = "█" * min(count, 40)
        print(f"  {status.name:<18} {count:>4}  {bar}")

    print("\n  per-account breakdown:")
    print(f"  {'phone':<18} {'joined':>7} {'already':>7} "
          f"{'skipped':>8} {'failed':>7} {'banned':>7}")
    print("  " + "─" * 58)
    for phone, cnt in sorted(by_phone.items()):
        print(f"  {phone:<18} "
              f"{cnt[JoinStatus.JOINED]:>7} "
              f"{cnt[JoinStatus.ALREADY_IN]:>7} "
              f"{cnt[JoinStatus.SKIPPED]:>8} "
              f"{cnt[JoinStatus.FAILED]:>7} "
              f"{cnt[JoinStatus.BANNED]:>7}")
    print("─" * 60 + "\n")


def write_csv_results(results: list[JoinResult], path: str = "join_results.csv") -> None:
    """Export all JoinResult entries to a CSV file."""
    if not results:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["phone", "target", "status",
                                               "detail", "ts"])
        writer.writeheader()
        for r in results:
            writer.writerow(r.to_csv_row())
    log.info(f"results written to {path}")


# TARGET_CHANNELS can be a single string or a list of targets
TARGET_CHANNELS: list[str] = [
    TARGET_CHANNEL,   # keep the existing single-target config
    # "another_channel",
]


async def main() -> None:
    accounts = load_accounts(ACCOUNTS_CSV_DIR)
    await connect_all(accounts)

    # print channel info before starting (uses first healthy account)
    probe = next((a for a in accounts if a.healthy() and a.client), None)
    if probe:
        for t in TARGET_CHANNELS:
            kind, normalised = resolve_channel_kind(t)
            if kind != ChannelKind.INVITE_HASH:
                info = await fetch_channel_info(probe.client, normalised)
                if info:
                    log.info(f"target: {info['title']!r}  "
                             f"members={info['member_count']}  "
                             f"kind={info['kind']}")

    # start health-check loop in background
    asyncio.create_task(health_check_loop(accounts))

    sem     = asyncio.Semaphore(GLOBAL_CONCURRENCY)
    results = await batch_join(accounts, TARGET_CHANNELS, sem)

    # disconnect all sessions cleanly
    for acc in accounts:
        if acc.client:
            try:
                await acc.client.disconnect()
            except Exception:
                pass

    print_summary(results)
    write_csv_results(results)

    if DRY_RUN:
        log.info("dry-run complete — no joins were made")
    else:
        joined = sum(1 for r in results if r.status == JoinStatus.JOINED)
        log.info(f"done — {joined}/{len(results)} joins successful")


if __name__ == "__main__":
    asyncio.run(main())