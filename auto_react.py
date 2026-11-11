# auto_react.py

import asyncio
import csv
import random
import time
import threading
import itertools
from account_manager import build_account_manager, get_joined_accounts
from collections import deque
from dataclasses import dataclass, field
from typing import Optional
from reaction_engine import (
    ReactionEngine, ChatConfigRegistry, Priority, QueueInspector
)
from utilities.message_listener import import MessageListener, EventKind

from telethon import TelegramClient, events
from telethon.tl.functions.messages import SendReactionRequest
from telethon.tl.types import ReactionEmoji

# ─── CONFIG ───────────────────────────────────────────────────────────────────

API_ID   = 34674448
API_HASH = "7098ee8e75f3c435f808ee9410a2f05b"

REACTION_EMOJI = "👍"           # emoji to send as reaction
REACTION_MODE: Literal["simultaneous", "staggered"] = "staggered"

# used only when mode = "staggered"
STAGGER_MIN_SEC = 1.0           # minimum delay between each account's reaction
STAGGER_MAX_SEC = 5.0           # maximum delay (randomised per account)

MAX_REACTIONS_PER_HOUR     = 30    # hard ceiling per account per rolling hour
MIN_GAP_BETWEEN_REACTIONS  = 2.0   # minimum seconds between reactions (same account)

# ─── CSV FOLDER ───────────────────────────────────────────────────────────────
# Drop any number of CSV files into this folder.
# auto_react.py will read ALL of them at startup — no need to specify filenames.
# Expected CSV columns: phone, session_name[, proxy_type, proxy_host, proxy_port, proxy_user, proxy_pass]
ACCOUNTS_CSV_DIR = "accounts"   # folder path, relative to this script or absolute
# ──────────────────────────────────────────────────────────────────────────────

mgr = await build_account_manager(flood_guard=flood_guard)
reactors = await get_joined_accounts(mgr, TARGET_CHANNEL)

listener = MessageListener(sender_account.client)
listener.on(EventKind.OUTGOING_MESSAGE, reaction_engine.handle_outgoing)
await listener.start()
await sender_account.client.run_until_disconnected()
await listener.stop()


@dataclass
class RateLimiter:
    """
    Token-bucket style rate limiter.
    max_per_hour  — maximum reactions allowed per rolling 60-minute window.
    min_gap_sec   — minimum seconds between two consecutive reactions.
    Thread-safe; safe to call from asyncio via run_in_executor if needed.
    """

    def __init__(self, max_per_hour: int = 30, min_gap_sec: float = 2.0):
        self.max_per_hour = max_per_hour
        self.min_gap_sec  = min_gap_sec
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

    def _purge_old(self, now: float) -> None:
        cutoff = now - 3600.0
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()

    def allowed(self) -> bool:
        """Return True if a reaction is permitted right now."""
        now = time.monotonic()
        with self._lock:
            self._purge_old(now)
            if len(self._timestamps) >= self.max_per_hour:
                return False
            if self._timestamps and (now - self._timestamps[-1]) < self.min_gap_sec:
                return False
            return True

    def record(self) -> None:
        """Call immediately after a reaction is successfully sent."""
        with self._lock:
            self._timestamps.append(time.monotonic())

    def seconds_until_allowed(self) -> float:
        """How many seconds to wait before the next reaction is permitted."""
        now = time.monotonic()
        with self._lock:
            self._purge_old(now)
            wait_gap   = 0.0
            wait_quota = 0.0
            if self._timestamps:
                gap = (self._timestamps[-1] + self.min_gap_sec) - now
                wait_gap = max(0.0, gap)
            if len(self._timestamps) >= self.max_per_hour:
                oldest = self._timestamps[0]
                wait_quota = max(0.0, (oldest + 3600.0) - now)
            return max(wait_gap, wait_quota)
            
            
# ─── PROXY HELPER ─────────────────────────────────────────────────────────────

from dataclasses import dataclass, field
from typing import Optional

@dataclass
class ProxyConfig:
    """
    Wraps a single proxy entry as Telethon expects it.

    Telethon proxy tuple format:
        (socks.SOCKS5, host, port, True, username, password)
        (socks.HTTP,   host, port, True, username, password)

    Pass None to use no proxy.
    """
    proxy_type: str        # "socks5" | "http" | ""
    host: str = ""
    port: int = 0
    username: str = ""
    password: str = ""

    def to_telethon(self) -> Optional[tuple]:
        import socks  # python-socks dependency bundled with telethon
        if not self.proxy_type:
            return None
        ptype = socks.SOCKS5 if self.proxy_type.lower() == "socks5" else socks.HTTP
        return (
            ptype,
            self.host,
            self.port,
            True,                            # rdns
            self.username or None,
            self.password or None,
        )


# ─── ACCOUNT ──────────────────────────────────────────────────────────────────

@dataclass
class Account:
    phone:   str
    session: str
    proxy:   ProxyConfig              = field(default_factory=lambda: ProxyConfig(""))
    limiter: RateLimiter              = field(default_factory=RateLimiter)
    client:  Optional[TelegramClient] = field(default=None, repr=False)

    # health-check state
    _consecutive_failures: int        = field(default=0, init=False, repr=False)
    MAX_FAILURES: int                 = field(default=5,  init=False, repr=False)

    def is_healthy(self) -> bool:
        return self._consecutive_failures < self.MAX_FAILURES

    def mark_ok(self) -> None:
        self._consecutive_failures = 0

    def mark_fail(self) -> None:
        self._consecutive_failures += 1


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
                limiter = RateLimiter(
                    max_per_hour = MAX_REACTIONS_PER_HOUR,
                    min_gap_sec  = MIN_GAP_BETWEEN_REACTIONS,
                ),
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
        Flat list of Account objects ready for connect_all().
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


async def connect_account(acc: Account) -> bool:
    """
    (Re)connect a single account.
    Returns True on success, False on failure.
    Passes proxy config to TelegramClient if one is defined.
    """
    try:
        proxy_tuple = acc.proxy.to_telethon()
        acc.client = TelegramClient(
            acc.session,
            API_ID,
            API_HASH,
            proxy=proxy_tuple,
        )
        await acc.client.start(phone=acc.phone)
        acc.mark_ok()
        proxy_tag = f" via {acc.proxy.proxy_type}:{acc.proxy.host}" if acc.proxy.proxy_type else ""
        print(f"[+] connected: {acc.phone}{proxy_tag}")
        return True
    except Exception as e:
        acc.mark_fail()
        print(f"[!] connect failed {acc.phone}: {e}")
        return False


async def connect_all(accounts: list[Account]) -> None:
    for acc in accounts:
        await connect_account(acc)


async def health_check_loop(
    accounts: list[Account],
    interval_sec: float = 120.0,
) -> None:
    """
    Background task.
    Every `interval_sec` seconds: ping each account with get_me().
    If ping fails → mark_fail(); if failure count hits MAX_FAILURES → reconnect.
    If reconnect also fails → account stays marked unhealthy (skipped by reactor).
    """
    while True:
        await asyncio.sleep(interval_sec)
        for acc in accounts:
            if acc.client is None:
                continue
            try:
                await asyncio.wait_for(acc.client.get_me(), timeout=15.0)
                was_unhealthy = not acc.is_healthy()
                acc.mark_ok()
                if was_unhealthy:
                    print(f"[~] {acc.phone} recovered")
            except Exception as e:
                acc.mark_fail()
                print(f"[!] health-check fail {acc.phone} "
                      f"(streak={acc._consecutive_failures}): {e}")
                if not acc.is_healthy():
                    print(f"[*] attempting reconnect: {acc.phone}")
                    try:
                        await acc.client.disconnect()
                    except Exception:
                        pass
                    success = await connect_account(acc)
                    if success:
                        print(f"[+] reconnected: {acc.phone}")
                    else:
                        print(f"[!] reconnect failed: {acc.phone} — skipping")   


import itertools

# Emoji pool — accounts cycle through this list in round-robin order.
# Each account remembers its own position independently.
REACTION_POOL: list[str] = ["👍", "❤️", "🔥", "🎉", "👏"]

# Per-account iterator; built once in main() and stored here.
_reaction_iterators: dict[str, itertools.cycle] = {}


def get_next_emoji(acc: Account) -> str:
    """Return the next emoji in the cycle for this account."""
    it = _reaction_iterators.get(acc.phone)
    if it is None:
        it = itertools.cycle(REACTION_POOL)
        _reaction_iterators[acc.phone] = it
    return next(it)


async def send_reaction(acc: Account, chat_id: int, msg_id: int) -> bool:
    """
    Send one reaction from `acc` to `msg_id` in `chat_id`.

    Checks:
      1. Account is healthy (connected, below failure threshold).
      2. Rate limiter permits a reaction right now.

    Returns True on success, False if skipped or failed.
    Handles FloodWaitError by sleeping the required duration then retrying once.
    """
    from telethon.errors import FloodWaitError, UserDeactivatedBanError

    if not acc.is_healthy():
        print(f"    [{acc.phone}] skipped — unhealthy")
        return False

    if not acc.limiter.allowed():
        wait = acc.limiter.seconds_until_allowed()
        print(f"    [{acc.phone}] rate-limited — next in {wait:.0f}s, skipping")
        return False

    emoji = get_next_emoji(acc)

    for attempt in range(2):   # one retry after FloodWait
        try:
            await acc.client(SendReactionRequest(
                peer=chat_id,
                msg_id=msg_id,
                reaction=[ReactionEmoji(emoticon=emoji)],
            ))
            acc.limiter.record()
            acc.mark_ok()
            print(f"    [{acc.phone}] reacted {emoji}")
            return True

        except FloodWaitError as e:
            if attempt == 0:
                wait = e.seconds + random.uniform(1, 5)
                print(f"    [{acc.phone}] FloodWait {e.seconds}s — sleeping {wait:.0f}s")
                await asyncio.sleep(wait)
            else:
                print(f"    [{acc.phone}] FloodWait again after retry — skipping")
                acc.mark_fail()
                return False

        except UserDeactivatedBanError:
            print(f"    [{acc.phone}] BANNED — marking unhealthy permanently")
            acc._consecutive_failures = acc.MAX_FAILURES
            return False

        except Exception as e:
            acc.mark_fail()
            print(f"    [{acc.phone}] error: {e}")
            return False

    return False


async def _react_one(acc: Account, chat_id: int, msg_id: int) -> None:
    """Wrapper used for simultaneous mode — absorbs exceptions."""
    await send_reaction(acc, chat_id, msg_id)


async def react_all(
    accounts: list[Account],
    chat_id: int,
    msg_id: int,
) -> None:
    if REACTION_MODE == "simultaneous":
        await asyncio.gather(*[
            _react_one(acc, chat_id, msg_id)
            for acc in accounts
        ])
    else:  # staggered
        for acc in accounts:
            await send_reaction(acc, chat_id, msg_id)
            delay = random.uniform(STAGGER_MIN_SEC, STAGGER_MAX_SEC)
            await asyncio.sleep(delay)


HEALTH_CHECK_INTERVAL = 120.0   # seconds between each health-check sweep

async def main() -> None:
    accounts = load_accounts(ACCOUNTS_CSV_DIR)
    await connect_all(accounts)
    
    registry = ChatConfigRegistry.load("chat_configs.json")
    engine   = ReactionEngine(
        accounts     = reactors,
        registry     = registry,
        concurrency  = 5,
        worker_count = 3,
    )
    asyncio.create_task(engine.run())
    
    inspector = QueueInspector(engine)

    # initialise per-account emoji iterators
    for acc in accounts:
        _reaction_iterators[acc.phone] = itertools.cycle(REACTION_POOL)

    sender   = accounts[0]
    reactors = accounts[1:]   # or: reactors = accounts  to include the sender

    # start health-check background task
    asyncio.create_task(health_check_loop(accounts, HEALTH_CHECK_INTERVAL))

    print(f"\n[*] listening for outgoing messages on {sender.phone} ...")
    print(f"    reaction pool : {REACTION_POOL}")
    print(f"    mode          : {REACTION_MODE}")
    print(f"    max/hour      : {MAX_REACTIONS_PER_HOUR}")
    print(f"    health-check  : every {HEALTH_CHECK_INTERVAL}s\n")

    @sender.client.on(events.NewMessage(outgoing=True))
    async def on_send(event):
        await engine.enqueue(
            chat_id  = event.chat_id,
            msg_id   = event.message.id,
            priority = Priority.NORMAL,
        )

    await sender.client.run_until_disconnected()
    registry.blacklist(chat_id=-1001234567890, reason="spam")
    await engine.enqueue(chat_id=chat_id, msg_id=reply_id, priority=Priority.HIGH)
    inspector.print_report()


if __name__ == "__main__":
    asyncio.run(main())