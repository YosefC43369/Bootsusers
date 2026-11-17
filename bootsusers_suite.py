# bootsusers_suite.py
# ==============================================================================
#  ██████╗  ██████╗  ██████╗ ████████╗███████╗██╗   ██╗███████╗███████╗██████╗
#  ██╔══██╗██╔═══██╗██╔═══██╗╚══██╔══╝██╔════╝██║   ██║██╔════╝██╔════╝██╔══██╗
#  ██████╔╝██║   ██║██║   ██║   ██║   ███████╗██║   ██║███████╗█████╗  ██████╔╝
#  ██╔══██╗██║   ██║██║   ██║   ██║   ╚════██║██║   ██║╚════██║██╔══╝  ██╔══██╗
#  ██████╔╝╚██████╔╝╚██████╔╝   ██║   ███████║╚██████╔╝███████║███████╗██║  ██║
#  ╚═════╝  ╚═════╝  ╚═════╝    ╚═╝   ╚══════╝ ╚═════╝ ╚══════╝╚══════╝╚═╝  ╚═╝
# ------------------------------------------------------------------------------
#  BOOTSUSERS // TELEGRAM OPS SUITE — single-file edition
#
#  Everything in one file:
#     • JOIN   — mass-join channels / private invite links
#     • REACT  — emoji reactions from the account pool
#     • STARS  — paid "star" reactions (Telegram Stars)
#     • VIEWS  — channel message view-count booster
#
#  Ships with a dark cybersecurity GUI (neon-green / matrix theme, Hack font).
#  The GUI needs `customtkinter`; the operational core runs headless too
#  (see `--cli`), so this module imports cleanly on a server with no display.
#
#  Install deps:   python install_dependencies.py       (or: pip install -r requirements.txt)
#  Run GUI:        python bootsusers_suite.py
#  Run headless:   python bootsusers_suite.py --cli views --channel @c --message latest --count 50
#  Build .exe:     pyinstaller --onefile --windowed --name Bootsusers bootsusers_suite.py
#
#  Accounts: drop .csv files (comma/semicolon/tab/pipe) into the accounts folder.
#  Recognised columns (tolerant aliases): phone, session_name, id/user_id,
#  email, proxy_type, proxy_host, proxy_port, proxy_user, proxy_pass.
#  Only `phone` is required; a missing session name is derived from the phone.
# ==============================================================================

from __future__ import annotations

import argparse
import asyncio
import csv
import glob
import logging
import os
import random
import re
import sys
import threading
import time
import queue as std_queue
from collections import Counter, deque
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Callable, Optional

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    RPCError,
    UserAlreadyParticipantError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    ChannelPrivateError,
    ChatAdminRequiredError,
    MsgIdInvalidError,
    UserDeactivatedBanError,
    AuthKeyUnregisteredError,
    UserBannedInChannelError,
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import (
    ImportChatInviteRequest,
    SendReactionRequest,
    SendPaidReactionRequest,
    GetMessagesViewsRequest,
)
from telethon.tl.functions.payments import GetStarsStatusRequest
from telethon.tl.types import (
    InputPeerSelf,
    ReactionEmoji,
    PaidReactionPrivacyAnonymous,
    PaidReactionPrivacyDefault,
)

# --- advanced operations (engagement / growth / lifecycle / state) -----------
# Bundled into the .exe; guarded so the core GUI still runs if a package is
# missing when run from source.
try:
    from engagement.ops import op_vote, op_forward, op_comment, op_story
    from growth import op_scrape, op_invite, op_dm, op_dripjoin, load_members
    from lifecycle.warmup import warmup_pool, WarmupConfig
    from orchestration import StateStore
    _ADVANCED = True
except Exception:  # pragma: no cover - keeps the core 4 ops working standalone
    _ADVANCED = False

# ==============================================================================
#  GLOBAL CONFIG
# ==============================================================================

API_ID   = 34674448
API_HASH = "7098ee8e75f3c435f808ee9410a2f05b"

DEFAULT_ACCOUNTS_DIR = "accounts"
CONNECT_TIMEOUT_SEC  = 30.0
FLOOD_MAX_WAIT_SEC   = 300
MAX_RETRIES          = 3
RETRY_BASE_DELAY     = 2.0

log = logging.getLogger("bootsusers")

LEVEL_INFO, LEVEL_OK, LEVEL_WARN, LEVEL_ERR, LEVEL_ACCENT = (
    "info", "ok", "warn", "err", "accent"
)
# A log sink is any callable (level:str, text:str) -> None.
LogFn = Callable[[str, str], None]
StopFn = Callable[[], bool]


def _noop_stop() -> bool:
    return False


# ==============================================================================
#  CORE — rate limiter / proxy / account / loader   (self-contained)
# ==============================================================================

class RateLimiter:
    """Token-bucket: ≤ max_per_hour ops, each ≥ min_gap_sec apart."""

    def __init__(self, max_per_hour: int = 120, min_gap_sec: float = 1.0):
        self.max_per_hour = max_per_hour
        self.min_gap_sec = min_gap_sec
        self._ts: "deque[float]" = deque()
        self._lock = threading.Lock()

    def _purge(self, now: float) -> None:
        cutoff = now - 3600.0
        while self._ts and self._ts[0] < cutoff:
            self._ts.popleft()

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
            gap = quota = 0.0
            if self._ts:
                gap = max(0.0, (self._ts[-1] + self.min_gap_sec) - now)
            if len(self._ts) >= self.max_per_hour:
                quota = max(0.0, (self._ts[0] + 3600.0) - now)
            return max(gap, quota)


@dataclass
class ProxyConfig:
    proxy_type: str = ""
    host: str = ""
    port: int = 0
    username: str = ""
    password: str = ""

    def to_telethon(self) -> Optional[tuple]:
        if not self.proxy_type:
            return None
        import socks
        ptype = socks.SOCKS5 if self.proxy_type.lower() == "socks5" else socks.HTTP
        return (ptype, self.host, self.port, True,
                self.username or None, self.password or None)


@dataclass
class Account:
    phone: str
    session: str
    user_id: str = ""
    email: str = ""
    proxy: ProxyConfig = field(default_factory=ProxyConfig)
    limiter: RateLimiter = field(default_factory=RateLimiter)
    client: Optional[TelegramClient] = field(default=None, repr=False)
    star_balance: int = field(default=-1, repr=False)

    _failures: int = field(default=0, init=False, repr=False)
    MAX_FAIL: int = field(default=5, init=False, repr=False)

    def healthy(self) -> bool:
        return self._failures < self.MAX_FAIL

    def mark_ok(self) -> None:
        self._failures = 0

    def mark_fail(self) -> None:
        self._failures += 1


# --- flexible delimited-file loader ------------------------------------------

_COLS = {
    "phone":   ("phone", "phone_number", "number", "msisdn", "tel", "telephone"),
    "session": ("session_name", "session", "session_file", "name"),
    "user_id": ("id", "user_id", "uid", "tg_id", "telegram_id", "account_id"),
    "email":   ("email", "mail", "email_address", "e-mail"),
    "p_type":  ("proxy_type", "proxytype"),
    "p_host":  ("proxy_host", "proxyhost", "proxy_ip"),
    "p_port":  ("proxy_port", "proxyport"),
    "p_user":  ("proxy_user", "proxyuser", "proxy_username"),
    "p_pass":  ("proxy_pass", "proxypass", "proxy_password"),
}


def _pick(row: dict, keys) -> str:
    for k in keys:
        if k in row and str(row[k]).strip():
            return str(row[k]).strip()
    return ""


def _sanitize_session(phone: str) -> str:
    return "session_" + re.sub(r"[^0-9A-Za-z]", "", phone)


def _sniff_delim(sample: str) -> str:
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
    except Exception:
        return ","


def _load_file(path: str) -> "list[Account]":
    out: "list[Account]" = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        sample = f.read(4096)
        f.seek(0)
        reader = csv.DictReader(f, delimiter=_sniff_delim(sample))
        if not reader.fieldnames:
            return out
        for raw in reader:
            row = {(k or "").strip().lower(): v for k, v in raw.items()}
            phone = _pick(row, _COLS["phone"])
            if not phone:
                continue
            port_s = _pick(row, _COLS["p_port"])
            out.append(Account(
                phone=phone,
                session=_pick(row, _COLS["session"]) or _sanitize_session(phone),
                user_id=_pick(row, _COLS["user_id"]),
                email=_pick(row, _COLS["email"]),
                proxy=ProxyConfig(
                    proxy_type=_pick(row, _COLS["p_type"]),
                    host=_pick(row, _COLS["p_host"]),
                    port=int(port_s) if port_s.isdigit() else 0,
                    username=_pick(row, _COLS["p_user"]),
                    password=_pick(row, _COLS["p_pass"]),
                ),
            ))
    return out


def load_accounts(folder: str = DEFAULT_ACCOUNTS_DIR, log_fn: LogFn = None) -> "list[Account]":
    """Merge every *.csv in *folder*, de-duplicated by phone."""
    def _say(level, msg):
        if log_fn:
            log_fn(level, msg)
    folder_path = os.path.abspath(folder)
    if not os.path.isdir(folder_path):
        raise FileNotFoundError(
            f"account folder not found: {folder_path!r}\n"
            f"Create it and drop your .csv files (id/email/phone) inside."
        )
    # read EVERY file in the folder (any extension), not just *.csv
    files = sorted(p for p in glob.glob(os.path.join(folder_path, "*"))
                   if os.path.isfile(p))
    if not files:
        raise FileNotFoundError(f"no files in: {folder_path!r}")

    merged: "list[Account]" = []
    seen: "set[str]" = set()
    dupes = 0
    for path in files:
        try:
            batch = _load_file(path)
        except Exception as exc:
            _say(LEVEL_WARN, f"skipping {os.path.basename(path)} — {exc}")
            continue
        for acc in batch:
            if acc.phone in seen:
                dupes += 1
                continue
            seen.add(acc.phone)
            merged.append(acc)
        _say(LEVEL_INFO, f"{os.path.basename(path)} → {len(batch)} (total {len(merged)})")
    if dupes:
        _say(LEVEL_WARN, f"removed {dupes} duplicate phone(s)")
    _say(LEVEL_OK, f"{len(merged)} account(s) from {len(files)} file(s)")
    return merged


# ==============================================================================
#  CORE — connection
# ==============================================================================

async def connect_account(acc: Account, log_fn: LogFn) -> bool:
    try:
        acc.client = TelegramClient(
            acc.session, API_ID, API_HASH, proxy=acc.proxy.to_telethon()
        )
        await asyncio.wait_for(acc.client.start(phone=acc.phone),
                               timeout=CONNECT_TIMEOUT_SEC)
        acc.mark_ok()
        tag = f" via {acc.proxy.proxy_type}:{acc.proxy.host}" if acc.proxy.proxy_type else ""
        log_fn(LEVEL_OK, f"connected {acc.phone}{tag}")
        return True
    except (AuthKeyUnregisteredError, UserDeactivatedBanError) as exc:
        acc._failures = acc.MAX_FAIL
        log_fn(LEVEL_ERR, f"{acc.phone} dead/banned: {exc}")
        return False
    except Exception as exc:
        acc.mark_fail()
        log_fn(LEVEL_ERR, f"connect failed {acc.phone}: {exc}")
        return False


async def connect_pool(accounts: "list[Account]", log_fn: LogFn,
                       should_stop: StopFn = _noop_stop, concurrency: int = 8) -> int:
    sem = asyncio.Semaphore(concurrency)
    ok_count = 0

    async def _one(acc: Account):
        nonlocal ok_count
        if should_stop():
            return
        async with sem:
            if should_stop():
                return
            if await connect_account(acc, log_fn):
                ok_count += 1

    await asyncio.gather(*(_one(a) for a in accounts))
    return ok_count


# ==============================================================================
#  CORE — shared resolvers
# ==============================================================================

_MSG_LINK_RE = re.compile(r"(?:https?://)?t\.me/(?:c/\d+|[A-Za-z0-9_]+)/(\d+)")
_INVITE_RE = re.compile(r"(?:t\.me/|telegram\.me/)(?:joinchat/|\+)([\w-]+)")


def parse_message_spec(spec: str) -> "tuple[str, object]":
    spec = spec.strip()
    m = _MSG_LINK_RE.fullmatch(spec)
    if m:
        return "ids", [int(m.group(1))]
    if spec.lower().startswith("latest"):
        _, _, n = spec.partition(":")
        return "latest", max(1, int(n) if n.strip().isdigit() else 1)
    if "," in spec:
        ids = [int(x) for x in spec.split(",") if x.strip().isdigit()]
        if ids:
            return "ids", ids
    if spec.isdigit():
        return "ids", [int(spec)]
    raise ValueError(f"unrecognised message value: {spec!r}")


async def resolve_message_ids(client: TelegramClient, channel, spec: str) -> "list[int]":
    kind, value = parse_message_spec(spec)
    if kind == "ids":
        return list(value)
    msgs = await client.get_messages(channel, limit=int(value))
    ids = [m.id for m in msgs if m is not None]
    if not ids:
        raise ValueError("channel has no readable messages")
    return ids


def classify_join_target(raw: str) -> "tuple[str, object]":
    raw = raw.strip()
    m = _INVITE_RE.search(raw)
    if m:
        return "hash", m.group(1)
    if raw.startswith("+"):
        return "hash", raw[1:]
    m2 = re.search(r"(?:t\.me/|@)([A-Za-z0-9_]+)", raw)
    if m2:
        return "username", m2.group(1)
    if raw.lstrip("-").isdigit():
        return "id", int(raw)
    return "username", raw


async def get_star_balance(acc: Account, log_fn: LogFn) -> int:
    try:
        res = await acc.client(GetStarsStatusRequest(peer=InputPeerSelf()))
        bal = res.balance
        acc.star_balance = int(getattr(bal, "amount", bal))
        return acc.star_balance
    except Exception as exc:
        log_fn(LEVEL_WARN, f"{acc.phone} balance read failed: {exc}")
        acc.star_balance = -1
        return -1


async def read_view_counts(client: TelegramClient, channel, ids: "list[int]") -> dict:
    try:
        res = await client(GetMessagesViewsRequest(peer=channel, id=ids, increment=False))
        return {mid: (v.views or 0) for mid, v in zip(ids, res.views)}
    except Exception:
        return {mid: -1 for mid in ids}


# ==============================================================================
#  OPERATIONS — each is GUI-agnostic: (accounts, params, log_fn, should_stop)
# ==============================================================================

async def op_join(accounts: "list[Account]", channel: str, *, limit: int,
                  log_fn: LogFn, should_stop: StopFn, dry_run: bool = False,
                  gap: "tuple[float, float]" = (2.0, 7.0)) -> Counter:
    kind, value = classify_join_target(channel)
    log_fn(LEVEL_ACCENT, f"JOIN → {channel}  ({kind})")
    stats: Counter = Counter()
    pool = [a for a in accounts if a.client and a.healthy()][:limit or None]
    for acc in pool:
        if should_stop():
            log_fn(LEVEL_WARN, "join stopped by operator")
            break
        if dry_run:
            log_fn(LEVEL_INFO, f"[dry] {acc.phone} would join {channel}")
            stats["dry"] += 1
            continue
        try:
            if kind == "hash":
                await acc.client(ImportChatInviteRequest(value))
            else:
                ent = await acc.client.get_entity(value)
                await acc.client(JoinChannelRequest(ent))
            acc.mark_ok()
            stats["joined"] += 1
            log_fn(LEVEL_OK, f"{acc.phone} joined {channel}")
        except UserAlreadyParticipantError:
            stats["already"] += 1
            log_fn(LEVEL_INFO, f"{acc.phone} already in")
        except (InviteHashExpiredError, InviteHashInvalidError):
            stats["expired"] += 1
            log_fn(LEVEL_ERR, f"{acc.phone} invite expired/invalid")
        except (UserBannedInChannelError, ChatAdminRequiredError, ChannelPrivateError) as e:
            stats["blocked"] += 1
            log_fn(LEVEL_ERR, f"{acc.phone} blocked: {type(e).__name__}")
        except FloodWaitError as e:
            stats["flood"] += 1
            log_fn(LEVEL_WARN, f"{acc.phone} floodwait {e.seconds}s")
            if e.seconds <= FLOOD_MAX_WAIT_SEC and not should_stop():
                await asyncio.sleep(e.seconds)
        except Exception as e:
            acc.mark_fail()
            stats["failed"] += 1
            log_fn(LEVEL_ERR, f"{acc.phone} error: {e}")
        if not should_stop():
            await asyncio.sleep(random.uniform(*gap))
    log_fn(LEVEL_ACCENT, f"JOIN done — {dict(stats)}")
    return stats


async def op_react(accounts: "list[Account]", channel: str, message: str, emoji: str, *,
                   log_fn: LogFn, should_stop: StopFn, dry_run: bool = False,
                   gap: "tuple[float, float]" = (1.0, 4.0)) -> Counter:
    stats: Counter = Counter()
    pool = [a for a in accounts if a.client and a.healthy()]
    if not pool:
        log_fn(LEVEL_ERR, "no connected account")
        return stats
    channel_ent = await pool[0].client.get_input_entity(channel)
    ids = await resolve_message_ids(pool[0].client, channel_ent, message)
    log_fn(LEVEL_ACCENT, f"REACT {emoji} → {channel} msgs {ids}")
    for mid in ids:
        for acc in pool:
            if should_stop():
                log_fn(LEVEL_WARN, "react stopped by operator")
                return stats
            if dry_run:
                stats["dry"] += 1
                continue
            try:
                await acc.client(SendReactionRequest(
                    peer=channel_ent, msg_id=mid, big=False, add_to_recent=True,
                    reaction=[ReactionEmoji(emoticon=emoji)],
                ))
                acc.mark_ok()
                stats["reacted"] += 1
                log_fn(LEVEL_OK, f"{acc.phone} reacted {emoji} on {mid}")
            except FloodWaitError as e:
                stats["flood"] += 1
                log_fn(LEVEL_WARN, f"{acc.phone} floodwait {e.seconds}s")
                if e.seconds <= FLOOD_MAX_WAIT_SEC and not should_stop():
                    await asyncio.sleep(e.seconds)
            except Exception as e:
                acc.mark_fail()
                stats["failed"] += 1
                log_fn(LEVEL_ERR, f"{acc.phone} react error: {e}")
            if not should_stop():
                await asyncio.sleep(random.uniform(*gap))
    log_fn(LEVEL_ACCENT, f"REACT done — {dict(stats)}")
    return stats


async def op_star(accounts: "list[Account]", channel: str, message: str, stars: int, *,
                  log_fn: LogFn, should_stop: StopFn, anonymous: bool = False,
                  clamp: bool = True, dry_run: bool = False,
                  gap: "tuple[float, float]" = (3.0, 9.0)) -> Counter:
    stats: Counter = Counter()
    pool = [a for a in accounts if a.client and a.healthy()]
    if not pool:
        log_fn(LEVEL_ERR, "no connected account")
        return stats
    channel_ent = await pool[0].client.get_input_entity(channel)
    ids = await resolve_message_ids(pool[0].client, channel_ent, message)
    privacy = PaidReactionPrivacyAnonymous() if anonymous else PaidReactionPrivacyDefault()
    log_fn(LEVEL_ACCENT, f"STARS ×{stars} → {channel} msgs {ids}")
    for mid in ids:
        for acc in pool:
            if should_stop():
                log_fn(LEVEL_WARN, "stars stopped by operator")
                return stats
            bal = acc.star_balance
            if bal < 0:
                bal = await get_star_balance(acc, log_fn)
            if bal <= 0:
                stats["no_stars"] += 1
                log_fn(LEVEL_INFO, f"{acc.phone} no stars — skip")
                continue
            send = stars
            if bal < stars:
                if clamp:
                    send = bal
                else:
                    stats["no_stars"] += 1
                    log_fn(LEVEL_INFO, f"{acc.phone} balance {bal}<{stars} — skip")
                    continue
            if dry_run:
                stats["dry"] += 1
                log_fn(LEVEL_INFO, f"[dry] {acc.phone} would send {send}★ (bal {bal})")
                continue
            try:
                await acc.client(SendPaidReactionRequest(
                    peer=channel_ent, msg_id=mid, count=send,
                    random_id=random.randrange(-(2**63), 2**63), private=privacy,
                ))
                acc.mark_ok()
                acc.star_balance = max(0, bal - send)
                stats["sent"] += 1
                stats["stars"] += send
                log_fn(LEVEL_OK, f"{acc.phone} ★{send} on {mid} (bal {bal}→{acc.star_balance})")
            except FloodWaitError as e:
                stats["flood"] += 1
                log_fn(LEVEL_WARN, f"{acc.phone} floodwait {e.seconds}s")
                if e.seconds <= FLOOD_MAX_WAIT_SEC and not should_stop():
                    await asyncio.sleep(e.seconds)
            except RPCError as e:
                msg = (getattr(e, "message", "") or str(e)).upper()
                if "BALANCE" in msg and "LOW" in msg:
                    stats["no_stars"] += 1
                    log_fn(LEVEL_INFO, f"{acc.phone} low balance — skip")
                else:
                    stats["failed"] += 1
                    log_fn(LEVEL_ERR, f"{acc.phone} star error: {e}")
            except Exception as e:
                acc.mark_fail()
                stats["failed"] += 1
                log_fn(LEVEL_ERR, f"{acc.phone} star error: {e}")
            if not should_stop():
                await asyncio.sleep(random.uniform(*gap))
    log_fn(LEVEL_ACCENT, f"STARS done — {dict(stats)}")
    return stats


async def op_views(accounts: "list[Account]", channel: str, message: str, target: int, *,
                   log_fn: LogFn, should_stop: StopFn, dry_run: bool = False,
                   gap: "tuple[float, float]" = (0.8, 3.0)) -> Counter:
    stats: Counter = Counter()
    pool = [a for a in accounts if a.client and a.healthy()]
    if not pool:
        log_fn(LEVEL_ERR, "no connected account")
        return stats
    channel_ent = await pool[0].client.get_input_entity(channel)
    ids = await resolve_message_ids(pool[0].client, channel_ent, message)
    before = await read_view_counts(pool[0].client, channel_ent, ids)

    want = len(pool) if target <= 0 else min(target, len(pool))
    if target > len(pool):
        log_fn(LEVEL_WARN, f"requested {target} views but only {len(pool)} accounts "
                           f"— adding {len(pool)} (1 view/account max)")
    selected = pool[:want]
    log_fn(LEVEL_ACCENT, f"VIEWS +{len(selected)} → {channel} msgs {ids}")

    for acc in selected:
        if should_stop():
            log_fn(LEVEL_WARN, "views stopped by operator")
            break
        if dry_run:
            stats["dry"] += 1
            continue
        try:
            await acc.client(GetMessagesViewsRequest(peer=channel_ent, id=ids, increment=True))
            acc.mark_ok()
            stats["viewed"] += 1
            log_fn(LEVEL_OK, f"{acc.phone} viewed {len(ids)} msg(s)")
        except FloodWaitError as e:
            stats["flood"] += 1
            log_fn(LEVEL_WARN, f"{acc.phone} floodwait {e.seconds}s")
            if e.seconds <= FLOOD_MAX_WAIT_SEC and not should_stop():
                await asyncio.sleep(e.seconds)
        except Exception as e:
            acc.mark_fail()
            stats["failed"] += 1
            log_fn(LEVEL_ERR, f"{acc.phone} view error: {e}")
        if not should_stop():
            await asyncio.sleep(random.uniform(*gap))

    after = before if dry_run else await read_view_counts(pool[0].client, channel_ent, ids)
    for mid in before:
        b, a = before.get(mid, -1), after.get(mid, -1)
        if a >= 0 and b >= 0:
            log_fn(LEVEL_ACCENT, f"msg {mid}: {b} → {a}  (+{max(0, a-b)})")
    log_fn(LEVEL_ACCENT, f"VIEWS done — {dict(stats)}")
    return stats


# ==============================================================================
#  ASYNC RUNNER + LOG BRIDGE   (thread bridge for the GUI)
# ==============================================================================

class LogBridge:
    """Thread-safe (level, text) queue: async side pushes, Tk polls & drains."""

    def __init__(self):
        self._q: "std_queue.Queue[tuple[str, str]]" = std_queue.Queue()

    def push(self, level: str, text: str) -> None:
        ts = time.strftime("%H:%M:%S")
        self._q.put_nowait((level, f"[{ts}] {text}"))

    def drain(self) -> "list[tuple[str, str]]":
        out = []
        try:
            while True:
                out.append(self._q.get_nowait())
        except std_queue.Empty:
            pass
        return out


class AsyncRunner:
    """Persistent asyncio loop in a daemon thread; submit coroutines onto it."""

    def __init__(self):
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever,
                                        daemon=True, name="bootsusers-async")
        self._thread.start()

    def submit(self, coro) -> "asyncio.Future":
        if self._loop is None:
            raise RuntimeError("AsyncRunner not started")
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def stop(self) -> None:
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)


# ==============================================================================
#  THEME  — cybersecurity neon-green / matrix
# ==============================================================================

THEME = {
    "bg":        "#060a07",   # deep green-black
    "panel":     "#0b120d",   # panels / sidebar
    "card":      "#0e1a12",   # raised card
    "card_hi":   "#112417",   # hovered card
    "border":    "#15351f",   # subtle green border
    "grid":      "#0f2016",
    "accent":    "#00ff9c",   # neon green (primary)
    "accent_dk": "#00b870",   # hover
    "accent_lo": "#064d32",   # pressed / track
    "lime":      "#39ff14",   # bright lime
    "text":      "#b8f5d0",   # soft green text
    "text_dim":  "#4e8366",   # dim green
    "text_br":   "#eafff2",   # bright
    "warn":      "#ffd93d",
    "danger":    "#ff5c7a",
    "ok":        "#2bff88",
    "log_bg":    "#040806",
}

LOG_COLORS = {
    LEVEL_INFO:   THEME["text"],
    LEVEL_OK:     THEME["ok"],
    LEVEL_WARN:   THEME["warn"],
    LEVEL_ERR:    THEME["danger"],
    LEVEL_ACCENT: THEME["accent"],
}

BANNER = r"""
  ╔══════════════════════════════════════════════════════════════╗
  ║   B O O T S U S E R S   //   T E L E G R A M   O P S   v2.0   ║
  ║   join · react · stars · views        [ status: ONLINE ]      ║
  ╚══════════════════════════════════════════════════════════════╝
"""


def resource_path(*parts: str) -> Path:
    """
    Resolve a bundled resource both when running from source and when frozen
    by PyInstaller (onefile extracts data into sys._MEIPASS).
    """
    base = getattr(sys, "_MEIPASS", None)
    root = Path(base) if base else Path(__file__).resolve().parent
    return root.joinpath(*parts)


def register_hack_font() -> str:
    """
    Try to make the 'Hack' font available and return the family name to use.
    Loads a bundled assets/Hack-Regular.ttf at runtime on Windows (private
    font) so the GUI looks right even when Hack is not system-installed.
    Falls back to a monospace family that always exists.
    """
    bundled = resource_path("assets", "Hack-Regular.ttf")
    if bundled.exists() and sys.platform.startswith("win"):
        try:
            import ctypes
            FR_PRIVATE = 0x10
            ctypes.windll.gdi32.AddFontResourceExW(str(bundled), FR_PRIVATE, 0)
            return "Hack"
        except Exception:
            pass
    # Probe installed families when Tk is available.
    try:
        import tkinter as tk
        from tkinter import font as tkfont
        _root = tk._default_root or tk.Tk()
        fams = set(tkfont.families(_root))
        for cand in ("Hack", "Hack Nerd Font", "JetBrains Mono", "Fira Code",
                     "Cascadia Mono", "Consolas", "DejaVu Sans Mono",
                     "Courier New", "Courier"):
            if cand in fams:
                return cand
    except Exception:
        pass
    return "Courier New"


# ==============================================================================
#  SHARED GUI STATE
# ==============================================================================

@dataclass
class SuiteState:
    accounts_dir: str = DEFAULT_ACCOUNTS_DIR
    accounts: "list[Account]" = field(default_factory=list)
    connected: int = 0
    with_stars: int = 0
    busy_op: Optional[str] = None            # which operation is running
    stop_flags: dict = field(default_factory=dict)   # op -> threading.Event
    store: object = None                     # optional StateStore (persistence)


# ==============================================================================
#  GUI  (lazy — importing customtkinter only happens here)
# ==============================================================================

def run_gui() -> int:
    try:
        import customtkinter as ctk
    except ImportError:
        print("customtkinter is required for the GUI.\n"
              "Install it:  pip install customtkinter\n"
              "Or run headless:  python bootsusers_suite.py --cli --help")
        return 1

    FONT = register_hack_font()
    runner = AsyncRunner()
    runner.start()
    bridge = LogBridge()
    state = SuiteState()
    if _ADVANCED:
        try:
            state.store = StateStore("data/bootsusers.db")
        except Exception:
            state.store = None

    def gui_log(level: str, text: str) -> None:
        bridge.push(level, text)

    def font(size=12, weight="normal"):
        return ctk.CTkFont(family=FONT, size=size, weight=weight)

    ctk.set_appearance_mode("dark")

    # ── main window ──────────────────────────────────────────────────────────
    app = ctk.CTk()
    app.title("Bootsusers // Telegram Ops")
    app.geometry("1120x760")
    app.minsize(980, 640)
    app.configure(fg_color=THEME["bg"])
    app.grid_columnconfigure(1, weight=1)
    app.grid_rowconfigure(1, weight=1)

    # ── top header banner ────────────────────────────────────────────────────
    header = ctk.CTkFrame(app, fg_color=THEME["panel"], corner_radius=0,
                          height=70, border_width=0)
    header.grid(row=0, column=0, columnspan=2, sticky="ew")
    header.grid_propagate(False)
    header.grid_columnconfigure(1, weight=1)

    logo = ctk.CTkLabel(header, text="◢◤ BOOTSUSERS", font=font(22, "bold"),
                        text_color=THEME["accent"])
    logo.grid(row=0, column=0, padx=24, pady=16, sticky="w")

    sub = ctk.CTkLabel(header, text="telegram ops suite · join / react / stars / views",
                       font=font(12), text_color=THEME["text_dim"])
    sub.grid(row=0, column=1, padx=8, pady=16, sticky="w")

    conn_pill = ctk.CTkLabel(header, text="● OFFLINE", font=font(13, "bold"),
                             text_color=THEME["danger"])
    conn_pill.grid(row=0, column=2, padx=24, pady=16, sticky="e")

    # ── left sidebar nav ─────────────────────────────────────────────────────
    sidebar = ctk.CTkFrame(app, fg_color=THEME["panel"], corner_radius=0, width=210)
    sidebar.grid(row=1, column=0, sticky="nsw")
    sidebar.grid_propagate(False)

    content = ctk.CTkFrame(app, fg_color=THEME["bg"], corner_radius=0)
    content.grid(row=1, column=1, sticky="nsew", padx=0, pady=0)
    content.grid_rowconfigure(1, weight=1)
    content.grid_columnconfigure(0, weight=1)

    # ── account bar (persistent, above the panels) ───────────────────────────
    acc_bar = ctk.CTkFrame(content, fg_color=THEME["card"], corner_radius=10,
                           border_width=1, border_color=THEME["border"])
    acc_bar.grid(row=0, column=0, sticky="ew", padx=16, pady=(16, 8))
    acc_bar.grid_columnconfigure(1, weight=1)

    ctk.CTkLabel(acc_bar, text="ACCOUNTS DIR", font=font(11, "bold"),
                 text_color=THEME["accent"]).grid(row=0, column=0, padx=(14, 8), pady=12)
    dir_entry = ctk.CTkEntry(acc_bar, font=font(12), fg_color=THEME["log_bg"],
                             border_color=THEME["border"], text_color=THEME["text"])
    dir_entry.insert(0, state.accounts_dir)
    dir_entry.grid(row=0, column=1, sticky="ew", padx=8, pady=12)

    stat_lbl = ctk.CTkLabel(acc_bar, text="loaded 0 · live 0 · ★ 0",
                            font=font(12), text_color=THEME["text_dim"])
    stat_lbl.grid(row=0, column=3, padx=14, pady=12)

    def refresh_stats():
        stat_lbl.configure(
            text=f"loaded {len(state.accounts)} · live {state.connected} · ★ {state.with_stars}")
        if state.connected > 0:
            conn_pill.configure(text=f"● ONLINE  ({state.connected})", text_color=THEME["ok"])
        else:
            conn_pill.configure(text="● OFFLINE", text_color=THEME["danger"])

    # forward declaration for buttons that enable/disable on busy
    op_start_buttons: dict = {}

    def set_busy(opname: Optional[str]):
        state.busy_op = opname
        for name, (start_btn, stop_btn) in op_start_buttons.items():
            if opname is None:
                start_btn.configure(state="normal")
                stop_btn.configure(state="disabled")
            elif name == opname:
                start_btn.configure(state="disabled")
                stop_btn.configure(state="normal")
            else:
                start_btn.configure(state="disabled")
                stop_btn.configure(state="disabled")

    def do_load():
        state.accounts_dir = dir_entry.get().strip() or DEFAULT_ACCOUNTS_DIR
        try:
            state.accounts = load_accounts(state.accounts_dir, gui_log)
            state.connected = 0
            state.with_stars = 0
            refresh_stats()
        except Exception as exc:
            gui_log(LEVEL_ERR, str(exc))

    def do_connect():
        if not state.accounts:
            gui_log(LEVEL_WARN, "load accounts first")
            return
        gui_log(LEVEL_ACCENT, f"connecting {len(state.accounts)} account(s) ...")

        async def _run():
            n = await connect_pool(state.accounts, gui_log)
            state.connected = n
            # read star balances so the Stars panel knows who can pay
            live = [a for a in state.accounts if a.client and a.healthy()]
            await asyncio.gather(*(get_star_balance(a, gui_log) for a in live))
            state.with_stars = sum(1 for a in live if a.star_balance > 0)
            gui_log(LEVEL_OK, f"{n} connected · {state.with_stars} hold stars")
            app.after(0, refresh_stats)

        runner.submit(_run())

    load_btn = ctk.CTkButton(acc_bar, text="LOAD", width=84, font=font(12, "bold"),
                             fg_color=THEME["accent_lo"], hover_color=THEME["accent_dk"],
                             text_color=THEME["text_br"], command=do_load)
    load_btn.grid(row=0, column=2, padx=4, pady=12)
    conn_btn = ctk.CTkButton(acc_bar, text="CONNECT", width=96, font=font(12, "bold"),
                             fg_color=THEME["accent"], hover_color=THEME["accent_dk"],
                             text_color="#06130c", command=do_connect)
    conn_btn.grid(row=0, column=4, padx=(4, 14), pady=12)

    # ── panel container (swapped by nav) ─────────────────────────────────────
    panel_host = ctk.CTkFrame(content, fg_color=THEME["bg"], corner_radius=0)
    panel_host.grid(row=1, column=0, sticky="nsew", padx=16, pady=8)
    panel_host.grid_rowconfigure(0, weight=1)
    panel_host.grid_columnconfigure(0, weight=1)

    # ── live log console (bottom) ────────────────────────────────────────────
    log_frame = ctk.CTkFrame(content, fg_color=THEME["card"], corner_radius=10,
                             border_width=1, border_color=THEME["border"], height=200)
    log_frame.grid(row=2, column=0, sticky="ew", padx=16, pady=(8, 16))
    log_frame.grid_propagate(False)
    log_frame.grid_columnconfigure(0, weight=1)
    log_frame.grid_rowconfigure(1, weight=1)
    ctk.CTkLabel(log_frame, text="▌ CONSOLE", font=font(11, "bold"),
                 text_color=THEME["accent"]).grid(row=0, column=0, sticky="w", padx=14, pady=(8, 2))
    console = ctk.CTkTextbox(log_frame, font=font(12), fg_color=THEME["log_bg"],
                             text_color=THEME["text"], border_width=0, wrap="word")
    console.grid(row=1, column=0, sticky="nsew", padx=10, pady=(0, 10))
    for level, color in LOG_COLORS.items():
        console.tag_config(level, foreground=color)
    console.configure(state="disabled")

    def append_log(level: str, text: str):
        console.configure(state="normal")
        console.insert("end", text + "\n", level)
        console.see("end")
        console.configure(state="disabled")

    def poll_logs():
        for level, text in bridge.drain():
            append_log(level, text)
        app.after(120, poll_logs)

    # ── operation panel factory ──────────────────────────────────────────────
    def make_field(parent, label, default="", row=0, width=360):
        ctk.CTkLabel(parent, text=label, font=font(12, "bold"),
                     text_color=THEME["text_dim"]).grid(
            row=row, column=0, sticky="w", padx=6, pady=(10, 2))
        e = ctk.CTkEntry(parent, width=width, font=font(13),
                         fg_color=THEME["log_bg"], border_color=THEME["border"],
                         text_color=THEME["text_br"])
        if default:
            e.insert(0, default)
        e.grid(row=row + 1, column=0, sticky="w", padx=6, pady=(0, 6))
        return e

    def build_op_panel(title, subtitle, builder):
        frame = ctk.CTkScrollableFrame(panel_host, fg_color=THEME["bg"],
                                       corner_radius=0, label_text="")
        frame.grid(row=0, column=0, sticky="nsew")
        card = ctk.CTkFrame(frame, fg_color=THEME["card"], corner_radius=12,
                            border_width=1, border_color=THEME["border"])
        card.pack(fill="x", padx=4, pady=4)
        ctk.CTkLabel(card, text=title, font=font(20, "bold"),
                     text_color=THEME["accent"]).pack(anchor="w", padx=20, pady=(16, 0))
        ctk.CTkLabel(card, text=subtitle, font=font(12),
                     text_color=THEME["text_dim"]).pack(anchor="w", padx=20, pady=(0, 12))
        body = ctk.CTkFrame(card, fg_color="transparent")
        body.pack(fill="x", padx=16, pady=(0, 18))
        builder(body)
        frame.grid_remove()
        return frame

    def run_op(opname, coro_factory):
        if state.busy_op:
            gui_log(LEVEL_WARN, f"{state.busy_op} already running")
            return
        live = [a for a in state.accounts if a.client and a.healthy()]
        if not live:
            gui_log(LEVEL_WARN, "connect accounts first")
            return
        stop_evt = threading.Event()
        state.stop_flags[opname] = stop_evt
        set_busy(opname)

        async def _wrap():
            try:
                await coro_factory(stop_evt.is_set)
            except Exception as exc:
                gui_log(LEVEL_ERR, f"{opname} crashed: {exc}")
            finally:
                app.after(0, lambda: set_busy(None))
                app.after(0, refresh_stats)

        runner.submit(_wrap())

    def stop_op(opname):
        evt = state.stop_flags.get(opname)
        if evt:
            evt.set()
            gui_log(LEVEL_WARN, f"stopping {opname} ...")

    def add_controls(parent, opname, make_coro):
        bar = ctk.CTkFrame(parent, fg_color="transparent")
        bar.grid(row=50, column=0, sticky="w", pady=(14, 0))
        start = ctk.CTkButton(bar, text=f"▶ START {opname.upper()}", font=font(13, "bold"),
                              fg_color=THEME["accent"], hover_color=THEME["accent_dk"],
                              text_color="#06130c", width=180,
                              command=lambda: run_op(opname, make_coro))
        start.grid(row=0, column=0, padx=(0, 10))
        stop = ctk.CTkButton(bar, text="■ STOP", font=font(13, "bold"),
                             fg_color=THEME["card_hi"], hover_color=THEME["danger"],
                             text_color=THEME["text_br"], width=110, state="disabled",
                             command=lambda: stop_op(opname))
        stop.grid(row=0, column=1)
        op_start_buttons[opname] = (start, stop)

    # ---- JOIN panel ----
    def build_join(body):
        ch = make_field(body, "CHANNEL / INVITE  (@user · t.me/+hash · id)",
                        "@your_channel", row=0)
        lim = make_field(body, "MAX ACCOUNTS  (0 = all)", "0", row=2, width=160)
        dry = ctk.CTkCheckBox(body, text="dry-run", font=font(12),
                              fg_color=THEME["accent"], text_color=THEME["text"])
        dry.grid(row=4, column=0, sticky="w", padx=6, pady=8)

        async def coro(should_stop):
            limit = int(lim.get()) if lim.get().strip().isdigit() else 0
            await op_join(state.accounts, ch.get().strip(), limit=limit,
                          log_fn=gui_log, should_stop=should_stop,
                          dry_run=bool(dry.get()))
        add_controls(body, "join", coro)

    # ---- REACT panel ----
    def build_react(body):
        ch = make_field(body, "CHANNEL", "@your_channel", row=0)
        msg = make_field(body, "MESSAGE  (id · id,list · latest · latest:N · link)",
                         "latest", row=2)
        emo = make_field(body, "EMOJI", "👍", row=4, width=120)
        dry = ctk.CTkCheckBox(body, text="dry-run", font=font(12),
                              fg_color=THEME["accent"], text_color=THEME["text"])
        dry.grid(row=6, column=0, sticky="w", padx=6, pady=8)

        async def coro(should_stop):
            await op_react(state.accounts, ch.get().strip(), msg.get().strip(),
                           emo.get().strip() or "👍", log_fn=gui_log,
                           should_stop=should_stop, dry_run=bool(dry.get()))
        add_controls(body, "react", coro)

    # ---- STARS panel ----
    def build_star(body):
        ch = make_field(body, "CHANNEL", "@your_channel", row=0)
        msg = make_field(body, "MESSAGE", "latest", row=2)
        cnt = make_field(body, "STARS PER ACCOUNT", "1", row=4, width=160)
        anon = ctk.CTkCheckBox(body, text="anonymous", font=font(12),
                               fg_color=THEME["accent"], text_color=THEME["text"])
        anon.grid(row=6, column=0, sticky="w", padx=6, pady=(8, 2))
        clamp = ctk.CTkCheckBox(body, text="clamp to balance", font=font(12),
                                fg_color=THEME["accent"], text_color=THEME["text"])
        clamp.select()
        clamp.grid(row=7, column=0, sticky="w", padx=6, pady=2)
        dry = ctk.CTkCheckBox(body, text="dry-run", font=font(12),
                              fg_color=THEME["accent"], text_color=THEME["text"])
        dry.grid(row=8, column=0, sticky="w", padx=6, pady=(2, 8))

        async def coro(should_stop):
            stars = int(cnt.get()) if cnt.get().strip().isdigit() else 1
            await op_star(state.accounts, ch.get().strip(), msg.get().strip(), stars,
                          log_fn=gui_log, should_stop=should_stop,
                          anonymous=bool(anon.get()), clamp=bool(clamp.get()),
                          dry_run=bool(dry.get()))
        add_controls(body, "stars", coro)

    # ---- VIEWS panel ----
    def build_views(body):
        ch = make_field(body, "CHANNEL", "@your_channel", row=0)
        msg = make_field(body, "MESSAGE", "latest", row=2)
        cnt = make_field(body, "HOW MANY VIEWS  (0 = all accounts)", "0", row=4, width=200)
        dry = ctk.CTkCheckBox(body, text="dry-run", font=font(12),
                              fg_color=THEME["accent"], text_color=THEME["text"])
        dry.grid(row=6, column=0, sticky="w", padx=6, pady=8)

        async def coro(should_stop):
            target = int(cnt.get()) if cnt.get().strip().isdigit() else 0
            await op_views(state.accounts, ch.get().strip(), msg.get().strip(), target,
                           log_fn=gui_log, should_stop=should_stop, dry_run=bool(dry.get()))
        add_controls(body, "views", coro)

    # ---- VOTE panel ----
    def build_vote(body):
        ch = make_field(body, "CHANNEL", "@your_channel", row=0)
        msg = make_field(body, "MESSAGE ID (the poll)", "1234", row=2, width=200)
        idx = make_field(body, "OPTION INDEX  (0 = first choice)", "0", row=4, width=160)
        dry = ctk.CTkCheckBox(body, text="dry-run", font=font(12),
                              fg_color=THEME["accent"], text_color=THEME["text"])
        dry.grid(row=6, column=0, sticky="w", padx=6, pady=8)

        async def coro(should_stop):
            oi = int(idx.get()) if idx.get().strip().lstrip("-").isdigit() else 0
            await op_vote(state.accounts, ch.get().strip(), msg.get().strip(),
                          option_index=oi, log_fn=gui_log, should_stop=should_stop,
                          dry_run=bool(dry.get()))
        add_controls(body, "vote", coro)

    # ---- FORWARD panel ----
    def build_forward(body):
        ch = make_field(body, "SOURCE CHANNEL", "@your_channel", row=0)
        msg = make_field(body, "MESSAGE ID(s)  (id or id,list)", "1234", row=2)
        to = make_field(body, "FORWARD TO  ('me' = Saved Messages)", "me", row=4, width=220)
        dry = ctk.CTkCheckBox(body, text="dry-run", font=font(12),
                              fg_color=THEME["accent"], text_color=THEME["text"])
        dry.grid(row=6, column=0, sticky="w", padx=6, pady=8)

        async def coro(should_stop):
            await op_forward(state.accounts, ch.get().strip(), msg.get().strip(),
                             to=to.get().strip() or "me", log_fn=gui_log,
                             should_stop=should_stop, dry_run=bool(dry.get()))
        add_controls(body, "forward", coro)

    # ---- COMMENT panel ----
    def build_comment(body):
        ch = make_field(body, "CHANNEL  (must have a discussion group)", "@your_channel", row=0)
        msg = make_field(body, "MESSAGE ID", "1234", row=2, width=200)
        txt = make_field(body, "COMMENT TEXT(s)  (separate with | )", "nice|🔥|great post", row=4, width=420)
        dry = ctk.CTkCheckBox(body, text="dry-run", font=font(12),
                              fg_color=THEME["accent"], text_color=THEME["text"])
        dry.grid(row=6, column=0, sticky="w", padx=6, pady=8)

        async def coro(should_stop):
            texts = [t for t in txt.get().split("|") if t.strip()] or ["nice"]
            await op_comment(state.accounts, ch.get().strip(), msg.get().strip(),
                             texts, log_fn=gui_log, should_stop=should_stop,
                             dry_run=bool(dry.get()))
        add_controls(body, "comment", coro)

    # ---- STORY panel ----
    def build_story(body):
        ch = make_field(body, "CHANNEL / USER (with active stories)", "@your_channel", row=0)
        emo = make_field(body, "REACT EMOJI  (blank = views only)", "🔥", row=2, width=140)
        dry = ctk.CTkCheckBox(body, text="dry-run", font=font(12),
                              fg_color=THEME["accent"], text_color=THEME["text"])
        dry.grid(row=4, column=0, sticky="w", padx=6, pady=8)

        async def coro(should_stop):
            await op_story(state.accounts, ch.get().strip(),
                           react_emoji=emo.get().strip(), log_fn=gui_log,
                           should_stop=should_stop, dry_run=bool(dry.get()))
        add_controls(body, "story", coro)

    # ---- SCRAPE panel ----
    def build_scrape(body):
        src = make_field(body, "SOURCE GROUP / CHANNEL", "@source_group", row=0)
        out = make_field(body, "SAVE TO FILE", "data/members.json", row=2, width=320)
        lim = make_field(body, "MAX MEMBERS  (0 = all)", "0", row=4, width=160)
        agg = ctk.CTkCheckBox(body, text="aggressive (deeper scrape)", font=font(12),
                              fg_color=THEME["accent"], text_color=THEME["text"])
        agg.grid(row=6, column=0, sticky="w", padx=6, pady=8)

        async def coro(should_stop):
            limit = int(lim.get()) if lim.get().strip().isdigit() else 0
            await op_scrape(state.accounts, src.get().strip(), out.get().strip(),
                            limit=limit, aggressive=bool(agg.get()),
                            log_fn=gui_log, should_stop=should_stop)
        add_controls(body, "scrape", coro)

    # ---- INVITE panel ----
    def build_invite(body):
        tgt = make_field(body, "TARGET CHANNEL (yours)", "@my_channel", row=0)
        mf = make_field(body, "MEMBERS FILE", "data/members.json", row=2, width=320)
        per = make_field(body, "INVITES PER ACCOUNT", "20", row=4, width=160)
        bat = make_field(body, "BATCH SIZE", "3", row=6, width=120)
        dry = ctk.CTkCheckBox(body, text="dry-run", font=font(12),
                              fg_color=THEME["accent"], text_color=THEME["text"])
        dry.grid(row=8, column=0, sticky="w", padx=6, pady=8)

        async def coro(should_stop):
            members = load_members(mf.get().strip())
            pa = int(per.get()) if per.get().strip().isdigit() else 20
            bs = int(bat.get()) if bat.get().strip().isdigit() else 3
            await op_invite(state.accounts, tgt.get().strip(), members,
                            per_account=pa, batch=bs, log_fn=gui_log,
                            should_stop=should_stop, store=state.store,
                            dry_run=bool(dry.get()))
        add_controls(body, "invite", coro)

    # ---- DM panel ----
    def build_dm(body):
        mf = make_field(body, "MEMBERS FILE", "data/members.json", row=0, width=320)
        tmpl = make_field(body, "MESSAGE  ({Hi|Hey} {first_name})",
                          "{Hi|Hey} {first_name}, check @my_channel", row=2, width=440)
        per = make_field(body, "DMs PER ACCOUNT", "15", row=4, width=160)
        typ = ctk.CTkCheckBox(body, text="simulate typing", font=font(12),
                              fg_color=THEME["accent"], text_color=THEME["text"])
        typ.select()
        typ.grid(row=6, column=0, sticky="w", padx=6, pady=(8, 2))
        dry = ctk.CTkCheckBox(body, text="dry-run", font=font(12),
                              fg_color=THEME["accent"], text_color=THEME["text"])
        dry.grid(row=7, column=0, sticky="w", padx=6, pady=(2, 8))

        async def coro(should_stop):
            members = load_members(mf.get().strip())
            pa = int(per.get()) if per.get().strip().isdigit() else 15
            await op_dm(state.accounts, members, [tmpl.get()], per_account=pa,
                        simulate_typing=bool(typ.get()), log_fn=gui_log,
                        should_stop=should_stop, store=state.store,
                        dry_run=bool(dry.get()))
        add_controls(body, "dm", coro)

    # ---- DRIP-JOIN panel ----
    def build_dripjoin(body):
        ch = make_field(body, "CHANNEL / INVITE", "@your_channel", row=0)
        wv = make_field(body, "ACCOUNTS PER WAVE", "3", row=2, width=160)
        dry = ctk.CTkCheckBox(body, text="dry-run", font=font(12),
                              fg_color=THEME["accent"], text_color=THEME["text"])
        dry.grid(row=4, column=0, sticky="w", padx=6, pady=8)

        async def coro(should_stop):
            ws = int(wv.get()) if wv.get().strip().isdigit() else 3
            await op_dripjoin(state.accounts, ch.get().strip(), wave_size=ws,
                              log_fn=gui_log, should_stop=should_stop,
                              dry_run=bool(dry.get()))
        add_controls(body, "dripjoin", coro)

    # ---- WARMUP panel ----
    def build_warmup(body):
        ctk.CTkLabel(body, text="Gradually ages fresh accounts: sets a profile, "
                     "idle-joins public channels, reads and lightly reacts.\n"
                     "Run it on several sessions over days until accounts graduate.",
                     font=font(12), text_color=THEME["text_dim"], justify="left"
                     ).grid(row=0, column=0, sticky="w", padx=6, pady=(4, 10))
        av = make_field(body, "AVATAR FOLDER  (optional)", "", row=1, width=320)

        async def coro(should_stop):
            cfg = WarmupConfig(avatar_dir=av.get().strip() or None)
            await warmup_pool(state.accounts, state.store, cfg, gui_log, should_stop)
        add_controls(body, "warmup", coro)

    panels = {
        "JOIN":  build_op_panel("⟳ JOIN", "mass-join channels & private invite links", build_join),
        "REACT": build_op_panel("♥ REACT", "emoji reactions from the account pool", build_react),
        "STARS": build_op_panel("★ STARS", "paid star reactions (Telegram Stars)", build_star),
        "VIEWS": build_op_panel("◉ VIEWS", "boost channel message view counts", build_views),
    }
    if _ADVANCED:
        panels.update({
            "VOTE":    build_op_panel("▤ VOTE", "cast votes on a channel poll", build_vote),
            "FORWARD": build_op_panel("➦ FORWARD", "forward/share a post to inflate shares", build_forward),
            "COMMENT": build_op_panel("💬 COMMENT", "comment into the linked discussion group", build_comment),
            "STORY":   build_op_panel("◈ STORY", "register story views + reactions", build_story),
            "SCRAPE":  build_op_panel("⛏ SCRAPE", "harvest a source group's members", build_scrape),
            "INVITE":  build_op_panel("➜ INVITE", "invite scraped members into your channel", build_invite),
            "DM":      build_op_panel("✉ DM", "templated mass direct messages", build_dm),
            "DRIP":    build_op_panel("⇣ DRIP-JOIN", "join in small waves (stealthier)", build_dripjoin),
            "WARMUP":  build_op_panel("♨ WARM-UP", "age fresh accounts before use", build_warmup),
        })

    current_panel = {"name": None}

    def show_panel(name):
        if current_panel["name"]:
            panels[current_panel["name"]].grid_remove()
        panels[name].grid()
        current_panel["name"] = name
        for n, b in nav_buttons.items():
            b.configure(fg_color=THEME["accent_lo"] if n == name else "transparent",
                        text_color=THEME["accent"] if n == name else THEME["text"])

    # ── sidebar nav buttons (grouped, scrollable) ────────────────────────────
    nav_buttons: dict = {}
    nav_scroll = ctk.CTkScrollableFrame(sidebar, fg_color=THEME["panel"],
                                        corner_radius=0, width=190)
    nav_scroll.pack(fill="both", expand=True, padx=0, pady=(14, 0))

    def _nav_group(title, items):
        ctk.CTkLabel(nav_scroll, text=title, font=font(10, "bold"),
                     text_color=THEME["text_dim"]).pack(anchor="w", padx=14, pady=(12, 2))
        for name, icon in items:
            if name not in panels:
                continue
            b = ctk.CTkButton(nav_scroll, text=f"  {icon}  {name}", anchor="w",
                              font=font(13, "bold"), corner_radius=8, height=34,
                              fg_color="transparent", hover_color=THEME["card_hi"],
                              text_color=THEME["text"],
                              command=lambda n=name: show_panel(n))
            b.pack(fill="x", padx=10, pady=2)
            nav_buttons[name] = b

    _nav_group("CORE", [("JOIN", "⟳"), ("REACT", "♥"), ("STARS", "★"), ("VIEWS", "◉")])
    if _ADVANCED:
        _nav_group("ENGAGE", [("VOTE", "▤"), ("FORWARD", "➦"),
                              ("COMMENT", "💬"), ("STORY", "◈")])
        _nav_group("GROWTH", [("SCRAPE", "⛏"), ("INVITE", "➜"),
                              ("DM", "✉"), ("DRIP", "⇣")])
        _nav_group("LIFECYCLE", [("WARMUP", "♨")])

    ctk.CTkLabel(sidebar, text="theme: green//matrix · font: " + FONT, font=font(9),
                 text_color=THEME["text_dim"]).pack(anchor="w", padx=14, pady=(6, 0))
    ctk.CTkLabel(sidebar, text="v2.1 · all-in-one", font=font(9),
                 text_color=THEME["accent"]).pack(anchor="w", padx=14, pady=(0, 10))

    # ── boot ────────────────────────────────────────────────────────────────
    for line in BANNER.strip("\n").splitlines():
        gui_log(LEVEL_ACCENT, line)
    gui_log(LEVEL_INFO, "ready — set ACCOUNTS DIR, LOAD, then CONNECT.")
    show_panel("JOIN")
    refresh_stats()
    poll_logs()

    def on_close():
        runner.stop()
        app.destroy()

    app.protocol("WM_DELETE_WINDOW", on_close)
    app.mainloop()
    return 0


# ==============================================================================
#  HEADLESS CLI  (no display needed)
# ==============================================================================

def _cli_log(level: str, text: str) -> None:
    prefix = {LEVEL_OK: "[+]", LEVEL_WARN: "[!]", LEVEL_ERR: "[x]",
              LEVEL_ACCENT: "[*]"}.get(level, "   ")
    print(f"{prefix} {text}")


async def _cli_prepare(args) -> "list[Account]":
    accounts = load_accounts(args.csv_dir, _cli_log)
    _cli_log(LEVEL_ACCENT, f"connecting {len(accounts)} account(s) ...")
    await connect_pool(accounts, _cli_log, concurrency=args.concurrency)
    live = [a for a in accounts if a.client and a.healthy()]
    if not live:
        raise RuntimeError("no account connected")
    if args.op == "stars":
        await asyncio.gather(*(get_star_balance(a, _cli_log) for a in live))
    return accounts


async def _cli_run(args) -> int:
    accounts = await _cli_prepare(args)
    try:
        if args.op == "join":
            await op_join(accounts, args.channel, limit=args.limit,
                          log_fn=_cli_log, should_stop=_noop_stop, dry_run=args.dry_run)
        elif args.op == "react":
            await op_react(accounts, args.channel, args.message, args.emoji,
                           log_fn=_cli_log, should_stop=_noop_stop, dry_run=args.dry_run)
        elif args.op == "stars":
            await op_star(accounts, args.channel, args.message, args.count,
                          log_fn=_cli_log, should_stop=_noop_stop,
                          anonymous=args.anonymous, dry_run=args.dry_run)
        elif args.op == "views":
            await op_views(accounts, args.channel, args.message, args.count,
                           log_fn=_cli_log, should_stop=_noop_stop, dry_run=args.dry_run)
    finally:
        for a in accounts:
            if a.client:
                try:
                    await a.client.disconnect()
                except Exception:
                    pass
    return 0


def build_cli_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="bootsusers_suite.py",
                                description="Bootsusers Telegram ops — single file (GUI + CLI).")
    p.add_argument("--cli", action="store_true", help="run headless instead of the GUI")
    p.add_argument("op", nargs="?", choices=["join", "react", "stars", "views"],
                   help="operation to run in --cli mode")
    p.add_argument("--channel", default="@your_channel")
    p.add_argument("--message", default="latest")
    p.add_argument("--emoji", default="👍")
    p.add_argument("--count", type=int, default=0,
                   help="stars-per-account (stars) or number of views (views)")
    p.add_argument("--limit", type=int, default=0, help="max accounts (join)")
    p.add_argument("--anonymous", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--csv-dir", default=DEFAULT_ACCOUNTS_DIR)
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p


def main() -> int:
    args = build_cli_parser().parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level),
                        format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
                        datefmt="%H:%M:%S")
    if args.cli:
        if not args.op:
            print("--cli requires an operation: join | react | stars | views")
            return 2
        try:
            return asyncio.run(_cli_run(args))
        except KeyboardInterrupt:
            print("interrupted")
            return 130
    return run_gui()


if __name__ == "__main__":
    raise SystemExit(main())
