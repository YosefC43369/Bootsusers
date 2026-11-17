# auto_views.py
# ==============================================================================
# Bootsusers — automated channel message VIEW-count booster (Telegram).
# ------------------------------------------------------------------------------
# Every account in the pool opens the target channel message(s) and registers a
# real view via messages.GetMessagesViews(increment=True). Telegram counts one
# view per account per message (server-side de-duplicated), so N distinct
# accounts add up to N views.
#
# Account sources — any .csv (or other delimited) file in ACCOUNTS_DIR whose
# columns carry the operator's own identifiers. The loader is tolerant about
# column names and will pick up any of:
#     phone / phone_number / number / msisdn / tel
#     session_name / session
#     id / user_id / uid / tg_id / telegram_id        (metadata)
#     email / mail / email_address                     (metadata)
#     proxy_type / proxy_host / proxy_port / proxy_user / proxy_pass
# `phone` is the only hard requirement (it is the MTProto login key); a missing
# session name is derived from the phone automatically.
#
# How many views to add is asked INTERACTIVELY right before the run (or passed
# with --views N for unattended use). If you request more views than you have
# accounts, every account is used and the shortfall is reported — a single
# account cannot add more than one view to the same message.
#
# Quick start:
#     python auto_views.py --channel @yourchannel --message latest
#     python auto_views.py --channel @chan --message 1234 --views 50
#     python auto_views.py --channel @chan --message 1234,1235 --views 200 --mode staggered
#     python auto_views.py --dry-run --channel @chan --message latest:3
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
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    RPCError,
    ChannelPrivateError,
    ChatAdminRequiredError,
    MsgIdInvalidError,
    AuthKeyUnregisteredError,
    UserDeactivatedBanError,
)
from telethon.tl.functions.messages import GetMessagesViewsRequest
try:
    from tg_resolve import resolve_channel_entity
except Exception:
    async def resolve_channel_entity(client, channel):
        return await client.get_input_entity(channel)

# ─── CONFIG (defaults; overridable on the command line) ───────────────────────

API_ID   = 34674448
API_HASH = "7098ee8e75f3c435f808ee9410a2f05b"

TARGET_CHANNEL = "@your_channel"
TARGET_MESSAGE = "latest"

# 0 → ask interactively ("type before it runs"); >0 → unattended target.
TARGET_VIEWS = 0

# "staggered"  → accounts view one at a time with a human-like gap,
# "simultaneous" → all selected accounts view together (faster, louder).
MODE = "staggered"
STAGGER_MIN_SEC = 0.8
STAGGER_MAX_SEC = 3.0

MAX_VIEW_OPS_PER_HOUR   = 200
MIN_GAP_BETWEEN_OPS_SEC = 0.5

MAX_RETRIES = 3
RETRY_BASE_DELAY = 2.0
FLOOD_MAX_WAIT_SEC = 300
CONNECT_TIMEOUT_SEC = 30.0

ACCOUNTS_DIR = "accounts"

log = logging.getLogger("auto_views")


# ─── RATE LIMITER ─────────────────────────────────────────────────────────────

class RateLimiter:
    def __init__(self, max_per_hour: int = 200, min_gap_sec: float = 0.5):
        self.max_per_hour = max_per_hour
        self.min_gap_sec = min_gap_sec
        self._timestamps: "deque[float]" = deque()
        self._lock = threading.Lock()

    def _purge(self, now: float) -> None:
        cutoff = now - 3600.0
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()

    def allowed(self) -> bool:
        now = time.monotonic()
        with self._lock:
            self._purge(now)
            if len(self._timestamps) >= self.max_per_hour:
                return False
            if self._timestamps and (now - self._timestamps[-1]) < self.min_gap_sec:
                return False
            return True

    def record(self) -> None:
        with self._lock:
            self._timestamps.append(time.monotonic())

    def seconds_until_allowed(self) -> float:
        now = time.monotonic()
        with self._lock:
            self._purge(now)
            wait_gap = 0.0
            wait_quota = 0.0
            if self._timestamps:
                wait_gap = max(0.0, (self._timestamps[-1] + self.min_gap_sec) - now)
            if len(self._timestamps) >= self.max_per_hour:
                wait_quota = max(0.0, (self._timestamps[0] + 3600.0) - now)
            return max(wait_gap, wait_quota)


# ─── PROXY ────────────────────────────────────────────────────────────────────

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


# ─── ACCOUNT ──────────────────────────────────────────────────────────────────

@dataclass
class Account:
    phone: str
    session: str
    user_id: str = ""              # metadata from the source file
    email: str = ""                # metadata from the source file
    proxy: ProxyConfig = field(default_factory=ProxyConfig)
    limiter: RateLimiter = field(default_factory=RateLimiter)
    client: Optional[TelegramClient] = field(default=None, repr=False)

    _failures: int = field(default=0, init=False, repr=False)
    MAX_FAIL: int = field(default=5, init=False, repr=False)

    def healthy(self) -> bool:
        return self._failures < self.MAX_FAIL

    def mark_ok(self) -> None:
        self._failures = 0

    def mark_fail(self) -> None:
        self._failures += 1


# ─── RESULT ───────────────────────────────────────────────────────────────────

class ViewStatus(Enum):
    VIEWED            = auto()
    SKIPPED_UNHEALTHY = auto()
    SKIPPED_RATELIMIT = auto()
    FLOOD_WAIT        = auto()
    BANNED            = auto()
    FAILED            = auto()
    DRY_RUN           = auto()


@dataclass
class ViewResult:
    phone: str
    status: ViewStatus
    msg_ids: "list[int]" = field(default_factory=list)
    detail: str = ""


# ─── FLEXIBLE CSV LOADING ─────────────────────────────────────────────────────

def _raise_csv_field_limit() -> None:
    """Allow very large CSV fields (big account dumps), capped at the largest
    C-long the platform accepts (avoids OverflowError on Windows)."""
    lim = sys.maxsize
    while lim > 1:
        try:
            csv.field_size_limit(lim)
            return
        except OverflowError:
            lim //= 10


_raise_csv_field_limit()


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


def _pick(row_lower: dict, keys) -> str:
    for k in keys:
        if k in row_lower and str(row_lower[k]).strip():
            return str(row_lower[k]).strip()
    return ""


def _sanitize_session(phone: str) -> str:
    return "session_" + re.sub(r"[^0-9A-Za-z]", "", phone)


def _sniff_delimiter(sample: str) -> str:
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
    except Exception:
        return ","


def _load_from_file(path: str) -> "list[Account]":
    # shared reader: header CSV/TSV/pipe + headerless .txt dumps
    from account_io import read_accounts, derive_session
    accounts: "list[Account]" = []
    for r in read_accounts(path):
        port_s = str(r.get("proxy_port", "") or "")
        accounts.append(Account(
            phone=r["phone"],
            session=r["session"] or derive_session(r["phone"]),
            user_id=r.get("user_id", ""),
            email=r.get("email", ""),
            proxy=ProxyConfig(
                proxy_type=r.get("proxy_type", ""),
                host=r.get("proxy_host", ""),
                port=int(port_s) if port_s.isdigit() else 0,
                username=r.get("proxy_user", ""),
                password=r.get("proxy_pass", ""),
            ),
            limiter=RateLimiter(MAX_VIEW_OPS_PER_HOUR, MIN_GAP_BETWEEN_OPS_SEC),
        ))
    return accounts


def load_accounts(folder: str = ACCOUNTS_DIR) -> "list[Account]":
    """
    Load accounts from `folder`, which may be a single FILE, a FOLDER (all
    files in it), or a glob pattern. Delimiter + columns are auto-detected.
    """
    folder = os.path.expanduser(folder)
    if os.path.isfile(folder):
        files = [os.path.abspath(folder)]
    elif any(ch in folder for ch in "*?["):
        files = sorted(os.path.abspath(f) for f in glob.glob(folder) if os.path.isfile(f))
    elif os.path.isdir(folder):
        files = sorted(os.path.abspath(p)
                       for p in glob.glob(os.path.join(os.path.abspath(folder), "*"))
                       if os.path.isfile(p))
    else:
        files = []
    if not files:
        raise FileNotFoundError(
            f"[load_accounts] no account file(s) at: {os.path.abspath(folder)!r}\n"
            f"Point this at a single file (accounts/main.csv) or a folder (accounts)."
        )

    merged: "list[Account]" = []
    seen: "set[str]" = set()
    dupes = 0
    for path in files:
        try:
            batch = _load_from_file(path)
        except Exception as exc:
            log.warning("[load_accounts] skipping %s — %s", path, exc)
            continue
        for acc in batch:
            key = acc.phone or acc.session
            if key in seen:
                dupes += 1
                continue
            seen.add(key)
            merged.append(acc)
        log.info("[load_accounts] %s → %d (cumulative %d)",
                 os.path.basename(path), len(batch), len(merged))

    if dupes:
        log.warning("[load_accounts] removed %d duplicate phone(s)", dupes)
    log.info("[load_accounts] %d accounts from %d file(s)", len(merged), len(files))
    return merged


# ─── CONNECT ──────────────────────────────────────────────────────────────────

async def connect_account(acc: Account) -> bool:
    try:
        acc.client = TelegramClient(
            acc.session, API_ID, API_HASH, proxy=acc.proxy.to_telethon()
        )
        await asyncio.wait_for(acc.client.connect(), timeout=CONNECT_TIMEOUT_SEC)
        if not await acc.client.is_user_authorized():
            raise RuntimeError('session not authorized — needs a logged-in .session file or session string (cannot sign in headlessly)')
        acc.mark_ok()
        tag = f" via {acc.proxy.proxy_type}:{acc.proxy.host}" if acc.proxy.proxy_type else ""
        log.info("[+] connected: %s%s", acc.phone, tag)
        return True
    except (AuthKeyUnregisteredError, UserDeactivatedBanError) as exc:
        acc._failures = acc.MAX_FAIL
        log.error("[!] %s — account dead/banned: %s", acc.phone, exc)
        return False
    except Exception as exc:
        acc.mark_fail()
        log.error("[!] connect failed %s: %s", acc.phone, exc)
        return False


async def connect_all(accounts: "list[Account]", concurrency: int = 8) -> None:
    sem = asyncio.Semaphore(concurrency)

    async def _one(acc: Account) -> None:
        async with sem:
            await connect_account(acc)

    await asyncio.gather(*(_one(a) for a in accounts))


# ─── MESSAGE RESOLUTION ───────────────────────────────────────────────────────

_MSG_LINK_RE = re.compile(r"(?:https?://)?t\.me/(?:c/\d+|[A-Za-z0-9_]+)/(\d+)")


def _parse_message_spec(spec: str) -> "tuple[str, object]":
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
    raise ValueError(f"unrecognised --message value: {spec!r}")


async def resolve_message_ids(client: TelegramClient, channel, spec: str) -> "list[int]":
    kind, value = _parse_message_spec(spec)
    if kind == "ids":
        return list(value)
    msgs = await client.get_messages(channel, limit=int(value))
    ids = [m.id for m in msgs if m is not None]
    if not ids:
        raise ValueError("channel has no readable messages")
    return ids


async def read_view_counts(client: TelegramClient, channel, msg_ids: "list[int]") -> dict:
    """Read current view counts without incrementing (increment=False)."""
    try:
        res = await client(GetMessagesViewsRequest(
            peer=channel, id=msg_ids, increment=False))
        return {mid: (v.views or 0) for mid, v in zip(msg_ids, res.views)}
    except Exception as exc:
        log.warning("could not read baseline views: %s", exc)
        return {mid: -1 for mid in msg_ids}


# ─── VIEW ONE ACCOUNT ─────────────────────────────────────────────────────────

async def view_once(
    acc: Account,
    channel,
    msg_ids: "list[int]",
    *,
    dry_run: bool = False,
) -> ViewResult:
    """Register one view from `acc` for every id in msg_ids (single RPC call)."""
    if not acc.healthy():
        return ViewResult(acc.phone, ViewStatus.SKIPPED_UNHEALTHY, msg_ids,
                          "account unhealthy")
    if not acc.limiter.allowed():
        wait = acc.limiter.seconds_until_allowed()
        return ViewResult(acc.phone, ViewStatus.SKIPPED_RATELIMIT, msg_ids,
                          f"rate-limited ({wait:.0f}s)")

    if dry_run:
        return ViewResult(acc.phone, ViewStatus.DRY_RUN, msg_ids, "dry-run")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await acc.client(GetMessagesViewsRequest(
                peer=channel, id=msg_ids, increment=True))
            acc.limiter.record()
            acc.mark_ok()
            log.info("[%s] 👁 viewed %d message(s)", acc.phone, len(msg_ids))
            return ViewResult(acc.phone, ViewStatus.VIEWED, msg_ids)

        except FloodWaitError as e:
            if e.seconds > FLOOD_MAX_WAIT_SEC:
                return ViewResult(acc.phone, ViewStatus.FLOOD_WAIT, msg_ids,
                                  f"floodwait {e.seconds}s")
            wait = e.seconds + random.uniform(0.3, 1.5)
            log.warning("[%s] FloodWait %ds — sleeping %.0fs", acc.phone, e.seconds, wait)
            await asyncio.sleep(wait)

        except (ChannelPrivateError, ChatAdminRequiredError) as e:
            return ViewResult(acc.phone, ViewStatus.FAILED, msg_ids, f"access: {e}")

        except MsgIdInvalidError:
            return ViewResult(acc.phone, ViewStatus.FAILED, msg_ids, "bad msg id")

        except (AuthKeyUnregisteredError, UserDeactivatedBanError) as e:
            acc._failures = acc.MAX_FAIL
            return ViewResult(acc.phone, ViewStatus.BANNED, msg_ids, str(e))

        except RPCError as e:
            acc.mark_fail()
            if attempt >= MAX_RETRIES:
                return ViewResult(acc.phone, ViewStatus.FAILED, msg_ids, str(e))
            await asyncio.sleep(RETRY_BASE_DELAY * attempt)

        except Exception as e:
            acc.mark_fail()
            log.warning("[%s] view error (attempt %d): %s", acc.phone, attempt, e)
            if attempt >= MAX_RETRIES:
                return ViewResult(acc.phone, ViewStatus.FAILED, msg_ids, str(e))
            await asyncio.sleep(RETRY_BASE_DELAY * attempt)

    return ViewResult(acc.phone, ViewStatus.FAILED, msg_ids, "exhausted retries")


# ─── ORCHESTRATION ────────────────────────────────────────────────────────────

async def run_views(
    accounts: "list[Account]",
    channel_spec: str,
    message_spec: str,
    target_views: int,
    *,
    dry_run: bool = False,
) -> "tuple[list[ViewResult], dict, dict]":
    """
    Add up to `target_views` views. Returns (results, before_counts, after_counts).
    """
    live = [a for a in accounts if a.client and a.healthy()]
    if not live:
        raise RuntimeError("no connected account available")

    resolver = live[0].client
    channel = await resolve_channel_entity(resolver, channel_spec)
    msg_ids = await resolve_message_ids(resolver, channel, message_spec)
    log.info("target resolved; message ids: %s", msg_ids)

    before = await read_view_counts(resolver, channel, msg_ids)

    # one account == one view per message, so cap selection at account count
    want = len(live) if target_views <= 0 else min(target_views, len(live))
    if target_views > len(live):
        log.warning("requested %d views but only %d accounts available — "
                    "adding %d (one view per account max)",
                    target_views, len(live), len(live))
    selected = live[:want]
    log.info("using %d account(s) to add views to %d message(s)",
             len(selected), len(msg_ids))

    results: "list[ViewResult]" = []
    if MODE == "simultaneous":
        results = list(await asyncio.gather(*[
            view_once(acc, channel, msg_ids, dry_run=dry_run) for acc in selected
        ]))
    else:  # staggered
        for acc in selected:
            res = await view_once(acc, channel, msg_ids, dry_run=dry_run)
            results.append(res)
            if res.status in (ViewStatus.VIEWED, ViewStatus.DRY_RUN):
                await asyncio.sleep(random.uniform(STAGGER_MIN_SEC, STAGGER_MAX_SEC))

    after = (before if dry_run
             else await read_view_counts(resolver, channel, msg_ids))
    return results, before, after


# ─── REPORT ───────────────────────────────────────────────────────────────────

def print_report(results, before: dict, after: dict, target_views: int) -> None:
    from collections import Counter
    by_status = Counter(r.status for r in results)
    viewed = by_status.get(ViewStatus.VIEWED, 0)
    dry = by_status.get(ViewStatus.DRY_RUN, 0)

    print("\n" + "=" * 62)
    print(" auto_views — campaign summary")
    print("=" * 62)
    label = {
        ViewStatus.VIEWED:            "views registered",
        ViewStatus.DRY_RUN:           "dry-run (would view)",
        ViewStatus.SKIPPED_UNHEALTHY: "skipped — unhealthy",
        ViewStatus.SKIPPED_RATELIMIT: "skipped — rate-limited",
        ViewStatus.FLOOD_WAIT:        "flood-wait",
        ViewStatus.BANNED:            "banned/dead",
        ViewStatus.FAILED:            "failed",
    }
    for status, text in label.items():
        n = by_status.get(status, 0)
        if n:
            print(f"  {text:<26} {n}")
    print("-" * 62)
    if target_views > 0:
        print(f"  requested views             {target_views}")
    print(f"  accounts that viewed        {viewed or dry}")
    print("-" * 62)
    print("  per-message view counts (before → after):")
    for mid in before:
        b = before.get(mid, -1)
        a = after.get(mid, -1)
        bs = "?" if b < 0 else str(b)
        as_ = "?" if a < 0 else str(a)
        delta = f"  (+{a - b})" if (a >= 0 and b >= 0 and a >= b) else ""
        print(f"    msg {mid:<12} {bs:>8} → {as_:<8}{delta}")
    print("=" * 62 + "\n")


# ─── VIEW-COUNT PROMPT ────────────────────────────────────────────────────────

def ask_target_views(cli_value: int, available: int) -> int:
    """
    Decide how many views to add.
      * cli_value > 0            → use it (unattended).
      * cli_value == 0 & TTY     → prompt the operator.
      * cli_value == 0 & no TTY  → use every available account.
    """
    if cli_value > 0:
        return cli_value
    if not sys.stdin.isatty():
        log.info("no --views given and stdin is not interactive — "
                 "using all %d account(s)", available)
        return available

    print(f"\nบัญชีที่พร้อมใช้งาน: {available}")
    while True:
        raw = input("จำนวนยอดวิวที่ต้องการเพิ่ม "
                    "(กด Enter หรือ 0 = ใช้ทุกบัญชี): ").strip()
        if raw == "" or raw == "0":
            return available
        if raw.isdigit() and int(raw) > 0:
            return int(raw)
        print("  กรุณาใส่จำนวนเต็มบวก หรือ 0")


# ─── CLI ──────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="auto_views.py",
        description="Boost Telegram channel message view counts from an account pool.",
    )
    p.add_argument("--channel", default=TARGET_CHANNEL,
                   help="target channel (@user, t.me link, or numeric id)")
    p.add_argument("--message", default=TARGET_MESSAGE,
                   help="message id / id,list / 'latest' / 'latest:N' / message link")
    p.add_argument("--views", type=int, default=TARGET_VIEWS,
                   help="how many views to add (0 or omit → ask interactively)")
    p.add_argument("--mode", choices=["staggered", "simultaneous"], default=MODE)
    p.add_argument("--csv-dir", default=ACCOUNTS_DIR,
                   help="folder holding the account files")
    p.add_argument("--api-id", type=int, default=API_ID)
    p.add_argument("--api-hash", default=API_HASH)
    p.add_argument("--concurrency", type=int, default=8,
                   help="parallel connections during startup")
    p.add_argument("--dry-run", action="store_true",
                   help="connect + resolve but register no views")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p


async def main_async(args: argparse.Namespace) -> int:
    global API_ID, API_HASH, TARGET_CHANNEL, TARGET_MESSAGE, MODE, ACCOUNTS_DIR

    API_ID = args.api_id
    API_HASH = args.api_hash
    TARGET_CHANNEL = args.channel
    TARGET_MESSAGE = args.message
    MODE = args.mode
    ACCOUNTS_DIR = args.csv_dir

    accounts = load_accounts(ACCOUNTS_DIR)
    if not accounts:
        log.error("no accounts loaded")
        return 2

    log.info("connecting %d account(s) ...", len(accounts))
    await connect_all(accounts, concurrency=args.concurrency)
    live = [a for a in accounts if a.client and a.healthy()]
    if not live:
        log.error("no account connected successfully")
        return 3
    log.info("%d/%d account(s) connected", len(live), len(accounts))

    # how many views — asked right before work starts
    target_views = ask_target_views(args.views, len(live))

    try:
        results, before, after = await run_views(
            accounts, TARGET_CHANNEL, TARGET_MESSAGE, target_views,
            dry_run=args.dry_run,
        )
    finally:
        for a in accounts:
            if a.client:
                try:
                    await a.client.disconnect()
                except Exception:
                    pass

    print_report(results, before, after, target_views)
    return 0


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        raise SystemExit(asyncio.run(main_async(args)))
    except KeyboardInterrupt:
        log.warning("interrupted by user")
        raise SystemExit(130)


if __name__ == "__main__":
    main()
