# auto_star.py
# ==============================================================================
# Bootsusers — automated paid "star" reactions (Telegram Stars) for channels.
# ------------------------------------------------------------------------------
# For every account in the CSV pool this bot:
#   1. connects the account,
#   2. reads its Telegram Stars balance (payments.GetStarsStatus),
#   3. if the account HAS stars  → sends a paid-star reaction
#      (messages.SendPaidReaction) to the configured channel message(s),
#   4. if the account has NO stars → it is skipped, no error, next account.
#
# Every account is assumed to belong to the operator. Drop one or more .csv
# files into the ACCOUNTS_CSV_DIR folder (created by you) at any time; all of
# them are merged and de-duplicated at startup.
#
# Expected CSV columns:
#     phone, session_name[, proxy_type, proxy_host, proxy_port, proxy_user, proxy_pass]
#
# Quick start:
#     python auto_star.py --channel https://t.me/yourchannel --message latest --stars 1
#     python auto_star.py --channel @chan --message 1234 --stars 5 --mode staggered
#     python auto_star.py --channel @chan --message 1234,1235,1236 --stars 2 --anonymous
#     python auto_star.py --dry-run --channel @chan --message latest:3
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
from telethon.tl.functions.messages import SendPaidReactionRequest
from telethon.tl.functions.payments import GetStarsStatusRequest
from telethon.tl.types import (
    InputPeerSelf,
    PaidReactionPrivacyAnonymous,
    PaidReactionPrivacyDefault,
)

# ─── CONFIG (defaults; every value is overridable on the command line) ────────

API_ID   = 34674448
API_HASH = "7098ee8e75f3c435f808ee9410a2f05b"

# Channel the stars are sent to. Accepts:
#   @username                      public channel
#   https://t.me/username          public link
#   https://t.me/+AbCdEf...         private invite link (account must already be a member)
#   -1001234567890                 numeric channel id
TARGET_CHANNEL = "@your_channel"

# Which message(s) receive the stars. Accepts:
#   1234                  single message id
#   1234,1235,1236        explicit list of ids
#   latest                the most recent post
#   latest:5              the 5 most recent posts
#   https://t.me/chan/1234            message link (id extracted)
#   https://t.me/c/1234567890/1234    private-channel message link
TARGET_MESSAGE = "latest"

# How many stars EACH account sends to EACH message.
STARS_PER_ACCOUNT = 1

# If an account's balance is below STARS_PER_ACCOUNT but still > 0:
#   True  → send whatever balance it has (clamp down),
#   False → skip the account entirely.
CLAMP_TO_BALANCE = True

# Reaction privacy:  "default" (your name shows) | "anonymous".
PRIVACY = "default"

# "staggered"  → one account at a time with a random human-like gap,
# "simultaneous" → all eligible accounts fire together (louder, riskier).
MODE = "staggered"
STAGGER_MIN_SEC = 3.0
STAGGER_MAX_SEC = 9.0

# Per-account rate limiting (paid reactions are cheap on limits, but we still
# behave). These guard against accidental double-runs hammering one account.
MAX_STARS_OPS_PER_HOUR    = 60
MIN_GAP_BETWEEN_OPS_SEC   = 2.0

# Retry policy for transient send failures.
MAX_SEND_RETRIES = 3
RETRY_BASE_DELAY = 2.0

# Honour FloodWait up to this many seconds (longer → skip that op).
FLOOD_MAX_WAIT_SEC = 300

ACCOUNTS_CSV_DIR = "accounts"

CONNECT_TIMEOUT_SEC = 30.0

log = logging.getLogger("auto_star")


# ─── RATE LIMITER ─────────────────────────────────────────────────────────────

class RateLimiter:
    """Token-bucket limiter: at most `max_per_hour` ops, min `min_gap` apart."""

    def __init__(self, max_per_hour: int = 60, min_gap_sec: float = 2.0):
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
    """Single proxy entry in the Telethon tuple shape."""

    proxy_type: str = ""          # "socks5" | "http" | ""
    host: str = ""
    port: int = 0
    username: str = ""
    password: str = ""

    def to_telethon(self) -> Optional[tuple]:
        if not self.proxy_type:
            return None
        import socks  # PySocks / python-socks, pulled in by Telethon
        ptype = socks.SOCKS5 if self.proxy_type.lower() == "socks5" else socks.HTTP
        return (
            ptype,
            self.host,
            self.port,
            True,                       # rdns
            self.username or None,
            self.password or None,
        )


# ─── ACCOUNT ──────────────────────────────────────────────────────────────────

@dataclass
class Account:
    phone: str
    session: str
    proxy: ProxyConfig = field(default_factory=ProxyConfig)
    limiter: RateLimiter = field(default_factory=RateLimiter)
    client: Optional[TelegramClient] = field(default=None, repr=False)

    # cached star balance (filled lazily; -1 = unknown)
    star_balance: int = field(default=-1, repr=False)

    _failures: int = field(default=0, init=False, repr=False)
    MAX_FAIL: int = field(default=5, init=False, repr=False)

    def healthy(self) -> bool:
        return self._failures < self.MAX_FAIL

    def mark_ok(self) -> None:
        self._failures = 0

    def mark_fail(self) -> None:
        self._failures += 1


# ─── RESULT ───────────────────────────────────────────────────────────────────

class StarStatus(Enum):
    SENT          = auto()   # stars successfully sent
    SKIPPED_NO_STARS = auto()   # account has zero balance → intentionally skipped
    SKIPPED_UNHEALTHY = auto()
    SKIPPED_RATELIMIT = auto()
    FLOOD_WAIT    = auto()
    BANNED        = auto()
    FAILED        = auto()
    DRY_RUN       = auto()


@dataclass
class StarResult:
    phone: str
    msg_id: int
    status: StarStatus
    stars_sent: int = 0
    balance_before: int = -1
    detail: str = ""


# ─── CSV LOADING ──────────────────────────────────────────────────────────────

def _load_from_file(path: str) -> "list[Account]":
    accounts: "list[Account]" = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            phone = (row.get("phone") or "").strip()
            session = (row.get("session_name") or "").strip()
            if not phone or not session:
                log.warning("[csv] %s — skipping row with missing phone/session", path)
                continue
            proxy = ProxyConfig(
                proxy_type=(row.get("proxy_type") or "").strip(),
                host=(row.get("proxy_host") or "").strip(),
                port=int(row["proxy_port"]) if (row.get("proxy_port") or "").strip() else 0,
                username=(row.get("proxy_user") or "").strip(),
                password=(row.get("proxy_pass") or "").strip(),
            )
            accounts.append(Account(
                phone=phone,
                session=session,
                proxy=proxy,
                limiter=RateLimiter(MAX_STARS_OPS_PER_HOUR, MIN_GAP_BETWEEN_OPS_SEC),
            ))
    return accounts


def load_accounts(folder: str = ACCOUNTS_CSV_DIR) -> "list[Account]":
    """Merge every *.csv in *folder* into one de-duplicated account list."""
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

    merged: "list[Account]" = []
    seen: "set[str]" = set()
    dupes = 0
    for path in csv_files:
        try:
            batch = _load_from_file(path)
        except Exception as exc:
            log.warning("[load_accounts] skipping %s — %s", path, exc)
            continue
        for acc in batch:
            if acc.phone in seen:
                dupes += 1
                continue
            seen.add(acc.phone)
            merged.append(acc)
        log.info("[load_accounts] %s → %d (cumulative %d)",
                 os.path.basename(path), len(batch), len(merged))

    if dupes:
        log.warning("[load_accounts] removed %d duplicate phone(s)", dupes)
    log.info("[load_accounts] %d accounts from %d file(s)", len(merged), len(csv_files))
    return merged


# ─── CONNECT ──────────────────────────────────────────────────────────────────

async def connect_account(acc: Account) -> bool:
    try:
        acc.client = TelegramClient(
            acc.session, API_ID, API_HASH, proxy=acc.proxy.to_telethon()
        )
        await asyncio.wait_for(acc.client.start(phone=acc.phone),
                               timeout=CONNECT_TIMEOUT_SEC)
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


async def connect_all(accounts: "list[Account]", concurrency: int = 5) -> None:
    sem = asyncio.Semaphore(concurrency)

    async def _one(acc: Account) -> None:
        async with sem:
            await connect_account(acc)

    await asyncio.gather(*(_one(a) for a in accounts))


# ─── STAR BALANCE ─────────────────────────────────────────────────────────────

async def get_star_balance(acc: Account) -> int:
    """
    Return the account's current Telegram Stars balance (whole stars).
    Returns 0 on 'no stars', -1 if the balance could not be read.
    """
    try:
        res = await acc.client(GetStarsStatusRequest(peer=InputPeerSelf()))
        bal = res.balance
        # Modern layers wrap the balance in StarsAmount(amount, nanos);
        # older ones return a bare int.
        amount = getattr(bal, "amount", bal)
        acc.star_balance = int(amount)
        return acc.star_balance
    except Exception as exc:
        log.warning("[%s] could not read star balance: %s", acc.phone, exc)
        acc.star_balance = -1
        return -1


# ─── TARGET / MESSAGE RESOLUTION ──────────────────────────────────────────────

_MSG_LINK_RE = re.compile(
    r"(?:https?://)?t\.me/(?:c/\d+|[A-Za-z0-9_]+)/(\d+)"
)


def _parse_message_spec(spec: str) -> "tuple[str, object]":
    """
    Classify TARGET_MESSAGE.
    Returns ("ids", [int,...])  or  ("latest", n).
    """
    spec = spec.strip()

    m = _MSG_LINK_RE.fullmatch(spec)
    if m:
        return "ids", [int(m.group(1))]

    if spec.lower().startswith("latest"):
        _, _, n = spec.partition(":")
        count = int(n) if n.strip().isdigit() else 1
        return "latest", max(1, count)

    if "," in spec:
        ids = [int(x) for x in spec.split(",") if x.strip().isdigit()]
        if ids:
            return "ids", ids

    if spec.isdigit():
        return "ids", [int(spec)]

    raise ValueError(f"unrecognised --message value: {spec!r}")


async def resolve_message_ids(client: TelegramClient, channel, spec: str) -> "list[int]":
    """Turn a TARGET_MESSAGE spec into a concrete list of message ids."""
    kind, value = _parse_message_spec(spec)
    if kind == "ids":
        return list(value)

    # kind == "latest"
    msgs = await client.get_messages(channel, limit=int(value))
    ids = [m.id for m in msgs if m is not None]
    if not ids:
        raise ValueError("channel has no readable messages to react to")
    return ids


def _privacy_object():
    return (PaidReactionPrivacyAnonymous()
            if PRIVACY.lower() == "anonymous"
            else PaidReactionPrivacyDefault())


# ─── SEND ONE PAID-STAR REACTION ──────────────────────────────────────────────

async def send_paid_star(
    acc: Account,
    channel,
    msg_id: int,
    requested: int,
    *,
    dry_run: bool = False,
) -> StarResult:
    """
    Send `requested` paid stars from `acc` to message `msg_id` in `channel`.

    Behaviour when balance is insufficient:
      * balance == 0            → SKIPPED_NO_STARS  (account simply has no stars)
      * 0 < balance < requested → send `balance` if CLAMP_TO_BALANCE else skip
    """
    if not acc.healthy():
        return StarResult(acc.phone, msg_id, StarStatus.SKIPPED_UNHEALTHY,
                          detail="account unhealthy")

    if not acc.limiter.allowed():
        wait = acc.limiter.seconds_until_allowed()
        return StarResult(acc.phone, msg_id, StarStatus.SKIPPED_RATELIMIT,
                          detail=f"rate-limited ({wait:.0f}s)")

    # --- decide how many stars this account can actually spend ---------------
    balance = acc.star_balance
    if balance < 0:
        balance = await get_star_balance(acc)

    if balance <= 0:
        log.info("[%s] no stars (balance=%s) — skipping msg %s",
                 acc.phone, balance, msg_id)
        return StarResult(acc.phone, msg_id, StarStatus.SKIPPED_NO_STARS,
                          balance_before=max(balance, 0),
                          detail="zero star balance")

    to_send = requested
    if balance < requested:
        if CLAMP_TO_BALANCE:
            to_send = balance
            log.info("[%s] balance %d < requested %d — clamping to %d",
                     acc.phone, balance, requested, to_send)
        else:
            return StarResult(acc.phone, msg_id, StarStatus.SKIPPED_NO_STARS,
                              balance_before=balance,
                              detail=f"balance {balance} < requested {requested}")

    if dry_run:
        log.info("[DRY-RUN] %s would send %d star(s) to msg %s (balance=%d)",
                 acc.phone, to_send, msg_id, balance)
        return StarResult(acc.phone, msg_id, StarStatus.DRY_RUN,
                          stars_sent=to_send, balance_before=balance,
                          detail="dry-run")

    # --- send with retry/backoff --------------------------------------------
    for attempt in range(1, MAX_SEND_RETRIES + 1):
        try:
            await acc.client(SendPaidReactionRequest(
                peer=channel,
                msg_id=msg_id,
                count=to_send,
                random_id=random.randrange(-(2 ** 63), 2 ** 63),
                private=_privacy_object(),
            ))
            acc.limiter.record()
            acc.mark_ok()
            acc.star_balance = max(0, balance - to_send)
            log.info("[%s] ★ sent %d star(s) to msg %s (balance %d → %d)",
                     acc.phone, to_send, msg_id, balance, acc.star_balance)
            return StarResult(acc.phone, msg_id, StarStatus.SENT,
                              stars_sent=to_send, balance_before=balance)

        except FloodWaitError as e:
            if e.seconds > FLOOD_MAX_WAIT_SEC:
                log.warning("[%s] FloodWait %ds > cap — skipping msg %s",
                            acc.phone, e.seconds, msg_id)
                return StarResult(acc.phone, msg_id, StarStatus.FLOOD_WAIT,
                                  balance_before=balance,
                                  detail=f"floodwait {e.seconds}s")
            wait = e.seconds + random.uniform(0.5, 2.0)
            log.warning("[%s] FloodWait %ds — sleeping %.0fs (attempt %d)",
                        acc.phone, e.seconds, wait, attempt)
            await asyncio.sleep(wait)

        except (ChannelPrivateError, ChatAdminRequiredError) as e:
            log.error("[%s] cannot access channel: %s", acc.phone, e)
            return StarResult(acc.phone, msg_id, StarStatus.FAILED,
                              balance_before=balance, detail=f"access: {e}")

        except MsgIdInvalidError as e:
            log.error("[%s] invalid message id %s: %s", acc.phone, msg_id, e)
            return StarResult(acc.phone, msg_id, StarStatus.FAILED,
                              balance_before=balance, detail="bad msg id")

        except (AuthKeyUnregisteredError, UserDeactivatedBanError) as e:
            acc._failures = acc.MAX_FAIL
            return StarResult(acc.phone, msg_id, StarStatus.BANNED,
                              balance_before=balance, detail=str(e))

        except RPCError as e:
            msg = (getattr(e, "message", "") or str(e)).upper()
            # Insufficient balance surfaces as a plain RPC error on some layers.
            if "BALANCE" in msg and "LOW" in msg:
                log.info("[%s] server reports low balance — skipping", acc.phone)
                return StarResult(acc.phone, msg_id, StarStatus.SKIPPED_NO_STARS,
                                  balance_before=balance, detail="BALANCE_TOO_LOW")
            acc.mark_fail()
            if attempt >= MAX_SEND_RETRIES:
                return StarResult(acc.phone, msg_id, StarStatus.FAILED,
                                  balance_before=balance, detail=str(e))
            await asyncio.sleep(RETRY_BASE_DELAY * attempt)

        except Exception as e:
            acc.mark_fail()
            log.warning("[%s] send error (attempt %d): %s", acc.phone, attempt, e)
            if attempt >= MAX_SEND_RETRIES:
                return StarResult(acc.phone, msg_id, StarStatus.FAILED,
                                  balance_before=balance, detail=str(e))
            await asyncio.sleep(RETRY_BASE_DELAY * attempt)

    return StarResult(acc.phone, msg_id, StarStatus.FAILED,
                      balance_before=balance, detail="exhausted retries")


# ─── ORCHESTRATION ────────────────────────────────────────────────────────────

async def run_campaign(
    accounts: "list[Account]",
    channel_spec: str,
    message_spec: str,
    stars: int,
    *,
    dry_run: bool = False,
) -> "list[StarResult]":
    """Resolve the target, then drive every account across every message id."""
    # Pick the first connected, healthy account to resolve the target entity
    # and the message ids (any member account can do this).
    resolver = next((a for a in accounts if a.client and a.healthy()), None)
    if resolver is None:
        raise RuntimeError("no connected account available to resolve the target")

    channel = await resolver.client.get_input_entity(channel_spec)
    msg_ids = await resolve_message_ids(resolver.client, channel, message_spec)
    log.info("target channel resolved; message ids: %s", msg_ids)

    results: "list[StarResult]" = []

    for msg_id in msg_ids:
        log.info("── message %s ──────────────────────────────", msg_id)

        if MODE == "simultaneous":
            batch = await asyncio.gather(*[
                send_paid_star(acc, channel, msg_id, stars, dry_run=dry_run)
                for acc in accounts if acc.client
            ])
            results.extend(batch)
        else:  # staggered
            for acc in accounts:
                if not acc.client:
                    continue
                res = await send_paid_star(acc, channel, msg_id, stars, dry_run=dry_run)
                results.append(res)
                if res.status in (StarStatus.SENT, StarStatus.DRY_RUN):
                    await asyncio.sleep(random.uniform(STAGGER_MIN_SEC, STAGGER_MAX_SEC))

    return results


# ─── REPORT ───────────────────────────────────────────────────────────────────

def print_report(results: "list[StarResult]") -> None:
    from collections import Counter
    by_status = Counter(r.status for r in results)
    total_stars = sum(r.stars_sent for r in results)

    print("\n" + "=" * 62)
    print(" auto_star — campaign summary")
    print("=" * 62)
    label = {
        StarStatus.SENT:              "sent",
        StarStatus.DRY_RUN:           "dry-run (would send)",
        StarStatus.SKIPPED_NO_STARS:  "skipped — no stars",
        StarStatus.SKIPPED_UNHEALTHY: "skipped — unhealthy",
        StarStatus.SKIPPED_RATELIMIT: "skipped — rate-limited",
        StarStatus.FLOOD_WAIT:        "flood-wait",
        StarStatus.BANNED:            "banned/dead",
        StarStatus.FAILED:            "failed",
    }
    for status, text in label.items():
        n = by_status.get(status, 0)
        if n:
            print(f"  {text:<26} {n}")
    print("-" * 62)
    print(f"  total star operations       {len(results)}")
    print(f"  total stars sent            {total_stars}")
    print("=" * 62 + "\n")


# ─── CLI ──────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="auto_star.py",
        description="Send automated paid-star reactions from a pool of accounts.",
    )
    p.add_argument("--channel", default=TARGET_CHANNEL,
                   help="target channel (@user, t.me link, or numeric id)")
    p.add_argument("--message", default=TARGET_MESSAGE,
                   help="message id / id,list / 'latest' / 'latest:N' / message link")
    p.add_argument("--stars", type=int, default=STARS_PER_ACCOUNT,
                   help="stars each account sends per message (default 1)")
    p.add_argument("--mode", choices=["staggered", "simultaneous"], default=MODE)
    p.add_argument("--anonymous", action="store_true",
                   help="send reactions anonymously")
    p.add_argument("--no-clamp", action="store_true",
                   help="skip accounts whose balance < --stars instead of clamping")
    p.add_argument("--csv-dir", default=ACCOUNTS_CSV_DIR,
                   help="folder holding the account .csv files")
    p.add_argument("--api-id", type=int, default=API_ID)
    p.add_argument("--api-hash", default=API_HASH)
    p.add_argument("--concurrency", type=int, default=5,
                   help="parallel connections during startup")
    p.add_argument("--dry-run", action="store_true",
                   help="resolve + report balances but send nothing")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p


async def main_async(args: argparse.Namespace) -> int:
    global API_ID, API_HASH, TARGET_CHANNEL, TARGET_MESSAGE, STARS_PER_ACCOUNT
    global MODE, PRIVACY, CLAMP_TO_BALANCE, ACCOUNTS_CSV_DIR

    API_ID = args.api_id
    API_HASH = args.api_hash
    TARGET_CHANNEL = args.channel
    TARGET_MESSAGE = args.message
    STARS_PER_ACCOUNT = args.stars
    MODE = args.mode
    PRIVACY = "anonymous" if args.anonymous else "default"
    CLAMP_TO_BALANCE = not args.no_clamp
    ACCOUNTS_CSV_DIR = args.csv_dir

    accounts = load_accounts(ACCOUNTS_CSV_DIR)
    if not accounts:
        log.error("no accounts loaded — nothing to do")
        return 2

    log.info("connecting %d account(s) ...", len(accounts))
    await connect_all(accounts, concurrency=args.concurrency)

    live = [a for a in accounts if a.client and a.healthy()]
    if not live:
        log.error("no account connected successfully")
        return 3

    # Pre-read balances so the staggered loop has them cached and the report
    # of 'who has stars' is accurate up front.
    log.info("reading star balances ...")
    await asyncio.gather(*(get_star_balance(a) for a in live))
    with_stars = [a for a in live if a.star_balance > 0]
    log.info("%d/%d connected account(s) hold stars",
             len(with_stars), len(live))

    try:
        results = await run_campaign(
            accounts, TARGET_CHANNEL, TARGET_MESSAGE, STARS_PER_ACCOUNT,
            dry_run=args.dry_run,
        )
    finally:
        for a in accounts:
            if a.client:
                try:
                    await a.client.disconnect()
                except Exception:
                    pass

    print_report(results)
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
