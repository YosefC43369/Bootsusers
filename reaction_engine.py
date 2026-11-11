"""
reaction_engine.py — core engine for auto_react.

Provides:
  - ReactionQueue          : priority-aware async queue with per-chat emoji config
  - ChatConfig             : per-chat settings (custom emoji pool, blacklist flag)
  - ChatConfigRegistry     : load/save chat configs from JSON
  - PriorityReactionJob    : a queued work unit with priority level
  - ReactionEngine         : orchestrates queue → rate-limit → send loop
  - EngineMetrics          : live counters exposed for dashboard/reporter use
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import threading
import itertools
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import IntEnum, auto
from pathlib import Path
from typing import Optional, Callable, Awaitable

from telethon.tl.functions.messages import SendReactionRequest
from telethon.tl.types import ReactionEmoji
from telethon.errors import FloodWaitError, UserDeactivatedBanError


# ─── RE-USE TYPES FROM auto_react ─────────────────────────────────────────────
# We import the live objects so we do not duplicate dataclass definitions.
# If reaction_engine is run standalone (tests, etc.) we provide minimal stubs.

try:
    from auto_react import Account, RateLimiter, REACTION_POOL
except ImportError:  # pragma: no cover
    # minimal stubs so the module imports cleanly in isolation
    @dataclass
    class Account:  # type: ignore[no-redef]
        phone: str
        session: str
        client: object = None
        _consecutive_failures: int = 0
        MAX_FAILURES: int = 5
        def is_healthy(self) -> bool: return self._consecutive_failures < self.MAX_FAILURES
        def mark_ok(self) -> None:    self._consecutive_failures = 0
        def mark_fail(self) -> None:  self._consecutive_failures += 1

    class RateLimiter:  # type: ignore[no-redef]
        def allowed(self) -> bool:              return True
        def record(self) -> None:               pass
        def seconds_until_allowed(self) -> float: return 0.0

    REACTION_POOL: list[str] = ["👍", "❤️", "🔥", "🎉", "👏"]


# ─── LOGGING ──────────────────────────────────────────────────────────────────

log = logging.getLogger("reaction_engine")


# ─── PRIORITY LEVELS ──────────────────────────────────────────────────────────

class Priority(IntEnum):
    """
    Reaction job priority.

    LOW     = background reactions (auto-triggered, low importance chats)
    NORMAL  = standard auto-react triggered by on_send
    HIGH    = manually enqueued or escalated (e.g. replies to the sender)
    URGENT  = force-react regardless of normal rate limits (use sparingly)
    """
    LOW    = 0
    NORMAL = 1
    HIGH   = 2
    URGENT = 3


# ─── PER-CHAT CONFIGURATION ───────────────────────────────────────────────────

@dataclass
class ChatConfig:
    """
    Per-chat settings for the reaction engine.

    Fields
    ------
    chat_id         : Telegram chat ID (integer or negative for channels)
    blacklisted     : if True, the engine will never react in this chat
    emoji_pool      : custom emoji list; if empty, falls back to global REACTION_POOL
    max_per_hour    : override per-hour cap for this chat (0 = use account limiter default)
    min_gap_sec     : override minimum gap between reactions in this chat (0 = use default)
    priority_boost  : add this value to every job's priority when it comes from this chat
    notes           : free-form annotation (not used by engine logic)
    """
    chat_id:        int
    blacklisted:    bool       = False
    emoji_pool:     list[str]  = field(default_factory=list)
    max_per_hour:   int        = 0
    min_gap_sec:    float      = 0.0
    priority_boost: int        = 0
    notes:          str        = ""

    def effective_emoji_pool(self) -> list[str]:
        """Return this chat's emoji pool, or the global fallback."""
        return self.emoji_pool if self.emoji_pool else REACTION_POOL

    def to_dict(self) -> dict:
        return {
            "chat_id":        self.chat_id,
            "blacklisted":    self.blacklisted,
            "emoji_pool":     self.emoji_pool,
            "max_per_hour":   self.max_per_hour,
            "min_gap_sec":    self.min_gap_sec,
            "priority_boost": self.priority_boost,
            "notes":          self.notes,
        }

    @classmethod
    def from_dict(cls, d: dict) -> ChatConfig:
        return cls(
            chat_id        = int(d["chat_id"]),
            blacklisted    = bool(d.get("blacklisted", False)),
            emoji_pool     = list(d.get("emoji_pool", [])),
            max_per_hour   = int(d.get("max_per_hour", 0)),
            min_gap_sec    = float(d.get("min_gap_sec", 0.0)),
            priority_boost = int(d.get("priority_boost", 0)),
            notes          = str(d.get("notes", "")),
        )


# ─── CHAT CONFIG REGISTRY ─────────────────────────────────────────────────────

class ChatConfigRegistry:
    """
    Loads, stores and hot-reloads per-chat config from a JSON file.

    JSON format:
      [
        {"chat_id": -1001234567890, "blacklisted": false, "emoji_pool": ["🔥","👏"]},
        {"chat_id": -1009876543210, "blacklisted": true,  "notes": "spam channel"},
        ...
      ]

    If the file does not exist it is created as an empty list on first save.
    Thread-safe reads; write operations acquire _lock.
    """

    def __init__(self, path: str | Path = "chat_configs.json"):
        self._path: Path                     = Path(path)
        self._configs: dict[int, ChatConfig] = {}
        self._lock = threading.Lock()

    # ── persistence ──────────────────────────────────────────────────────────

    @classmethod
    def load(cls, path: str | Path = "chat_configs.json") -> ChatConfigRegistry:
        """Create a registry and load existing config file (if any)."""
        reg = cls(path)
        reg.reload()
        return reg

    def reload(self) -> None:
        """Re-read the JSON file from disk.  Safe to call at any time."""
        if not self._path.exists():
            log.info(f"chat config file not found ({self._path}), starting empty")
            return
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            with self._lock:
                self._configs = {
                    int(entry["chat_id"]): ChatConfig.from_dict(entry)
                    for entry in raw
                }
            log.info(f"loaded {len(self._configs)} chat configs from {self._path}")
        except Exception as e:
            log.error(f"failed to load chat configs: {e}")

    def save(self) -> None:
        """Persist current state to the JSON file."""
        with self._lock:
            data = [cfg.to_dict() for cfg in self._configs.values()]
        try:
            self._path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            log.info(f"saved {len(data)} chat configs to {self._path}")
        except Exception as e:
            log.error(f"failed to save chat configs: {e}")

    # ── access ───────────────────────────────────────────────────────────────

    def get(self, chat_id: int) -> ChatConfig:
        """Return the config for chat_id, or a fresh default if not found."""
        with self._lock:
            return self._configs.get(chat_id, ChatConfig(chat_id=chat_id))

    def set(self, cfg: ChatConfig) -> None:
        """Add or replace a config entry."""
        with self._lock:
            self._configs[cfg.chat_id] = cfg

    def blacklist(self, chat_id: int, reason: str = "") -> None:
        """Convenience: blacklist a chat and persist."""
        cfg = self.get(chat_id)
        cfg.blacklisted = True
        cfg.notes       = reason or cfg.notes
        self.set(cfg)
        self.save()

    def whitelist(self, chat_id: int) -> None:
        """Remove blacklist flag and persist."""
        cfg = self.get(chat_id)
        cfg.blacklisted = False
        self.set(cfg)
        self.save()

    def is_blacklisted(self, chat_id: int) -> bool:
        return self.get(chat_id).blacklisted

    def all_configs(self) -> list[ChatConfig]:
        with self._lock:
            return list(self._configs.values())


# ─── REACTION JOB ─────────────────────────────────────────────────────────────

@dataclass(order=True)
class PriorityReactionJob:
    """
    A single unit of work for the reaction engine.

    sort_key    : lower value = higher priority in a min-heap.
                  We negate priority so URGENT (3) → sort_key = -3, pops first.
    enqueued_at : monotonic timestamp for age-based tie-breaking and metrics.
    """
    sort_key:    float = field(compare=True,  default=0.0)
    chat_id:     int   = field(compare=False, default=0)
    msg_id:      int   = field(compare=False, default=0)
    priority:    int   = field(compare=False, default=Priority.NORMAL)
    enqueued_at: float = field(compare=False, default_factory=time.monotonic)
    # optional: a specific subset of accounts to use (None = use all)
    account_filter: Optional[set[str]] = field(compare=False, default=None)

    @classmethod
    def make(
        cls,
        chat_id:        int,
        msg_id:         int,
        priority:       int   = Priority.NORMAL,
        account_filter: Optional[set[str]] = None,
    ) -> PriorityReactionJob:
        return cls(
            sort_key       = -float(priority),   # negate: higher priority → smaller key
            chat_id        = chat_id,
            msg_id         = msg_id,
            priority       = priority,
            enqueued_at    = time.monotonic(),
            account_filter = account_filter,
        )


# ─── METRICS ──────────────────────────────────────────────────────────────────

@dataclass
class EngineMetrics:
    """
    Live counters — thread-safe via threading.Lock.
    Exposed for integration with metrics.py / dashboard.py.
    """
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    total_enqueued:  int = 0
    total_sent:      int = 0
    total_skipped:   int = 0   # rate-limited or unhealthy
    total_failed:    int = 0   # Telethon errors
    total_flooded:   int = 0   # FloodWaitError events
    total_banned:    int = 0   # UserDeactivatedBanError
    blacklist_drops: int = 0   # jobs dropped at enqueue time

    # running totals per chat  (chat_id → sent count)
    per_chat: dict[int, int] = field(default_factory=dict)
    # running totals per account (phone → sent count)
    per_account: dict[str, int] = field(default_factory=dict)

    # timing — last N send latencies (seconds) for P50/P95 calcs
    _latencies: deque = field(
        default_factory=lambda: deque(maxlen=200), repr=False
    )

    def bump_enqueued(self):
        with self._lock: self.total_enqueued += 1

    def bump_sent(self, chat_id: int, phone: str, latency: float):
        with self._lock:
            self.total_sent += 1
            self.per_chat[chat_id]    = self.per_chat.get(chat_id, 0) + 1
            self.per_account[phone]   = self.per_account.get(phone, 0) + 1
            self._latencies.append(latency)

    def bump_skipped(self):
        with self._lock: self.total_skipped += 1

    def bump_failed(self):
        with self._lock: self.total_failed += 1

    def bump_flooded(self):
        with self._lock: self.total_flooded += 1

    def bump_banned(self):
        with self._lock: self.total_banned += 1

    def bump_blacklist_drop(self):
        with self._lock: self.blacklist_drops += 1

    def latency_p50(self) -> float:
        """Median send latency over the last 200 operations."""
        with self._lock:
            if not self._latencies:
                return 0.0
            s = sorted(self._latencies)
            return s[len(s) // 2]

    def latency_p95(self) -> float:
        with self._lock:
            if not self._latencies:
                return 0.0
            s = sorted(self._latencies)
            return s[int(len(s) * 0.95)]

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "total_enqueued":  self.total_enqueued,
                "total_sent":      self.total_sent,
                "total_skipped":   self.total_skipped,
                "total_failed":    self.total_failed,
                "total_flooded":   self.total_flooded,
                "total_banned":    self.total_banned,
                "blacklist_drops": self.blacklist_drops,
                "latency_p50_ms":  round(self.latency_p50() * 1000, 1),
                "latency_p95_ms":  round(self.latency_p95() * 1000, 1),
                "per_chat":        dict(self.per_chat),
                "per_account":     dict(self.per_account),
            }


# ─── PER-ACCOUNT EMOJI STATE ──────────────────────────────────────────────────

class EmojiCycler:
    """
    Maintains a per-(account, chat) emoji iterator so each account cycles
    through the emoji pool independently, and different chats can have
    different pools without interfering.

      cycler = EmojiCycler()
      emoji  = cycler.next(phone=acc.phone, chat_id=chat_id, pool=["👍","❤️"])
    """

    def __init__(self):
        # (phone, chat_id) → itertools.cycle
        self._iters: dict[tuple[str, int], itertools.cycle] = {}
        self._lock = threading.Lock()

    def next(self, phone: str, chat_id: int, pool: list[str]) -> str:
        key = (phone, chat_id)
        with self._lock:
            if key not in self._iters:
                # start at a random offset so accounts don't all begin on 👍
                offset = random.randint(0, max(len(pool) - 1, 0))
                shifted = pool[offset:] + pool[:offset]
                self._iters[key] = itertools.cycle(shifted)
            return next(self._iters[key])

    def reset(self, phone: str, chat_id: int) -> None:
        with self._lock:
            self._iters.pop((phone, chat_id), None)

    def reset_account(self, phone: str) -> None:
        with self._lock:
            keys = [k for k in self._iters if k[0] == phone]
            for k in keys:
                del self._iters[k]


# ─── REACTION ENGINE ──────────────────────────────────────────────────────────

class ReactionEngine:
    """
    Priority-aware reaction engine.

    Responsibilities
    ----------------
    1. Accept jobs via enqueue() — checks blacklist immediately at enqueue time.
    2. Dispatch jobs from an asyncio.PriorityQueue to worker coroutines.
    3. Per-job: select healthy accounts, check rate limits, send reaction,
       record metrics, handle FloodWait / ban errors.
    4. Expose metrics via self.metrics for external monitoring.

    Parameters
    ----------
    accounts        : list of Account objects (from auto_react.py)
    registry        : ChatConfigRegistry (per-chat config + blacklist)
    concurrency     : max simultaneous reaction sends across all accounts/chats
    worker_count    : number of asyncio worker tasks draining the queue
    flood_backoff   : extra seconds added on top of Telegram's FloodWait
    max_retries     : per-job retry attempts on transient errors
    on_sent         : optional async callback(account, chat_id, msg_id, emoji)
    on_failed       : optional async callback(account, chat_id, msg_id, error)
    """

    def __init__(
        self,
        accounts:     list[Account],
        registry:     ChatConfigRegistry,
        concurrency:  int   = 5,
        worker_count: int   = 3,
        flood_backoff: float = 3.0,
        max_retries:  int   = 2,
        on_sent:   Optional[Callable[..., Awaitable[None]]] = None,
        on_failed: Optional[Callable[..., Awaitable[None]]] = None,
    ):
        self.accounts      = accounts
        self.registry      = registry
        self.concurrency   = concurrency
        self.worker_count  = worker_count
        self.flood_backoff = flood_backoff
        self.max_retries   = max_retries
        self.on_sent       = on_sent
        self.on_failed     = on_failed

        self._queue:      asyncio.PriorityQueue[PriorityReactionJob] = asyncio.PriorityQueue()
        self._semaphore:  asyncio.Semaphore = asyncio.Semaphore(concurrency)
        self._emoji:      EmojiCycler       = EmojiCycler()
        self.metrics:     EngineMetrics     = EngineMetrics()
        self._running:    bool              = False
        self._tasks:      list[asyncio.Task] = []

    # ── public API ───────────────────────────────────────────────────────────

    async def enqueue(
        self,
        chat_id:        int,
        msg_id:         int,
        priority:       int                    = Priority.NORMAL,
        account_filter: Optional[set[str]]     = None,
    ) -> bool:
        """
        Add a reaction job to the queue.

        Returns False immediately if chat_id is blacklisted (job is dropped).
        Applies priority_boost from ChatConfig before inserting.
        """
        cfg = self.registry.get(chat_id)
        if cfg.blacklisted:
            log.debug(f"[engine] blacklisted chat {chat_id} — drop job")
            self.metrics.bump_blacklist_drop()
            return False

        boosted = min(int(Priority.URGENT), priority + cfg.priority_boost)
        job = PriorityReactionJob.make(
            chat_id        = chat_id,
            msg_id         = msg_id,
            priority       = boosted,
            account_filter = account_filter,
        )
        await self._queue.put(job)
        self.metrics.bump_enqueued()
        log.debug(f"[engine] enqueued job chat={chat_id} msg={msg_id} prio={boosted}")
        return True

    async def enqueue_urgent(self, chat_id: int, msg_id: int) -> bool:
        """Shorthand: enqueue at URGENT priority."""
        return await self.enqueue(chat_id, msg_id, priority=Priority.URGENT)

    async def run(self) -> None:
        """
        Start the engine.  Spawns worker_count worker tasks and runs until
        the coroutine is cancelled.

        Typical usage:
          asyncio.create_task(engine.run())
        """
        self._running = True
        self._tasks = [
            asyncio.create_task(self._worker(worker_id=i))
            for i in range(self.worker_count)
        ]
        log.info(
            f"[engine] started — {self.worker_count} workers, "
            f"concurrency={self.concurrency}"
        )
        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            log.info("[engine] cancelled — shutting down workers")
            for t in self._tasks:
                t.cancel()
            raise

    async def stop(self) -> None:
        """Graceful stop: drain the queue, then cancel workers."""
        await self._queue.join()
        for t in self._tasks:
            t.cancel()
        self._running = False
        log.info("[engine] stopped")

    def queue_depth(self) -> int:
        return self._queue.qsize()

    # ── internals ────────────────────────────────────────────────────────────

    async def _worker(self, worker_id: int) -> None:
        """Single worker task — pulls jobs and dispatches them."""
        log.debug(f"[engine] worker-{worker_id} started")
        while True:
            job: PriorityReactionJob = await self._queue.get()
            try:
                async with self._semaphore:
                    await self._dispatch(job)
            except Exception as e:
                log.error(f"[engine] worker-{worker_id} unhandled error: {e}")
            finally:
                self._queue.task_done()

    async def _dispatch(self, job: PriorityReactionJob) -> None:
        """
        Execute one reaction job across all eligible accounts.
        Each account sends its own reaction; they run concurrently via gather.
        """
        cfg   = self.registry.get(job.chat_id)
        pool  = cfg.effective_emoji_pool()

        eligible = [
            acc for acc in self.accounts
            if acc.is_healthy()
            and acc.client is not None
            and (job.account_filter is None or acc.phone in job.account_filter)
        ]

        if not eligible:
            log.warning(
                f"[engine] no eligible accounts for chat={job.chat_id} "
                f"msg={job.msg_id}"
            )
            self.metrics.bump_skipped()
            return

        # fire all eligible accounts in parallel (semaphore already acquired above)
        results = await asyncio.gather(
            *[
                self._send_one(acc, job, pool, cfg)
                for acc in eligible
            ],
            return_exceptions=True,
        )

        sent = sum(1 for r in results if r is True)
        log.info(
            f"[engine] chat={job.chat_id} msg={job.msg_id} "
            f"sent={sent}/{len(eligible)} prio={job.priority}"
        )

    async def _send_one(
        self,
        acc:  Account,
        job:  PriorityReactionJob,
        pool: list[str],
        cfg:  ChatConfig,
    ) -> bool:
        """
        Send a single reaction from one account.
        Returns True on success, False on skip/failure.
        """
        # URGENT priority bypasses rate limiter
        if job.priority < Priority.URGENT:
            if not acc.limiter.allowed():
                wait = acc.limiter.seconds_until_allowed()
                log.debug(
                    f"  [{acc.phone}] rate-limited on chat={job.chat_id} "
                    f"— next in {wait:.0f}s"
                )
                self.metrics.bump_skipped()
                return False

        emoji = self._emoji.next(
            phone   = acc.phone,
            chat_id = job.chat_id,
            pool    = pool,
        )

        t_start = time.monotonic()
        for attempt in range(1, self.max_retries + 1):
            try:
                await acc.client(SendReactionRequest(
                    peer    = job.chat_id,
                    msg_id  = job.msg_id,
                    reaction = [ReactionEmoji(emoticon=emoji)],
                ))
                latency = time.monotonic() - t_start
                acc.limiter.record()
                acc.mark_ok()
                self.metrics.bump_sent(job.chat_id, acc.phone, latency)
                log.info(f"  [{acc.phone}] ✓ {emoji} → chat={job.chat_id} "
                         f"msg={job.msg_id} ({latency*1000:.0f}ms)")

                if self.on_sent:
                    try:
                        await self.on_sent(acc, job.chat_id, job.msg_id, emoji)
                    except Exception as cb_err:
                        log.warning(f"  [{acc.phone}] on_sent callback error: {cb_err}")

                return True

            except FloodWaitError as e:
                self.metrics.bump_flooded()
                extra  = random.uniform(1.0, self.flood_backoff)
                total  = e.seconds + extra
                log.warning(
                    f"  [{acc.phone}] FloodWait {e.seconds}s "
                    f"(+{extra:.1f}s jitter) attempt {attempt}/{self.max_retries}"
                )
                if attempt < self.max_retries:
                    await asyncio.sleep(total)
                else:
                    acc.mark_fail()
                    self.metrics.bump_failed()
                    if self.on_failed:
                        try:
                            await self.on_failed(acc, job.chat_id, job.msg_id, e)
                        except Exception: pass
                    return False

            except UserDeactivatedBanError:
                log.error(f"  [{acc.phone}] BANNED — marking permanently unhealthy")
                acc._consecutive_failures = acc.MAX_FAILURES
                self.metrics.bump_banned()
                self._emoji.reset_account(acc.phone)
                if self.on_failed:
                    try:
                        await self.on_failed(acc, job.chat_id, job.msg_id,
                                             UserDeactivatedBanError())
                    except Exception: pass
                return False

            except Exception as e:
                acc.mark_fail()
                self.metrics.bump_failed()
                log.warning(
                    f"  [{acc.phone}] error attempt {attempt}/{self.max_retries}: {e}"
                )
                if attempt < self.max_retries:
                    backoff = 2.0 ** (attempt - 1) + random.uniform(0.5, 2.0)
                    await asyncio.sleep(backoff)
                else:
                    if self.on_failed:
                        try:
                            await self.on_failed(acc, job.chat_id, job.msg_id, e)
                        except Exception: pass
                    return False

        return False


# ─── QUEUE INSPECTOR ──────────────────────────────────────────────────────────

class QueueInspector:
    """
    Read-only view into the engine's state for monitoring/dashboard use.
    Attach to a running ReactionEngine and poll for stats.

      inspector = QueueInspector(engine)
      print(inspector.report())
    """

    def __init__(self, engine: ReactionEngine):
        self._engine = engine

    def report(self) -> dict:
        snap = self._engine.metrics.snapshot()
        snap["queue_depth"]   = self._engine.queue_depth()
        snap["worker_count"]  = self._engine.worker_count
        snap["concurrency"]   = self._engine.concurrency
        return snap

    def print_report(self) -> None:
        r = self.report()
        print("\n── Reaction Engine Status ──────────────────────")
        print(f"  queue depth   : {r['queue_depth']}")
        print(f"  workers       : {r['worker_count']}  concurrency={r['concurrency']}")
        print(f"  enqueued      : {r['total_enqueued']}")
        print(f"  sent          : {r['total_sent']}")
        print(f"  skipped       : {r['total_skipped']}")
        print(f"  failed        : {r['total_failed']}")
        print(f"  flood events  : {r['total_flooded']}")
        print(f"  bans          : {r['total_banned']}")
        print(f"  bl drops      : {r['blacklist_drops']}")
        print(f"  latency P50   : {r['latency_p50_ms']} ms")
        print(f"  latency P95   : {r['latency_p95_ms']} ms")
        if r["per_account"]:
            print("  per account :")
            for phone, cnt in sorted(r["per_account"].items()):
                bar = "█" * min(cnt, 30)
                print(f"    {phone:<18} {cnt:>5}  {bar}")
        print("────────────────────────────────────────────────\n")