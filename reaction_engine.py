# reaction_engine.py
# connect_all() has run.
#
# Wiring (in auto_react.py main()):
#   from reaction_engine import ReactionEngine, ChatConfigRegistry
#   registry = ChatConfigRegistry("chat_config.json")
#   engine   = ReactionEngine(accounts, registry, concurrency=5)
#   asyncio.create_task(engine.run())
#   # In the NewMessage handler:
#   await engine.enqueue(chat_id, msg_id)

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import math
import random
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import IntEnum
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# ── try to import real types from auto_react; fall back to stubs ──────────────
try:
    from auto_react import (
        Account,
        RateLimiter,
        REACTION_POOL,
        MIN_GAP_BETWEEN_REACTIONS,
        MAX_REACTIONS_PER_HOUR,
        send_reaction,
    )
    _STUB_MODE = False
except ImportError:
    logger.warning("auto_react not found — running in stub mode")
    _STUB_MODE = True

    # ── minimal stubs for standalone testing ──────────────────────────────────
    REACTION_POOL         = ["👍", "❤️", "🔥", "🎉", "👏"]
    MIN_GAP_BETWEEN_REACTIONS = 2.0
    MAX_REACTIONS_PER_HOUR    = 30

    class _RateLimiter:
        def __init__(self, max_per_hour: int = 30, min_gap: float = 2.0):
            self.max_per_hour = max_per_hour
            self.min_gap      = min_gap
            self._lock        = threading.Lock()
            self._timestamps: list[float] = []
            self._last_send   = 0.0

        def can_send(self) -> bool:
            now = time.monotonic()
            with self._lock:
                self._timestamps = [t for t in self._timestamps if now - t < 3600]
                if len(self._timestamps) >= self.max_per_hour:
                    return False
                if now - self._last_send < self.min_gap:
                    return False
                return True

        def mark_sent(self):
            now = time.monotonic()
            with self._lock:
                self._timestamps.append(now)
                self._last_send = now

    RateLimiter = _RateLimiter

    @dataclass
    class Account:
        phone:   str
        session: str
        proxy:   object = None
        limiter: _RateLimiter = field(default_factory=_RateLimiter)
        client:  object = None
        _consecutive_failures: int = 0
        MAX_FAILURES: int = 5

        def is_healthy(self)  -> bool: return self._consecutive_failures < self.MAX_FAILURES
        def mark_ok(self)           : self._consecutive_failures = 0
        def mark_fail(self)         : self._consecutive_failures += 1

    async def send_reaction(acc: Account, chat_id: int, msg_id: int) -> bool:
        await asyncio.sleep(random.uniform(0.1, 0.3))
        logger.debug("[stub] send_reaction acc=%s chat=%s msg=%s",
                     acc.phone, chat_id, msg_id)
        return True


# ══════════════════════════════════════════════════════════════════════════════
#  PRIORITY ENUM
# ══════════════════════════════════════════════════════════════════════════════

class Priority(IntEnum):
    LOW    = 0
    NORMAL = 1
    HIGH   = 2
    URGENT = 3


# ══════════════════════════════════════════════════════════════════════════════
#  CHAT CONFIG
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ChatConfig:
    """Per-chat reaction settings.  Stored and loaded from JSON."""
    chat_id:       int
    blacklisted:   bool      = False
    emoji_pool:    list[str] = field(default_factory=list)
    max_per_hour:  int       = 0        # 0 = use global default
    min_gap_sec:   float     = 0.0      # 0.0 = use global default
    priority_boost: int      = 0        # added to incoming priority
    notes:         str       = ""

    def effective_emoji_pool(self) -> list[str]:
        return self.emoji_pool if self.emoji_pool else list(REACTION_POOL)

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
    def from_dict(cls, d: dict) -> "ChatConfig":
        return cls(
            chat_id        = int(d["chat_id"]),
            blacklisted    = bool(d.get("blacklisted", False)),
            emoji_pool     = list(d.get("emoji_pool", [])),
            max_per_hour   = int(d.get("max_per_hour", 0)),
            min_gap_sec    = float(d.get("min_gap_sec", 0.0)),
            priority_boost = int(d.get("priority_boost", 0)),
            notes          = str(d.get("notes", "")),
        )


# ══════════════════════════════════════════════════════════════════════════════
#  CHAT CONFIG REGISTRY
# ══════════════════════════════════════════════════════════════════════════════

class ChatConfigRegistry:
    """
    JSON-backed per-chat config store.
    Thread-safe; reloads from disk when the file changes (mtime-checked).
    """

    def __init__(self, path: str = "chat_config.json"):
        self._path:    Path                   = Path(path)
        self._lock:    threading.Lock         = threading.Lock()
        self._configs: dict[int, ChatConfig]  = {}
        self._mtime:   float                  = 0.0
        self.load()

    # ── persistence ──────────────────────────────────────────────────────────

    def load(self) -> None:
        if not self._path.exists():
            return
        try:
            mtime = self._path.stat().st_mtime
            with self._lock:
                raw = json.loads(self._path.read_text(encoding="utf-8"))
                self._configs = {
                    int(k): ChatConfig.from_dict(v)
                    for k, v in raw.items()
                }
                self._mtime = mtime
        except Exception as e:
            logger.warning("ChatConfigRegistry.load: %s", e)

    def reload(self) -> bool:
        """Reload only if file changed on disk.  Returns True if reloaded."""
        if not self._path.exists():
            return False
        mtime = self._path.stat().st_mtime
        if mtime != self._mtime:
            self.load()
            return True
        return False

    def save(self) -> None:
        with self._lock:
            data = {str(k): v.to_dict() for k, v in self._configs.items()}
        try:
            self._path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            self._mtime = self._path.stat().st_mtime
        except Exception as e:
            logger.error("ChatConfigRegistry.save: %s", e)

    # ── accessors ─────────────────────────────────────────────────────────────

    def get(self, chat_id: int) -> ChatConfig:
        with self._lock:
            return self._configs.get(chat_id, ChatConfig(chat_id=chat_id))

    def set(self, cfg: ChatConfig) -> None:
        with self._lock:
            self._configs[cfg.chat_id] = cfg
        self.save()

    def blacklist(self, chat_id: int, reason: str = "") -> None:
        cfg = self.get(chat_id)
        cfg.blacklisted = True
        cfg.notes       = reason or cfg.notes
        self.set(cfg)

    def whitelist(self, chat_id: int) -> None:
        cfg = self.get(chat_id)
        cfg.blacklisted = False
        self.set(cfg)

    def is_blacklisted(self, chat_id: int) -> bool:
        return self.get(chat_id).blacklisted

    def all_configs(self) -> list[ChatConfig]:
        with self._lock:
            return list(self._configs.values())


# ══════════════════════════════════════════════════════════════════════════════
#  PRIORITY REACTION JOB
# ══════════════════════════════════════════════════════════════════════════════

@dataclass(order=True)
class PriorityReactionJob:
    """
    Sortable job for asyncio.PriorityQueue.
    Lower sort_key = pops first → negate priority so URGENT=3 → sort_key=-3.
    """
    sort_key:       int            = field(compare=True)
    chat_id:        int            = field(compare=False)
    msg_id:         int            = field(compare=False)
    priority:       Priority       = field(compare=False, default=Priority.NORMAL)
    enqueued_at:    float          = field(compare=False, default_factory=time.monotonic)
    account_filter: Optional[list] = field(compare=False, default=None)
    # account_filter: list[str] of phone numbers to restrict to, or None = all

    @classmethod
    def make(
        cls,
        chat_id:        int,
        msg_id:         int,
        priority:       Priority       = Priority.NORMAL,
        account_filter: Optional[list] = None,
        priority_boost: int            = 0,
    ) -> "PriorityReactionJob":
        effective = min(Priority.URGENT, priority + priority_boost)
        return cls(
            sort_key       = -int(effective),
            chat_id        = chat_id,
            msg_id         = msg_id,
            priority       = Priority(effective),
            account_filter = account_filter,
        )


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE METRICS
# ══════════════════════════════════════════════════════════════════════════════

class EngineMetrics:
    """
    Thread-safe counters + rolling-window latency tracker.
    All public methods are safe to call from any thread or coroutine.
    """

    def __init__(self):
        self._lock = threading.Lock()
        # global counters
        self.total_enqueued:  int = 0
        self.total_sent:      int = 0
        self.total_skipped:   int = 0
        self.total_failed:    int = 0
        self.total_flooded:   int = 0
        self.total_banned:    int = 0
        self.blacklist_drops: int = 0
        # per-chat  {chat_id: {"sent":n,"failed":n}}
        self._per_chat:    dict[int, dict] = {}
        # per-account {phone: {"sent":n,"failed":n}}
        self._per_account: dict[str, dict] = {}
        # latency ring buffer (seconds from enqueue to send)
        self._latencies: deque[float] = deque(maxlen=200)

    # ── bump helpers ──────────────────────────────────────────────────────────

    def bump_enqueued(self, n: int = 1):
        with self._lock: self.total_enqueued += n

    def bump_sent(self, chat_id: int, phone: str, latency: float):
        with self._lock:
            self.total_sent += 1
            self._latencies.append(latency)
            self._per_chat.setdefault(chat_id, {"sent": 0, "failed": 0})
            self._per_chat[chat_id]["sent"] += 1
            self._per_account.setdefault(phone, {"sent": 0, "failed": 0})
            self._per_account[phone]["sent"] += 1

    def bump_skipped(self, n: int = 1):
        with self._lock: self.total_skipped += n

    def bump_failed(self, chat_id: int, phone: str):
        with self._lock:
            self.total_failed += 1
            self._per_chat.setdefault(chat_id, {"sent": 0, "failed": 0})
            self._per_chat[chat_id]["failed"] += 1
            self._per_account.setdefault(phone, {"sent": 0, "failed": 0})
            self._per_account[phone]["failed"] += 1

    def bump_flooded(self, n: int = 1):
        with self._lock: self.total_flooded += n

    def bump_banned(self, phone: str):
        with self._lock:
            self.total_banned += 1
            self._per_account.setdefault(phone, {"sent": 0, "failed": 0})
            self._per_account[phone]["failed"] += 1

    def bump_blacklist_drop(self, n: int = 1):
        with self._lock: self.blacklist_drops += n

    # ── percentile latency ────────────────────────────────────────────────────

    def _percentile(self, pct: float) -> float:
        with self._lock:
            data = sorted(self._latencies)
        if not data:
            return 0.0
        idx = max(0, min(len(data) - 1, int(math.ceil(len(data) * pct / 100)) - 1))
        return data[idx]

    def latency_p50(self) -> float: return self._percentile(50)
    def latency_p95(self) -> float: return self._percentile(95)

    # ── snapshot ──────────────────────────────────────────────────────────────

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
                "latency_p50":     self.latency_p50(),
                "latency_p95":     self.latency_p95(),
                "top_chats": sorted(
                    self._per_chat.items(),
                    key=lambda kv: kv[1]["sent"],
                    reverse=True,
                )[:10],
                "top_accounts": sorted(
                    self._per_account.items(),
                    key=lambda kv: kv[1]["sent"],
                    reverse=True,
                ),
            }

    def log_summary(self):
        s = self.snapshot()
        logger.info(
            "[metrics] enqueued=%d sent=%d skipped=%d failed=%d "
            "flooded=%d banned=%d blk_drops=%d p50=%.2fs p95=%.2fs",
            s["total_enqueued"], s["total_sent"], s["total_skipped"],
            s["total_failed"], s["total_flooded"], s["total_banned"],
            s["blacklist_drops"], s["latency_p50"], s["latency_p95"],
        )


# ══════════════════════════════════════════════════════════════════════════════
#  EMOJI CYCLER
# ══════════════════════════════════════════════════════════════════════════════

class EmojiCycler:
    """
    Per-(phone, chat_id) emoji iterator with random starting offset.
    All 10 accounts start at different positions so the first N reactions
    are spread across the pool rather than all 👍.
    """

    def __init__(self):
        self._iters: dict[tuple[str, int], itertools.cycle] = {}
        self._lock = threading.Lock()

    def _key(self, phone: str, chat_id: int) -> tuple[str, int]:
        return (phone, chat_id)

    def _make_iter(self, pool: list[str]) -> itertools.cycle:
        shuffled = list(pool)
        offset   = random.randint(0, max(0, len(shuffled) - 1))
        shuffled = shuffled[offset:] + shuffled[:offset]
        return itertools.cycle(shuffled)

    def next(self, phone: str, chat_id: int, pool: list[str]) -> str:
        key = self._key(phone, chat_id)
        with self._lock:
            if key not in self._iters:
                self._iters[key] = self._make_iter(pool)
            return next(self._iters[key])

    def reset(self, phone: str, chat_id: int, pool: list[str]) -> None:
        """Force a fresh cycle for this (phone, chat_id) pair."""
        key = self._key(phone, chat_id)
        with self._lock:
            self._iters[key] = self._make_iter(pool)

    def reset_account(self, phone: str) -> None:
        """Drop all iterators for a given phone (e.g. after session restart)."""
        with self._lock:
            keys = [k for k in self._iters if k[0] == phone]
            for k in keys:
                del self._iters[k]


# ══════════════════════════════════════════════════════════════════════════════
#  REACTION ENGINE
# ══════════════════════════════════════════════════════════════════════════════

class ReactionEngine:
    """
    Priority-queue-based engine that routes incoming reaction jobs to healthy
    accounts while respecting per-chat config, rate limits, and blacklists.

    Usage
    -----
    engine = ReactionEngine(accounts, registry)
    asyncio.create_task(engine.run())          # start background workers
    await engine.enqueue(chat_id, msg_id)      # normal priority
    await engine.enqueue_urgent(chat_id, msg_id)
    engine.stop()                              # signal shutdown
    """

    def __init__(
        self,
        accounts:       list,                # list[Account]
        registry:       ChatConfigRegistry,
        concurrency:    int      = 5,
        worker_count:   int      = 3,
        flood_backoff:  float    = 3.0,       # extra wait on FloodWait (multiplier)
        max_retries:    int      = 2,
        on_sent:        Optional[Callable]  = None,  # (phone, chat_id, msg_id) → None
        on_failed:      Optional[Callable]  = None,  # (phone, chat_id, msg_id, exc) → None
    ):
        self._accounts      = accounts
        self._registry      = registry
        self._concurrency   = concurrency
        self._worker_count  = worker_count
        self._flood_backoff = flood_backoff
        self._max_retries   = max_retries
        self._on_sent       = on_sent
        self._on_failed     = on_failed

        self._queue:   asyncio.PriorityQueue  = asyncio.PriorityQueue()
        self._sem:     asyncio.Semaphore      = asyncio.Semaphore(concurrency)
        self._running: bool                   = False
        self._stop_ev: asyncio.Event          = asyncio.Event()

        self.metrics = EngineMetrics()
        self.cycler  = EmojiCycler()

        # per-account flood cooldown {phone: resume_at (monotonic)}
        self._flood_until: dict[str, float] = {}
        self._flood_lock   = threading.Lock()

    # ── public API ────────────────────────────────────────────────────────────

    async def enqueue(
        self,
        chat_id:        int,
        msg_id:         int,
        priority:       Priority       = Priority.NORMAL,
        account_filter: Optional[list] = None,
    ) -> None:
        # check blacklist early so it never touches the queue
        self._registry.reload()
        cfg = self._registry.get(chat_id)
        if cfg.blacklisted:
            self.metrics.bump_blacklist_drop()
            logger.debug("[engine] blacklisted chat_id=%s — drop", chat_id)
            return

        job = PriorityReactionJob.make(
            chat_id        = chat_id,
            msg_id         = msg_id,
            priority       = priority,
            account_filter = account_filter,
            priority_boost = cfg.priority_boost,
        )
        await self._queue.put(job)
        self.metrics.bump_enqueued()
        logger.debug("[engine] queued chat=%s msg=%s pri=%s depth=%d",
                     chat_id, msg_id, job.priority.name, self._queue.qsize())

    async def enqueue_urgent(self, chat_id: int, msg_id: int) -> None:
        await self.enqueue(chat_id, msg_id, Priority.URGENT)

    def stop(self) -> None:
        self._running = False
        self._stop_ev.set()

    def queue_depth(self) -> int:
        return self._queue.qsize()

    # ── lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        self._running = True
        self._stop_ev.clear()
        logger.info("[engine] starting %d workers, concurrency=%d",
                    self._worker_count, self._concurrency)
        workers = [
            asyncio.create_task(self._worker(i))
            for i in range(self._worker_count)
        ]
        await self._stop_ev.wait()
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        self.metrics.log_summary()
        logger.info("[engine] stopped")

    # ── workers ───────────────────────────────────────────────────────────────

    async def _worker(self, worker_id: int) -> None:
        logger.debug("[worker %d] started", worker_id)
        while self._running:
            try:
                job: PriorityReactionJob = await asyncio.wait_for(
                    self._queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            async with self._sem:
                await self._dispatch(job)

            self._queue.task_done()

    async def _dispatch(self, job: PriorityReactionJob) -> None:
        """Select eligible accounts and fire _send_one for each."""
        cfg      = self._registry.get(job.chat_id)
        pool     = cfg.effective_emoji_pool()
        accounts = self._select_accounts(job)

        if not accounts:
            self.metrics.bump_skipped()
            logger.debug("[engine] no eligible accounts for chat=%s", job.chat_id)
            return

        tasks = [
            asyncio.create_task(self._send_one(acc, job, pool, cfg))
            for acc in accounts
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    def _select_accounts(self, job: PriorityReactionJob) -> list:
        """Return healthy accounts that pass the job's optional filter."""
        candidates = [
            a for a in self._accounts
            if a.is_healthy() and (
                job.account_filter is None or
                a.phone in job.account_filter
            )
        ]
        return candidates

    async def _send_one(
        self,
        acc:  object,       # Account
        job:  PriorityReactionJob,
        pool: list[str],
        cfg:  ChatConfig,
    ) -> None:
        # check per-account flood cooldown
        now = time.monotonic()
        with self._flood_lock:
            if now < self._flood_until.get(acc.phone, 0):
                self.metrics.bump_skipped()
                return

        # apply per-chat rate limits if configured, else use account limiter
        if cfg.max_per_hour > 0 or cfg.min_gap_sec > 0.0:
            # build a one-off limiter for this chat
            max_h = cfg.max_per_hour  if cfg.max_per_hour  > 0 else MAX_REACTIONS_PER_HOUR
            gap   = cfg.min_gap_sec   if cfg.min_gap_sec   > 0 else MIN_GAP_BETWEEN_REACTIONS
            lim = RateLimiter(max_h, gap)
            # copy timestamps from account limiter so we don't double-count
            if hasattr(acc, 'limiter') and hasattr(acc.limiter, '_timestamps'):
                lim._timestamps = list(acc.limiter._timestamps)
            can = lim.can_send()
        else:
            # URGENT bypasses rate limiter
            can = (job.priority == Priority.URGENT) or acc.limiter.can_send()

        if not can:
            self.metrics.bump_skipped()
            logger.debug("[engine] rate-limited acc=%s chat=%s",
                         acc.phone, job.chat_id)
            return

        # pick emoji
        emoji = self.cycler.next(acc.phone, job.chat_id, pool)

        # attempt send with retries
        enqueue_lag = time.monotonic() - job.enqueued_at
        attempt     = 0
        sent        = False

        while attempt <= self._max_retries and not sent:
            attempt += 1
            try:
                ok = await send_reaction(acc, job.chat_id, job.msg_id)
                if ok:
                    acc.mark_ok()
                    if hasattr(acc.limiter, 'mark_sent'):
                        acc.limiter.mark_sent()
                    self.metrics.bump_sent(
                        job.chat_id,
                        acc.phone,
                        enqueue_lag + (time.monotonic() - job.enqueued_at - enqueue_lag),
                    )
                    if self._on_sent:
                        try:
                            self._on_sent(acc.phone, job.chat_id, job.msg_id)
                        except Exception:
                            pass
                    sent = True
                    logger.debug("[engine] sent  acc=%s chat=%s msg=%s emoji=%s",
                                 acc.phone, job.chat_id, job.msg_id, emoji)
                else:
                    raise RuntimeError("send_reaction returned False")

            except Exception as exc:
                name = type(exc).__name__
                logger.warning("[engine] attempt %d/%d failed acc=%s exc=%s",
                               attempt, self._max_retries + 1, acc.phone, exc)

                # flood wait
                if "FloodWait" in name or "flood_wait" in name.lower():
                    wait_secs = getattr(exc, 'seconds', 60) * self._flood_backoff
                    logger.warning("[engine] FloodWait acc=%s — cooldown %.0fs",
                                   acc.phone, wait_secs)
                    with self._flood_lock:
                        self._flood_until[acc.phone] = time.monotonic() + wait_secs
                    self.metrics.bump_flooded()
                    break  # don't retry; job will be re-enqueued on next message

                # ban / deactivated
                if any(x in name for x in ("Deactivated", "Banned", "AuthKey")):
                    acc.mark_fail()
                    self.metrics.bump_banned(acc.phone)
                    self.cycler.reset_account(acc.phone)
                    logger.error("[engine] BANNED acc=%s — marked unhealthy", acc.phone)
                    break

                # generic failure — back off between retries
                if attempt <= self._max_retries:
                    await asyncio.sleep(random.uniform(0.5, 1.5) * attempt)
                else:
                    acc.mark_fail()
                    self.metrics.bump_failed(job.chat_id, acc.phone)
                    if self._on_failed:
                        try:
                            self._on_failed(acc.phone, job.chat_id, job.msg_id, exc)
                        except Exception:
                            pass


# ══════════════════════════════════════════════════════════════════════════════
#  QUEUE INSPECTOR  — debug / monitor
# ══════════════════════════════════════════════════════════════════════════════

class QueueInspector:
    """
    Utility to report current engine state.
    Call print_report() from a monitoring loop or on demand.
    """

    def __init__(self, engine: ReactionEngine):
        self._engine = engine

    def report(self) -> dict:
        m = self._engine.metrics.snapshot()
        return {
            "queue_depth":     self._engine.queue_depth(),
            "running":         self._engine._running,
            **m,
        }

    def print_report(self) -> None:
        r   = self.report()
        sep = "─" * 52
        print(sep)
        print("  Reaction Engine — Status Report")
        print(sep)
        print(f"  Running:      {r['running']}")
        print(f"  Queue depth:  {r['queue_depth']}")
        print(f"  Enqueued:     {r['total_enqueued']}")
        print(f"  Sent:         {r['total_sent']}")
        print(f"  Skipped:      {r['total_skipped']}")
        print(f"  Failed:       {r['total_failed']}")
        print(f"  Flooded:      {r['total_flooded']}")
        print(f"  Banned:       {r['total_banned']}")
        print(f"  Blk drops:    {r['blacklist_drops']}")
        print(f"  P50 latency:  {r['latency_p50']:.2f}s")
        print(f"  P95 latency:  {r['latency_p95']:.2f}s")
        if r["top_chats"]:
            print("  Top chats:")
            for cid, stats in r["top_chats"][:5]:
                print(f"    {cid}: sent={stats['sent']} failed={stats['failed']}")
        if r["top_accounts"]:
            print("  Accounts:")
            for phone, stats in r["top_accounts"]:
                print(f"    {phone}: sent={stats['sent']} failed={stats['failed']}")
        print(sep)