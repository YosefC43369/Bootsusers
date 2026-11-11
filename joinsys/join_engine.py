"""
// Developer : YOSCRYPT
"""

from __future__ import annotations

import asyncio
import enum
import itertools
import logging
import random
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Callable, Coroutine, Any

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    UserAlreadyParticipantError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    ChannelPrivateError,
    ChatAdminRequiredError,
    PeerFloodError,
    UserBannedInChannelError,
    TooManyRequestsError,
    ChatWriteForbiddenError,
    UserNotParticipantError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
from telethon.tl.types import Channel, Chat, ChatInviteAlready

log = logging.getLogger("join_engine")


# ---------------------------------------------------------------------------
# Enums & constants
# ---------------------------------------------------------------------------

class JoinPriority(enum.IntEnum):
    LOW    = 0
    NORMAL = 1
    HIGH   = 2
    URGENT = 3


class JoinStatus(enum.Enum):
    PENDING      = "pending"
    IN_PROGRESS  = "in_progress"
    SUCCESS      = "success"
    ALREADY_IN   = "already_in"
    FLOOD_WAIT   = "flood_wait"
    INVALID      = "invalid"
    BANNED       = "banned"
    PRIVATE      = "private"
    RATE_LIMITED = "rate_limited"
    FAILED       = "failed"
    EXHAUSTED    = "exhausted"   # all retries used up


class TargetKind(enum.Enum):
    USERNAME   = "username"
    INVITE     = "invite"
    CHAT_ID    = "chat_id"


# FSM states for per-job retry logic
class _JobState(enum.Enum):
    READY   = "ready"
    RUNNING = "running"
    WAITING = "waiting"   # flood cooldown
    DONE    = "done"
    DEAD    = "dead"      # exhausted / permanent error


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class JoinTarget:
    """One channel/group to join."""
    raw: str                              # e.g. "@chan", "t.me/joinchat/ABC", "-100123"
    kind: TargetKind = TargetKind.USERNAME
    priority: JoinPriority = JoinPriority.NORMAL
    label: str = ""                       # human-readable tag

    def __post_init__(self):
        raw = self.raw.strip()
        if raw.startswith(("https://t.me/+", "https://t.me/joinchat/",
                           "t.me/+", "t.me/joinchat/")):
            self.kind = TargetKind.INVITE
        elif raw.lstrip("-").isdigit():
            self.kind = TargetKind.CHAT_ID
        else:
            self.kind = TargetKind.USERNAME


@dataclass
class JoinResult:
    phone: str
    target: JoinTarget
    status: JoinStatus
    entity_id: int | None = None
    entity_title: str = ""
    attempts: int = 0
    flood_wait_sec: int = 0
    error_msg: str = ""
    elapsed_ms: float = 0.0
    ts: float = field(default_factory=time.time)


@dataclass(order=True)
class _PriorityJob:
    sort_key: int           # negative priority for min-heap ascending pop
    job_id: int = field(compare=False)
    phone: str = field(compare=False)
    target: JoinTarget = field(compare=False)
    state: _JobState = field(compare=False, default=_JobState.READY)
    attempts: int = field(compare=False, default=0)
    next_attempt_at: float = field(compare=False, default=0.0)
    result: JoinResult | None = field(compare=False, default=None)


# ---------------------------------------------------------------------------
# Engine metrics
# ---------------------------------------------------------------------------

class EngineMetrics:
    def __init__(self, window: int = 500):
        self._lock = asyncio.Lock()
        self._window = window
        self.total_enqueued: int = 0
        self.total_success: int = 0
        self.total_already_in: int = 0
        self.total_failed: int = 0
        self.total_flood_waits: int = 0
        self.total_banned: int = 0
        self._latencies: deque[float] = deque(maxlen=window)

    async def record(self, result: JoinResult):
        async with self._lock:
            if result.status == JoinStatus.SUCCESS:
                self.total_success += 1
            elif result.status == JoinStatus.ALREADY_IN:
                self.total_already_in += 1
            elif result.status in (JoinStatus.BANNED, JoinStatus.PRIVATE):
                self.total_banned += 1
            elif result.status == JoinStatus.FLOOD_WAIT:
                self.total_flood_waits += 1
            elif result.status in (JoinStatus.FAILED, JoinStatus.EXHAUSTED, JoinStatus.INVALID):
                self.total_failed += 1
            if result.elapsed_ms > 0:
                self._latencies.append(result.elapsed_ms)

    def _percentile(self, pct: float) -> float:
        if not self._latencies:
            return 0.0
        s = sorted(self._latencies)
        idx = int(len(s) * pct / 100)
        return s[min(idx, len(s) - 1)]

    @property
    def p50_ms(self) -> float:
        return self._percentile(50)

    @property
    def p95_ms(self) -> float:
        return self._percentile(95)

    def report(self) -> dict:
        return {
            "enqueued": self.total_enqueued,
            "success": self.total_success,
            "already_in": self.total_already_in,
            "failed": self.total_failed,
            "flood_waits": self.total_flood_waits,
            "banned": self.total_banned,
            "p50_ms": round(self.p50_ms, 1),
            "p95_ms": round(self.p95_ms, 1),
        }


# ---------------------------------------------------------------------------
# Per-account throttle
# ---------------------------------------------------------------------------

class AccountThrottle:
    """
    Tracks joins per account per hour.
    Hard cap: max_per_hour.  Soft gap: min_gap_sec between consecutive joins.
    """

    def __init__(
        self,
        max_per_hour: int = 8,
        min_gap_sec: float = 30.0,
        flood_cooldown_sec: float = 300.0,
    ):
        self.max_per_hour = max_per_hour
        self.min_gap_sec = min_gap_sec
        self.flood_cooldown_sec = flood_cooldown_sec

        self._join_times: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=max_per_hour * 2))
        self._last_join: dict[str, float] = {}
        self._flood_until: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def is_ready(self, phone: str) -> tuple[bool, float]:
        """
        Returns (can_join, wait_seconds).
        wait_seconds == 0 means ready now.
        """
        async with self._lock:
            now = time.monotonic()

            # flood cooldown
            flood_until = self._flood_until.get(phone, 0.0)
            if now < flood_until:
                return False, flood_until - now

            # min gap
            last = self._last_join.get(phone, 0.0)
            gap_remaining = (last + self.min_gap_sec) - now
            if gap_remaining > 0:
                return False, gap_remaining

            # hourly cap
            window_start = now - 3600.0
            times = self._join_times[phone]
            recent = sum(1 for t in times if t > window_start)
            if recent >= self.max_per_hour:
                oldest_in_window = min((t for t in times if t > window_start), default=now)
                wait = (oldest_in_window + 3600.0) - now
                return False, max(wait, 1.0)

            return True, 0.0

    async def record_join(self, phone: str):
        async with self._lock:
            now = time.monotonic()
            self._join_times[phone].append(now)
            self._last_join[phone] = now

    async def record_flood(self, phone: str, wait_sec: int):
        async with self._lock:
            self._flood_until[phone] = time.monotonic() + wait_sec + 5.0

    async def set_flood_cooldown(self, phone: str, seconds: float | None = None):
        async with self._lock:
            cd = seconds if seconds is not None else self.flood_cooldown_sec
            self._flood_until[phone] = time.monotonic() + cd

    async def join_count_last_hour(self, phone: str) -> int:
        async with self._lock:
            now = time.monotonic()
            window_start = now - 3600.0
            return sum(1 for t in self._join_times[phone] if t > window_start)


# ---------------------------------------------------------------------------
# Core join logic
# ---------------------------------------------------------------------------

async def _resolve_invite_hash(raw: str) -> str:
    """Extract invite hash from various t.me link formats."""
    for prefix in (
        "https://t.me/+",
        "https://t.me/joinchat/",
        "t.me/+",
        "t.me/joinchat/",
    ):
        if raw.startswith(prefix):
            return raw[len(prefix):]
    return raw


async def _do_join(
    client: TelegramClient,
    target: JoinTarget,
    jitter: float = 1.5,
) -> tuple[JoinStatus, int | None, str, int]:
    """
    Attempt a single join.
    Returns (status, entity_id, entity_title, flood_wait_sec).
    """
    await asyncio.sleep(random.uniform(0.3, jitter))

    try:
        if target.kind == TargetKind.INVITE:
            h = await _resolve_invite_hash(target.raw)
            # check invite before joining
            try:
                inv = await client(CheckChatInviteRequest(h))
                if isinstance(inv, ChatInviteAlready):
                    eid = getattr(inv.chat, "id", None)
                    etitle = getattr(inv.chat, "title", "")
                    return JoinStatus.ALREADY_IN, eid, etitle, 0
            except Exception:
                pass
            updates = await client(ImportChatInviteRequest(h))
            chat = updates.chats[0] if updates.chats else None
            eid = getattr(chat, "id", None)
            etitle = getattr(chat, "title", "")
            return JoinStatus.SUCCESS, eid, etitle, 0

        elif target.kind == TargetKind.USERNAME:
            uname = target.raw.lstrip("@")
            entity = await client.get_entity(uname)
            updates = await client(JoinChannelRequest(entity))
            chat = updates.chats[0] if updates.chats else entity
            eid = getattr(chat, "id", None)
            etitle = getattr(chat, "title", uname)
            return JoinStatus.SUCCESS, eid, etitle, 0

        elif target.kind == TargetKind.CHAT_ID:
            chat_id = int(target.raw)
            entity = await client.get_entity(chat_id)
            updates = await client(JoinChannelRequest(entity))
            chat = updates.chats[0] if updates.chats else entity
            eid = getattr(chat, "id", None)
            etitle = getattr(chat, "title", str(chat_id))
            return JoinStatus.SUCCESS, eid, etitle, 0

    except UserAlreadyParticipantError:
        return JoinStatus.ALREADY_IN, None, "", 0
    except FloodWaitError as e:
        return JoinStatus.FLOOD_WAIT, None, "", e.seconds
    except PeerFloodError:
        return JoinStatus.FLOOD_WAIT, None, "", 600
    except TooManyRequestsError as e:
        wait = getattr(e, "seconds", 60)
        return JoinStatus.FLOOD_WAIT, None, "", wait
    except (InviteHashExpiredError, InviteHashInvalidError):
        return JoinStatus.INVALID, None, "", 0
    except (UserBannedInChannelError, ChatAdminRequiredError):
        return JoinStatus.BANNED, None, "", 0
    except ChannelPrivateError:
        return JoinStatus.PRIVATE, None, "", 0
    except ChatWriteForbiddenError:
        return JoinStatus.BANNED, None, "", 0
    except (UsernameInvalidError, UsernameNotOccupiedError):
        return JoinStatus.INVALID, None, "", 0
    except Exception as exc:
        log.warning("join error for %s: %s", target.raw, exc)
        return JoinStatus.FAILED, None, "", 0

    return JoinStatus.FAILED, None, "", 0


# ---------------------------------------------------------------------------
# Join Engine
# ---------------------------------------------------------------------------

class JoinEngine:
    """
    Async priority-queue join engine.

    Features
    --------
    - asyncio.PriorityQueue with URGENT bypass lane
    - Per-account AccountThrottle (hourly cap + min-gap + flood cooldown)
    - Retry FSM with exponential back-off (max_retries configurable)
    - Global concurrency semaphore
    - EngineMetrics with P50/P95 latency
    - Result callback hooks
    - Graceful shutdown
    """

    def __init__(
        self,
        clients: dict[str, TelegramClient],
        *,
        max_retries: int = 3,
        global_concurrency: int = 5,
        max_per_hour: int = 8,
        min_gap_sec: float = 30.0,
        flood_cooldown_sec: float = 300.0,
        worker_count: int = 10,
        jitter: float = 1.5,
        result_callback: Callable[[JoinResult], Coroutine[Any, Any, None]] | None = None,
    ):
        self.clients = clients                   # phone -> TelegramClient
        self.max_retries = max_retries
        self.jitter = jitter
        self.result_callback = result_callback

        self._queue: asyncio.PriorityQueue[_PriorityJob] = asyncio.PriorityQueue()
        self._urgent_queue: asyncio.Queue[_PriorityJob] = asyncio.Queue()
        self._sem = asyncio.Semaphore(global_concurrency)
        self._throttle = AccountThrottle(max_per_hour, min_gap_sec, flood_cooldown_sec)
        self._metrics = EngineMetrics()
        self._worker_count = worker_count
        self._running = False
        self._workers: list[asyncio.Task] = []
        self._job_counter = itertools.count(1)
        self._pending_jobs: dict[int, _PriorityJob] = {}
        self._results: list[JoinResult] = []
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def enqueue(
        self,
        phone: str,
        target: JoinTarget | str,
        priority: JoinPriority = JoinPriority.NORMAL,
    ) -> int:
        """
        Add a join task.  Returns job_id.
        If phone is "*", round-robin across all loaded clients.
        """
        if isinstance(target, str):
            target = JoinTarget(target, priority=priority)

        if phone == "*":
            phone = self._next_phone()

        job = _PriorityJob(
            sort_key=-int(priority),
            job_id=next(self._job_counter),
            phone=phone,
            target=target,
        )
        async with self._lock:
            self._pending_jobs[job.job_id] = job
            self._metrics.total_enqueued += 1

        if priority == JoinPriority.URGENT:
            await self._urgent_queue.put(job)
        else:
            await self._queue.put(job)

        log.debug("enqueued job %d  phone=%s  target=%s  prio=%s",
                  job.job_id, phone, target.raw, priority.name)
        return job.job_id

    async def enqueue_batch(
        self,
        targets: list[str | JoinTarget],
        phones: list[str],
        priority: JoinPriority = JoinPriority.NORMAL,
    ) -> list[int]:
        """
        Distribute targets round-robin across phones.
        Returns list of job_ids in the same order as targets.
        """
        ids: list[int] = []
        phone_cycle = itertools.cycle(phones)
        for t in targets:
            jid = await self.enqueue(next(phone_cycle), t, priority)
            ids.append(jid)
        return ids

    async def start(self):
        if self._running:
            return
        self._running = True
        self._workers = [
            asyncio.create_task(self._worker(i), name=f"join-worker-{i}")
            for i in range(self._worker_count)
        ]
        log.info("JoinEngine started — %d workers", self._worker_count)

    async def stop(self, drain_timeout: float = 30.0):
        """Graceful shutdown: drain queue then cancel workers."""
        self._running = False
        try:
            await asyncio.wait_for(self._drain_signal(), timeout=drain_timeout)
        except asyncio.TimeoutError:
            log.warning("drain timeout after %.0fs", drain_timeout)
        for w in self._workers:
            w.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        log.info("JoinEngine stopped")

    async def _drain_signal(self):
        while not self._queue.empty() or not self._urgent_queue.empty():
            await asyncio.sleep(0.5)

    def metrics(self) -> dict:
        return self._metrics.report()

    def results(self) -> list[JoinResult]:
        return list(self._results)

    async def job_status(self, job_id: int) -> _JobState | None:
        async with self._lock:
            job = self._pending_jobs.get(job_id)
            return job.state if job else None

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _next_phone(self) -> str:
        return random.choice(list(self.clients.keys()))

    async def _worker(self, worker_id: int):
        log.debug("worker %d started", worker_id)
        backoff = 0.1
        while self._running:
            job = await self._fetch_next_job()
            if job is None:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 2.0)
                continue
            backoff = 0.1

            async with self._sem:
                await self._process_job(job, worker_id)

    async def _fetch_next_job(self) -> _PriorityJob | None:
        # urgent queue takes absolute priority
        try:
            return self._urgent_queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
        try:
            return self._queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    async def _process_job(self, job: _PriorityJob, worker_id: int):
        phone = job.phone
        client = self.clients.get(phone)
        if client is None:
            log.error("no client for phone=%s", phone)
            await self._finish_job(job, JoinStatus.FAILED, error_msg="no client")
            return

        # wait for throttle
        while True:
            ready, wait_sec = await self._throttle.is_ready(phone)
            if ready:
                break
            log.debug("worker %d  job %d  throttle wait %.1fs", worker_id, job.job_id, wait_sec)
            job.state = _JobState.WAITING
            await asyncio.sleep(wait_sec + random.uniform(0, 2.0))

        job.state = _JobState.RUNNING
        job.attempts += 1
        t0 = time.monotonic()

        status, eid, etitle, flood_sec = await _do_join(client, job.target, self.jitter)
        elapsed_ms = (time.monotonic() - t0) * 1000.0

        if status == JoinStatus.SUCCESS:
            await self._throttle.record_join(phone)
            await self._finish_job(job, status, eid, etitle, elapsed_ms=elapsed_ms)

        elif status == JoinStatus.ALREADY_IN:
            await self._finish_job(job, status, eid, etitle, elapsed_ms=elapsed_ms)

        elif status == JoinStatus.FLOOD_WAIT:
            await self._throttle.record_flood(phone, flood_sec)
            if job.attempts <= self.max_retries:
                log.info("job %d  flood wait %ds  attempt %d/%d",
                         job.job_id, flood_sec, job.attempts, self.max_retries)
                job.state = _JobState.WAITING
                await asyncio.sleep(flood_sec + random.uniform(5, 15))
                # re-enqueue
                if job.target.priority == JoinPriority.URGENT:
                    await self._urgent_queue.put(job)
                else:
                    await self._queue.put(job)
            else:
                await self._finish_job(
                    job, JoinStatus.EXHAUSTED,
                    flood_wait_sec=flood_sec, elapsed_ms=elapsed_ms,
                )

        elif status in (JoinStatus.INVALID, JoinStatus.BANNED, JoinStatus.PRIVATE):
            # permanent errors — no retry
            await self._finish_job(job, status, elapsed_ms=elapsed_ms)

        else:
            # transient failure — exponential backoff retry
            if job.attempts <= self.max_retries:
                delay = (2 ** job.attempts) + random.uniform(0, 3.0)
                log.debug("job %d  retry %d/%d in %.1fs",
                          job.job_id, job.attempts, self.max_retries, delay)
                job.state = _JobState.WAITING
                await asyncio.sleep(delay)
                await self._queue.put(job)
            else:
                await self._finish_job(
                    job, JoinStatus.EXHAUSTED, elapsed_ms=elapsed_ms,
                    error_msg=f"exhausted after {job.attempts} attempts",
                )

    async def _finish_job(
        self,
        job: _PriorityJob,
        status: JoinStatus,
        entity_id: int | None = None,
        entity_title: str = "",
        flood_wait_sec: int = 0,
        elapsed_ms: float = 0.0,
        error_msg: str = "",
    ):
        result = JoinResult(
            phone=job.phone,
            target=job.target,
            status=status,
            entity_id=entity_id,
            entity_title=entity_title,
            attempts=job.attempts,
            flood_wait_sec=flood_wait_sec,
            error_msg=error_msg,
            elapsed_ms=elapsed_ms,
        )
        job.state = _JobState.DONE if status not in (
            JoinStatus.EXHAUSTED, JoinStatus.FAILED, JoinStatus.INVALID,
            JoinStatus.BANNED, JoinStatus.PRIVATE,
        ) else _JobState.DEAD
        job.result = result

        async with self._lock:
            self._results.append(result)

        await self._metrics.record(result)
        log.info("job %d  %s  phone=%s  target=%s  attempts=%d  %.0fms",
                 job.job_id, status.value, job.phone,
                 job.target.raw, job.attempts, elapsed_ms)

        if self.result_callback:
            try:
                await self.result_callback(result)
            except Exception as cb_exc:
                log.error("result_callback error: %s", cb_exc)


# ---------------------------------------------------------------------------
# Convenience runner
# ---------------------------------------------------------------------------

class BatchJoinRunner:
    """
    High-level wrapper: takes a list of targets + a list of phones,
    fires up JoinEngine, waits for completion, returns results.

    Usage
    -----
    runner = BatchJoinRunner(clients)
    results = await runner.run(targets=["@chan1", "@chan2"], phones=["79001", "79002"])
    """

    def __init__(
        self,
        clients: dict[str, TelegramClient],
        *,
        max_per_hour: int = 8,
        min_gap_sec: float = 30.0,
        flood_cooldown_sec: float = 300.0,
        global_concurrency: int = 5,
        max_retries: int = 3,
        worker_count: int = 10,
        jitter: float = 1.5,
    ):
        self._engine = JoinEngine(
            clients,
            max_retries=max_retries,
            global_concurrency=global_concurrency,
            max_per_hour=max_per_hour,
            min_gap_sec=min_gap_sec,
            flood_cooldown_sec=flood_cooldown_sec,
            worker_count=worker_count,
            jitter=jitter,
        )

    async def run(
        self,
        targets: list[str | JoinTarget],
        phones: list[str],
        priority: JoinPriority = JoinPriority.NORMAL,
        drain_timeout: float = 3600.0,
    ) -> list[JoinResult]:
        await self._engine.start()
        await self._engine.enqueue_batch(targets, phones, priority)

        # wait for queue to empty
        try:
            await asyncio.wait_for(
                self._engine._drain_signal(),
                timeout=drain_timeout,
            )
        except asyncio.TimeoutError:
            log.warning("BatchJoinRunner drain timeout")

        await asyncio.sleep(2.0)   # let final callbacks land
        await self._engine.stop()
        return self._engine.results()

    def metrics(self) -> dict:
        return self._engine.metrics()


# ---------------------------------------------------------------------------
# Queue inspector (debug)
# ---------------------------------------------------------------------------

class JoinQueueInspector:
    def __init__(self, engine: JoinEngine):
        self._engine = engine

    def report(self) -> str:
        e = self._engine
        m = e.metrics()
        lines = [
            "=== JoinEngine Inspector ===",
            f"Queue depth (normal): {e._queue.qsize()}",
            f"Queue depth (urgent): {e._urgent_queue.qsize()}",
            f"Workers: {len(e._workers)}  running={e._running}",
            f"Pending jobs tracked: {len(e._pending_jobs)}",
            f"Results collected: {len(e._results)}",
            "--- Metrics ---",
        ]
        for k, v in m.items():
            lines.append(f"  {k}: {v}")
        lines.append("===========================")
        return "\n".join(lines)

    def pending_by_state(self) -> dict[str, list[int]]:
        out: dict[str, list[int]] = defaultdict(list)
        for jid, job in self._engine._pending_jobs.items():
            out[job.state.value].append(jid)
        return dict(out)

    def flood_status(self) -> dict[str, float]:
        """Returns seconds remaining in flood cooldown per phone."""
        now = time.monotonic()
        return {
            phone: max(0.0, until - now)
            for phone, until in self._engine._throttle._flood_until.items()
            if until > now
        }


# ---------------------------------------------------------------------------
# Quick-config helpers
# ---------------------------------------------------------------------------

def make_engine(
    clients: dict[str, TelegramClient],
    *,
    aggressive: bool = False,
) -> JoinEngine:
    """
    Two presets.
    aggressive=False  → conservative (default): 8/hr, 30s gap
    aggressive=True   → fast: 15/hr, 10s gap  (higher ban risk)
    """
    if aggressive:
        return JoinEngine(
            clients,
            max_per_hour=15,
            min_gap_sec=10.0,
            flood_cooldown_sec=180.0,
            global_concurrency=10,
            worker_count=15,
            jitter=0.8,
        )
    return JoinEngine(
        clients,
        max_per_hour=8,
        min_gap_sec=30.0,
        flood_cooldown_sec=300.0,
        global_concurrency=5,
        worker_count=10,
        jitter=1.5,
    )