# lang: Python 3.11+, file: flood_guard.py, target: Windows 11 / Linux
# Full flood protection stack: FloodWait tracking, circuit breaker per account,
# global guard, recovery scheduler, escalation, persistence, event hooks.

from __future__ import annotations

import asyncio
import enum
import json
import logging
import math
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterator, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class FloodLevel(enum.IntEnum):
    NONE     = 0   # no active flood wait
    LIGHT    = 1   # < 60s
    MODERATE = 2   # 60-300s
    HEAVY    = 3   # 300-3600s
    EXTREME  = 4   # > 3600s — likely IP/account ban signal


class CircuitState(enum.Enum):
    CLOSED     = "closed"       # normal, calls pass through
    OPEN       = "open"         # blocked, returning fast-fail
    HALF_OPEN  = "half_open"    # probing — one call allowed


class GuardEvent(enum.Enum):
    FLOOD_DETECTED   = "flood_detected"
    FLOOD_EXPIRED    = "flood_expired"
    CIRCUIT_OPEN     = "circuit_open"
    CIRCUIT_HALF     = "circuit_half_open"
    CIRCUIT_CLOSED   = "circuit_closed"
    ESCALATION       = "escalation"
    GLOBAL_PAUSE     = "global_pause"
    RECOVERY_START   = "recovery_start"
    RECOVERY_DONE    = "recovery_done"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class FloodRecord:
    phone: str
    wait_seconds: int
    detected_at: float
    source: str         # e.g. action name or error class
    level: FloodLevel
    resolved_at: float = 0.0

    @property
    def is_active(self) -> bool:
        if self.resolved_at:
            return False
        return time.time() < self.detected_at + self.wait_seconds

    @property
    def remaining_sec(self) -> float:
        if not self.is_active:
            return 0.0
        return max(0.0, (self.detected_at + self.wait_seconds) - time.time())


@dataclass
class CircuitStats:
    phone: str
    state: CircuitState
    consecutive_failures: int
    consecutive_successes: int
    total_trips: int
    last_trip_ts: float
    last_success_ts: float
    open_until: float
    failure_rate_1m: float


@dataclass
class FloodSummary:
    phone: str
    current_level: FloodLevel
    active_records: int
    total_records: int
    longest_wait_sec: int
    avg_wait_sec: float
    circuit_state: CircuitState
    flood_rate_1h: float     # floods per hour over last hour


# ---------------------------------------------------------------------------
# Circuit breaker (per account)
# ---------------------------------------------------------------------------

class CircuitBreaker:
    """
    Per-account circuit breaker.

    CLOSED  → calls allowed; on threshold failures → OPEN
    OPEN    → fast-fail for cool_down_sec; then → HALF_OPEN
    HALF_OPEN → one probe call; success → CLOSED, failure → OPEN again
    """

    def __init__(self,
                 phone: str,
                 failure_threshold: int = 5,
                 success_threshold: int = 2,
                 cool_down_sec:   float = 60.0,
                 half_open_timeout: float = 30.0):
        self.phone               = phone
        self.failure_threshold   = failure_threshold
        self.success_threshold   = success_threshold
        self.cool_down_sec       = cool_down_sec
        self.half_open_timeout   = half_open_timeout

        self.state               = CircuitState.CLOSED
        self.consecutive_failures = 0
        self.consecutive_successes = 0
        self.total_trips         = 0
        self.last_trip_ts        = 0.0
        self.last_success_ts     = 0.0
        self.open_until          = 0.0
        self._half_open_lock     = asyncio.Lock()
        self._probe_allowed      = False

        # rolling failure timestamps (1 min window)
        self._failure_ts: deque = deque(maxlen=200)
        self._success_ts: deque = deque(maxlen=200)

    # --- state machine ---

    def _transition(self, new_state: CircuitState) -> None:
        old = self.state
        self.state = new_state
        logger.info("Circuit [%s] %s → %s", self.phone, old.value, new_state.value)

    def is_callable(self) -> bool:
        """Check whether a call should be allowed right now."""
        now = time.monotonic()
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if now >= self.open_until:
                self._transition(CircuitState.HALF_OPEN)
                self._probe_allowed = True
                return True
            return False
        # HALF_OPEN: allow exactly one probe
        if self._probe_allowed:
            self._probe_allowed = False
            return True
        return False

    def record_success(self) -> None:
        now = time.monotonic()
        self._success_ts.append(now)
        self.last_success_ts = now
        self.consecutive_failures = 0

        if self.state == CircuitState.HALF_OPEN:
            self.consecutive_successes += 1
            if self.consecutive_successes >= self.success_threshold:
                self.consecutive_successes = 0
                self._transition(CircuitState.CLOSED)

    def record_failure(self, force_open: bool = False) -> bool:
        """Returns True if circuit tripped to OPEN."""
        now = time.monotonic()
        self._failure_ts.append(now)
        self.consecutive_failures += 1
        self.consecutive_successes = 0

        if force_open or self.consecutive_failures >= self.failure_threshold:
            return self._trip()
        if self.state == CircuitState.HALF_OPEN:
            return self._trip()
        return False

    def _trip(self) -> bool:
        self.total_trips += 1
        self.last_trip_ts = time.monotonic()
        self.open_until   = time.monotonic() + self.cool_down_sec
        self._probe_allowed = False
        self._transition(CircuitState.OPEN)
        return True

    def force_close(self) -> None:
        self.consecutive_failures = 0
        self.consecutive_successes = 0
        self._transition(CircuitState.CLOSED)

    def extend_cool_down(self, additional_sec: float) -> None:
        self.open_until = max(self.open_until, time.monotonic() + additional_sec)

    def stats(self) -> CircuitStats:
        now = time.monotonic()
        cutoff = now - 60.0
        failures_1m = sum(1 for ts in self._failure_ts if ts > cutoff)
        total_1m    = failures_1m + sum(1 for ts in self._success_ts if ts > cutoff)
        rate_1m     = failures_1m / total_1m if total_1m else 0.0
        return CircuitStats(
            phone=self.phone,
            state=self.state,
            consecutive_failures=self.consecutive_failures,
            consecutive_successes=self.consecutive_successes,
            total_trips=self.total_trips,
            last_trip_ts=self.last_trip_ts,
            last_success_ts=self.last_success_ts,
            open_until=self.open_until,
            failure_rate_1m=rate_1m,
        )


# ---------------------------------------------------------------------------
# Flood tracker (per account)
# ---------------------------------------------------------------------------

class AccountFloodTracker:
    """
    Tracks all FloodWait events for one account.
    Computes current flood level, escalation, and recovery timeline.
    """

    _ESCALATION_THRESHOLD = 3    # 3+ floods in a window → escalate

    def __init__(self, phone: str, escalation_window_sec: float = 300.0):
        self.phone          = phone
        self._records: list[FloodRecord] = []
        self._window        = escalation_window_sec
        self._escalation_level: int = 0   # 0=none, 1,2,3=escalating
        self._last_escalation_ts: float = 0.0

    def record_flood(self, wait_seconds: int, source: str = "") -> FloodRecord:
        level = self._classify(wait_seconds)
        rec   = FloodRecord(
            phone=self.phone,
            wait_seconds=wait_seconds,
            detected_at=time.time(),
            source=source,
            level=level,
        )
        self._records.append(rec)
        self._check_escalation()
        logger.info("FloodWait on %s: %ds (%s)", self.phone, wait_seconds, level.name)
        return rec

    def mark_resolved(self, record_id: int) -> None:
        if 0 <= record_id < len(self._records):
            self._records[record_id].resolved_at = time.time()

    def resolve_all(self) -> None:
        now = time.time()
        for r in self._records:
            if r.is_active:
                r.resolved_at = now

    def active_records(self) -> list[FloodRecord]:
        return [r for r in self._records if r.is_active]

    def current_level(self) -> FloodLevel:
        active = self.active_records()
        if not active:
            return FloodLevel.NONE
        return max(r.level for r in active)

    def max_remaining_sec(self) -> float:
        active = self.active_records()
        if not active:
            return 0.0
        return max(r.remaining_sec for r in active)

    def flood_rate_1h(self) -> float:
        cutoff = time.time() - 3600.0
        count  = sum(1 for r in self._records if r.detected_at >= cutoff)
        return count / 1.0   # floods per hour

    def _classify(self, seconds: int) -> FloodLevel:
        if seconds <= 0:
            return FloodLevel.NONE
        if seconds < 60:
            return FloodLevel.LIGHT
        if seconds < 300:
            return FloodLevel.MODERATE
        if seconds < 3600:
            return FloodLevel.HEAVY
        return FloodLevel.EXTREME

    def _check_escalation(self) -> bool:
        cutoff  = time.time() - self._window
        recent  = [r for r in self._records if r.detected_at >= cutoff]
        if len(recent) >= self._ESCALATION_THRESHOLD:
            self._escalation_level = min(3, self._escalation_level + 1)
            self._last_escalation_ts = time.time()
            return True
        return False

    def decay_escalation(self, now: float) -> None:
        if self._escalation_level and now - self._last_escalation_ts > self._window * 2:
            self._escalation_level = max(0, self._escalation_level - 1)
            self._last_escalation_ts = now

    def summary(self, circuit_state: CircuitState) -> FloodSummary:
        all_waits = [r.wait_seconds for r in self._records]
        return FloodSummary(
            phone=self.phone,
            current_level=self.current_level(),
            active_records=len(self.active_records()),
            total_records=len(self._records),
            longest_wait_sec=max(all_waits) if all_waits else 0,
            avg_wait_sec=statistics.mean(all_waits) if all_waits else 0.0,
            circuit_state=circuit_state,
            flood_rate_1h=self.flood_rate_1h(),
        )

    def to_dict(self) -> dict:
        return {
            "phone": self.phone,
            "escalation_level": self._escalation_level,
            "records": [
                {
                    "wait_sec": r.wait_seconds,
                    "detected_at": r.detected_at,
                    "resolved_at": r.resolved_at,
                    "source": r.source,
                    "level": r.level.name,
                }
                for r in self._records[-100:]   # keep last 100
            ],
        }

    @classmethod
    def from_dict(cls, d: dict) -> "AccountFloodTracker":
        t = cls(phone=d["phone"])
        t._escalation_level = d.get("escalation_level", 0)
        for rd in d.get("records", []):
            r = FloodRecord(
                phone=d["phone"],
                wait_seconds=rd["wait_sec"],
                detected_at=rd["detected_at"],
                resolved_at=rd.get("resolved_at", 0.0),
                source=rd.get("source", ""),
                level=FloodLevel[rd.get("level", "NONE")],
            )
            t._records.append(r)
        return t


# ---------------------------------------------------------------------------
# Recovery scheduler
# ---------------------------------------------------------------------------

class RecoveryScheduler:
    """
    Monitors accounts in flood state and schedules their re-activation.
    Runs as a background asyncio task.
    """

    def __init__(self,
                 poll_interval: float = 5.0,
                 on_recovered: Optional[Callable[[str], None]] = None):
        self._poll     = poll_interval
        self._cb       = on_recovered
        self._tasks: dict[str, asyncio.Task] = {}
        self._running  = False
        self._main_task: Optional[asyncio.Task] = None

    def start(self, get_flooded: Callable[[], dict[str, float]]) -> None:
        """get_flooded returns phone → remaining_seconds dict."""
        self._running   = True
        self._get_flooded = get_flooded
        self._main_task = asyncio.create_task(self._loop())

    def stop(self) -> None:
        self._running = False
        if self._main_task:
            self._main_task.cancel()
        for t in self._tasks.values():
            t.cancel()

    async def _loop(self) -> None:
        while self._running:
            flooded = self._get_flooded()
            for phone, remaining in flooded.items():
                if phone not in self._tasks or self._tasks[phone].done():
                    self._tasks[phone] = asyncio.create_task(
                        self._wait_and_recover(phone, remaining)
                    )
            await asyncio.sleep(self._poll)

    async def _wait_and_recover(self, phone: str, remaining: float) -> None:
        await asyncio.sleep(max(0.0, remaining))
        logger.info("Recovery: %s flood expired", phone)
        if self._cb:
            try:
                self._cb(phone)
            except Exception as exc:
                logger.debug("Recovery callback error: %s", exc)


# ---------------------------------------------------------------------------
# Global flood guard
# ---------------------------------------------------------------------------

class FloodGuard:
    """
    Central flood guard for all Bootsusers accounts.

    Features
    --------
    - Per-account FloodRecord tracking
    - Per-account CircuitBreaker
    - Global pause when too many accounts flood simultaneously
    - Recovery scheduler for automatic reactivation
    - Escalation detection (repeated floods → back off harder)
    - Persistence to JSON
    - Event callbacks
    """

    _GLOBAL_FLOOD_RATIO  = 0.5     # if >50% accounts flooded → global pause
    _GLOBAL_PAUSE_MULT   = 1.2     # multiply longest flood by this for global pause

    def __init__(self,
                 store_path: Optional[str | Path] = None,
                 circuit_failure_threshold: int   = 5,
                 circuit_cool_down_sec:     float = 60.0,
                 circuit_success_threshold: int   = 2,
                 escalation_window_sec:     float = 300.0,
                 auto_recovery:             bool  = True):

        self._store_path          = Path(store_path) if store_path else None
        self._cb_fail_thresh      = circuit_failure_threshold
        self._cb_cool_down        = circuit_cool_down_sec
        self._cb_success_thresh   = circuit_success_threshold
        self._esc_window          = escalation_window_sec
        self._auto_recovery       = auto_recovery

        self._trackers:  dict[str, AccountFloodTracker] = {}
        self._circuits:  dict[str, CircuitBreaker]      = {}
        self._lock       = asyncio.Lock()
        self._global_pause_until: float = 0.0
        self._event_cbs: list[Callable[[GuardEvent, dict], None]] = []
        self._running    = False

        self._scheduler  = RecoveryScheduler(
            poll_interval=5.0,
            on_recovered=self._on_account_recovered,
        )

        # aggregate counters
        self._total_floods:  int = 0
        self._total_trips:   int = 0
        self._total_blocked: int = 0

    # --- lifecycle ---

    async def start(self) -> None:
        await self._load_state()
        if self._auto_recovery:
            self._scheduler.start(self._get_flooded_map)
        self._running = True
        logger.info("FloodGuard started — accounts tracked: %d", len(self._trackers))

    async def stop(self) -> None:
        self._running = False
        self._scheduler.stop()
        await self._save_state()

    # --- account registration ---

    async def register(self, phone: str) -> None:
        async with self._lock:
            if phone not in self._trackers:
                self._trackers[phone] = AccountFloodTracker(phone, self._esc_window)
            if phone not in self._circuits:
                self._circuits[phone] = CircuitBreaker(
                    phone,
                    failure_threshold=self._cb_fail_thresh,
                    success_threshold=self._cb_success_thresh,
                    cool_down_sec=self._cb_cool_down,
                )

    async def register_many(self, phones: list[str]) -> None:
        for phone in phones:
            await self.register(phone)

    # --- primary API ---

    async def is_blocked(self, phone: str) -> tuple[bool, float]:
        """
        Returns (blocked, wait_seconds).
        Checks both circuit state and active flood records.
        """
        await self.register(phone)
        async with self._lock:
            now = time.monotonic()

            # global pause
            if now < self._global_pause_until:
                wait = self._global_pause_until - now
                self._total_blocked += 1
                return True, wait

            circuit = self._circuits[phone]
            tracker = self._trackers[phone]

            # circuit breaker
            if not circuit.is_callable():
                wait = max(0.0, circuit.open_until - now)
                self._total_blocked += 1
                return True, wait

            # active flood
            remaining = tracker.max_remaining_sec()
            if remaining > 0:
                self._total_blocked += 1
                return True, remaining

            return False, 0.0

    async def record_flood(self,
                            phone: str,
                            wait_seconds: int,
                            source: str = "") -> FloodRecord:
        """
        Called when a FloodWaitError is received for an account.
        """
        await self.register(phone)
        async with self._lock:
            self._total_floods += 1
            tracker = self._trackers[phone]
            circuit = self._circuits[phone]
            rec     = tracker.record_flood(wait_seconds, source)

            # escalation multiplier
            mult = 1 + 0.5 * tracker._escalation_level
            effective_wait = wait_seconds * mult

            # trip circuit on heavy/extreme floods
            if rec.level >= FloodLevel.HEAVY:
                tripped = circuit.record_failure(force_open=True)
                circuit.extend_cool_down(effective_wait)
                if tripped:
                    self._total_trips += 1
                    self._emit(GuardEvent.CIRCUIT_OPEN, {"phone": phone, "wait_sec": effective_wait})

            if tracker._escalation_level > 0:
                self._emit(GuardEvent.ESCALATION, {
                    "phone": phone,
                    "escalation_level": tracker._escalation_level,
                    "wait_sec": wait_seconds,
                })

            self._emit(GuardEvent.FLOOD_DETECTED, {
                "phone": phone,
                "wait_sec": wait_seconds,
                "level": rec.level.name,
                "source": source,
            })

            await self._check_global_flood()
            return rec

    async def record_success(self, phone: str) -> None:
        """Call after a successful Telegram API call for this account."""
        await self.register(phone)
        async with self._lock:
            circuit = self._circuits[phone]
            prev    = circuit.state
            circuit.record_success()
            if prev != CircuitState.CLOSED and circuit.state == CircuitState.CLOSED:
                self._emit(GuardEvent.CIRCUIT_CLOSED, {"phone": phone})

    async def record_failure(self, phone: str, error: str = "") -> bool:
        """
        Call on generic failure (not flood).
        Returns True if circuit tripped to OPEN.
        """
        await self.register(phone)
        async with self._lock:
            circuit = self._circuits[phone]
            tripped = circuit.record_failure()
            if tripped:
                self._total_trips += 1
                self._emit(GuardEvent.CIRCUIT_OPEN, {
                    "phone": phone, "error": error, "cool_down": circuit.cool_down_sec
                })
            return tripped

    async def force_open(self, phone: str, cool_down_sec: Optional[float] = None) -> None:
        """Manually trip the circuit for an account."""
        await self.register(phone)
        async with self._lock:
            circuit = self._circuits[phone]
            if cool_down_sec:
                circuit.cool_down_sec = cool_down_sec
            circuit.record_failure(force_open=True)
            self._emit(GuardEvent.CIRCUIT_OPEN, {"phone": phone, "manual": True})

    async def force_close(self, phone: str) -> None:
        """Manually reset the circuit and clear flood records."""
        await self.register(phone)
        async with self._lock:
            self._circuits[phone].force_close()
            self._trackers[phone].resolve_all()
            self._emit(GuardEvent.CIRCUIT_CLOSED, {"phone": phone, "manual": True})

    # --- global flood ---

    async def _check_global_flood(self) -> None:
        """Must be called under self._lock."""
        phones    = list(self._trackers.keys())
        if not phones:
            return
        flooded   = sum(1 for p in phones if self._trackers[p].current_level() > FloodLevel.NONE)
        ratio     = flooded / len(phones)
        if ratio >= self._GLOBAL_FLOOD_RATIO:
            # compute global pause as max remaining * multiplier
            max_rem = max((self._trackers[p].max_remaining_sec() for p in phones), default=0.0)
            pause   = max_rem * self._GLOBAL_PAUSE_MULT
            self._global_pause_until = time.monotonic() + pause
            logger.warning("Global pause %.0fs (%.0f%% accounts flooded)", pause, ratio * 100)
            self._emit(GuardEvent.GLOBAL_PAUSE, {"pause_sec": pause, "ratio": ratio})

    async def clear_global_pause(self) -> None:
        async with self._lock:
            self._global_pause_until = 0.0

    # --- context manager ---

    class _Guard:
        def __init__(self, fg: "FloodGuard", phone: str, source: str):
            self._fg     = fg
            self._phone  = phone
            self._source = source
            self._blocked = False

        async def __aenter__(self) -> "FloodGuard._Guard":
            blocked, wait = await self._fg.is_blocked(self._phone)
            self._blocked = blocked
            if blocked:
                raise FloodBlockedError(self._phone, wait)
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
            if exc_type is None:
                await self._fg.record_success(self._phone)
                return False
            name = exc_type.__name__ if exc_type else ""
            if "FloodWait" in name:
                import re
                m = re.search(r"(\d+)", str(exc_val))
                wait_sec = int(m.group(1)) if m else 60
                await self._fg.record_flood(self._phone, wait_sec, self._source)
            else:
                await self._fg.record_failure(self._phone, str(exc_val))
            return False

    def guard(self, phone: str, source: str = "") -> "_Guard":
        """
        Async context manager — checks block state on entry,
        records success/failure/flood on exit.

        Usage:
            async with flood_guard.guard(phone, "join_group"):
                await client.join_channel(...)
        """
        return self._Guard(self, phone, source)

    # --- wait helper ---

    async def wait_until_clear(self,
                                phone: str,
                                *,
                                max_wait: float = 600.0,
                                poll_interval: float = 5.0) -> bool:
        """
        Block until the account's flood/circuit clears.
        Returns True when clear, False if max_wait exceeded.
        """
        deadline = time.monotonic() + max_wait
        while time.monotonic() < deadline:
            blocked, remaining = await self.is_blocked(phone)
            if not blocked:
                return True
            sleep = min(remaining, poll_interval, deadline - time.monotonic())
            if sleep <= 0:
                break
            await asyncio.sleep(sleep)
        return False

    # --- stats & reporting ---

    def summary(self, phone: str) -> Optional[FloodSummary]:
        tracker = self._trackers.get(phone)
        circuit = self._circuits.get(phone)
        if not tracker or not circuit:
            return None
        return tracker.summary(circuit.state)

    def all_summaries(self) -> dict[str, FloodSummary]:
        result = {}
        for phone in self._trackers:
            s = self.summary(phone)
            if s:
                result[phone] = s
        return result

    def circuit_stats(self, phone: str) -> Optional[CircuitStats]:
        c = self._circuits.get(phone)
        return c.stats() if c else None

    def report(self) -> dict:
        global_paused = time.monotonic() < self._global_pause_until
        return {
            "global_paused": global_paused,
            "global_pause_remaining_sec": max(0.0, self._global_pause_until - time.monotonic()),
            "total_floods":  self._total_floods,
            "total_trips":   self._total_trips,
            "total_blocked": self._total_blocked,
            "accounts": {
                phone: {
                    "summary": {
                        "current_level":   s.current_level.name,
                        "active_records":  s.active_records,
                        "total_records":   s.total_records,
                        "circuit_state":   s.circuit_state.value,
                        "flood_rate_1h":   round(s.flood_rate_1h, 2),
                    },
                    "circuit": {
                        "state":                cs.state.value,
                        "consecutive_failures": cs.consecutive_failures,
                        "total_trips":          cs.total_trips,
                        "failure_rate_1m":      round(cs.failure_rate_1m, 3),
                    } if (cs := self.circuit_stats(phone)) else {},
                }
                for phone in self._trackers
                if (s := self.summary(phone))
            }
        }

    # --- events ---

    def on_event(self, cb: Callable[[GuardEvent, dict], None]) -> None:
        self._event_cbs.append(cb)

    def _emit(self, event: GuardEvent, data: dict) -> None:
        data["ts"] = time.time()
        for cb in self._event_cbs:
            try:
                cb(event, data)
            except Exception:
                pass

    # --- recovery ---

    def _on_account_recovered(self, phone: str) -> None:
        logger.info("FloodGuard: %s recovered from flood", phone)
        self._emit(GuardEvent.RECOVERY_DONE, {"phone": phone})

    def _get_flooded_map(self) -> dict[str, float]:
        return {
            phone: tracker.max_remaining_sec()
            for phone, tracker in self._trackers.items()
            if tracker.max_remaining_sec() > 0
        }

    # --- persistence ---

    async def _save_state(self) -> None:
        if not self._store_path:
            return
        state = {
            "global_pause_until": self._global_pause_until,
            "total_floods":  self._total_floods,
            "total_trips":   self._total_trips,
            "total_blocked": self._total_blocked,
            "trackers": {
                phone: t.to_dict()
                for phone, t in self._trackers.items()
            },
            "circuits": {
                phone: {
                    "consecutive_failures": c.consecutive_failures,
                    "total_trips": c.total_trips,
                    "open_until":  c.open_until,
                    "state":       c.state.value,
                }
                for phone, c in self._circuits.items()
            },
        }
        self._store_path.parent.mkdir(parents=True, exist_ok=True)
        self._store_path.write_text(json.dumps(state, indent=2), encoding="utf-8")

    async def _load_state(self) -> None:
        if not self._store_path or not self._store_path.exists():
            return
        try:
            raw = json.loads(self._store_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return

        self._global_pause_until = raw.get("global_pause_until", 0.0)
        self._total_floods       = raw.get("total_floods", 0)
        self._total_trips        = raw.get("total_trips", 0)
        self._total_blocked      = raw.get("total_blocked", 0)

        for phone, td in raw.get("trackers", {}).items():
            self._trackers[phone] = AccountFloodTracker.from_dict(td)

        for phone, cd in raw.get("circuits", {}).items():
            if phone not in self._circuits:
                self._circuits[phone] = CircuitBreaker(
                    phone,
                    failure_threshold=self._cb_fail_thresh,
                    success_threshold=self._cb_success_thresh,
                    cool_down_sec=self._cb_cool_down,
                )
            c = self._circuits[phone]
            c.consecutive_failures = cd.get("consecutive_failures", 0)
            c.total_trips          = cd.get("total_trips", 0)
            c.open_until           = cd.get("open_until", 0.0)
            try:
                c.state = CircuitState(cd.get("state", "closed"))
            except ValueError:
                c.state = CircuitState.CLOSED


# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------

class FloodBlockedError(Exception):
    def __init__(self, phone: str, wait_sec: float):
        self.phone    = phone
        self.wait_sec = wait_sec
        super().__init__(f"Account {phone} is flood-blocked for {wait_sec:.0f}s")


# ---------------------------------------------------------------------------
# Integrator — wires FloodGuard + RateController together
# ---------------------------------------------------------------------------

class FloodAwareThrottler:
    """
    Combines FloodGuard + RateController into a single call gate.

    Call flow:
      1. FloodGuard.is_blocked(phone) → if blocked, raise or wait
      2. RateController.wait(phone, action) → rate-limit
      3. Execute the coroutine
      4. On FloodWaitError → record_flood + apply_flood_wait
      5. On success → record_success
    """

    def __init__(self,
                 guard: FloodGuard,
                 rate_ctrl: "RateController",    # imported from rate_controller
                 max_wait_guard: float = 300.0,
                 max_wait_rate:  float = 120.0):
        self._guard      = guard
        self._rate       = rate_ctrl
        self._mw_guard   = max_wait_guard
        self._mw_rate    = max_wait_rate

    async def call(self,
                   phone: str,
                   action: str,          # ActionType.value string
                   fn: Callable,
                   *args,
                   retries: int = 3,
                   **kwargs):
        from infrastructure.rate_controller import ActionType, Priority

        try:
            act = ActionType(action)
        except ValueError:
            act = ActionType.GENERIC

        for attempt in range(retries):
            # flood check
            clear = await self._guard.wait_until_clear(phone, max_wait=self._mw_guard)
            if not clear:
                raise FloodBlockedError(phone, self._guard._trackers.get(phone, AccountFloodTracker(phone)).max_remaining_sec())

            # rate check
            ok = await self._rate.wait(phone, act, max_wait=self._mw_rate)
            if not ok:
                raise TimeoutError(f"Rate limit wait exceeded for {phone}/{action}")

            try:
                result = await fn(*args, **kwargs)
                await self._guard.record_success(phone)
                return result
            except Exception as exc:
                name = type(exc).__name__
                if "FloodWait" in name:
                    import re
                    m = re.search(r"(\d+)", str(exc))
                    wait_sec = int(m.group(1)) if m else 60
                    await self._guard.record_flood(phone, wait_sec, action)
                    await self._rate.apply_flood_wait(phone, wait_sec)
                    if attempt < retries - 1:
                        await asyncio.sleep(wait_sec)
                        continue
                else:
                    await self._guard.record_failure(phone, str(exc))
                raise

        raise RuntimeError("All retries exhausted")


# ---------------------------------------------------------------------------
# Convenience builders
# ---------------------------------------------------------------------------

async def build_flood_guard(
    phones: list[str],
    *,
    store_path: Optional[str | Path] = "data/flood_guard.json",
    failure_threshold: int   = 5,
    cool_down_sec:     float = 60.0,
    auto_recovery:     bool  = True,
) -> FloodGuard:
    guard = FloodGuard(
        store_path=store_path,
        circuit_failure_threshold=failure_threshold,
        circuit_cool_down_sec=cool_down_sec,
        auto_recovery=auto_recovery,
    )
    await guard.start()
    await guard.register_many(phones)
    return guard


def flood_level_description(level: FloodLevel) -> str:
    return {
        FloodLevel.NONE:     "clear",
        FloodLevel.LIGHT:    "light (<60s)",
        FloodLevel.MODERATE: "moderate (60-300s)",
        FloodLevel.HEAVY:    "heavy (5-60min)",
        FloodLevel.EXTREME:  "extreme (>1h) — possible IP/account restriction",
    }[level]


def circuit_state_description(state: CircuitState) -> str:
    return {
        CircuitState.CLOSED:    "normal — calls passing",
        CircuitState.OPEN:      "BLOCKED — cooling down",
        CircuitState.HALF_OPEN: "probing — one call allowed",
    }[state]


async def print_flood_report(guard: FloodGuard) -> None:
    report = guard.report()
    print(f"\n{'='*60}")
    print("FLOOD GUARD REPORT")
    print(f"{'='*60}")
    if report["global_paused"]:
        print(f"⛔ GLOBAL PAUSE: {report['global_pause_remaining_sec']:.0f}s remaining")
    print(f"Total floods: {report['total_floods']}")
    print(f"Total circuit trips: {report['total_trips']}")
    print(f"Total blocked calls: {report['total_blocked']}")
    print(f"\nAccounts:")
    for phone, data in report["accounts"].items():
        s   = data["summary"]
        cst = data.get("circuit", {}).get("state", "unknown")
        print(f"  {phone}  level={s['current_level']}  circuit={cst}  "
              f"floods_1h={s['flood_rate_1h']}")