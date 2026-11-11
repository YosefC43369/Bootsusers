# rate_controller.py
# Full rate-limiting stack: token bucket (burst), sliding window (sustained),
# per-account per-action limits, adaptive backoff, priority lanes, persistence.

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
from typing import Callable, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enums & constants
# ---------------------------------------------------------------------------

class ActionType(enum.Enum):
    """Telegram actions that have distinct rate limits."""
    JOIN_GROUP        = "join_group"
    REACT             = "react"
    SEND_MESSAGE      = "send_message"
    EDIT_MESSAGE      = "edit_message"
    DELETE_MESSAGE    = "delete_message"
    GET_ENTITY        = "get_entity"
    RESOLVE_USERNAME  = "resolve_username"
    EXPORT_INVITE     = "export_invite"
    REVOKE_INVITE     = "revoke_invite"
    GET_PARTICIPANTS  = "get_participants"
    CHECK_INVITE      = "check_invite"
    GET_DIALOGS       = "get_dialogs"
    UPLOAD_FILE       = "upload_file"
    DOWNLOAD_FILE     = "download_file"
    GENERIC           = "generic"


class Priority(enum.IntEnum):
    LOW    = 0
    NORMAL = 1
    HIGH   = 2
    URGENT = 3


class LimitResult(enum.Enum):
    ALLOWED    = "allowed"
    THROTTLED  = "throttled"   # wait and retry
    REJECTED   = "rejected"    # over hard cap, drop


@dataclass
class RateWindow:
    """Sliding-window counter for a fixed time period."""
    period_sec: float
    max_calls:  int
    _timestamps: deque = field(default_factory=deque)

    def is_allowed(self) -> bool:
        now = time.monotonic()
        cutoff = now - self.period_sec
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()
        if len(self._timestamps) < self.max_calls:
            self._timestamps.append(now)
            return True
        return False

    def wait_time(self) -> float:
        """Seconds until a slot opens."""
        if len(self._timestamps) < self.max_calls:
            return 0.0
        oldest = self._timestamps[0]
        return max(0.0, (oldest + self.period_sec) - time.monotonic())

    def reset(self) -> None:
        self._timestamps.clear()


# ---------------------------------------------------------------------------
# Token bucket
# ---------------------------------------------------------------------------

class TokenBucket:
    """
    Classic leaky/token bucket for burst control.

    capacity   — max tokens (burst ceiling)
    rate_per_s — tokens added per second (sustained throughput)
    """

    def __init__(self, capacity: float, rate_per_s: float):
        self.capacity   = capacity
        self.rate_per_s = rate_per_s
        self._tokens    = capacity
        self._last_refill: float = time.monotonic()
        self._lock      = asyncio.Lock()

    def _refill(self) -> None:
        now     = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate_per_s)
        self._last_refill = now

    async def acquire(self, cost: float = 1.0) -> bool:
        """Non-blocking. Returns True if token was acquired."""
        async with self._lock:
            self._refill()
            if self._tokens >= cost:
                self._tokens -= cost
                return True
            return False

    async def wait_and_acquire(self, cost: float = 1.0, timeout: float = 60.0) -> bool:
        """Blocking. Returns True when acquired, False if timeout hit."""
        deadline = time.monotonic() + timeout
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= cost:
                    self._tokens -= cost
                    return True
                deficit   = cost - self._tokens
                wait_time = deficit / self.rate_per_s
            if time.monotonic() + wait_time > deadline:
                return False
            await asyncio.sleep(min(wait_time, 0.1))

    @property
    def available(self) -> float:
        self._refill()
        return self._tokens

    def reset(self) -> None:
        self._tokens    = self.capacity
        self._last_refill = time.monotonic()


# ---------------------------------------------------------------------------
# Action limit spec
# ---------------------------------------------------------------------------

@dataclass
class ActionLimitSpec:
    """
    Layered rate limit for one action type.

    burst_capacity  — max tokens in bucket (short burst)
    burst_rate      — tokens/s refill
    per_minute      — sliding window cap
    per_hour        — sliding window cap
    per_day         — sliding window cap
    min_gap_ms      — minimum milliseconds between consecutive calls
    """
    burst_capacity: float = 10.0
    burst_rate:     float = 1.0       # tokens/s
    per_minute:     int   = 30
    per_hour:       int   = 300
    per_day:        int   = 2000
    min_gap_ms:     float = 100.0
    hard_cap_per_hour: int = 500      # REJECTED (drop) if exceeded


# Default specs per action type — calibrated for Telegram's undocumented limits
_DEFAULT_SPECS: dict[ActionType, ActionLimitSpec] = {
    ActionType.JOIN_GROUP:       ActionLimitSpec(burst_capacity=3,  burst_rate=0.05, per_minute=2,  per_hour=10,  per_day=50,   min_gap_ms=5000, hard_cap_per_hour=20),
    ActionType.REACT:            ActionLimitSpec(burst_capacity=20, burst_rate=2.0,  per_minute=60, per_hour=400, per_day=2000, min_gap_ms=50),
    ActionType.SEND_MESSAGE:     ActionLimitSpec(burst_capacity=10, burst_rate=0.5,  per_minute=20, per_hour=100, per_day=500,  min_gap_ms=200),
    ActionType.EDIT_MESSAGE:     ActionLimitSpec(burst_capacity=8,  burst_rate=0.5,  per_minute=15, per_hour=80,  per_day=400,  min_gap_ms=300),
    ActionType.DELETE_MESSAGE:   ActionLimitSpec(burst_capacity=10, burst_rate=1.0,  per_minute=20, per_hour=100, per_day=500,  min_gap_ms=100),
    ActionType.GET_ENTITY:       ActionLimitSpec(burst_capacity=20, burst_rate=2.0,  per_minute=60, per_hour=400, per_day=3000, min_gap_ms=50),
    ActionType.RESOLVE_USERNAME: ActionLimitSpec(burst_capacity=10, burst_rate=0.5,  per_minute=10, per_hour=50,  per_day=200,  min_gap_ms=500),
    ActionType.EXPORT_INVITE:    ActionLimitSpec(burst_capacity=5,  burst_rate=0.1,  per_minute=3,  per_hour=20,  per_day=100,  min_gap_ms=2000),
    ActionType.REVOKE_INVITE:    ActionLimitSpec(burst_capacity=5,  burst_rate=0.1,  per_minute=3,  per_hour=20,  per_day=100,  min_gap_ms=2000),
    ActionType.GET_PARTICIPANTS: ActionLimitSpec(burst_capacity=5,  burst_rate=0.2,  per_minute=5,  per_hour=30,  per_day=200,  min_gap_ms=1000),
    ActionType.CHECK_INVITE:     ActionLimitSpec(burst_capacity=10, burst_rate=0.5,  per_minute=10, per_hour=60,  per_day=300,  min_gap_ms=500),
    ActionType.GET_DIALOGS:      ActionLimitSpec(burst_capacity=5,  burst_rate=0.2,  per_minute=5,  per_hour=20,  per_day=100,  min_gap_ms=1000),
    ActionType.UPLOAD_FILE:      ActionLimitSpec(burst_capacity=3,  burst_rate=0.1,  per_minute=3,  per_hour=10,  per_day=50,   min_gap_ms=3000),
    ActionType.DOWNLOAD_FILE:    ActionLimitSpec(burst_capacity=5,  burst_rate=0.2,  per_minute=5,  per_hour=30,  per_day=200,  min_gap_ms=500),
    ActionType.GENERIC:          ActionLimitSpec(burst_capacity=15, burst_rate=1.0,  per_minute=30, per_hour=200, per_day=1000, min_gap_ms=100),
}


# ---------------------------------------------------------------------------
# Per-account per-action limiter
# ---------------------------------------------------------------------------

@dataclass
class _ActionState:
    spec:        ActionLimitSpec
    bucket:      TokenBucket
    win_minute:  RateWindow
    win_hour:    RateWindow
    win_day:     RateWindow
    hard_hour:   RateWindow
    last_call_ts: float = 0.0
    total_calls:  int   = 0
    total_throttled: int = 0
    total_rejected:  int = 0
    wait_ms_history: deque = field(default_factory=lambda: deque(maxlen=100))


def _make_action_state(spec: ActionLimitSpec) -> _ActionState:
    return _ActionState(
        spec       = spec,
        bucket     = TokenBucket(spec.burst_capacity, spec.burst_rate),
        win_minute = RateWindow(60.0,    spec.per_minute),
        win_hour   = RateWindow(3600.0,  spec.per_hour),
        win_day    = RateWindow(86400.0, spec.per_day),
        hard_hour  = RateWindow(3600.0,  spec.hard_cap_per_hour),
    )


class AccountLimiter:
    """
    Single-account rate limiter — holds one _ActionState per ActionType.
    Thread-safe via asyncio.Lock.
    """

    def __init__(self, phone: str, specs: Optional[dict[ActionType, ActionLimitSpec]] = None):
        self.phone       = phone
        self._specs      = specs or _DEFAULT_SPECS
        self._states: dict[ActionType, _ActionState] = {}
        self._lock       = asyncio.Lock()
        self._global_pause_until: float = 0.0   # flood-induced pause

    def _get_state(self, action: ActionType) -> _ActionState:
        if action not in self._states:
            spec = self._specs.get(action, _DEFAULT_SPECS[ActionType.GENERIC])
            self._states[action] = _make_action_state(spec)
        return self._states[action]

    async def check(self, action: ActionType) -> tuple[LimitResult, float]:
        """
        Returns (LimitResult, wait_seconds).
        ALLOWED  → proceed; wait=0
        THROTTLED → sleep wait seconds then retry
        REJECTED  → drop this call
        """
        async with self._lock:
            now = time.monotonic()

            # global pause (e.g., from FloodWait)
            if now < self._global_pause_until:
                wait = self._global_pause_until - now
                return LimitResult.THROTTLED, wait

            st = self._get_state(action)

            # hard cap → reject immediately
            if not st.hard_hour.is_allowed():
                st.total_rejected += 1
                # note: hard_hour.is_allowed() already pops from window; revert by not recording
                return LimitResult.REJECTED, 0.0

            # min gap check
            gap_wait = max(0.0, st.last_call_ts + st.spec.min_gap_ms / 1000.0 - now)
            if gap_wait > 0:
                st.total_throttled += 1
                st.wait_ms_history.append(gap_wait * 1000)
                return LimitResult.THROTTLED, gap_wait

            # sliding windows
            for win in (st.win_minute, st.win_hour, st.win_day):
                wt = win.wait_time()
                if wt > 0:
                    st.total_throttled += 1
                    st.wait_ms_history.append(wt * 1000)
                    return LimitResult.THROTTLED, wt

            # token bucket
            if not await st.bucket.acquire():
                deficit = 1.0 - st.bucket.available
                wt = deficit / st.spec.burst_rate
                st.total_throttled += 1
                st.wait_ms_history.append(wt * 1000)
                return LimitResult.THROTTLED, wt

            # all clear — record call
            st.last_call_ts = now
            st.total_calls += 1
            # consume sliding window slots (they're consumed inside is_allowed)
            st.win_minute.is_allowed()
            st.win_hour.is_allowed()
            st.win_day.is_allowed()
            return LimitResult.ALLOWED, 0.0

    async def wait_until_allowed(self,
                                  action: ActionType,
                                  *,
                                  max_wait: float = 300.0,
                                  priority: Priority = Priority.NORMAL) -> bool:
        """
        Block until the action is allowed or max_wait is exceeded.
        Returns True if allowed, False if timed out.
        HIGH/URGENT priority cuts sleep intervals by 50%.
        """
        deadline = time.monotonic() + max_wait
        backoff  = 0.05
        urgency  = 0.5 if priority >= Priority.HIGH else 1.0

        while True:
            result, wait_sec = await self.check(action)
            if result == LimitResult.ALLOWED:
                return True
            if result == LimitResult.REJECTED:
                return False
            # THROTTLED
            sleep_for = min(wait_sec * urgency, deadline - time.monotonic())
            if sleep_for <= 0:
                return False
            await asyncio.sleep(sleep_for)

    def apply_flood_wait(self, seconds: float) -> None:
        """Call when a FloodWaitError arrives for this account."""
        self._global_pause_until = time.monotonic() + seconds
        # also cool down the per-action states
        for st in self._states.values():
            st.win_minute.reset()

    def stats(self, action: Optional[ActionType] = None) -> dict:
        result = {}
        targets = {action: self._get_state(action)} if action else self._states
        for act, st in targets.items():
            wh = sorted(st.wait_ms_history)
            result[act.value] = {
                "total_calls":     st.total_calls,
                "total_throttled": st.total_throttled,
                "total_rejected":  st.total_rejected,
                "avg_wait_ms":     statistics.mean(wh) if wh else 0,
                "p95_wait_ms":     wh[max(0, int(len(wh)*0.95)-1)] if wh else 0,
                "tokens_available": round(st.bucket.available, 2),
                "last_call_ts":    st.last_call_ts,
            }
        return result

    def reset(self, action: Optional[ActionType] = None) -> None:
        targets = [action] if action else list(self._states.keys())
        for act in targets:
            if act in self._states:
                st = self._states[act]
                st.bucket.reset()
                st.win_minute.reset()
                st.win_hour.reset()
                st.win_day.reset()
                st.hard_hour.reset()
                st.last_call_ts = 0.0
        self._global_pause_until = 0.0


# ---------------------------------------------------------------------------
# Global rate controller
# ---------------------------------------------------------------------------

class RateController:
    """
    Central controller for all accounts and all actions.

    Usage
    -----
    rc = RateController()
    rc.add_account(phone)

    allowed = await rc.acquire(phone, ActionType.REACT)
    if not allowed:
        # skip or enqueue

    # or block:
    await rc.wait(phone, ActionType.JOIN_GROUP, priority=Priority.HIGH)
    """

    def __init__(self,
                 specs: Optional[dict[ActionType, ActionLimitSpec]] = None,
                 store_path: Optional[str | Path] = None):
        self._specs  = specs or _DEFAULT_SPECS
        self._limiters: dict[str, AccountLimiter] = {}
        self._store_path = Path(store_path) if store_path else None
        self._lock   = asyncio.Lock()
        self._global_pause_until: float = 0.0
        self._event_cbs: list[Callable[[str, dict], None]] = []

        # aggregate stats
        self._total_allowed   = 0
        self._total_throttled = 0
        self._total_rejected  = 0

    # --- account management ---

    async def add_account(self, phone: str) -> AccountLimiter:
        async with self._lock:
            if phone not in self._limiters:
                self._limiters[phone] = AccountLimiter(phone, self._specs)
            return self._limiters[phone]

    async def remove_account(self, phone: str) -> bool:
        async with self._lock:
            return bool(self._limiters.pop(phone, None))

    def get_limiter(self, phone: str) -> Optional[AccountLimiter]:
        return self._limiters.get(phone)

    async def ensure_limiter(self, phone: str) -> AccountLimiter:
        if phone not in self._limiters:
            return await self.add_account(phone)
        return self._limiters[phone]

    # --- core acquire ---

    async def acquire(self,
                      phone: str,
                      action: ActionType,
                      *,
                      priority: Priority = Priority.NORMAL) -> tuple[LimitResult, float]:
        """
        Non-blocking check. Returns (LimitResult, wait_seconds).
        """
        # global pause (e.g., system-wide rate limit)
        now = time.monotonic()
        if now < self._global_pause_until:
            wait = self._global_pause_until - now
            self._total_throttled += 1
            return LimitResult.THROTTLED, wait

        limiter = await self.ensure_limiter(phone)
        result, wait = await limiter.check(action)

        if result == LimitResult.ALLOWED:
            self._total_allowed += 1
        elif result == LimitResult.THROTTLED:
            self._total_throttled += 1
            self._emit("throttled", {"phone": phone, "action": action.value, "wait_sec": wait})
        else:
            self._total_rejected += 1
            self._emit("rejected", {"phone": phone, "action": action.value})

        return result, wait

    async def wait(self,
                   phone: str,
                   action: ActionType,
                   *,
                   max_wait: float = 300.0,
                   priority: Priority = Priority.NORMAL) -> bool:
        """
        Blocking acquire. Returns True when allowed.
        URGENT / HIGH priority shorten sleep intervals.
        """
        limiter = await self.ensure_limiter(phone)
        ok = await limiter.wait_until_allowed(action, max_wait=max_wait, priority=priority)
        if ok:
            self._total_allowed += 1
        return ok

    # --- context manager ---

    class _Ticket:
        def __init__(self, ctrl: "RateController", phone: str, action: ActionType,
                     priority: Priority, max_wait: float):
            self._ctrl     = ctrl
            self._phone    = phone
            self._action   = action
            self._priority = priority
            self._max_wait = max_wait

        async def __aenter__(self) -> bool:
            self._ok = await self._ctrl.wait(
                self._phone, self._action,
                max_wait=self._max_wait, priority=self._priority
            )
            return self._ok

        async def __aexit__(self, *_) -> None:
            pass

    def ticket(self,
               phone: str,
               action: ActionType,
               *,
               priority: Priority = Priority.NORMAL,
               max_wait: float = 300.0) -> "_Ticket":
        """
        Usage:
            async with rate_ctrl.ticket(phone, ActionType.REACT) as ok:
                if ok:
                    await client.send_reaction(...)
        """
        return self._Ticket(self, phone, action, priority, max_wait)

    # --- flood propagation ---

    async def apply_flood_wait(self,
                                phone: str,
                                seconds: float,
                                *,
                                propagate_global: bool = False) -> None:
        limiter = await self.ensure_limiter(phone)
        limiter.apply_flood_wait(seconds)
        logger.info("FloodWait applied to %s for %.0fs", phone, seconds)
        self._emit("flood_wait", {"phone": phone, "seconds": seconds})

        if propagate_global and seconds > 60:
            # Extreme flood — slow down all accounts
            jitter_factor = 0.2
            self._global_pause_until = time.monotonic() + seconds * jitter_factor
            logger.warning("Global pause %.0fs due to extreme flood on %s",
                           seconds * jitter_factor, phone)

    # --- custom spec override ---

    async def set_spec(self, phone: str, action: ActionType, spec: ActionLimitSpec) -> None:
        limiter = await self.ensure_limiter(phone)
        async with limiter._lock:
            old_state = limiter._states.pop(action, None)
            new_state = _make_action_state(spec)
            if old_state:
                new_state.total_calls    = old_state.total_calls
                new_state.total_throttled = old_state.total_throttled
            limiter._states[action] = new_state

    async def set_global_spec(self, action: ActionType, spec: ActionLimitSpec) -> None:
        self._specs[action] = spec
        for limiter in self._limiters.values():
            await self.set_spec(limiter.phone, action, spec)

    # --- stats & reporting ---

    def stats(self, phone: Optional[str] = None) -> dict:
        if phone:
            limiter = self._limiters.get(phone)
            if not limiter:
                return {}
            return limiter.stats()
        return {
            "global": {
                "total_allowed":   self._total_allowed,
                "total_throttled": self._total_throttled,
                "total_rejected":  self._total_rejected,
                "accounts":        len(self._limiters),
            },
            "accounts": {
                ph: lim.stats() for ph, lim in self._limiters.items()
            },
        }

    def reset(self, phone: Optional[str] = None, action: Optional[ActionType] = None) -> None:
        if phone:
            lim = self._limiters.get(phone)
            if lim:
                lim.reset(action)
        else:
            for lim in self._limiters.values():
                lim.reset(action)

    # --- persistence ---

    async def save_state(self) -> None:
        if not self._store_path:
            return
        snapshot: dict = {}
        for phone, lim in self._limiters.items():
            snapshot[phone] = {
                "global_pause_until": lim._global_pause_until,
                "stats": lim.stats(),
            }
        self._store_path.parent.mkdir(parents=True, exist_ok=True)
        self._store_path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")

    async def load_state(self) -> None:
        if not self._store_path or not self._store_path.exists():
            return
        try:
            snap = json.loads(self._store_path.read_text(encoding="utf-8"))
            for phone, data in snap.items():
                lim = await self.ensure_limiter(phone)
                pause = data.get("global_pause_until", 0.0)
                if pause > time.monotonic():
                    lim._global_pause_until = pause
        except Exception as exc:
            logger.warning("Failed to load rate state: %s", exc)

    # --- events ---

    def on_event(self, cb: Callable[[str, dict], None]) -> None:
        self._event_cbs.append(cb)

    def _emit(self, name: str, data: dict) -> None:
        for cb in self._event_cbs:
            try:
                cb(name, data)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Adaptive backoff calculator
# ---------------------------------------------------------------------------

class AdaptiveBackoff:
    """
    Tracks recent throttle/flood events per account and computes
    an adaptive multiplier to slow down preemptively.
    """

    def __init__(self,
                 window_sec: float = 300.0,
                 base_multiplier: float = 1.0,
                 max_multiplier: float = 10.0,
                 decay_rate: float = 0.9):
        self._window   = window_sec
        self._base     = base_multiplier
        self._max      = max_multiplier
        self._decay    = decay_rate
        self._events: deque = deque()   # (timestamp, severity)

    def record(self, severity: float = 1.0) -> None:
        self._events.append((time.monotonic(), severity))

    def _prune(self) -> None:
        cutoff = time.monotonic() - self._window
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()

    def multiplier(self) -> float:
        self._prune()
        if not self._events:
            return self._base
        total_severity = sum(sev for _, sev in self._events)
        raw = self._base + math.log1p(total_severity) * self._decay
        return min(raw, self._max)

    def reset(self) -> None:
        self._events.clear()


# ---------------------------------------------------------------------------
# Multi-account coordinator
# ---------------------------------------------------------------------------

class MultiAccountRateCoordinator:
    """
    Wraps a RateController and adds cross-account coordination:
    - Load balancing: pick the least-throttled account for a job
    - Global throttle tracking
    - Jitter injection to avoid thundering-herd
    """

    def __init__(self,
                 controller: RateController,
                 jitter_ms: float = 100.0):
        self._ctrl   = controller
        self._jitter = jitter_ms / 1000.0
        self._backoffs: dict[str, AdaptiveBackoff] = defaultdict(AdaptiveBackoff)

    async def pick_account(self,
                            phones: list[str],
                            action: ActionType) -> Optional[str]:
        """Return the phone that can proceed soonest for this action."""
        best_phone: Optional[str] = None
        best_wait  = float("inf")
        for phone in phones:
            _, wait = await self._ctrl.acquire(phone, action)
            if wait == 0.0:
                return phone   # immediately available
            if wait < best_wait:
                best_wait  = wait
                best_phone = phone
        return best_phone

    async def execute(self,
                      phones: list[str],
                      action: ActionType,
                      coro_factory: Callable[[str], "asyncio.Coroutine"],
                      *,
                      max_wait: float = 120.0,
                      priority: Priority = Priority.NORMAL) -> tuple[Optional[str], Optional[Exception]]:
        """
        Pick an account, wait until allowed, run the coro.
        Returns (phone_used, exception_or_None).
        """
        # pick & jitter
        phone = await self.pick_account(phones, action)
        if not phone:
            return None, RuntimeError("No accounts available")

        multiplier = self._backoffs[phone].multiplier()
        if self._jitter > 0:
            await asyncio.sleep(random.uniform(0, self._jitter) * multiplier)

        ok = await self._ctrl.wait(phone, action, max_wait=max_wait, priority=priority)
        if not ok:
            self._backoffs[phone].record(1.0)
            return phone, TimeoutError(f"Rate limit wait exceeded {max_wait}s for {phone}/{action.value}")

        try:
            await coro_factory(phone)
            self._backoffs[phone].reset()
            return phone, None
        except Exception as exc:
            self._backoffs[phone].record(2.0)
            return phone, exc


# ---------------------------------------------------------------------------
# Presets
# ---------------------------------------------------------------------------

def aggressive_specs() -> dict[ActionType, ActionLimitSpec]:
    """Higher caps for trusted IPs / residential proxies."""
    out = dict(_DEFAULT_SPECS)
    out[ActionType.JOIN_GROUP]   = ActionLimitSpec(burst_capacity=5,  burst_rate=0.1,  per_minute=4,  per_hour=20,  per_day=100, min_gap_ms=3000, hard_cap_per_hour=30)
    out[ActionType.REACT]        = ActionLimitSpec(burst_capacity=30, burst_rate=3.0,  per_minute=90, per_hour=600, per_day=3000, min_gap_ms=30)
    out[ActionType.SEND_MESSAGE] = ActionLimitSpec(burst_capacity=15, burst_rate=1.0,  per_minute=30, per_hour=150, per_day=800,  min_gap_ms=100)
    return out


def conservative_specs() -> dict[ActionType, ActionLimitSpec]:
    """Lower caps to minimise risk of TG account restrictions."""
    out = dict(_DEFAULT_SPECS)
    out[ActionType.JOIN_GROUP]   = ActionLimitSpec(burst_capacity=1,  burst_rate=0.02, per_minute=1,  per_hour=5,   per_day=20,  min_gap_ms=10000, hard_cap_per_hour=8)
    out[ActionType.REACT]        = ActionLimitSpec(burst_capacity=10, burst_rate=0.5,  per_minute=20, per_hour=100, per_day=500,  min_gap_ms=200)
    out[ActionType.SEND_MESSAGE] = ActionLimitSpec(burst_capacity=5,  burst_rate=0.2,  per_minute=10, per_hour=50,  per_day=200,  min_gap_ms=500)
    return out


def build_rate_controller(
    phones: list[str],
    *,
    preset: str = "default",
    store_path: Optional[str | Path] = None,
) -> RateController:
    """
    Quick builder. preset: 'default' | 'aggressive' | 'conservative'
    """
    specs_map = {
        "default":      _DEFAULT_SPECS,
        "aggressive":   aggressive_specs(),
        "conservative": conservative_specs(),
    }
    specs = specs_map.get(preset, _DEFAULT_SPECS)
    ctrl  = RateController(specs=specs, store_path=store_path)
    # pre-register accounts synchronously (safe before event loop is running)
    for phone in phones:
        ctrl._limiters[phone] = AccountLimiter(phone, specs)
    return ctrl


# ---------------------------------------------------------------------------
# Rate-limit aware wrapper for Telethon calls
# ---------------------------------------------------------------------------

class ThrottledCaller:
    """
    Wraps any coroutine with rate-limit pre-check and FloodWait handling.

    Usage:
        caller = ThrottledCaller(rate_ctrl)
        result = await caller.call(phone, ActionType.REACT, client.send_reaction, chat, msg_id, ...)
    """

    def __init__(self,
                 controller: RateController,
                 max_wait: float = 120.0,
                 flood_wait_multiplier: float = 1.1):
        self._ctrl      = controller
        self._max_wait  = max_wait
        self._fwm       = flood_wait_multiplier

    async def call(self,
                   phone: str,
                   action: ActionType,
                   fn: Callable,
                   *args,
                   priority: Priority = Priority.NORMAL,
                   retries: int = 3,
                   **kwargs):
        """
        Rate-limited call with auto FloodWait retry.
        Raises the last exception if all retries exhaust.
        """
        for attempt in range(retries):
            ok = await self._ctrl.wait(phone, action, max_wait=self._max_wait, priority=priority)
            if not ok:
                raise TimeoutError(f"Rate limit exceeded for {phone}/{action.value}")
            try:
                return await fn(*args, **kwargs)
            except Exception as exc:
                name = type(exc).__name__
                if "FloodWait" in name:
                    import re
                    m = re.search(r"(\d+)", str(exc))
                    wait_sec = int(m.group(1)) if m else 60
                    actual = wait_sec * self._fwm
                    await self._ctrl.apply_flood_wait(phone, actual)
                    if attempt < retries - 1:
                        await asyncio.sleep(actual)
                        continue
                raise
        raise RuntimeError("Unreachable")