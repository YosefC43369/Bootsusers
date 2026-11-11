# reaction_scheduler.py, target: Windows 11
#
# Wiring (in auto_react.py main() or launcher.py):
#   from reaction_scheduler import ReactionScheduler, ScheduleConfig, TimeWindow
#   sched = ReactionScheduler(engine, default_tz="Asia/Bangkok")
#   sched.load_config("schedule.json")
#   asyncio.create_task(sched.run())
#   # In the NewMessage handler:
#   await sched.handle_message(chat_id, msg_id, text)

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta
from enum import Enum
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

logger = logging.getLogger(__name__)

# ── safe ZoneInfo ─────────────────────────────────────────────────────────────

def _tz(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, KeyError):
        logger.warning("Unknown timezone %r — falling back to UTC", name)
        return ZoneInfo("UTC")


# ══════════════════════════════════════════════════════════════════════════════
#  ENUMS
# ══════════════════════════════════════════════════════════════════════════════

class DayOfWeek(int, Enum):
    MON = 0
    TUE = 1
    WED = 2
    THU = 3
    FRI = 4
    SAT = 5
    SUN = 6


class ScheduleMode(str, Enum):
    ALWAYS   = "always"    # react to everything within active windows
    CALENDAR = "calendar"  # only on explicitly listed calendar dates
    WEEKDAY  = "weekday"   # only on specified days of week
    COMBINED = "combined"  # weekday filter AND calendar dates


# ══════════════════════════════════════════════════════════════════════════════
#  TIME WINDOW
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class TimeWindow:
    """
    A named time-of-day window, evaluated in a specific timezone.
    Example: TimeWindow("active", dtime(9,0), dtime(22,0), "Asia/Bangkok")
    """
    name:     str
    start:    dtime           # local time-of-day
    end:      dtime           # local time-of-day  (end < start = spans midnight)
    timezone: str = "UTC"
    weight:   float = 1.0    # relative frequency when multiple windows overlap

    def _tz(self) -> ZoneInfo:
        return _tz(self.timezone)

    def contains(self, dt: Optional[datetime] = None) -> bool:
        """True if `dt` (or now) falls inside this window."""
        tz  = self._tz()
        now = (dt or datetime.now(tz)).astimezone(tz)
        t   = now.time().replace(tzinfo=None)
        s, e = self.start, self.end
        if s <= e:                        # normal window (same day)
            return s <= t < e
        else:                             # midnight-spanning window
            return t >= s or t < e

    def next_open(self, dt: Optional[datetime] = None) -> datetime:
        """Return the next moment this window opens (may be today or tomorrow)."""
        tz  = self._tz()
        now = (dt or datetime.now(tz)).astimezone(tz)
        t   = now.time().replace(tzinfo=None)
        s   = self.start

        candidate = now.replace(
            hour=s.hour, minute=s.minute, second=0, microsecond=0
        )
        if candidate <= now:
            candidate += timedelta(days=1)
        return candidate

    def seconds_until_open(self, dt: Optional[datetime] = None) -> float:
        tz  = _tz(self.timezone)
        now = (dt or datetime.now(tz)).astimezone(tz)
        if self.contains(now):
            return 0.0
        nxt = self.next_open(now)
        return max(0.0, (nxt - now).total_seconds())

    def to_dict(self) -> dict:
        return {
            "name":     self.name,
            "start":    self.start.strftime("%H:%M"),
            "end":      self.end.strftime("%H:%M"),
            "timezone": self.timezone,
            "weight":   self.weight,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "TimeWindow":
        def _t(s: str) -> dtime:
            h, m = s.split(":")
            return dtime(int(h), int(m))
        return cls(
            name     = d["name"],
            start    = _t(d["start"]),
            end      = _t(d["end"]),
            timezone = d.get("timezone", "UTC"),
            weight   = float(d.get("weight", 1.0)),
        )


# ══════════════════════════════════════════════════════════════════════════════
#  CALENDAR ENTRY
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class CalendarEntry:
    """
    A single date with optional override windows and suppression flag.
    date_str format: "YYYY-MM-DD" or "MM-DD" (recurring every year).
    """
    date_str:    str                        # "2025-04-01" or "04-01"
    suppress:    bool        = False        # True = do NOT react on this date
    windows:     list[TimeWindow] = field(default_factory=list)
    label:       str         = ""          # e.g. "Songkran holiday"
    recurring:   bool        = False       # derive from "MM-DD" format

    def matches(self, d: date) -> bool:
        try:
            if self.recurring or len(self.date_str) == 5:
                mm, dd = self.date_str.split("-")
                return d.month == int(mm) and d.day == int(dd)
            else:
                return date.fromisoformat(self.date_str) == d
        except (ValueError, AttributeError):
            return False

    def to_dict(self) -> dict:
        return {
            "date":      self.date_str,
            "suppress":  self.suppress,
            "windows":   [w.to_dict() for w in self.windows],
            "label":     self.label,
            "recurring": self.recurring,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "CalendarEntry":
        return cls(
            date_str  = d["date"],
            suppress  = bool(d.get("suppress", False)),
            windows   = [TimeWindow.from_dict(w) for w in d.get("windows", [])],
            label     = d.get("label", ""),
            recurring = bool(d.get("recurring", False)),
        )


# ══════════════════════════════════════════════════════════════════════════════
#  CHAT SCHEDULE CONFIG
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ChatScheduleConfig:
    """
    Per-chat scheduler policy.  Stored in schedule.json under each chat_id key.
    """
    chat_id:        int
    enabled:        bool             = True
    mode:           ScheduleMode     = ScheduleMode.ALWAYS
    timezone:       str              = "UTC"
    windows:        list[TimeWindow] = field(default_factory=list)
    allowed_days:   list[DayOfWeek]  = field(default_factory=list)   # for WEEKDAY mode
    calendar:       list[CalendarEntry] = field(default_factory=list)
    max_per_window: int              = 0    # 0 = unlimited within window
    delay_outside:  bool             = True # True = queue until next window; False = drop
    notes:          str              = ""

    def _tz(self) -> ZoneInfo:
        return _tz(self.timezone)

    def local_now(self) -> datetime:
        return datetime.now(self._tz())

    # ── calendar lookup ───────────────────────────────────────────────────────

    def _calendar_entry_for(self, d: Optional[date] = None) -> Optional[CalendarEntry]:
        d = d or self.local_now().date()
        for entry in self.calendar:
            if entry.matches(d):
                return entry
        return None

    def is_suppressed_today(self) -> bool:
        entry = self._calendar_entry_for()
        return entry is not None and entry.suppress

    # ── window resolution ─────────────────────────────────────────────────────

    def effective_windows(self) -> list[TimeWindow]:
        """
        Return the windows that apply right now based on calendar overrides.
        Calendar-entry windows override global windows when present.
        """
        entry = self._calendar_entry_for()
        if entry and not entry.suppress and entry.windows:
            return entry.windows
        return self.windows

    # ── day-of-week gate ──────────────────────────────────────────────────────

    def is_allowed_day(self, dt: Optional[datetime] = None) -> bool:
        if not self.allowed_days:
            return True         # no restriction
        now = (dt or self.local_now())
        return DayOfWeek(now.weekday()) in self.allowed_days

    # ── main gate ─────────────────────────────────────────────────────────────

    def should_react_now(self) -> tuple[bool, str]:
        """
        Returns (allow: bool, reason: str).
        reason is a short diagnostic string for logs.
        """
        if not self.enabled:
            return False, "disabled"

        if self.is_suppressed_today():
            return False, "calendar-suppressed"

        now = self.local_now()

        if self.mode == ScheduleMode.WEEKDAY:
            if not self.is_allowed_day(now):
                return False, f"wrong-day({DayOfWeek(now.weekday()).name})"

        if self.mode in (ScheduleMode.WEEKDAY, ScheduleMode.ALWAYS, ScheduleMode.CALENDAR):
            if self.mode == ScheduleMode.CALENDAR:
                entry = self._calendar_entry_for()
                if entry is None:
                    return False, "not-in-calendar"
                if entry.suppress:
                    return False, "calendar-suppressed"

        windows = self.effective_windows()
        if not windows:
            return True, "no-windows-always-open"

        for w in windows:
            if w.contains(now):
                return True, f"window({w.name})"

        return False, "outside-all-windows"

    def seconds_until_next_window(self) -> float:
        """How many seconds until any window opens (0 if already open)."""
        windows = self.effective_windows()
        if not windows:
            return 0.0
        delays = [w.seconds_until_open() for w in windows]
        return min(delays)

    # ── serialization ─────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            "chat_id":        self.chat_id,
            "enabled":        self.enabled,
            "mode":           self.mode.value,
            "timezone":       self.timezone,
            "windows":        [w.to_dict() for w in self.windows],
            "allowed_days":   [d.name for d in self.allowed_days],
            "calendar":       [c.to_dict() for c in self.calendar],
            "max_per_window": self.max_per_window,
            "delay_outside":  self.delay_outside,
            "notes":          self.notes,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ChatScheduleConfig":
        return cls(
            chat_id        = int(d["chat_id"]),
            enabled        = bool(d.get("enabled", True)),
            mode           = ScheduleMode(d.get("mode", "always")),
            timezone       = d.get("timezone", "UTC"),
            windows        = [TimeWindow.from_dict(w) for w in d.get("windows", [])],
            allowed_days   = [DayOfWeek[n] for n in d.get("allowed_days", [])],
            calendar       = [CalendarEntry.from_dict(c) for c in d.get("calendar", [])],
            max_per_window = int(d.get("max_per_window", 0)),
            delay_outside  = bool(d.get("delay_outside", True)),
            notes          = d.get("notes", ""),
        )


# ══════════════════════════════════════════════════════════════════════════════
#  DEFERRED JOB  (queued when outside window, replayed when window opens)
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class DeferredJob:
    chat_id:    int
    msg_id:     int
    text:       str
    created_at: float = field(default_factory=time.monotonic)
    max_age:    float = 7200.0   # discard if older than this (seconds)

    def is_stale(self) -> bool:
        return (time.monotonic() - self.created_at) > self.max_age


# ══════════════════════════════════════════════════════════════════════════════
#  SCHEDULE REGISTRY
# ══════════════════════════════════════════════════════════════════════════════

class ScheduleRegistry:
    """
    JSON-backed registry of ChatScheduleConfig objects.
    mtime-checked hot-reload, thread-safe.
    """

    def __init__(self, path: str = "schedule.json"):
        self._path:  Path = Path(path)
        self._lock:  threading.Lock = threading.Lock()
        self._data:  dict[int, ChatScheduleConfig] = {}
        self._mtime: float = 0.0
        self.load()

    def load(self) -> None:
        if not self._path.exists():
            return
        try:
            mtime = self._path.stat().st_mtime
            raw   = json.loads(self._path.read_text(encoding="utf-8"))
            with self._lock:
                self._data = {
                    int(k): ChatScheduleConfig.from_dict(v)
                    for k, v in raw.items()
                }
                self._mtime = mtime
        except Exception as e:
            logger.warning("ScheduleRegistry.load: %s", e)

    def reload(self) -> bool:
        if not self._path.exists():
            return False
        try:
            mtime = self._path.stat().st_mtime
            if mtime != self._mtime:
                self.load()
                return True
        except OSError:
            pass
        return False

    def save(self) -> None:
        with self._lock:
            data = {str(k): v.to_dict() for k, v in self._data.items()}
        self._path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self._mtime = self._path.stat().st_mtime

    def get(self, chat_id: int) -> Optional[ChatScheduleConfig]:
        with self._lock:
            return self._data.get(chat_id)

    def set(self, cfg: ChatScheduleConfig) -> None:
        with self._lock:
            self._data[cfg.chat_id] = cfg
        self.save()

    def all_chats(self) -> list[int]:
        with self._lock:
            return list(self._data.keys())


# ══════════════════════════════════════════════════════════════════════════════
#  WINDOW COUNTER  (per-chat per-window-open-period rate tracking)
# ══════════════════════════════════════════════════════════════════════════════

class WindowCounter:
    """
    Tracks how many reactions were sent in the *current* open window period
    for a given chat.  Resets automatically when the window re-opens.
    """

    def __init__(self):
        self._lock:    threading.Lock          = threading.Lock()
        self._counts:  dict[int, int]          = {}
        self._periods: dict[int, str]          = {}  # chat_id → period key

    @staticmethod
    def _period_key(cfg: ChatScheduleConfig) -> str:
        """Unique key for the current open period (hour granularity)."""
        now = cfg.local_now()
        return now.strftime("%Y-%m-%dT%H")

    def increment(self, cfg: ChatScheduleConfig) -> int:
        """Bump counter for chat and return new value."""
        key = self._period_key(cfg)
        with self._lock:
            if self._periods.get(cfg.chat_id) != key:
                self._counts[cfg.chat_id]  = 0
                self._periods[cfg.chat_id] = key
            self._counts[cfg.chat_id] += 1
            return self._counts[cfg.chat_id]

    def current(self, cfg: ChatScheduleConfig) -> int:
        key = self._period_key(cfg)
        with self._lock:
            if self._periods.get(cfg.chat_id) != key:
                return 0
            return self._counts.get(cfg.chat_id, 0)

    def reset(self, chat_id: int) -> None:
        with self._lock:
            self._counts.pop(chat_id, None)
            self._periods.pop(chat_id, None)


# ══════════════════════════════════════════════════════════════════════════════
#  REACTION SCHEDULER
# ══════════════════════════════════════════════════════════════════════════════

class ReactionScheduler:
    """
    Wraps ReactionEngine.enqueue() with:
      - timezone-aware time-window gating
      - calendar suppression / override
      - day-of-week filtering
      - per-window reaction count limit
      - deferred job queue (replay when window opens)
      - hot-reload of schedule.json

    Usage
    -----
    sched = ReactionScheduler(engine, default_tz="Asia/Bangkok")
    sched.load_config("schedule.json")
    asyncio.create_task(sched.run())
    await sched.handle_message(chat_id, msg_id, text)
    """

    def __init__(
        self,
        engine,                             # ReactionEngine
        default_tz:    str   = "UTC",
        deferred_cap:  int   = 500,         # max deferred jobs in memory
        reload_every:  float = 30.0,        # seconds between config reloads
    ):
        self._engine        = engine
        self._default_tz    = default_tz
        self._deferred_cap  = deferred_cap
        self._reload_every  = reload_every

        self._registry: Optional[ScheduleRegistry] = None
        self._counter   = WindowCounter()
        self._deferred:  list[DeferredJob]          = []
        self._defer_lock = asyncio.Lock()
        self._running    = False
        self._stop_ev    = asyncio.Event()

    # ── config ────────────────────────────────────────────────────────────────

    def load_config(self, path: str = "schedule.json") -> None:
        self._registry = ScheduleRegistry(path)
        logger.info("[scheduler] loaded config from %s (%d chats)",
                    path, len(self._registry.all_chats()))

    def set_chat_schedule(self, cfg: ChatScheduleConfig) -> None:
        if self._registry is None:
            self._registry = ScheduleRegistry.__new__(ScheduleRegistry)
            self._registry._path  = Path("schedule.json")
            self._registry._lock  = threading.Lock()
            self._registry._data  = {}
            self._registry._mtime = 0.0
        self._registry.set(cfg)

    # ── public API ────────────────────────────────────────────────────────────

    async def handle_message(
        self,
        chat_id: int,
        msg_id:  int,
        text:    str = "",
    ) -> bool:
        """
        Gate a reaction through the schedule.
        Returns True if enqueued, False if dropped/deferred.
        """
        cfg = self._get_cfg(chat_id)

        allow, reason = cfg.should_react_now()

        if allow:
            # check per-window cap
            if cfg.max_per_window > 0:
                count = self._counter.increment(cfg)
                if count > cfg.max_per_window:
                    logger.debug("[scheduler] chat=%s window cap hit (%d/%d)",
                                 chat_id, count, cfg.max_per_window)
                    return False
            else:
                self._counter.increment(cfg)

            await self._engine.enqueue(chat_id, msg_id)
            logger.debug("[scheduler] chat=%s msg=%s → enqueued (%s)",
                         chat_id, msg_id, reason)
            return True

        else:
            if cfg.delay_outside and reason not in ("disabled", "calendar-suppressed"):
                await self._defer(chat_id, msg_id, text, cfg)
                logger.debug("[scheduler] chat=%s msg=%s → deferred (%s)",
                             chat_id, msg_id, reason)
            else:
                logger.debug("[scheduler] chat=%s msg=%s → dropped (%s)",
                             chat_id, msg_id, reason)
            return False

    def stop(self) -> None:
        self._running = False
        self._stop_ev.set()

    # ── background loop ───────────────────────────────────────────────────────

    async def run(self) -> None:
        """
        Background task:
          - every reload_every seconds: reload schedule.json
          - every 60 seconds: attempt to drain deferred queue
        """
        self._running = True
        self._stop_ev.clear()
        logger.info("[scheduler] background loop started")

        last_reload = time.monotonic()
        last_drain  = time.monotonic()

        while self._running:
            try:
                await asyncio.wait_for(
                    asyncio.shield(self._stop_ev.wait()),
                    timeout=5.0
                )
                break  # stop was signalled
            except asyncio.TimeoutError:
                pass   # normal tick

            now = time.monotonic()

            if self._registry and now - last_reload >= self._reload_every:
                reloaded = self._registry.reload()
                if reloaded:
                    logger.info("[scheduler] config reloaded")
                last_reload = now

            if now - last_drain >= 60.0:
                await self._drain_deferred()
                last_drain = now

        logger.info("[scheduler] stopped")

    # ── deferred queue ────────────────────────────────────────────────────────

    async def _defer(
        self,
        chat_id: int,
        msg_id:  int,
        text:    str,
        cfg:     ChatScheduleConfig,
    ) -> None:
        async with self._defer_lock:
            # enforce cap — drop oldest stale first, then oldest if still full
            self._deferred = [j for j in self._deferred if not j.is_stale()]
            if len(self._deferred) >= self._deferred_cap:
                self._deferred.pop(0)
            self._deferred.append(DeferredJob(
                chat_id = chat_id,
                msg_id  = msg_id,
                text    = text,
            ))

    async def _drain_deferred(self) -> None:
        async with self._defer_lock:
            if not self._deferred:
                return

            remaining = []
            drained   = 0

            for job in self._deferred:
                if job.is_stale():
                    continue

                cfg   = self._get_cfg(job.chat_id)
                allow, reason = cfg.should_react_now()

                if allow:
                    if cfg.max_per_window > 0:
                        count = self._counter.increment(cfg)
                        if count > cfg.max_per_window:
                            remaining.append(job)
                            continue

                    await self._engine.enqueue(job.chat_id, job.msg_id)
                    drained += 1
                    logger.debug("[scheduler] drained deferred chat=%s msg=%s",
                                 job.chat_id, job.msg_id)
                else:
                    remaining.append(job)

            self._deferred = remaining
            if drained:
                logger.info("[scheduler] drained %d deferred jobs (%d remain)",
                            drained, len(self._deferred))

    # ── helpers ───────────────────────────────────────────────────────────────

    def _get_cfg(self, chat_id: int) -> ChatScheduleConfig:
        if self._registry:
            cfg = self._registry.get(chat_id)
            if cfg:
                return cfg
        # default: always-on, global timezone
        return ChatScheduleConfig(
            chat_id  = chat_id,
            timezone = self._default_tz,
        )

    # ── introspection ─────────────────────────────────────────────────────────

    def status(self, chat_id: int) -> dict:
        cfg   = self._get_cfg(chat_id)
        allow, reason = cfg.should_react_now()
        wait  = cfg.seconds_until_next_window() if not allow else 0.0
        return {
            "chat_id":          chat_id,
            "enabled":          cfg.enabled,
            "mode":             cfg.mode.value,
            "timezone":         cfg.timezone,
            "allow_now":        allow,
            "reason":           reason,
            "seconds_to_open":  wait,
            "deferred_count":   len([j for j in self._deferred if j.chat_id == chat_id]),
            "window_count":     self._counter.current(cfg),
            "max_per_window":   cfg.max_per_window,
        }

    def global_status(self) -> dict:
        chats = self._registry.all_chats() if self._registry else []
        return {
            "total_deferred": len(self._deferred),
            "chat_count":     len(chats),
            "chats":          {cid: self.status(cid) for cid in chats},
        }

    def print_status(self, chat_id: Optional[int] = None) -> None:
        sep = "─" * 56
        if chat_id is not None:
            s = self.status(chat_id)
            print(sep)
            print(f"  Scheduler — chat {chat_id}")
            print(sep)
            for k, v in s.items():
                print(f"  {k:<22}: {v}")
            print(sep)
        else:
            gs = self.global_status()
            print(sep)
            print(f"  Scheduler — {gs['chat_count']} chats, "
                  f"{gs['total_deferred']} deferred")
            print(sep)
            for cid, s in gs["chats"].items():
                status_str = "OPEN " if s["allow_now"] else f"WAIT {s['seconds_to_open']:.0f}s"
                print(f"  {cid:<14} {status_str:<14} {s['reason']}")
            print(sep)


# ══════════════════════════════════════════════════════════════════════════════
#  QUICK-CONFIG HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def business_hours_schedule(
    chat_id:  int,
    tz:       str = "Asia/Bangkok",
    start:    str = "09:00",
    end:      str = "18:00",
    days:     Optional[list[str]] = None,
) -> ChatScheduleConfig:
    """
    One-liner: react only during business hours on weekdays.
    days defaults to MON–FRI.
    """
    if days is None:
        days = ["MON", "TUE", "WED", "THU", "FRI"]
    h, m = start.split(":"); s = dtime(int(h), int(m))
    h, m = end.split(":"); e = dtime(int(h), int(m))
    return ChatScheduleConfig(
        chat_id      = chat_id,
        mode         = ScheduleMode.WEEKDAY,
        timezone     = tz,
        windows      = [TimeWindow("business", s, e, tz)],
        allowed_days = [DayOfWeek[d] for d in days],
        delay_outside= True,
    )


def night_owl_schedule(
    chat_id: int,
    tz:      str = "Asia/Bangkok",
    start:   str = "20:00",
    end:     str = "02:00",   # midnight-spanning
) -> ChatScheduleConfig:
    h, m = start.split(":"); s = dtime(int(h), int(m))
    h, m = end.split(":"); e = dtime(int(h), int(m))
    return ChatScheduleConfig(
        chat_id  = chat_id,
        mode     = ScheduleMode.ALWAYS,
        timezone = tz,
        windows  = [TimeWindow("night", s, e, tz)],
        delay_outside = False,  # drop outside window, don't pile up
    )