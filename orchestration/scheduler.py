# orchestration/scheduler.py
# ==============================================================================
#  Lightweight async scheduler — turns one-shot ops into recurring campaigns.
# ------------------------------------------------------------------------------
#  Supports interval jobs, daily-at-HH:MM jobs, and optional gating by an
#  active time window (so a job only fires during, say, 09:00–23:00). No
#  third-party deps; one asyncio task drives all jobs. For richer calendar
#  rules the repo's reaction_scheduler.TimeWindow can be passed as `window`.
# ==============================================================================

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Awaitable, Callable, Optional

JobCoro = Callable[[], Awaitable]


def _seconds_until_daily(hour: int, minute: int, now: Optional[datetime] = None) -> float:
    now = now or datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


@dataclass
class ScheduledJob:
    name: str
    coro_factory: JobCoro
    interval_sec: Optional[float] = None       # recurring every N seconds
    daily_at: Optional["tuple[int, int]"] = None  # (hour, minute)
    jitter_sec: float = 0.0
    max_runs: Optional[int] = None
    window: object = None                       # optional .contains()/.seconds_until_open()
    enabled: bool = True

    runs: int = field(default=0, init=False)
    last_run: float = field(default=0.0, init=False)
    next_run: float = field(default=0.0, init=False)

    def compute_next(self, now: float) -> float:
        if self.interval_sec is not None:
            base = now + self.interval_sec
        elif self.daily_at is not None:
            base = now + _seconds_until_daily(*self.daily_at)
        else:
            base = now  # run-once immediately
        if self.jitter_sec:
            base += random.uniform(0, self.jitter_sec)
        self.next_run = base
        return base

    def due(self, now: float) -> bool:
        if not self.enabled:
            return False
        if self.max_runs is not None and self.runs >= self.max_runs:
            return False
        return now >= self.next_run


class AsyncScheduler:
    """
    Drives a set of ScheduledJob objects from a single loop.

        sched = AsyncScheduler()
        sched.every(300, my_coro, name="boost", jitter_sec=30)
        sched.daily(21, 30, nightly_coro, name="nightly")
        await sched.start()
        ...
        await sched.stop()
    """

    def __init__(self, tick_sec: float = 1.0, log: Callable[[str, str], None] = None):
        self.tick_sec = tick_sec
        self.jobs: "dict[str, ScheduledJob]" = {}
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._log = log or (lambda lvl, msg: None)

    # -- registration -------------------------------------------------------
    def add(self, job: ScheduledJob) -> ScheduledJob:
        job.compute_next(time.time())
        self.jobs[job.name] = job
        return job

    def every(self, seconds: float, coro_factory: JobCoro, *, name: str,
              jitter_sec: float = 0.0, max_runs: Optional[int] = None,
              window: object = None) -> ScheduledJob:
        return self.add(ScheduledJob(name=name, coro_factory=coro_factory,
                                     interval_sec=seconds, jitter_sec=jitter_sec,
                                     max_runs=max_runs, window=window))

    def daily(self, hour: int, minute: int, coro_factory: JobCoro, *, name: str,
              jitter_sec: float = 0.0, window: object = None) -> ScheduledJob:
        return self.add(ScheduledJob(name=name, coro_factory=coro_factory,
                                     daily_at=(hour, minute), jitter_sec=jitter_sec,
                                     window=window))

    def enable(self, name: str, on: bool = True) -> None:
        if name in self.jobs:
            self.jobs[name].enabled = on

    def remove(self, name: str) -> None:
        self.jobs.pop(name, None)

    # -- run loop -----------------------------------------------------------
    async def start(self) -> None:
        self._stop.clear()
        self._task = asyncio.create_task(self._loop())

    async def _loop(self) -> None:
        while not self._stop.is_set():
            now = time.time()
            for job in list(self.jobs.values()):
                if not job.due(now):
                    continue
                # window gate
                if job.window is not None:
                    try:
                        if not job.window.contains():
                            job.next_run = now + job.window.seconds_until_open()
                            continue
                    except Exception:
                        pass
                job.runs += 1
                job.last_run = now
                job.compute_next(now)
                self._log("accent", f"scheduler → firing '{job.name}' (run {job.runs})")
                asyncio.create_task(self._safe_run(job))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.tick_sec)
            except asyncio.TimeoutError:
                pass

    async def _safe_run(self, job: ScheduledJob) -> None:
        try:
            await job.coro_factory()
        except Exception as exc:
            self._log("err", f"scheduled job '{job.name}' failed: {exc}")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    def status(self) -> "list[dict]":
        now = time.time()
        return [{
            "name": j.name, "enabled": j.enabled, "runs": j.runs,
            "in_sec": round(max(0.0, j.next_run - now), 1),
            "kind": "interval" if j.interval_sec else ("daily" if j.daily_at else "once"),
        } for j in self.jobs.values()]
