# orchestration/workers.py
# ==============================================================================
#  Async work queue for fan-out across large account pools.
# ------------------------------------------------------------------------------
#  A bounded-concurrency worker pool backed by a priority queue, with retry +
#  exponential backoff and a result/metrics callback. This is the single-node
#  foundation for "distributed workers": the queue sits behind the JobBackend
#  interface, so the in-memory backend used here can be swapped for a Redis /
#  Celery / NATS adapter (see RedisBackend stub) without touching callers.
# ==============================================================================

from __future__ import annotations

import asyncio
import itertools
import random
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

# A job body is a zero-arg coroutine factory, so it can be retried cleanly.
CoroFactory = Callable[[], Awaitable[Any]]
ResultCB = Callable[["JobResult"], None]


@dataclass(order=True)
class Job:
    priority: int
    seq: int = field(compare=True)
    factory: CoroFactory = field(compare=False)
    name: str = field(default="", compare=False)
    max_retries: int = field(default=0, compare=False)
    backoff_base: float = field(default=2.0, compare=False)
    attempt: int = field(default=0, compare=False)


@dataclass
class JobResult:
    name: str
    ok: bool
    value: Any = None
    error: str = ""
    attempts: int = 1
    elapsed: float = 0.0


class JobBackend:
    """Queue abstraction. Swap the implementation to go multi-node."""

    async def put(self, job: Job) -> None: ...
    async def get(self) -> Job: ...
    def qsize(self) -> int: ...
    async def join(self) -> None: ...
    def task_done(self) -> None: ...


class InMemoryBackend(JobBackend):
    def __init__(self):
        self._q: "asyncio.PriorityQueue[Job]" = asyncio.PriorityQueue()

    async def put(self, job: Job) -> None:
        await self._q.put(job)

    async def get(self) -> Job:
        return await self._q.get()

    def qsize(self) -> int:
        return self._q.qsize()

    async def join(self) -> None:
        await self._q.join()

    def task_done(self) -> None:
        self._q.task_done()


class WorkerPool:
    """
    Run up to `concurrency` jobs at once off a shared priority queue.

    Usage:
        pool = WorkerPool(concurrency=20, on_result=cb)
        await pool.start()
        for acc in accounts:
            await pool.submit(lambda a=acc: do_thing(a), priority=5, name=a.phone,
                              max_retries=2)
        await pool.join()     # wait for the queue to drain
        await pool.stop()
    """

    def __init__(self, concurrency: int = 10, on_result: Optional[ResultCB] = None,
                 backend: Optional[JobBackend] = None):
        self.concurrency = concurrency
        self.on_result = on_result
        self.backend = backend or InMemoryBackend()
        self._seq = itertools.count()
        self._workers: "list[asyncio.Task]" = []
        self._stop = asyncio.Event()
        self._results: "list[JobResult]" = []
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        self._stop.clear()
        self._workers = [asyncio.create_task(self._worker(i))
                         for i in range(self.concurrency)]

    async def submit(self, factory: CoroFactory, *, priority: int = 5,
                     name: str = "", max_retries: int = 0) -> None:
        await self.backend.put(Job(priority=priority, seq=next(self._seq),
                                   factory=factory, name=name,
                                   max_retries=max_retries))

    async def _worker(self, wid: int) -> None:
        while not self._stop.is_set():
            try:
                job = await asyncio.wait_for(self.backend.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            t0 = time.monotonic()
            try:
                job.attempt += 1
                value = await job.factory()
                res = JobResult(name=job.name, ok=True, value=value,
                                attempts=job.attempt, elapsed=time.monotonic() - t0)
                await self._emit(res)
            except asyncio.CancelledError:
                self.backend.task_done()
                break
            except Exception as exc:
                if job.attempt <= job.max_retries:
                    delay = job.backoff_base ** job.attempt + random.uniform(0, 0.5)
                    await asyncio.sleep(delay)
                    await self.backend.put(job)         # requeue for retry
                else:
                    res = JobResult(name=job.name, ok=False, error=str(exc),
                                    attempts=job.attempt,
                                    elapsed=time.monotonic() - t0)
                    await self._emit(res)
            finally:
                self.backend.task_done()

    async def _emit(self, res: JobResult) -> None:
        async with self._lock:
            self._results.append(res)
        if self.on_result:
            try:
                self.on_result(res)
            except Exception:
                pass

    async def join(self) -> None:
        await self.backend.join()

    async def stop(self) -> None:
        self._stop.set()
        for w in self._workers:
            w.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()

    @property
    def results(self) -> "list[JobResult]":
        return list(self._results)


# ---- extension point for multi-node scale (documented stub) ------------------

class RedisBackend(JobBackend):
    """
    Placeholder for a Redis-backed queue (e.g. via `redis.asyncio`), enabling
    multiple machines to share one job stream. Implement put/get/join against a
    Redis list or stream; the WorkerPool above works unchanged. Left unimported
    so the suite has no hard Redis dependency.
    """
    def __init__(self, *_, **__):
        raise NotImplementedError(
            "RedisBackend is an extension point — implement against redis.asyncio "
            "to distribute the queue across nodes.")
