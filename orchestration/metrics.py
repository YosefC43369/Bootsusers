# orchestration/metrics.py
# ==============================================================================
#  Telemetry for the Bootsusers suite.
# ------------------------------------------------------------------------------
#  Two layers:
#    • MetricsRegistry — live in-memory counters + rolling time-series buckets,
#      cheap to update from the hot path (every op result).
#    • Aggregations over StateStore.action_log for historical reporting
#      (success rate, ban rate, per-op throughput, per-account leaderboards).
#
#  Pure stdlib. The registry is thread-safe so the async worker and the GUI
#  poll thread can share it.
# ==============================================================================

from __future__ import annotations

import threading
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from typing import Optional

# Status tokens the ops emit; grouped into "good" vs "ban-ish" for rate calc.
GOOD = {"joined", "already", "reacted", "sent", "viewed", "voted",
        "forwarded", "commented", "story_viewed", "dry"}
BAN  = {"banned", "blocked", "deactivated"}
SOFT = {"flood", "no_stars", "expired", "rate_limited", "skipped"}


@dataclass
class Bucket:
    """One time-slice of counters."""
    start: float
    counts: Counter = field(default_factory=Counter)

    def add(self, key: str, n: int = 1) -> None:
        self.counts[key] += n


class MetricsRegistry:
    """
    Live counters + a ring of fixed-width time buckets for rate graphs.

    bucket_sec   width of each time slice (default 60s)
    history      how many buckets to keep (default 120 → 2h at 60s)
    """

    def __init__(self, bucket_sec: float = 60.0, history: int = 120):
        self.bucket_sec = bucket_sec
        self.history = history
        self._lock = threading.Lock()
        self._totals: Counter = Counter()
        self._by_op: "defaultdict[str, Counter]" = defaultdict(Counter)
        self._by_phone: "defaultdict[str, Counter]" = defaultdict(Counter)
        self._buckets: "deque[Bucket]" = deque(maxlen=history)
        self._started = time.time()

    # -- recording ----------------------------------------------------------
    def record(self, op: str, status: str, phone: str = "", stars: int = 0,
               count: int = 1) -> None:
        now = time.time()
        with self._lock:
            self._totals[status] += count
            self._totals[f"op:{op}"] += count
            if stars:
                self._totals["stars"] += stars
            self._by_op[op][status] += count
            if phone:
                self._by_phone[phone][status] += count
                self._by_phone[phone]["_total"] += count
            self._current_bucket(now).add(status, count)
            self._current_bucket(now).add(f"{op}:{status}", count)

    def _current_bucket(self, now: float) -> Bucket:
        slot = now - (now % self.bucket_sec)
        if not self._buckets or self._buckets[-1].start != slot:
            self._buckets.append(Bucket(start=slot))
        return self._buckets[-1]

    # -- live reads ---------------------------------------------------------
    def snapshot(self) -> dict:
        with self._lock:
            good = sum(self._totals[s] for s in GOOD)
            ban = sum(self._totals[s] for s in BAN)
            soft = sum(self._totals[s] for s in SOFT)
            total = good + ban + soft + self._totals["failed"]
            return {
                "uptime_sec": round(time.time() - self._started, 1),
                "actions_total": total,
                "good": good,
                "ban": ban,
                "soft": soft,
                "failed": self._totals["failed"],
                "stars_sent": self._totals["stars"],
                "success_rate": round(good / total, 4) if total else 0.0,
                "ban_rate": round(ban / total, 4) if total else 0.0,
                "by_op": {op: dict(c) for op, c in self._by_op.items()},
            }

    def rate_series(self, status: Optional[str] = None) -> "list[tuple[float, int]]":
        """Per-bucket counts for a sparkline (status=None → all actions)."""
        with self._lock:
            out = []
            for b in self._buckets:
                if status is None:
                    n = sum(v for k, v in b.counts.items() if ":" not in k)
                else:
                    n = b.counts.get(status, 0)
                out.append((b.start, n))
            return out

    def top_accounts(self, n: int = 10) -> "list[tuple[str, int]]":
        with self._lock:
            ranked = sorted(self._by_phone.items(),
                            key=lambda kv: kv[1].get("_total", 0), reverse=True)
            return [(p, c.get("_total", 0)) for p, c in ranked[:n]]

    def reset(self) -> None:
        with self._lock:
            self._totals.clear()
            self._by_op.clear()
            self._by_phone.clear()
            self._buckets.clear()
            self._started = time.time()


# -- historical aggregations over StateStore -----------------------------------

def report_from_store(store, window_sec: Optional[float] = None) -> dict:
    """
    Build a historical report from the persisted action_log.
    window_sec limits to the last N seconds (None = all time).
    """
    since = (time.time() - window_sec) if window_sec else None
    ops = ("join", "react", "stars", "views", "vote", "forward", "comment", "story")
    out = {"window_sec": window_sec, "per_op": {}}
    grand_total = grand_good = grand_ban = 0
    for op in ops:
        total = store.count_actions(op=op, since=since)
        if total == 0:
            continue
        good = sum(store.count_actions(op=op, status=s, since=since) for s in GOOD)
        ban = sum(store.count_actions(op=op, status=s, since=since) for s in BAN)
        out["per_op"][op] = {
            "total": total, "good": good, "ban": ban,
            "success_rate": round(good / total, 4) if total else 0.0,
        }
        grand_total += total
        grand_good += good
        grand_ban += ban
    out["total"] = grand_total
    out["success_rate"] = round(grand_good / grand_total, 4) if grand_total else 0.0
    out["ban_rate"] = round(grand_ban / grand_total, 4) if grand_total else 0.0
    return out
