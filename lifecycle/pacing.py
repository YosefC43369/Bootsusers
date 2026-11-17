# lifecycle/pacing.py
# ==============================================================================
#  Human-behaviour pacing — make the pool act like people, not a cron job.
# ------------------------------------------------------------------------------
#  Three tools:
#    • human_delay()      — lognormal (not uniform) inter-action delays, so the
#      timing histogram looks organic.
#    • simulate_typing()  — sends the "typing…" action and waits a duration
#      proportional to the message length before the actual send.
#    • mark_read()        — reads history (sends a read receipt) before acting,
#      the way a real user opens a chat before reacting.
#  Plus a PacingGovernor that spaces the whole pool's actions over a window so
#  N accounts don't all fire in the same second.
# ==============================================================================

from __future__ import annotations

import asyncio
import math
import random
import time
from dataclasses import dataclass
from typing import Optional

from telethon.tl.functions.messages import SetTypingRequest, ReadHistoryRequest
from telethon.tl.functions.channels import ReadHistoryRequest as ChannelReadHistory
from telethon.tl.types import (
    SendMessageTypingAction,
    SendMessageCancelAction,
)


@dataclass
class PacingProfile:
    """Per-action timing. median_sec is the typical gap; spread shapes the tail."""
    median_sec: float = 4.0
    spread: float = 0.5          # lognormal sigma (0.4-0.8 looks human)
    min_sec: float = 0.8
    max_sec: float = 60.0

    def sample(self) -> float:
        # lognormal around the median: exp(ln(median) + sigma*N(0,1))
        val = math.exp(math.log(self.median_sec) + self.spread * random.gauss(0, 1))
        return max(self.min_sec, min(self.max_sec, val))


# sensible defaults per action class
PROFILES = {
    "react":  PacingProfile(median_sec=3.5, spread=0.6),
    "view":   PacingProfile(median_sec=1.5, spread=0.7, min_sec=0.4),
    "star":   PacingProfile(median_sec=5.0, spread=0.6),
    "join":   PacingProfile(median_sec=6.0, spread=0.5, min_sec=2.0),
    "message": PacingProfile(median_sec=8.0, spread=0.7, min_sec=2.0),
    "vote":   PacingProfile(median_sec=4.0, spread=0.6),
}


async def human_delay(action: str = "react") -> float:
    prof = PROFILES.get(action, PacingProfile())
    d = prof.sample()
    await asyncio.sleep(d)
    return d


def typing_seconds_for(text: str) -> float:
    """Approximate how long a human takes to type `text` (~4 chars/sec + noise)."""
    base = len(text) / 4.0
    return max(0.6, min(12.0, base * random.uniform(0.7, 1.3)))


async def simulate_typing(client, peer, text: str) -> None:
    """Show 'typing…' for a realistic duration, then cancel."""
    try:
        await client(SetTypingRequest(peer=peer, action=SendMessageTypingAction()))
        await asyncio.sleep(typing_seconds_for(text))
        await client(SetTypingRequest(peer=peer, action=SendMessageCancelAction()))
    except Exception:
        pass  # typing indicators are best-effort cosmetics


async def mark_read(client, peer, max_id: int = 0, *, is_channel: bool = False) -> None:
    """Send a read receipt for `peer` up to max_id (0 = latest)."""
    try:
        if is_channel:
            await client(ChannelReadHistory(channel=peer, max_id=max_id))
        else:
            await client(ReadHistoryRequest(peer=peer, max_id=max_id))
    except Exception:
        pass


class PacingGovernor:
    """
    Spreads the pool's actions across a target window so they don't synchronise.
    Hand it the number of accounts and a window; it hands back staggered offsets.
    """

    def __init__(self, window_sec: float = 60.0, jitter: float = 0.25):
        self.window_sec = window_sec
        self.jitter = jitter

    def offsets(self, n: int) -> "list[float]":
        if n <= 0:
            return []
        step = self.window_sec / n
        out = []
        for i in range(n):
            base = i * step
            out.append(max(0.0, base + random.uniform(-step * self.jitter,
                                                       step * self.jitter)))
        random.shuffle(out)
        return out

    async def pace(self, index: int, total: int) -> None:
        """Sleep this account's share of the window before it acts."""
        if total <= 1:
            return
        step = self.window_sec / total
        await asyncio.sleep(max(0.0, index * step + random.uniform(0, step * self.jitter)))
