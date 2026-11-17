# growth/drip.py
# ==============================================================================
#  Drip-join — join a channel in small waves instead of one burst.
# ------------------------------------------------------------------------------
#  A hundred accounts joining in the same minute is an obvious signal. Drip
#  spreads the pool into waves of `wave_size` with a randomized gap between
#  waves, so membership climbs at a human cadence. Delegates the actual join to
#  the suite's op_join so error handling / channel resolution stay in one place.
# ==============================================================================

from __future__ import annotations

import asyncio
import random
from collections import Counter
from typing import Callable

LogFn = Callable[[str, str], None]
StopFn = Callable[[], bool]


async def op_dripjoin(accounts, channel: str, *,
                      wave_size: int = 3, wave_gap=(30.0, 90.0),
                      log_fn: LogFn, should_stop: StopFn,
                      dry_run: bool = False) -> Counter:
    """Join `channel` in waves of `wave_size`, pausing wave_gap between waves."""
    from bootsusers_suite import op_join

    pool = [a for a in accounts if getattr(a, "client", None) and a.healthy()]
    total: Counter = Counter()
    waves = [pool[i:i + wave_size] for i in range(0, len(pool), wave_size)]
    log_fn("accent", f"DRIP-JOIN {channel}: {len(pool)} accounts in "
                     f"{len(waves)} wave(s) of {wave_size}")
    for w, wave in enumerate(waves, 1):
        if should_stop():
            break
        log_fn("accent", f"wave {w}/{len(waves)} — {len(wave)} account(s)")
        res = await op_join(wave, channel, limit=0, log_fn=log_fn,
                            should_stop=should_stop, dry_run=dry_run, gap=(1.0, 3.0))
        total.update(res)
        if w < len(waves) and not should_stop():
            delay = random.uniform(*wave_gap)
            log_fn("info", f"waiting {delay:.0f}s before next wave ...")
            await asyncio.sleep(delay)
    log_fn("accent", f"DRIP-JOIN done — {dict(total)}")
    return total
