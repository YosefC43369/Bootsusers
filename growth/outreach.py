# growth/outreach.py
# ==============================================================================
#  Mass DM / outreach — templated direct messages across the account pool.
# ------------------------------------------------------------------------------
#  Sends personalised DMs to a target list, spreading recipients across accounts
#  with human pacing (typing indicator sized to the message) and spintax/field
#  templating. PeerFlood / restriction quarantines the sending account through
#  the health layer. This is a ban-magnet: default throughput is deliberately
#  low and per-account capped.
#
#  Template fields: {first_name} {last_name} {username} {id}
#  Spintax: "{Hi|Hello|Hey} {first_name}" picks one option at random.
# ==============================================================================

from __future__ import annotations

import asyncio
import random
import re
from collections import Counter
from typing import Callable, Optional

from telethon.errors import (
    FloodWaitError,
    PeerFloodError,
    UserPrivacyRestrictedError,
    UserIsBlockedError,
    UserRestrictedError,
    InputUserDeactivatedError,
)

LogFn = Callable[[str, str], None]
StopFn = Callable[[], bool]
FLOOD_MAX_WAIT_SEC = 300

_SPIN_RE = re.compile(r"\{([^{}]*\|[^{}]*)\}")


def spin(text: str) -> str:
    """Resolve {a|b|c} spintax groups (non-nested)."""
    def _pick(m):
        return random.choice(m.group(1).split("|"))
    # resolve repeatedly in case of multiple groups on one pass
    prev = None
    while prev != text:
        prev = text
        text = _SPIN_RE.sub(_pick, text)
    return text


def render(template: str, member: dict) -> str:
    body = spin(template)
    return body.format(
        first_name=member.get("first_name") or "",
        last_name=member.get("last_name") or "",
        username=member.get("username") or "",
        id=member.get("id") or "",
    ).strip()


async def op_dm(accounts, members, templates, *,
                per_account: int = 15, simulate_typing: bool = True,
                log_fn: LogFn, should_stop: StopFn, store=None,
                dry_run: bool = False, gap=(25.0, 60.0)) -> Counter:
    """
    DM each member in `members` with a rendered `templates` message
    (str or list). Recipients are split across the pool, capped per account.
    """
    stats: Counter = Counter()
    pool = [a for a in accounts if getattr(a, "client", None) and a.healthy()]
    if not pool:
        log_fn("err", "no connected account")
        return stats
    if isinstance(templates, str):
        templates = [templates]

    hm = None
    if store is not None:
        try:
            from lifecycle.health import HealthMonitor
            hm = HealthMonitor(store, log=log_fn)
        except Exception:
            hm = None

    try:
        from lifecycle.pacing import simulate_typing as _typing
    except Exception:
        _typing = None

    # target key per member (username preferred, else id)
    targets = [m for m in members if m.get("username") or m.get("id")]
    log_fn("accent", f"DM → {len(targets)} recipient(s) across {len(pool)} account(s)")

    idx = 0
    for acc in pool:
        if should_stop() or idx >= len(targets):
            break
        assigned = targets[idx: idx + per_account]
        idx += per_account
        sent = 0
        for member in assigned:
            if should_stop():
                break
            key = member.get("username") or member.get("id")
            text = render(random.choice(templates), member)
            if dry_run:
                stats["dry"] += 1
                continue
            try:
                entity = await acc.client.get_entity(key)
                if simulate_typing and _typing:
                    await _typing(acc.client, entity, text)
                await acc.client.send_message(entity, text)
                acc.mark_ok()
                sent += 1
                stats["sent"] += 1
                log_fn("ok", f"{acc.phone} → {key}")
            except (UserPrivacyRestrictedError,):
                stats["privacy"] += 1
            except (UserIsBlockedError,):
                stats["blocked"] += 1
            except (InputUserDeactivatedError,):
                stats["dead_target"] += 1
            except (PeerFloodError, UserRestrictedError) as e:
                stats["spam_block"] += 1
                log_fn("warn", f"{acc.phone} spam-blocked — quarantining")
                if hm:
                    hm.on_error(acc.phone, e)
                break
            except FloodWaitError as e:
                stats["flood"] += 1
                log_fn("warn", f"{acc.phone} floodwait {e.seconds}s")
                if hm:
                    hm.on_error(acc.phone, e)
                if e.seconds <= FLOOD_MAX_WAIT_SEC and not should_stop():
                    await asyncio.sleep(e.seconds)
                else:
                    break
            except Exception as e:
                acc.mark_fail()
                stats["failed"] += 1
                log_fn("err", f"{acc.phone} dm error: {e}")
            if not should_stop():
                await asyncio.sleep(random.uniform(*gap))
        if sent and store:
            store.set_cooldown(acc.phone, "dm", 3600)
    log_fn("accent", f"DM done — {dict(stats)}")
    return stats
