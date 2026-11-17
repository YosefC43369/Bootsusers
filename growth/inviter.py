# growth/inviter.py
# ==============================================================================
#  Add scraped members into a target channel/group you control.
# ------------------------------------------------------------------------------
#  The highest ban-risk operation in the suite, so it is the most defensive:
#    • resolves targets by @username first (access_hash is per-account, so a
#      scraped hash from another account is unreliable); id is a best-effort
#      fallback;
#    • invites in small batches per account with human pacing between them;
#    • classifies privacy / "too many channels" / already-member as expected
#      skips, and treats PeerFlood / restriction as a spam signal that
#      quarantines the account through the health layer.
# ==============================================================================

from __future__ import annotations

import random
from collections import Counter
from typing import Callable, Optional

from telethon.tl.functions.channels import InviteToChannelRequest
from telethon.errors import (
    FloodWaitError,
    PeerFloodError,
    UserPrivacyRestrictedError,
    UserChannelsTooMuchError,
    UserNotMutualContactError,
    UserAlreadyParticipantError,
    UserRestrictedError,
    ChatAdminRequiredError,
)

LogFn = Callable[[str, str], None]
StopFn = Callable[[], bool]
FLOOD_MAX_WAIT_SEC = 300


def _targets_from_members(members, prefer_username=True) -> "list[object]":
    """Return a resolution key per member: @username if present, else numeric id."""
    keys = []
    for m in members:
        uname = m.get("username")
        if uname:
            keys.append(uname)
        elif not prefer_username and m.get("id"):
            keys.append(m["id"])
    return keys


async def op_invite(accounts, target: str, members, *,
                    per_account: int = 20, batch: int = 3,
                    allow_id_fallback: bool = False,
                    log_fn: LogFn, should_stop: StopFn,
                    store=None, dry_run: bool = False,
                    gap=(20.0, 50.0)) -> Counter:
    """
    Invite `members` (list of scraped dicts) into `target`.
    Each account invites at most `per_account` users, `batch` at a time.
    """
    stats: Counter = Counter()
    pool = [a for a in accounts if getattr(a, "client", None) and a.healthy()]
    if not pool:
        log_fn("err", "no connected account")
        return stats

    keys = _targets_from_members(members, prefer_username=not allow_id_fallback)
    if not keys:
        log_fn("err", "no invitable targets (need @usernames; enable id-fallback "
                      "to try numeric ids)")
        return stats

    # health layer for spam quarantine
    hm = None
    if store is not None:
        try:
            from lifecycle.health import HealthMonitor
            hm = HealthMonitor(store, log=log_fn)
        except Exception:
            hm = None

    try:
        target_ent = await pool[0].client.get_entity(target)
    except Exception as exc:
        log_fn("err", f"cannot resolve target {target}: {exc}")
        return stats

    log_fn("accent", f"INVITE → {target}: {len(keys)} candidate(s) across "
                     f"{len(pool)} account(s)")

    idx = 0
    for acc in pool:
        if should_stop() or idx >= len(keys):
            break
        assigned = keys[idx: idx + per_account]
        idx += per_account
        sent = 0
        for i in range(0, len(assigned), batch):
            if should_stop():
                break
            chunk = assigned[i: i + batch]
            # resolve each key to an input entity for THIS account
            input_users = []
            for key in chunk:
                try:
                    input_users.append(await acc.client.get_input_entity(key))
                except Exception:
                    stats["unresolved"] += 1
            if not input_users:
                continue
            if dry_run:
                stats["dry"] += len(input_users)
                continue
            try:
                await acc.client(InviteToChannelRequest(channel=target_ent,
                                                        users=input_users))
                acc.mark_ok()
                sent += len(input_users)
                stats["invited"] += len(input_users)
                log_fn("ok", f"{acc.phone} invited {len(input_users)} user(s)")
            except (UserPrivacyRestrictedError, UserNotMutualContactError):
                stats["privacy"] += len(input_users)
            except UserChannelsTooMuchError:
                stats["too_many_channels"] += len(input_users)
            except UserAlreadyParticipantError:
                stats["already"] += len(input_users)
            except (PeerFloodError, UserRestrictedError) as e:
                stats["spam_block"] += 1
                log_fn("warn", f"{acc.phone} spam-blocked — quarantining")
                if hm:
                    hm.on_error(acc.phone, e)
                break  # stop using this account
            except ChatAdminRequiredError:
                stats["no_permission"] += 1
                log_fn("err", f"{acc.phone} lacks invite permission on {target}")
                break
            except FloodWaitError as e:
                stats["flood"] += 1
                log_fn("warn", f"{acc.phone} floodwait {e.seconds}s")
                if hm:
                    hm.on_error(acc.phone, e)
                if e.seconds <= FLOOD_MAX_WAIT_SEC and not should_stop():
                    import asyncio
                    await asyncio.sleep(e.seconds)
                else:
                    break
            except Exception as e:
                acc.mark_fail()
                stats["failed"] += 1
                log_fn("err", f"{acc.phone} invite error: {e}")
            if not should_stop():
                import asyncio
                await asyncio.sleep(random.uniform(*gap))
        if sent and store:
            store.set_cooldown(acc.phone, "invite", 3600)
    log_fn("accent", f"INVITE done — {dict(stats)}")
    return stats
