# engagement/ops.py
# ==============================================================================
#  Extra engagement operations — poll votes, forwards/shares, comments, stories.
# ------------------------------------------------------------------------------
#  Same shape as the suite's core ops: each takes an account list plus a
#  (log_fn, should_stop) pair and returns a Counter, so they plug straight into
#  the campaign OperationRegistry and the GUI.
#
#    • op_vote    — cast a poll vote from each account (by option index or text)
#    • op_forward — forward/share a channel post (to Saved Messages by default)
#    • op_comment — post a comment into a channel's linked discussion group
#    • op_story   — register story views and (optionally) story reactions
# ==============================================================================

from __future__ import annotations

import asyncio
import random
from collections import Counter
from typing import Callable, Optional

from telethon.errors import FloodWaitError, RPCError
from telethon.tl.functions.messages import ForwardMessagesRequest
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.stories import (
    ReadStoriesRequest,
    SendReactionRequest as StorySendReaction,
    GetPeerStoriesRequest,
)
from telethon.tl.types import InputPeerSelf, ReactionEmoji

try:
    from tg_resolve import resolve_channel_entity
except Exception:  # pragma: no cover
    async def resolve_channel_entity(client, channel):
        return await client.get_input_entity(channel)

LogFn = Callable[[str, str], None]
StopFn = Callable[[], bool]
FLOOD_MAX_WAIT_SEC = 300


def _pool(accounts):
    return [a for a in accounts if getattr(a, "client", None) and a.healthy()]


# ------------------------------------------------------------------------------
#  POLL VOTE
# ------------------------------------------------------------------------------

async def op_vote(accounts, channel: str, message: str, *,
                  option_index: int = 0, option_text: str = "",
                  log_fn: LogFn, should_stop: StopFn, dry_run: bool = False,
                  gap=(2.0, 6.0)) -> Counter:
    stats: Counter = Counter()
    pool = _pool(accounts)
    if not pool:
        log_fn("err", "no connected account")
        return stats
    chan = await resolve_channel_entity(pool[0].client, channel)
    mid = int(message) if str(message).isdigit() else None
    if mid is None:
        log_fn("err", "vote needs a numeric message id")
        return stats

    # resolve the poll option bytes from the message
    msg = await pool[0].client.get_messages(chan, ids=[mid])
    msg = msg[0] if isinstance(msg, list) else msg
    poll = getattr(getattr(msg, "media", None), "poll", None)
    if poll is None:
        log_fn("err", f"message {mid} has no poll")
        return stats
    answers = poll.answers
    chosen = None
    if option_text:
        for a in answers:
            txt = getattr(getattr(a, "text", None), "text", "") or ""
            if option_text.lower() in txt.lower():
                chosen = a.option
                break
    if chosen is None:
        idx = max(0, min(option_index, len(answers) - 1))
        chosen = answers[idx].option
    log_fn("accent", f"VOTE option#{option_index} → {channel}/{mid}")

    from telethon.tl.functions.messages import SendVoteRequest
    for acc in pool:
        if should_stop():
            break
        if dry_run:
            stats["dry"] += 1
            continue
        try:
            await acc.client(SendVoteRequest(peer=chan, msg_id=mid, options=[chosen]))
            acc.mark_ok(); stats["voted"] += 1
            log_fn("ok", f"{acc.phone} voted on {mid}")
        except FloodWaitError as e:
            stats["flood"] += 1
            if e.seconds <= FLOOD_MAX_WAIT_SEC and not should_stop():
                await asyncio.sleep(e.seconds)
        except Exception as e:
            acc.mark_fail(); stats["failed"] += 1
            log_fn("err", f"{acc.phone} vote error: {e}")
        if not should_stop():
            await asyncio.sleep(random.uniform(*gap))
    log_fn("accent", f"VOTE done — {dict(stats)}")
    return stats


# ------------------------------------------------------------------------------
#  FORWARD / SHARE
# ------------------------------------------------------------------------------

async def op_forward(accounts, channel: str, message: str, *,
                     to: str = "me", log_fn: LogFn, should_stop: StopFn,
                     dry_run: bool = False, gap=(2.0, 6.0)) -> Counter:
    """Forward a post to inflate its share count. `to`='me' → Saved Messages."""
    stats: Counter = Counter()
    pool = _pool(accounts)
    if not pool:
        log_fn("err", "no connected account")
        return stats
    src = await resolve_channel_entity(pool[0].client, channel)
    ids = [int(x) for x in str(message).split(",") if x.strip().isdigit()]
    if not ids:
        log_fn("err", "forward needs numeric message id(s)")
        return stats
    log_fn("accent", f"FORWARD {ids} from {channel} → {to}")
    for acc in pool:
        if should_stop():
            break
        if dry_run:
            stats["dry"] += 1
            continue
        try:
            dest = InputPeerSelf() if to == "me" else await acc.client.get_input_entity(to)
            await acc.client(ForwardMessagesRequest(
                from_peer=src, id=ids, to_peer=dest,
                random_id=[random.randrange(-(2**63), 2**63) for _ in ids],
                drop_author=False, silent=True))
            acc.mark_ok(); stats["forwarded"] += 1
            log_fn("ok", f"{acc.phone} forwarded {len(ids)} msg(s)")
        except FloodWaitError as e:
            stats["flood"] += 1
            if e.seconds <= FLOOD_MAX_WAIT_SEC and not should_stop():
                await asyncio.sleep(e.seconds)
        except Exception as e:
            acc.mark_fail(); stats["failed"] += 1
            log_fn("err", f"{acc.phone} forward error: {e}")
        if not should_stop():
            await asyncio.sleep(random.uniform(*gap))
    log_fn("accent", f"FORWARD done — {dict(stats)}")
    return stats


# ------------------------------------------------------------------------------
#  COMMENT (channel's linked discussion group)
# ------------------------------------------------------------------------------

async def op_comment(accounts, channel: str, message: str, texts, *,
                     log_fn: LogFn, should_stop: StopFn, dry_run: bool = False,
                     gap=(5.0, 15.0)) -> Counter:
    """
    Post a comment on a channel post via its linked discussion group.
    `texts` is a str or a list of strings (one is chosen per account).
    """
    stats: Counter = Counter()
    pool = _pool(accounts)
    if not pool:
        log_fn("err", "no connected account")
        return stats
    if isinstance(texts, str):
        texts = [texts]
    mid = int(message) if str(message).isdigit() else None
    if mid is None:
        log_fn("err", "comment needs a numeric message id")
        return stats
    chan = await pool[0].client.get_entity(channel)

    # confirm the channel actually has a linked discussion group
    try:
        full = await pool[0].client(GetFullChannelRequest(chan))
        if not getattr(full.full_chat, "linked_chat_id", None):
            log_fn("err", f"{channel} has no linked discussion group")
            return stats
    except Exception as e:
        log_fn("warn", f"could not verify discussion group: {e}")

    log_fn("accent", f"COMMENT → {channel}/{mid} ({len(pool)} accts)")
    for acc in pool:
        if should_stop():
            break
        text = random.choice(texts)
        if dry_run:
            stats["dry"] += 1
            continue
        try:
            # high-level comment: posts into the linked discussion, threaded
            await acc.client.send_message(chan, text, comment_to=mid)
            acc.mark_ok(); stats["commented"] += 1
            log_fn("ok", f"{acc.phone} commented on {mid}")
        except FloodWaitError as e:
            stats["flood"] += 1
            if e.seconds <= FLOOD_MAX_WAIT_SEC and not should_stop():
                await asyncio.sleep(e.seconds)
        except Exception as e:
            acc.mark_fail(); stats["failed"] += 1
            log_fn("err", f"{acc.phone} comment error: {e}")
        if not should_stop():
            await asyncio.sleep(random.uniform(*gap))
    log_fn("accent", f"COMMENT done — {dict(stats)}")
    return stats


# ------------------------------------------------------------------------------
#  STORY views + reactions
# ------------------------------------------------------------------------------

async def op_story(accounts, channel: str, *,
                   react_emoji: str = "", log_fn: LogFn, should_stop: StopFn,
                   dry_run: bool = False, gap=(1.0, 4.0)) -> Counter:
    """
    Register views on all active stories of `channel`; optionally react.
    """
    stats: Counter = Counter()
    pool = _pool(accounts)
    if not pool:
        log_fn("err", "no connected account")
        return stats
    peer = await resolve_channel_entity(pool[0].client, channel)
    try:
        res = await pool[0].client(GetPeerStoriesRequest(peer=peer))
        stories = list(getattr(res.stories, "stories", []))
    except Exception as e:
        log_fn("err", f"could not read stories: {e}")
        return stats
    if not stories:
        log_fn("warn", f"{channel} has no active stories")
        return stats
    story_ids = [s.id for s in stories]
    max_id = max(story_ids)
    log_fn("accent", f"STORY views → {channel} stories {story_ids}"
                     + (f" react {react_emoji}" if react_emoji else ""))

    for acc in pool:
        if should_stop():
            break
        if dry_run:
            stats["dry"] += 1
            continue
        try:
            peer_a = await resolve_channel_entity(acc.client, channel)
            await acc.client(ReadStoriesRequest(peer=peer_a, max_id=max_id))
            acc.mark_ok(); stats["story_viewed"] += 1
            if react_emoji:
                for sid in story_ids:
                    try:
                        await acc.client(StorySendReaction(
                            peer=peer_a, story_id=sid,
                            reaction=ReactionEmoji(emoticon=react_emoji)))
                        stats["story_reacted"] += 1
                    except Exception:
                        pass
            log_fn("ok", f"{acc.phone} viewed {len(story_ids)} story(ies)")
        except FloodWaitError as e:
            stats["flood"] += 1
            if e.seconds <= FLOOD_MAX_WAIT_SEC and not should_stop():
                await asyncio.sleep(e.seconds)
        except Exception as e:
            acc.mark_fail(); stats["failed"] += 1
            log_fn("err", f"{acc.phone} story error: {e}")
        if not should_stop():
            await asyncio.sleep(random.uniform(*gap))
    log_fn("accent", f"STORY done — {dict(stats)}")
    return stats
