# tg_resolve.py
# ==============================================================================
#  Shared channel resolver — accepts @username, public/private links, ids, and
#  private invite links (t.me/+HASH or t.me/joinchat/HASH).
# ------------------------------------------------------------------------------
#  Telethon's get_input_entity() handles usernames, public t.me links and
#  numeric ids, but it cannot turn a private *invite hash* into an entity. This
#  helper bridges that: for an invite link it calls CheckChatInvite and, when
#  the account is already a member (ChatInviteAlready / ChatInvitePeek), returns
#  the real channel entity — so react / views / stars / vote / story / etc. can
#  target a private channel by its invite link instead of its numeric id.
#  If the account has not joined yet, it raises a clear error telling you to run
#  JOIN first.
# ==============================================================================

from __future__ import annotations

import re

from telethon.tl.functions.messages import CheckChatInviteRequest
from telethon.tl.types import ChatInviteAlready, ChatInvitePeek

_INVITE_RE = re.compile(r"(?:https?://)?(?:t\.me|telegram\.me)/(?:joinchat/|\+)([\w-]+)")


def invite_hash(channel: str):
    """Return the invite hash if `channel` is a private invite link, else None."""
    channel = channel.strip()
    m = _INVITE_RE.search(channel)
    if m:
        return m.group(1)
    if channel.startswith("+"):
        return channel[1:]
    return None


async def resolve_channel_entity(client, channel: str):
    """
    Resolve `channel` (any accepted form) to an input entity usable by every op.

    Accepts:
      @username · username · https://t.me/username · t.me/username
      -1001234567890 (numeric id)
      https://t.me/+HASH · t.me/joinchat/HASH · +HASH   (private invite)
    """
    channel = channel.strip()
    h = invite_hash(channel)
    if h:
        res = await client(CheckChatInviteRequest(h))
        chat = getattr(res, "chat", None)
        if isinstance(res, (ChatInviteAlready, ChatInvitePeek)) and chat is not None:
            return await client.get_input_entity(chat)
        # ChatInvite preview only — the account has not joined this channel
        raise ValueError(
            "this private invite link points to a channel the account has not "
            "joined yet — run JOIN with this link first, then use its numeric id "
            "or the same invite link once joined."
        )
    return await client.get_input_entity(channel)
