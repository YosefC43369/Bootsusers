# lifecycle/warmup.py
# ==============================================================================
#  Account warm-up — age fresh accounts before using them for real work.
# ------------------------------------------------------------------------------
#  A brand-new account that instantly mass-joins and reacts is the easiest thing
#  for Telegram to flag. Warm-up runs gentle, escalating activity over several
#  sessions so the account looks lived-in before the suite points it at a target:
#
#    stage 0  →  set a profile, join 1-2 large public channels, read history
#    stage 1  →  read more, drop a light reaction or two
#    stage 2  →  join another channel, occasional reaction
#    stage 3+ →  graduated: cleared for normal operations
#
#  Stage progress is persisted in the StateStore (warmup_stage) and gated by
#  time, so one call advances an account by at most one stage per cooldown.
# ==============================================================================

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Optional

from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import ReactionEmoji
from telethon.tl.functions.messages import SendReactionRequest

from .pacing import human_delay, mark_read

GRADUATED_STAGE = 3

# Safe, huge public channels to idle in during warm-up (edit freely).
DEFAULT_WARMUP_CHANNELS = [
    "telegram", "durov", "TelegramTips",
]

# minimum seconds between two stage advances for the same account
STAGE_COOLDOWN_SEC = 6 * 3600


@dataclass
class WarmupConfig:
    channels: "list[str]" = field(default_factory=lambda: list(DEFAULT_WARMUP_CHANNELS))
    reactions: "list[str]" = field(default_factory=lambda: ["👍", "🔥", "❤️", "👏"])
    read_limit: int = 20
    stage_cooldown_sec: float = STAGE_COOLDOWN_SEC
    randomize_profile: bool = True
    avatar_dir: Optional[str] = None


async def _join(acc, username: str, log) -> bool:
    try:
        ent = await acc.client.get_entity(username)
        await acc.client(JoinChannelRequest(ent))
        log("ok", f"{acc.phone} warmup-joined @{username}")
        return True
    except Exception as exc:
        log("warn", f"{acc.phone} warmup-join @{username} failed: {exc}")
        return False


async def _read(acc, username: str, cfg: WarmupConfig, log) -> Optional[int]:
    try:
        ent = await acc.client.get_entity(username)
        msgs = await acc.client.get_messages(ent, limit=cfg.read_limit)
        if msgs:
            await mark_read(acc.client, ent, msgs[0].id, is_channel=True)
            log("info", f"{acc.phone} read {len(msgs)} from @{username}")
            return msgs[0].id if msgs else None
    except Exception as exc:
        log("warn", f"{acc.phone} warmup-read @{username} failed: {exc}")
    return None


async def _light_reaction(acc, username: str, cfg: WarmupConfig, log) -> None:
    try:
        ent = await acc.client.get_entity(username)
        msgs = await acc.client.get_messages(ent, limit=10)
        if not msgs:
            return
        target = random.choice(msgs)
        emoji = random.choice(cfg.reactions)
        await acc.client(SendReactionRequest(
            peer=ent, msg_id=target.id, big=False, add_to_recent=True,
            reaction=[ReactionEmoji(emoticon=emoji)]))
        log("ok", f"{acc.phone} warmup-reacted {emoji} in @{username}")
    except Exception as exc:
        log("warn", f"{acc.phone} warmup-react @{username} failed: {exc}")


async def warmup_step(acc, store, cfg: Optional[WarmupConfig] = None, log=None) -> int:
    """
    Advance one account by at most one warm-up stage. Returns the new stage.
    Respects a per-account cooldown so repeated calls don't over-activate it.
    """
    cfg = cfg or WarmupConfig()
    log = log or (lambda lvl, msg: None)

    rec = store.get_account(acc.phone) if store else None
    stage = rec.warmup_stage if rec else 0
    if stage >= GRADUATED_STAGE:
        log("info", f"{acc.phone} already graduated (stage {stage})")
        return stage

    # gate by cooldown
    if store and store.cooldown_remaining(acc.phone, "warmup") > 0:
        log("info", f"{acc.phone} warmup on cooldown — stage stays {stage}")
        return stage

    log("accent", f"{acc.phone} warm-up stage {stage} → {stage + 1}")

    if stage == 0:
        if cfg.randomize_profile:
            try:
                from .profile import randomize_profile
                await randomize_profile(acc, avatar_dir=cfg.avatar_dir, log=log)
            except Exception as exc:
                log("warn", f"{acc.phone} profile randomize skipped: {exc}")
        ch = cfg.channels[:2]
        for c in ch:
            await _join(acc, c, log)
            await human_delay("join")
            await _read(acc, c, cfg, log)
            await human_delay("view")

    elif stage == 1:
        for c in cfg.channels[:2]:
            await _read(acc, c, cfg, log)
            await human_delay("view")
        if cfg.channels:
            await _light_reaction(acc, random.choice(cfg.channels[:2]), cfg, log)

    elif stage == 2:
        if len(cfg.channels) > 2:
            await _join(acc, cfg.channels[2], log)
            await human_delay("join")
        for c in cfg.channels:
            await _read(acc, c, cfg, log)
            await human_delay("view")
        await _light_reaction(acc, random.choice(cfg.channels), cfg, log)

    new_stage = stage + 1
    if store:
        store.set_warmup_stage(acc.phone, new_stage)
        store.set_cooldown(acc.phone, "warmup", cfg.stage_cooldown_sec)
        if new_stage >= GRADUATED_STAGE:
            store.set_status(acc.phone, "ready")
    if new_stage >= GRADUATED_STAGE:
        log("ok", f"{acc.phone} graduated — ready for operations")
    return new_stage


async def warmup_pool(accounts, store, cfg: Optional[WarmupConfig] = None,
                      log=None, should_stop=None) -> dict:
    """Run one warm-up step across every connected account."""
    log = log or (lambda lvl, msg: None)
    should_stop = should_stop or (lambda: False)
    result = {"advanced": 0, "graduated": 0, "skipped": 0}
    for acc in accounts:
        if should_stop():
            break
        if getattr(acc, "client", None) is None or not acc.healthy():
            result["skipped"] += 1
            continue
        before = store.get_account(acc.phone).warmup_stage if store and store.get_account(acc.phone) else 0
        after = await warmup_step(acc, store, cfg, log)
        if after > before:
            result["advanced"] += 1
        if after >= GRADUATED_STAGE:
            result["graduated"] += 1
    log("accent", f"warm-up sweep — {result}")
    return result
