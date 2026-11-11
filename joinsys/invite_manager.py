"""
// lang: Python 3.11+, file: join_system/invite_manager.py
// target: Windows 11 / Linux, Telethon 1.36+
// purpose: Full lifecycle management for Telegram invite links —
//          generation, tracking, rotation, expiry, quota enforcement,
//          per-channel link pools, usage analytics, and persistence.
"""

from __future__ import annotations

import asyncio
import enum
import json
import logging
import random
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable

from telethon import TelegramClient
from telethon.errors import (
    ChatAdminRequiredError,
    FloodWaitError,
    UserNotParticipantError,
    ChatWriteForbiddenError,
)
from telethon.tl.functions.messages import (
    ExportChatInviteRequest,
    EditExportedChatInviteRequest,
    DeleteExportedChatInviteRequest,
    GetExportedChatInvitesRequest,
    GetAdminsWithInvitesRequest,
    GetChatInviteImportersRequest,
)
from telethon.tl.types import (
    ChatInviteExported,
    ChatInviteImporter,
)

log = logging.getLogger("invite_manager")


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class InviteStatus(enum.Enum):
    ACTIVE    = "active"
    EXPIRED   = "expired"
    REVOKED   = "revoked"
    EXHAUSTED = "exhausted"   # usage_limit reached
    UNKNOWN   = "unknown"


class RotationPolicy(enum.Enum):
    NONE           = "none"           # never rotate
    ON_EXHAUSTION  = "on_exhaustion"  # rotate when usage limit hit
    TIMED          = "timed"          # rotate every N seconds
    USAGE_PERCENT  = "usage_percent"  # rotate at X% usage
    ROUND_ROBIN    = "round_robin"    # distribute across pool


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class InviteLink:
    link: str
    chat_id: int
    creator_phone: str
    created_at: float = field(default_factory=time.time)
    expire_date: float | None = None          # Unix timestamp
    usage_limit: int | None = None            # None = unlimited
    usage_count: int = 0
    request_needed: bool = False              # approval-required link
    title: str = ""
    status: InviteStatus = InviteStatus.ACTIVE
    revoked_at: float | None = None
    last_used_at: float | None = None
    link_id: str = ""                         # hash part of t.me/+HASH

    def __post_init__(self):
        if not self.link_id:
            # extract hash from link
            for prefix in ("https://t.me/+", "t.me/+",
                           "https://t.me/joinchat/", "t.me/joinchat/"):
                if self.link.startswith(prefix):
                    self.link_id = self.link[len(prefix):]
                    break
            else:
                self.link_id = self.link

    @property
    def is_expired(self) -> bool:
        if self.expire_date and time.time() > self.expire_date:
            return True
        return False

    @property
    def is_exhausted(self) -> bool:
        if self.usage_limit is not None and self.usage_count >= self.usage_limit:
            return True
        return False

    @property
    def is_usable(self) -> bool:
        return (
            self.status == InviteStatus.ACTIVE
            and not self.is_expired
            and not self.is_exhausted
        )

    @property
    def uses_remaining(self) -> int | None:
        if self.usage_limit is None:
            return None
        return max(0, self.usage_limit - self.usage_count)

    @property
    def age_seconds(self) -> float:
        return time.time() - self.created_at

    def to_dict(self) -> dict:
        d = asdict(self)
        d["status"] = self.status.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "InviteLink":
        d = dict(d)
        d["status"] = InviteStatus(d.get("status", "active"))
        return cls(**d)


@dataclass
class LinkUsageEvent:
    link_id: str
    chat_id: int
    user_id: int | None
    ts: float = field(default_factory=time.time)
    success: bool = True
    error: str = ""


@dataclass
class ChannelLinkConfig:
    """Per-channel invite link policy."""
    chat_id: int
    rotation_policy: RotationPolicy = RotationPolicy.ON_EXHAUSTION
    default_usage_limit: int | None = 100
    default_expire_hours: float | None = 72.0
    request_needed: bool = False
    min_pool_size: int = 2              # maintain at least N active links
    max_pool_size: int = 10
    rotate_at_percent: float = 0.80    # for USAGE_PERCENT policy
    rotate_every_sec: float = 86400.0  # for TIMED policy
    creator_phone: str = ""            # preferred creator; "" = any


# ---------------------------------------------------------------------------
# Invite store (persistence)
# ---------------------------------------------------------------------------

class InviteStore:
    """
    JSON-backed store for invite links.
    Thread-safe via asyncio.Lock.
    """

    def __init__(self, path: str | Path = "invite_store.json"):
        self._path = Path(path)
        self._data: dict[str, InviteLink] = {}   # link_id -> InviteLink
        self._lock = asyncio.Lock()
        self._dirty = False
        if self._path.exists():
            self._load()

    def _load(self):
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            loaded = 0
            for link_id, d in raw.items():
                try:
                    self._data[link_id] = InviteLink.from_dict(d)
                    loaded += 1
                except Exception as e:
                    log.warning("skip bad entry %s: %s", link_id, e)
            log.debug("invite store loaded %d links from %s", loaded, self._path)
        except Exception as e:
            log.warning("invite store load error: %s", e)

    def _save(self):
        if not self._dirty:
            return
        try:
            out = {k: v.to_dict() for k, v in self._data.items()}
            self._path.write_text(
                json.dumps(out, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            self._dirty = False
        except Exception as e:
            log.warning("invite store save error: %s", e)

    async def put(self, link: InviteLink):
        async with self._lock:
            self._data[link.link_id] = link
            self._dirty = True

    async def get(self, link_id: str) -> InviteLink | None:
        async with self._lock:
            return self._data.get(link_id)

    async def get_by_link(self, link: str) -> InviteLink | None:
        async with self._lock:
            for il in self._data.values():
                if il.link == link:
                    return il
        return None

    async def all_for_chat(self, chat_id: int) -> list[InviteLink]:
        async with self._lock:
            return [il for il in self._data.values() if il.chat_id == chat_id]

    async def active_for_chat(self, chat_id: int) -> list[InviteLink]:
        return [il for il in await self.all_for_chat(chat_id) if il.is_usable]

    async def mark_revoked(self, link_id: str):
        async with self._lock:
            il = self._data.get(link_id)
            if il:
                il.status = InviteStatus.REVOKED
                il.revoked_at = time.time()
                self._dirty = True

    async def increment_usage(self, link_id: str) -> InviteLink | None:
        async with self._lock:
            il = self._data.get(link_id)
            if il:
                il.usage_count += 1
                il.last_used_at = time.time()
                if il.is_exhausted:
                    il.status = InviteStatus.EXHAUSTED
                self._dirty = True
            return il

    async def flush(self):
        async with self._lock:
            self._save()

    async def stats(self) -> dict:
        async with self._lock:
            total = len(self._data)
            by_status: dict[str, int] = defaultdict(int)
            by_chat: dict[int, int] = defaultdict(int)
            for il in self._data.values():
                by_status[il.status.value] += 1
                by_chat[il.chat_id] += 1
            return {
                "total": total,
                "by_status": dict(by_status),
                "by_chat": dict(by_chat),
            }

    async def prune_old(self, max_age_days: float = 30.0) -> int:
        cutoff = time.time() - max_age_days * 86400.0
        async with self._lock:
            to_delete = [
                k for k, v in self._data.items()
                if v.created_at < cutoff and v.status != InviteStatus.ACTIVE
            ]
            for k in to_delete:
                del self._data[k]
            if to_delete:
                self._dirty = True
            return len(to_delete)


# ---------------------------------------------------------------------------
# Invite manager
# ---------------------------------------------------------------------------

class InviteManager:
    """
    Full invite link lifecycle manager.

    Responsibilities
    ----------------
    - Create new invite links via Telegram API
    - Maintain per-channel link pools (min/max pool size)
    - Rotate links according to RotationPolicy
    - Track usage counts and sync with Telegram
    - Revoke exhausted/expired links
    - Pull import logs (who joined via which link)
    - Persist everything to InviteStore
    - Emit events via optional callback

    Usage
    -----
    mgr = InviteManager(clients, store=InviteStore("store.json"))
    await mgr.ensure_pool(chat_id, config)
    link = await mgr.get_link(chat_id)
    """

    def __init__(
        self,
        clients: dict[str, TelegramClient],
        *,
        store: InviteStore | None = None,
        store_path: str | Path = "invite_store.json",
        event_callback: Callable[[str, dict], None] | None = None,
        flood_max_wait: int = 120,
        jitter: float = 1.0,
        auto_sync_interval: float = 300.0,
        auto_prune: bool = True,
    ):
        self._clients = clients
        self._phones = list(clients.keys())
        self._store = store or InviteStore(store_path)
        self._event_cb = event_callback
        self._flood_max_wait = flood_max_wait
        self._jitter = jitter
        self._channel_configs: dict[int, ChannelLinkConfig] = {}
        self._usage_events: deque[LinkUsageEvent] = deque(maxlen=5000)
        self._rr_index: dict[int, int] = defaultdict(int)   # chat_id -> index
        self._lock = asyncio.Lock()
        self._auto_sync_interval = auto_sync_interval
        self._auto_prune = auto_prune
        self._sync_task: asyncio.Task | None = None
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self):
        self._running = True
        self._sync_task = asyncio.create_task(self._auto_sync_loop(), name="invite-sync")
        log.info("InviteManager started")

    async def stop(self):
        self._running = False
        if self._sync_task:
            self._sync_task.cancel()
            await asyncio.gather(self._sync_task, return_exceptions=True)
        await self._store.flush()
        log.info("InviteManager stopped")

    async def _auto_sync_loop(self):
        while self._running:
            await asyncio.sleep(self._auto_sync_interval)
            try:
                await self._sync_all_channels()
                if self._auto_prune:
                    pruned = await self._store.prune_old()
                    if pruned:
                        log.info("pruned %d old invite links", pruned)
            except Exception as e:
                log.error("auto sync error: %s", e)

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    def configure_channel(self, config: ChannelLinkConfig):
        self._channel_configs[config.chat_id] = config

    def get_config(self, chat_id: int) -> ChannelLinkConfig:
        return self._channel_configs.get(chat_id, ChannelLinkConfig(chat_id=chat_id))

    # ------------------------------------------------------------------
    # Link creation
    # ------------------------------------------------------------------

    async def create_link(
        self,
        chat_id: int,
        *,
        usage_limit: int | None = None,
        expire_hours: float | None = None,
        request_needed: bool = False,
        title: str = "",
        phone: str | None = None,
    ) -> InviteLink:
        """Create a new invite link for chat_id."""
        cfg = self.get_config(chat_id)
        limit = usage_limit if usage_limit is not None else cfg.default_usage_limit
        expire_secs = (expire_hours or cfg.default_expire_hours or 0) * 3600

        expire_ts = None
        expire_date_obj = None
        if expire_secs > 0:
            import datetime
            expire_ts = time.time() + expire_secs
            expire_date_obj = datetime.datetime.fromtimestamp(expire_ts, tz=datetime.timezone.utc)

        creator = phone or cfg.creator_phone or random.choice(self._phones)
        client = self._clients[creator]

        await asyncio.sleep(random.uniform(0, self._jitter))

        try:
            entity = await client.get_entity(chat_id)
            result: ChatInviteExported = await client(ExportChatInviteRequest(
                peer=entity,
                expire_date=expire_date_obj,
                usage_limit=limit,
                request_needed=request_needed,
                title=title or None,
            ))
        except FloodWaitError as e:
            wait = min(e.seconds, self._flood_max_wait)
            log.warning("flood wait %ds creating link for %d", wait, chat_id)
            await asyncio.sleep(wait)
            entity = await client.get_entity(chat_id)
            result = await client(ExportChatInviteRequest(
                peer=entity,
                expire_date=expire_date_obj,
                usage_limit=limit,
                request_needed=request_needed,
                title=title or None,
            ))
        except ChatAdminRequiredError:
            raise PermissionError(f"not admin in chat {chat_id}")

        il = InviteLink(
            link=result.link,
            chat_id=chat_id,
            creator_phone=creator,
            expire_date=expire_ts,
            usage_limit=limit,
            request_needed=request_needed,
            title=title,
            status=InviteStatus.ACTIVE,
        )
        await self._store.put(il)
        self._emit("link_created", {"chat_id": chat_id, "link": il.link, "limit": limit})
        log.info("created link %s for chat %d (limit=%s expire_h=%s)",
                 il.link, chat_id, limit, expire_hours)
        return il

    # ------------------------------------------------------------------
    # Link retrieval
    # ------------------------------------------------------------------

    async def get_link(
        self,
        chat_id: int,
        *,
        policy: RotationPolicy | None = None,
    ) -> InviteLink | None:
        """
        Return the best available invite link for chat_id.
        Auto-creates if pool is empty.
        """
        cfg = self.get_config(chat_id)
        effective_policy = policy or cfg.rotation_policy
        active = await self._store.active_for_chat(chat_id)

        if not active:
            log.info("pool empty for chat %d — creating new link", chat_id)
            try:
                il = await self.create_link(chat_id)
                return il
            except Exception as e:
                log.error("failed to create link for %d: %s", chat_id, e)
                return None

        if effective_policy == RotationPolicy.ROUND_ROBIN:
            async with self._lock:
                idx = self._rr_index[chat_id] % len(active)
                self._rr_index[chat_id] += 1
            return active[idx]

        elif effective_policy in (RotationPolicy.ON_EXHAUSTION, RotationPolicy.USAGE_PERCENT):
            # prefer link with most remaining capacity
            def _score(il: InviteLink) -> float:
                if il.usage_limit is None:
                    return float("inf")
                return il.usage_limit - il.usage_count
            return max(active, key=_score)

        elif effective_policy == RotationPolicy.TIMED:
            # newest link
            return max(active, key=lambda il: il.created_at)

        # default: first active
        return active[0]

    # ------------------------------------------------------------------
    # Pool management
    # ------------------------------------------------------------------

    async def ensure_pool(self, chat_id: int) -> list[InviteLink]:
        """
        Ensure the pool for chat_id meets min_pool_size.
        Creates new links as needed.
        Returns all active links for chat_id.
        """
        cfg = self.get_config(chat_id)
        active = await self._store.active_for_chat(chat_id)

        to_create = max(0, cfg.min_pool_size - len(active))
        created: list[InviteLink] = []
        for _ in range(to_create):
            try:
                il = await self.create_link(chat_id)
                created.append(il)
                await asyncio.sleep(random.uniform(1.0, 3.0))
            except Exception as e:
                log.error("pool replenish failed for %d: %s", chat_id, e)
                break

        active = await self._store.active_for_chat(chat_id)
        return active

    async def rotate_link(
        self,
        chat_id: int,
        old_link_id: str,
        *,
        revoke_old: bool = True,
    ) -> InviteLink | None:
        """
        Create a new link, optionally revoke the old one.
        Returns the new link.
        """
        cfg = self.get_config(chat_id)
        active = await self._store.active_for_chat(chat_id)

        if len(active) < cfg.max_pool_size:
            new_link = await self.create_link(chat_id)
        else:
            # already at max — just return the best available
            new_link = await self.get_link(chat_id)

        if revoke_old:
            old = await self._store.get(old_link_id)
            if old:
                await self.revoke_link(chat_id, old_link_id, phone=old.creator_phone)

        return new_link

    # ------------------------------------------------------------------
    # Revocation
    # ------------------------------------------------------------------

    async def revoke_link(
        self,
        chat_id: int,
        link_id: str,
        *,
        phone: str | None = None,
    ) -> bool:
        """Revoke an invite link via Telegram API and mark in store."""
        il = await self._store.get(link_id)
        if il is None:
            log.warning("revoke: link %s not in store", link_id)
            return False

        creator = phone or il.creator_phone or random.choice(self._phones)
        client = self._clients.get(creator)
        if client is None:
            return False

        try:
            entity = await client.get_entity(chat_id)
            await client(EditExportedChatInviteRequest(
                peer=entity,
                link=il.link,
                revoked=True,
            ))
            await self._store.mark_revoked(link_id)
            self._emit("link_revoked", {"chat_id": chat_id, "link": il.link})
            log.info("revoked link %s for chat %d", il.link, chat_id)
            return True
        except FloodWaitError as e:
            log.warning("flood revoke link %s: wait %ds", link_id, e.seconds)
            return False
        except Exception as e:
            log.error("revoke error %s: %s", link_id, e)
            return False

    async def revoke_all(self, chat_id: int) -> int:
        """Revoke all active links for chat_id. Returns count revoked."""
        active = await self._store.active_for_chat(chat_id)
        count = 0
        for il in active:
            ok = await self.revoke_link(chat_id, il.link_id)
            if ok:
                count += 1
            await asyncio.sleep(random.uniform(0.5, 1.5))
        return count

    # ------------------------------------------------------------------
    # Usage tracking
    # ------------------------------------------------------------------

    async def record_use(
        self,
        link_id: str,
        user_id: int | None = None,
        success: bool = True,
        error: str = "",
    ) -> InviteLink | None:
        """
        Call after a user follows an invite link.
        Updates usage count and fires rotation check.
        """
        il = await self._store.increment_usage(link_id)
        if il is None:
            return None

        event = LinkUsageEvent(
            link_id=link_id,
            chat_id=il.chat_id,
            user_id=user_id,
            success=success,
            error=error,
        )
        self._usage_events.append(event)
        self._emit("link_used", {
            "link_id": link_id,
            "chat_id": il.chat_id,
            "usage_count": il.usage_count,
            "usage_limit": il.usage_limit,
        })

        # check rotation triggers
        await self._check_rotation(il)
        return il

    async def _check_rotation(self, il: InviteLink):
        """Trigger auto-rotation if the policy says so."""
        cfg = self.get_config(il.chat_id)

        if cfg.rotation_policy == RotationPolicy.ON_EXHAUSTION and il.is_exhausted:
            log.info("link %s exhausted → rotating for chat %d", il.link_id, il.chat_id)
            await self.rotate_link(il.chat_id, il.link_id)

        elif cfg.rotation_policy == RotationPolicy.USAGE_PERCENT:
            if il.usage_limit and il.usage_limit > 0:
                pct = il.usage_count / il.usage_limit
                if pct >= cfg.rotate_at_percent:
                    log.info("link %s at %.0f%% → rotating for chat %d",
                             il.link_id, pct * 100, il.chat_id)
                    await self.rotate_link(il.chat_id, il.link_id)

        elif cfg.rotation_policy == RotationPolicy.TIMED:
            if il.age_seconds >= cfg.rotate_every_sec:
                log.info("link %s age %.0fs → rotating for chat %d",
                         il.link_id, il.age_seconds, il.chat_id)
                await self.rotate_link(il.chat_id, il.link_id)

    # ------------------------------------------------------------------
    # Import log (who joined via link)
    # ------------------------------------------------------------------

    async def fetch_importers(
        self,
        chat_id: int,
        link: str,
        *,
        phone: str | None = None,
        limit: int = 100,
    ) -> list[ChatInviteImporter]:
        """
        Fetch the list of users who joined via a specific invite link.
        Requires admin access.
        """
        creator = phone or random.choice(self._phones)
        client = self._clients[creator]
        try:
            entity = await client.get_entity(chat_id)
            result = await client(GetChatInviteImportersRequest(
                peer=entity,
                link=link,
                limit=limit,
                offset_date=0,
                offset_user=None,
            ))
            return result.importers
        except Exception as e:
            log.error("fetch_importers error for %d link %s: %s", chat_id, link, e)
            return []

    async def total_joins_via_link(self, chat_id: int, link: str) -> int:
        importers = await self.fetch_importers(chat_id, link, limit=1)
        # Telegram returns count in the response — approximate via store
        il = await self._store.get_by_link(link)
        return il.usage_count if il else len(importers)

    # ------------------------------------------------------------------
    # Sync with Telegram
    # ------------------------------------------------------------------

    async def sync_channel(
        self,
        chat_id: int,
        *,
        phone: str | None = None,
    ) -> int:
        """
        Pull current invite links from Telegram, update store.
        Returns count of synced links.
        """
        creator = phone or random.choice(self._phones)
        client = self._clients[creator]

        try:
            entity = await client.get_entity(chat_id)
            result = await client(GetExportedChatInvitesRequest(
                peer=entity,
                admin_id=await client.get_me(),
                limit=100,
                revoked=False,
            ))
            count = 0
            for invite in (result.invites or []):
                if not isinstance(invite, ChatInviteExported):
                    continue
                existing = await self._store.get_by_link(invite.link)
                if existing:
                    existing.usage_count = invite.usage or 0
                    if invite.revoked:
                        existing.status = InviteStatus.REVOKED
                    elif existing.is_expired or existing.is_exhausted:
                        existing.status = InviteStatus.EXPIRED
                    await self._store.put(existing)
                else:
                    expire_ts = None
                    if invite.expire_date:
                        expire_ts = invite.expire_date.timestamp()
                    new_il = InviteLink(
                        link=invite.link,
                        chat_id=chat_id,
                        creator_phone=creator,
                        usage_limit=invite.usage_limit,
                        usage_count=invite.usage or 0,
                        expire_date=expire_ts,
                        request_needed=invite.request_needed,
                        title=invite.title or "",
                        status=InviteStatus.REVOKED if invite.revoked else InviteStatus.ACTIVE,
                    )
                    await self._store.put(new_il)
                count += 1
            await self._store.flush()
            log.info("synced %d links for chat %d", count, chat_id)
            return count
        except FloodWaitError as e:
            log.warning("flood sync chat %d wait %ds", chat_id, e.seconds)
            return 0
        except Exception as e:
            log.error("sync_channel error %d: %s", chat_id, e)
            return 0

    async def _sync_all_channels(self):
        for chat_id in list(self._channel_configs.keys()):
            await self.sync_channel(chat_id)
            await asyncio.sleep(random.uniform(1.0, 3.0))

    # ------------------------------------------------------------------
    # Analytics
    # ------------------------------------------------------------------

    async def usage_report(self, chat_id: int) -> dict:
        all_links = await self._store.all_for_chat(chat_id)
        active = [il for il in all_links if il.is_usable]
        total_uses = sum(il.usage_count for il in all_links)
        recent_events = [e for e in self._usage_events if e.chat_id == chat_id]
        return {
            "chat_id": chat_id,
            "total_links": len(all_links),
            "active_links": len(active),
            "total_uses": total_uses,
            "recent_events_last_5000": len(recent_events),
            "links": [
                {
                    "link_id": il.link_id,
                    "status": il.status.value,
                    "usage": f"{il.usage_count}/{il.usage_limit or '∞'}",
                    "created": il.created_at,
                    "expires": il.expire_date,
                }
                for il in sorted(all_links, key=lambda x: x.created_at, reverse=True)[:20]
            ],
        }

    async def global_stats(self) -> dict:
        return await self._store.stats()

    def usage_events(self, chat_id: int | None = None) -> list[LinkUsageEvent]:
        if chat_id is None:
            return list(self._usage_events)
        return [e for e in self._usage_events if e.chat_id == chat_id]

    def top_links(self, n: int = 10) -> list[tuple[str, int]]:
        """Return (link_id, usage_count) sorted by usage, top n."""
        counts: dict[str, int] = defaultdict(int)
        for e in self._usage_events:
            if e.success:
                counts[e.link_id] += 1
        return sorted(counts.items(), key=lambda x: x[1], reverse=True)[:n]

    # ------------------------------------------------------------------
    # Event emission
    # ------------------------------------------------------------------

    def _emit(self, event_name: str, data: dict):
        if self._event_cb:
            try:
                self._event_cb(event_name, data)
            except Exception as e:
                log.debug("event_callback error: %s", e)


# ---------------------------------------------------------------------------
# Quick builders
# ---------------------------------------------------------------------------

def make_invite_manager(
    clients: dict[str, TelegramClient],
    store_path: str = "invite_store.json",
    event_callback: Callable[[str, dict], None] | None = None,
) -> InviteManager:
    return InviteManager(
        clients,
        store_path=store_path,
        event_callback=event_callback,
        jitter=1.0,
        auto_sync_interval=300.0,
    )


def unlimited_link_config(chat_id: int, creator_phone: str = "") -> ChannelLinkConfig:
    """Unlimited uses, no expiry, single link, no rotation."""
    return ChannelLinkConfig(
        chat_id=chat_id,
        rotation_policy=RotationPolicy.NONE,
        default_usage_limit=None,
        default_expire_hours=None,
        min_pool_size=1,
        max_pool_size=1,
        creator_phone=creator_phone,
    )


def disposable_link_config(
    chat_id: int,
    uses_per_link: int = 50,
    expire_hours: float = 24.0,
    creator_phone: str = "",
) -> ChannelLinkConfig:
    """Short-lived links, auto-rotate on exhaustion."""
    return ChannelLinkConfig(
        chat_id=chat_id,
        rotation_policy=RotationPolicy.ON_EXHAUSTION,
        default_usage_limit=uses_per_link,
        default_expire_hours=expire_hours,
        min_pool_size=3,
        max_pool_size=10,
        creator_phone=creator_phone,
    )


def timed_rotation_config(
    chat_id: int,
    rotate_every_hours: float = 24.0,
    creator_phone: str = "",
) -> ChannelLinkConfig:
    """Rotate link every N hours regardless of usage."""
    return ChannelLinkConfig(
        chat_id=chat_id,
        rotation_policy=RotationPolicy.TIMED,
        default_usage_limit=None,
        default_expire_hours=rotate_every_hours * 1.5,
        rotate_every_sec=rotate_every_hours * 3600.0,
        min_pool_size=2,
        max_pool_size=5,
        creator_phone=creator_phone,
    )


# ---------------------------------------------------------------------------
# Bulk link creator helper
# ---------------------------------------------------------------------------

async def create_links_for_channels(
    clients: dict[str, TelegramClient],
    chat_ids: list[int],
    *,
    usage_limit: int | None = 100,
    expire_hours: float = 48.0,
    store_path: str = "invite_store.json",
) -> dict[int, InviteLink]:
    """
    Create one invite link per channel.  Returns {chat_id: InviteLink}.
    """
    mgr = make_invite_manager(clients, store_path)
    out: dict[int, InviteLink] = {}
    for chat_id in chat_ids:
        try:
            il = await mgr.create_link(
                chat_id,
                usage_limit=usage_limit,
                expire_hours=expire_hours,
            )
            out[chat_id] = il
        except Exception as e:
            log.error("create link for %d: %s", chat_id, e)
        await asyncio.sleep(random.uniform(2.0, 5.0))
    await mgr._store.flush()
    return out