"""
// join_system/channel_resolver.py
"""

from __future__ import annotations

import asyncio
import enum
import hashlib
import json
import logging
import re
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import AsyncIterator

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
    ChannelPrivateError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    ChatIdInvalidError,
)
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import CheckChatInviteRequest
from telethon.tl.types import (
    Channel,
    Chat,
    ChannelFull,
    ChatFull,
    ChatInvite,
    ChatInviteAlready,
    InputPeerChannel,
    InputPeerChat,
    PeerChannel,
    PeerChat,
    User,
)

log = logging.getLogger("channel_resolver")


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class ResolveStatus(enum.Enum):
    OK          = "ok"
    NOT_FOUND   = "not_found"
    PRIVATE     = "private"
    INVALID     = "invalid"
    FLOOD       = "flood"
    CACHED      = "cached"
    ERROR       = "error"


class EntityType(enum.Enum):
    CHANNEL       = "channel"      # broadcast
    MEGAGROUP     = "megagroup"    # supergroup
    GIGAGROUP     = "gigagroup"    # broadcast group
    BASIC_GROUP   = "basic_group"  # legacy chat
    USER          = "user"
    BOT           = "bot"
    UNKNOWN       = "unknown"


# ---------------------------------------------------------------------------
# Resolved entity
# ---------------------------------------------------------------------------

@dataclass
class ResolvedEntity:
    # core
    raw_input: str
    status: ResolveStatus
    entity_type: EntityType = EntityType.UNKNOWN

    # identity
    entity_id: int = 0
    access_hash: int = 0
    username: str = ""
    title: str = ""

    # metadata
    participants_count: int = 0
    is_verified: bool = False
    is_scam: bool = False
    is_restricted: bool = False
    is_broadcast: bool = False
    is_megagroup: bool = False
    is_gigagroup: bool = False
    has_link: bool = False
    linked_chat_id: int | None = None
    about: str = ""
    invite_link: str = ""
    photo_id: int | None = None

    # admin info
    can_view_participants: bool = False
    can_set_username: bool = False
    can_set_stickers: bool = False
    hidden_prehistory: bool = False

    # timing
    resolved_at: float = field(default_factory=time.time)
    error_msg: str = ""
    flood_wait_sec: int = 0


def _entity_type_from_tl(entity) -> EntityType:
    if isinstance(entity, Channel):
        if entity.megagroup:
            return EntityType.MEGAGROUP
        if entity.gigagroup:
            return EntityType.GIGAGROUP
        return EntityType.CHANNEL
    if isinstance(entity, Chat):
        return EntityType.BASIC_GROUP
    if isinstance(entity, User):
        return EntityType.BOT if entity.bot else EntityType.USER
    return EntityType.UNKNOWN


def _build_resolved(raw: str, entity, full=None) -> ResolvedEntity:
    etype = _entity_type_from_tl(entity)
    r = ResolvedEntity(
        raw_input=raw,
        status=ResolveStatus.OK,
        entity_type=etype,
        entity_id=entity.id,
        access_hash=getattr(entity, "access_hash", 0) or 0,
        username=getattr(entity, "username", "") or "",
        title=getattr(entity, "title", "") or getattr(entity, "first_name", "") or "",
        is_verified=getattr(entity, "verified", False),
        is_scam=getattr(entity, "scam", False),
        is_restricted=getattr(entity, "restricted", False),
        is_broadcast=getattr(entity, "broadcast", False),
        is_megagroup=getattr(entity, "megagroup", False),
        is_gigagroup=getattr(entity, "gigagroup", False),
        participants_count=getattr(entity, "participants_count", 0) or 0,
    )
    if full is not None:
        if isinstance(full, ChannelFull):
            r.about = full.about or ""
            r.participants_count = full.participants_count or r.participants_count
            r.has_link = full.has_link
            r.linked_chat_id = full.linked_chat_id
            r.invite_link = full.exported_invite.link if full.exported_invite else ""
            r.can_view_participants = full.can_view_participants
            r.can_set_username = full.can_set_username
            r.can_set_stickers = full.can_set_stickers
            r.hidden_prehistory = full.hidden_prehistory
        elif isinstance(full, ChatFull):
            r.about = full.about or ""
            r.participants_count = getattr(full.participants, "participants", [])
            if isinstance(r.participants_count, list):
                r.participants_count = len(r.participants_count)
    return r


# ---------------------------------------------------------------------------
# Invite link parser
# ---------------------------------------------------------------------------

_INVITE_RE = re.compile(
    r"(?:https?://)?t\.me/(?:joinchat/|\+)([a-zA-Z0-9_-]+)",
    re.IGNORECASE,
)
_USERNAME_RE = re.compile(r"(?:https?://)?t\.me/([a-zA-Z][a-zA-Z0-9_]{3,})", re.IGNORECASE)
_AT_RE = re.compile(r"^@?([a-zA-Z][a-zA-Z0-9_]{3,})$")


def classify_input(raw: str) -> tuple[str, str]:
    """
    Returns (kind, value) where kind is "invite" | "username" | "chat_id" | "unknown".
    """
    s = raw.strip()
    if m := _INVITE_RE.match(s):
        return "invite", m.group(1)
    if s.lstrip("-").isdigit():
        return "chat_id", s
    if m := _USERNAME_RE.match(s):
        return "username", m.group(1)
    if m := _AT_RE.match(s):
        return "username", m.group(1)
    return "unknown", s


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

class ResolverCache:
    """
    Two-level cache:
    - L1: in-memory dict (fast, lost on restart)
    - L2: JSON file on disk (persists between sessions)

    Cache key = SHA-1 of the normalized input string.
    TTL configurable per-level.
    """

    def __init__(
        self,
        cache_path: str | Path | None = None,
        mem_ttl: float = 3600.0,
        disk_ttl: float = 86400.0 * 7,
    ):
        self._mem: dict[str, ResolvedEntity] = {}
        self._mem_ts: dict[str, float] = {}
        self.mem_ttl = mem_ttl
        self.disk_ttl = disk_ttl
        self._path = Path(cache_path) if cache_path else None
        self._lock = asyncio.Lock()
        self._dirty = False

        if self._path and self._path.exists():
            self._load_disk()

    # --- helpers ---

    @staticmethod
    def _key(raw: str) -> str:
        return hashlib.sha1(raw.strip().lower().encode()).hexdigest()

    def _load_disk(self):
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            now = time.time()
            loaded = 0
            for k, v in data.items():
                ts = v.get("resolved_at", 0)
                if now - ts < self.disk_ttl:
                    try:
                        v["status"] = ResolveStatus(v["status"])
                        v["entity_type"] = EntityType(v["entity_type"])
                        self._mem[k] = ResolvedEntity(**v)
                        self._mem_ts[k] = ts
                        loaded += 1
                    except Exception:
                        pass
            log.debug("cache loaded %d entries from %s", loaded, self._path)
        except Exception as e:
            log.warning("cache load failed: %s", e)

    def _save_disk(self):
        if not self._path or not self._dirty:
            return
        try:
            out = {}
            for k, e in self._mem.items():
                d = asdict(e)
                d["status"] = e.status.value
                d["entity_type"] = e.entity_type.value
                out[k] = d
            self._path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
            self._dirty = False
            log.debug("cache saved %d entries to %s", len(out), self._path)
        except Exception as e:
            log.warning("cache save failed: %s", e)

    # --- public ---

    async def get(self, raw: str) -> ResolvedEntity | None:
        k = self._key(raw)
        async with self._lock:
            e = self._mem.get(k)
            if e is None:
                return None
            ts = self._mem_ts.get(k, 0)
            if time.time() - ts > self.mem_ttl:
                del self._mem[k]
                del self._mem_ts[k]
                return None
            return e

    async def put(self, raw: str, entity: ResolvedEntity):
        k = self._key(raw)
        async with self._lock:
            self._mem[k] = entity
            self._mem_ts[k] = time.time()
            self._dirty = True

    async def invalidate(self, raw: str):
        k = self._key(raw)
        async with self._lock:
            self._mem.pop(k, None)
            self._mem_ts.pop(k, None)
            self._dirty = True

    async def flush(self):
        async with self._lock:
            self._save_disk()

    async def stats(self) -> dict:
        async with self._lock:
            return {
                "entries": len(self._mem),
                "dirty": self._dirty,
                "path": str(self._path) if self._path else None,
            }

    async def prune_expired(self):
        now = time.time()
        async with self._lock:
            expired = [k for k, ts in self._mem_ts.items() if now - ts > self.mem_ttl]
            for k in expired:
                self._mem.pop(k, None)
                self._mem_ts.pop(k, None)
            if expired:
                self._dirty = True
            return len(expired)


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------

class ChannelResolver:
    """
    Resolves Telegram targets using a pool of TelegramClients.

    - Rotates clients to spread API load
    - L1/L2 caching via ResolverCache
    - Flood-aware: waits then retries on FloodWaitError
    - Optionally fetches full channel info (member count, about, invite link)
    - Batch async resolution via resolve_many()
    - Streaming async generator via stream_resolve()
    """

    def __init__(
        self,
        clients: dict[str, TelegramClient],
        *,
        cache_path: str | Path | None = "resolver_cache.json",
        mem_ttl: float = 3600.0,
        disk_ttl: float = 86400.0 * 7,
        fetch_full: bool = True,
        batch_concurrency: int = 5,
        flood_max_wait: int = 120,
        jitter: float = 0.5,
    ):
        if not clients:
            raise ValueError("at least one client required")
        self._clients = clients
        self._phones = list(clients.keys())
        self._phone_cycle = iter([])      # rebuilt lazily
        self._fetch_full = fetch_full
        self._batch_sem = asyncio.Semaphore(batch_concurrency)
        self._flood_max_wait = flood_max_wait
        self._jitter = jitter
        self._cache = ResolverCache(cache_path, mem_ttl, disk_ttl)
        self._lock = asyncio.Lock()
        self._resolve_count = 0
        self._cache_hits = 0
        self._client_index = 0

    # ------------------------------------------------------------------
    # Client rotation
    # ------------------------------------------------------------------

    def _next_client(self) -> TelegramClient:
        async with asyncio.Lock():
            pass
        idx = self._client_index % len(self._phones)
        self._client_index += 1
        phone = self._phones[idx]
        return self._clients[phone], phone

    # ------------------------------------------------------------------
    # Core resolve
    # ------------------------------------------------------------------

    async def resolve(self, raw: str, *, use_cache: bool = True) -> ResolvedEntity:
        """Resolve a single target.  Returns ResolvedEntity."""
        # check cache first
        if use_cache:
            cached = await self._cache.get(raw)
            if cached is not None:
                cached.status = ResolveStatus.CACHED
                self._cache_hits += 1
                return cached

        await asyncio.sleep(random.uniform(0, self._jitter))
        kind, value = classify_input(raw)

        client, phone = self._next_client()
        entity = None
        full = None

        try:
            if kind == "invite":
                entity, full = await self._resolve_invite(client, raw, value)
            elif kind == "username":
                entity, full = await self._resolve_username(client, raw, value)
            elif kind == "chat_id":
                entity, full = await self._resolve_chat_id(client, raw, int(value))
            else:
                return ResolvedEntity(
                    raw_input=raw,
                    status=ResolveStatus.INVALID,
                    error_msg=f"cannot parse input: {raw!r}",
                )
        except FloodWaitError as e:
            wait = min(e.seconds, self._flood_max_wait)
            log.warning("flood wait %ds resolving %s — waiting…", wait, raw)
            await asyncio.sleep(wait)
            # one retry after flood
            try:
                if kind == "invite":
                    entity, full = await self._resolve_invite(client, raw, value)
                elif kind == "username":
                    entity, full = await self._resolve_username(client, raw, value)
                elif kind == "chat_id":
                    entity, full = await self._resolve_chat_id(client, raw, int(value))
            except Exception as retry_exc:
                return ResolvedEntity(
                    raw_input=raw,
                    status=ResolveStatus.ERROR,
                    error_msg=str(retry_exc),
                    flood_wait_sec=e.seconds,
                )
        except (UsernameInvalidError, UsernameNotOccupiedError):
            return ResolvedEntity(raw_input=raw, status=ResolveStatus.NOT_FOUND,
                                  error_msg="username not found")
        except ChannelPrivateError:
            return ResolvedEntity(raw_input=raw, status=ResolveStatus.PRIVATE,
                                  error_msg="channel is private")
        except (InviteHashExpiredError, InviteHashInvalidError):
            return ResolvedEntity(raw_input=raw, status=ResolveStatus.INVALID,
                                  error_msg="invite link expired or invalid")
        except ChatIdInvalidError:
            return ResolvedEntity(raw_input=raw, status=ResolveStatus.INVALID,
                                  error_msg="invalid chat id")
        except Exception as exc:
            return ResolvedEntity(raw_input=raw, status=ResolveStatus.ERROR,
                                  error_msg=str(exc))

        if entity is None:
            return ResolvedEntity(raw_input=raw, status=ResolveStatus.NOT_FOUND)

        result = _build_resolved(raw, entity, full)
        await self._cache.put(raw, result)
        self._resolve_count += 1
        log.debug("resolved %s → id=%d type=%s title=%r",
                  raw, result.entity_id, result.entity_type.value, result.title)
        return result

    # ------------------------------------------------------------------
    # Type-specific resolution
    # ------------------------------------------------------------------

    async def _resolve_invite(
        self,
        client: TelegramClient,
        raw: str,
        hash_: str,
    ):
        inv = await client(CheckChatInviteRequest(hash_))
        if isinstance(inv, ChatInviteAlready):
            entity = inv.chat
        elif isinstance(inv, ChatInvite):
            # not yet joined — build a minimal entity placeholder
            # (we can't get full info without joining)
            fake = _InvitePlaceholder(raw, inv)
            return fake, None
        else:
            return None, None

        full = None
        if self._fetch_full:
            full = await self._get_full(client, entity)
        return entity, full

    async def _resolve_username(
        self,
        client: TelegramClient,
        raw: str,
        username: str,
    ):
        entity = await client.get_entity(username)
        full = None
        if self._fetch_full and isinstance(entity, (Channel, Chat)):
            full = await self._get_full(client, entity)
        return entity, full

    async def _resolve_chat_id(
        self,
        client: TelegramClient,
        raw: str,
        chat_id: int,
    ):
        entity = await client.get_entity(chat_id)
        full = None
        if self._fetch_full and isinstance(entity, (Channel, Chat)):
            full = await self._get_full(client, entity)
        return entity, full

    async def _get_full(self, client: TelegramClient, entity):
        try:
            if isinstance(entity, Channel):
                result = await client(GetFullChannelRequest(entity))
                return result.full_chat
            elif isinstance(entity, Chat):
                from telethon.tl.functions.messages import GetFullChatRequest
                result = await client(GetFullChatRequest(entity.id))
                return result.full_chat
        except Exception as e:
            log.debug("get_full failed for %d: %s", entity.id, e)
        return None

    # ------------------------------------------------------------------
    # Batch & streaming
    # ------------------------------------------------------------------

    async def resolve_many(
        self,
        targets: list[str],
        *,
        use_cache: bool = True,
    ) -> list[ResolvedEntity]:
        """Resolve all targets concurrently, respecting batch_concurrency."""
        results = [None] * len(targets)

        async def _bounded_resolve(idx: int, raw: str):
            async with self._batch_sem:
                results[idx] = await self.resolve(raw, use_cache=use_cache)

        await asyncio.gather(
            *(_bounded_resolve(i, t) for i, t in enumerate(targets))
        )
        return results  # type: ignore[return-value]

    async def stream_resolve(
        self,
        targets: list[str],
        *,
        use_cache: bool = True,
    ) -> AsyncIterator[ResolvedEntity]:
        """Async generator — yields results as they complete."""
        queue: asyncio.Queue[ResolvedEntity] = asyncio.Queue()

        async def _worker(raw: str):
            r = await self.resolve(raw, use_cache=use_cache)
            await queue.put(r)

        tasks = [asyncio.create_task(_worker(t)) for t in targets]
        for _ in range(len(targets)):
            yield await queue.get()
        await asyncio.gather(*tasks, return_exceptions=True)

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    async def flush_cache(self):
        await self._cache.flush()

    async def cache_stats(self) -> dict:
        s = await self._cache.stats()
        s["resolve_count"] = self._resolve_count
        s["cache_hits"] = self._cache_hits
        return s

    async def prune_cache(self) -> int:
        return await self._cache.prune_expired()

    def stats(self) -> dict:
        return {
            "total_resolved": self._resolve_count,
            "cache_hits": self._cache_hits,
            "clients": len(self._clients),
        }

    async def bulk_validate(
        self,
        targets: list[str],
    ) -> dict[str, bool]:
        """
        Quick validity check — returns {target: is_valid} without full metadata.
        Uses cache.
        """
        results = await self.resolve_many(targets, use_cache=True)
        return {
            r.raw_input: r.status in (ResolveStatus.OK, ResolveStatus.CACHED)
            for r in results
        }


# ---------------------------------------------------------------------------
# Invite placeholder (for not-yet-joined groups)
# ---------------------------------------------------------------------------

class _InvitePlaceholder:
    """
    Wraps a ChatInvite response as a fake entity so _build_resolved works
    without an actual joined entity.
    """
    def __init__(self, raw: str, inv: ChatInvite):
        self.id = 0
        self.access_hash = 0
        self.title = getattr(inv, "title", "")
        self.username = ""
        self.verified = False
        self.scam = getattr(inv, "scam", False)
        self.restricted = False
        self.broadcast = getattr(inv, "broadcast", False)
        self.megagroup = getattr(inv, "megagroup", False)
        self.gigagroup = False
        self.participants_count = getattr(inv, "participants_count", 0) or 0
        self._raw = raw


# ---------------------------------------------------------------------------
# Normalizer — de-duplicate & clean a list of targets
# ---------------------------------------------------------------------------

class TargetNormalizer:
    """
    Cleans and de-duplicates a list of raw target strings.

    - Strips whitespace, lowercases usernames
    - Removes duplicates (keeps first occurrence)
    - Splits lines from a text block
    - Filters obviously invalid entries
    """

    _MIN_USERNAME_LEN = 4

    @classmethod
    def from_text(cls, text: str) -> list[str]:
        """Parse newline/comma/space-separated targets from raw text."""
        raw = re.split(r"[\n,;\s]+", text)
        return cls.normalize(raw)

    @classmethod
    def normalize(cls, targets: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for t in targets:
            t = t.strip()
            if not t:
                continue
            kind, value = classify_input(t)
            if kind == "unknown":
                continue
            # normalize username
            if kind == "username":
                t = "@" + value.lower()
            key = t.lower()
            if key not in seen:
                seen.add(key)
                out.append(t)
        return out

    @classmethod
    def split_by_kind(
        cls,
        targets: list[str],
    ) -> dict[str, list[str]]:
        """Returns {"invite": [...], "username": [...], "chat_id": [...]}"""
        buckets: dict[str, list[str]] = {
            "invite": [],
            "username": [],
            "chat_id": [],
        }
        for t in targets:
            kind, _ = classify_input(t)
            if kind in buckets:
                buckets[kind].append(t)
        return buckets


# ---------------------------------------------------------------------------
# Entity info formatter
# ---------------------------------------------------------------------------

def format_entity(e: ResolvedEntity) -> str:
    lines = [
        f"  raw       : {e.raw_input}",
        f"  status    : {e.status.value}",
        f"  type      : {e.entity_type.value}",
        f"  id        : {e.entity_id}",
        f"  title     : {e.title!r}",
        f"  username  : @{e.username}" if e.username else "  username  : (none)",
        f"  members   : {e.participants_count:,}",
        f"  verified  : {e.is_verified}  scam={e.is_scam}  restricted={e.is_restricted}",
        f"  about     : {e.about[:80]!r}" if e.about else "  about     : (empty)",
        f"  invite    : {e.invite_link}" if e.invite_link else "  invite    : (none)",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Quick resolver (single client shortcut)
# ---------------------------------------------------------------------------

async def quick_resolve(
    client: TelegramClient,
    target: str,
    *,
    fetch_full: bool = True,
) -> ResolvedEntity:
    """One-shot resolver — no caching, no rotation, just a quick lookup."""
    resolver = ChannelResolver(
        {"_single": client},
        cache_path=None,
        fetch_full=fetch_full,
        batch_concurrency=1,
    )
    return await resolver.resolve(target, use_cache=False)


# ---------------------------------------------------------------------------
# Invite link extractor from multiple formats
# ---------------------------------------------------------------------------

def extract_invite_hashes(text: str) -> list[str]:
    """
    Pull every invite hash from a block of text.
    Handles:  t.me/+HASH  t.me/joinchat/HASH  @username  -100chatid
    Returns list of raw invite strings ready for channel_resolver.
    """
    patterns = [
        r"https?://t\.me/\+([a-zA-Z0-9_-]+)",
        r"https?://t\.me/joinchat/([a-zA-Z0-9_-]+)",
        r"t\.me/\+([a-zA-Z0-9_-]+)",
        r"t\.me/joinchat/([a-zA-Z0-9_-]+)",
    ]
    out: list[str] = []
    seen: set[str] = set()
    for pat in patterns:
        for m in re.finditer(pat, text, re.IGNORECASE):
            full = m.group(0)
            if full not in seen:
                seen.add(full)
                out.append(full)
    return out


def extract_usernames(text: str) -> list[str]:
    """Extract @username references from a text block."""
    out: list[str] = []
    seen: set[str] = set()
    for m in re.finditer(r"@([a-zA-Z][a-zA-Z0-9_]{3,})", text):
        u = "@" + m.group(1).lower()
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


# ---------------------------------------------------------------------------
# Bulk resolve helper
# ---------------------------------------------------------------------------

async def bulk_resolve_from_file(
    path: str | Path,
    clients: dict[str, TelegramClient],
    *,
    cache_path: str | Path | None = "resolver_cache.json",
    fetch_full: bool = False,
    batch_concurrency: int = 5,
) -> list[ResolvedEntity]:
    """
    Read targets from a text file (one per line, any format),
    normalize them, resolve all, return results.
    """
    text = Path(path).read_text(encoding="utf-8")
    targets = TargetNormalizer.from_text(text)
    log.info("bulk_resolve: %d unique targets from %s", len(targets), path)

    resolver = ChannelResolver(
        clients,
        cache_path=cache_path,
        fetch_full=fetch_full,
        batch_concurrency=batch_concurrency,
    )
    results = await resolver.resolve_many(targets)
    await resolver.flush_cache()
    return results


import random   # noqa: E402  (needed inside class method above)