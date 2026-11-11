# utilities/message_listener.py
# Telethon event listener layer.
# Decouples raw Telethon events from downstream logic by providing a typed
# event bus.  auto_react.py's inline @client.on() handler is replaced by
# a MessageListener that:
#   • normalises raw Telethon events into typed EventPayload objects
#   • routes them to registered handlers (reaction_engine, join_tracker, etc.)
#   • tracks statistics and applies per-event filtering rules
#   • supports graceful start / stop without disconnecting the client
#
# Integration with auto_react.py
# --------------------------------
# from utilities.message_listener import MessageListener, EventKind
# listener = MessageListener(sender_account.client)
# listener.on(EventKind.OUTGOING_MESSAGE, reaction_engine.handle_outgoing)
# await listener.start()
# await sender_account.client.run_until_disconnected()
# await listener.stop()

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from telethon import TelegramClient, events
from telethon.tl.types import (
    Channel,
    Chat,
    Message,
    MessageService,
    User,
)
from telethon.errors import (
    FloodWaitError,
    RPCError,
    UserDeactivatedBanError,
)

logger = logging.getLogger("message_listener")


# ---------------------------------------------------------------------------
# Event kinds
# ---------------------------------------------------------------------------


class EventKind(Enum):
    """
    Canonical set of event types the listener understands.

    Maps to the Telethon event classes the listener subscribes to.
    """

    OUTGOING_MESSAGE = auto()   # a message sent by our own account
    INCOMING_MESSAGE = auto()   # a message received in any chat
    CHANNEL_MESSAGE  = auto()   # a message posted in a channel we watch
    EDITED_MESSAGE   = auto()   # any message was edited
    DELETED_MESSAGE  = auto()   # one or more messages were deleted
    CHAT_ACTION      = auto()   # user joined/left, title changed, etc.
    ALBUM            = auto()   # grouped media (album) received
    ANY              = auto()   # wildcard — receives everything above


# ---------------------------------------------------------------------------
# Typed payloads
# ---------------------------------------------------------------------------


@dataclass
class EventPayload:
    """
    Base class for all normalised event payloads.

    Every payload carries:
    - kind       — which EventKind this is
    - raw        — the original Telethon event object (typed subclass)
    - received_at — monotonic timestamp when the payload was created
    - chat_id    — the numeric chat / channel ID (None for deletions)
    - sender_id  — the numeric user ID who triggered the event (None if unknown)
    """

    kind: EventKind
    raw: Any
    received_at: float = field(default_factory=time.monotonic)
    chat_id: Optional[int] = None
    sender_id: Optional[int] = None

    def age(self) -> float:
        """Seconds since this payload was created."""
        return time.monotonic() - self.received_at


@dataclass
class OutgoingMessagePayload(EventPayload):
    """
    Payload for EventKind.OUTGOING_MESSAGE.

    Emitted whenever the monitored account (the "sender") sends a message.
    This is the primary trigger for reaction_engine.
    """

    message_id: int = 0
    text: str = ""
    is_reply: bool = False
    reply_to_msg_id: Optional[int] = None
    peer_type: str = ""   # "user" | "group" | "channel"


@dataclass
class IncomingMessagePayload(EventPayload):
    """
    Payload for EventKind.INCOMING_MESSAGE and EventKind.CHANNEL_MESSAGE.

    Emitted for every message received in any chat the monitored account
    participates in.  Includes channel posts when channel_id is set.
    """

    message_id: int = 0
    text: str = ""
    is_reply: bool = False
    reply_to_msg_id: Optional[int] = None
    peer_type: str = ""
    channel_id: Optional[int] = None
    grouped_id: Optional[int] = None   # album group


@dataclass
class EditedMessagePayload(EventPayload):
    """Payload for EventKind.EDITED_MESSAGE."""

    message_id: int = 0
    text: str = ""
    edit_date: Optional[float] = None


@dataclass
class DeletedMessagePayload(EventPayload):
    """
    Payload for EventKind.DELETED_MESSAGE.

    message_ids lists all deleted message IDs in the event.
    """

    message_ids: List[int] = field(default_factory=list)


@dataclass
class ChatActionPayload(EventPayload):
    """
    Payload for EventKind.CHAT_ACTION.

    action_type is a human-readable string derived from the MessageService
    action class name, e.g. 'MessageActionChatAddUser'.
    """

    action_type: str = ""
    action_users: List[int] = field(default_factory=list)   # affected user IDs


@dataclass
class AlbumPayload(EventPayload):
    """
    Payload for EventKind.ALBUM.

    messages is the list of Message objects in the album,
    grouped_id is Telegram's album group identifier.
    """

    messages: List[Any] = field(default_factory=list)
    grouped_id: Optional[int] = None


# Union type for handler signatures
AnyPayload = Union[
    OutgoingMessagePayload,
    IncomingMessagePayload,
    EditedMessagePayload,
    DeletedMessagePayload,
    ChatActionPayload,
    AlbumPayload,
    EventPayload,
]

# A handler is any async callable that accepts an AnyPayload
Handler = Callable[[AnyPayload], Awaitable[None]]


# ---------------------------------------------------------------------------
# Filter definitions
# ---------------------------------------------------------------------------


@dataclass
class EventFilter:
    """
    Optional filter applied to events before they reach handlers.

    All specified constraints must be satisfied for the event to pass.
    Unset constraints (None / empty) are ignored.
    """

    allowed_chat_ids: Optional[Set[int]] = None    # whitelist of chat IDs
    blocked_chat_ids: Optional[Set[int]] = None    # blacklist of chat IDs
    allowed_sender_ids: Optional[Set[int]] = None  # whitelist of sender IDs
    blocked_sender_ids: Optional[Set[int]] = None  # blacklist of sender IDs
    min_text_length: int = 0                        # skip messages shorter than this
    max_age_sec: Optional[float] = None             # skip events older than this
    require_text: bool = False                      # skip events with no text

    def matches(self, payload: AnyPayload) -> bool:
        if self.max_age_sec is not None and payload.age() > self.max_age_sec:
            return False

        if self.allowed_chat_ids and payload.chat_id not in self.allowed_chat_ids:
            return False

        if self.blocked_chat_ids and payload.chat_id in self.blocked_chat_ids:
            return False

        if self.allowed_sender_ids and payload.sender_id not in self.allowed_sender_ids:
            return False

        if self.blocked_sender_ids and payload.sender_id in self.blocked_sender_ids:
            return False

        text = getattr(payload, "text", "")
        if self.require_text and not text:
            return False

        if self.min_text_length and len(text) < self.min_text_length:
            return False

        return True


# ---------------------------------------------------------------------------
# Listener statistics
# ---------------------------------------------------------------------------


@dataclass
class ListenerStats:
    """Running counters maintained by the listener."""

    events_received: int = 0
    events_dispatched: int = 0
    events_filtered: int = 0
    handler_errors: int = 0
    flood_waits: int = 0
    reconnects: int = 0
    start_time: float = field(default_factory=time.monotonic)

    def uptime_sec(self) -> float:
        return time.monotonic() - self.start_time

    def to_dict(self) -> dict:
        return {
            "events_received":   self.events_received,
            "events_dispatched": self.events_dispatched,
            "events_filtered":   self.events_filtered,
            "handler_errors":    self.handler_errors,
            "flood_waits":       self.flood_waits,
            "reconnects":        self.reconnects,
            "uptime_sec":        round(self.uptime_sec(), 1),
        }


# ---------------------------------------------------------------------------
# Handler registry entry
# ---------------------------------------------------------------------------


@dataclass
class HandlerEntry:
    kind: EventKind
    handler: Handler
    event_filter: Optional[EventFilter]
    name: str
    priority: int   # lower = called first
    enabled: bool = True


# ---------------------------------------------------------------------------
# MessageListener
# ---------------------------------------------------------------------------


class MessageListener:
    """
    Wraps a Telethon TelegramClient and routes events to registered handlers.

    Lifecycle
    ---------
    1. Create the listener with the connected TelegramClient.
    2. Register handlers with .on() / .on_outgoing() / .on_incoming().
    3. Call await listener.start() to attach Telethon event hooks.
    4. Run the client normally (run_until_disconnected, etc.).
    5. Call await listener.stop() to detach hooks and flush the queue.

    Thread safety
    -------------
    All public methods are safe to call from async code.
    Handler registration / deregistration is thread-safe.
    """

    def __init__(
        self,
        client: TelegramClient,
        *,
        queue_maxsize: int = 1024,
        dispatch_concurrency: int = 8,
        error_handler: Optional[Callable[[Exception, AnyPayload], Awaitable[None]]] = None,
        watch_chat_ids: Optional[Set[int]] = None,
    ) -> None:
        """
        Parameters
        ----------
        client
            A connected (or at least initialised) Telethon TelegramClient.
        queue_maxsize
            Maximum number of events waiting to be dispatched.
            When the queue is full, new events are dropped and logged.
        dispatch_concurrency
            How many handlers may run concurrently per event.
        error_handler
            Optional async callable invoked when a handler raises.
            Signature: (exception, payload) -> None.
        watch_chat_ids
            If provided, CHANNEL_MESSAGE events are only emitted for these
            channel / chat IDs.  None means all chats.
        """
        self._client = client
        self._queue_maxsize = queue_maxsize
        self._dispatch_concurrency = dispatch_concurrency
        self._error_handler = error_handler
        self._watch_chat_ids = watch_chat_ids

        self._handlers: List[HandlerEntry] = []
        self._stats = ListenerStats()

        self._running = False
        self._queue: Optional[asyncio.Queue] = None
        self._dispatch_task: Optional[asyncio.Task] = None
        self._telethon_callbacks: List[Any] = []  # (event_builder, callback) pairs

        # Album buffer: grouped_id → (timestamp, [Message])
        self._album_buffer: Dict[int, Tuple[float, List[Any]]] = {}
        self._album_flush_interval = 1.5   # seconds to wait before emitting an album

    # ------------------------------------------------------------------ #
    # Handler registration
    # ------------------------------------------------------------------ #

    def on(
        self,
        kind: EventKind,
        handler: Handler,
        *,
        event_filter: Optional[EventFilter] = None,
        name: Optional[str] = None,
        priority: int = 100,
    ) -> "MessageListener":
        """
        Register an async handler for a specific EventKind.

        Returns self for method chaining.

        Example
        -------
        listener.on(EventKind.OUTGOING_MESSAGE, reaction_engine.handle_outgoing)
        """
        entry = HandlerEntry(
            kind=kind,
            handler=handler,
            event_filter=event_filter,
            name=name or handler.__name__,
            priority=priority,
        )
        self._handlers.append(entry)
        self._handlers.sort(key=lambda e: e.priority)
        logger.info("Registered handler '%s' for %s (priority=%d)", entry.name, kind, priority)
        return self

    def on_outgoing(
        self,
        handler: Handler,
        *,
        event_filter: Optional[EventFilter] = None,
        name: Optional[str] = None,
        priority: int = 100,
    ) -> "MessageListener":
        """Shortcut for .on(EventKind.OUTGOING_MESSAGE, ...)."""
        return self.on(
            EventKind.OUTGOING_MESSAGE,
            handler,
            event_filter=event_filter,
            name=name,
            priority=priority,
        )

    def on_incoming(
        self,
        handler: Handler,
        *,
        event_filter: Optional[EventFilter] = None,
        name: Optional[str] = None,
        priority: int = 100,
    ) -> "MessageListener":
        """Shortcut for .on(EventKind.INCOMING_MESSAGE, ...)."""
        return self.on(
            EventKind.INCOMING_MESSAGE,
            handler,
            event_filter=event_filter,
            name=name,
            priority=priority,
        )

    def on_channel(
        self,
        handler: Handler,
        *,
        event_filter: Optional[EventFilter] = None,
        name: Optional[str] = None,
        priority: int = 100,
    ) -> "MessageListener":
        """Shortcut for .on(EventKind.CHANNEL_MESSAGE, ...)."""
        return self.on(
            EventKind.CHANNEL_MESSAGE,
            handler,
            event_filter=event_filter,
            name=name,
            priority=priority,
        )

    def on_edit(
        self,
        handler: Handler,
        *,
        event_filter: Optional[EventFilter] = None,
        name: Optional[str] = None,
        priority: int = 100,
    ) -> "MessageListener":
        """Shortcut for .on(EventKind.EDITED_MESSAGE, ...)."""
        return self.on(
            EventKind.EDITED_MESSAGE,
            handler,
            event_filter=event_filter,
            name=name,
            priority=priority,
        )

    def on_delete(
        self,
        handler: Handler,
        *,
        event_filter: Optional[EventFilter] = None,
        name: Optional[str] = None,
        priority: int = 100,
    ) -> "MessageListener":
        """Shortcut for .on(EventKind.DELETED_MESSAGE, ...)."""
        return self.on(
            EventKind.DELETED_MESSAGE,
            handler,
            event_filter=event_filter,
            name=name,
            priority=priority,
        )

    def on_action(
        self,
        handler: Handler,
        *,
        event_filter: Optional[EventFilter] = None,
        name: Optional[str] = None,
        priority: int = 100,
    ) -> "MessageListener":
        """Shortcut for .on(EventKind.CHAT_ACTION, ...)."""
        return self.on(
            EventKind.CHAT_ACTION,
            handler,
            event_filter=event_filter,
            name=name,
            priority=priority,
        )

    def on_album(
        self,
        handler: Handler,
        *,
        event_filter: Optional[EventFilter] = None,
        name: Optional[str] = None,
        priority: int = 100,
    ) -> "MessageListener":
        """Shortcut for .on(EventKind.ALBUM, ...)."""
        return self.on(
            EventKind.ALBUM,
            handler,
            event_filter=event_filter,
            name=name,
            priority=priority,
        )

    def off(self, name: str) -> int:
        """
        Deregister all handlers with the given name.
        Returns the number of handlers removed.
        """
        before = len(self._handlers)
        self._handlers = [e for e in self._handlers if e.name != name]
        removed = before - len(self._handlers)
        if removed:
            logger.info("Deregistered %d handler(s) named '%s'", removed, name)
        return removed

    def set_enabled(self, name: str, enabled: bool) -> int:
        """Enable or disable a handler by name without removing it."""
        count = 0
        for entry in self._handlers:
            if entry.name == name:
                entry.enabled = enabled
                count += 1
        return count

    def list_handlers(self) -> List[dict]:
        return [
            {
                "name":    e.name,
                "kind":    e.kind.name,
                "priority": e.priority,
                "enabled": e.enabled,
            }
            for e in self._handlers
        ]

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """Attach Telethon event hooks and start the dispatch loop."""
        if self._running:
            logger.warning("MessageListener.start() called while already running")
            return

        self._queue = asyncio.Queue(maxsize=self._queue_maxsize)
        self._running = True
        self._stats = ListenerStats()

        # Telethon callback: outgoing messages
        async def _on_outgoing(event: events.NewMessage.Event) -> None:
            await self._handle_new_message(event, outgoing=True)

        # Telethon callback: incoming messages
        async def _on_incoming(event: events.NewMessage.Event) -> None:
            await self._handle_new_message(event, outgoing=False)

        # Telethon callback: edited messages
        async def _on_edit(event: events.MessageEdited.Event) -> None:
            await self._handle_edit(event)

        # Telethon callback: deleted messages
        async def _on_delete(event: events.MessageDeleted.Event) -> None:
            await self._handle_delete(event)

        # Telethon callback: chat actions
        async def _on_action(event: events.ChatAction.Event) -> None:
            await self._handle_action(event)

        # Register with Telethon
        self._client.add_event_handler(_on_outgoing, events.NewMessage(outgoing=True))
        self._client.add_event_handler(_on_incoming, events.NewMessage(incoming=True))
        self._client.add_event_handler(_on_edit,     events.MessageEdited())
        self._client.add_event_handler(_on_delete,   events.MessageDeleted())
        self._client.add_event_handler(_on_action,   events.ChatAction())

        # Store references so we can remove them on stop()
        self._telethon_callbacks = [
            (_on_outgoing, events.NewMessage(outgoing=True)),
            (_on_incoming, events.NewMessage(incoming=True)),
            (_on_edit,     events.MessageEdited()),
            (_on_delete,   events.MessageDeleted()),
            (_on_action,   events.ChatAction()),
        ]

        # Start the async dispatch worker
        self._dispatch_task = asyncio.create_task(
            self._dispatch_loop(), name="message_listener_dispatch"
        )

        logger.info("MessageListener started")

    async def stop(self, timeout: float = 10.0) -> None:
        """Detach hooks, flush the queue, and stop the dispatch loop."""
        if not self._running:
            return

        self._running = False

        # Detach Telethon handlers
        for callback, builder in self._telethon_callbacks:
            try:
                self._client.remove_event_handler(callback, builder)
            except Exception as e:
                logger.debug("Error removing handler: %s", e)
        self._telethon_callbacks.clear()

        # Let the queue drain
        if self._queue:
            try:
                await asyncio.wait_for(self._queue.join(), timeout=timeout)
            except asyncio.TimeoutError:
                remaining = self._queue.qsize()
                if remaining:
                    logger.warning(
                        "MessageListener.stop(): queue not empty after %.1fs "
                        "(%d events dropped)", timeout, remaining
                    )

        # Cancel the dispatch loop
        if self._dispatch_task and not self._dispatch_task.done():
            self._dispatch_task.cancel()
            try:
                await asyncio.wait_for(self._dispatch_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        logger.info("MessageListener stopped — %s", self._stats.to_dict())

    # ------------------------------------------------------------------ #
    # Telethon raw event handlers
    # ------------------------------------------------------------------ #

    async def _handle_new_message(
        self,
        event: events.NewMessage.Event,
        outgoing: bool,
    ) -> None:
        self._stats.events_received += 1
        msg: Message = event.message

        # Album detection: if this message belongs to a media group, buffer it.
        grouped_id = getattr(msg, "grouped_id", None)
        if grouped_id and not outgoing:
            await self._buffer_album_message(msg, event)
            return

        sender_id: Optional[int] = None
        try:
            if msg.sender_id:
                sender_id = msg.sender_id
        except Exception:
            pass

        # Determine peer type
        chat = None
        try:
            chat = await event.get_chat()
        except Exception:
            pass

        peer_type = _classify_peer(chat)

        if outgoing:
            payload = OutgoingMessagePayload(
                kind=EventKind.OUTGOING_MESSAGE,
                raw=event,
                chat_id=event.chat_id,
                sender_id=sender_id,
                message_id=msg.id,
                text=msg.text or "",
                is_reply=bool(msg.reply_to),
                reply_to_msg_id=getattr(msg.reply_to, "reply_to_msg_id", None),
                peer_type=peer_type,
            )
        else:
            is_channel = isinstance(chat, Channel) and getattr(chat, "broadcast", False)
            kind = EventKind.CHANNEL_MESSAGE if is_channel else EventKind.INCOMING_MESSAGE

            # Channel watch filter
            if (
                is_channel
                and self._watch_chat_ids is not None
                and event.chat_id not in self._watch_chat_ids
            ):
                return

            payload = IncomingMessagePayload(
                kind=kind,
                raw=event,
                chat_id=event.chat_id,
                sender_id=sender_id,
                message_id=msg.id,
                text=msg.text or "",
                is_reply=bool(msg.reply_to),
                reply_to_msg_id=getattr(msg.reply_to, "reply_to_msg_id", None),
                peer_type=peer_type,
                channel_id=event.chat_id if is_channel else None,
                grouped_id=grouped_id,
            )

        await self._enqueue(payload)

    async def _handle_edit(self, event: events.MessageEdited.Event) -> None:
        self._stats.events_received += 1
        msg: Message = event.message

        sender_id = getattr(msg, "sender_id", None)
        edit_date_ts: Optional[float] = None
        if msg.edit_date:
            edit_date_ts = msg.edit_date.timestamp()

        payload = EditedMessagePayload(
            kind=EventKind.EDITED_MESSAGE,
            raw=event,
            chat_id=event.chat_id,
            sender_id=sender_id,
            message_id=msg.id,
            text=msg.text or "",
            edit_date=edit_date_ts,
        )
        await self._enqueue(payload)

    async def _handle_delete(self, event: events.MessageDeleted.Event) -> None:
        self._stats.events_received += 1
        payload = DeletedMessagePayload(
            kind=EventKind.DELETED_MESSAGE,
            raw=event,
            chat_id=getattr(event, "chat_id", None),
            sender_id=None,
            message_ids=list(event.deleted_ids),
        )
        await self._enqueue(payload)

    async def _handle_action(self, event: events.ChatAction.Event) -> None:
        self._stats.events_received += 1

        action = None
        try:
            action = getattr(event.action_message, "action", None) if event.action_message else None
        except Exception:
            pass

        action_type = type(action).__name__ if action else "unknown"
        action_users: List[int] = []

        if action and hasattr(action, "users"):
            action_users = list(action.users or [])
        elif action and hasattr(action, "user_id"):
            action_users = [action.user_id]

        sender_id: Optional[int] = None
        try:
            sender_id = event.sender_id
        except Exception:
            pass

        payload = ChatActionPayload(
            kind=EventKind.CHAT_ACTION,
            raw=event,
            chat_id=event.chat_id,
            sender_id=sender_id,
            action_type=action_type,
            action_users=action_users,
        )
        await self._enqueue(payload)

    # ------------------------------------------------------------------ #
    # Album buffering
    # ------------------------------------------------------------------ #

    async def _buffer_album_message(
        self,
        msg: Message,
        event: events.NewMessage.Event,
    ) -> None:
        """
        Buffer incoming album messages.  After _album_flush_interval seconds
        with no new additions, emit a single AlbumPayload.
        """
        grouped_id: int = msg.grouped_id  # type: ignore[assignment]

        if grouped_id not in self._album_buffer:
            self._album_buffer[grouped_id] = (time.monotonic(), [])
            # Schedule a flush
            asyncio.create_task(
                self._flush_album(grouped_id, event.chat_id),
                name=f"album_flush_{grouped_id}",
            )

        _, messages = self._album_buffer[grouped_id]
        messages.append(msg)
        # Update timestamp so the flush sees the latest arrival
        self._album_buffer[grouped_id] = (time.monotonic(), messages)

    async def _flush_album(self, grouped_id: int, chat_id: Optional[int]) -> None:
        await asyncio.sleep(self._album_flush_interval)

        entry = self._album_buffer.pop(grouped_id, None)
        if not entry:
            return

        _, messages = entry
        payload = AlbumPayload(
            kind=EventKind.ALBUM,
            raw=messages,
            chat_id=chat_id,
            sender_id=getattr(messages[0], "sender_id", None) if messages else None,
            messages=messages,
            grouped_id=grouped_id,
        )
        await self._enqueue(payload)

    # ------------------------------------------------------------------ #
    # Queue and dispatch
    # ------------------------------------------------------------------ #

    async def _enqueue(self, payload: AnyPayload) -> None:
        """Put a payload into the dispatch queue.  Drops if full."""
        if self._queue is None:
            return
        try:
            self._queue.put_nowait(payload)
        except asyncio.QueueFull:
            logger.warning(
                "Event queue full (maxsize=%d) — dropping %s",
                self._queue_maxsize, payload.kind.name,
            )

    async def _dispatch_loop(self) -> None:
        """
        Worker that pulls payloads off the queue and dispatches them
        to registered handlers concurrently up to dispatch_concurrency.
        """
        semaphore = asyncio.Semaphore(self._dispatch_concurrency)

        async def dispatch_one(payload: AnyPayload) -> None:
            async with semaphore:
                await self._dispatch(payload)
            if self._queue:
                self._queue.task_done()

        while self._running or (self._queue and not self._queue.empty()):
            try:
                if self._queue is None:
                    break
                payload = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                asyncio.create_task(dispatch_one(payload), name="dispatch_one")
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Dispatch loop error: %s", e)

    async def _dispatch(self, payload: AnyPayload) -> None:
        """Call all matching handlers for a payload."""
        matching = [
            e for e in self._handlers
            if e.enabled and (e.kind == payload.kind or e.kind == EventKind.ANY)
        ]

        if not matching:
            return

        for entry in matching:
            # Apply filter if present
            if entry.event_filter and not entry.event_filter.matches(payload):
                self._stats.events_filtered += 1
                continue

            self._stats.events_dispatched += 1
            try:
                await entry.handler(payload)
            except Exception as exc:
                self._stats.handler_errors += 1
                logger.error(
                    "Handler '%s' raised for %s: %s",
                    entry.name, payload.kind.name, exc,
                    exc_info=True,
                )
                if self._error_handler:
                    try:
                        await self._error_handler(exc, payload)
                    except Exception as inner:
                        logger.error("Error handler itself raised: %s", inner)

    # ------------------------------------------------------------------ #
    # Introspection
    # ------------------------------------------------------------------ #

    def stats(self) -> dict:
        return self._stats.to_dict()

    @property
    def is_running(self) -> bool:
        return self._running


# ---------------------------------------------------------------------------
# MultiClientListener
# ---------------------------------------------------------------------------


class MultiClientListener:
    """
    Manages a MessageListener per TelegramClient.

    Use when you have multiple accounts connected simultaneously and want
    each account's events routed through the same handler registry.

    Usage
    -----
    ml = MultiClientListener()
    ml.add_client(acc1.client, phone=acc1.phone)
    ml.add_client(acc2.client, phone=acc2.phone)
    ml.on(EventKind.OUTGOING_MESSAGE, some_handler)
    await ml.start_all()
    # ... run ...
    await ml.stop_all()
    """

    def __init__(self, **listener_kwargs: Any) -> None:
        self._listeners: Dict[str, MessageListener] = {}
        self._pending_handlers: List[Tuple[EventKind, Handler, dict]] = []
        self._listener_kwargs = listener_kwargs

    def add_client(
        self,
        client: TelegramClient,
        phone: str,
        *,
        watch_chat_ids: Optional[Set[int]] = None,
    ) -> "MessageListener":
        """
        Register a client. Returns the MessageListener created for it.
        Any handlers already registered via self.on() are automatically
        applied to the new listener.
        """
        lst = MessageListener(
            client,
            watch_chat_ids=watch_chat_ids,
            **self._listener_kwargs,
        )
        for kind, handler, kwargs in self._pending_handlers:
            lst.on(kind, handler, **kwargs)
        self._listeners[phone] = lst
        return lst

    def remove_client(self, phone: str) -> None:
        lst = self._listeners.pop(phone, None)
        if lst and lst.is_running:
            asyncio.create_task(lst.stop())

    def on(
        self,
        kind: EventKind,
        handler: Handler,
        **kwargs: Any,
    ) -> "MultiClientListener":
        """
        Register a handler on all existing and future clients.
        Returns self for chaining.
        """
        self._pending_handlers.append((kind, handler, kwargs))
        for lst in self._listeners.values():
            lst.on(kind, handler, **kwargs)
        return self

    async def start_all(self) -> None:
        await asyncio.gather(*[lst.start() for lst in self._listeners.values()])
        logger.info("MultiClientListener: started %d listeners", len(self._listeners))

    async def stop_all(self, timeout: float = 10.0) -> None:
        await asyncio.gather(*[lst.stop(timeout=timeout) for lst in self._listeners.values()])
        logger.info("MultiClientListener: all listeners stopped")

    def stats(self) -> Dict[str, dict]:
        return {phone: lst.stats() for phone, lst in self._listeners.items()}

    def list_handlers(self) -> List[dict]:
        # Same handlers on all listeners, return from first
        if self._listeners:
            first = next(iter(self._listeners.values()))
            return first.list_handlers()
        return []


# ---------------------------------------------------------------------------
# Convenience adapters
# ---------------------------------------------------------------------------


def wrap_auto_react(
    sender_client: TelegramClient,
    *,
    on_outgoing: Handler,
    watch_chat_ids: Optional[Set[int]] = None,
) -> MessageListener:
    """
    Drop-in replacement for the inline @client.on(events.NewMessage(outgoing=True))
    pattern used in auto_react.py.

    Returns a configured MessageListener. Call await listener.start() before
    running the client.

    Example
    -------
    # Old code (auto_react.py):
    @sender.client.on(events.NewMessage(outgoing=True))
    async def on_send(event):
        await react_all(reactors, event.chat_id, event.message.id)

    # New code using the listener:
    async def on_send(payload: OutgoingMessagePayload) -> None:
        await react_all(reactors, payload.chat_id, payload.message_id)

    listener = wrap_auto_react(sender.client, on_outgoing=on_send)
    await listener.start()
    await sender.client.run_until_disconnected()
    await listener.stop()
    """
    listener = MessageListener(sender_client, watch_chat_ids=watch_chat_ids)
    listener.on_outgoing(on_outgoing, name="auto_react_outgoing")
    return listener


def make_reaction_handler(
    react_all_fn: Callable[[int, int], Awaitable[None]],
) -> Handler:
    """
    Build a typed handler that calls the react_all function from auto_react.py.

    react_all_fn should have signature: (chat_id, msg_id) -> Awaitable[None].

    Usage
    -----
    handler = make_reaction_handler(lambda cid, mid: react_all(reactors, cid, mid))
    listener.on_outgoing(handler, name="reaction_trigger")
    """
    async def _handler(payload: AnyPayload) -> None:
        if not isinstance(payload, OutgoingMessagePayload):
            return
        logger.debug(
            "Outgoing message %d in %s — triggering reactions",
            payload.message_id, payload.chat_id,
        )
        await react_all_fn(payload.chat_id, payload.message_id)

    _handler.__name__ = "reaction_handler"
    return _handler


# ---------------------------------------------------------------------------
# Peer type classifier
# ---------------------------------------------------------------------------


def _classify_peer(chat: Any) -> str:
    """Return 'user' | 'group' | 'channel' | 'unknown'."""
    if chat is None:
        return "unknown"
    if isinstance(chat, User):
        return "user"
    if isinstance(chat, Channel):
        return "channel"
    if isinstance(chat, Chat):
        return "group"
    # Fallback: check by class name (handles InputPeer variants)
    name = type(chat).__name__.lower()
    if "user" in name:
        return "user"
    if "channel" in name:
        return "channel"
    if "chat" in name:
        return "group"
    return "unknown"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


__all__ = [
    # Enums
    "EventKind",
    # Payloads
    "EventPayload",
    "OutgoingMessagePayload",
    "IncomingMessagePayload",
    "EditedMessagePayload",
    "DeletedMessagePayload",
    "ChatActionPayload",
    "AlbumPayload",
    # Filter
    "EventFilter",
    # Listener
    "MessageListener",
    "MultiClientListener",
    # Stats
    "ListenerStats",
    # Adapters
    "wrap_auto_react",
    "make_reaction_handler",
    # Utils
    "_classify_peer",
]