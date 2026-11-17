# reactive/engine.py
# ==============================================================================
#  Reactive automation engine — event → trigger → action.
# ------------------------------------------------------------------------------
#  Turns the suite from "press start" into "set it and forget it". A dedicated
#  listener account watches target chats; when something happens it fires an
#  operation against the whole pool through the campaign OperationRegistry (so
#  every reactive action is gated, logged and metered like any other op).
#
#  Built-in watchers:
#    • watch_new_posts  — new post in a channel → auto views / reactions / stars
#    • watch_keywords   — message text matches a KeywordRuleSet → run ops
#    • watch_mentions   — the listener account is mentioned → run ops / alert
#    • watch_dms        — incoming private message → auto-responder
#
#  Actions are declared as a list of (op_name, params) and reuse the registered
#  operations; {message} is auto-filled with the triggering message id.
# ==============================================================================

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from telethon import events

LogFn = Callable[[str, str], None]


@dataclass
class ReactiveStats:
    triggers_fired: int = 0
    actions_run: int = 0
    last_fired: float = 0.0


class ReactiveEngine:
    """
    Drives reactive triggers off a listener client, dispatching to the
    OperationRegistry against the live account pool.

        eng = ReactiveEngine(listener_client, pool_provider, registry, ctx, log)
        eng.watch_new_posts("@chan", [("views", {}), ("react", {"emoji": "🔥"})])
        eng.watch_keywords(["@grp"], ruleset, [("react", {"emoji": "{emoji}"})])
        await eng.start()
    """

    def __init__(self, listener_client, pool_provider: Callable[[], list],
                 registry, ctx, log: LogFn = None,
                 cooldown_sec: float = 10.0):
        self.client = listener_client
        self.pool_provider = pool_provider
        self.registry = registry
        self.ctx = ctx
        self.log = log or (lambda lvl, msg: None)
        self.cooldown_sec = cooldown_sec
        self.stats = ReactiveStats()
        self._handlers: list = []
        self._locks: "dict[str, asyncio.Lock]" = {}
        self._last_fire: "dict[str, float]" = {}
        self._running = False

    # -- internal dispatch --------------------------------------------------
    async def _run_actions(self, key: str, actions, message_id: Optional[int],
                           extra: Optional[dict] = None) -> None:
        # de-dupe / debounce per trigger key
        now = time.time()
        if now - self._last_fire.get(key, 0) < self.cooldown_sec:
            return
        self._last_fire[key] = now
        lock = self._locks.setdefault(key, asyncio.Lock())
        if lock.locked():
            return
        async with lock:
            self.stats.triggers_fired += 1
            self.stats.last_fired = now
            pool = [a for a in self.pool_provider()
                    if getattr(a, "client", None) and a.healthy()]
            if not pool:
                self.log("warn", f"reactive '{key}': no accounts in pool")
                return
            for op_name, params in actions:
                p = dict(params or {})
                if extra:
                    p = {k: (extra.get(v[1:-1], v) if isinstance(v, str)
                             and v.startswith("{") and v.endswith("}") else v)
                         for k, v in p.items()}
                if message_id is not None and "message" not in p:
                    p["message"] = str(message_id)
                try:
                    op = self.registry.get(op_name)
                except KeyError as e:
                    self.log("err", f"reactive: {e}")
                    continue
                self.log("accent", f"reactive '{key}' → {op_name} {p}")
                try:
                    await op(pool, p, self.ctx)
                    self.stats.actions_run += 1
                except Exception as exc:
                    self.log("err", f"reactive action {op_name} failed: {exc}")

    # -- watchers -----------------------------------------------------------
    def watch_new_posts(self, channel, actions) -> None:
        """New post in `channel` → run actions (message auto-filled)."""
        async def handler(event):
            await self._run_actions(f"newpost:{channel}", actions, event.message.id)
        self.client.add_event_handler(handler, events.NewMessage(chats=channel))
        self._handlers.append(handler)
        self.log("ok", f"watching new posts in {channel}")

    def watch_keywords(self, chats, ruleset, actions) -> None:
        """
        Messages in `chats` whose text matches `ruleset` (emoji_strategy
        KeywordRuleSet) → run actions. The matched emoji is exposed as {emoji}.
        """
        async def handler(event):
            text = event.raw_text or ""
            emoji = ruleset.match(text)
            if emoji is None:
                return
            await self._run_actions(
                f"kw:{event.chat_id}", actions, event.message.id,
                extra={"emoji": emoji})
        self.client.add_event_handler(handler, events.NewMessage(chats=chats))
        self._handlers.append(handler)
        self.log("ok", f"watching keywords in {chats}")

    def watch_mentions(self, actions) -> None:
        """The listener account is mentioned → run actions / alert."""
        async def handler(event):
            if not getattr(event.message, "mentioned", False):
                return
            await self._run_actions(f"mention:{event.chat_id}", actions,
                                    event.message.id)
        self.client.add_event_handler(handler, events.NewMessage(incoming=True))
        self._handlers.append(handler)
        self.log("ok", "watching mentions")

    def watch_dms(self, reply_texts, *, only_new_contacts: bool = False) -> None:
        """Incoming private message → auto-reply from the listener account."""
        import random

        async def handler(event):
            if not event.is_private:
                return
            text = random.choice(reply_texts) if isinstance(reply_texts, list) else reply_texts
            try:
                await event.reply(text)
                self.stats.actions_run += 1
                self.log("ok", f"auto-replied to {event.chat_id}")
            except Exception as exc:
                self.log("err", f"auto-reply failed: {exc}")
        self.client.add_event_handler(handler, events.NewMessage(incoming=True))
        self._handlers.append(handler)
        self.log("ok", "auto-responder active for DMs")

    # -- lifecycle ----------------------------------------------------------
    async def start(self) -> None:
        self._running = True
        self.log("accent", "reactive engine online — listening ...")

    async def run_forever(self) -> None:
        await self.start()
        await self.client.run_until_disconnected()

    def stop(self) -> None:
        for h in self._handlers:
            try:
                self.client.remove_event_handler(h)
            except Exception:
                pass
        self._handlers.clear()
        self._running = False
        self.log("warn", "reactive engine stopped")
