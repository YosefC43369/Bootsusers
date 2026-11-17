# lifecycle/health.py
# ==============================================================================
#  Ban / health detection + automatic quarantine.
# ------------------------------------------------------------------------------
#  Classifies Telethon failures into health signals and decides what to do with
#  the account: a hard ban parks it for good, a spam-block escalates its
#  quarantine (1h → 6h → 24h → 3d), a FloodWait parks it for the wait. State is
#  persisted through the StateStore so a quarantined account stays out of the
#  pool across restarts. Optionally fronts each account with a flood_guard
#  CircuitBreaker when that module is available.
# ==============================================================================

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional

from telethon.errors import (
    FloodWaitError,
    PeerFloodError,
    UserDeactivatedBanError,
    UserDeactivatedError,
    PhoneNumberBannedError,
    AuthKeyUnregisteredError,
    UserBannedInChannelError,
    ChatWriteForbiddenError,
    ChannelPrivateError,
    UserRestrictedError,
)


class HealthSignal(Enum):
    OK          = auto()
    FLOOD       = auto()   # transient rate limit
    SPAM_BLOCK  = auto()   # peer-flood / restricted → escalating cooldown
    HARD_BAN    = auto()   # account dead → permanent quarantine
    ACCESS      = auto()   # per-target access issue, account itself is fine


# escalation ladder (seconds) for repeated spam-blocks
_SPAM_LADDER = [3600, 6 * 3600, 24 * 3600, 3 * 24 * 3600]
_PERMANENT = 365 * 24 * 3600


def classify_error(exc: BaseException) -> "tuple[HealthSignal, float]":
    """Map an exception to (signal, suggested_quarantine_seconds)."""
    if isinstance(exc, (UserDeactivatedBanError, UserDeactivatedError,
                        PhoneNumberBannedError, AuthKeyUnregisteredError)):
        return HealthSignal.HARD_BAN, _PERMANENT
    if isinstance(exc, (PeerFloodError, UserRestrictedError)):
        return HealthSignal.SPAM_BLOCK, _SPAM_LADDER[0]
    if isinstance(exc, FloodWaitError):
        return HealthSignal.FLOOD, float(getattr(exc, "seconds", 60))
    if isinstance(exc, (UserBannedInChannelError, ChatWriteForbiddenError,
                        ChannelPrivateError)):
        return HealthSignal.ACCESS, 0.0
    return HealthSignal.OK, 0.0


@dataclass
class HealthMonitor:
    """
    Applies health policy against a StateStore.

        hm = HealthMonitor(store)
        hm.on_error(acc.phone, exc)        # after a failed op
        hm.on_success(acc.phone)           # after a good op
        if hm.is_available(acc.phone): ...
    """
    store: object
    log: object = None                      # optional (level, msg) callable
    breakers: Optional[dict] = None         # phone -> flood_guard.CircuitBreaker

    def _say(self, level: str, msg: str) -> None:
        if self.log:
            self.log(level, msg)

    def _spam_quarantine_seconds(self, phone: str) -> float:
        # escalate by how many times this account was already quarantined
        key = f"spam_strikes:{phone}"
        strikes = int(self.store.kv_get(key, 0)) if self.store else 0
        secs = _SPAM_LADDER[min(strikes, len(_SPAM_LADDER) - 1)]
        if self.store:
            self.store.kv_set(key, strikes + 1)
        return secs

    def on_error(self, phone: str, exc: BaseException) -> HealthSignal:
        signal, secs = classify_error(exc)
        if signal == HealthSignal.HARD_BAN:
            if self.store:
                self.store.quarantine(phone, secs, f"hard-ban: {type(exc).__name__}")
                self.store.set_status(phone, "banned")
            self._say("err", f"{phone} HARD BAN ({type(exc).__name__}) — quarantined")
        elif signal == HealthSignal.SPAM_BLOCK:
            secs = self._spam_quarantine_seconds(phone)
            if self.store:
                self.store.quarantine(phone, secs, f"spam-block: {type(exc).__name__}")
            self._say("warn", f"{phone} spam-block — quarantined {secs/3600:.0f}h")
        elif signal == HealthSignal.FLOOD:
            if self.store:
                self.store.set_cooldown(phone, "global", secs)
            self._say("warn", f"{phone} floodwait {secs:.0f}s")
        if self.store and signal in (HealthSignal.SPAM_BLOCK, HealthSignal.HARD_BAN):
            self.store.bump_failures(phone)
        if self.breakers is not None and phone in self.breakers:
            try:
                self.breakers[phone].record_failure(
                    force_open=(signal == HealthSignal.HARD_BAN))
            except Exception:
                pass
        return signal

    def on_success(self, phone: str) -> None:
        if self.store:
            self.store.bump_failures(phone, reset=True)
            self.store.kv_set(f"spam_strikes:{phone}", 0)
        if self.breakers is not None and phone in self.breakers:
            try:
                self.breakers[phone].record_success()
            except Exception:
                pass

    def is_available(self, phone: str) -> bool:
        if self.store and self.store.is_quarantined(phone):
            return False
        if self.store and self.store.cooldown_remaining(phone, "global") > 0:
            return False
        if self.breakers is not None and phone in self.breakers:
            try:
                return self.breakers[phone].is_callable()
            except Exception:
                return True
        return True


async def probe_account(client, phone: str = "") -> "tuple[HealthSignal, str]":
    """
    Active health check: get_me() and inspect restriction flags.
    Returns (signal, detail). Non-destructive.
    """
    try:
        me = await client.get_me()
        if me is None:
            return HealthSignal.HARD_BAN, "get_me returned None"
        if getattr(me, "restricted", False):
            reasons = getattr(me, "restriction_reason", None) or []
            txt = ";".join(getattr(r, "reason", "") for r in reasons) or "restricted"
            return HealthSignal.SPAM_BLOCK, txt
        return HealthSignal.OK, "healthy"
    except Exception as exc:
        signal, _ = classify_error(exc)
        return signal, str(exc)
