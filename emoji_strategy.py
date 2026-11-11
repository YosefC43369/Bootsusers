# emoji_strategy.py, target: Windows 11
#
# Wiring (in reaction_engine.py _send_one or launcher.py):
#   from emoji_strategy import EmojiSelector, UserEmojiProfile, KeywordRuleSet
#   selector = EmojiSelector(rules=KeywordRuleSet.default(), default_pool=REACTION_POOL)
#   emoji = selector.pick(
#               phone=acc.phone, chat_id=chat_id,
#               text=message_text, sender_id=sender_id)

from __future__ import annotations

import json
import logging
import math
import random
import re
import threading
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from itertools import cycle
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── default reaction pool (fallback) ─────────────────────────────────────────
DEFAULT_POOL: list[str] = [
    "👍", "❤️", "🔥", "🎉", "👏", "😍", "🤩", "💯", "⚡", "✨"
]


# ══════════════════════════════════════════════════════════════════════════════
#  SELECTION MODE
# ══════════════════════════════════════════════════════════════════════════════

class SelectionMode(str, Enum):
    ROUND_ROBIN      = "round_robin"       # cycle in order, random start
    WEIGHTED_RANDOM  = "weighted_random"   # weighted probability draw
    PURE_RANDOM      = "pure_random"       # uniform random, no repeat prevention
    CONTEXT_AWARE    = "context_aware"     # keyword → emoji map, falls back to weighted
    USER_PROFILE     = "user_profile"      # sender's preferred emoji, falls back to context
    ADAPTIVE         = "adaptive"          # learns from which emojis get most engagement


# ══════════════════════════════════════════════════════════════════════════════
#  WEIGHTED POOL ENTRY
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class WeightedEmoji:
    emoji:  str
    weight: float = 1.0

    @staticmethod
    def from_pool(pool: list[str], uniform_weight: float = 1.0) -> list["WeightedEmoji"]:
        return [WeightedEmoji(e, uniform_weight) for e in pool]

    @staticmethod
    def from_dict(d: dict) -> "WeightedEmoji":
        return WeightedEmoji(emoji=d["emoji"], weight=float(d.get("weight", 1.0)))

    def to_dict(self) -> dict:
        return {"emoji": self.emoji, "weight": self.weight}


def _weighted_choice(options: list[WeightedEmoji]) -> str:
    """Draw one emoji by weight; uniform if all weights equal."""
    if not options:
        return "👍"
    total  = sum(o.weight for o in options)
    target = random.uniform(0, total)
    acc    = 0.0
    for o in options:
        acc += o.weight
        if acc >= target:
            return o.emoji
    return options[-1].emoji


# ══════════════════════════════════════════════════════════════════════════════
#  KEYWORD RULE
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class KeywordRule:
    """
    If any pattern in `keywords` matches the message text (case-insensitive),
    pick from `emojis` using weighted random.
    priority: higher = checked first.
    """
    name:     str
    keywords: list[str]                      # plain strings or regex patterns
    emojis:   list[WeightedEmoji]
    priority: int   = 0
    use_regex: bool = False                  # True = treat keywords as regex

    def matches(self, text: str) -> bool:
        t = text.lower()
        for kw in self.keywords:
            if self.use_regex:
                if re.search(kw, t, re.IGNORECASE):
                    return True
            else:
                if kw.lower() in t:
                    return True
        return False

    def pick(self) -> str:
        return _weighted_choice(self.emojis)

    def to_dict(self) -> dict:
        return {
            "name":      self.name,
            "keywords":  self.keywords,
            "emojis":    [e.to_dict() for e in self.emojis],
            "priority":  self.priority,
            "use_regex": self.use_regex,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "KeywordRule":
        return cls(
            name      = d["name"],
            keywords  = d["keywords"],
            emojis    = [WeightedEmoji.from_dict(e) for e in d["emojis"]],
            priority  = int(d.get("priority", 0)),
            use_regex = bool(d.get("use_regex", False)),
        )


# ══════════════════════════════════════════════════════════════════════════════
#  KEYWORD RULE SET
# ══════════════════════════════════════════════════════════════════════════════

class KeywordRuleSet:
    """
    Ordered collection of KeywordRule objects.
    Rules are evaluated in descending priority order.
    """

    def __init__(self, rules: Optional[list[KeywordRule]] = None):
        self._rules: list[KeywordRule] = sorted(
            rules or [], key=lambda r: r.priority, reverse=True
        )
        self._lock = threading.Lock()

    def add(self, rule: KeywordRule) -> None:
        with self._lock:
            self._rules.append(rule)
            self._rules.sort(key=lambda r: r.priority, reverse=True)

    def remove(self, name: str) -> bool:
        with self._lock:
            before = len(self._rules)
            self._rules = [r for r in self._rules if r.name != name]
            return len(self._rules) < before

    def match(self, text: str) -> Optional[str]:
        """Return the emoji picked by the first matching rule, or None."""
        with self._lock:
            rules = list(self._rules)
        for rule in rules:
            if rule.matches(text):
                return rule.pick()
        return None

    def to_list(self) -> list[dict]:
        with self._lock:
            return [r.to_dict() for r in self._rules]

    @classmethod
    def from_list(cls, data: list[dict]) -> "KeywordRuleSet":
        return cls([KeywordRule.from_dict(d) for d in data])

    @classmethod
    def default(cls) -> "KeywordRuleSet":
        """
        Built-in sensible English + Thai keyword rules covering common
        message sentiments.  Add your own on top.
        """
        rules = [
            KeywordRule(
                name     = "love",
                keywords = ["love", "lov", "❤", "heart", "รัก", "ชอบ"],
                emojis   = [
                    WeightedEmoji("❤️",  3.0),
                    WeightedEmoji("😍",  2.0),
                    WeightedEmoji("🥰",  1.5),
                    WeightedEmoji("💕",  1.0),
                ],
                priority = 10,
            ),
            KeywordRule(
                name     = "fire_hype",
                keywords = ["fire", "🔥", "lit", "banger", "เด็ด", "แจ่ม", "โคตร"],
                emojis   = [
                    WeightedEmoji("🔥",  3.0),
                    WeightedEmoji("💯",  2.0),
                    WeightedEmoji("⚡",  1.5),
                    WeightedEmoji("🤩",  1.0),
                ],
                priority = 9,
            ),
            KeywordRule(
                name     = "congratulations",
                keywords = ["congrat", "well done", "ยินดี", "เก่ง", "สุด", "win", "won"],
                emojis   = [
                    WeightedEmoji("🎉",  3.0),
                    WeightedEmoji("👏",  2.5),
                    WeightedEmoji("🥳",  1.5),
                    WeightedEmoji("🏆",  1.0),
                ],
                priority = 8,
            ),
            KeywordRule(
                name     = "funny",
                keywords = ["lol", "lmao", "haha", "😂", "555", "ฮา", "ตลก"],
                emojis   = [
                    WeightedEmoji("😂",  3.0),
                    WeightedEmoji("🤣",  2.5),
                    WeightedEmoji("😹",  1.0),
                ],
                priority = 7,
            ),
            KeywordRule(
                name     = "sad",
                keywords = ["sad", "cry", "😢", "เศร้า", "ร้องไห้", "miss"],
                emojis   = [
                    WeightedEmoji("🥺",  2.5),
                    WeightedEmoji("😢",  2.0),
                    WeightedEmoji("💔",  1.5),
                    WeightedEmoji("🤍",  1.0),
                ],
                priority = 7,
            ),
            KeywordRule(
                name     = "anger",
                keywords = ["angry", "mad", "โกรธ", "เดือด", "ทำไม", "wtf"],
                emojis   = [
                    WeightedEmoji("😤",  2.0),
                    WeightedEmoji("💢",  2.0),
                    WeightedEmoji("🤬",  1.5),
                ],
                priority = 6,
            ),
            KeywordRule(
                name     = "shock_surprise",
                keywords = ["wow", "omg", "เหรอ", "จริงๆ", "shock", "นี่มัน", "เฮ้ย"],
                emojis   = [
                    WeightedEmoji("😮",  2.5),
                    WeightedEmoji("🤯",  2.0),
                    WeightedEmoji("😱",  1.5),
                    WeightedEmoji("👀",  1.0),
                ],
                priority = 6,
            ),
            KeywordRule(
                name     = "cool",
                keywords = ["cool", "nice", "ดี", "เจ๋ง", "โอเค", "ok", "good"],
                emojis   = [
                    WeightedEmoji("👍",  3.0),
                    WeightedEmoji("✨",  2.0),
                    WeightedEmoji("💯",  1.5),
                    WeightedEmoji("😎",  1.0),
                ],
                priority = 5,
            ),
            KeywordRule(
                name     = "food",
                keywords = ["food", "eat", "กิน", "อาหาร", "อร่อย", "yummy", "delicious"],
                emojis   = [
                    WeightedEmoji("😋",  3.0),
                    WeightedEmoji("🤤",  2.0),
                    WeightedEmoji("🍔",  1.0),
                    WeightedEmoji("❤️",  0.5),
                ],
                priority = 5,
            ),
            KeywordRule(
                name     = "money_crypto",
                keywords = ["money", "profit", "pump", "moon", "เงิน", "กำไร", "btc", "crypto"],
                emojis   = [
                    WeightedEmoji("🚀",  3.0),
                    WeightedEmoji("💰",  2.5),
                    WeightedEmoji("🔥",  2.0),
                    WeightedEmoji("💎",  1.5),
                    WeightedEmoji("📈",  1.0),
                ],
                priority = 8,
            ),
            KeywordRule(
                name     = "question",
                keywords = ["?", "คำถาม", "ถาม", "ทำไม", "อะไร", "ไหน", "ยังไง"],
                emojis   = [
                    WeightedEmoji("🤔",  3.0),
                    WeightedEmoji("👀",  1.5),
                    WeightedEmoji("💡",  1.0),
                ],
                priority = 3,
            ),
            KeywordRule(
                name     = "news_announcement",
                keywords = ["news", "announce", "ประกาศ", "แจ้ง", "update", "breaking"],
                emojis   = [
                    WeightedEmoji("👀",  2.5),
                    WeightedEmoji("📢",  2.0),
                    WeightedEmoji("🔔",  1.5),
                    WeightedEmoji("💡",  1.0),
                ],
                priority = 4,
            ),
        ]
        return cls(rules)


# ══════════════════════════════════════════════════════════════════════════════
#  PER-USER EMOJI PROFILE
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class UserEmojiProfile:
    """
    Records what emojis a specific Telegram user (sender_id) tends to use
    or respond well to.  Updated via observe() when we see their messages.
    """
    user_id:     int
    emoji_freq:  dict[str, float] = field(default_factory=dict)  # emoji → weight
    last_seen:   float            = field(default_factory=time.time)
    sample_count: int             = 0

    _DECAY_DAYS: float = 30.0  # weights decay toward 1.0 over 30 days

    def observe(self, text: str) -> None:
        """Extract emojis from a message and increment their weight."""
        found = _extract_emojis(text)
        for e in found:
            self.emoji_freq[e] = self.emoji_freq.get(e, 1.0) + 1.0
        self.sample_count += 1
        self.last_seen     = time.time()

    def decay(self) -> None:
        """Soft-decay weights toward 1.0 based on elapsed days."""
        days = (time.time() - self.last_seen) / 86400
        if days < 1.0:
            return
        factor = max(0.5, 1.0 - (days / self._DECAY_DAYS) * 0.5)
        for e in self.emoji_freq:
            self.emoji_freq[e] = 1.0 + (self.emoji_freq[e] - 1.0) * factor

    def preferred_pool(self, top_n: int = 5) -> list[WeightedEmoji]:
        """Return top-N emojis by weight as a weighted pool."""
        if not self.emoji_freq:
            return []
        ranked = sorted(self.emoji_freq.items(), key=lambda kv: kv[1], reverse=True)
        return [WeightedEmoji(e, w) for e, w in ranked[:top_n]]

    def to_dict(self) -> dict:
        return {
            "user_id":     self.user_id,
            "emoji_freq":  self.emoji_freq,
            "last_seen":   self.last_seen,
            "sample_count": self.sample_count,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "UserEmojiProfile":
        p              = cls(user_id=int(d["user_id"]))
        p.emoji_freq   = {k: float(v) for k, v in d.get("emoji_freq", {}).items()}
        p.last_seen    = float(d.get("last_seen", time.time()))
        p.sample_count = int(d.get("sample_count", 0))
        return p


def _extract_emojis(text: str) -> list[str]:
    """
    Pull emoji characters out of a string.
    Covers most Unicode emoji ranges; not exhaustive but fast.
    """
    emoji_pattern = re.compile(
        "["
        "\U0001F300-\U0001F9FF"   # misc symbols, emoticons
        "\U0001FA00-\U0001FA6F"   # chess pieces, etc.
        "\U0001FA70-\U0001FAFF"   # symbols and pictographs
        "\U00002702-\U000027B0"   # dingbats
        "\U000024C2-\U0001F251"   # enclosed chars
        "]+",
        flags=re.UNICODE,
    )
    return emoji_pattern.findall(text)


# ══════════════════════════════════════════════════════════════════════════════
#  USER PROFILE REGISTRY
# ══════════════════════════════════════════════════════════════════════════════

class UserProfileRegistry:
    """
    JSON-backed store of UserEmojiProfile keyed by Telegram user_id.
    Thread-safe; mtime-checked reload.
    """

    def __init__(self, path: str = "user_profiles.json"):
        self._path:  Path = Path(path)
        self._lock:  threading.Lock = threading.Lock()
        self._data:  dict[int, UserEmojiProfile] = {}
        self._mtime: float = 0.0
        self.load()

    def load(self) -> None:
        if not self._path.exists():
            return
        try:
            mtime = self._path.stat().st_mtime
            raw   = json.loads(self._path.read_text(encoding="utf-8"))
            with self._lock:
                self._data = {
                    int(k): UserEmojiProfile.from_dict(v)
                    for k, v in raw.items()
                }
                self._mtime = mtime
        except Exception as e:
            logger.warning("UserProfileRegistry.load: %s", e)

    def reload(self) -> bool:
        if not self._path.exists():
            return False
        try:
            if self._path.stat().st_mtime != self._mtime:
                self.load()
                return True
        except OSError:
            pass
        return False

    def save(self) -> None:
        with self._lock:
            data = {str(k): v.to_dict() for k, v in self._data.items()}
        self._path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        try:
            self._mtime = self._path.stat().st_mtime
        except OSError:
            pass

    def get(self, user_id: int) -> Optional[UserEmojiProfile]:
        with self._lock:
            return self._data.get(user_id)

    def observe(self, user_id: int, text: str) -> None:
        with self._lock:
            if user_id not in self._data:
                self._data[user_id] = UserEmojiProfile(user_id=user_id)
            self._data[user_id].observe(text)

    def decay_all(self) -> None:
        with self._lock:
            for p in self._data.values():
                p.decay()

    def prune(self, max_days: float = 90.0) -> int:
        cutoff = time.time() - max_days * 86400
        with self._lock:
            before = len(self._data)
            self._data = {
                uid: p for uid, p in self._data.items()
                if p.last_seen >= cutoff
            }
            return before - len(self._data)


# ══════════════════════════════════════════════════════════════════════════════
#  ADAPTIVE TRACKER  (learns which emojis get engagement)
# ══════════════════════════════════════════════════════════════════════════════

class AdaptiveTracker:
    """
    Tracks send counts and estimated engagement per emoji per chat.
    engagement_signal() should be called when you detect a reply/reaction
    to a message you reacted to, as a proxy for "that emoji landed well."
    """

    def __init__(self):
        self._lock = threading.Lock()
        # {chat_id: {emoji: {"sent": n, "engaged": n}}}
        self._data: dict[int, dict[str, dict[str, int]]] = {}

    def record_send(self, chat_id: int, emoji: str) -> None:
        with self._lock:
            c = self._data.setdefault(chat_id, {})
            e = c.setdefault(emoji, {"sent": 0, "engaged": 0})
            e["sent"] += 1

    def engagement_signal(self, chat_id: int, emoji: str, weight: float = 1.0) -> None:
        """Call this when engagement is detected on a message we reacted to."""
        with self._lock:
            c = self._data.setdefault(chat_id, {})
            e = c.setdefault(emoji, {"sent": 0, "engaged": 0})
            e["engaged"] += max(0, weight)

    def effective_weights(
        self,
        chat_id:   int,
        base_pool: list[WeightedEmoji],
        alpha:     float = 0.3,            # blend: 0=base only, 1=adaptive only
    ) -> list[WeightedEmoji]:
        """
        Return the base pool with weights blended toward adaptive engagement rates.
        Emojis not yet tracked default to base weight.
        """
        with self._lock:
            stats = self._data.get(chat_id, {})

        # compute engagement rate per emoji
        result = []
        for we in base_pool:
            s = stats.get(we.emoji, {})
            sent    = s.get("sent", 0)
            engaged = s.get("engaged", 0.0)
            if sent >= 5:                  # need enough data points
                rate        = engaged / sent
                # scale to weight range [0.1, 5.0]
                adapt_w     = 0.1 + rate * 4.9
                blended     = (1.0 - alpha) * we.weight + alpha * adapt_w
            else:
                blended = we.weight
            result.append(WeightedEmoji(we.emoji, blended))

        return result

    def top_emojis(self, chat_id: int, n: int = 5) -> list[tuple[str, float]]:
        """Return (emoji, engagement_rate) sorted by rate descending."""
        with self._lock:
            stats = self._data.get(chat_id, {})
        scored = []
        for emoji, s in stats.items():
            sent = s.get("sent", 0)
            if sent == 0:
                continue
            scored.append((emoji, s.get("engaged", 0.0) / sent))
        return sorted(scored, key=lambda x: x[1], reverse=True)[:n]


# ══════════════════════════════════════════════════════════════════════════════
#  ROUND-ROBIN STATE  (per-account per-chat)
# ══════════════════════════════════════════════════════════════════════════════

class RoundRobinState:
    """
    Per-(phone, chat_id) cycling iterators with random starting offset.
    Distinct from EmojiCycler in reaction_engine to avoid coupling.
    """

    def __init__(self):
        self._iters: dict[tuple, cycle] = {}
        self._lock   = threading.Lock()

    def _make(self, pool: list[str]) -> cycle:
        p = list(pool)
        random.shuffle(p)       # random start
        return cycle(p)

    def next(self, phone: str, chat_id: int, pool: list[str]) -> str:
        key = (phone, chat_id)
        with self._lock:
            if key not in self._iters or not pool:
                self._iters[key] = self._make(pool or DEFAULT_POOL)
            return next(self._iters[key])

    def reset(self, phone: str, chat_id: int) -> None:
        with self._lock:
            self._iters.pop((phone, chat_id), None)


# ══════════════════════════════════════════════════════════════════════════════
#  EMOJI SELECTOR  — main entry point
# ══════════════════════════════════════════════════════════════════════════════

class EmojiSelector:
    """
    Unified emoji selection pipeline.

    Priority chain (each can fall through to the next):
      1. User profile (sender's preferred emoji)        — SelectionMode.USER_PROFILE
      2. Context / keyword match                        — SelectionMode.CONTEXT_AWARE
      3. Adaptive weighted pick                         — SelectionMode.ADAPTIVE
      4. Weighted random from pool                      — SelectionMode.WEIGHTED_RANDOM
      5. Round-robin from pool                          — SelectionMode.ROUND_ROBIN
      6. Pure random                                    — SelectionMode.PURE_RANDOM

    Default mode is CONTEXT_AWARE → falls back to WEIGHTED_RANDOM.
    Pass mode=SelectionMode.USER_PROFILE for the full chain.
    """

    def __init__(
        self,
        default_pool:    Optional[list[str]] = None,
        rules:           Optional[KeywordRuleSet] = None,
        user_profiles:   Optional[UserProfileRegistry] = None,
        adaptive:        Optional[AdaptiveTracker] = None,
        mode:            SelectionMode = SelectionMode.CONTEXT_AWARE,
        weighted_pool:   Optional[list[WeightedEmoji]] = None,
        adaptive_alpha:  float = 0.3,
    ):
        self._pool         = default_pool or list(DEFAULT_POOL)
        self._rules        = rules or KeywordRuleSet.default()
        self._profiles     = user_profiles or UserProfileRegistry.__new__(
                                UserProfileRegistry)
        self._adaptive     = adaptive or AdaptiveTracker()
        self._mode         = mode
        self._rr           = RoundRobinState()
        self._alpha        = adaptive_alpha

        # weighted pool: if not supplied, build uniform weights from pool
        self._weighted: list[WeightedEmoji] = weighted_pool or [
            WeightedEmoji(e, 1.0) for e in self._pool
        ]

    # ── main pick ─────────────────────────────────────────────────────────────

    def pick(
        self,
        phone:     str                  = "",
        chat_id:   int                  = 0,
        text:      str                  = "",
        sender_id: Optional[int]        = None,
        pool:      Optional[list[str]]  = None,
    ) -> str:
        """
        Select the best emoji for this context.
        pool overrides the default pool for this call only.
        """
        base_pool = [WeightedEmoji(e, 1.0) for e in (pool or self._pool)]

        # ── 1. user profile ──────────────────────────────────────────────────
        if self._mode == SelectionMode.USER_PROFILE and sender_id is not None:
            profile = None
            try:
                profile = self._profiles.get(sender_id)
            except Exception:
                pass
            if profile:
                preferred = profile.preferred_pool(top_n=5)
                if preferred:
                    emoji = _weighted_choice(preferred)
                    self._adaptive.record_send(chat_id, emoji)
                    return emoji

        # ── 2. context / keyword ─────────────────────────────────────────────
        if self._mode in (
            SelectionMode.USER_PROFILE,
            SelectionMode.CONTEXT_AWARE,
            SelectionMode.ADAPTIVE,
        ) and text:
            match = self._rules.match(text)
            if match:
                self._adaptive.record_send(chat_id, match)
                return match

        # ── 3. adaptive weighted ─────────────────────────────────────────────
        if self._mode in (SelectionMode.USER_PROFILE, SelectionMode.ADAPTIVE):
            aw = self._adaptive.effective_weights(chat_id, base_pool, self._alpha)
            if aw:
                emoji = _weighted_choice(aw)
                self._adaptive.record_send(chat_id, emoji)
                return emoji

        # ── 4. weighted random ───────────────────────────────────────────────
        if self._mode in (
            SelectionMode.USER_PROFILE,
            SelectionMode.CONTEXT_AWARE,
            SelectionMode.ADAPTIVE,
            SelectionMode.WEIGHTED_RANDOM,
        ):
            emoji = _weighted_choice(self._weighted if not pool else base_pool)
            self._adaptive.record_send(chat_id, emoji)
            return emoji

        # ── 5. round-robin ───────────────────────────────────────────────────
        if self._mode == SelectionMode.ROUND_ROBIN:
            emoji = self._rr.next(phone, chat_id, pool or self._pool)
            self._adaptive.record_send(chat_id, emoji)
            return emoji

        # ── 6. pure random (fallback) ────────────────────────────────────────
        emoji = random.choice(pool or self._pool)
        self._adaptive.record_send(chat_id, emoji)
        return emoji

    # ── feedback ──────────────────────────────────────────────────────────────

    def record_engagement(
        self,
        chat_id: int,
        emoji:   str,
        weight:  float = 1.0,
    ) -> None:
        """
        Signal that a reaction landed well.
        Call this if you can detect replies or further reactions to a message
        you already reacted to.
        """
        self._adaptive.engagement_signal(chat_id, emoji, weight)

    def observe_sender(self, sender_id: int, text: str) -> None:
        """
        Feed a user's own message text so the profile learns their emoji habits.
        Call this in your NewMessage handler for all messages (not just ones
        you react to).
        """
        try:
            self._profiles.observe(sender_id, text)
        except Exception as e:
            logger.debug("observe_sender: %s", e)

    # ── introspection ─────────────────────────────────────────────────────────

    def top_for_chat(self, chat_id: int, n: int = 5) -> list[tuple[str, float]]:
        return self._adaptive.top_emojis(chat_id, n)

    def test_text(self, text: str) -> str:
        """Dry-run: show which emoji a given text would pick."""
        match = self._rules.match(text)
        return match if match else "(no keyword match — would use weighted random)"

    def print_rule_matches(self, text: str) -> None:
        sep = "─" * 48
        print(sep)
        print(f"  Text: {text!r}")
        print(sep)
        with self._rules._lock:
            rules = list(self._rules._rules)
        for rule in rules:
            if rule.matches(text):
                print(f"  ✓ Rule [{rule.priority}] {rule.name!r}  →  {rule.pick()}")
            else:
                print(f"  · Rule [{rule.priority}] {rule.name!r}")
        print(sep)


# ══════════════════════════════════════════════════════════════════════════════
#  QUICK-CONFIG HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def make_selector(
    pool:  Optional[list[str]] = None,
    mode:  SelectionMode       = SelectionMode.CONTEXT_AWARE,
    extra_rules: Optional[list[KeywordRule]] = None,
) -> EmojiSelector:
    """
    One-liner factory.
    make_selector(pool=["👍","❤️","🔥"], mode=SelectionMode.WEIGHTED_RANDOM)
    """
    rules = KeywordRuleSet.default()
    if extra_rules:
        for r in extra_rules:
            rules.add(r)

    weighted = [WeightedEmoji(e, 1.0) for e in (pool or DEFAULT_POOL)]
    return EmojiSelector(
        default_pool  = pool or DEFAULT_POOL,
        rules         = rules,
        mode          = mode,
        weighted_pool = weighted,
    )