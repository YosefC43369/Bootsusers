# lifecycle/profile.py
# ==============================================================================
#  Per-account profile randomization — names, bios, avatars.
# ------------------------------------------------------------------------------
#  Fresh accounts that all share the default empty profile are trivially
#  clusterable. This gives each account a stable, human-looking identity:
#  first/last name, an optional bio, and an optional avatar pulled from a folder
#  of images. Identity is seeded by the phone number so it stays consistent
#  across runs (re-rolling a profile every session is itself a fingerprint).
# ==============================================================================

from __future__ import annotations

import os
import random
from dataclasses import dataclass
from typing import Optional

from telethon.tl.functions.account import UpdateProfileRequest

FIRST_NAMES = [
    "Alex", "Sam", "Jordan", "Taylor", "Casey", "Riley", "Morgan", "Jamie",
    "Chris", "Dana", "Lee", "Robin", "Drew", "Quinn", "Avery", "Parker",
    "Noah", "Liam", "Emma", "Olivia", "Mia", "Leo", "Max", "Nina",
    "Ivan", "Dmitri", "Sergei", "Anna", "Elena", "Pavel", "Marco", "Luca",
]
LAST_NAMES = [
    "Gray", "Stone", "Vale", "Kane", "Frost", "Reed", "Cross", "Hale",
    "Wolfe", "Blake", "Pike", "Fox", "Lane", "Ward", "Cole", "Finch",
    "Volkov", "Orlov", "Rossi", "Bauer", "Novak", "Marek", "Costa", "Dubois",
]
BIO_TEMPLATES = [
    "", "", "",                       # many real users leave it blank
    "coffee · code · music",
    "just here to read",
    "📍 somewhere",
    "building things",
    "traveler ✈️",
    "night owl 🌙",
    "less talk, more do",
    "crypto & chill",
    "photography enthusiast 📷",
]


@dataclass
class ProfileIdentity:
    first_name: str
    last_name: str
    about: str

    def __str__(self) -> str:
        return f"{self.first_name} {self.last_name} | {self.about!r}"


def identity_for(phone: str, *, stable: bool = True) -> ProfileIdentity:
    """
    Generate an identity. When stable=True it is deterministic per phone, so the
    same account always gets the same name/bio.
    """
    rng = random.Random(phone) if stable else random.Random()
    return ProfileIdentity(
        first_name=rng.choice(FIRST_NAMES),
        last_name=rng.choice(LAST_NAMES),
        about=rng.choice(BIO_TEMPLATES),
    )


def _pick_avatar(avatar_dir: str, phone: str, stable: bool = True) -> Optional[str]:
    try:
        imgs = [os.path.join(avatar_dir, f) for f in sorted(os.listdir(avatar_dir))
                if f.lower().endswith((".jpg", ".jpeg", ".png", ".webp"))]
    except Exception:
        return None
    if not imgs:
        return None
    rng = random.Random(phone) if stable else random.Random()
    return rng.choice(imgs)


async def randomize_profile(acc, *, avatar_dir: Optional[str] = None,
                            stable: bool = True, set_avatar: bool = True,
                            log=None) -> ProfileIdentity:
    """
    Apply a randomized identity to `acc.client`. Returns the identity used.
    `avatar_dir` (optional) is a folder of images; one is chosen and uploaded.
    """
    ident = identity_for(acc.phone, stable=stable)
    try:
        await acc.client(UpdateProfileRequest(
            first_name=ident.first_name,
            last_name=ident.last_name,
            about=ident.about,
        ))
        if log:
            log("ok", f"{acc.phone} profile → {ident.first_name} {ident.last_name}")
    except Exception as exc:
        if log:
            log("err", f"{acc.phone} profile update failed: {exc}")
        return ident

    if set_avatar and avatar_dir:
        path = _pick_avatar(avatar_dir, acc.phone, stable)
        if path:
            try:
                uploaded = await acc.client.upload_file(path)
                from telethon.tl.functions.photos import UploadProfilePhotoRequest
                await acc.client(UploadProfilePhotoRequest(file=uploaded))
                if log:
                    log("ok", f"{acc.phone} avatar set ({os.path.basename(path)})")
            except Exception as exc:
                if log:
                    log("warn", f"{acc.phone} avatar upload failed: {exc}")
    return ident
