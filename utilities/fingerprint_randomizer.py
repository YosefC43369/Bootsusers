# lang: Python 3.11+, file: utilities/fingerprint_randomizer.py, target: Windows 11 / PyInstaller .exe
# Device fingerprint randomizer for Telethon TelegramClient sessions.
# Generates per-account device_model, system_version, app_version, lang_code,
# system_lang_code kwargs so each session appears as a distinct Android device.
# Integrates with account_manager.Account.build_client().

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import re
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

logger = logging.getLogger("fingerprint_randomizer")

# ---------------------------------------------------------------------------
# Fingerprint store path
# ---------------------------------------------------------------------------
DEFAULT_STORE_PATH = "data/fingerprints.json"


# ---------------------------------------------------------------------------
# Raw device corpus
# Every entry is (manufacturer, model_name, marketing_name).
# Sourced from public GSMArena-style data — real devices, real model strings.
# ---------------------------------------------------------------------------

_ANDROID_DEVICES: List[Tuple[str, str, str]] = [
    # Samsung Galaxy S-series
    ("Samsung", "SM-S911B",  "Galaxy S23"),
    ("Samsung", "SM-S911U",  "Galaxy S23"),
    ("Samsung", "SM-S916B",  "Galaxy S23+"),
    ("Samsung", "SM-S918B",  "Galaxy S23 Ultra"),
    ("Samsung", "SM-S921B",  "Galaxy S24"),
    ("Samsung", "SM-S926B",  "Galaxy S24+"),
    ("Samsung", "SM-S928B",  "Galaxy S24 Ultra"),
    ("Samsung", "SM-S931B",  "Galaxy S25"),
    ("Samsung", "SM-S936B",  "Galaxy S25+"),
    ("Samsung", "SM-S938B",  "Galaxy S25 Ultra"),
    # Samsung Galaxy A-series
    ("Samsung", "SM-A546B",  "Galaxy A54 5G"),
    ("Samsung", "SM-A556B",  "Galaxy A55 5G"),
    ("Samsung", "SM-A356B",  "Galaxy A35 5G"),
    ("Samsung", "SM-A346B",  "Galaxy A34 5G"),
    ("Samsung", "SM-A155F",  "Galaxy A15"),
    ("Samsung", "SM-A256B",  "Galaxy A25 5G"),
    ("Samsung", "SM-A057F",  "Galaxy A05s"),
    # Samsung Galaxy M-series
    ("Samsung", "SM-M546B",  "Galaxy M54 5G"),
    ("Samsung", "SM-M346B",  "Galaxy M34 5G"),
    ("Samsung", "SM-M156B",  "Galaxy M15 5G"),
    # Google Pixel
    ("Google",  "Pixel 7",   "Pixel 7"),
    ("Google",  "Pixel 7 Pro","Pixel 7 Pro"),
    ("Google",  "Pixel 7a",  "Pixel 7a"),
    ("Google",  "Pixel 8",   "Pixel 8"),
    ("Google",  "Pixel 8 Pro","Pixel 8 Pro"),
    ("Google",  "Pixel 8a",  "Pixel 8a"),
    ("Google",  "Pixel 9",   "Pixel 9"),
    ("Google",  "Pixel 9 Pro","Pixel 9 Pro"),
    ("Google",  "Pixel 9 Pro XL","Pixel 9 Pro XL"),
    ("Google",  "Pixel 9 Pro Fold","Pixel 9 Pro Fold"),
    # OnePlus
    ("OnePlus", "CPH2447",   "OnePlus 11"),
    ("OnePlus", "CPH2449",   "OnePlus 11 5G"),
    ("OnePlus", "PH8110",    "OnePlus 12"),
    ("OnePlus", "PH8110EU",  "OnePlus 12"),
    ("OnePlus", "CPH2581",   "OnePlus 12R"),
    ("OnePlus", "CPH2609",   "OnePlus Nord CE 4"),
    ("OnePlus", "CPH2423",   "OnePlus Nord CE 3 Lite"),
    ("OnePlus", "CPH2495",   "OnePlus Nord 3 5G"),
    ("OnePlus", "CPH2573",   "OnePlus Nord CE4 Lite"),
    # Xiaomi
    ("Xiaomi",  "2211133G",  "Xiaomi 13"),
    ("Xiaomi",  "2210132G",  "Xiaomi 13 Pro"),
    ("Xiaomi",  "23049PCD8G","Xiaomi 13T"),
    ("Xiaomi",  "23078PND5G","Xiaomi 13T Pro"),
    ("Xiaomi",  "24030PN60G","Xiaomi 14"),
    ("Xiaomi",  "24061AP61G","Xiaomi 14T"),
    ("Xiaomi",  "24071PX39G","Xiaomi 14T Pro"),
    # Redmi
    ("Xiaomi",  "23054RA19I","Redmi Note 12 Pro 5G"),
    ("Xiaomi",  "2309BRN4EG","Redmi Note 13 Pro+"),
    ("Xiaomi",  "2409BRN7EG","Redmi Note 14 Pro+"),
    ("Xiaomi",  "23106RN0DA","Redmi 13C"),
    ("Xiaomi",  "24085RA3EG","Redmi 14C"),
    # POCO
    ("Xiaomi",  "22111317PG","POCO X5 Pro 5G"),
    ("Xiaomi",  "23049PCD8I","POCO X6 Pro"),
    ("Xiaomi",  "24076PA4EI","POCO X7 Pro"),
    ("Xiaomi",  "23076RN4BI","POCO M6 Pro"),
    # OPPO
    ("OPPO",    "CPH2525",   "OPPO Find X7"),
    ("OPPO",    "CPH2559",   "OPPO Find X7 Ultra"),
    ("OPPO",    "CPH2609",   "OPPO Reno12"),
    ("OPPO",    "CPH2611",   "OPPO Reno12 Pro"),
    ("OPPO",    "CPH2423",   "OPPO A58 5G"),
    ("OPPO",    "CPH2481",   "OPPO A78 5G"),
    ("OPPO",    "CPH2609",   "OPPO Reno11 F"),
    # Vivo
    ("vivo",    "V2309",     "vivo V29"),
    ("vivo",    "V2317",     "vivo V29 Pro"),
    ("vivo",    "V2401",     "vivo V30"),
    ("vivo",    "V2403",     "vivo V30 Pro"),
    ("vivo",    "V2411",     "vivo V40 Pro"),
    ("vivo",    "V2316",     "vivo Y200"),
    ("vivo",    "V2318",     "vivo Y100"),
    # Realme
    ("realme",  "RMX3761",   "realme GT 3"),
    ("realme",  "RMX3888",   "realme GT5 Pro"),
    ("realme",  "RMX3996",   "realme GT 6"),
    ("realme",  "RMX3834",   "realme 12 Pro+"),
    ("realme",  "RMX3998",   "realme 13 Pro+"),
    ("realme",  "RMX3627",   "realme C55"),
    ("realme",  "RMX3835",   "realme C65"),
    # Motorola
    ("motorola","XT2341-3",  "moto g84 5G"),
    ("motorola","XT2343-3",  "moto g54 5G"),
    ("motorola","XT2401-4",  "motorola edge 50 Pro"),
    ("motorola","XT2403-4",  "motorola edge 50 Ultra"),
    ("motorola","XT2411-1",  "motorola g85 5G"),
    ("motorola","XT2413-1",  "motorola edge 50 Fusion"),
    # Nokia
    ("Nokia",   "TA-1491",   "Nokia G60 5G"),
    ("Nokia",   "TA-1476",   "Nokia G42 5G"),
    ("Nokia",   "TA-1570",   "Nokia G22"),
    ("Nokia",   "TA-1583",   "Nokia C32"),
    ("Nokia",   "TA-1565",   "Nokia C22"),
    # Sony Xperia
    ("Sony",    "XQ-EC54",   "Xperia 1 V"),
    ("Sony",    "XQ-DQ54",   "Xperia 5 V"),
    ("Sony",    "XQ-CC54",   "Xperia 10 V"),
    ("Sony",    "XQ-FD54",   "Xperia 1 VI"),
    ("Sony",    "XQ-ES54",   "Xperia 5 VI"),
    # Asus
    ("asus",    "AI2302",    "ASUS ROG Phone 7"),
    ("asus",    "AI2401",    "ASUS ROG Phone 8"),
    ("asus",    "AI2401D",   "ASUS ROG Phone 8 Pro"),
    ("asus",    "ZF9",       "ASUS Zenfone 9"),
    ("asus",    "ZF10",      "ASUS Zenfone 10"),
    ("asus",    "AI2302",    "ASUS Zenfone 11 Ultra"),
    # Huawei (Android-based, no GMS, still used in some regions)
    ("HUAWEI",  "LNY-L29",   "HUAWEI nova 11 Pro"),
    ("HUAWEI",  "MDY-LX9",   "HUAWEI Mate 60 Pro"),
    ("HUAWEI",  "BRP-AN00",  "HUAWEI P60 Pro"),
    # Honor
    ("HONOR",   "ELZ-AN00",  "HONOR Magic5 Pro"),
    ("HONOR",   "REA-NX9",   "HONOR Magic6 Pro"),
    ("HONOR",   "PEY-AN00",  "HONOR 90"),
    ("HONOR",   "REF-AN00",  "HONOR 100"),
    # iQOO
    ("vivo",    "V2254A",    "iQOO 12"),
    ("vivo",    "V2334A",    "iQOO 12 Pro"),
    ("vivo",    "V2401A",    "iQOO Neo9 Pro"),
    # Infinix
    ("Infinix", "X6816",     "Infinix NOTE 30"),
    ("Infinix", "X6815",     "Infinix NOTE 30 Pro"),
    ("Infinix", "X6821",     "Infinix HOT 40 Pro"),
    # Tecno
    ("TECNO",   "CK9n",      "TECNO PHANTOM X2 Pro"),
    ("TECNO",   "LI7n",      "TECNO POVA 6 Pro 5G"),
]

# ---------------------------------------------------------------------------
# Android version corpus
# (android_version_string, build_id_prefix, sdk_level)
# ---------------------------------------------------------------------------

_ANDROID_VERSIONS: List[Tuple[str, str, int]] = [
    ("12",   "SP1A",  31),
    ("12L",  "SPB5",  32),
    ("13",   "TQ3A",  33),
    ("13",   "TQ2B",  33),
    ("14",   "AP1A",  34),
    ("14",   "AP2A",  34),
    ("15",   "BP1A",  35),
]

# ---------------------------------------------------------------------------
# Telegram app version corpus
# Real app versions from Google Play release history.
# ---------------------------------------------------------------------------

_TELEGRAM_VERSIONS: List[str] = [
    "9.3.3",
    "9.4.1",
    "9.5.2",
    "9.6.1",
    "9.6.3",
    "9.7.5",
    "10.0.1",
    "10.1.1",
    "10.2.3",
    "10.3.2",
    "10.4.1",
    "10.5.0",
    "10.6.1",
    "10.7.5",
    "10.8.2",
    "10.9.0",
    "11.0.1",
    "11.1.4",
    "11.2.0",
    "11.3.3",
]

# ---------------------------------------------------------------------------
# Language / locale corpus
# (lang_code, system_lang_code, display_name)
# ---------------------------------------------------------------------------

_LOCALES: List[Tuple[str, str, str]] = [
    ("en",    "en-US",  "English (US)"),
    ("en",    "en-GB",  "English (UK)"),
    ("en",    "en-AU",  "English (AU)"),
    ("en",    "en-IN",  "English (IN)"),
    ("th",    "th-TH",  "Thai"),
    ("id",    "id-ID",  "Indonesian"),
    ("ms",    "ms-MY",  "Malay"),
    ("vi",    "vi-VN",  "Vietnamese"),
    ("tl",    "tl-PH",  "Filipino"),
    ("km",    "km-KH",  "Khmer"),
    ("my",    "my-MM",  "Burmese"),
    ("lo",    "lo-LA",  "Lao"),
    ("zh",    "zh-CN",  "Chinese (Simplified)"),
    ("zh",    "zh-TW",  "Chinese (Traditional)"),
    ("ja",    "ja-JP",  "Japanese"),
    ("ko",    "ko-KR",  "Korean"),
    ("ar",    "ar-SA",  "Arabic (Saudi)"),
    ("ar",    "ar-EG",  "Arabic (Egypt)"),
    ("ru",    "ru-RU",  "Russian"),
    ("uk",    "uk-UA",  "Ukrainian"),
    ("de",    "de-DE",  "German"),
    ("fr",    "fr-FR",  "French"),
    ("es",    "es-ES",  "Spanish"),
    ("pt",    "pt-BR",  "Portuguese (Brazil)"),
    ("it",    "it-IT",  "Italian"),
    ("tr",    "tr-TR",  "Turkish"),
    ("fa",    "fa-IR",  "Persian"),
    ("hi",    "hi-IN",  "Hindi"),
    ("bn",    "bn-BD",  "Bengali"),
    ("ur",    "ur-PK",  "Urdu"),
]

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class DeviceFingerprint:
    """
    Complete fingerprint passed to TelegramClient constructor.

    Fields match Telethon's TelegramClient kwargs exactly:
    device_model, system_version, app_version, lang_code, system_lang_code.
    Additional metadata stored for bookkeeping.
    """

    phone: str                # account phone this fingerprint is pinned to
    manufacturer: str         # e.g. "Samsung"
    model_id: str             # e.g. "SM-S911B" (internal model number)
    marketing_name: str       # e.g. "Galaxy S23" (what Telegram shows)
    device_model: str         # value sent to Telegram as device_model
    system_version: str       # e.g. "Android 14"
    app_version: str          # e.g. "10.3.2"
    lang_code: str            # e.g. "en"
    system_lang_code: str     # e.g. "en-US"
    android_sdk: int          # API level (informational)
    build_id: str             # e.g. "AP2A.240905.003"
    created_at: float = field(default_factory=time.time)
    rotated_at: float = field(default_factory=time.time)
    rotation_count: int = 0

    # ------------------------------------------------------------------ #
    # Telethon kwargs
    # ------------------------------------------------------------------ #

    def to_telethon_kwargs(self) -> dict:
        """
        Return the dict of kwargs that overrides TelegramClient defaults.
        Pass as **fp.to_telethon_kwargs() in build_client().
        """
        return {
            "device_model":     self.device_model,
            "system_version":   self.system_version,
            "app_version":      self.app_version,
            "lang_code":        self.lang_code,
            "system_lang_code": self.system_lang_code,
        }

    # ------------------------------------------------------------------ #
    # Serialization
    # ------------------------------------------------------------------ #

    def to_dict(self) -> dict:
        return {
            "phone":            self.phone,
            "manufacturer":     self.manufacturer,
            "model_id":         self.model_id,
            "marketing_name":   self.marketing_name,
            "device_model":     self.device_model,
            "system_version":   self.system_version,
            "app_version":      self.app_version,
            "lang_code":        self.lang_code,
            "system_lang_code": self.system_lang_code,
            "android_sdk":      self.android_sdk,
            "build_id":         self.build_id,
            "created_at":       self.created_at,
            "rotated_at":       self.rotated_at,
            "rotation_count":   self.rotation_count,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "DeviceFingerprint":
        return cls(
            phone=d["phone"],
            manufacturer=d["manufacturer"],
            model_id=d["model_id"],
            marketing_name=d["marketing_name"],
            device_model=d["device_model"],
            system_version=d["system_version"],
            app_version=d["app_version"],
            lang_code=d["lang_code"],
            system_lang_code=d["system_lang_code"],
            android_sdk=d.get("android_sdk", 34),
            build_id=d.get("build_id", ""),
            created_at=d.get("created_at", 0.0),
            rotated_at=d.get("rotated_at", 0.0),
            rotation_count=d.get("rotation_count", 0),
        )

    def summary(self) -> str:
        return (
            f"{self.phone}: {self.device_model} / Android {self.system_version.split()[-1]}"
            f" / TG {self.app_version} / {self.system_lang_code}"
        )


# ---------------------------------------------------------------------------
# Generation strategies
# ---------------------------------------------------------------------------


class FingerprintStrategy:
    """
    Pluggable strategy for controlling how random a fingerprint is.

    Subclass and override pick_device(), pick_android(), pick_tg_version(),
    pick_locale() to bias towards specific cohorts.
    """

    def pick_device(
        self,
        rng: random.Random,
        exclude: Optional[Set[str]] = None,
    ) -> Tuple[str, str, str]:
        """Return (manufacturer, model_id, marketing_name)."""
        exclude = exclude or set()
        candidates = [d for d in _ANDROID_DEVICES if d[1] not in exclude]
        if not candidates:
            candidates = _ANDROID_DEVICES
        return rng.choice(candidates)

    def pick_android(
        self,
        rng: random.Random,
        min_sdk: int = 31,
    ) -> Tuple[str, str, int]:
        """Return (version_string, build_id_prefix, sdk_level)."""
        candidates = [v for v in _ANDROID_VERSIONS if v[2] >= min_sdk]
        if not candidates:
            candidates = _ANDROID_VERSIONS
        return rng.choice(candidates)

    def pick_tg_version(self, rng: random.Random) -> str:
        return rng.choice(_TELEGRAM_VERSIONS)

    def pick_locale(self, rng: random.Random) -> Tuple[str, str, str]:
        """Return (lang_code, system_lang_code, display_name)."""
        return rng.choice(_LOCALES)


class MidRangeStrategy(FingerprintStrategy):
    """
    Bias toward mid-range Samsung / Xiaomi / Realme — the most common
    devices in SEA markets where these accounts are likely used.
    """

    _PREFERRED_MANUFACTURERS = {"Samsung", "Xiaomi", "realme", "OPPO", "vivo"}

    def pick_device(self, rng, exclude=None):
        exclude = exclude or set()
        candidates = [
            d for d in _ANDROID_DEVICES
            if d[0] in self._PREFERRED_MANUFACTURERS and d[1] not in exclude
        ]
        if not candidates:
            candidates = _ANDROID_DEVICES
        return rng.choice(candidates)

    def pick_android(self, rng, min_sdk=33):
        candidates = [v for v in _ANDROID_VERSIONS if v[2] >= min_sdk]
        return rng.choice(candidates)

    def pick_locale(self, rng):
        sea_locales = [l for l in _LOCALES if l[1] in (
            "th-TH", "id-ID", "ms-MY", "vi-VN", "tl-PH",
            "km-KH", "my-MM", "lo-LA", "en-IN", "en-GB",
        )]
        return rng.choice(sea_locales)


class FlagshipStrategy(FingerprintStrategy):
    """Bias toward Samsung/Google flagship devices — Android 14+."""

    _FLAGSHIP_MODELS = {
        "SM-S928B", "SM-S918B", "SM-S916B", "SM-S911B",
        "SM-S921B", "SM-S926B", "SM-S931B", "SM-S936B", "SM-S938B",
        "Pixel 8 Pro", "Pixel 9 Pro", "Pixel 9 Pro XL", "Pixel 9",
        "XQ-EC54", "XQ-FD54",
    }

    def pick_device(self, rng, exclude=None):
        exclude = exclude or set()
        candidates = [
            d for d in _ANDROID_DEVICES
            if d[1] in self._FLAGSHIP_MODELS and d[1] not in exclude
        ]
        if not candidates:
            candidates = _ANDROID_DEVICES
        return rng.choice(candidates)

    def pick_android(self, rng, min_sdk=34):
        candidates = [v for v in _ANDROID_VERSIONS if v[2] >= min_sdk]
        return rng.choice(candidates)


# ---------------------------------------------------------------------------
# Build ID generator
# ---------------------------------------------------------------------------


def _make_build_id(prefix: str, rng: random.Random) -> str:
    """
    Generate a plausible Android build ID string.
    Format: PREFIX.YYMMDD.NNN  e.g.  AP2A.240905.003
    """
    year = rng.randint(23, 24)
    month = rng.randint(1, 12)
    day = rng.randint(1, 28)
    seq = rng.randint(1, 9)
    return f"{prefix}.{year:02d}{month:02d}{day:02d}.00{seq}"


def _make_device_model_string(
    manufacturer: str,
    model_id: str,
    marketing_name: str,
    rng: random.Random,
) -> str:
    """
    Build the device_model string Telegram actually sees.
    Some apps report marketing name, some report model ID, some combine.
    We pick one of three real-world patterns at random.
    """
    patterns = [
        marketing_name,                            # "Galaxy S23"
        f"{manufacturer} {marketing_name}",        # "Samsung Galaxy S23"
        model_id,                                  # "SM-S911B"
    ]
    return rng.choice(patterns)


# ---------------------------------------------------------------------------
# Core randomizer
# ---------------------------------------------------------------------------


class FingerprintRandomizer:
    """
    Generates, stores, retrieves, and rotates DeviceFingerprint objects.

    One fingerprint is pinned to each phone number and persisted to disk
    so the same account always presents the same device until explicitly
    rotated.  Rotation is tracked and can be audited.

    Usage
    -----
    r = FingerprintRandomizer()
    fp = r.get_or_create("+66812345678")
    client = TelegramClient(session, api_id, api_hash, **fp.to_telethon_kwargs())
    """

    def __init__(
        self,
        store_path: str = DEFAULT_STORE_PATH,
        strategy: Optional[FingerprintStrategy] = None,
        seed: Optional[int] = None,
        min_sdk: int = 31,
        deduplicate_devices: bool = True,
    ) -> None:
        self._store_path = store_path
        self._strategy = strategy or FingerprintStrategy()
        self._rng = random.Random(seed)  # isolated RNG — not global random
        self._min_sdk = min_sdk
        self._deduplicate_devices = deduplicate_devices
        self._store: Dict[str, DeviceFingerprint] = {}
        self._load()

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def get_or_create(self, phone: str) -> DeviceFingerprint:
        """Return existing fingerprint or generate and persist a new one."""
        phone = _normalise_phone(phone)
        if phone in self._store:
            return self._store[phone]
        fp = self._generate(phone)
        self._store[phone] = fp
        self._save()
        logger.info("Created fingerprint for %s: %s", phone, fp.summary())
        return fp

    def get(self, phone: str) -> Optional[DeviceFingerprint]:
        """Return pinned fingerprint or None if not yet created."""
        return self._store.get(_normalise_phone(phone))

    def rotate(self, phone: str, *, force: bool = False) -> DeviceFingerprint:
        """
        Generate a new fingerprint for the given phone.
        The new fingerprint will differ from the old one in device,
        Android version, and optionally locale.

        force=True skips the deduplication check and always generates fresh.
        """
        phone = _normalise_phone(phone)
        old = self._store.get(phone)
        old_model = old.model_id if old else None
        exclude = {old_model} if old_model and not force else None

        fp = self._generate(phone, exclude_models=exclude)
        if old:
            fp.rotation_count = old.rotation_count + 1
            fp.created_at = old.created_at
        self._store[phone] = fp
        self._save()
        logger.info(
            "Rotated fingerprint for %s (#%d): %s",
            phone, fp.rotation_count, fp.summary()
        )
        return fp

    def rotate_many(
        self,
        phones: Sequence[str],
        *,
        force: bool = False,
    ) -> Dict[str, DeviceFingerprint]:
        """Batch-rotate multiple accounts."""
        result: Dict[str, DeviceFingerprint] = {}
        for phone in phones:
            result[phone] = self.rotate(phone, force=force)
        return result

    def bulk_create(
        self,
        phones: Sequence[str],
        *,
        overwrite: bool = False,
    ) -> Dict[str, DeviceFingerprint]:
        """
        Create fingerprints for all supplied phones that don't have one yet.
        If overwrite=True, existing fingerprints are replaced.
        Returns a mapping phone → fingerprint.
        """
        result: Dict[str, DeviceFingerprint] = {}
        for phone in phones:
            p = _normalise_phone(phone)
            if overwrite or p not in self._store:
                fp = self._generate(p)
                self._store[p] = fp
                result[p] = fp
            else:
                result[p] = self._store[p]
        self._save()
        logger.info(
            "bulk_create: %d fingerprints generated / refreshed", len(result)
        )
        return result

    def delete(self, phone: str) -> bool:
        """Remove a pinned fingerprint.  Returns True if it existed."""
        phone = _normalise_phone(phone)
        existed = phone in self._store
        self._store.pop(phone, None)
        if existed:
            self._save()
            logger.info("Deleted fingerprint for %s", phone)
        return existed

    def all_fingerprints(self) -> Dict[str, DeviceFingerprint]:
        """Return a copy of the full store."""
        return dict(self._store)

    def stats(self) -> dict:
        """Aggregate statistics across all stored fingerprints."""
        fps = list(self._store.values())
        if not fps:
            return {"total": 0}

        manufacturers = {}
        android_versions = {}
        tg_versions = {}
        locales = {}
        for fp in fps:
            manufacturers[fp.manufacturer] = manufacturers.get(fp.manufacturer, 0) + 1
            av = fp.system_version.split()[-1] if fp.system_version.startswith("Android") else fp.system_version
            android_versions[av] = android_versions.get(av, 0) + 1
            tg_versions[fp.app_version] = tg_versions.get(fp.app_version, 0) + 1
            locales[fp.system_lang_code] = locales.get(fp.system_lang_code, 0) + 1

        total_rotations = sum(fp.rotation_count for fp in fps)
        return {
            "total": len(fps),
            "total_rotations": total_rotations,
            "manufacturers": manufacturers,
            "android_versions": android_versions,
            "tg_versions": tg_versions,
            "locales": locales,
            "unique_devices": len({fp.model_id for fp in fps}),
        }

    def report(self) -> str:
        """Human-readable fingerprint diversity report."""
        s = self.stats()
        lines = [
            f"Fingerprint Store — {s.get('total', 0)} accounts",
            f"  Unique devices:   {s.get('unique_devices', 0)}",
            f"  Total rotations:  {s.get('total_rotations', 0)}",
            "",
            "  Manufacturers:",
        ]
        for mfr, count in sorted(
            s.get("manufacturers", {}).items(), key=lambda x: -x[1]
        ):
            lines.append(f"    {mfr:<20} {count}")
        lines.append("  Android versions:")
        for av, count in sorted(
            s.get("android_versions", {}).items(), key=lambda x: -x[1]
        ):
            lines.append(f"    Android {av:<10} {count}")
        lines.append("  TG versions:")
        for tv, count in sorted(
            s.get("tg_versions", {}).items(), key=lambda x: -x[1]
        ):
            lines.append(f"    {tv:<12} {count}")
        lines.append("  Locales:")
        for lc, count in sorted(
            s.get("locales", {}).items(), key=lambda x: -x[1]
        ):
            lines.append(f"    {lc:<20} {count}")
        return "\n".join(lines)

    def apply_to_account(self, acc: "Account") -> DeviceFingerprint:
        """
        Convenience: get-or-create a fingerprint for acc.phone and
        patch acc.build_client so the kwargs are injected automatically.

        Does not rebuild an already-connected client — call before connect.
        """
        fp = self.get_or_create(acc.phone)
        _patch_account(acc, fp)
        return fp

    def apply_to_many(self, accounts: Sequence["Account"]) -> Dict[str, DeviceFingerprint]:
        result = {}
        for acc in accounts:
            result[acc.phone] = self.apply_to_account(acc)
        return result

    # ------------------------------------------------------------------ #
    # Internal generation
    # ------------------------------------------------------------------ #

    def _generate(
        self,
        phone: str,
        exclude_models: Optional[set] = None,
    ) -> DeviceFingerprint:
        # Collect already-used model IDs for deduplication
        used_models: set = set()
        if self._deduplicate_devices:
            used_models = {fp.model_id for fp in self._store.values()}
        if exclude_models:
            used_models |= exclude_models

        manufacturer, model_id, marketing_name = self._strategy.pick_device(
            self._rng, exclude=used_models
        )
        android_ver, build_prefix, sdk_level = self._strategy.pick_android(
            self._rng, min_sdk=self._min_sdk
        )
        tg_version = self._strategy.pick_tg_version(self._rng)
        lang_code, system_lang_code, _ = self._strategy.pick_locale(self._rng)

        device_model_str = _make_device_model_string(
            manufacturer, model_id, marketing_name, self._rng
        )
        build_id = _make_build_id(build_prefix, self._rng)
        system_version_str = f"Android {android_ver}"

        return DeviceFingerprint(
            phone=phone,
            manufacturer=manufacturer,
            model_id=model_id,
            marketing_name=marketing_name,
            device_model=device_model_str,
            system_version=system_version_str,
            app_version=tg_version,
            lang_code=lang_code,
            system_lang_code=system_lang_code,
            android_sdk=sdk_level,
            build_id=build_id,
        )

    # ------------------------------------------------------------------ #
    # Persistence
    # ------------------------------------------------------------------ #

    def _load(self) -> None:
        path = Path(self._store_path)
        if not path.exists():
            logger.debug("Fingerprint store not found at %s — starting fresh", path)
            return
        try:
            with path.open("r", encoding="utf-8") as fh:
                raw: dict = json.load(fh)
            self._store = {
                phone: DeviceFingerprint.from_dict(d)
                for phone, d in raw.items()
            }
            logger.info(
                "Loaded %d fingerprints from %s", len(self._store), path
            )
        except Exception as e:
            logger.error("Failed to load fingerprint store: %s", e)
            self._store = {}

    def _save(self) -> None:
        path = Path(self._store_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = str(path) + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(
                    {phone: fp.to_dict() for phone, fp in self._store.items()},
                    fh,
                    indent=2,
                    ensure_ascii=False,
                )
            os.replace(tmp, str(path))
        except Exception as e:
            logger.error("Failed to save fingerprint store: %s", e)


# ---------------------------------------------------------------------------
# Account patching
# ---------------------------------------------------------------------------


def _patch_account(acc: "Account", fp: DeviceFingerprint) -> None:
    """
    Monkey-patch acc.build_client() so it injects fingerprint kwargs.
    Uses a closure — the original build_client is preserved.

    This is intentionally non-invasive: account_manager.py is unchanged.
    """
    original_build_client = acc.build_client.__func__  # unbound method

    def patched_build_client(self_inner):
        client = original_build_client(self_inner)
        # Telethon reads these properties from the Session on first connect;
        # we override them on the client object directly after construction.
        client._device_model = fp.device_model
        client._system_version = fp.system_version
        client._app_version = fp.app_version
        client._lang_code = fp.lang_code
        client._system_lang_code = fp.system_lang_code
        return client

    import types
    acc.build_client = types.MethodType(patched_build_client, acc)


# ---------------------------------------------------------------------------
# Phone normalizer
# ---------------------------------------------------------------------------


def _normalise_phone(phone: str) -> str:
    """Strip whitespace and ensure leading +."""
    phone = phone.strip().replace(" ", "").replace("-", "")
    if phone and not phone.startswith("+"):
        phone = "+" + phone
    return phone


# ---------------------------------------------------------------------------
# Standalone helpers (usable without full AccountManager)
# ---------------------------------------------------------------------------


def generate_fingerprint(
    phone: str,
    *,
    strategy: Optional[FingerprintStrategy] = None,
    seed: Optional[int] = None,
    min_sdk: int = 31,
) -> DeviceFingerprint:
    """
    One-shot fingerprint generation without persistence.

    Useful for testing and for callers that manage their own storage.
    """
    rng = random.Random(seed)
    s = strategy or FingerprintStrategy()
    manufacturer, model_id, marketing_name = s.pick_device(rng)
    android_ver, build_prefix, sdk_level = s.pick_android(rng, min_sdk=min_sdk)
    tg_version = s.pick_tg_version(rng)
    lang_code, system_lang_code, _ = s.pick_locale(rng)
    device_model_str = _make_device_model_string(manufacturer, model_id, marketing_name, rng)
    build_id = _make_build_id(build_prefix, rng)
    return DeviceFingerprint(
        phone=_normalise_phone(phone),
        manufacturer=manufacturer,
        model_id=model_id,
        marketing_name=marketing_name,
        device_model=device_model_str,
        system_version=f"Android {android_ver}",
        app_version=tg_version,
        lang_code=lang_code,
        system_lang_code=system_lang_code,
        android_sdk=sdk_level,
        build_id=build_id,
    )


def fingerprints_for_accounts(
    phones: Sequence[str],
    *,
    strategy: Optional[FingerprintStrategy] = None,
    store_path: str = DEFAULT_STORE_PATH,
    rotate: bool = False,
) -> Dict[str, DeviceFingerprint]:
    """
    Convenience wrapper: build or load fingerprints for a list of phones.

    rotate=True forces rotation of all existing fingerprints.
    """
    r = FingerprintRandomizer(store_path=store_path, strategy=strategy)
    if rotate:
        return r.rotate_many(phones)
    return r.bulk_create(phones)


def apply_fingerprints(
    accounts: Sequence["Account"],
    *,
    store_path: str = DEFAULT_STORE_PATH,
    strategy: Optional[FingerprintStrategy] = None,
    rotate: bool = False,
) -> Dict[str, DeviceFingerprint]:
    """
    Apply fingerprints to a list of Account objects from account_manager.

    Call this BEFORE connect_all() / AccountManager.start().

    Example
    -------
    from account_manager import load_accounts
    from utilities.fingerprint_randomizer import apply_fingerprints

    accounts = load_accounts("accounts.csv")
    fingerprints = apply_fingerprints(accounts)
    # accounts[i].build_client() now injects the correct device fingerprint
    """
    r = FingerprintRandomizer(store_path=store_path, strategy=strategy)
    result: Dict[str, DeviceFingerprint] = {}
    for acc in accounts:
        if rotate:
            fp = r.rotate(acc.phone)
        else:
            fp = r.get_or_create(acc.phone)
        _patch_account(acc, fp)
        result[acc.phone] = fp
    logger.info(
        "Applied fingerprints to %d accounts (rotate=%s)", len(result), rotate
    )
    return result


# ---------------------------------------------------------------------------
# Diversity validator
# ---------------------------------------------------------------------------


class DiversityValidator:
    """
    Checks that a set of fingerprints is diverse enough to avoid
    detection as a coordinated fleet.

    Thresholds are conservative defaults; adjust per use-case.
    """

    def __init__(
        self,
        max_same_device_pct: float = 0.15,
        max_same_android_pct: float = 0.40,
        max_same_tg_version_pct: float = 0.50,
        max_same_locale_pct: float = 0.70,
    ) -> None:
        self.max_same_device_pct = max_same_device_pct
        self.max_same_android_pct = max_same_android_pct
        self.max_same_tg_version_pct = max_same_tg_version_pct
        self.max_same_locale_pct = max_same_locale_pct

    @dataclass
    class ValidationResult:
        passed: bool
        violations: List[str]
        warnings: List[str]

    def validate(
        self, fingerprints: Sequence[DeviceFingerprint]
    ) -> "DiversityValidator.ValidationResult":
        fps = list(fingerprints)
        n = len(fps)
        if n == 0:
            return self.ValidationResult(passed=True, violations=[], warnings=[])

        violations: List[str] = []
        warnings: List[str] = []

        # Device deduplication
        device_counts: Dict[str, int] = {}
        for fp in fps:
            device_counts[fp.model_id] = device_counts.get(fp.model_id, 0) + 1
        max_device_count = max(device_counts.values())
        max_device_pct = max_device_count / n
        if max_device_pct > self.max_same_device_pct:
            most_common = max(device_counts, key=lambda k: device_counts[k])
            violations.append(
                f"Device '{most_common}' used by {max_device_count}/{n} accounts "
                f"({max_device_pct:.0%} > limit {self.max_same_device_pct:.0%})"
            )

        # Android version
        av_counts: Dict[str, int] = {}
        for fp in fps:
            av = fp.system_version
            av_counts[av] = av_counts.get(av, 0) + 1
        max_av = max(av_counts.values()) / n
        if max_av > self.max_same_android_pct:
            mc = max(av_counts, key=lambda k: av_counts[k])
            violations.append(
                f"Android version '{mc}' over-represented: {max_av:.0%} > limit {self.max_same_android_pct:.0%}"
            )

        # TG version
        tv_counts: Dict[str, int] = {}
        for fp in fps:
            tv_counts[fp.app_version] = tv_counts.get(fp.app_version, 0) + 1
        max_tv = max(tv_counts.values()) / n
        if max_tv > self.max_same_tg_version_pct:
            mc = max(tv_counts, key=lambda k: tv_counts[k])
            warnings.append(
                f"TG version '{mc}' over-represented: {max_tv:.0%} (consider more diversity)"
            )

        # Locale
        lc_counts: Dict[str, int] = {}
        for fp in fps:
            lc_counts[fp.system_lang_code] = lc_counts.get(fp.system_lang_code, 0) + 1
        max_lc = max(lc_counts.values()) / n
        if max_lc > self.max_same_locale_pct:
            mc = max(lc_counts, key=lambda k: lc_counts[k])
            warnings.append(
                f"Locale '{mc}' over-represented: {max_lc:.0%} — normal but noted"
            )

        return self.ValidationResult(
            passed=len(violations) == 0,
            violations=violations,
            warnings=warnings,
        )

    def validate_and_log(
        self, fingerprints: Sequence[DeviceFingerprint]
    ) -> bool:
        result = self.validate(fingerprints)
        for v in result.violations:
            logger.error("Fingerprint diversity VIOLATION: %s", v)
        for w in result.warnings:
            logger.warning("Fingerprint diversity warning: %s", w)
        if result.passed:
            logger.info(
                "Fingerprint diversity OK across %d accounts", len(list(fingerprints))
            )
        return result.passed


# ---------------------------------------------------------------------------
# Auto-rotation policy
# ---------------------------------------------------------------------------


class RotationPolicy:
    """
    Defines when a fingerprint should be automatically rotated.

    Use with RotationScheduler to trigger rotation without manual intervention.
    """

    def __init__(
        self,
        max_age_days: float = 30.0,
        max_rotations: int = 100,
        rotate_on_ban: bool = True,
        rotate_on_flood_count: int = 10,
    ) -> None:
        self.max_age_days = max_age_days
        self.max_rotations = max_rotations
        self.rotate_on_ban = rotate_on_ban
        self.rotate_on_flood_count = rotate_on_flood_count

    def should_rotate(
        self,
        fp: DeviceFingerprint,
        *,
        ban_detected: bool = False,
        flood_count: int = 0,
    ) -> Tuple[bool, str]:
        """
        Returns (should_rotate, reason_string).
        """
        now = time.time()
        age_days = (now - fp.rotated_at) / 86400.0

        if ban_detected and self.rotate_on_ban:
            return True, f"ban detected"

        if flood_count >= self.rotate_on_flood_count:
            return True, f"flood count {flood_count} >= threshold {self.rotate_on_flood_count}"

        if age_days >= self.max_age_days:
            return True, f"fingerprint age {age_days:.1f}d >= limit {self.max_age_days}d"

        if fp.rotation_count >= self.max_rotations:
            return True, f"rotation count {fp.rotation_count} >= max {self.max_rotations}"

        return False, ""


class RotationScheduler:
    """
    Background task that periodically checks all fingerprints against
    a RotationPolicy and triggers rotation when due.
    """

    def __init__(
        self,
        randomizer: FingerprintRandomizer,
        policy: Optional[RotationPolicy] = None,
        check_interval_sec: float = 3600.0,
    ) -> None:
        self._randomizer = randomizer
        self._policy = policy or RotationPolicy()
        self._interval = check_interval_sec
        self._task: Optional["asyncio.Task"] = None
        self._running = False

    async def start(self) -> None:
        import asyncio as _asyncio
        if self._running:
            return
        self._running = True
        self._task = _asyncio.create_task(self._loop(), name="fingerprint_rotation")
        logger.info("RotationScheduler started (interval=%.0fs)", self._interval)

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                import asyncio as _asyncio
                await _asyncio.wait_for(self._task, timeout=5.0)
            except Exception:
                pass
        logger.info("RotationScheduler stopped")

    async def _loop(self) -> None:
        import asyncio as _asyncio
        while self._running:
            try:
                self._check_all()
            except Exception as e:
                logger.error("RotationScheduler error: %s", e)
            await _asyncio.sleep(self._interval)

    def _check_all(self) -> None:
        fps = self._randomizer.all_fingerprints()
        rotated = 0
        for phone, fp in fps.items():
            should, reason = self._policy.should_rotate(fp)
            if should:
                self._randomizer.rotate(phone)
                logger.info("Auto-rotated %s: %s", phone, reason)
                rotated += 1
        if rotated:
            logger.info("RotationScheduler: rotated %d fingerprints this cycle", rotated)

    def check_account(
        self,
        phone: str,
        *,
        ban_detected: bool = False,
        flood_count: int = 0,
    ) -> bool:
        """
        Check one account immediately against policy.
        Returns True if rotation was performed.
        """
        fp = self._randomizer.get(phone)
        if fp is None:
            return False
        should, reason = self._policy.should_rotate(
            fp, ban_detected=ban_detected, flood_count=flood_count
        )
        if should:
            self._randomizer.rotate(phone)
            logger.info("Policy-triggered rotation for %s: %s", phone, reason)
            return True
        return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    # Data classes
    "DeviceFingerprint",
    # Strategies
    "FingerprintStrategy",
    "MidRangeStrategy",
    "FlagshipStrategy",
    # Core randomizer
    "FingerprintRandomizer",
    # Standalone helpers
    "generate_fingerprint",
    "fingerprints_for_accounts",
    "apply_fingerprints",
    # Diversity
    "DiversityValidator",
    # Rotation
    "RotationPolicy",
    "RotationScheduler",
    # Utils
    "_normalise_phone",
]