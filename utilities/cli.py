# lang: Python 3.11+, file: utilities/cli.py, target: Windows 11 / PyInstaller .exe
# Command-line interface for the Bootsusers automation suite.
#
# Runs any script in the suite with flags instead of editing config files.
# Every constant from auto_react.py and auto_join.py can be overridden at runtime.
#
# Usage examples
# --------------
# Run the reaction bot:
#   python -m utilities.cli react \
#       --csv accounts.csv \
#       --mode staggered \
#       --stagger-min 2.0 \
#       --stagger-max 8.0 \
#       --max-per-hour 20 \
#       --emoji "👍" "❤️" "🔥"
#
# Run the join bot:
#   python -m utilities.cli join \
#       --csv accounts.csv \
#       --targets groups.txt \
#       --batch-size 5 \
#       --delay 60
#
# Run fingerprint operations:
#   python -m utilities.cli fingerprint report --csv accounts.csv
#   python -m utilities.cli fingerprint rotate --csv accounts.csv --phone +66891234567
#
# Show stats for the current session:
#   python -m utilities.cli status --csv accounts.csv

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def _configure_logging(level_name: str, log_file: Optional[str] = None) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=level,
        format="[%(asctime)s] %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
        handlers=handlers,
    )


logger = logging.getLogger("cli")


# ---------------------------------------------------------------------------
# Config overrides dataclasses
# Those map 1-to-1 to the constants in auto_react.py / auto_join.py so the
# CLI can override them before the target module is imported and run.
# ---------------------------------------------------------------------------


@dataclass
class ReactConfig:
    """Runtime overrides for auto_react.py constants."""

    # API credentials (required)
    api_id: Optional[int] = None
    api_hash: Optional[str] = None

    # CSV / session
    accounts_csv: str = "Telegram_70_000_000_[YOSCRYPT].csv"
    sender_csv: Optional[str] = None      # separate sender CSV; None = first row of accounts_csv

    # Reaction parameters
    reaction_pool: List[str] = None   # type: ignore[assignment]
    reaction_mode: str = "staggered"  # "simultaneous" | "staggered"
    stagger_min_sec: float = 1.0
    stagger_max_sec: float = 5.0
    max_reactions_per_hour: int = 30
    min_gap_between_reactions: float = 2.0
    health_check_interval: float = 120.0

    # Fingerprint
    fingerprint_store: Optional[str] = None   # path to fingerprint JSON store
    use_fingerprints: bool = True

    # Proxy
    proxy_file: Optional[str] = None   # path to proxy list file (one per line)

    # Misc
    dry_run: bool = False    # connect but never actually send reactions
    verbose: bool = False

    def __post_init__(self) -> None:
        if self.reaction_pool is None:
            self.reaction_pool = ["👍", "❤️", "🔥", "🎉", "👏"]


@dataclass
class JoinConfig:
    """Runtime overrides for auto_join.py constants."""

    # API credentials (required)
    api_id: Optional[int] = None
    api_hash: Optional[str] = None

    # CSV / session
    accounts_csv: str = "Telegram_70_000_000_[YOSCRYPT].csv"

    # Targets
    targets: List[str] = None   # type: ignore[assignment]
    targets_file: Optional[str] = None    # one target per line

    # Join parameters
    batch_size: int = 5
    delay_between_joins: float = 60.0
    delay_between_batches: float = 300.0
    max_joins_per_account: int = 10
    max_joins_per_hour: int = 5

    # Fingerprint
    fingerprint_store: Optional[str] = None
    use_fingerprints: bool = True

    # Proxy
    proxy_file: Optional[str] = None

    # Misc
    dry_run: bool = False
    verbose: bool = False

    def __post_init__(self) -> None:
        if self.targets is None:
            self.targets = []


@dataclass
class FingerprintConfig:
    """Config for fingerprint subcommands."""

    accounts_csv: str = "Telegram_70_000_000_[YOSCRYPT].csv"
    store_path: str = "fingerprints.json"
    phone: Optional[str] = None
    strategy: str = "auto"    # "auto" | "midrange" | "flagship"
    force: bool = False
    verbose: bool = False


# ---------------------------------------------------------------------------
# Argument parsing helpers
# ---------------------------------------------------------------------------


def _add_common_args(p: argparse.ArgumentParser) -> None:
    """Add arguments shared by every subcommand."""
    p.add_argument(
        "--csv",
        dest="accounts_csv",
        default=os.environ.get("BOOTSUSERS_CSV", "Telegram_70_000_000_[YOSCRYPT].csv"),
        help="Path to accounts CSV file (phone,session_name[,proxy_*]). "
             "Default: $BOOTSUSERS_CSV or Telegram_70_000_000_[YOSCRYPT].csv",
    )
    p.add_argument(
        "--api-id",
        dest="api_id",
        type=int,
        default=int(os.environ.get("TELEGRAM_API_ID", "34674448")),
        help="Telegram API ID.  Default: $TELEGRAM_API_ID or 34674448",
    )
    p.add_argument(
        "--api-hash",
        dest="api_hash",
        default=os.environ.get("TELEGRAM_API_HASH", "7098ee8e75f3c435f808ee9410a2f05b"),
        help="Telegram API hash.  Default: $TELEGRAM_API_HASH",
    )
    p.add_argument(
        "--log-level",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.  Default: INFO",
    )
    p.add_argument(
        "--log-file",
        dest="log_file",
        default=None,
        metavar="PATH",
        help="Append logs to this file in addition to stderr.",
    )
    p.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="Connect accounts but do not send any reactions or join any groups.",
    )
    p.add_argument(
        "--fingerprint-store",
        dest="fingerprint_store",
        default=os.environ.get("BOOTSUSERS_FP_STORE", None),
        metavar="PATH",
        help="Path to the fingerprint JSON store.  "
             "Default: $BOOTSUSERS_FP_STORE (auto-detected if unset).",
    )
    p.add_argument(
        "--no-fingerprints",
        dest="use_fingerprints",
        action="store_false",
        help="Disable per-account device fingerprint randomisation.",
    )
    p.add_argument(
        "--proxy-file",
        dest="proxy_file",
        default=None,
        metavar="PATH",
        help="Path to proxy list (one socks5://user:pass@host:port per line). "
             "Overrides proxy columns in the CSV.",
    )


def _build_react_parser(sub: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p: argparse.ArgumentParser = sub.add_parser(
        "react",
        help="Run the reaction bot (auto_react.py).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
            Start the reaction bot.  Monitors the sender account for outgoing
            messages and fires reactions from every reactor account.

            Examples
            --------
            Staggered mode with custom emoji pool:
              python -m utilities.cli react --mode staggered --emoji "👍" "❤️" "🔥"

            Simultaneous mode, limit to 10 reactions/hour:
              python -m utilities.cli react --mode simultaneous --max-per-hour 10

            Dry-run to verify accounts connect without sending anything:
              python -m utilities.cli react --dry-run
        """),
    )
    _add_common_args(p)

    # Sender
    p.add_argument(
        "--sender-csv",
        dest="sender_csv",
        default=None,
        metavar="PATH",
        help="Separate CSV for the sender account.  "
             "If omitted, the first row of --csv is used as the sender.",
    )

    # Emoji
    p.add_argument(
        "--emoji",
        dest="reaction_pool",
        nargs="+",
        default=None,
        metavar="EMOJI",
        help='Emoji to use as reactions.  '
             'Default: "👍" "❤️" "🔥" "🎉" "👏"',
    )

    # Mode
    p.add_argument(
        "--mode",
        dest="reaction_mode",
        default="staggered",
        choices=["simultaneous", "staggered"],
        help="Reaction dispatch mode.  "
             "simultaneous: all accounts react at once via asyncio.gather(). "
             "staggered: random delay between each account.  Default: staggered",
    )

    # Stagger timing
    p.add_argument(
        "--stagger-min",
        dest="stagger_min_sec",
        type=float,
        default=1.0,
        metavar="SEC",
        help="Minimum stagger delay in seconds.  Default: 1.0",
    )
    p.add_argument(
        "--stagger-max",
        dest="stagger_max_sec",
        type=float,
        default=5.0,
        metavar="SEC",
        help="Maximum stagger delay in seconds.  Default: 5.0",
    )

    # Rate limiting
    p.add_argument(
        "--max-per-hour",
        dest="max_reactions_per_hour",
        type=int,
        default=30,
        metavar="N",
        help="Maximum reactions per account per rolling hour.  Default: 30",
    )
    p.add_argument(
        "--min-gap",
        dest="min_gap_between_reactions",
        type=float,
        default=2.0,
        metavar="SEC",
        help="Minimum seconds between reactions from the same account.  Default: 2.0",
    )

    # Health check
    p.add_argument(
        "--health-interval",
        dest="health_check_interval",
        type=float,
        default=120.0,
        metavar="SEC",
        help="Interval in seconds between health checks.  Default: 120.0",
    )

    return p


def _build_join_parser(sub: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p: argparse.ArgumentParser = sub.add_parser(
        "join",
        help="Run the join bot (auto_join.py).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
            Start the auto-join bot.  Distributes join requests across the
            account pool for a list of Telegram groups or channels.

            Examples
            --------
            Join from a text file of invite links:
              python -m utilities.cli join --targets-file groups.txt

            Inline targets:
              python -m utilities.cli join --targets @mygroup t.me/+abc123

            Custom batch size and delays:
              python -m utilities.cli join --batch-size 10 --delay 90 --batch-delay 600
        """),
    )
    _add_common_args(p)

    # Targets
    target_group = p.add_mutually_exclusive_group()
    target_group.add_argument(
        "--targets",
        dest="targets",
        nargs="+",
        default=[],
        metavar="TARGET",
        help="One or more Telegram group usernames or invite links to join.",
    )
    target_group.add_argument(
        "--targets-file",
        dest="targets_file",
        default=None,
        metavar="PATH",
        help="Text file with one target per line (# lines are comments).",
    )

    # Join parameters
    p.add_argument(
        "--batch-size",
        dest="batch_size",
        type=int,
        default=5,
        metavar="N",
        help="Number of accounts to join with per batch.  Default: 5",
    )
    p.add_argument(
        "--delay",
        dest="delay_between_joins",
        type=float,
        default=60.0,
        metavar="SEC",
        help="Seconds between individual joins within a batch.  Default: 60",
    )
    p.add_argument(
        "--batch-delay",
        dest="delay_between_batches",
        type=float,
        default=300.0,
        metavar="SEC",
        help="Seconds to wait between batches.  Default: 300",
    )
    p.add_argument(
        "--max-per-account",
        dest="max_joins_per_account",
        type=int,
        default=10,
        metavar="N",
        help="Maximum number of groups a single account may join.  Default: 10",
    )
    p.add_argument(
        "--max-per-hour",
        dest="max_joins_per_hour",
        type=int,
        default=5,
        metavar="N",
        help="Maximum joins per account per rolling hour.  Default: 5",
    )

    return p


def _build_fingerprint_parser(sub: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p: argparse.ArgumentParser = sub.add_parser(
        "fingerprint",
        help="Manage device fingerprints for accounts.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
            Device fingerprint management.

            Subcommands
            -----------
            report    — Print a diversity report for all accounts in the CSV.
            generate  — Pre-generate fingerprints for all accounts.
            rotate    — Force-rotate the fingerprint for one or all accounts.
            delete    — Remove a stored fingerprint.
            show      — Show the fingerprint for a specific account.
        """),
    )
    p.add_argument(
        "--csv",
        dest="accounts_csv",
        default=os.environ.get("BOOTSUSERS_CSV", "Telegram_70_000_000_[YOSCRYPT].csv"),
        help="Path to accounts CSV file.",
    )
    p.add_argument(
        "--store",
        dest="store_path",
        default=os.environ.get("BOOTSUSERS_FP_STORE", "fingerprints.json"),
        metavar="PATH",
        help="Path to fingerprint JSON store.  Default: fingerprints.json",
    )
    p.add_argument(
        "--phone",
        dest="phone",
        default=None,
        metavar="PHONE",
        help="Target a single phone number.  "
             "If omitted, all accounts in the CSV are targeted.",
    )
    p.add_argument(
        "--strategy",
        dest="strategy",
        default="auto",
        choices=["auto", "midrange", "flagship"],
        help="Device generation strategy.  "
             "auto: pick midrange or flagship based on hash of phone. "
             "midrange: Samsung/Xiaomi/realme mid-range. "
             "flagship: Samsung S+/Google Pixel.  Default: auto",
    )
    p.add_argument(
        "--force",
        dest="force",
        action="store_true",
        help="Rotate even if the fingerprint already exists.",
    )
    p.add_argument(
        "--log-level",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )

    # Fingerprint actions as a positional
    p.add_argument(
        "action",
        choices=["report", "generate", "rotate", "delete", "show"],
        help="Action to perform.",
    )

    return p


def _build_status_parser(sub: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p: argparse.ArgumentParser = sub.add_parser(
        "status",
        help="Show account health and pool statistics.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
            Probe all accounts in the CSV and print a health table.

            Checks:
            - Session file exists on disk
            - Stored fingerprint present (if fingerprint store is configured)
            - Proxy reachability (if proxy columns are present)

            Does NOT make any Telegram API calls unless --connect is passed.

            Examples
            --------
            Offline check (fast):
              python -m utilities.cli status --csv accounts.csv

            Live connection check (slow, makes API calls):
              python -m utilities.cli status --csv accounts.csv --connect
        """),
    )
    p.add_argument(
        "--csv",
        dest="accounts_csv",
        default=os.environ.get("BOOTSUSERS_CSV", "Telegram_70_000_000_[YOSCRYPT].csv"),
    )
    p.add_argument(
        "--fingerprint-store",
        dest="fingerprint_store",
        default=os.environ.get("BOOTSUSERS_FP_STORE", "fingerprints.json"),
        metavar="PATH",
    )
    p.add_argument(
        "--connect",
        dest="live_connect",
        action="store_true",
        help="Attempt live Telegram connections (requires api-id and api-hash).",
    )
    p.add_argument(
        "--api-id",
        dest="api_id",
        type=int,
        default=int(os.environ.get("TELEGRAM_API_ID", "34674448")),
    )
    p.add_argument(
        "--api-hash",
        dest="api_hash",
        default=os.environ.get("TELEGRAM_API_HASH", "7098ee8e75f3c435f808ee9410a2f05b"),
    )
    p.add_argument(
        "--log-level",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    p.add_argument(
        "--json",
        dest="output_json",
        action="store_true",
        help="Output as JSON instead of a human-readable table.",
    )
    return p


# ---------------------------------------------------------------------------
# CSV reader
# ---------------------------------------------------------------------------


def _read_csv_phones(path: str) -> List[str]:
    """Return list of phone numbers from a CSV file."""
    phones: List[str] = []
    if not os.path.exists(path):
        logger.error("CSV not found: %s", path)
        return phones
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            phone = row.get("phone", "").strip()
            if phone:
                phones.append(phone)
    return phones


def _read_targets_file(path: str) -> List[str]:
    """Read join targets from a text file, skipping blank lines and # comments."""
    targets: List[str] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                targets.append(line)
    return targets


# ---------------------------------------------------------------------------
# Command implementations
# ---------------------------------------------------------------------------


async def _cmd_react(cfg: ReactConfig) -> int:
    """
    Launch the reaction bot.

    Patches the global constants in auto_react.py with cfg values, then
    runs the module's main() coroutine.  All heavy lifting stays in
    auto_react.py; this layer only injects config.
    """
    logger.info("--- react command ---")
    logger.info("CSV:         %s", cfg.accounts_csv)
    logger.info("Mode:        %s", cfg.reaction_mode)
    logger.info("Emoji pool:  %s", cfg.reaction_pool)
    logger.info("Stagger:     %.1f – %.1fs", cfg.stagger_min_sec, cfg.stagger_max_sec)
    logger.info("Rate limit:  %d/hr, min gap %.1fs", cfg.max_reactions_per_hour, cfg.min_gap_between_reactions)
    logger.info("Health chk:  every %.0fs", cfg.health_check_interval)
    logger.info("Fingerprints: %s", "enabled" if cfg.use_fingerprints else "disabled")
    logger.info("Dry run:     %s", cfg.dry_run)

    # ---- Monkey-patch auto_react.py constants before importing ----
    # We import auto_react lazily so that setting these module-level names
    # before the import fires takes effect.  Since auto_react reads them at
    # call time (not at import time), setting them afterwards also works.
    try:
        import auto_react as _ar
    except ImportError:
        logger.error("auto_react.py not found — ensure it is on PYTHONPATH")
        return 1

    _ar.API_ID                     = cfg.api_id or _ar.API_ID
    _ar.API_HASH                   = cfg.api_hash or _ar.API_HASH
    _ar.ACCOUNTS_CSV               = cfg.accounts_csv
    _ar.REACTION_MODE              = cfg.reaction_mode       # type: ignore[attr-defined]
    _ar.STAGGER_MIN_SEC            = cfg.stagger_min_sec
    _ar.STAGGER_MAX_SEC            = cfg.stagger_max_sec
    _ar.MAX_REACTIONS_PER_HOUR     = cfg.max_reactions_per_hour
    _ar.MIN_GAP_BETWEEN_REACTIONS  = cfg.min_gap_between_reactions
    _ar.HEALTH_CHECK_INTERVAL      = cfg.health_check_interval

    # multi-emoji pool
    if cfg.reaction_pool and len(cfg.reaction_pool) > 1:
        _ar.REACTION_POOL = cfg.reaction_pool              # type: ignore[attr-defined]
    elif cfg.reaction_pool and len(cfg.reaction_pool) == 1:
        _ar.REACTION_EMOJI = cfg.reaction_pool[0]          # type: ignore[attr-defined]

    # Apply fingerprints if requested
    if cfg.use_fingerprints:
        try:
            from utilities.fingerprint_randomizer import (
                FingerprintRandomizer,
                apply_fingerprints,
            )
            phones = _read_csv_phones(cfg.accounts_csv)
            fp_rand = FingerprintRandomizer(
                store_path=cfg.fingerprint_store or "fingerprints.json"
            )
            fp_rand.bulk_create(phones)
            logger.info("Fingerprints generated for %d accounts", len(phones))
        except ImportError:
            logger.warning("fingerprint_randomizer not available — skipping fingerprints")
        except Exception as e:
            logger.warning("Fingerprint init error: %s", e)

    # Proxy file override
    if cfg.proxy_file:
        _inject_proxies_from_file(_ar, cfg.proxy_file)

    if cfg.dry_run:
        logger.info("DRY RUN — stopping before accounts connect")
        return 0

    # Run the actual main coroutine
    if hasattr(_ar, "main"):
        await _ar.main()
    else:
        # Fallback: call the top-level async entry point directly
        logger.error(
            "auto_react.py has no main() coroutine.  "
            "Add `async def main(): ...` to auto_react.py."
        )
        return 1

    return 0


async def _cmd_join(cfg: JoinConfig) -> int:
    """Launch the join bot."""
    logger.info("--- join command ---")
    logger.info("CSV:         %s", cfg.accounts_csv)

    # Resolve targets
    targets: List[str] = list(cfg.targets)
    if cfg.targets_file:
        try:
            targets += _read_targets_file(cfg.targets_file)
        except OSError as e:
            logger.error("Cannot read targets file: %s", e)
            return 1

    if not targets:
        logger.error("No targets specified — use --targets or --targets-file")
        return 1

    logger.info("Targets:     %d", len(targets))
    logger.info("Batch size:  %d", cfg.batch_size)
    logger.info("Join delay:  %.0fs", cfg.delay_between_joins)
    logger.info("Batch delay: %.0fs", cfg.delay_between_batches)
    logger.info("Fingerprints: %s", "enabled" if cfg.use_fingerprints else "disabled")
    logger.info("Dry run:     %s", cfg.dry_run)

    try:
        import auto_join as _aj
    except ImportError:
        logger.error("auto_join.py not found — ensure it is on PYTHONPATH")
        return 1

    _aj.API_ID                  = cfg.api_id or _aj.API_ID        # type: ignore[attr-defined]
    _aj.API_HASH                = cfg.api_hash or _aj.API_HASH    # type: ignore[attr-defined]
    _aj.ACCOUNTS_CSV            = cfg.accounts_csv
    _aj.BATCH_SIZE              = cfg.batch_size
    _aj.JOIN_DELAY_SEC          = cfg.delay_between_joins
    _aj.BATCH_DELAY_SEC         = cfg.delay_between_batches
    _aj.MAX_JOINS_PER_ACCOUNT   = cfg.max_joins_per_account

    if cfg.use_fingerprints:
        try:
            from utilities.fingerprint_randomizer import FingerprintRandomizer
            phones = _read_csv_phones(cfg.accounts_csv)
            fp_rand = FingerprintRandomizer(
                store_path=cfg.fingerprint_store or "fingerprints.json"
            )
            fp_rand.bulk_create(phones)
            logger.info("Fingerprints generated for %d accounts", len(phones))
        except ImportError:
            logger.warning("fingerprint_randomizer not available — skipping fingerprints")

    if cfg.dry_run:
        logger.info("DRY RUN — targets resolved: %s", targets[:5])
        return 0

    if hasattr(_aj, "main"):
        await _aj.main(targets=targets)
    elif hasattr(_aj, "run"):
        await _aj.run(targets)
    else:
        logger.error(
            "auto_join.py has no main() or run() coroutine.  "
            "Add `async def main(targets): ...` to auto_join.py."
        )
        return 1

    return 0


def _cmd_fingerprint(cfg: FingerprintConfig) -> int:
    """Handle fingerprint subcommands (synchronous)."""
    _configure_logging(cfg.log_level)

    try:
        from utilities.fingerprint_randomizer import (
            FingerprintRandomizer,
            MidRangeStrategy,
            FlagshipStrategy,
        )
    except ImportError:
        logger.error("fingerprint_randomizer module not found")
        return 1

    strategy_cls = None
    if cfg.strategy == "midrange":
        strategy_cls = MidRangeStrategy
    elif cfg.strategy == "flagship":
        strategy_cls = FlagshipStrategy

    fp = FingerprintRandomizer(
        store_path=cfg.store_path,
        default_strategy=strategy_cls() if strategy_cls else None,
    )

    action = cfg.action

    if action == "report":
        phones = _read_csv_phones(cfg.accounts_csv) if not cfg.phone else [cfg.phone]
        if not phones:
            logger.error("No phones found in %s", cfg.accounts_csv)
            return 1
        # generate any missing
        fp.bulk_create(phones)
        print(fp.report())
        return 0

    if action == "generate":
        phones = _read_csv_phones(cfg.accounts_csv) if not cfg.phone else [cfg.phone]
        if not phones:
            logger.error("No phones found in %s", cfg.accounts_csv)
            return 1
        created = 0
        for phone in phones:
            existing = fp.get(phone)
            if existing is None or cfg.force:
                fp.get_or_create(phone)
                created += 1
        logger.info("Done. %d fingerprints created/updated.", created)
        return 0

    if action == "rotate":
        if cfg.phone:
            result = fp.rotate(cfg.phone, force=cfg.force)
            if result:
                print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
            else:
                logger.error("No fingerprint found for %s", cfg.phone)
                return 1
        else:
            phones = _read_csv_phones(cfg.accounts_csv)
            if not phones:
                logger.error("No phones found in %s", cfg.accounts_csv)
                return 1
            results = fp.rotate_many(phones)
            logger.info("Rotated %d fingerprints", len(results))
        return 0

    if action == "delete":
        if not cfg.phone:
            logger.error("--phone is required for 'delete'")
            return 1
        fp.delete(cfg.phone)
        logger.info("Deleted fingerprint for %s", cfg.phone)
        return 0

    if action == "show":
        if not cfg.phone:
            logger.error("--phone is required for 'show'")
            return 1
        result = fp.get(cfg.phone)
        if result is None:
            logger.error("No fingerprint found for %s", cfg.phone)
            return 1
        print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
        return 0

    logger.error("Unknown action: %s", action)
    return 1


async def _cmd_status(
    accounts_csv: str,
    fingerprint_store: Optional[str],
    live_connect: bool,
    api_id: int,
    api_hash: str,
    output_json: bool,
) -> int:
    """Print a health status table for all accounts."""
    if not os.path.exists(accounts_csv):
        logger.error("CSV not found: %s", accounts_csv)
        return 1

    rows: List[dict] = []
    with open(accounts_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({k: v.strip() for k, v in row.items()})

    # Load fingerprint store if available
    fp_map: Dict[str, bool] = {}
    if fingerprint_store and os.path.exists(fingerprint_store):
        try:
            from utilities.fingerprint_randomizer import FingerprintRandomizer
            fp = FingerprintRandomizer(store_path=fingerprint_store)
            for phone, _ in fp.all_fingerprints().items():
                fp_map[phone] = True
        except Exception as e:
            logger.warning("Could not load fingerprints: %s", e)

    results: List[dict] = []

    for row in rows:
        phone   = row.get("phone", "?")
        session = row.get("session_name", "")

        # Check session file on disk
        session_ok = False
        for ext in ("", ".session"):
            if os.path.exists(session + ext):
                session_ok = True
                break

        entry: dict = {
            "phone":        phone,
            "session":      session,
            "session_ok":   session_ok,
            "fingerprint":  fp_map.get(phone, False),
            "proxy_type":   row.get("proxy_type", ""),
            "proxy_host":   row.get("proxy_host", ""),
        }

        if live_connect:
            connected, me_id = await _probe_account(
                phone=phone,
                session=session,
                api_id=api_id,
                api_hash=api_hash,
                proxy_type=row.get("proxy_type", ""),
                proxy_host=row.get("proxy_host", ""),
                proxy_port=int(row["proxy_port"]) if row.get("proxy_port", "").strip() else 0,
                proxy_user=row.get("proxy_user", ""),
                proxy_pass=row.get("proxy_pass", ""),
            )
            entry["connected"] = connected
            entry["me_id"]     = me_id

        results.append(entry)

    if output_json:
        print(json.dumps(results, indent=2, ensure_ascii=False))
        return 0

    # Human-readable table
    _print_status_table(results, include_connect=live_connect)
    return 0


async def _probe_account(
    phone: str,
    session: str,
    api_id: int,
    api_hash: str,
    proxy_type: str,
    proxy_host: str,
    proxy_port: int,
    proxy_user: str,
    proxy_pass: str,
) -> tuple[bool, Optional[int]]:
    """
    Attempt a live Telegram connection and return (success, user_id).
    Disconnects immediately after.
    """
    try:
        import socks
        proxy = None
        if proxy_type.lower() == "socks5":
            proxy = (
                socks.SOCKS5,
                proxy_host,
                proxy_port,
                True,
                proxy_user or None,
                proxy_pass or None,
            )
        elif proxy_type.lower() == "http":
            proxy = (
                socks.HTTP,
                proxy_host,
                proxy_port,
                True,
                proxy_user or None,
                proxy_pass or None,
            )
    except ImportError:
        proxy = None

    from telethon import TelegramClient
    try:
        client = TelegramClient(session, api_id, api_hash, proxy=proxy)
        await asyncio.wait_for(client.connect(), timeout=15.0)
        me = await asyncio.wait_for(client.get_me(), timeout=10.0)
        await client.disconnect()
        return True, (me.id if me else None)
    except Exception as e:
        logger.debug("Probe failed for %s: %s", phone, e)
        try:
            await client.disconnect()
        except Exception:
            pass
        return False, None


def _print_status_table(rows: List[dict], include_connect: bool) -> None:
    """Print a formatted ASCII table of account status."""
    COL_W = {
        "phone":       14,
        "session":     22,
        "session_ok":   8,
        "fingerprint":  6,
        "proxy":       22,
        "connected":    9,
    }

    def cell(value: Any, width: int, right: bool = False) -> str:
        s = str(value)
        if len(s) > width:
            s = s[: width - 2] + ".."
        return s.rjust(width) if right else s.ljust(width)

    headers = ["phone", "session", "ses_ok", "fp", "proxy"]
    if include_connect:
        headers.append("conn")

    sep = " | "
    widths = [COL_W.get(h, 10) for h in headers]

    # Header row
    header_row = sep.join(cell(h.upper(), w) for h, w in zip(headers, widths))
    print(header_row)
    print("-" * len(header_row))

    for r in rows:
        proxy_str = r.get("proxy_type", "") + ("@" + r.get("proxy_host", "") if r.get("proxy_host") else "")
        values = [
            r.get("phone", "?"),
            r.get("session", ""),
            "✓" if r.get("session_ok") else "✗",
            "✓" if r.get("fingerprint") else "—",
            proxy_str or "—",
        ]
        if include_connect:
            conn_str = "✓ " + str(r.get("me_id", "")) if r.get("connected") else "✗"
            values.append(conn_str)
        print(sep.join(cell(v, w) for v, w in zip(values, widths)))

    print()
    total = len(rows)
    ses_ok = sum(1 for r in rows if r.get("session_ok"))
    fp_ok  = sum(1 for r in rows if r.get("fingerprint"))
    print(f"Total: {total} accounts  |  Sessions OK: {ses_ok}/{total}  |  Fingerprints: {fp_ok}/{total}")
    if include_connect:
        connected = sum(1 for r in rows if r.get("connected"))
        print(f"Connected: {connected}/{total}")


# ---------------------------------------------------------------------------
# Proxy file injection
# ---------------------------------------------------------------------------


def _inject_proxies_from_file(module: Any, path: str) -> None:
    """
    Read a proxy list file and patch the module so that
    connect_account() uses the proxies in round-robin order.

    File format (one per line):
        socks5://user:pass@host:port
        http://host:port
        socks5://host:port   (no auth)

    Lines starting with # are skipped.
    """
    proxies: List[str] = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    proxies.append(line)
    except OSError as e:
        logger.error("Cannot read proxy file %s: %s", path, e)
        return

    if not proxies:
        logger.warning("Proxy file %s is empty — no proxies loaded", path)
        return

    logger.info("Loaded %d proxies from %s", len(proxies), path)
    # Store on the module for downstream use
    module._CLI_PROXY_LIST = proxies   # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Config → dataclass converters
# ---------------------------------------------------------------------------


def _args_to_react_config(args: argparse.Namespace) -> ReactConfig:
    return ReactConfig(
        api_id=args.api_id,
        api_hash=args.api_hash,
        accounts_csv=args.accounts_csv,
        sender_csv=getattr(args, "sender_csv", None),
        reaction_pool=getattr(args, "reaction_pool", None) or None,
        reaction_mode=getattr(args, "reaction_mode", "staggered"),
        stagger_min_sec=getattr(args, "stagger_min_sec", 1.0),
        stagger_max_sec=getattr(args, "stagger_max_sec", 5.0),
        max_reactions_per_hour=getattr(args, "max_reactions_per_hour", 30),
        min_gap_between_reactions=getattr(args, "min_gap_between_reactions", 2.0),
        health_check_interval=getattr(args, "health_check_interval", 120.0),
        fingerprint_store=getattr(args, "fingerprint_store", None),
        use_fingerprints=getattr(args, "use_fingerprints", True),
        proxy_file=getattr(args, "proxy_file", None),
        dry_run=args.dry_run,
    )


def _args_to_join_config(args: argparse.Namespace) -> JoinConfig:
    return JoinConfig(
        api_id=args.api_id,
        api_hash=args.api_hash,
        accounts_csv=args.accounts_csv,
        targets=getattr(args, "targets", []) or [],
        targets_file=getattr(args, "targets_file", None),
        batch_size=getattr(args, "batch_size", 5),
        delay_between_joins=getattr(args, "delay_between_joins", 60.0),
        delay_between_batches=getattr(args, "delay_between_batches", 300.0),
        max_joins_per_account=getattr(args, "max_joins_per_account", 10),
        max_joins_per_hour=getattr(args, "max_joins_per_hour", 5),
        fingerprint_store=getattr(args, "fingerprint_store", None),
        use_fingerprints=getattr(args, "use_fingerprints", True),
        proxy_file=getattr(args, "proxy_file", None),
        dry_run=args.dry_run,
    )


def _args_to_fingerprint_config(args: argparse.Namespace) -> FingerprintConfig:
    return FingerprintConfig(
        accounts_csv=args.accounts_csv,
        store_path=args.store_path,
        phone=args.phone,
        strategy=args.strategy,
        force=args.force,
        log_level=args.log_level,
    )


# ---------------------------------------------------------------------------
# Root parser
# ---------------------------------------------------------------------------


def _build_root_parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="bootsusers",
        description=textwrap.dedent("""\
            Bootsusers — Telegram automation suite.
            Manage accounts, reactions, group joins, and device fingerprints
            through a single CLI without editing config files.

            Available subcommands:

              react        — Start the reaction bot (auto_react.py)
              join         — Start the group-join bot (auto_join.py)
              fingerprint  — Manage per-account device fingerprints
              status       — Show account health and pool statistics

            Run `bootsusers <subcommand> --help` for detailed options.

            Environment variables
            ---------------------
            BOOTSUSERS_CSV       — default accounts CSV path
            BOOTSUSERS_FP_STORE  — default fingerprint store path
            TELEGRAM_API_ID      — Telegram API ID (overrides --api-id)
            TELEGRAM_API_HASH    — Telegram API hash (overrides --api-hash)
        """),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    root.add_argument(
        "--version",
        action="version",
        version="%(prog)s 1.0.0",
    )
    return root


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    """Build and return the fully configured argument parser."""
    root = _build_root_parser()
    sub = root.add_subparsers(dest="command", metavar="<subcommand>")
    sub.required = True

    _build_react_parser(sub)
    _build_join_parser(sub)
    _build_fingerprint_parser(sub)
    _build_status_parser(sub)

    return root


def main(argv: Optional[List[str]] = None) -> int:
    """
    CLI entry point.

    Returns an exit code (0 = success, non-zero = error).
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    # Fingerprint command is synchronous
    if args.command == "fingerprint":
        cfg = _args_to_fingerprint_config(args)
        return _cmd_fingerprint(cfg)

    # All other commands are async
    _configure_logging(
        getattr(args, "log_level", "INFO"),
        getattr(args, "log_file", None),
    )

    if args.command == "react":
        cfg = _args_to_react_config(args)
        return asyncio.run(_cmd_react(cfg))

    if args.command == "join":
        cfg = _args_to_join_config(args)
        return asyncio.run(_cmd_join(cfg))

    if args.command == "status":
        return asyncio.run(
            _cmd_status(
                accounts_csv=args.accounts_csv,
                fingerprint_store=args.fingerprint_store,
                live_connect=args.live_connect,
                api_id=args.api_id,
                api_hash=args.api_hash,
                output_json=args.output_json,
            )
        )

    logger.error("Unknown command: %s", args.command)
    return 1


# ---------------------------------------------------------------------------
# __main__ support — allows both:
#   python utilities/cli.py react ...
#   python -m utilities.cli react ...
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    sys.exit(main())