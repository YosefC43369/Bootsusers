#!/usr/bin/env python3
# ==============================================================================
# install_dependencies.py
# ------------------------------------------------------------------------------
# Self-contained dependency bootstrapper for the Bootsusers Telegram suite.
#
# Responsibilities:
#   * verify the running interpreter is new enough,
#   * make sure pip / setuptools / wheel are present and current,
#   * install every runtime requirement (telethon, aiohttp, customtkinter, …)
#     with network-aware retry + exponential backoff,
#   * degrade gracefully when an OPTIONAL package (customtkinter on a headless
#     host, cryptg where no wheel exists) cannot be built,
#   * verify the result by actually importing each module and probing the two
#     Telethon API surfaces the suite depends on (paid reactions + stars),
#   * print a precise, human-readable report and exit non-zero on hard failure.
#
# Usage:
#     python install_dependencies.py                # install everything
#     python install_dependencies.py --no-gui       # skip customtkinter
#     python install_dependencies.py --user         # pip --user install
#     python install_dependencies.py --upgrade      # force-upgrade all pins
#     python install_dependencies.py --dry-run      # show plan, install nothing
#     python install_dependencies.py --verify-only  # only run the import probes
#
# The script has ZERO third-party imports of its own, so it can run on a bare
# interpreter before anything is installed.
# ==============================================================================

from __future__ import annotations

import argparse
import importlib
import os
import platform
import subprocess
import sys
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

# ------------------------------------------------------------------------------
# Terminal colour helpers (disabled automatically when not a TTY or on legacy
# Windows consoles that don't understand ANSI).
# ------------------------------------------------------------------------------

_SUPPORTS_COLOR = (
    sys.stdout.isatty()
    and os.environ.get("NO_COLOR") is None
    and (platform.system() != "Windows" or "WT_SESSION" in os.environ
         or os.environ.get("TERM") == "xterm")
)


def _c(code: str, text: str) -> str:
    if not _SUPPORTS_COLOR:
        return text
    return f"\033[{code}m{text}\033[0m"


def ok(t: str) -> str:    return _c("32", t)   # green
def warn(t: str) -> str:  return _c("33", t)   # yellow
def err(t: str) -> str:   return _c("31", t)   # red
def bold(t: str) -> str:  return _c("1", t)    # bold
def dim(t: str) -> str:   return _c("2", t)    # dim


# ------------------------------------------------------------------------------
# Minimum supported runtime.
# ------------------------------------------------------------------------------

MIN_PYTHON = (3, 9)


# ------------------------------------------------------------------------------
# A single installable requirement.
#
#   spec        — exact pip requirement string (may carry version pins / markers)
#   import_name — module name used by the post-install import probe
#   optional    — True ⇒ a build/install failure is a WARNING, not an error
#   reason      — one-line human description for the report
#   probe       — optional callable returning (ok: bool, detail: str) run after
#                 import, used to confirm a specific API surface exists.
# ------------------------------------------------------------------------------

@dataclass
class Requirement:
    spec: str
    import_name: str
    optional: bool = False
    reason: str = ""
    probe: Optional[Callable[[], "tuple[bool, str]"]] = None

    @property
    def pip_name(self) -> str:
        # strip version pins / environment markers for display
        for sep in (">=", "<=", "==", "~=", ">", "<", ";", "!="):
            if sep in self.spec:
                return self.spec.split(sep, 1)[0].strip()
        return self.spec.strip()


# ------------------------------------------------------------------------------
# API probes — run inside the *target* interpreter after install so we fail
# loudly if a too-old Telethon silently satisfied the version pin.
# ------------------------------------------------------------------------------

def _probe_telethon_paid_api() -> "tuple[bool, str]":
    try:
        from telethon.tl.functions.messages import SendPaidReactionRequest  # noqa
        from telethon.tl.functions.payments import GetStarsStatusRequest    # noqa
        from telethon.tl.types import (                                     # noqa
            PaidReactionPrivacyDefault,
            PaidReactionPrivacyAnonymous,
            PaidReactionPrivacyPeer,
            StarsAmount,
        )
        import telethon
        return True, f"paid-star API present (telethon {telethon.__version__})"
    except Exception as exc:  # pragma: no cover - defensive
        return False, f"missing paid-star API: {exc}"


def _probe_aiohttp() -> "tuple[bool, str]":
    try:
        import aiohttp
        return True, f"aiohttp {aiohttp.__version__}"
    except Exception as exc:  # pragma: no cover
        return False, str(exc)


def _probe_customtkinter() -> "tuple[bool, str]":
    # Importing customtkinter spins up a hidden Tk root on some platforms, which
    # fails on a headless box even when the package is installed correctly.
    # Treat "installed but no display" as a soft success.
    try:
        import customtkinter
        return True, f"customtkinter {getattr(customtkinter, '__version__', '?')}"
    except ImportError as exc:
        return False, f"not importable: {exc}"
    except Exception as exc:
        # TclError / no $DISPLAY — the package is there, just can't open a window
        return True, f"installed (no display: {type(exc).__name__})"


# ------------------------------------------------------------------------------
# The canonical dependency set, kept in lockstep with requirements.txt.
# ------------------------------------------------------------------------------

REQUIREMENTS: "list[Requirement]" = [
    Requirement(
        spec="telethon>=1.45.0,<2.0.0",
        import_name="telethon",
        optional=False,
        reason="MTProto client — joins, reactions, paid-star reactions",
        probe=_probe_telethon_paid_api,
    ),
    Requirement(
        spec="aiohttp>=3.9.0,<4.0.0",
        import_name="aiohttp",
        optional=False,
        reason="async HTTP for proxy health-probing",
        probe=_probe_aiohttp,
    ),
    Requirement(
        spec="PySocks>=1.7.1",
        import_name="socks",
        optional=False,
        reason="SOCKS5/HTTP proxy transport for Telethon",
    ),
    Requirement(
        spec="pyaes>=1.6.1",
        import_name="pyaes",
        optional=False,
        reason="pure-python AES fallback (Telethon crypto)",
    ),
    Requirement(
        spec="rsa>=4.9",
        import_name="rsa",
        optional=False,
        reason="RSA key handling (Telethon auth)",
    ),
    Requirement(
        spec="cryptg>=0.4.0",
        import_name="cryptg",
        optional=True,
        reason="optional C-accelerated AES-IGE (5-10x faster)",
    ),
    Requirement(
        spec="customtkinter>=5.2.2,<6.0.0",
        import_name="customtkinter",
        optional=True,
        reason="desktop GUI for launcher.py (Windows 11)",
        probe=_probe_customtkinter,
    ),
]


# ------------------------------------------------------------------------------
# Result record for the final report.
# ------------------------------------------------------------------------------

@dataclass
class InstallOutcome:
    req: Requirement
    installed: bool = False
    imported: bool = False
    probed_ok: bool = True
    detail: str = ""
    skipped: bool = False

    @property
    def is_hard_failure(self) -> bool:
        if self.req.optional or self.skipped:
            return False
        return not (self.imported and self.probed_ok)


# ------------------------------------------------------------------------------
# Low-level pip driver.
# ------------------------------------------------------------------------------

def _run(cmd: "list[str]", *, dry_run: bool) -> "tuple[int, str]":
    """Run a subprocess, streaming nothing, capturing combined output."""
    printable = " ".join(cmd)
    print(dim(f"    $ {printable}"))
    if dry_run:
        return 0, "(dry-run)"
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        return proc.returncode, proc.stdout or ""
    except Exception as exc:  # pragma: no cover - subprocess launch failure
        return 1, f"failed to launch pip: {exc}"


def _pip_base(user: bool) -> "list[str]":
    base = [sys.executable, "-m", "pip", "install", "--disable-pip-version-check"]
    if user and not _in_virtualenv():
        base.append("--user")
    return base


def _in_virtualenv() -> bool:
    return (
        hasattr(sys, "real_prefix")
        or (hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix)
        or os.environ.get("VIRTUAL_ENV") is not None
    )


def pip_install(
    spec: str,
    *,
    user: bool,
    upgrade: bool,
    dry_run: bool,
    retries: int = 3,
    backoff: float = 2.5,
) -> "tuple[bool, str]":
    """Install one spec with retry + exponential backoff on transient errors."""
    cmd = _pip_base(user)
    if upgrade:
        cmd.append("--upgrade")
    cmd.append(spec)

    last_output = ""
    for attempt in range(1, retries + 1):
        code, output = _run(cmd, dry_run=dry_run)
        last_output = output
        if code == 0:
            return True, output
        transient = any(
            token in output.lower()
            for token in (
                "timed out", "timeout", "connection", "temporary failure",
                "network", "reset by peer", "read timed out", "503", "502",
            )
        )
        if attempt < retries and transient:
            wait = backoff ** attempt
            print(warn(f"    transient pip error (attempt {attempt}/{retries}) "
                       f"— retrying in {wait:.1f}s"))
            time.sleep(wait)
            continue
        break
    return False, last_output


# ------------------------------------------------------------------------------
# Pre-flight: interpreter + build tooling.
# ------------------------------------------------------------------------------

def check_python() -> bool:
    print(bold("\n[1/4] Interpreter check"))
    v = sys.version_info
    print(f"    python   : {platform.python_implementation()} "
          f"{v.major}.{v.minor}.{v.micro}")
    print(f"    platform : {platform.system()} {platform.release()} "
          f"({platform.machine()})")
    print(f"    venv     : {'yes' if _in_virtualenv() else 'no'}  "
          f"({sys.prefix})")
    if (v.major, v.minor) < MIN_PYTHON:
        print(err(f"    ✗ Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ required, "
                  f"found {v.major}.{v.minor}"))
        return False
    print(ok("    ✓ interpreter OK"))
    return True


def ensure_build_tools(user: bool, dry_run: bool) -> None:
    print(bold("\n[2/4] Build tooling"))
    # A stale setuptools is the #1 cause of pyaes/cryptg wheel build failures
    # (the infamous `install_layout` AttributeError on old Debian setuptools).
    ok_, out = pip_install(
        "pip", user=user, upgrade=True, dry_run=dry_run, retries=2
    )
    for pkg in ("setuptools", "wheel"):
        got, out = pip_install(
            pkg, user=user, upgrade=True, dry_run=dry_run, retries=2
        )
        status = ok("✓") if got else warn("△")
        print(f"    {status} {pkg}")


# ------------------------------------------------------------------------------
# Install + verify loop.
# ------------------------------------------------------------------------------

def install_all(
    reqs: "list[Requirement]",
    *,
    user: bool,
    upgrade: bool,
    dry_run: bool,
    skip_gui: bool,
) -> "list[InstallOutcome]":
    print(bold("\n[3/4] Installing requirements"))
    outcomes: "list[InstallOutcome]" = []

    for req in reqs:
        if skip_gui and req.import_name == "customtkinter":
            print(f"  • {req.pip_name:<16} {dim('skipped (--no-gui)')}")
            outcomes.append(InstallOutcome(req=req, skipped=True,
                                           detail="skipped via --no-gui"))
            continue

        tag = dim("[optional]") if req.optional else ""
        print(f"  • {req.pip_name:<16} {tag}  {dim(req.reason)}")
        got, output = pip_install(
            req.spec, user=user, upgrade=upgrade, dry_run=dry_run
        )
        outcome = InstallOutcome(req=req, installed=got)
        if not got:
            tail = _tail(output, 4)
            if req.optional:
                print(warn(f"    △ optional install failed — continuing\n{tail}"))
                outcome.detail = "optional build/install failed"
            else:
                print(err(f"    ✗ install failed\n{tail}"))
                outcome.detail = "install failed"
        outcomes.append(outcome)

    return outcomes


def verify_all(outcomes: "list[InstallOutcome]", *, dry_run: bool) -> None:
    print(bold("\n[4/4] Verifying imports"))
    if dry_run:
        print(dim("    (dry-run — skipping live import probes)"))
        return

    for outcome in outcomes:
        req = outcome.req
        if outcome.skipped:
            continue
        # Import must happen in a clean sense: drop any cached partial import.
        sys.modules.pop(req.import_name, None)
        try:
            importlib.invalidate_caches()
            importlib.import_module(req.import_name)
            outcome.imported = True
        except Exception as exc:
            outcome.imported = False
            outcome.detail = f"import failed: {exc}"
            marker = warn("△") if req.optional else err("✗")
            print(f"    {marker} import {req.import_name:<16} {outcome.detail}")
            continue

        if req.probe is not None:
            probe_ok, probe_detail = req.probe()
            outcome.probed_ok = probe_ok
            outcome.detail = probe_detail
            marker = ok("✓") if probe_ok else (
                warn("△") if req.optional else err("✗"))
            print(f"    {marker} import {req.import_name:<16} {probe_detail}")
        else:
            print(f"    {ok('✓')} import {req.import_name:<16} ok")


def _tail(text: str, lines: int) -> str:
    rows = [r for r in text.splitlines() if r.strip()]
    return "\n".join("      " + r for r in rows[-lines:])


# ------------------------------------------------------------------------------
# Final report + exit code.
# ------------------------------------------------------------------------------

def report(outcomes: "list[InstallOutcome]") -> int:
    print(bold("\n" + "=" * 70))
    print(bold(" Summary"))
    print(bold("=" * 70))

    hard_failures = [o for o in outcomes if o.is_hard_failure]
    soft_warnings = [
        o for o in outcomes
        if not o.is_hard_failure and (o.skipped or o.req.optional)
        and not (o.imported and o.probed_ok)
    ]

    for o in outcomes:
        name = o.req.pip_name
        if o.skipped:
            line = f" {warn('SKIP'):<6} {name:<16} {o.detail}"
        elif o.is_hard_failure:
            line = f" {err('FAIL'):<6} {name:<16} {o.detail or 'unavailable'}"
        elif o.req.optional and not (o.imported and o.probed_ok):
            line = f" {warn('WARN'):<6} {name:<16} {o.detail or 'optional, absent'}"
        else:
            line = f" {ok('OK'):<6} {name:<16} {o.detail or 'ready'}"
        print(line)

    print(bold("-" * 70))
    if hard_failures:
        print(err(f" {len(hard_failures)} required dependency(ies) failed."))
        print(" Fix the errors above, then re-run:  "
              + bold("python install_dependencies.py"))
        return 1

    if soft_warnings:
        print(ok(" All REQUIRED dependencies are installed and verified."))
        print(warn(f" {len(soft_warnings)} optional component(s) absent "
                   f"(the CLI works; the GUI launcher may not)."))
    else:
        print(ok(" All dependencies installed and verified. You're ready."))

    print(dim("\n Next:"))
    print(dim("   python auto_star.py   --help      # paid-star reaction bot"))
    print(dim("   python auto_join.py               # channel joiner"))
    print(dim("   python auto_react.py              # emoji reaction bot"))
    print(dim("   python utilities/cli.py --help    # unified CLI"))
    print(dim("   python launcher.py                # desktop GUI (needs customtkinter)"))
    return 0


# ------------------------------------------------------------------------------
# Entry point.
# ------------------------------------------------------------------------------

def main(argv: "Optional[list[str]]" = None) -> int:
    parser = argparse.ArgumentParser(
        prog="install_dependencies.py",
        description="Install & verify the Bootsusers Telegram-suite dependencies.",
    )
    parser.add_argument("--no-gui", action="store_true",
                        help="skip customtkinter (headless / server install)")
    parser.add_argument("--user", action="store_true",
                        help="pip --user install (ignored inside a virtualenv)")
    parser.add_argument("--upgrade", action="store_true",
                        help="force-upgrade packages already installed")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the plan without installing anything")
    parser.add_argument("--verify-only", action="store_true",
                        help="skip installation, only run the import probes")
    parser.add_argument("--no-build-tools", action="store_true",
                        help="do not touch pip/setuptools/wheel")
    args = parser.parse_args(argv)

    print(bold("Bootsusers — dependency bootstrapper"))

    if not check_python():
        return 1

    if args.verify_only:
        outcomes = [InstallOutcome(req=r) for r in REQUIREMENTS]
        if args.no_gui:
            for o in outcomes:
                if o.req.import_name == "customtkinter":
                    o.skipped = True
        verify_all(outcomes, dry_run=False)
        return report(outcomes)

    if not args.no_build_tools:
        ensure_build_tools(user=args.user, dry_run=args.dry_run)

    outcomes = install_all(
        REQUIREMENTS,
        user=args.user,
        upgrade=args.upgrade,
        dry_run=args.dry_run,
        skip_gui=args.no_gui,
    )
    verify_all(outcomes, dry_run=args.dry_run)
    return report(outcomes)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print(err("\nInterrupted."))
        raise SystemExit(130)
