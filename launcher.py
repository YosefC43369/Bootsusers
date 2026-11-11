# launcher.py, target: Windows 11 (tkinter + customtkinter)
# Build to .exe:  pyinstaller --onefile --windowed --icon=assets/icon.ico launcher.py

"""
Bootsusers Launcher — Cybersecurity Team UI
Font: Hack (bundled via assets/Hack-Regular.ttf)

Features
--------
- Select how many accounts to use for reactions (1 → N)
- Select how many accounts to use for auto-join (1 → N)
- Load accounts CSV
- Configure chat/channel targets inline
- Start / Stop each operation independently
- Live log console with colour-coded output
- Status strip (healthy accounts, queue depth, sent count)
"""

from __future__ import annotations

import asyncio
import csv
import os
import sys
import threading
import time
import queue as std_queue
from pathlib import Path
from typing import Optional

# ── customtkinter ─────────────────────────────────────────────────────────────
try:
    import customtkinter as ctk
except ImportError:
    sys.exit(
        "customtkinter not installed.\n"
        "Run:  pip install customtkinter\n"
    )

# ── optional: load Hack font if bundled ───────────────────────────────────────
try:
    from tkinter import font as tkfont
    import tkinter as tk
    _HACK_PATH = Path(__file__).parent / "assets" / "Hack-Regular.ttf"
    if _HACK_PATH.exists():
        # Tk does not support direct TTF loading; we rely on the system having
        # Hack installed, or fall back gracefully to Courier New.
        _FONT_FAMILY = "Hack"
    else:
        _FONT_FAMILY = "Courier New"
except Exception:
    _FONT_FAMILY = "Courier New"


# ═══════════════════════════════════════════════════════════════════════════════
#  COLOUR PALETTE  — cybersecurity / terminal aesthetic
# ═══════════════════════════════════════════════════════════════════════════════

C = {
    "bg":          "#0a0e14",   # near-black base
    "panel":       "#0d1117",   # panel / sidebar
    "border":      "#1e2a38",   # subtle border
    "accent":      "#00d4ff",   # electric cyan — primary interactive
    "accent2":     "#7f5af0",   # purple accent
    "warn":        "#f5a623",   # amber warning
    "danger":      "#ff4c6a",   # red error / ban
    "ok":          "#2cb67d",   # green success
    "text":        "#cdd6f4",   # main text
    "text_dim":    "#6e7f8d",   # secondary / dim
    "text_bright": "#ffffff",
    "log_bg":      "#080c11",
    "log_info":    "#cdd6f4",
    "log_ok":      "#2cb67d",
    "log_warn":    "#f5a623",
    "log_err":     "#ff4c6a",
    "log_accent":  "#00d4ff",
    "btn_hover":   "#0099bb",
    "btn_active":  "#005f75",
}

# ═══════════════════════════════════════════════════════════════════════════════
#  LOG BRIDGE  — thread-safe pipe from asyncio → Tkinter
# ═══════════════════════════════════════════════════════════════════════════════

class LogBridge:
    """
    Thread-safe message queue.
    asyncio side pushes (level, text) tuples;
    Tkinter polls via after() and drains them into the console widget.
    """
    INFO    = "info"
    OK      = "ok"
    WARN    = "warn"
    ERR     = "err"
    ACCENT  = "accent"

    def __init__(self):
        self._q: std_queue.Queue[tuple[str, str]] = std_queue.Queue()

    def push(self, level: str, text: str) -> None:
        ts = time.strftime("%H:%M:%S")
        self._q.put_nowait((level, f"[{ts}] {text}"))

    def drain(self) -> list[tuple[str, str]]:
        out = []
        try:
            while True:
                out.append(self._q.get_nowait())
        except std_queue.Empty:
            pass
        return out


log_bridge = LogBridge()


# ═══════════════════════════════════════════════════════════════════════════════
#  ASYNC RUNNER  — single background event loop for all Telethon work
# ═══════════════════════════════════════════════════════════════════════════════

class AsyncRunner:
    """
    Runs a persistent asyncio event loop in a daemon thread.
    Use submit() to schedule coroutines from the main (Tkinter) thread.
    """

    def __init__(self):
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever,
            daemon=True,
            name="bootsusers-async",
        )
        self._thread.start()

    def submit(self, coro) -> asyncio.Future:
        if self._loop is None:
            raise RuntimeError("AsyncRunner not started")
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def stop(self) -> None:
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)


runner = AsyncRunner()


# ═══════════════════════════════════════════════════════════════════════════════
#  ACCOUNT LOADER
# ═══════════════════════════════════════════════════════════════════════════════

def load_csv(path: str) -> list[dict]:
    """Return rows from a CSV as list[dict].  Returns [] on error."""
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception as e:
        log_bridge.push(LogBridge.ERR, f"CSV load failed: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════════════════
#  WIDGETS — reusable styled components
# ═══════════════════════════════════════════════════════════════════════════════

def _font(size: int = 12, weight: str = "normal") -> tuple:
    return (_FONT_FAMILY, size, weight)


class SectionHeader(ctk.CTkLabel):
    def __init__(self, parent, text: str, **kwargs):
        super().__init__(
            parent,
            text=f"  ▸ {text}",
            font=_font(11, "bold"),
            text_color=C["accent"],
            anchor="w",
            **kwargs,
        )


class Divider(ctk.CTkFrame):
    def __init__(self, parent, **kwargs):
        super().__init__(
            parent,
            height=1,
            fg_color=C["border"],
            **kwargs,
        )


class CyberButton(ctk.CTkButton):
    def __init__(self, parent, text: str, command=None,
                 color: str = "accent", **kwargs):
        fg   = C[color]
        hover = C.get(color + "_hover", C["btn_hover"])
        super().__init__(
            parent,
            text=text,
            command=command,
            font=_font(11, "bold"),
            fg_color=fg,
            hover_color=hover,
            text_color=C["bg"],
            corner_radius=4,
            border_width=0,
            **kwargs,
        )


class StopButton(ctk.CTkButton):
    def __init__(self, parent, text: str = "■ STOP", command=None, **kwargs):
        super().__init__(
            parent,
            text=text,
            command=command,
            font=_font(11, "bold"),
            fg_color=C["danger"],
            hover_color="#cc0033",
            text_color=C["text_bright"],
            corner_radius=4,
            **kwargs,
        )


class AccountSlider(ctk.CTkFrame):
    """
    Labelled slider: 'Use N of M accounts'
    Returns the selected count via .get()
    """

    def __init__(self, parent, label: str, max_val: int = 10, **kwargs):
        super().__init__(
            parent,
            fg_color="transparent",
            **kwargs,
        )
        self._var = ctk.IntVar(value=max_val)
        self._max = max_val

        self._label = ctk.CTkLabel(
            self, text=label,
            font=_font(10), text_color=C["text_dim"],
            anchor="w",
        )
        self._label.grid(row=0, column=0, sticky="w", padx=(0, 8))

        self._slider = ctk.CTkSlider(
            self,
            from_=1, to=max(max_val, 1),
            number_of_steps=max(max_val - 1, 1),
            variable=self._var,
            width=160,
            button_color=C["accent"],
            button_hover_color=C["btn_hover"],
            progress_color=C["accent"],
            fg_color=C["border"],
            command=self._on_slide,
        )
        self._slider.grid(row=0, column=1, padx=(0, 8))

        self._count_lbl = ctk.CTkLabel(
            self,
            text=f"{max_val}/{max_val}",
            font=_font(10, "bold"),
            text_color=C["accent"],
            width=48,
        )
        self._count_lbl.grid(row=0, column=2)

    def _on_slide(self, value):
        n = int(value)
        self._var.set(n)
        self._count_lbl.configure(text=f"{n}/{self._max}")

    def get(self) -> int:
        return int(self._var.get())

    def update_max(self, new_max: int):
        self._max = new_max
        self._slider.configure(to=max(new_max, 1),
                               number_of_steps=max(new_max - 1, 1))
        v = min(self.get(), new_max)
        self._var.set(v)
        self._count_lbl.configure(text=f"{v}/{new_max}")


class StatusBar(ctk.CTkFrame):
    """Bottom strip — healthy / queue / sent counters."""

    def __init__(self, parent, **kwargs):
        super().__init__(
            parent,
            fg_color=C["panel"],
            height=28,
            corner_radius=0,
            **kwargs,
        )
        self._cells: dict[str, ctk.CTkLabel] = {}
        specs = [
            ("accounts", "ACCOUNTS  0"),
            ("queue",    "QUEUE     0"),
            ("sent",     "SENT      0"),
            ("banned",   "BANNED    0"),
            ("status",   "IDLE"),
        ]
        for i, (key, default) in enumerate(specs):
            lbl = ctk.CTkLabel(
                self,
                text=default,
                font=_font(9, "bold"),
                text_color=C["text_dim"],
                anchor="w",
            )
            lbl.pack(side="left", padx=14)
            self._cells[key] = lbl

        sep = ctk.CTkLabel(self, text="", fg_color=C["border"], width=1)
        sep.pack(side="right")

    def set(self, key: str, value: str, color: Optional[str] = None):
        lbl = self._cells.get(key)
        if lbl:
            lbl.configure(
                text=value,
                text_color=color or C["text_dim"],
            )


# ═══════════════════════════════════════════════════════════════════════════════
#  LOG CONSOLE WIDGET
# ═══════════════════════════════════════════════════════════════════════════════

class LogConsole(ctk.CTkTextbox):
    """
    Scrolling read-only terminal output.
    Tags map to log levels → colours.
    """

    TAG_COLORS = {
        LogBridge.INFO:   C["log_info"],
        LogBridge.OK:     C["log_ok"],
        LogBridge.WARN:   C["log_warn"],
        LogBridge.ERR:    C["log_err"],
        LogBridge.ACCENT: C["log_accent"],
    }

    def __init__(self, parent, **kwargs):
        super().__init__(
            parent,
            font=_font(10),
            fg_color=C["log_bg"],
            text_color=C["log_info"],
            wrap="word",
            state="disabled",
            border_width=1,
            border_color=C["border"],
            **kwargs,
        )
        # configure colour tags on the underlying Text widget
        for tag, color in self.TAG_COLORS.items():
            self._textbox.tag_configure(tag, foreground=color)

    def append(self, level: str, text: str) -> None:
        self.configure(state="normal")
        self._textbox.insert("end", text + "\n", level)
        self.configure(state="disabled")
        self._textbox.see("end")

    def clear(self) -> None:
        self.configure(state="normal")
        self.delete("0.0", "end")
        self.configure(state="disabled")


# ═══════════════════════════════════════════════════════════════════════════════
#  REACT TAB
# ═══════════════════════════════════════════════════════════════════════════════

class ReactTab(ctk.CTkFrame):
    def __init__(self, parent, app: "App", **kwargs):
        super().__init__(parent, fg_color="transparent", **kwargs)
        self._app     = app
        self._running = False

        # ── section: accounts ─────────────────────────────────────────────
        SectionHeader(self, "ACCOUNT SELECTION").pack(fill="x", pady=(10, 2))
        Divider(self).pack(fill="x", padx=8, pady=(0, 8))

        self._slider = AccountSlider(self, label="React accounts:", max_val=10)
        self._slider.pack(anchor="w", padx=16, pady=4)

        # ── section: config ───────────────────────────────────────────────
        SectionHeader(self, "REACTION CONFIG").pack(fill="x", pady=(12, 2))
        Divider(self).pack(fill="x", padx=8, pady=(0, 8))

        cfg_row = ctk.CTkFrame(self, fg_color="transparent")
        cfg_row.pack(fill="x", padx=16, pady=4)

        ctk.CTkLabel(cfg_row, text="Mode:", font=_font(10),
                     text_color=C["text_dim"]).grid(row=0, column=0, sticky="w")
        self._mode_var = ctk.StringVar(value="staggered")
        self._mode_seg = ctk.CTkSegmentedButton(
            cfg_row,
            values=["simultaneous", "staggered"],
            variable=self._mode_var,
            font=_font(10),
            selected_color=C["accent"],
            selected_hover_color=C["btn_hover"],
            unselected_color=C["border"],
            fg_color=C["panel"],
            text_color=C["text"],
        )
        self._mode_seg.grid(row=0, column=1, padx=(8, 0))

        ctk.CTkLabel(cfg_row, text="Max/hour:", font=_font(10),
                     text_color=C["text_dim"]).grid(row=1, column=0, sticky="w", pady=(6, 0))
        self._max_hour = ctk.CTkEntry(
            cfg_row, width=72, font=_font(10),
            fg_color=C["panel"], border_color=C["border"],
            text_color=C["text"],
        )
        self._max_hour.insert(0, "30")
        self._max_hour.grid(row=1, column=1, sticky="w", padx=(8, 0), pady=(6, 0))

        # ── section: emoji pool ───────────────────────────────────────────
        SectionHeader(self, "EMOJI POOL").pack(fill="x", pady=(12, 2))
        Divider(self).pack(fill="x", padx=8, pady=(0, 8))

        ctk.CTkLabel(self, text="  Comma-separated emoji (e.g. 👍,❤️,🔥)",
                     font=_font(9), text_color=C["text_dim"], anchor="w").pack(
            fill="x", padx=16)
        self._emoji_entry = ctk.CTkEntry(
            self, font=_font(11),
            fg_color=C["panel"], border_color=C["border"],
            text_color=C["accent"], placeholder_text="👍,❤️,🔥,🎉,👏",
        )
        self._emoji_entry.pack(fill="x", padx=16, pady=(4, 0))

        # ── section: controls ─────────────────────────────────────────────
        SectionHeader(self, "CONTROLS").pack(fill="x", pady=(12, 2))
        Divider(self).pack(fill="x", padx=8, pady=(0, 8))

        btn_row = ctk.CTkFrame(self, fg_color="transparent")
        btn_row.pack(anchor="w", padx=16, pady=6)

        self._start_btn = CyberButton(
            btn_row, "▶  START REACT",
            command=self._start, color="ok", width=160,
        )
        self._start_btn.pack(side="left", padx=(0, 8))

        self._stop_btn = StopButton(btn_row, command=self._stop, width=120)
        self._stop_btn.pack(side="left")
        self._stop_btn.configure(state="disabled")

        # ── indicator ─────────────────────────────────────────────────────
        self._status_lbl = ctk.CTkLabel(
            self, text="●  IDLE",
            font=_font(10, "bold"), text_color=C["text_dim"], anchor="w",
        )
        self._status_lbl.pack(anchor="w", padx=16, pady=(4, 0))

    def update_account_count(self, n: int):
        self._slider.update_max(n)

    def _start(self):
        if self._running:
            return
        accounts = self._app.get_react_accounts(self._slider.get())
        if not accounts:
            log_bridge.push(LogBridge.ERR, "No accounts loaded — load a CSV first")
            return
        emoji_raw = self._emoji_entry.get().strip()
        pool      = [e.strip() for e in emoji_raw.split(",") if e.strip()] \
                    or ["👍", "❤️", "🔥", "🎉", "👏"]
        mode      = self._mode_var.get()
        max_hour  = int(self._max_hour.get() or 30)

        self._running = True
        self._start_btn.configure(state="disabled")
        self._stop_btn.configure(state="normal")
        self._status_lbl.configure(text="●  RUNNING", text_color=C["ok"])

        log_bridge.push(LogBridge.ACCENT,
                        f"[REACT] starting — {len(accounts)} accounts, "
                        f"mode={mode}, pool={pool}, max/h={max_hour}")

        runner.submit(
            self._app.run_react(accounts, pool, mode, max_hour)
        )

    def _stop(self):
        self._running = False
        self._start_btn.configure(state="normal")
        self._stop_btn.configure(state="disabled")
        self._status_lbl.configure(text="●  STOPPED", text_color=C["warn"])
        log_bridge.push(LogBridge.WARN, "[REACT] stop requested")
        self._app.stop_react()

    def set_running(self, running: bool):
        self._running = running
        if not running:
            self._start_btn.configure(state="normal")
            self._stop_btn.configure(state="disabled")
            self._status_lbl.configure(text="●  IDLE", text_color=C["text_dim"])


# ═══════════════════════════════════════════════════════════════════════════════
#  JOIN TAB
# ═══════════════════════════════════════════════════════════════════════════════

class JoinTab(ctk.CTkFrame):
    def __init__(self, parent, app: "App", **kwargs):
        super().__init__(parent, fg_color="transparent", **kwargs)
        self._app     = app
        self._running = False

        # ── section: accounts ─────────────────────────────────────────────
        SectionHeader(self, "ACCOUNT SELECTION").pack(fill="x", pady=(10, 2))
        Divider(self).pack(fill="x", padx=8, pady=(0, 8))

        self._slider = AccountSlider(self, label="Join accounts:", max_val=10)
        self._slider.pack(anchor="w", padx=16, pady=4)

        # ── section: targets ──────────────────────────────────────────────
        SectionHeader(self, "TARGET CHANNELS").pack(fill="x", pady=(12, 2))
        Divider(self).pack(fill="x", padx=8, pady=(0, 8))

        ctk.CTkLabel(
            self,
            text="  One channel per line  (username, t.me/+HASH, or numeric ID)",
            font=_font(9), text_color=C["text_dim"], anchor="w",
        ).pack(fill="x", padx=16)

        self._targets_box = ctk.CTkTextbox(
            self,
            height=100,
            font=_font(10),
            fg_color=C["panel"],
            text_color=C["accent"],
            border_width=1,
            border_color=C["border"],
        )
        self._targets_box.pack(fill="x", padx=16, pady=(4, 0))

        # ── section: config ───────────────────────────────────────────────
        SectionHeader(self, "JOIN CONFIG").pack(fill="x", pady=(12, 2))
        Divider(self).pack(fill="x", padx=8, pady=(0, 8))

        cfg_row = ctk.CTkFrame(self, fg_color="transparent")
        cfg_row.pack(fill="x", padx=16, pady=4)

        for col, (lbl_text, default, attr) in enumerate([
            ("Max joins/hour:", "8",  "_max_joins"),
            ("Min gap (sec):",  "30", "_min_gap"),
            ("Concurrency:",    "3",  "_concurrency"),
        ]):
            ctk.CTkLabel(cfg_row, text=lbl_text, font=_font(10),
                         text_color=C["text_dim"]).grid(
                row=col, column=0, sticky="w", pady=2)
            entry = ctk.CTkEntry(
                cfg_row, width=72, font=_font(10),
                fg_color=C["panel"], border_color=C["border"],
                text_color=C["text"],
            )
            entry.insert(0, default)
            entry.grid(row=col, column=1, sticky="w", padx=(8, 0), pady=2)
            setattr(self, attr, entry)

        self._dryrun_var = ctk.BooleanVar(value=False)
        ctk.CTkCheckBox(
            self,
            text="DRY RUN (simulate, no actual joins)",
            variable=self._dryrun_var,
            font=_font(10),
            text_color=C["text_dim"],
            fg_color=C["accent2"],
            hover_color=C["accent"],
            checkmark_color=C["bg"],
        ).pack(anchor="w", padx=16, pady=(6, 0))

        # ── controls ──────────────────────────────────────────────────────
        SectionHeader(self, "CONTROLS").pack(fill="x", pady=(12, 2))
        Divider(self).pack(fill="x", padx=8, pady=(0, 8))

        btn_row = ctk.CTkFrame(self, fg_color="transparent")
        btn_row.pack(anchor="w", padx=16, pady=6)

        self._start_btn = CyberButton(
            btn_row, "▶  START JOIN",
            command=self._start, color="accent2", width=160,
        )
        self._start_btn.pack(side="left", padx=(0, 8))

        self._stop_btn = StopButton(btn_row, command=self._stop, width=120)
        self._stop_btn.pack(side="left")
        self._stop_btn.configure(state="disabled")

        self._status_lbl = ctk.CTkLabel(
            self, text="●  IDLE",
            font=_font(10, "bold"), text_color=C["text_dim"], anchor="w",
        )
        self._status_lbl.pack(anchor="w", padx=16, pady=(4, 0))

    def update_account_count(self, n: int):
        self._slider.update_max(n)

    def _start(self):
        if self._running:
            return
        accounts = self._app.get_join_accounts(self._slider.get())
        if not accounts:
            log_bridge.push(LogBridge.ERR, "No accounts loaded — load a CSV first")
            return
        targets_raw = self._targets_box.get("0.0", "end").strip()
        targets = [t.strip() for t in targets_raw.splitlines() if t.strip()]
        if not targets:
            log_bridge.push(LogBridge.ERR, "[JOIN] no target channels entered")
            return

        dry_run     = self._dryrun_var.get()
        max_joins   = int(self._max_joins.get() or 8)
        min_gap     = float(self._min_gap.get() or 30)
        concurrency = int(self._concurrency.get() or 3)

        self._running = True
        self._start_btn.configure(state="disabled")
        self._stop_btn.configure(state="normal")
        self._status_lbl.configure(
            text="●  RUNNING" + (" [DRY]" if dry_run else ""),
            text_color=C["ok"],
        )

        log_bridge.push(LogBridge.ACCENT,
                        f"[JOIN] starting — {len(accounts)} accounts, "
                        f"{len(targets)} targets, dry={dry_run}, "
                        f"max/h={max_joins}, gap={min_gap}s, "
                        f"conc={concurrency}")

        runner.submit(
            self._app.run_join(
                accounts, targets, dry_run, max_joins, min_gap, concurrency
            )
        )

    def _stop(self):
        self._running = False
        self._start_btn.configure(state="normal")
        self._stop_btn.configure(state="disabled")
        self._status_lbl.configure(text="●  STOPPED", text_color=C["warn"])
        log_bridge.push(LogBridge.WARN, "[JOIN] stop requested")
        self._app.stop_join()

    def set_running(self, running: bool):
        self._running = running
        if not running:
            self._start_btn.configure(state="normal")
            self._stop_btn.configure(state="disabled")
            self._status_lbl.configure(text="●  IDLE", text_color=C["text_dim"])


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN APPLICATION
# ═══════════════════════════════════════════════════════════════════════════════

class App(ctk.CTk):

    def __init__(self):
        super().__init__()

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")

        self.title("Bootsusers — Telegram Automation")
        self.geometry("900x680")
        self.minsize(820, 600)
        self.configure(fg_color=C["bg"])

        # internal state
        self._all_accounts:  list[dict] = []
        self._csv_path:      str        = ""
        self._react_stop:    bool       = False
        self._join_stop:     bool       = False
        self._react_task:    Optional[asyncio.Future] = None
        self._join_task:     Optional[asyncio.Future] = None

        runner.start()
        self._build_ui()
        self._poll_logs()

    # ── layout ───────────────────────────────────────────────────────────────

    def _build_ui(self):
        # ── top bar ──────────────────────────────────────────────────────
        topbar = ctk.CTkFrame(self, fg_color=C["panel"], height=48, corner_radius=0)
        topbar.pack(fill="x", side="top")
        topbar.pack_propagate(False)

        ctk.CTkLabel(
            topbar,
            text="  ◈ BOOTSUSERS",
            font=(_FONT_FAMILY, 15, "bold"),
            text_color=C["accent"],
        ).pack(side="left", padx=4)

        ctk.CTkLabel(
            topbar,
            text="Telegram Automation Toolkit",
            font=_font(9),
            text_color=C["text_dim"],
        ).pack(side="left", padx=(0, 20))

        self._csv_lbl = ctk.CTkLabel(
            topbar, text="No CSV loaded",
            font=_font(9), text_color=C["text_dim"],
        )
        self._csv_lbl.pack(side="right", padx=16)

        CyberButton(
            topbar, "  📂  LOAD CSV",
            command=self._load_csv,
            color="accent",
            width=120, height=30,
        ).pack(side="right", padx=(0, 4))

        # ── main split ───────────────────────────────────────────────────
        main = ctk.CTkFrame(self, fg_color="transparent")
        main.pack(fill="both", expand=True, padx=0, pady=0)

        # LEFT: control panel
        left = ctk.CTkFrame(main, fg_color=C["panel"], width=380, corner_radius=0)
        left.pack(side="left", fill="y")
        left.pack_propagate(False)

        tabs = ctk.CTkTabview(
            left,
            fg_color=C["panel"],
            segmented_button_fg_color=C["border"],
            segmented_button_selected_color=C["accent"],
            segmented_button_selected_hover_color=C["btn_hover"],
            segmented_button_unselected_color=C["border"],
            segmented_button_unselected_hover_color=C["bg"],
            text_color=C["text"],
            text_color_disabled=C["text_dim"],
        )
        tabs.pack(fill="both", expand=True, padx=0, pady=0)
        tabs.add("⚡  REACT")
        tabs.add("🔗  JOIN")

        self._react_tab = ReactTab(tabs.tab("⚡  REACT"), app=self)
        self._react_tab.pack(fill="both", expand=True)

        self._join_tab = JoinTab(tabs.tab("🔗  JOIN"), app=self)
        self._join_tab.pack(fill="both", expand=True)

        # RIGHT: log console
        right = ctk.CTkFrame(main, fg_color=C["bg"], corner_radius=0)
        right.pack(side="right", fill="both", expand=True)

        log_header = ctk.CTkFrame(right, fg_color=C["panel"], height=32, corner_radius=0)
        log_header.pack(fill="x")
        log_header.pack_propagate(False)
        ctk.CTkLabel(
            log_header,
            text="  ◈ CONSOLE OUTPUT",
            font=_font(9, "bold"),
            text_color=C["accent"],
        ).pack(side="left", padx=4, pady=4)

        CyberButton(
            log_header, "CLEAR",
            command=self._clear_log,
            color="warn", width=60, height=22,
        ).pack(side="right", padx=8, pady=4)

        self._console = LogConsole(right)
        self._console.pack(fill="both", expand=True, padx=4, pady=4)

        # ── bottom status bar ─────────────────────────────────────────────
        self._statusbar = StatusBar(self)
        self._statusbar.pack(fill="x", side="bottom")

        # startup message
        log_bridge.push(LogBridge.ACCENT,
                        "Bootsusers launcher ready. Load a CSV to begin.")
        log_bridge.push(LogBridge.INFO,
                        f"Font: {_FONT_FAMILY} | "
                        f"Python {sys.version.split()[0]}")

    # ── CSV handling ─────────────────────────────────────────────────────────

    def _load_csv(self):
        from tkinter import filedialog
        path = filedialog.askopenfilename(
            title="Select Accounts CSV",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if not path:
            return
        rows = load_csv(path)
        if not rows:
            log_bridge.push(LogBridge.ERR, "CSV is empty or could not be read")
            return

        self._all_accounts = rows
        self._csv_path     = path
        n = len(rows)

        self._csv_lbl.configure(
            text=f"✓  {Path(path).name}  ({n} accounts)",
            text_color=C["ok"],
        )
        self._react_tab.update_account_count(n)
        self._join_tab.update_account_count(n)
        self._statusbar.set("accounts", f"ACCOUNTS  {n}", C["ok"])

        log_bridge.push(LogBridge.OK,
                        f"Loaded {n} accounts from {Path(path).name}")
        for row in rows[:3]:
            log_bridge.push(LogBridge.INFO,
                            f"  ↳ {row.get('phone','?')}  "
                            f"session={row.get('session_name','?')}  "
                            f"proxy={row.get('proxy_type','none')}")
        if n > 3:
            log_bridge.push(LogBridge.INFO, f"  ↳ ... and {n - 3} more")

    # ── account selectors ────────────────────────────────────────────────────

    def get_react_accounts(self, n: int) -> list[dict]:
        return self._all_accounts[:n]

    def get_join_accounts(self, n: int) -> list[dict]:
        return self._all_accounts[:n]

    # ── async operation bridges ──────────────────────────────────────────────

    async def run_react(
        self,
        account_rows: list[dict],
        pool:    list[str],
        mode:    str,
        max_per_hour: int,
    ):
        """Bridge: connect accounts and start the react loop."""
        import importlib, sys as _sys
        # patch config in auto_react before importing
        try:
            import auto_react as ar
        except ImportError:
            log_bridge.push(LogBridge.ERR,
                            "auto_react.py not found — place it in the same folder")
            self._react_tab.set_running(False)
            return

        # inject GUI config into the module
        ar.REACTION_POOL         = pool
        ar.REACTION_MODE         = mode
        ar.MAX_REACTIONS_PER_HOUR = max_per_hour

        try:
            accounts = []
            for row in account_rows:
                from auto_react import Account, ProxyConfig, RateLimiter
                proxy = ProxyConfig(
                    proxy_type = row.get("proxy_type", "").strip(),
                    host       = row.get("proxy_host", "").strip(),
                    port       = int(row["proxy_port"]) if row.get("proxy_port", "").strip() else 0,
                    username   = row.get("proxy_user", "").strip(),
                    password   = row.get("proxy_pass", "").strip(),
                )
                accounts.append(Account(
                    phone   = row["phone"].strip(),
                    session = row["session_name"].strip(),
                    proxy   = proxy,
                    limiter = RateLimiter(max_per_hour, ar.MIN_GAP_BETWEEN_REACTIONS),
                ))

            await ar.connect_all(accounts)
            healthy = sum(1 for a in accounts if a.is_healthy())
            log_bridge.push(LogBridge.OK,
                            f"[REACT] {healthy}/{len(accounts)} accounts connected")

            # update status bar
            self.after(0, lambda: self._statusbar.set(
                "accounts", f"ACCOUNTS  {healthy}", C["ok"]
            ))
            self.after(0, lambda: self._statusbar.set(
                "status", "REACT RUNNING", C["ok"]
            ))

            # start health check background
            asyncio.create_task(ar.health_check_loop(accounts, ar.HEALTH_CHECK_INTERVAL))

            sender   = accounts[0]
            reactors = accounts[1:] if len(accounts) > 1 else accounts

            @sender.client.on(__import__("telethon").events.NewMessage(outgoing=True))
            async def on_send(event):
                if self._react_stop:
                    return
                chat_id = event.chat_id
                msg_id  = event.message.id
                log_bridge.push(LogBridge.INFO,
                                f"[REACT] msg {msg_id} in {chat_id}")
                await ar.react_all(reactors, chat_id, msg_id)

            log_bridge.push(LogBridge.ACCENT,
                            "[REACT] listening for outgoing messages...")
            await sender.client.run_until_disconnected()

        except Exception as e:
            log_bridge.push(LogBridge.ERR, f"[REACT] fatal: {e}")
        finally:
            self.after(0, lambda: self._react_tab.set_running(False))
            self.after(0, lambda: self._statusbar.set("status", "IDLE", C["text_dim"]))

    async def run_join(
        self,
        account_rows:  list[dict],
        targets:       list[str],
        dry_run:       bool,
        max_joins:     int,
        min_gap:       float,
        concurrency:   int,
    ):
        """Bridge: connect accounts and batch-join channels."""
        try:
            import auto_join as aj
        except ImportError:
            log_bridge.push(LogBridge.ERR,
                            "auto_join.py not found — place it in the same folder")
            self._join_tab.set_running(False)
            return

        aj.MAX_JOINS_PER_HOUR = max_joins
        aj.MIN_JOIN_GAP_SEC   = min_gap
        aj.GLOBAL_CONCURRENCY = concurrency
        aj.DRY_RUN            = dry_run

        try:
            accounts = []
            for row in account_rows:
                from auto_join import Account, ProxyConfig, RateLimiter
                proxy = ProxyConfig(
                    proxy_type = row.get("proxy_type", "").strip(),
                    host       = row.get("proxy_host", "").strip(),
                    port       = int(row["proxy_port"]) if row.get("proxy_port", "").strip() else 0,
                    username   = row.get("proxy_user", "").strip(),
                    password   = row.get("proxy_pass", "").strip(),
                )
                accounts.append(Account(
                    phone   = row["phone"].strip(),
                    session = row["session_name"].strip(),
                    proxy   = proxy,
                    limiter = RateLimiter(max_joins, min_gap),
                ))

            await aj.connect_all(accounts)
            healthy = sum(1 for a in accounts if a.healthy())
            log_bridge.push(LogBridge.OK,
                            f"[JOIN] {healthy}/{len(accounts)} connected")

            self.after(0, lambda: self._statusbar.set("status", "JOIN RUNNING", C["accent2"]))

            asyncio.create_task(aj.health_check_loop(accounts))
            sem     = asyncio.Semaphore(concurrency)
            results = await aj.batch_join(accounts, targets, sem)

            # results to log
            joined  = sum(1 for r in results if r.status.name == "JOINED")
            skipped = sum(1 for r in results if r.status.name == "SKIPPED")
            failed  = sum(1 for r in results
                         if r.status.name not in ("JOINED", "ALREADY_IN", "SKIPPED"))
            banned  = sum(1 for r in results if r.status.name == "BANNED")

            log_bridge.push(LogBridge.OK,
                            f"[JOIN] done — joined={joined} skipped={skipped} "
                            f"failed={failed} banned={banned}")

            self.after(0, lambda: self._statusbar.set(
                "sent", f"JOINED  {joined}", C["ok"]
            ))
            if banned:
                self.after(0, lambda: self._statusbar.set(
                    "banned", f"BANNED  {banned}", C["danger"]
                ))

            # print detailed summary to log
            for r in results:
                color = {
                    "JOINED":   LogBridge.OK,
                    "ALREADY_IN": LogBridge.INFO,
                    "BANNED":   LogBridge.ERR,
                    "FAILED":   LogBridge.ERR,
                    "SKIPPED":  LogBridge.WARN,
                }.get(r.status.name, LogBridge.INFO)
                log_bridge.push(color,
                                f"  {r.phone:<18} {r.status.name:<14} {r.target}")

            for acc in accounts:
                if acc.client:
                    try:
                        await acc.client.disconnect()
                    except Exception:
                        pass

        except Exception as e:
            log_bridge.push(LogBridge.ERR, f"[JOIN] fatal: {e}")
        finally:
            self.after(0, lambda: self._join_tab.set_running(False))
            self.after(0, lambda: self._statusbar.set("status", "IDLE", C["text_dim"]))

    # ── stop signals ─────────────────────────────────────────────────────────

    def stop_react(self):
        self._react_stop = True
        if self._react_task:
            self._react_task.cancel()
        self._react_stop = False

    def stop_join(self):
        self._join_stop = True
        if self._join_task:
            self._join_task.cancel()
        self._join_stop = False

    # ── log polling ──────────────────────────────────────────────────────────

    def _poll_logs(self):
        for level, text in log_bridge.drain():
            self._console.append(level, text)
        self.after(120, self._poll_logs)   # poll every 120 ms

    def _clear_log(self):
        self._console.clear()

    # ── cleanup ──────────────────────────────────────────────────────────────

    def on_close(self):
        runner.stop()
        self.destroy()


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    app = App()
    app.protocol("WM_DELETE_WINDOW", app.on_close)
    app.mainloop()


if __name__ == "__main__":
    main()