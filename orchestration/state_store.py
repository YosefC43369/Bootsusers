# orchestration/state_store.py
# ==============================================================================
#  Persistent state for the Bootsusers suite — SQLite, stdlib only.
# ------------------------------------------------------------------------------
#  Backbone for:
#    • account registry + health/quarantine (lifecycle layer)
#    • per-account / per-action cooldowns
#    • an append-only action log (feeds metrics)
#    • campaign definitions + run history (orchestration layer)
#    • a generic key/value store
#
#  Thread-safe: a single connection shared behind a re-entrant lock
#  (check_same_thread=False), WAL journal for concurrent reads. Good to the
#  low-thousands of accounts; past that, front it with the worker queue.
# ==============================================================================

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional

SCHEMA_VERSION = 1

_DDL = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
CREATE TABLE IF NOT EXISTS accounts (
    phone                 TEXT PRIMARY KEY,
    session               TEXT,
    user_id               TEXT,
    email                 TEXT,
    status                TEXT    DEFAULT 'unknown',
    star_balance          INTEGER DEFAULT -1,
    consecutive_failures  INTEGER DEFAULT 0,
    warmup_stage          INTEGER DEFAULT 0,
    quarantined_until     REAL    DEFAULT 0,
    quarantine_reason     TEXT    DEFAULT '',
    last_seen             REAL    DEFAULT 0,
    created_at            REAL
);
CREATE TABLE IF NOT EXISTS action_log (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts      REAL,
    phone   TEXT,
    op      TEXT,
    target  TEXT,
    status  TEXT,
    detail  TEXT
);
CREATE INDEX IF NOT EXISTS idx_action_ts  ON action_log(ts);
CREATE INDEX IF NOT EXISTS idx_action_op  ON action_log(op);
CREATE TABLE IF NOT EXISTS cooldowns (
    phone   TEXT,
    action  TEXT,
    until   REAL,
    PRIMARY KEY (phone, action)
);
CREATE TABLE IF NOT EXISTS campaigns (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT UNIQUE,
    spec_json   TEXT,
    status      TEXT DEFAULT 'idle',
    created_at  REAL
);
CREATE TABLE IF NOT EXISTS campaign_runs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id  INTEGER,
    started_at   REAL,
    finished_at  REAL,
    status       TEXT,
    stats_json   TEXT
);
CREATE TABLE IF NOT EXISTS kv (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


@dataclass
class AccountRecord:
    phone: str
    session: str = ""
    user_id: str = ""
    email: str = ""
    status: str = "unknown"
    star_balance: int = -1
    consecutive_failures: int = 0
    warmup_stage: int = 0
    quarantined_until: float = 0.0
    quarantine_reason: str = ""
    last_seen: float = 0.0
    created_at: float = field(default_factory=time.time)

    def is_quarantined(self, now: Optional[float] = None) -> bool:
        return (now or time.time()) < self.quarantined_until


class StateStore:
    """Thread-safe SQLite state store."""

    def __init__(self, path: str = "data/bootsusers.db"):
        self.path = path
        if os.path.dirname(path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")
            self._conn.executescript(_DDL)
            cur = self._conn.execute("SELECT value FROM meta WHERE key='schema_version'")
            row = cur.fetchone()
            if row is None:
                self._conn.execute(
                    "INSERT INTO meta(key, value) VALUES('schema_version', ?)",
                    (str(SCHEMA_VERSION),))
            self._conn.commit()

    # -- lifecycle ----------------------------------------------------------
    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _commit(self) -> None:
        self._conn.commit()

    # -- accounts -----------------------------------------------------------
    def upsert_account(self, phone: str, **fields) -> None:
        with self._lock:
            cur = self._conn.execute("SELECT phone FROM accounts WHERE phone=?", (phone,))
            if cur.fetchone() is None:
                self._conn.execute(
                    "INSERT INTO accounts(phone, created_at) VALUES(?, ?)",
                    (phone, time.time()))
            if fields:
                cols = ", ".join(f"{k}=?" for k in fields)
                self._conn.execute(
                    f"UPDATE accounts SET {cols} WHERE phone=?",
                    (*fields.values(), phone))
            self._commit()

    def get_account(self, phone: str) -> Optional[AccountRecord]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM accounts WHERE phone=?", (phone,)).fetchone()
        return self._row_to_account(row) if row else None

    def all_accounts(self) -> "list[AccountRecord]":
        with self._lock:
            rows = self._conn.execute("SELECT * FROM accounts").fetchall()
        return [self._row_to_account(r) for r in rows]

    def set_status(self, phone: str, status: str) -> None:
        self.upsert_account(phone, status=status, last_seen=time.time())

    def set_star_balance(self, phone: str, balance: int) -> None:
        self.upsert_account(phone, star_balance=int(balance))

    def set_warmup_stage(self, phone: str, stage: int) -> None:
        self.upsert_account(phone, warmup_stage=int(stage))

    def bump_failures(self, phone: str, reset: bool = False) -> int:
        with self._lock:
            rec = self.get_account(phone) or AccountRecord(phone=phone)
            n = 0 if reset else rec.consecutive_failures + 1
            self.upsert_account(phone, consecutive_failures=n, last_seen=time.time())
            return n

    # -- quarantine ---------------------------------------------------------
    def quarantine(self, phone: str, seconds: float, reason: str = "") -> None:
        until = time.time() + max(0.0, seconds)
        self.upsert_account(phone, quarantined_until=until,
                            quarantine_reason=reason, status="quarantined")

    def clear_quarantine(self, phone: str) -> None:
        self.upsert_account(phone, quarantined_until=0.0, quarantine_reason="")

    def is_quarantined(self, phone: str, now: Optional[float] = None) -> bool:
        rec = self.get_account(phone)
        return bool(rec and rec.is_quarantined(now))

    def quarantined_phones(self) -> "list[str]":
        now = time.time()
        return [a.phone for a in self.all_accounts() if a.is_quarantined(now)]

    # -- cooldowns ----------------------------------------------------------
    def set_cooldown(self, phone: str, action: str, seconds: float) -> None:
        until = time.time() + max(0.0, seconds)
        with self._lock:
            self._conn.execute(
                "INSERT INTO cooldowns(phone, action, until) VALUES(?,?,?) "
                "ON CONFLICT(phone, action) DO UPDATE SET until=excluded.until",
                (phone, action, until))
            self._commit()

    def cooldown_remaining(self, phone: str, action: str) -> float:
        with self._lock:
            row = self._conn.execute(
                "SELECT until FROM cooldowns WHERE phone=? AND action=?",
                (phone, action)).fetchone()
        return max(0.0, row["until"] - time.time()) if row else 0.0

    def is_on_cooldown(self, phone: str, action: str) -> bool:
        return self.cooldown_remaining(phone, action) > 0

    # -- action log ---------------------------------------------------------
    def log_action(self, phone: str, op: str, target: str,
                   status: str, detail: str = "") -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO action_log(ts, phone, op, target, status, detail) "
                "VALUES(?,?,?,?,?,?)",
                (time.time(), phone, op, target, status, detail))
            self._commit()

    def recent_actions(self, limit: int = 100, op: Optional[str] = None) -> "list[dict]":
        with self._lock:
            if op:
                rows = self._conn.execute(
                    "SELECT * FROM action_log WHERE op=? ORDER BY id DESC LIMIT ?",
                    (op, limit)).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM action_log ORDER BY id DESC LIMIT ?",
                    (limit,)).fetchall()
        return [dict(r) for r in rows]

    def count_actions(self, op: Optional[str] = None, status: Optional[str] = None,
                      since: Optional[float] = None) -> int:
        q = "SELECT COUNT(*) AS n FROM action_log WHERE 1=1"
        args: "list[Any]" = []
        if op:
            q += " AND op=?"; args.append(op)
        if status:
            q += " AND status=?"; args.append(status)
        if since:
            q += " AND ts>=?"; args.append(since)
        with self._lock:
            return int(self._conn.execute(q, args).fetchone()["n"])

    # -- campaigns ----------------------------------------------------------
    def save_campaign(self, name: str, spec: dict) -> int:
        with self._lock:
            self._conn.execute(
                "INSERT INTO campaigns(name, spec_json, created_at) VALUES(?,?,?) "
                "ON CONFLICT(name) DO UPDATE SET spec_json=excluded.spec_json",
                (name, json.dumps(spec), time.time()))
            self._commit()
            row = self._conn.execute(
                "SELECT id FROM campaigns WHERE name=?", (name,)).fetchone()
            return int(row["id"])

    def get_campaign(self, name: str) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM campaigns WHERE name=?", (name,)).fetchone()
        if not row:
            return None
        d = dict(row)
        d["spec"] = json.loads(d.pop("spec_json") or "{}")
        return d

    def start_campaign_run(self, campaign_id: int) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO campaign_runs(campaign_id, started_at, status) "
                "VALUES(?,?, 'running')", (campaign_id, time.time()))
            self._commit()
            return int(cur.lastrowid)

    def finish_campaign_run(self, run_id: int, status: str, stats: dict) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE campaign_runs SET finished_at=?, status=?, stats_json=? "
                "WHERE id=?", (time.time(), status, json.dumps(stats), run_id))
            self._commit()

    def campaign_history(self, campaign_id: int, limit: int = 50) -> "list[dict]":
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM campaign_runs WHERE campaign_id=? "
                "ORDER BY id DESC LIMIT ?", (campaign_id, limit)).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["stats"] = json.loads(d.pop("stats_json") or "{}")
            out.append(d)
        return out

    # -- generic kv ---------------------------------------------------------
    def kv_set(self, key: str, value: Any) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO kv(key, value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, json.dumps(value)))
            self._commit()

    def kv_get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return json.loads(row["value"]) if row else default

    # -- helpers ------------------------------------------------------------
    @staticmethod
    def _row_to_account(row: sqlite3.Row) -> AccountRecord:
        return AccountRecord(
            phone=row["phone"], session=row["session"] or "",
            user_id=row["user_id"] or "", email=row["email"] or "",
            status=row["status"] or "unknown",
            star_balance=row["star_balance"] if row["star_balance"] is not None else -1,
            consecutive_failures=row["consecutive_failures"] or 0,
            warmup_stage=row["warmup_stage"] or 0,
            quarantined_until=row["quarantined_until"] or 0.0,
            quarantine_reason=row["quarantine_reason"] or "",
            last_seen=row["last_seen"] or 0.0,
            created_at=row["created_at"] or 0.0,
        )
