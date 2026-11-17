# account_io.py
# ==============================================================================
#  Content-aware account-file reader shared by every entrypoint.
# ------------------------------------------------------------------------------
#  Instead of trusting header names or column positions, this profiles the
#  actual VALUES in each column (regex classification + per-column majority
#  vote, Elasticsearch-style field detection) and decides what each column is:
#  phone, Telegram user id, email, session string, api_id / api_hash, proxy
#  type / host / port / user / pass, username, or name.
#
#  Supported inputs:
#    • CSV/TSV/PSV WITH a header (any column order, tolerant aliases);
#    • headerless .txt dumps — one account per line in any shape:
#          +15550001
#          15550001                       (id only)
#          123456789:+15550001            (id + phone, any order)
#          +15550001:session_name
#          +15550001,session,socks5,host,port
#          user@mail.com +15550001
#          1AbC<very-long-session-string>
#    • shuffled / mismatched columns — resolved by content, not header.
#
#  A row is accepted as an account when it has ANY usable identifier: a phone,
#  a Telegram id, a session string, or a username. (A phone or session is what
#  actually lets it log in; an id-only row still loads for use as a target.)
# ==============================================================================

from __future__ import annotations

import csv
import re
from collections import Counter

# --- value patterns -----------------------------------------------------------
_EMAIL_RE    = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_IPV4_RE     = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")
_HOSTNAME_RE = re.compile(r"^(?=.{4,253}$)([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}$")
_HEX32_RE    = re.compile(r"^[0-9a-fA-F]{32}$")
_SESSION_RE  = re.compile(r"^[A-Za-z0-9+/=_\-]{90,}$")      # long base64-ish blob
_USERNAME_RE = re.compile(r"^@?[A-Za-z][A-Za-z0-9_]{3,31}$")
_WORD_RE     = re.compile(r"^[A-Za-z][\w .\-]{0,63}$")
_PROXY_KW    = {"socks5", "socks4", "http", "https", "mtproto"}

# header aliases (used as a strong hint when a real header is present)
_ALIASES = {
    "phone":      ("phone", "phone_number", "number", "msisdn", "tel", "telephone"),
    "session":    ("session_name", "session", "session_file", "name"),
    "user_id":    ("id", "user_id", "uid", "tg_id", "telegram_id", "account_id"),
    "username":   ("username", "user_name", "handle", "login"),
    "email":      ("email", "mail", "email_address", "e-mail"),
    "api_id":     ("api_id", "apiid", "app_id"),
    "api_hash":   ("api_hash", "apihash", "app_hash"),
    "proxy_type": ("proxy_type", "proxytype", "scheme"),
    "proxy_host": ("proxy_host", "proxyhost", "proxy_ip", "host", "ip"),
    "proxy_port": ("proxy_port", "proxyport", "port"),
    "proxy_user": ("proxy_user", "proxyuser", "proxy_username"),
    "proxy_pass": ("proxy_pass", "proxypass", "proxy_password"),
}
_FIELDS = tuple(_ALIASES.keys())
_HEADER_WORDS = {a for keys in _ALIASES.values() for a in keys}


def raise_csv_field_limit() -> None:
    """Allow very large CSV fields; step down on OverflowError (Windows-safe)."""
    import sys
    lim = sys.maxsize
    while lim > 1:
        try:
            csv.field_size_limit(lim)
            return
        except OverflowError:
            lim //= 10


def _empty_row() -> dict:
    return {f: "" for f in _FIELDS}


def derive_session(seed: str) -> str:
    seed = re.sub(r"[^0-9A-Za-z]", "", seed or "") or "unknown"
    return "session_" + seed


# ------------------------------------------------------------------------------
#  Value-level classifier
# ------------------------------------------------------------------------------

def classify_token(v: str) -> str:
    """Classify a single value into a coarse type tag."""
    v = (v or "").strip()
    if not v:
        return "empty"
    low = v.lower()
    if low in _PROXY_KW:
        return "proxy_type"
    if _EMAIL_RE.match(v):
        return "email"
    if _HEX32_RE.match(v):
        return "api_hash"
    if _IPV4_RE.match(v) or _HOSTNAME_RE.match(v):
        return "host"
    digits = re.sub(r"[ \-]", "", v)
    if re.fullmatch(r"\+\d{7,15}", digits):
        return "phone"                       # explicit international phone
    if digits.isdigit():
        n = len(digits)
        if n >= 11:
            return "phone"                   # long bare number → phone (with CC)
        if n <= 5:
            return "smallint"                # port / api_id-ish
        return "int"                         # 6-10 digits → id or local phone
    if _SESSION_RE.match(v):
        return "session_string"
    if v.startswith("@") and _USERNAME_RE.match(v):
        return "username"
    if _WORD_RE.match(v):
        return "word"                        # session name / first name / handle
    return "other"


# ------------------------------------------------------------------------------
#  Grid reading (header or headerless, delimiter auto-detect)
# ------------------------------------------------------------------------------

def _detect_delim(lines: "list[str]") -> "str | None":
    for d in (",", ";", "\t", "|", ":"):
        counts = [ln.count(d) for ln in lines]
        if counts and min(counts) >= 1 and len(set(counts)) <= 2:
            return d
    ws = [len(ln.split()) for ln in lines]
    if ws and min(ws) >= 2:
        return " "
    return None                              # single-column file


def _split(line: str, delim: "str | None") -> "list[str]":
    if delim is None:
        return [line.strip()]
    if delim == " ":
        return line.split()
    return [c.strip() for c in line.split(delim)]


def _looks_like_header(cells: "list[str]") -> bool:
    low = [(c or "").strip().lower() for c in cells]
    if not any(c in _HEADER_WORDS for c in low):
        return False
    # a header row should contain no obvious data values (phones / ids / emails)
    data_like = {"phone", "int", "email", "session_string", "api_hash", "host"}
    return not any(classify_token(c) in data_like for c in cells)


def _read_grid(path: str) -> "tuple[list[list[str]], list[str] | None]":
    raise_csv_field_limit()
    with open(path, encoding="utf-8-sig", errors="ignore") as f:
        text = f.read()
    lines = [ln for ln in text.splitlines()
             if ln.strip() and not ln.strip().startswith(("#", "//"))]
    if not lines:
        return [], None
    delim = _detect_delim(lines[:100])
    rows = [_split(ln, delim) for ln in lines]
    header = None
    if _looks_like_header(rows[0]):
        header = [(c or "").strip().lower() for c in rows[0]]
        rows = rows[1:]
    return rows, header


# ------------------------------------------------------------------------------
#  Column profiling → field assignment
# ------------------------------------------------------------------------------

def _median_len(values: "list[str]") -> float:
    lens = sorted(len(re.sub(r"[ \-]", "", v)) for v in values if v.strip())
    if not lens:
        return 0.0
    m = len(lens) // 2
    return lens[m] if len(lens) % 2 else (lens[m - 1] + lens[m]) / 2


def _assign_columns(rows: "list[list[str]]", header: "list[str] | None") -> "dict[int, str]":
    ncol = max((len(r) for r in rows), default=0)
    cols = [[(r[i] if i < len(r) else "") for r in rows[:300]] for i in range(ncol)]

    assigned: "dict[int, str]" = {}
    tags: "dict[int, str]" = {}

    # 1) strong header hints
    if header:
        for i in range(ncol):
            h = header[i] if i < len(header) else ""
            for field, aliases in _ALIASES.items():
                if h in aliases:
                    assigned[i] = field
                    break

    # 2) content classification for every column
    host_cols, int_cols, small_cols, word_cols = [], [], [], []
    for i in range(ncol):
        if i in assigned:
            continue
        vals = [c for c in cols[i] if c.strip()]
        if not vals:
            continue
        top = Counter(classify_token(c) for c in vals).most_common(1)[0][0]
        tags[i] = top
        if top == "phone":
            assigned[i] = "phone"
        elif top == "email":
            assigned[i] = "email"
        elif top == "api_hash":
            assigned[i] = "api_hash"
        elif top == "session_string":
            assigned[i] = "session"
        elif top == "username":
            assigned[i] = "username"
        elif top == "proxy_type":
            assigned[i] = "proxy_type"
        elif top == "host":
            host_cols.append(i)
        elif top == "int":
            int_cols.append(i)
        elif top == "smallint":
            small_cols.append(i)
        elif top == "word":
            word_cols.append(i)

    # 3) integer columns (6-10 digits) → Telegram user id. A phone is only a
    #    value carrying a '+' or >=11 digits (country code), so a bare short
    #    number is treated as an id, which also makes id-only files load.
    for i in int_cols:
        assigned.setdefault(i, "user_id")

    # 4) host / port / small ints
    for i in host_cols:
        assigned.setdefault(i, "proxy_host")
    have_host = "proxy_host" in assigned.values()
    for i in small_cols:
        if have_host and "proxy_port" not in assigned.values():
            assigned[i] = "proxy_port"
        else:
            assigned.setdefault(i, "api_id")

    # 5) word columns → first becomes session, rest username/name
    for i in word_cols:
        if "session" not in assigned.values():
            assigned[i] = "session"
        else:
            assigned.setdefault(i, "username")

    return assigned


# ------------------------------------------------------------------------------
#  Public API
# ------------------------------------------------------------------------------

def _usable(rec: dict) -> bool:
    return bool(rec["phone"] or rec["user_id"] or rec["session"] or rec["username"])


def read_rows(path: str) -> "list[dict]":
    """
    Parse `path` into normalized account dicts via content analysis.
    Each dict has all _FIELDS keys; `session` is always filled (derived from the
    best available identifier when absent). Rows with no identifier are dropped.
    """
    try:
        rows, header = _read_grid(path)
    except Exception:
        return []
    if not rows:
        return []
    colmap = _assign_columns(rows, header)

    out: "list[dict]" = []
    for cells in rows:
        rec = _empty_row()
        for i, val in enumerate(cells):
            field = colmap.get(i)
            val = (val or "").strip()
            if not field or not val or rec[field]:
                continue
            if field == "phone":
                val = re.sub(r"[ \-]", "", val)
            if field == "username":
                val = val.lstrip("@")
            rec[field] = val
        if not _usable(rec):
            continue
        if not rec["session"]:
            rec["session"] = derive_session(
                rec["phone"] or rec["user_id"] or rec["username"])
        out.append(rec)
    return out
