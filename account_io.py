# account_io.py
# ==============================================================================
#  Shared account-file reader for every entrypoint.
# ------------------------------------------------------------------------------
#  Returns normalized row dicts from an account file, handling:
#    • delimited files WITH a header  (.csv/.txt/.tsv, comma/semicolon/tab/pipe)
#      with tolerant column aliases (phone/number/msisdn, session/name, proxy_*,
#      id/email, api_id/api_hash);
#    • HEADERLESS text files (.txt dumps), one account per line in any of:
#          +123456789
#          +123456789:session_name
#          +123456789:api_id:api_hash
#          +123456789,session,proxy_type,proxy_host,proxy_port
#          +123456789 | socks5 | 1.2.3.4 | 1080
#      — the first phone-like token is the phone; a following short word token is
#      taken as the session name, otherwise it is derived from the phone.
#  Also lifts the csv field-size limit so very large fields don't abort a load.
# ==============================================================================

from __future__ import annotations

import csv
import re

_ALIASES = {
    "phone":    ("phone", "phone_number", "number", "msisdn", "tel", "telephone"),
    "session":  ("session_name", "session", "session_file", "name"),
    "user_id":  ("id", "user_id", "uid", "tg_id", "telegram_id", "account_id"),
    "email":    ("email", "mail", "email_address", "e-mail"),
    "api_id":   ("api_id", "apiid", "app_id"),
    "api_hash": ("api_hash", "apihash", "app_hash"),
    "proxy_type": ("proxy_type", "proxytype"),
    "proxy_host": ("proxy_host", "proxyhost", "proxy_ip"),
    "proxy_port": ("proxy_port", "proxyport"),
    "proxy_user": ("proxy_user", "proxyuser", "proxy_username"),
    "proxy_pass": ("proxy_pass", "proxypass", "proxy_password"),
}

_FIELDS = tuple(_ALIASES.keys())
_PHONE_RE = re.compile(r"\+?\d{7,15}")
_SESSION_RE = re.compile(r"[\w.\-]{1,64}")


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


def _pick(row: dict, keys) -> str:
    for k in keys:
        if k in row and str(row[k]).strip():
            return str(row[k]).strip()
    return ""


def _looks_like_phone(token: str) -> bool:
    t = token.replace(" ", "").replace("-", "")
    return bool(re.fullmatch(r"\+?\d{7,15}", t))


def _empty_row() -> dict:
    return {f: "" for f in _FIELDS}


def _sniff_delim(sample: str) -> str:
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
    except Exception:
        return ","


def _read_with_header(path: str) -> "list[dict]":
    out: "list[dict]" = []
    with open(path, newline="", encoding="utf-8-sig", errors="ignore") as f:
        sample = f.read(4096)
        f.seek(0)
        reader = csv.DictReader(f, delimiter=_sniff_delim(sample))
        if not reader.fieldnames:
            return out
        # A real header must expose a phone-ish column; otherwise this is a
        # headerless file and the first line is data, so bail to the fallback.
        header = {(h or "").strip().lower() for h in reader.fieldnames}
        if not (header & set(_ALIASES["phone"])):
            return out
        for raw in reader:
            row = {(k or "").strip().lower(): v for k, v in raw.items()}
            rec = _empty_row()
            for field, keys in _ALIASES.items():
                rec[field] = _pick(row, keys)
            if rec["phone"]:
                out.append(rec)
    return out


_LINE_PHONE = re.compile(r"\+?\d[\d\s\-]{5,16}\d")
_PROXY_KW = {"socks5", "socks4", "http", "https", "mtproto"}


def _read_headerless(path: str) -> "list[dict]":
    out: "list[dict]" = []
    with open(path, encoding="utf-8-sig", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("//"):
                continue
            m = _LINE_PHONE.search(line)
            if not m:
                continue
            phone = re.sub(r"[\s\-]", "", m.group())
            if not _looks_like_phone(phone):
                continue
            # tokens after the phone: split on structural delimiters, but if the
            # remainder has none, fall back to whitespace (space-separated dumps)
            tail_str = line[m.end():]
            if re.search(r"[,;:\t|]", tail_str):
                rest = [t.strip() for t in re.split(r"[,;:\t|]+", tail_str) if t.strip()]
            else:
                rest = tail_str.split()
            rec = _empty_row()
            rec["phone"] = phone

            if rest and rest[0].lower() in _PROXY_KW and len(rest) >= 3:
                # phone | type | host | port [...]
                rec["proxy_type"] = rest[0].lower()
                rec["proxy_host"] = rest[1]
                rec["proxy_port"] = rest[2]
                if len(rest) >= 5:
                    rec["proxy_user"], rec["proxy_pass"] = rest[3], rest[4]
            elif rest:
                first = rest[0]
                # a plausible session name = short word, not another phone, not
                # a bare number (that's usually an api_id), not a proxy keyword
                if (_SESSION_RE.fullmatch(first) and not _looks_like_phone(first)
                        and not first.isdigit() and first.lower() not in _PROXY_KW):
                    rec["session"] = first
                    tail = rest[1:]
                else:
                    tail = rest[1:]
                # optional proxy block after the session token
                if tail and tail[0].lower() in _PROXY_KW and len(tail) >= 3:
                    rec["proxy_type"] = tail[0].lower()
                    rec["proxy_host"] = tail[1]
                    rec["proxy_port"] = tail[2]
                    if len(tail) >= 5:
                        rec["proxy_user"], rec["proxy_pass"] = tail[3], tail[4]
            out.append(rec)
    return out


def read_rows(path: str) -> "list[dict]":
    """
    Return a list of normalized account dicts from `path`. Tries header-based
    parsing first, then falls back to headerless line parsing (plain .txt dumps).
    Every dict has all _FIELDS keys (missing ones are "").
    """
    raise_csv_field_limit()
    try:
        rows = _read_with_header(path)
    except Exception:
        rows = []
    if not rows:
        try:
            rows = _read_headerless(path)
        except Exception:
            rows = []
    return rows


def derive_session(phone: str) -> str:
    return "session_" + re.sub(r"[^0-9A-Za-z]", "", phone)
