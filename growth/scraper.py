# growth/scraper.py
# ==============================================================================
#  Member scraping — harvest a source group/channel's participant list.
# ------------------------------------------------------------------------------
#  Collects (id, username, name, access_hash, bot/deleted flags) for every
#  reachable participant, de-duplicated, saved to JSON for later invite/DM
#  campaigns. Note: access_hash is per-account in Telegram, so cross-account
#  invites should prefer username resolution — the scraper keeps both and the
#  inviter falls back gracefully.
# ==============================================================================

from __future__ import annotations

import json
import os
from collections import Counter
from typing import Callable, Optional

LogFn = Callable[[str, str], None]
StopFn = Callable[[], bool]


def _member_dict(u) -> dict:
    return {
        "id": u.id,
        "username": getattr(u, "username", None),
        "first_name": getattr(u, "first_name", None),
        "last_name": getattr(u, "last_name", None),
        "access_hash": getattr(u, "access_hash", None),
        "bot": bool(getattr(u, "bot", False)),
        "deleted": bool(getattr(u, "deleted", False)),
        "phone": getattr(u, "phone", None),
    }


async def scrape_members(client, source, *, limit: int = 0,
                         aggressive: bool = False, search: str = "",
                         skip_bots: bool = True, skip_deleted: bool = True,
                         log_fn: LogFn, should_stop: StopFn) -> "list[dict]":
    """Iterate participants of `source` and return member dicts."""
    out: "list[dict]" = []
    seen: "set[int]" = set()
    kwargs = {}
    if aggressive:
        kwargs["aggressive"] = True
    if search:
        kwargs["search"] = search
    try:
        async for user in client.iter_participants(source, limit=limit or None, **kwargs):
            if should_stop():
                break
            if user.id in seen:
                continue
            if skip_bots and getattr(user, "bot", False):
                continue
            if skip_deleted and getattr(user, "deleted", False):
                continue
            seen.add(user.id)
            out.append(_member_dict(user))
            if len(out) % 200 == 0:
                log_fn("info", f"scraped {len(out)} members ...")
    except Exception as exc:
        log_fn("err", f"scrape error: {exc}")
    log_fn("ok", f"scraped {len(out)} members from source")
    return out


def save_members(members: "list[dict]", path: str, log_fn: LogFn = None) -> None:
    if os.path.dirname(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(members, f, ensure_ascii=False, indent=2)
    if log_fn:
        log_fn("ok", f"saved {len(members)} members → {path}")


def load_members(path: str) -> "list[dict]":
    with open(path, encoding="utf-8") as f:
        return json.load(f)


async def op_scrape(accounts, source: str, out_file: str, *,
                    limit: int = 0, aggressive: bool = False, search: str = "",
                    log_fn: LogFn, should_stop: StopFn,
                    dry_run: bool = False) -> Counter:
    """Registry-compatible op: use the first healthy account to scrape `source`."""
    stats: Counter = Counter()
    pool = [a for a in accounts if getattr(a, "client", None) and a.healthy()]
    if not pool:
        log_fn("err", "no connected account")
        return stats
    if dry_run:
        log_fn("info", f"[dry] would scrape {source} → {out_file}")
        stats["dry"] += 1
        return stats
    members = await scrape_members(pool[0].client, source, limit=limit,
                                   aggressive=aggressive, search=search,
                                   log_fn=log_fn, should_stop=should_stop)
    save_members(members, out_file, log_fn)
    stats["scraped"] += len(members)
    with_username = sum(1 for m in members if m.get("username"))
    stats["with_username"] += with_username
    log_fn("accent", f"SCRAPE done — {len(members)} ({with_username} have @username)")
    return stats
