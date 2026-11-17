# bootsusers_pro.py
# ==============================================================================
#  BOOTSUSERS // PRO — orchestration console.
# ------------------------------------------------------------------------------
#  Ties the whole advanced stack together on top of the single-file suite:
#
#    • every operation (join/react/stars/views + vote/forward/comment/story)
#      runs through the campaign OperationRegistry, persisted + metered;
#    • SQLite state: account registry, health, quarantine, cooldowns, history;
#    • health layer: dead/restricted accounts auto-quarantined and skipped;
#    • warm-up sweeps to age fresh accounts;
#    • campaign pipelines from a JSON/YAML file, optionally scheduled;
#    • reactive engine: a listener account auto-boosts new posts / keywords;
#    • live + historical metrics; optional FastAPI control plane.
#
#  Headless and scriptable. The GUI (bootsusers_suite.py) stays the single-file
#  front-end; this is the power-user / automation entrypoint.
#
#  Examples:
#    python bootsusers_pro.py op views   --channel @c --message latest --count 50
#    python bootsusers_pro.py op story   --channel @c --emoji 🔥
#    python bootsusers_pro.py warmup
#    python bootsusers_pro.py campaign campaigns/nightly.json
#    python bootsusers_pro.py reactive   --channel @c --emoji 🔥 --views
#    python bootsusers_pro.py metrics
#    python bootsusers_pro.py accounts
#    python bootsusers_pro.py serve --port 8900
# ==============================================================================

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass, field
from typing import Optional

import bootsusers_suite as suite
from bootsusers_suite import Account, load_accounts, connect_pool, get_star_balance

from orchestration import (
    StateStore, MetricsRegistry, report_from_store,
    OperationRegistry, CampaignContext, CampaignRunner, CampaignSpec,
    AsyncScheduler, build_default_registry,
)
from lifecycle import HealthMonitor, probe_account, HealthSignal, warmup_pool, WarmupConfig


def _log(level: str, text: str) -> None:
    prefix = {"ok": "[+]", "warn": "[!]", "err": "[x]", "accent": "[*]"}.get(level, "   ")
    print(f"{prefix} {text}")


# ------------------------------------------------------------------------------
#  Runtime — bundles store + metrics + health + registry
# ------------------------------------------------------------------------------

@dataclass
class SuiteRuntime:
    store: StateStore
    metrics: MetricsRegistry
    health: HealthMonitor
    registry: OperationRegistry
    accounts: "list[Account]" = field(default_factory=list)

    @classmethod
    def build(cls, db_path: str = "data/bootsusers.db") -> "SuiteRuntime":
        store = StateStore(db_path)
        metrics = MetricsRegistry()
        health = HealthMonitor(store, log=_log)
        registry = build_default_registry()
        return cls(store=store, metrics=metrics, health=health, registry=registry)

    def context(self, should_stop=lambda: False) -> CampaignContext:
        return CampaignContext(store=self.store, metrics=self.metrics,
                               log=_log, should_stop=should_stop)

    def live(self) -> "list[Account]":
        return [a for a in self.accounts if a.client and a.healthy()
                and not self.store.is_quarantined(a.phone)]


async def prepare(rt: SuiteRuntime, csv_dir: str, concurrency: int,
                  health_check: bool = True) -> None:
    rt.accounts = load_accounts(csv_dir, _log)
    for a in rt.accounts:                      # register in the store
        rt.store.upsert_account(a.phone, session=a.session,
                                user_id=a.user_id, email=a.email)
    _log("accent", f"connecting {len(rt.accounts)} account(s) ...")
    await connect_pool(rt.accounts, _log, concurrency=concurrency)

    live = [a for a in rt.accounts if a.client and a.healthy()]
    for a in rt.accounts:
        rt.store.set_status(a.phone, "connected" if a in live else "offline")

    if health_check and live:
        _log("accent", "health-probing connected accounts ...")
        async def _probe(acc):
            sig, detail = await probe_account(acc.client, acc.phone)
            if sig == HealthSignal.HARD_BAN:
                rt.store.quarantine(acc.phone, 365 * 24 * 3600, "probe:hard-ban")
                _log("err", f"{acc.phone} banned — quarantined")
            elif sig == HealthSignal.SPAM_BLOCK:
                rt.health.on_error(acc.phone, Exception(detail))
                _log("warn", f"{acc.phone} restricted — {detail}")
            else:
                rt.store.set_status(acc.phone, "healthy")
        await asyncio.gather(*(_probe(a) for a in live))

    usable = rt.live()
    _log("ok", f"{len(usable)}/{len(rt.accounts)} account(s) usable "
               f"({len(rt.store.quarantined_phones())} quarantined)")


async def disconnect_all(rt: SuiteRuntime) -> None:
    for a in rt.accounts:
        if a.client:
            try:
                await a.client.disconnect()
            except Exception:
                pass


# ------------------------------------------------------------------------------
#  Commands
# ------------------------------------------------------------------------------

async def cmd_op(args) -> int:
    rt = SuiteRuntime.build(args.db)
    await prepare(rt, args.csv_dir, args.concurrency)
    pool = rt.live()
    if args.op == "stars":
        await asyncio.gather(*(get_star_balance(a, _log) for a in pool))
    if not pool:
        _log("err", "no usable accounts")
        return 3
    params = {
        "channel": args.channel, "message": args.message, "emoji": args.emoji,
        "count": args.count, "stars": args.count, "limit": args.limit,
        "anonymous": args.anonymous, "dry_run": args.dry_run,
        "option_index": args.option_index, "text": args.text, "to": args.to,
    }
    ctx = rt.context()
    try:
        op = rt.registry.get(args.op)
    except KeyError as e:
        _log("err", str(e))
        return 2
    try:
        result = await op(pool, params, ctx)
        for k, v in (result or {}).items():
            rt.metrics.record(args.op, k, count=int(v))
    finally:
        await disconnect_all(rt)
    print(json.dumps({"op": args.op, "result": dict(result or {}),
                      "live_metrics": rt.metrics.snapshot()}, ensure_ascii=False, indent=2))
    rt.store.close()
    return 0


async def cmd_warmup(args) -> int:
    rt = SuiteRuntime.build(args.db)
    await prepare(rt, args.csv_dir, args.concurrency, health_check=False)
    cfg = WarmupConfig(avatar_dir=args.avatar_dir)
    try:
        res = await warmup_pool(rt.live(), rt.store, cfg, _log)
    finally:
        await disconnect_all(rt)
    print(json.dumps(res, ensure_ascii=False, indent=2))
    rt.store.close()
    return 0


async def cmd_campaign(args) -> int:
    rt = SuiteRuntime.build(args.db)
    spec = CampaignSpec.from_file(args.file)
    await prepare(rt, args.csv_dir, args.concurrency)
    ctx = rt.context()
    runner = CampaignRunner(rt.registry, ctx)
    try:
        if args.schedule_every:
            sched = AsyncScheduler(log=_log)
            sched.every(args.schedule_every, lambda: runner.run(spec, rt.live()),
                        name=spec.name, jitter_sec=args.schedule_every * 0.1)
            await sched.start()
            _log("accent", f"campaign '{spec.name}' scheduled every "
                           f"{args.schedule_every}s — Ctrl-C to stop")
            try:
                while True:
                    await asyncio.sleep(3600)
            except (KeyboardInterrupt, asyncio.CancelledError):
                await sched.stop()
        else:
            out = await runner.run(spec, rt.live())
            print(json.dumps(out, ensure_ascii=False, indent=2))
    finally:
        await disconnect_all(rt)
    rt.store.close()
    return 0


async def cmd_reactive(args) -> int:
    from reactive import ReactiveEngine
    rt = SuiteRuntime.build(args.db)
    await prepare(rt, args.csv_dir, args.concurrency)
    pool = rt.live()
    if not pool:
        _log("err", "no usable accounts")
        return 3
    listener = pool[0]                          # first account listens
    actions = []
    if args.views:
        actions.append(("views", {"count": args.count}))
    if args.emoji:
        actions.append(("react", {"emoji": args.emoji}))
    if args.stars:
        actions.append(("stars", {"stars": args.stars}))
    if not actions:
        actions = [("views", {})]
    eng = ReactiveEngine(listener.client, rt.live, rt.registry, rt.context(), log=_log)
    eng.watch_new_posts(args.channel, actions)
    _log("accent", f"reactive boost on new posts in {args.channel}: {actions}")
    _log("accent", "listening — Ctrl-C to stop")
    try:
        await eng.run_forever()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        eng.stop()
        await disconnect_all(rt)
    rt.store.close()
    return 0


def cmd_metrics(args) -> int:
    store = StateStore(args.db)
    rep = report_from_store(store, args.window)
    print(json.dumps(rep, ensure_ascii=False, indent=2))
    store.close()
    return 0


def cmd_accounts(args) -> int:
    store = StateStore(args.db)
    recs = store.all_accounts()
    rows = [{
        "phone": r.phone, "status": r.status, "stars": r.star_balance,
        "warmup": r.warmup_stage, "failures": r.consecutive_failures,
        "quarantined": r.is_quarantined(),
        "quarantine_reason": r.quarantine_reason,
    } for r in recs]
    print(json.dumps({"count": len(rows),
                      "quarantined": store.quarantined_phones(),
                      "accounts": rows}, ensure_ascii=False, indent=2))
    store.close()
    return 0


async def cmd_serve(args) -> int:
    from orchestration.api_server import create_app, serve
    rt = SuiteRuntime.build(args.db)
    await prepare(rt, args.csv_dir, args.concurrency)
    app = create_app(store=rt.store, metrics=rt.metrics, registry=rt.registry,
                     accounts_provider=rt.live, token=args.token)
    _log("accent", f"serving control plane on http://{args.host}:{args.port}")
    # uvicorn.run is blocking; run in a thread so our loop can hold connections
    import threading
    threading.Thread(target=serve, args=(app, args.host, args.port),
                     daemon=True).start()
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await disconnect_all(rt)
    return 0


# ------------------------------------------------------------------------------
#  CLI
# ------------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="bootsusers_pro.py",
        description="Bootsusers PRO — orchestration console (state, health, "
                    "campaigns, reactive, metrics).")
    p.add_argument("--db", default="data/bootsusers.db")
    p.add_argument("--csv-dir", default=suite.DEFAULT_ACCOUNTS_DIR)
    p.add_argument("--concurrency", type=int, default=8)
    sub = p.add_subparsers(dest="cmd", required=True)

    o = sub.add_parser("op", help="run a single operation")
    o.add_argument("op", choices=["join", "react", "stars", "views",
                                  "vote", "forward", "comment", "story"])
    o.add_argument("--channel", default="@your_channel")
    o.add_argument("--message", default="latest")
    o.add_argument("--emoji", default="👍")
    o.add_argument("--count", type=int, default=0)
    o.add_argument("--limit", type=int, default=0)
    o.add_argument("--option-index", type=int, default=0)
    o.add_argument("--text", default="")
    o.add_argument("--to", default="me")
    o.add_argument("--anonymous", action="store_true")
    o.add_argument("--dry-run", action="store_true")

    w = sub.add_parser("warmup", help="run a warm-up sweep over the pool")
    w.add_argument("--avatar-dir", default=None)

    c = sub.add_parser("campaign", help="run a campaign pipeline from a file")
    c.add_argument("file")
    c.add_argument("--schedule-every", type=float, default=0,
                   help="re-run every N seconds instead of once")

    r = sub.add_parser("reactive", help="auto-boost new posts in a channel")
    r.add_argument("--channel", required=True)
    r.add_argument("--emoji", default="")
    r.add_argument("--count", type=int, default=0)
    r.add_argument("--stars", type=int, default=0)
    r.add_argument("--views", action="store_true")

    m = sub.add_parser("metrics", help="print historical metrics from the DB")
    m.add_argument("--window", type=float, default=None, help="last N seconds")

    sub.add_parser("accounts", help="list accounts + quarantine status")

    s = sub.add_parser("serve", help="launch the FastAPI control plane")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8900)
    s.add_argument("--token", default=None)
    return p


def main() -> int:
    args = build_parser().parse_args()
    import logging
    logging.basicConfig(level=logging.WARNING)
    try:
        if args.cmd == "op":
            return asyncio.run(cmd_op(args))
        if args.cmd == "warmup":
            return asyncio.run(cmd_warmup(args))
        if args.cmd == "campaign":
            return asyncio.run(cmd_campaign(args))
        if args.cmd == "reactive":
            return asyncio.run(cmd_reactive(args))
        if args.cmd == "serve":
            return asyncio.run(cmd_serve(args))
        if args.cmd == "metrics":
            return cmd_metrics(args)
        if args.cmd == "accounts":
            return cmd_accounts(args)
    except KeyboardInterrupt:
        print("interrupted")
        return 130
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
