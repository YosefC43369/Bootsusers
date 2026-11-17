# orchestration/campaign.py
# ==============================================================================
#  Campaign engine — declarative multi-step automation pipelines.
# ------------------------------------------------------------------------------
#  A campaign is a named list of steps loaded from a dict / JSON / YAML file.
#  Each step runs one registered operation against a gated slice of the account
#  pool (skip quarantined, skip on-cooldown, cap count), logs every result to
#  the StateStore + MetricsRegistry, applies a post-action cooldown, and records
#  run history. Steps run sequentially; a step may fan its accounts out through
#  the WorkerPool for concurrency.
#
#  Operations are decoupled: the engine calls whatever is in the
#  OperationRegistry, so the suite's join/react/stars/views (and the engagement
#  / lifecycle ops) all plug in through one adapter signature:
#
#      async def op(accounts, params: dict, ctx: CampaignContext) -> Counter
# ==============================================================================

from __future__ import annotations

import json
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

from .state_store import StateStore
from .metrics import MetricsRegistry

OpFn = Callable[[list, dict, "CampaignContext"], Awaitable[Counter]]


# ------------------------------------------------------------------------------
#  Context passed to every operation
# ------------------------------------------------------------------------------

@dataclass
class CampaignContext:
    store: Optional[StateStore] = None
    metrics: Optional[MetricsRegistry] = None
    log: Callable[[str, str], None] = lambda lvl, msg: None
    should_stop: Callable[[], bool] = lambda: False

    def record(self, op: str, phone: str, status: str, target: str = "",
               detail: str = "", stars: int = 0) -> None:
        if self.store:
            self.store.log_action(phone, op, target, status, detail)
        if self.metrics:
            self.metrics.record(op, status, phone, stars=stars)


# ------------------------------------------------------------------------------
#  Operation registry
# ------------------------------------------------------------------------------

class OperationRegistry:
    def __init__(self):
        self._ops: "dict[str, OpFn]" = {}

    def register(self, name: str, fn: OpFn) -> None:
        self._ops[name] = fn

    def get(self, name: str) -> OpFn:
        if name not in self._ops:
            raise KeyError(f"operation not registered: {name!r} "
                           f"(have: {sorted(self._ops)})")
        return self._ops[name]

    def names(self) -> "list[str]":
        return sorted(self._ops)


# ------------------------------------------------------------------------------
#  Campaign spec
# ------------------------------------------------------------------------------

@dataclass
class StepSpec:
    op: str
    params: dict = field(default_factory=dict)
    count: int = 0                 # 0 = all eligible accounts
    cooldown_sec: float = 0.0      # per-account cooldown after this op
    skip_quarantined: bool = True
    concurrency: int = 0           # 0 = run op once with the whole slice
    pause_after_sec: float = 0.0   # delay before the next step

    @classmethod
    def from_dict(cls, d: dict) -> "StepSpec":
        return cls(
            op=d["op"],
            params=d.get("params", {}),
            count=int(d.get("count", 0)),
            cooldown_sec=float(d.get("cooldown_sec", 0)),
            skip_quarantined=bool(d.get("skip_quarantined", True)),
            concurrency=int(d.get("concurrency", 0)),
            pause_after_sec=float(d.get("pause_after_sec", 0)),
        )


@dataclass
class CampaignSpec:
    name: str
    steps: "list[StepSpec]" = field(default_factory=list)
    loop: bool = False             # repeat the whole pipeline
    loop_gap_sec: float = 0.0

    @classmethod
    def from_dict(cls, d: dict) -> "CampaignSpec":
        return cls(
            name=d.get("name", "campaign"),
            steps=[StepSpec.from_dict(s) for s in d.get("steps", [])],
            loop=bool(d.get("loop", False)),
            loop_gap_sec=float(d.get("loop_gap_sec", 0)),
        )

    @classmethod
    def from_file(cls, path: str) -> "CampaignSpec":
        text = open(path, encoding="utf-8").read()
        if path.lower().endswith((".yaml", ".yml")):
            try:
                import yaml  # optional
                data = yaml.safe_load(text)
            except ImportError as e:
                raise RuntimeError("PyYAML not installed; use a .json campaign "
                                   "file or `pip install pyyaml`") from e
        else:
            data = json.loads(text)
        return cls.from_dict(data)

    def to_dict(self) -> dict:
        return {
            "name": self.name, "loop": self.loop, "loop_gap_sec": self.loop_gap_sec,
            "steps": [{
                "op": s.op, "params": s.params, "count": s.count,
                "cooldown_sec": s.cooldown_sec, "skip_quarantined": s.skip_quarantined,
                "concurrency": s.concurrency, "pause_after_sec": s.pause_after_sec,
            } for s in self.steps],
        }


# ------------------------------------------------------------------------------
#  Runner
# ------------------------------------------------------------------------------

class CampaignRunner:
    def __init__(self, registry: OperationRegistry, ctx: CampaignContext):
        self.registry = registry
        self.ctx = ctx

    def _eligible(self, accounts: list, step: StepSpec) -> list:
        """Filter the pool for one step: healthy, not quarantined, not on cooldown."""
        pool = []
        for acc in accounts:
            if getattr(acc, "client", None) is None or not acc.healthy():
                continue
            phone = acc.phone
            if self.ctx.store:
                if step.skip_quarantined and self.ctx.store.is_quarantined(phone):
                    continue
                if self.ctx.store.is_on_cooldown(phone, step.op):
                    continue
            pool.append(acc)
        if step.count > 0:
            pool = pool[:step.count]
        return pool

    async def run(self, spec: CampaignSpec, accounts: list) -> dict:
        import asyncio
        store = self.ctx.store
        campaign_id = store.save_campaign(spec.name, spec.to_dict()) if store else 0
        run_id = store.start_campaign_run(campaign_id) if store else 0
        agg: Counter = Counter()
        status = "ok"

        try:
            passes = 0
            while True:
                passes += 1
                self.ctx.log("accent", f"campaign '{spec.name}' — pass {passes}")
                for i, step in enumerate(spec.steps, 1):
                    if self.ctx.should_stop():
                        self.ctx.log("warn", "campaign stopped by operator")
                        status = "stopped"
                        raise _Stop()
                    pool = self._eligible(accounts, step)
                    self.ctx.log("accent",
                                 f"step {i}/{len(spec.steps)}: {step.op} "
                                 f"→ {len(pool)} eligible account(s)")
                    if not pool:
                        self.ctx.log("warn", f"step {i}: no eligible accounts — skip")
                        continue
                    op = self.registry.get(step.op)
                    result = await op(pool, step.params, self.ctx)
                    for k, v in (result or Counter()).items():
                        agg[f"{step.op}:{k}"] += v
                        # feed live metrics + a per-run summary row from the result
                        if self.ctx.metrics:
                            self.ctx.metrics.record(step.op, k, count=int(v))
                        if self.ctx.store:
                            self.ctx.store.log_action(
                                "", step.op, str(step.params.get("channel", "")),
                                k, f"count={v}")
                    # apply cooldowns to the accounts we just used
                    if store and step.cooldown_sec > 0:
                        for acc in pool:
                            store.set_cooldown(acc.phone, step.op, step.cooldown_sec)
                    if step.pause_after_sec > 0 and not self.ctx.should_stop():
                        await asyncio.sleep(step.pause_after_sec)
                if not spec.loop:
                    break
                if self.ctx.should_stop():
                    break
                if spec.loop_gap_sec > 0:
                    await asyncio.sleep(spec.loop_gap_sec)
        except _Stop:
            pass
        except Exception as exc:
            status = "error"
            self.ctx.log("err", f"campaign error: {exc}")
        finally:
            if store:
                store.finish_campaign_run(run_id, status, dict(agg))
        self.ctx.log("accent", f"campaign '{spec.name}' done [{status}] — {dict(agg)}")
        return {"status": status, "stats": dict(agg)}


class _Stop(Exception):
    pass


# ------------------------------------------------------------------------------
#  Default registry — wires the suite's operations (lazy import, no cycle)
# ------------------------------------------------------------------------------

def build_default_registry() -> OperationRegistry:
    """
    Adapt bootsusers_suite's op_* (and engagement/lifecycle ops when present)
    to the (accounts, params, ctx) -> Counter signature.
    """
    reg = OperationRegistry()

    try:
        import bootsusers_suite as s

        async def join(accounts, params, ctx):
            return await s.op_join(accounts, params.get("channel", ""),
                                   limit=int(params.get("limit", 0)),
                                   log_fn=ctx.log, should_stop=ctx.should_stop,
                                   dry_run=bool(params.get("dry_run", False)))

        async def react(accounts, params, ctx):
            return await s.op_react(accounts, params.get("channel", ""),
                                    params.get("message", "latest"),
                                    params.get("emoji", "👍"),
                                    log_fn=ctx.log, should_stop=ctx.should_stop,
                                    dry_run=bool(params.get("dry_run", False)))

        async def stars(accounts, params, ctx):
            return await s.op_star(accounts, params.get("channel", ""),
                                   params.get("message", "latest"),
                                   int(params.get("stars", 1)),
                                   log_fn=ctx.log, should_stop=ctx.should_stop,
                                   anonymous=bool(params.get("anonymous", False)),
                                   clamp=bool(params.get("clamp", True)),
                                   dry_run=bool(params.get("dry_run", False)))

        async def views(accounts, params, ctx):
            return await s.op_views(accounts, params.get("channel", ""),
                                    params.get("message", "latest"),
                                    int(params.get("count", 0)),
                                    log_fn=ctx.log, should_stop=ctx.should_stop,
                                    dry_run=bool(params.get("dry_run", False)))

        reg.register("join", join)
        reg.register("react", react)
        reg.register("stars", stars)
        reg.register("views", views)
    except Exception:
        pass  # suite not importable in this context; registry stays empty

    # engagement ops (section 1) — vote / forward / comment / story
    try:
        from engagement.ops import op_vote, op_forward, op_comment, op_story

        async def vote(accounts, params, ctx):
            return await op_vote(accounts, params.get("channel", ""),
                                 params.get("message", ""),
                                 option_index=int(params.get("option_index", 0)),
                                 option_text=params.get("option_text", ""),
                                 log_fn=ctx.log, should_stop=ctx.should_stop,
                                 dry_run=bool(params.get("dry_run", False)))

        async def forward(accounts, params, ctx):
            return await op_forward(accounts, params.get("channel", ""),
                                    params.get("message", ""),
                                    to=params.get("to", "me"),
                                    log_fn=ctx.log, should_stop=ctx.should_stop,
                                    dry_run=bool(params.get("dry_run", False)))

        async def comment(accounts, params, ctx):
            return await op_comment(accounts, params.get("channel", ""),
                                    params.get("message", ""),
                                    params.get("texts", params.get("text", "")),
                                    log_fn=ctx.log, should_stop=ctx.should_stop,
                                    dry_run=bool(params.get("dry_run", False)))

        async def story(accounts, params, ctx):
            return await op_story(accounts, params.get("channel", ""),
                                  react_emoji=params.get("emoji", ""),
                                  log_fn=ctx.log, should_stop=ctx.should_stop,
                                  dry_run=bool(params.get("dry_run", False)))

        reg.register("vote", vote)
        reg.register("forward", forward)
        reg.register("comment", comment)
        reg.register("story", story)
    except Exception:
        pass

    # growth ops (phase 2) — scrape / invite / dm / dripjoin
    try:
        from growth import op_scrape, op_invite, op_dm, op_dripjoin, load_members

        async def scrape(accounts, params, ctx):
            return await op_scrape(accounts, params.get("source", ""),
                                   params.get("out_file", "data/members.json"),
                                   limit=int(params.get("limit", 0)),
                                   aggressive=bool(params.get("aggressive", False)),
                                   search=params.get("search", ""),
                                   log_fn=ctx.log, should_stop=ctx.should_stop,
                                   dry_run=bool(params.get("dry_run", False)))

        def _gap(params, default):
            g = params.get("gap")
            return tuple(g) if isinstance(g, (list, tuple)) and len(g) == 2 else default

        async def invite(accounts, params, ctx):
            members = params.get("members") or load_members(
                params.get("members_file", "data/members.json"))
            return await op_invite(accounts, params.get("target", ""), members,
                                   per_account=int(params.get("per_account", 20)),
                                   batch=int(params.get("batch", 3)),
                                   allow_id_fallback=bool(params.get("allow_id_fallback", False)),
                                   log_fn=ctx.log, should_stop=ctx.should_stop,
                                   store=ctx.store, dry_run=bool(params.get("dry_run", False)),
                                   gap=_gap(params, (20.0, 50.0)))

        async def dm(accounts, params, ctx):
            members = params.get("members") or load_members(
                params.get("members_file", "data/members.json"))
            return await op_dm(accounts, members,
                               params.get("templates", params.get("text", "")),
                               per_account=int(params.get("per_account", 15)),
                               simulate_typing=bool(params.get("simulate_typing", True)),
                               log_fn=ctx.log, should_stop=ctx.should_stop,
                               store=ctx.store, dry_run=bool(params.get("dry_run", False)),
                               gap=_gap(params, (25.0, 60.0)))

        async def dripjoin(accounts, params, ctx):
            return await op_dripjoin(accounts, params.get("channel", ""),
                                     wave_size=int(params.get("wave_size", 3)),
                                     wave_gap=_gap(params, (30.0, 90.0)),
                                     log_fn=ctx.log, should_stop=ctx.should_stop,
                                     dry_run=bool(params.get("dry_run", False)))

        reg.register("scrape", scrape)
        reg.register("invite", invite)
        reg.register("dm", dm)
        reg.register("dripjoin", dripjoin)
    except Exception:
        pass

    return reg
