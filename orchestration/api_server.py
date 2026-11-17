# orchestration/api_server.py
# ==============================================================================
#  Optional REST control plane (FastAPI) for the Bootsusers suite.
# ------------------------------------------------------------------------------
#  Exposes endpoints to read live/historical metrics, list accounts, trigger a
#  single operation, and launch a campaign — so external automation (cron, CI,
#  another service) can drive the pool over HTTP.
#
#  FastAPI + uvicorn are NOT required dependencies; the module imports cleanly
#  without them and create_app() raises a clear error if they're missing.
#  Install to enable:  pip install fastapi uvicorn
# ==============================================================================

from __future__ import annotations

import asyncio
from typing import Callable, Optional

from .state_store import StateStore
from .metrics import MetricsRegistry, report_from_store
from .campaign import (OperationRegistry, CampaignContext, CampaignRunner,
                       CampaignSpec)


def create_app(
    *,
    store: StateStore,
    metrics: MetricsRegistry,
    registry: OperationRegistry,
    accounts_provider: Callable[[], list],
    token: Optional[str] = None,
):
    """
    Build the FastAPI app. `accounts_provider` returns the live account pool,
    `token` (optional) enables a simple Bearer-token guard on mutating routes.
    """
    try:
        from fastapi import FastAPI, HTTPException, Header
        from pydantic import BaseModel
    except ImportError as e:
        raise RuntimeError(
            "FastAPI is not installed. Run: pip install fastapi uvicorn") from e

    app = FastAPI(title="Bootsusers Control Plane", version="2.0")

    def _auth(authorization: Optional[str]) -> None:
        if token is None:
            return
        if authorization != f"Bearer {token}":
            raise HTTPException(status_code=401, detail="bad token")

    class OpRequest(BaseModel):
        op: str
        params: dict = {}
        count: int = 0

    class CampaignRequest(BaseModel):
        spec: dict

    @app.get("/health")
    async def health():
        return {"ok": True, "ops": registry.names()}

    @app.get("/metrics/live")
    async def metrics_live():
        return metrics.snapshot()

    @app.get("/metrics/history")
    async def metrics_history(window_sec: Optional[float] = None):
        return report_from_store(store, window_sec)

    @app.get("/accounts")
    async def accounts():
        recs = store.all_accounts()
        return {
            "count": len(recs),
            "quarantined": store.quarantined_phones(),
            "accounts": [{
                "phone": r.phone, "status": r.status, "stars": r.star_balance,
                "failures": r.consecutive_failures, "warmup": r.warmup_stage,
            } for r in recs],
        }

    @app.post("/op")
    async def run_op(req: OpRequest, authorization: Optional[str] = Header(None)):
        _auth(authorization)
        pool = [a for a in accounts_provider() if getattr(a, "client", None)
                and a.healthy()]
        if req.count > 0:
            pool = pool[:req.count]
        if not pool:
            raise HTTPException(409, "no connected accounts")
        ctx = CampaignContext(store=store, metrics=metrics)
        try:
            op = registry.get(req.op)
        except KeyError as e:
            raise HTTPException(404, str(e))
        result = await op(pool, req.params, ctx)
        return {"op": req.op, "accounts": len(pool), "result": dict(result or {})}

    @app.post("/campaign")
    async def run_campaign(req: CampaignRequest,
                           authorization: Optional[str] = Header(None)):
        _auth(authorization)
        pool = [a for a in accounts_provider() if getattr(a, "client", None)
                and a.healthy()]
        if not pool:
            raise HTTPException(409, "no connected accounts")
        ctx = CampaignContext(store=store, metrics=metrics)
        runner = CampaignRunner(registry, ctx)
        spec = CampaignSpec.from_dict(req.spec)
        # launch in background so the HTTP call returns immediately
        asyncio.create_task(runner.run(spec, pool))
        return {"launched": spec.name, "steps": len(spec.steps)}

    return app


def serve(app, host: str = "127.0.0.1", port: int = 8900) -> None:
    """Blocking uvicorn server (convenience)."""
    try:
        import uvicorn
    except ImportError as e:
        raise RuntimeError("uvicorn is not installed. Run: pip install uvicorn") from e
    uvicorn.run(app, host=host, port=port)
