"""The shared half of every compute-server instance: routes, auth, job plumbing.

`create_app()` owns everything that was byte-identical between the
materials-framework and thermocalc servers. An instance contributes exactly
two things: a Capabilities descriptor, and whatever startup probing its own
toolchain needs. LAMMPS would have been the third copy — see
docs/infra-cleanup-2026-08.md S-1.

Instances may still add their own routes to the returned app; that is how the
deprecated /manifest and /mlips shims survive their migration period.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

from fastapi import Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from compute_server_base.auth import check_ws_token, require_token, set_audience

# Runtime import, not TYPE_CHECKING: FastAPI resolves the /capabilities return
# annotation into a response model when the route is registered, and a forward
# ref it can't resolve fails at request time, not import time.
from compute_server_base.capabilities import Capabilities  # noqa: TC001 — see above; must resolve at runtime
from compute_server_base.jobs import TERMINAL_STATUSES, JobManager

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable


class JobRequest(BaseModel):
    """POST /jobs request body — see jobs.Job for field meanings."""

    code: str
    entrypoint: str = "run"
    variables: dict[str, Any] = {}
    correlation_id: str | None = None
    timeout_s: float | None = None


def create_app(
    *,
    tool_name: str,
    capabilities: Callable[[], Capabilities],
    on_startup: Callable[[], None] | None = None,
    mcp_mount: Callable[[FastAPI], Any] | None = None,
) -> FastAPI:
    """Build an instance's FastAPI app with the shared routes mounted.

    Args:
        tool_name: Identity reported in job provenance and the app title.
        capabilities: Called per request rather than once at import, so an
            instance reports what is true now — a mount that disappeared or a
            probe that started failing shows up without a restart.
        on_startup: Instance-specific startup probing, run after the job worker
            starts. Raising here aborts startup; log instead if the server
            should come up degraded and report it through `capabilities`.
        mcp_mount: Called with the finished app to mount an MCP facade, and
            returning the MCPServer. Taken as a callback rather than a value
            because the facade needs the built app while the app's lifespan
            needs the facade's session manager — this is what unties that knot.

    Returns:
        The app, with `app.state.jobs` holding the JobManager.
    """
    # Before any route exists: a dashboard-issued token carries `aud: tool:<name>`,
    # so this server has to know its own name to reject one minted for a sibling
    # instance (S-4).
    set_audience(tool_name)

    manager = JobManager()
    # Assigned below, after the app exists; the lifespan closure reads it at
    # startup, by which point mcp_mount has run.
    mcp_server: list[Any] = []

    @contextlib.asynccontextmanager
    async def _lifespan(_app: FastAPI) -> AsyncIterator[None]:
        manager.start()
        if on_startup is not None:
            on_startup()
        if mcp_server:
            async with mcp_server[0].session_manager.run():
                yield
        else:
            yield

    app = FastAPI(title=f"{tool_name} compute server", docs_url=None, redoc_url=None, lifespan=_lifespan)
    app.state.jobs = manager

    @app.get("/health")
    async def health() -> dict[str, str]:
        """Unauthenticated liveness check."""
        return {"status": "ok"}

    @app.get("/capabilities", dependencies=[Depends(require_token)])
    async def get_capabilities() -> Capabilities:
        """What this instance can do and whether it can do it on this host."""
        return capabilities()

    @app.post("/jobs", dependencies=[Depends(require_token)])
    async def create_job(req: JobRequest) -> dict[str, str]:
        """Enqueue a job; returns immediately with a job_id in queued status."""
        job = await manager.submit(req.code, req.entrypoint, req.variables, req.correlation_id, req.timeout_s)
        return {"job_id": job.id, "status": job.status.value}

    @app.get("/jobs/{job_id}", dependencies=[Depends(require_token)])
    async def job_status(job_id: str) -> dict[str, Any]:
        """Point-in-time status; poll this or use the WS stream for live updates."""
        job = manager.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return {"job_id": job.id, "status": job.status.value, "correlation_id": job.correlation_id}

    @app.get("/jobs/{job_id}/result", dependencies=[Depends(require_token)])
    async def job_result(job_id: str) -> dict[str, Any]:
        """Final values/error/provenance. 409s until the job reaches a terminal status."""
        job = manager.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status not in TERMINAL_STATUSES:
            raise HTTPException(status_code=409, detail=f"job is {job.status.value}, not finished yet")
        return {
            "status": job.status.value,
            "values": job.values,
            "error": job.error,
            "provenance": {
                "tool_name": tool_name,
                "wall_time_s": job.wall_time_s(),
            },
        }

    @app.post("/jobs/{job_id}/cancel", dependencies=[Depends(require_token)])
    async def cancel_job(job_id: str) -> dict[str, str]:
        """Best-effort cancel — see jobs.py module docstring for what that means for a running job."""
        ok = await manager.cancel(job_id)
        if not ok:
            raise HTTPException(status_code=404, detail="job not found")
        return {"status": "ok"}

    @app.websocket("/jobs/{job_id}/stream")
    async def job_stream(websocket: WebSocket, job_id: str) -> None:
        """Replay log history then live-stream new log/result/error events for a job."""
        token = websocket.headers.get("authorization", "").removeprefix("Bearer ").strip()
        if not token:
            token = websocket.query_params.get("token", "")
        if not check_ws_token(token):
            await websocket.close(code=4401)
            return

        await websocket.accept()
        job = manager.get(job_id)
        if job is None:
            await websocket.send_json({"type": "error", "message": "job not found"})
            await websocket.close()
            return

        snapshot, queue = job.subscribe()
        try:
            for event in snapshot:
                await websocket.send_json(event)
            if job.status not in TERMINAL_STATUSES:
                while True:
                    event = await queue.get()
                    if event is None:
                        break
                    await websocket.send_json(event)
        except WebSocketDisconnect:
            pass
        finally:
            job.unsubscribe(queue)
            with contextlib.suppress(Exception):
                await websocket.close()

    if mcp_mount is not None:
        mcp_server.append(mcp_mount(app))

    return app
