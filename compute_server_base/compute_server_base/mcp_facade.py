"""An MCP face on the same job engine the REST routes drive.

One process, one queue, one credential — agents attach over MCP, programs keep
using REST. MCP is for planning and selection; REST is for execution, which is
why nothing existing has to migrate (docs/infra-cleanup-2026-08.md S-2b).

Three things this file is careful about:

- **Tools are async-shaped.** A relax or a LAMMPS run outlives one request, and
  an MCP tool call is request/response. `submit_*` enqueues and returns a job
  id; the caller polls `get_job` and reads `get_result`. Nothing blocks.
- **The tool list is reconciled at startup.** It is generated from the same
  Capabilities descriptor the REST `/capabilities` route serves, after that
  descriptor has filtered backends by live import probe. The MCP tool list
  therefore cannot advertise something the instance can't run.
- **Planning stays out.** `search_workflows` is a lookup over the knowledge
  pack, not a decision. A tool server that makes experimental decisions is the
  wrong split.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from starlette.responses import JSONResponse

from compute_server_base.auth import check_ws_token
from compute_server_base.jobs import TERMINAL_STATUSES
from compute_server_base.knowledge import load_knowledge, reconcile, search

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from fastapi import FastAPI
    from mcp.server import MCPServer

    from compute_server_base.capabilities import Capabilities, Operation
    from compute_server_base.jobs import JobManager
    from compute_server_base.knowledge import KnowledgeDoc


def _bearer_guard(inner: Any) -> Any:
    """Wrap the mounted MCP app in the same bearer check the REST routes use.

    A Starlette middleware on the sub-app would be the obvious move, but the
    mounted app is driven by its own session manager and the token has to be
    rejected before any of that runs — so this sits in front as plain ASGI.
    """

    async def guarded(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await inner(scope, receive, send)
            return
        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers", [])}
        token = headers.get("authorization", "").removeprefix("Bearer ").removeprefix("bearer ").strip()
        if not check_ws_token(token):
            await JSONResponse({"detail": "invalid or missing bearer token"}, status_code=401)(scope, receive, send)
            return
        await inner(scope, receive, send)

    return guarded


def mount_mcp(
    app: FastAPI,
    *,
    tool_name: str,
    capabilities: Capabilities,
    manager: JobManager,
    job_builders: dict[str, Callable[..., tuple[str, dict[str, Any]]]],
    knowledge_dir: Path | None = None,
    uri_scheme: str | None = None,
    path: str = "/mcp",
) -> MCPServer:
    """Mount an MCP endpoint on an existing compute-server app.

    Args:
        app: The app from `create_app()`. Its lifespan must run the returned
            server's `session_manager` — `create_app` does this when given an
            `mcp_server`, so instances rarely wire it by hand.
        tool_name: Identity, used in the MCP server name and default URI scheme.
        capabilities: The already-reconciled descriptor. Passed as a value, not
            a callable, precisely because the tool list is fixed at startup: an
            MCP client caches it, so it must not shift under them mid-session.
        manager: The same JobManager the REST routes use. One queue.
        job_builders: Operation name → callable returning `(code, variables)`
            for a job. An advertised operation with no builder gets no
            `submit_*` tool, since the server would not know how to run it.
        knowledge_dir: Pack root. Documents describing unadvertised operations
            are dropped.
        uri_scheme: Resource URI scheme, e.g. `mf` for `mf://workflows/eos`.
            Defaults to `tool_name`.
        path: Mount point on the parent app.

    Returns:
        The MCPServer, so the caller can run its session manager.
    """
    # Imported here, not at module scope: an instance that mounts no MCP facade
    # should not lose its REST routes to an import error in this dependency.
    # thermocalc runs under qemu-user emulation, where a broken wheel is a real
    # possibility, and it is REST-only.
    from mcp.server import MCPServer

    scheme = uri_scheme or tool_name
    operations = {op.name: op.backends for op in capabilities.operations}
    docs = reconcile(load_knowledge(knowledge_dir), operations) if knowledge_dir else []

    instructions = (
        f"{tool_name} runs {'; '.join(f'{op.name} ({", ".join(op.backends)})' for op in capabilities.operations)}. "
        "Jobs are asynchronous: submit_* returns a job id, then poll get_job and read get_result. "
        "Everything advertised here has been checked against this image at startup, so you can trust "
        "the list rather than a document. Planning is yours; this server executes and answers questions."
    )
    mcp = MCPServer(name=tool_name, version=capabilities.version, instructions=instructions)

    for operation in capabilities.operations:
        builder = job_builders.get(operation.name)
        if builder is None:
            continue
        _register_submit(mcp, operation=operation, builder=builder, manager=manager)

    _register_job_tools(mcp, manager=manager, tool_name=tool_name)
    _register_knowledge(mcp, docs=docs, scheme=scheme)

    mcp_app = mcp.streamable_http_app(streamable_http_path="/")
    app.mount(path, _bearer_guard(mcp_app))
    return mcp


def _register_submit(
    mcp: MCPServer,
    *,
    operation: Operation,
    builder: Callable[..., tuple[str, dict[str, Any]]],
    manager: JobManager,
) -> None:
    """Register one `submit_<operation>` tool that enqueues and returns immediately."""
    inputs = "\n".join(f"  - {name}: {desc}" for name, desc in operation.inputs.items())
    returns = "\n".join(f"  - {name}: {desc}" for name, desc in operation.returns.items())
    description = (
        f"{operation.description}\n\n"
        f"Backends available on this instance: {', '.join(operation.backends)}.\n"
        f"Inputs:\n{inputs}\n"
        f"On success get_result returns:\n{returns}\n\n"
        "Returns a job id immediately — this does not wait for the calculation."
    )

    async def submit(backend: str, arguments: dict[str, Any], correlation_id: str | None = None) -> dict[str, str]:
        if backend not in operation.backends:
            return {
                "error": f"backend {backend!r} does not serve {operation.name} on this instance",
                "available": ", ".join(operation.backends),
            }
        code, variables = builder(backend=backend, **arguments)
        job = await manager.submit(code, "run", variables, correlation_id, None)
        return {"job_id": job.id, "status": job.status.value}

    submit.__name__ = f"submit_{operation.name}"
    mcp.tool(name=f"submit_{operation.name}", description=description)(submit)


def _register_job_tools(mcp: MCPServer, *, manager: JobManager, tool_name: str) -> None:
    """Register the poll/read/cancel half of the async job pattern."""

    @mcp.tool(description="Current status of a submitted job: queued, running, succeeded, failed or cancelled.")
    async def get_job(job_id: str) -> dict[str, Any]:
        job = manager.get(job_id)
        if job is None:
            return {"error": "job not found", "job_id": job_id}
        return {"job_id": job.id, "status": job.status.value, "correlation_id": job.correlation_id}

    @mcp.tool(
        description=(
            "Final values, error and provenance for a finished job. "
            "Returns a not-finished marker rather than blocking if the job is still running."
        )
    )
    async def get_result(job_id: str) -> dict[str, Any]:
        job = manager.get(job_id)
        if job is None:
            return {"error": "job not found", "job_id": job_id}
        if job.status not in TERMINAL_STATUSES:
            return {"status": job.status.value, "finished": False}
        return {
            "status": job.status.value,
            "finished": True,
            "values": job.values,
            "error": job.error,
            "provenance": {"tool_name": tool_name, "wall_time_s": job.wall_time_s()},
        }

    @mcp.tool(
        description=(
            "Best-effort cancel. A queued job is dropped; a running job's result is discarded but its thread runs to completion."
        )
    )
    async def cancel_job(job_id: str) -> dict[str, Any]:
        ok = await manager.cancel(job_id)
        return {"job_id": job_id, "cancelled": ok} if ok else {"error": "job not found", "job_id": job_id}


def _body_reader(body: str, name: str) -> Callable[[], str]:
    """Build a zero-argument handler returning one document's text.

    The body is closed over rather than passed as a defaulted parameter: MCP
    rejects a handler that declares parameters against a URI with no `{...}`
    template variables, and a defaulted parameter still counts as declared.
    """

    def read() -> str:
        return body

    read.__name__ = name
    return read


def _register_knowledge(mcp: MCPServer, *, docs: list[KnowledgeDoc], scheme: str) -> None:
    """Serve the reconciled knowledge pack as resources, plus a lookup tool."""
    if not docs:
        return

    for doc in docs:
        uri = f"{scheme}://{doc.slug}"
        read = _body_reader(doc.body, f"read_{doc.slug.replace('/', '_').replace('-', '_')}")
        mcp.resource(uri, name=doc.title, description=doc.description, mime_type="text/markdown")(read)

    catalogue = "\n".join(f"- {scheme}://{d.slug} — {d.title}: {d.description}" for d in docs)

    @mcp.tool(
        description=(
            "Find knowledge documents for this tool by keyword — workflows, preconditions, refusal "
            "rules and how to read a result. Returns resource URIs to read.\n\n"
            f"Available documents:\n{catalogue}"
        )
    )
    async def search_workflows(query: str, limit: int = 5) -> dict[str, Any]:
        hits = search(docs, query, limit)
        return {
            "query": query,
            "results": [{"uri": f"{scheme}://{d.slug}", "title": d.title, "description": d.description} for d in hits],
        }

    for doc in docs:
        if not doc.slug.startswith("prompts/"):
            continue
        name = doc.slug.split("/", 1)[1]
        render = _body_reader(doc.body, name.replace("-", "_"))
        mcp.prompt(name=name, description=doc.description)(render)
