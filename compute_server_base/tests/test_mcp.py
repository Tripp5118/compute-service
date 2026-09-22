r"""Coverage for the MCP facade and startup knowledge reconciliation (S-2b).

The point of S-2b is that an advertisement can be trusted flatly: if an
operation is listed, this image can run it, and if a knowledge document is
readable, it describes work this image can do. The check the plan asks for is
"remove a calculator and confirm the operation disappears from both
/capabilities and the MCP tool list" — that is `test_missing_backend_*` below,
with a backend whose probe module does not exist standing in for one uninstalled
from an image.

The MCPServer is driven directly rather than over the streamable-HTTP transport:
these assertions are about what got registered, and the transport is the SDK's
to test. One HTTP case covers the mount itself.

Run:
    cd compute_server_base && PYTHONPATH=. uv run --with fastapi --with httpx --with pyjwt --with mcp --with pytest pytest tests -v
"""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING, Any

import pytest
from fastapi.testclient import TestClient

if TYPE_CHECKING:
    from pathlib import Path

os.environ.setdefault("COMPUTE_SERVER_TOKEN", "test-token-for-pytest")

from compute_server_base import (  # noqa: E402 — env var must be set before import
    Capabilities,
    Host,
    Operation,
    available_backends,
    create_app,
    mount_mcp,
)

TOKEN = os.environ["COMPUTE_SERVER_TOKEN"]
AUTH = {"Authorization": f"Bearer {TOKEN}"}

# "json" stands in for an installed calculator, "no_such_module_at_all" for one
# left out of the image. available_backends probes both for real.
REGISTRY = [
    {"id": "installed", "probe_module": "json", "operations": ["echo"]},
    {"id": "absent", "probe_module": "no_such_module_at_all", "operations": ["vanished", "echo"]},
]

OPERATION_SPECS = [
    {
        "name": "echo",
        "inputs": {"value": "A number to double."},
        "returns": {"doubled": "Twice the input."},
        "description": "Double a number.",
    },
    {
        "name": "vanished",
        "inputs": {"value": "Unused."},
        "returns": {"never": "This operation has no working backend."},
        "description": "Should never be advertised.",
    },
]

DOCS = {
    "general.md": ("---\ntitle: General guidance\ndescription: Applies whatever is installed.\n---\n\nAlways relax first.\n"),
    "workflows/echo.md": (
        "---\ntitle: Echoing\ndescription: How to double a number.\nrequires_operation: echo\n---\n\nDoubling doubles.\n"
    ),
    "workflows/vanished.md": (
        "---\ntitle: Vanished workflow\ndescription: Needs a backend this image lacks.\n"
        "requires_operation: vanished\n---\n\nUnreachable.\n"
    ),
    "prompts/get-started.md": (
        "---\ntitle: Get started\ndescription: Usage guidance for a fresh agent.\n---\n\nRead the workflows first.\n"
    ),
}


def _echo_builder(*, value: int, **_: Any) -> tuple[str, dict[str, Any]]:
    return "def run(value):\n    return {'doubled': value * 2}\n", {"value": value}


def _capabilities() -> Capabilities:
    operations = [
        Operation(
            name=spec["name"],
            backends=available_backends(REGISTRY, spec["name"]),
            inputs=spec["inputs"],
            returns=spec["returns"],
            description=spec["description"],
        )
        for spec in OPERATION_SPECS
    ]
    operations = [op for op in operations if op.backends]
    return Capabilities(
        tool="stub",
        version="0.0",
        contract_version="1.0",
        ready=bool(operations),
        host=Host.native(),
        operations=operations,
    )


@pytest.fixture(scope="module")
def knowledge_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A pack on disk: one universal document, two operation-scoped, one prompt."""
    root = tmp_path_factory.mktemp("knowledge")
    for name, text in DOCS.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return root


@pytest.fixture
def wired(knowledge_dir: Path) -> tuple[Any, Any]:
    """The app and its MCPServer, wired exactly as an instance wires them.

    Per test, not per module: the streamable-HTTP session manager refuses a
    second `run()`, so an app's lifespan can only be entered once — which is
    fine for a server process and not for a shared fixture.
    """
    holder: dict[str, Any] = {}

    def _mount(app: Any) -> Any:
        server = mount_mcp(
            app,
            tool_name="stub",
            capabilities=_capabilities(),
            manager=app.state.jobs,
            job_builders={"echo": _echo_builder, "vanished": _echo_builder},
            knowledge_dir=knowledge_dir,
            uri_scheme="stub",
        )
        holder["server"] = server
        return server

    app = create_app(tool_name="stub", capabilities=_capabilities, mcp_mount=_mount)
    return app, holder["server"]


def _run(app: Any, coro_factory: Any) -> Any:
    """Drive a coroutine with the app's lifespan up, in one event loop.

    The JobManager binds its queue to the running loop at start(), so the
    coroutine under test has to share the loop the lifespan started on — which
    rules out TestClient's separate portal thread for the MCP calls.
    """

    async def main() -> Any:
        async with app.router.lifespan_context(app):
            return await coro_factory()

    return asyncio.run(main())


def test_missing_backend_hides_operation_from_capabilities(wired: tuple[Any, Any]) -> None:
    """Half of the S-2b check: the REST advertisement follows the import probe."""
    app, _ = wired
    with TestClient(app) as client:
        payload = client.get("/capabilities", headers=AUTH).json()

    names = [op["name"] for op in payload["operations"]]
    assert "echo" in names
    assert "vanished" not in names, "an operation with no importable backend must not be advertised"
    assert payload["operations"][names.index("echo")]["backends"] == ["installed"]


def test_missing_backend_hides_operation_from_mcp_tool_list(wired: tuple[Any, Any]) -> None:
    """The other half: the two faces cannot disagree, because one derives from the other."""
    app, server = wired
    tools = {tool.name for tool in _run(app, server.list_tools)}

    assert "submit_echo" in tools
    assert "submit_vanished" not in tools, "the MCP list must not advertise what /capabilities won't"
    # The async job pattern: submit returns an id, these read it. Nothing blocks.
    assert {"get_job", "get_result", "cancel_job", "search_workflows"} <= tools


def test_submit_tool_describes_its_inputs_and_backends(wired: tuple[Any, Any]) -> None:
    """The operation's prose reaches the tool description, which is what an agent reads."""
    app, server = wired
    tools = {tool.name: tool for tool in _run(app, server.list_tools)}
    description = tools["submit_echo"].description or ""

    # Prose is the interface here — an agent picks a backend and arguments from
    # this text, so the operation's own words have to reach it.
    assert "Double a number." in description
    assert "installed" in description
    assert "value" in description
    assert "does not wait" in description


def test_submit_poll_and_read_over_mcp(wired: tuple[Any, Any]) -> None:
    """The async job pattern end to end on the same queue the REST routes use."""
    app, server = wired

    async def flow() -> dict[str, Any]:
        submitted = await server.call_tool("submit_echo", {"backend": "installed", "arguments": {"value": 21}})
        job_id = submitted.structured_content["job_id"]

        for _ in range(300):
            status = await server.call_tool("get_job", {"job_id": job_id})
            if status.structured_content["status"] in ("succeeded", "failed", "cancelled"):
                break
            await asyncio.sleep(0.1)

        result = await server.call_tool("get_result", {"job_id": job_id})
        return result.structured_content

    payload = _run(app, flow)
    assert payload["finished"] is True
    assert payload["status"] == "succeeded"
    assert payload["values"] == {"doubled": 42}
    assert payload["provenance"]["tool_name"] == "stub"


def test_submit_refuses_a_backend_that_does_not_serve_the_operation(wired: tuple[Any, Any]) -> None:
    """Refused before it becomes a job, and refused as a value an agent can branch on."""
    app, server = wired

    async def flow() -> dict[str, Any]:
        result = await server.call_tool("submit_echo", {"backend": "absent", "arguments": {"value": 1}})
        return result.structured_content

    payload = _run(app, flow)
    assert payload["refused"] is True
    assert payload["reason"] == "backend_not_served"
    assert "absent" in payload["detail"]
    # The context is what makes it actionable: an agent retries with one of these.
    assert payload["context"]["available"] == ["installed"]


def test_arbitrary_code_can_be_submitted_over_mcp(wired: tuple[Any, Any]) -> None:
    """S-16. Named operations are the convenience path; this is the one the servers exist for."""
    app, server = wired

    async def flow() -> dict[str, Any]:
        submitted = await server.call_tool(
            "submit_code",
            {
                "code": "def measure(a, b):\n    return {'sum': a + b}\n",
                "entrypoint": "measure",
                "variables": {"a": 2, "b": 5},
            },
        )
        job_id = submitted.structured_content["job_id"]
        for _ in range(300):
            status = await server.call_tool("get_job", {"job_id": job_id})
            if status.structured_content["status"] in ("succeeded", "failed", "cancelled"):
                break
            await asyncio.sleep(0.1)
        result = await server.call_tool("get_result", {"job_id": job_id})
        return result.structured_content

    payload = _run(app, flow)
    assert payload["status"] == "succeeded"
    # Not an operation in the registry, and no builder — the point of S-16.
    assert payload["values"] == {"sum": 7}


def test_capability_descriptor_is_readable_over_mcp(wired: tuple[Any, Any]) -> None:
    """S-17. An agent has to see the host facts, not only the tool list generated from them."""
    app, server = wired

    async def flow() -> dict[str, Any]:
        result = await server.call_tool("get_capabilities", {})
        return result.structured_content

    payload = _run(app, flow)
    over_rest = _capabilities().model_dump(mode="json")

    # The two faces must not be able to disagree about what this host is: the
    # MCP read and the REST route serve the same descriptor object.
    assert payload["host"] == over_rest["host"]
    assert payload["contract_version"] == over_rest["contract_version"]
    assert payload["version"] == over_rest["version"]
    # under_emulation is the field the whole item exists for: it decides whether
    # a number off this instance gets validated before use.
    assert "under_emulation" in payload["host"]


def _not_ready() -> Capabilities:
    """An instance whose compute is absent — thermocalc with no engine bind-mounted."""
    return Capabilities(
        tool="stub",
        version="0.0",
        contract_version="1.0",
        ready=False,
        host=Host(container_arch="x86_64", solver_arch=None, arch_match=None),
        operations=[],
    )


def _not_ready_app(knowledge_dir: Path) -> tuple[Any, Any]:
    """A wired not-ready instance. Built per call: one session manager, one run."""
    holder: dict[str, Any] = {}

    def _mount(app: Any) -> Any:
        holder["server"] = mount_mcp(
            app,
            tool_name="stub",
            capabilities=_not_ready(),
            manager=app.state.jobs,
            job_builders={},
            knowledge_dir=knowledge_dir,
            uri_scheme="stub",
        )
        return holder["server"]

    app = create_app(tool_name="stub", capabilities=_not_ready, mcp_mount=_mount)
    return app, holder["server"]


def test_a_not_ready_instance_refuses_on_both_faces(knowledge_dir: Path) -> None:
    """S-18. The same body over HTTP and over MCP, so a client reads one shape.

    Two apps, not one: the streamable-HTTP session manager can only be run
    once, so a TestClient and a driven MCPServer cannot share an instance.
    """
    rest_app, _ = _not_ready_app(knowledge_dir)
    with TestClient(rest_app) as client:
        response = client.post("/jobs", json={"code": "def run():\n    return 1\n"}, headers=AUTH)
    assert response.status_code == 422, "a refusal is not a 500 — 'will not' and 'broke' are different outcomes"
    over_rest = response.json()

    mcp_app, server = _not_ready_app(knowledge_dir)

    async def flow() -> dict[str, Any]:
        result = await server.call_tool("submit_code", {"code": "def run():\n    return 1\n"})
        return result.structured_content

    over_mcp = _run(mcp_app, flow)

    assert over_rest == over_mcp, "one refusal shape, whichever face the caller is on"
    assert over_rest["refused"] is True
    assert over_rest["reason"] == "instance_not_ready"


def test_knowledge_is_reconciled_against_live_operations(wired: tuple[Any, Any]) -> None:
    """A readable document describes work this image can do — invariant 7, applied to prose."""
    app, server = wired
    uris = {str(resource.uri) for resource in _run(app, server.list_resources)}

    assert "stub://workflows/echo" in uris
    assert "stub://general" in uris, "a document with no required operation is always served"
    assert "stub://workflows/vanished" not in uris, "a document describing an unavailable operation must not be readable"


def test_knowledge_body_is_readable_and_searchable(wired: tuple[Any, Any]) -> None:
    """Guards the closure that carries a document's body — a defaulted parameter registers nothing."""
    app, server = wired

    async def flow() -> tuple[str, dict[str, Any]]:
        contents = list(await server.read_resource("stub://workflows/echo"))
        found = await server.call_tool("search_workflows", {"query": "doubling"})
        return contents[0].content, found.structured_content

    body, found = _run(app, flow)
    assert "Doubling doubles." in body
    assert [hit["uri"] for hit in found["results"]] == ["stub://workflows/echo"]


def test_prompts_come_from_the_pack(wired: tuple[Any, Any]) -> None:
    """Only `prompts/` documents become MCP prompts; the rest stay resources."""
    app, server = wired
    names = {prompt.name for prompt in _run(app, server.list_prompts)}
    assert names == {"get-started"}


def test_mcp_mount_requires_the_bearer_token(wired: tuple[Any, Any]) -> None:
    """One credential for both faces: the guard runs before the session manager sees anything."""
    app, _ = wired
    with TestClient(app) as client:
        assert client.post("/mcp/", json={}).status_code == 401
        assert client.post("/mcp/", json={}, headers={"Authorization": "Bearer wrong"}).status_code == 401
