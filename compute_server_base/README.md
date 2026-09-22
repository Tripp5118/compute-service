# compute-server-base

The shared half of every compute-server instance in `tools/`: the job queue,
bearer auth, the `/capabilities` contract, and `create_app()`, which mounts
every route an instance does not define for itself.

Not published and not installed standalone — each tool image copies this
directory in and `pip install`s it (see any `tools/*/Dockerfile`).

## Writing an instance

```python
from compute_server_base import Capabilities, Host, Operation, create_app

def _capabilities() -> Capabilities:
    return Capabilities(
        tool="my-tool",
        version="1.0",
        contract_version="1.0",
        ready=True,
        host=Host.native(),
        operations=[Operation(name="relax", backends=["orb"], description="...")],
    )

app = create_app(tool_name="my-tool", capabilities=_capabilities)
```

`create_app()` owns `/health`, `/capabilities`, `POST /jobs`,
`GET /jobs/{id}`, `GET /jobs/{id}/result`, `POST /jobs/{id}/cancel`, and the
`/jobs/{id}/stream` websocket. Add instance-specific routes to the returned
app.

`capabilities` is called per request, so report probe results rather than a
fixed claim — `probe_import()` and `available_backends()` are here for that.
An operation should list only backends that actually import in this image.

## Python version

Kept 3.10-compatible because the thermocalc image is pinned there for
TC-Python. No `StrEnum`, no `match`.

## Tests

```bash
cd compute_server_base
PYTHONPATH=. uv run --with fastapi --with httpx --with pyjwt --with mcp --with pytest pytest tests -v
```

Run them from this directory, not the repository root. A job executes in a child
process, and that child resolves `compute_server_base` from its own path — from
the root the parent imports fine, the child does not, and the failure surfaces
as an empty job workspace rather than an import error.

These exercise the shared path with plain-Python jobs, so they need neither
toolchain installed.
