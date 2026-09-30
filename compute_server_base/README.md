# compute-server-base

The shared half of every compute-server instance: the job queue, bearer auth,
and `create_app()`, which mounts every route an instance does not define for
itself.

**The API is CRUD for jobs.** It takes Python source, an entrypoint and its
arguments; it queues, runs, reports and returns. It never describes or wraps the
library the image installs — no per-operation route, no operations list, no
call-shape descriptors. What the tool can compute is `agent.md`'s job, in the
instance's own repository.

Not published and not installed standalone — each tool image copies this
directory in and `pip install`s it (see any `tools/*/Dockerfile`).

## Writing an instance

```python
from compute_server_base import create_app

app = create_app(tool_name="my-tool")
```

That is the whole of it. `create_app()` owns `/health`, `POST /jobs`,
`GET /jobs`, `GET /jobs/{id}`, `GET /jobs/{id}/result`, the file and archive
routes, `POST /jobs/{id}/cancel`, and the `/jobs/{id}/stream` websocket.

An instance may add a route for its own **input format** — LAMMPS's `POST /run`
takes an input script, which is its native input rather than a function
signature upstream can rename. It must not add a route that names a call into
the library: the moment the API encodes a call signature, an upstream rename
makes the server wrong and forces a rebuild to fix it.

## Python version

Kept 3.10-compatible because the thermocalc image is pinned there for
TC-Python. No `StrEnum`, no `match`.

## Tests

```bash
cd compute_server_base
PYTHONPATH=. uv run --with fastapi --with httpx --with pyjwt --with pytest pytest tests -v
```

Run them from this directory, not the repository root. A job executes in a child
process, and that child resolves `compute_server_base` from its own path — from
the root the parent imports fine, the child does not, and the failure surfaces
as an empty job workspace rather than an import error.

These exercise the shared path with plain-Python jobs, so they need neither
toolchain installed.
