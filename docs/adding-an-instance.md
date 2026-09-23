# Adding a brand-new tool server

Moved here from `spark-dashboard/AGENTS.md` on 2026-09-23, when the tool servers
were split into their own repositories. Building an instance is this repository's
subject; registering and credentialling one is still the dashboard's.

`docs/execution-servers.md` says what a server has to do and why before you decide
how yours does it. `spark-tool-lammps` is the worked example — read it alongside
this. What you write, in a new repository of its own:

1. `Dockerfile` — your base image, your toolchain, and an install of
   `compute_server_base` from this repository.
2. `compose.yml` — copy `spark-tool-materials-framework`'s. Bind `127.0.0.1`. Join
   both the project default network and `spark-network`, so the dashboard can probe
   `/capabilities`.
3. `server/main.py` — a `Capabilities` descriptor listing your operations, and
   startup probing that says whether the tool is actually ready.
4. `knowledge/` — markdown documents describing the workflows, one per operation.
   Optional, but this is what an agent reads to use the tool well.
5. Optionally, a submission route of your own. The inherited `POST /jobs` takes
   Python source; if your tool's natural input is something else, add a route that
   builds the job for the caller. `POST /run` on lammps takes an input script and
   its files and is four lines of `manager.submit()`.

What you get for free: the job queue, bearer auth on both credential paths,
`/health`, `/capabilities`, `POST /jobs`, job polling, results, cancel, log
streaming, and the MCP facade.

Two rules that are not optional:

- **Advertise only what works.** Operations are filtered by live import probe.
  Never list an operation from a static table. Every consumer's right to trust
  `/capabilities` flatly depends on this holding.
- **Keep `compute_server_base` importable on Python 3.10.** One instance image is
  pinned there.

Then register it with the dashboard and run its `scripts/start_tool.sh` — see
`spark-dashboard/AGENTS.md` for both.
