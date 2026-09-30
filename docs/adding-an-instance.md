# Adding a brand-new tool server

Building an instance is this repository's subject; registering and credentialling
one is the dashboard's.

**Start from `spark-tool-template`** — create your repository from it with GitHub's
"Use this template", rather than copying an existing tool repository by hand. It is
a working compute server with no solver, so it builds, starts and runs a job before
you change anything. `spark-tool-lammps` remains a worked example of a finished one,
and `docs/execution-servers.md` says what a server must do and why.

What you then change:

1. `server/main.py` — set `TOOL_NAME`, set the import that decides readiness, and
   replace `OPERATIONS` with what your tool can do.
2. `requirements.txt` — your solver, pinned, with a comment saying why that pin.
3. `Dockerfile` — system packages your solver needs. Keep the `SOURCE_REF` build
   argument; it stamps the image with the commit it was built from.
4. `compose.yml` — your service name, an unused port bound to `127.0.0.1`, and
   resource ceilings. Join both the project default network and `spark-network`, so
   the dashboard can probe `/capabilities`.
5. `agent.md` and `knowledge/` — the agent definition for your tool and the
   knowledge base it points at. These stay in the repository and are **not** copied
   into the image: a consumer fetches them from git at a commit and builds the
   tool-use agent on its own side, so a wrong instruction is fixed by a commit
   rather than a rebuild and a redeploy.
6. Optionally, a submission route of your own. The inherited `POST /jobs` takes
   Python source; if your tool's natural input is something else, add a route that
   builds the job for the caller. `POST /run` on lammps takes an input script and
   its files and is four lines of `manager.submit()`.

What you get for free: the job queue, bearer auth on both credential paths,
`/health`, `/capabilities`, `POST /jobs`, job polling, results, files, cancel and
log streaming.

Two rules that are not optional:

- **Advertise only what works.** Operations are filtered by live import probe.
  Never list an operation from a static table. Every consumer's right to trust
  `/capabilities` flatly depends on this holding.
- **Keep `compute_server_base` importable on Python 3.10.** One instance image is
  pinned there.

Then register it with the dashboard and run its `scripts/start_tool.sh` — see
`spark-dashboard/AGENTS.md` for both.
