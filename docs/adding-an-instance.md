# Adding a brand-new tool server

Building an instance is this repository's subject; registering and credentialling
one is the dashboard's.

**Start from `spark-tool-template`** — create your repository from it with GitHub's
"Use this template", rather than copying an existing tool repository by hand. It is
a working compute server with no solver, so it builds, starts and runs a job before
you change anything. `spark-tool-lammps` remains a worked example of a finished one,
and `docs/execution-servers.md` says what a server must do and why.

What you then change:

1. `server/main.py` — set `TOOL_NAME`. That is usually all. The API is job CRUD and
   comes from `compute_server_base`; your instance does not describe or wrap the
   library it hosts.
2. `requirements.txt` — your solver, pinned, with a comment saying why that pin.
3. `Dockerfile` — system packages your solver needs, and the release tag build
   argument that stamps the image.
4. `compose.yml` — your service name, an unused port bound to `127.0.0.1`, and
   resource ceilings. Join both the project default network and `spark-network`.
5. `agent.md` and `knowledge/` — the agent definition for your tool and the
   knowledge base it points at. **This is where the work is.** It is the only place
   that says what your tool can compute and how to write job code against it. These
   stay in the repository and are not copied into the image: a consumer fetches them
   at a release tag and builds the tool-use agent on its own side, so a wrong
   instruction is fixed by a commit rather than a rebuild and a redeploy.
6. Optionally, a route for your tool's own **input format** — not for a call into
   its library. `POST /run` on lammps takes an input script, because a script is
   LAMMPS's native input rather than a function signature upstream can rename.

What you get for free: the job queue, bearer auth on both credential paths,
`/health`, `POST /jobs`, job polling, results, files, archive, cancel and log
streaming.

Three rules that are not optional:

- **Never add an endpoint that names a call into your tool.** No `submit_relax`, no
  `POST /operations/{name}`, no operations list, no `inputs`/`returns` descriptors.
  The moment the API encodes a library's call signature, an upstream rename makes
  the *server* wrong and forces a rebuild to fix. The consumer sends Python; the
  Python calls the library.
- **Say what the tool can do in `agent.md`, never over the wire.** A running
  container answers what a job did, not what the tool can do.
- **Keep `compute_server_base` importable on Python 3.10.** One instance image is
  pinned there.

Then register it with the dashboard and run its `scripts/start_tool.sh` — see
`spark-dashboard/AGENTS.md` for both.
