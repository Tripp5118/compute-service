# Execution servers — what they are for and what they have to do

This is the requirements document for the compute servers: the goal they serve,
the surface every instance has to expose, what each one exposes today, and what is
missing. Read it before changing `compute_server_base/`,
`compute_interface/`, or any tool server built on them.

The three instances are `spark-tool-materials-framework`, `spark-tool-lammps` and
`spark-tool-thermocalc`, each its own repository.

## The goal

A trusted client sends code or a script to a machine that has a solver installed on
it. The server runs it and gives back everything it produced: the values the code
returned, the files it wrote, and its output while it is still running.

Most of those clients are agents, not people. So an agent that has never seen the
machine has to be able to find out what the machine can do, decide whether the job it
has in mind is possible there, and be told no in a form it can act on when it is not.

That is the whole of it. Everything below is in service of those two paragraphs.

Two things follow, and they are the reason the design looks the way it does.

**Arbitrary code is the point, not an escape hatch.** A fixed menu of named
calculations is a calculator behind HTTP. The client writes the calculation. A named
operation is a convenience over the same execution path, not a replacement for it, and
a client that can only call named operations cannot do the work these servers exist
for.

**The server reports; it does not decide.** It says what it can run, how long a job
took, on what host, under what conditions. It does not hold a cost model, a budget, a
schedule, or an opinion about which calculation is worth doing. Those belong to the
client.

## Two faces on one process

One process, one job queue, one credential. `compute_server_base/app.py` serves REST;
`compute_server_base/mcp_facade.py` mounts MCP at `/mcp` on the same app behind the
same bearer check.

**REST is for execution.** A running calculation makes many calls and should not carry
an MCP client to do it. `matflow`'s relax node and the `campaign.py` autoBO's agent
writes are both REST callers.

**MCP is for finding out what the server can do and for an agent to drive it.** An
agent attaches, reads the operations and the knowledge pack, submits work, polls, reads
files. This is where a tool's own documentation belongs, because it versions with the
image that implements it.

The split is temporal, not a preference: nothing reaches into a calculation while the
calculation is running.

## What every instance must expose

### Execution, over REST

| Route | Requirement |
|---|---|
| `POST /jobs` | Arbitrary code plus an entrypoint name and its arguments. Returns a job id immediately. |
| `GET /jobs/{id}` | Status: queued, running, succeeded, failed, cancelled. |
| `GET /jobs/{id}/result` | Return values, the error and its traceback if there was one, the manifest of files written, wall time, and which tool ran it. |
| `GET /jobs/{id}/files` | What the job wrote — path, size, mtime. No contents. |
| `GET /jobs/{id}/files/{path}` | One file out of the workspace. Paths resolved and refused outside it. |
| `GET /jobs/{id}/archive` | The whole workspace as one tar.gz. |
| `DELETE /jobs/{id}/files` | Reclaim the workspace. Nothing else does. |
| `POST /jobs/{id}/cancel` | A queued job is dropped; a running one has its process group killed. So does `timeout_s`. |
| `WS /jobs/{id}/stream` | Log history replayed, then live output until the job finishes. |
| `GET /jobs` | Every job this process knows about, plus `worker_alive` and the queued/running counts. One worker runs one job at a time, so a job that cannot finish holds every job behind it — this is where that shows. |
| `GET /capabilities` | Identity, version, contract version, host facts, and the operations this instance can serve right now. |
| `GET /health` | Unauthenticated liveness. |

Every job runs in its own directory with the process cwd set to it, so code that names
its output files relatively writes them where they can be fetched from.

### Discovery and agent use, over MCP at `/mcp`

| Requirement | Why |
|---|---|
| Submit arbitrary code | Same reason as `POST /jobs`. An agent that can only call named operations cannot write its own calculation. |
| One submit tool per advertised operation | The convenience path. Generated from `Capabilities` after the backend probe, so it cannot advertise something the image cannot run. |
| Read the capability descriptor | An agent has to be able to see the host facts and the version, not just the tool list generated from them. Emulation and architecture mismatch change whether a number is trustworthy. |
| Poll, read result, cancel | The async pattern. An MCP call is request/response and a calculation outlives one. |
| List and read files from the workspace | For tools whose real output is files. Bulk retrieval stays on HTTP. |
| The knowledge pack as resources, with keyword lookup | The tool's own documentation, shipped in the image so it versions with the code it describes. |
| Refuse in a form an agent can act on | "This instance cannot run this" is a normal response. A traceback is not. |

## What each instance has today

All three inherit the full REST surface above from `create_app()`, including the
workspace, the file routes and the WebSocket stream. That half is identical
everywhere. They also all inherit `submit_code`, `get_capabilities` and the refusal
shape, so the MCP column below means "has a facade mounted", not "has only the named
operations".

| | Operations | Knowledge docs | `/capabilities` | `/mcp` | Deprecated shims |
|---|---|---|---|---|---|
| materials-framework | 18 | 26 | yes | yes | `/manifest`, `/mlips` still served |
| lammps | 2 — `run_input_script`, `check_input_script` | 9 | yes | yes | none, and never had them |
| thermocalc | 1 — `run_tc_python` | 3 | yes | yes | `/manifest` still served |

Job shape differs by instance and that is deliberate. materials-framework and
thermocalc take Python source. LAMMPS takes an input script, because a LAMMPS caller
has a script rather than Python and should not have to know the server executes one by
importing a runner — `POST /run` wraps it and submits it through the same queue.

## What is still open

Everything the requirements above describe is built. One thing is not finished, and it
is not this repository's code.

**The deprecated shims are served unevenly.** materials-framework and thermocalc still
serve `/manifest`; materials-framework also serves `/mlips`. LAMMPS was built after
those were deprecated and serves neither, so a consumer that asks a LAMMPS server what
it can do through `/manifest` gets a 404 and reaches none of its knowledge documents.

They stay until matflow is repointed. matflow's `MaterialsFrameworkRelaxNode` calls
`/mlips` and matflow is paused, so nobody is watching it to catch a break. The
replacement on the consumer side is `compute_interface` and `GET /capabilities`.

## Standing constraints

- **MCP is additive.** REST callers do not migrate. matflow's relax node and autoBO's
  generated `campaign.py` stay on REST.
- **Jobs stay async on both faces.** Nothing blocks a request for the length of a
  calculation.
- **No experimental decisions in a server.** `search_workflows` is a lookup over the
  knowledge pack. A server that chooses what calculation to run is the wrong split.
- **The knowledge pack ships in the image** and is reconciled against what is actually
  installed at startup, so an instance cannot advertise a workflow whose calculator
  will not import.
- **A job runs in a child process, and stopping it means killing that process group.**
  In a thread pool thread, `timeout_s` and `cancel` cannot stop anything: one job ran
  19 hours past its timeout holding 19 cores and 44 GB while the server reported it as
  failed. A thread cannot be interrupted; a process can be killed. The process group
  rather than the process, because job code spawns its own subprocesses — LAMMPS runs
  `lmp` under `Popen`. The child also sets its own cwd, so there is no process-global
  chdir for a second job to race.
- **Nothing the worker waits for may be unbounded.** Reading a job's output waits for
  EOF, and EOF on a pipe needs every writer closed — including a process the job started
  and never waited on, which inherits stdout and holds the write end. That waits
  forever, and since one worker consumes the whole queue, every job submitted afterwards
  stays queued. The group is killed on every path out of a job, and the read after it is
  bounded. Adding a wait to the worker without a bound puts the queue back there.

**Not in scope here, deliberately.** No cost model, no price, no budget, no schedule,
no queue policy. An instrument reports what a job took and what host it ran on. The
cost a later paper needs is a value a user assigns to a matflow sequence standing for a
step in an SDL digital twin; it is not a resource measurement and no execution server
can produce it. Queue occupancy for scheduling those nodes is that system's queue, not
this one's.
