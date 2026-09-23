# Execution servers — what they are for and what they have to do

Written 2026-08-26. This is the requirements document for the compute servers: the
goal these servers serve, the surface every instance has to expose, what each one
exposes today, and what is missing. Read it before changing `compute_server_base/`,
`compute_interface/`, or any tool server built on them.

Moved here from `spark-dashboard/docs/execution-servers.md` on 2026-09-23, when the
tool servers were split into their own repositories: `spark-tool-materials-framework`,
`spark-tool-lammps`, `spark-tool-thermocalc`. Paths below that used to read
`tools/<name>/` are now that tool's repository root.

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

## What each instance has today, measured 2026-08-26

All three inherit the full REST surface above from `create_app()`, including the
workspace, the file routes and the WebSocket stream. That half is done and identical
everywhere. Since 2026-08-26 they also all inherit `submit_code`, `get_capabilities` and
the refusal shape, so the MCP column below means "has a facade mounted", not "has only
the named operations".

| | Operations | Knowledge docs | `/capabilities` | `/mcp` | Deprecated shims | Live output verified |
|---|---|---|---|---|---|---|
| materials-framework | 5 — `relax`, `equation_of_state`, `cubic_elastic_constants`, `phonons`, `predict_formation_energy` | 13 | yes | yes | `/manifest`, `/mlips` still served | anything the submitted Python prints |
| lammps | 2 — `run_input_script`, `check_input_script` | 9 | yes | yes | none, and never had them | yes — `lmp` runs under `Popen` and each stdout line is printed as it arrives (`server/runner.py:189`) |
| thermocalc | 1 — `run_tc_python` | 3 | yes | yes | `/manifest` still served | anything the submitted Python prints |

Job shape differs by instance and that is deliberate. materials-framework and
thermocalc take Python source. LAMMPS takes an input script, because a LAMMPS caller
has a script rather than Python and should not have to know the server executes one by
importing a runner — `POST /run` wraps it and submits it through the same queue.

## What is missing

Five things, each with what it breaks. **Items 1, 2 and 3 were built on 2026-08-26**;
their statements are kept below with what landed, because the reasoning is the record of
why the surface has the shape it has.

### 1. Arbitrary code cannot be submitted over MCP — built 2026-08-26

`submit_code` in `mcp_facade.py` takes the same `code`, `entrypoint`, `variables`,
`correlation_id` and `timeout_s` that `POST /jobs` takes, and calls the same
`manager.submit()`. Every instance has it. The original statement follows.


`POST /jobs` accepts `code` and `entrypoint`. The MCP facade registers one
`submit_<operation>` per entry in the instance's `JOB_BUILDERS` dict and nothing else,
so an agent attached over MCP can call the five named materials-framework operations
and cannot write a calculation of its own.

**What it breaks:** the primary use case, for every agent client. materials-framework
has five operations against a Python environment that has ASE, pymatgen and five MLIP
packages in it, and an agent on MCP can reach the five.

**What replaces it:** a submit tool on the MCP facade that takes source and an
entrypoint, mirroring `POST /jobs`. It belongs in `mcp_facade.py`, once, for all three
instances.

### 2. The capability descriptor is not readable over MCP — built 2026-08-26

A `get_capabilities` tool, rather than a resource: an agent lists tools on connect, and
MCP clients vary in whether they read resources at all. The original statement follows.


`mount_mcp()` takes `capabilities` and uses it to generate the tool list at startup.
Nothing returns the descriptor itself, so an agent on MCP cannot read
`host.under_emulation`, `host.arch_match`, `host.solver_arch`, `version` or
`contract_version`.

**What it breaks:** trusting a number. Thermo-Calc's engine is x86-64 and runs under
qemu on an ARM host; the guideline for that case is that a result produced under
emulation is validated before it is used. An agent that cannot read the flag cannot
apply the rule.

**What replaces it:** a `get_capabilities` tool, or the descriptor as an MCP resource,
in `mcp_facade.py`.

### 3. Refusal is a document, not a response — built 2026-08-26

`compute_server_base/refusal.py`. A `Refusal` body — `refused`, `reason`, `detail`,
`context` — with a closed four-value `RefusalReason`: `instance_not_ready`,
`operation_not_served`, `backend_not_served`, `architecture_mismatch`. MCP tools return
it; HTTP routes raise `Refused` and a handler in `create_app()` returns the identical
body as a 422.

The fifth reason this section proposed, "precondition not met", was left out on purpose.
A precondition is a scientific judgment, and a server that decides those is deciding
which calculations are worth doing — the thing this document exists to prevent. Every
reason in the set is something the server can check by looking. Preconditions stay in the
knowledge packs, where an agent reads them and decides for itself.

The original statement follows.


`materials-framework/knowledge/rules/refusals.md` and
`lammps/knowledge/rules/refusals.md` describe what the instance should decline. They
are documents an agent may or may not have read. On the wire, an impossible job either
400s on a backend name or runs and fails.

**What it breaks:** an agent cannot distinguish "this instance will not do this, and
here is why" from "this broke." The two call for different next actions.

**What replaces it:** a refusal response shape on both faces, carrying the reason in a
fixed set — wrong architecture, calculator not installed, operation not served,
precondition not met — plus the free text. `mcp_facade.py`'s submit path returns a
dict already, so the MCP half is a shape decision rather than new plumbing.

### 4. The deprecated shims are served unevenly, and autoBO calls them

materials-framework and thermocalc still serve `/manifest`; materials-framework also
serves `/mlips`. LAMMPS was built after those were deprecated and serves neither.
autoBO's `src/autobo/truth/potential_runner.py` calls both, and
`autoBO/guidelines/agent/compute-server.md` documents `runner.manifest()` and
`runner.mlips()` as how to check what a tool can do — so every campaign autoBO's agent
writes reads capability through them.

**What it breaks:** autoBO can submit jobs to LAMMPS, because `POST /jobs` exists, and
gets a 404 when it asks what LAMMPS can do. The operation-keyed contract and all 9
LAMMPS knowledge documents are unreachable from autoBO.

**What replaces it:** autoBO moves to `GET /capabilities`, on the client side. The
shims stay until matflow is repointed — matflow's `MaterialsFrameworkRelaxNode` calls
`/mlips` and matflow is paused, so nobody is watching it to catch a break.

### 5. There is no client package — built 2026-08-26

`compute_interface/`. Synchronous; owns submit, poll, wait, result, cancel, the
file routes, the archive, discard, `GET /capabilities`, the WebSocket log stream, and an
MCP session constructor behind an extra. It defines no copy of `Capabilities` — the
descriptor comes back as parsed JSON, because a second model of the server's own shape is
the drift this item exists to stop. A refusal arrives as `Refused`, a type distinct from
`httpx.HTTPError`.

The original statement follows.


Two hand-rolled clients exist against the same protocol:
`matflow/backend/matflow/nodes/remote/base.py::ComputeServerClient` (async httpx plus
websockets) and `autoBO/src/autobo/truth/potential_runner.py::PotentialRunner` (sync
httpx, polling). Both call `/manifest`. Neither calls `/capabilities`.

**What it breaks:** both consumers drifted onto a deprecated contract without anyone
noticing, and any new consumer writes a third client.

**What replaces it:** a client package next to `compute_server_base` in this repo, not
in its own. `Capabilities` carries `contract_version`, and a client that versions apart
from the protocol it speaks is how the drift happened. It owns submit, poll, stream,
result, cancel, files, archive, discard, `GET /capabilities`, and the MCP attachment.
Consumers: autoBO, matflow, and anything later. No cost model in it.

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
  Execution lived in a thread pool thread until 2026-09-04, which made `timeout_s` and
  `cancel` unable to stop anything: one job ran 19 hours past its timeout, holding 19
  cores and 44 GB, while the server reported it as failed. A thread cannot be
  interrupted; a process can be killed. The process group rather than the process
  because job code spawns its own subprocesses — LAMMPS runs `lmp` under `Popen`.
  This also retired the `os.chdir` hazard: the child sets its own cwd, so there is no
  process-global chdir for a second job to race.
- **Nothing the worker waits for may be unbounded.** Reading a job's output waits for
  EOF, and EOF on a pipe needs every writer closed — including a process the job started
  and never waited on, which inherits stdout and holds the write end. That waited
  forever, and since one worker consumes the whole queue, every job submitted afterwards
  stayed queued (fixed 2026-09-10). The group is killed on every path out of a job, and
  the read after it is bounded. Adding a wait to the worker without a bound puts the
  queue back where it was.

## What needs building, in order

Five items, tracked as S-16 through S-20 in `spark-dashboard/docs/todo.md`. The order is what
depends on what, not what is most interesting.

**Who is waiting on this.** autoBO is the first agent client. It needs to attach to an
execution server, read what the server can do, write and submit its own calculation,
and be refused when the job is impossible. That is the whole of what it needs from this
repo, and it is what none of the current MCP surface supports.

| | Item | Depends on | Unblocks |
|---|---|---|---|
| 1 | ~~**S-16** Arbitrary code over MCP~~ **done 2026-08-26** | nothing | every agent client |
| 2 | ~~**S-17** Capability descriptor readable over MCP~~ **done 2026-08-26** | nothing; same file as S-16 | trusting a number from an emulated host |
| 3 | ~~**S-18** Refusal as a response shape~~ **done 2026-08-26** | a decision about the reason set | autoBO reporting a refusal as an outcome rather than a crash |
| 4 | ~~**S-19** The client package~~ **done 2026-08-26** | S-18, so the client has a refusal to represent | autoBO and matflow off `/manifest` |
| 5 | **S-20** Issue autoBO a client subject for lammps | nothing; operator step | a second instrument reachable at all |

S-16 through S-19 were one session, 2026-08-26. What is left is S-20, an operator step
independent of everything else, and the consumer-side migrations that S-19 unblocks:
autoBO off `/manifest` and `/mlips`, and matflow's relax node whenever it unpauses.

**Not in scope here, deliberately.** No cost model, no price, no budget, no schedule,
no queue policy. An instrument reports what a job took and what host it ran on.

An earlier version of this paragraph called two of those — a measured cost basis per
operation, and visible queue occupancy — future work for this repo. That was wrong on
both. The cost a later paper needs is a value a user assigns to a matflow sequence
standing for a step in an SDL digital twin; it is not a resource measurement and no
execution server can produce it. The occupancy is for scheduling matflow nodes in that
same digital twin, which is a different system's queue, not this one's. Neither belongs
here, now or later.
