# Execution servers — what they are for and what they have to do

This is the requirements document for the compute servers: the goal they serve, the
surface every instance has to expose, and the line the API must not cross. Read it
before changing `compute_server_base/`, `compute_interface/`, or any tool server
built on them.

The three instances are `spark-tool-materials-framework`, `spark-tool-lammps` and
`spark-tool-thermocalc`, each its own repository. `spark-tool-template` is the
skeleton a new one is created from.

## The goal

A trusted client sends Python to a machine that has a solver installed on it. The
server runs it and gives back everything it produced: the values the code returned,
the files it wrote, and its output while it is still running.

That is the whole of it.

## The API is CRUD for jobs

**The API handles jobs. It does not describe, wrap or expose the tool it hosts.**

A job is Python source, an entrypoint name and that entrypoint's arguments. The
server queues it, runs it in a child process with its own directory as cwd, and
reports what happened. What the code does with the installed library — which
calculator, which analyzer, which arguments — is the code's business and nothing the
server knows or cares about.

This is the line the design keeps getting pulled across, so it is worth stating why
it holds.

**An endpoint per tool call is a calculator behind HTTP.** The moment a server grows
`submit_relax` or `POST /operations/relax`, its API encodes that library's current
call signature. Upstream renames an argument and the server is wrong — not the job
code, the server, which now has to be rebuilt and redeployed to say the same thing a
different way. Arbitrary code has no such coupling: when the library changes, the
job code changes, and the job code is written by the consumer who is already reading
that library's documentation.

**Describing calls is the same mistake one level removed.** A capability descriptor
listing operations with their `inputs` and `returns` is a call-shape contract even
when nothing dispatches on it. Every consumer that reads it, and every document that
explains it, grows toward calling those operations. That is how this drifted.

**What the tool can compute is not the server's question.** It is answered by
`agent.md` and the knowledge files beside it in the tool's repository, fetched by
the consumer at a release tag. A running container answers what a *job* did, never
what the *tool* can do.

## What every instance exposes

| Route | Requirement |
|---|---|
| `POST /jobs` | Python source, an entrypoint name, its arguments, and the release tag the caller built its agent from. Returns a job id immediately. |
| `GET /jobs` | Every job this process knows about, plus `worker_alive` and the queued/running counts. One worker runs one job at a time, so a job that cannot finish holds every job behind it — this is where that shows, and it is what an operator dashboard reads. |
| `GET /jobs/{id}` | Status: queued, running, succeeded, failed, cancelled. |
| `GET /jobs/{id}/result` | Return values, the error and its traceback if there was one, the manifest of files written, and provenance: which tool ran it, which release it was built from, and how long it took. |
| `GET /jobs/{id}/files` | What the job wrote — path, size, mtime. No contents. |
| `GET /jobs/{id}/files/{path}` | One file out of the workspace. Paths resolved and refused outside it. |
| `GET /jobs/{id}/archive` | The whole workspace as one tar.gz. |
| `DELETE /jobs/{id}/files` | Reclaim the workspace. Nothing else does. |
| `POST /jobs/{id}/cancel` | A queued job is dropped; a running one has its process group killed. So does `timeout_s`. |
| `WS /jobs/{id}/stream` | Log history replayed, then live output until the job finishes. |
| `GET /health` | Unauthenticated liveness, plus the tool's name and the release it was built from. |

Every job runs in its own directory with the process cwd set to it, so code that
names its output files relatively writes them where they can be fetched from.

An instance may add a route for its own **input format** — not for a call into its
library. `POST /run` on LAMMPS takes an input script, because a script is LAMMPS's
native input rather than a function signature that upstream can rename. That is the
only kind of instance route there is a reason for.

## One version, and what it is for

There is **one version on the wire: the tool repository's release tag.** The image
is built from it, and `agent.md` and the knowledge base come from it. One tag, one
meaning.

A consumer fetches `agent.md` and the KB at a tag, builds its tool-use agent from
them, and sends that tag with every job. If it does not match what the server was
built from, the server refuses the job and says so: the consumer's agent definition
is stale, reload it and resubmit. Cut a release whenever a change would make an
agent's instructions wrong, and drift becomes impossible to miss instead of
something to poll for.

The solver's own version — `materialsframework`, the LAMMPS build, TC-Python — is a
fact about the image, documented in that repository's `agent.md`. It is not on the
wire, because nothing on the wire should depend on it.

## Standing constraints

- **Arbitrary code is the point, not an escape hatch.** A trusted consumer's Python
  is the interface. A server that only runs things it has names for cannot do the
  work these exist for.
- **The server reports; it does not decide.** What a job did, how long it took, on
  what host. No cost model, no budget, no schedule, no opinion about which
  calculation is worth running.
- **Jobs are asynchronous.** Nothing blocks a request for the length of a
  calculation.
- **A job runs in a child process, and stopping it means killing that process
  group.** In a thread pool thread, `timeout_s` and `cancel` cannot stop anything:
  one job ran 19 hours past its timeout holding 19 cores and 44 GB while the server
  reported it as failed. A thread cannot be interrupted; a process can be killed.
  The process group rather than the process, because job code spawns its own
  subprocesses — LAMMPS runs `lmp` under `Popen`. The child also sets its own cwd,
  so there is no process-global chdir for a second job to race.
- **Nothing the worker waits for may be unbounded.** Reading a job's output waits
  for EOF, and EOF on a pipe needs every writer closed — including a process the job
  started and never waited on, which inherits stdout and holds the write end. That
  waits forever, and since one worker consumes the whole queue, every job submitted
  afterwards stays queued. The group is killed on every path out of a job, and the
  read after it is bounded. Adding a wait to the worker without a bound puts the
  queue back there.
- **A refusal is a value, not a failure.** "This server will not run that" comes
  back as structured JSON a consumer can branch on, not a traceback. A stale agent
  version is one of these.

**Not in scope here, deliberately.** No cost model, no price, no budget, no
schedule, no queue policy. An instrument reports what a job took and what host it
ran on. The cost a later paper needs is a value a user assigns to a matflow sequence
standing for a step in an SDL digital twin; it is not a resource measurement and no
execution server can produce it. Queue occupancy for scheduling those nodes is that
system's queue, not this one's.
