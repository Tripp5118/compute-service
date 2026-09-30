# Methodology

The architectural decisions behind `compute_server_base` and `compute_interface` —
the two halves of one protocol. Changing any of these changes what every tool
server and every consumer can rely on.

The dashboard keeps the decisions about how *it* credentials and probes a tool
server; the decisions about what a tool server is and what it promises are here.

---

## The API is CRUD for jobs, and never a wrapper around the tool

A tool server is an instance of a template, not an independent service.
Everything generic — the job queue, bearer auth, and every shared route — lives
in `compute_server_base/`, installed into each image. This was extracted once
the second instance's `jobs.py` was byte-identical to the first's and a third
(LAMMPS) was foreseeable.

**The API takes jobs and reports what happened to them.** Python source, an
entrypoint, its arguments. Queue it, run it, return values, files, logs, status.
What the code does with the installed library is the code's business.

The API describes nothing about the tool it hosts, and this is the decision the
design keeps drifting away from. An endpoint per tool call — `submit_relax`,
`POST /operations/relax` — encodes that library's current call signature into the
server. Upstream renames an argument and the *server* is wrong, and has to be
rebuilt and redeployed to say the same thing differently. Arbitrary code has no
such coupling: when the library changes, the job code changes, and the job code
is written by the consumer already reading that library's documentation.

A capability descriptor listing operations with their `inputs` and `returns` is
the same mistake one level removed. It is a call-shape contract even when nothing
dispatches on it, and every consumer that reads one and every document that
explains one grows toward calling those operations. `GET /capabilities` is gone
for that reason, along with `Capabilities`, `Host` and `Operation`.

What a tool can compute is answered by `agent.md` and the knowledge files beside
it in that tool's repository, fetched by the consumer at a release tag. A running
container answers what a *job* did, never what the *tool* can do.

An instance may still add a route for its own **input format**, which is not the
same thing as a call into its library: `POST /run` on LAMMPS takes an input
script, because a script is LAMMPS's native input rather than a function
signature upstream can rename.

The deprecated `/manifest` and `/mlips` shims go with `/capabilities`. They were
kept on the belief that matflow's relax node called `/mlips`. It does not: every
`mlips` mention in matflow is a docstring, a comment, a UI tooltip or a test
name, its MLIP list is a hardcoded dict (`materials_framework_relax.py:31-34`),
and its `manifest()` client method has no callers. What matflow actually calls is
`POST /jobs`, the job stream, `GET /jobs/{id}/result` and
`POST /jobs/{id}/cancel` — job CRUD, so it is unaffected by any of this.

## One client package, shipped with the server it calls

Consumers do not hand-roll HTTP against these servers. `compute_interface` is the
consumer half, and it ships from this repository alongside `compute_server_base`,
so one release covers both halves of the protocol and a change to either lands in
one commit.

**The rule is that a consumer imports the package.** Shipping the two together is
how that stays honest, not why it matters. matflow's `ComputeServerClient` and
autoBO's `PotentialRunner` both sat on `/manifest` for months after the servers
moved to `/capabilities`, and neither imported anything from here — each
reimplemented the HTTP calls in its own project, where nothing connected them to
the contract. A consumer that reimplements them again is outside every guarantee
below, whatever repository it lives in.

The client **defines no copy of anything the server owns.** A refusal's `reason`
stays a string rather than a copied enum. A second model of the same shape is the
drift in miniature, so there is not one.

It holds no cost model, no scheduling, no retry policy and no opinion about which
calculation is worth running — the same split every other decision here holds.

## One version, declared by the consumer on every job

There is one version on the wire: **the tool repository's release tag.** The image
is built from it, and `agent.md` and the knowledge base come from it. One tag, one
meaning.

There were three, and that was two too many. `contract_version` was the protocol,
`version` was the solver's, `source_ref` was the instance repo's build commit;
each skewed independently, all three were read off a descriptor that no longer
exists, and none of them answered the question a consumer actually has.

The question is: *is the agent I built still right about this server?* So the
consumer answers it rather than polling for it. It fetches `agent.md` and the KB
at a tag, builds its tool-use agent, and sends that tag with every job. If it does
not match what the server was built from, the server refuses the job and says the
definition is stale. Cut a release whenever a change would make an agent's
instructions wrong, and drift surfaces at the moment of use instead of never.

The solver's own version is a fact about the image, documented in that
repository's `agent.md`. It is not on the wire, because nothing on the wire
should depend on it.

## Refusal is a value with a closed reason set, and the set holds no judgments

"This instance will not run that" travels as data as a `Refusal` body
(`refused`, `reason`, `detail`, `context`) from an HTTP route, which
`create_app()` turns into a 422. An agent that gets a traceback cannot tell "this
machine cannot do this, and here is why" from "this broke", and those call for
different next actions — pick another instance, versus retry or report a bug.

`reason` is a closed set precisely so a caller can branch on it, and every value in
it is something the server establishes **by looking**: the instance is not ready, the
operation is not advertised here, the named backend does not serve that operation,
the compute is running on an architecture it was not built for.

There is deliberately no "precondition not met", which the requirements document
proposed. A precondition — relax before elastic constants, equilibrate before
measuring — is a scientific judgment, and a server that starts making those is
deciding which calculations are worth doing. That is the split this whole design
holds: the instrument reports, the client decides. Preconditions live in each tool's
knowledge pack, where an agent reads them and decides for itself. Adding a reason
that requires a judgment is a methodology change, not a new enum value.

## Tool knowledge lives in the repository, read by the consumer

A tool's domain knowledge — workflows, preconditions, valid orderings, refusal
conditions and how to read a result — lives as `agent.md` and the knowledge files
it names at the root of that tool's private repository. A consumer fetches those
files at a known commit, filters the documents against live `/capabilities`, and
builds its tool-use agent locally.

The provider never serves that prose. Its job is to execute and report facts;
the consumer owns retrieval, prompt assembly and the decision about what to do
next. A document's `requires_operation` and `requires_backends` fields still
matter, but filtering happens client-side against the live descriptor rather
than once inside a container.

Because the prose is what a language model reads, its writing quality is part of
the tool's source interface. Keeping it in the repository makes it reviewable
and correctable without rebuilding a running image.

Instances still choose their own base image and platform — thermocalc is
pinned to `linux/amd64` and Python 3.10 for TC-Python, materials-framework is
aarch64-native on 3.12 — so the base package stays 3.10-compatible.

## Job output: values are returned, files are fetched, and never the reverse

A job's JSON result carries what it *computed*; a job's files carry what it
*wrote*, and the two travel over different routes. Every job runs with its own
directory as cwd (`workspace_root()/<job_id>`), and the result carries a
manifest of that directory rather than its contents.

The alternative — one payload holding both — collapses as soon as a tool's real
output is a file. A LAMMPS run writes a log, dump files and a restart, and a
trajectory is routinely gigabytes; base64 inside a JSON body inside, often, a
language model's context is not a thing to build on. Keying the split on *size*
instead ("inline it if it's small") would mean every consumer holding a
threshold rule and every job author guessing which side of it they are on.

So the MCP reads are deliberately asymmetric: `read_job_file` is capped at 64 kB
and refuses binary, because an agent reading a trajectory frame by frame is a
mistake the API should make awkward rather than merely discourage. Bulk
retrieval is HTTP.

Nothing sweeps a workspace. `DELETE /jobs/{id}/files` is the only thing that
reclaims it, because only the caller knows whether a trajectory is still wanted
— and a TTL sweeper that guesses wrong destroys the expensive half of a
campaign. A disk that actually fills is what should buy a sweeper.

The fetch route resolves a path and refuses anything outside the workspace,
symlinks included. Not because the job is untrusted — it already runs arbitrary
code — but because the *caller* must not be able to read the host through it.

---

## Key Invariants

1. The API is CRUD for jobs. It never grows a route, a field or a descriptor that
   names a call into the hosted tool — no per-operation endpoint, no operations
   list, no `inputs`/`returns` call shapes. Adding one is a methodology change, and
   the reason to refuse it is that it couples this server to a library signature
   upstream can rename. A route for an instance's own *input format*, like LAMMPS's
   `POST /run`, is not that. `compute_server_base/` stays Python 3.10-compatible
   while any instance image is.
2. A tool server never consults the dashboard to authenticate a caller. Verification
   is a local signature check plus an audience check, so compute keeps working when
   the dashboard is down — adding a network call or a synced allowlist to that path
   is a methodology change.
3. A tool server advertises nothing about its tool. It does not say what the tool
   can compute, which backends are installed, or what a call into it looks like.
   That question is answered by `agent.md` and its knowledge base in the tool's
   repository, and a job either imports what it needs or fails saying so.
4. Arbitrary Python from a trusted consumer is the interface, not an escape hatch
   beside a menu of named operations. A server that only runs things it has names
   for cannot do the work these exist for, and every such name is a signature that
   can go stale.
5. A refusal's `reason` vocabulary is closed, and every value in it is mechanically
   checkable by the server. Adding one that requires a judgment about whether a
   calculation is sensible is a methodology change, because it moves experimental
   decisions into the instrument.
6. A consumer reaches a compute server through `compute_interface`, not through HTTP
   it writes itself. The package defines no second copy of a shape the server owns
   and ships from this repository so both halves release together. Reimplementing
   the calls in a consuming project is how `/manifest` outlived its deprecation in
   two projects at once, and it forfeits every guarantee in this list.
7. A consumer declares the release tag it built its agent from on every job, and a
   server that was built from a different one refuses the job rather than running
   it. Work done under instructions already known to be stale is worse than work
   not done, because nothing downstream can tell the difference.
