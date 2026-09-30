# Methodology

The architectural decisions behind `compute_server_base` and `compute_interface` —
the two halves of one protocol. Changing any of these changes what every tool
server and every consumer can rely on.

The dashboard keeps the decisions about how *it* credentials and probes a tool
server; the decisions about what a tool server is and what it promises are here.

---

## Compute-server template: one shared base, one capability contract

A tool server is an instance of a template, not an independent service.
Everything generic — the job queue, bearer auth, and every shared route — lives
in `compute_server_base/`, installed into each image. An instance's
`server/main.py` contributes exactly two things: a `Capabilities` descriptor and
its own startup probing. This was extracted once the second instance's `jobs.py`
was byte-identical to the first's and a third (LAMMPS) was foreseeable.

`GET /capabilities` is the contract every consumer reads — autoBO's tool
agents, MatFlow's node catalog, the dashboard's tools page. It is keyed on
**operations**, not calculators or models: a backend is advertised under each
operation it can actually serve and nowhere else. Capability is genuinely
non-uniform — MEGNet predicts formation energy and has no `.relax()` at all —
so a contract keyed on "calculator" pushes that special case into every
consumer, as matflow `67a418c` shows. Backends are filtered by live import
probe per request, so an advertisement can be trusted flatly rather than
weighed against a separate document.

The predecessors `/manifest` and `/mlips` survive as deprecated shims because
MatFlow's relax node calls `/mlips` and that project is paused. They go once
its client is repointed.

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

The client **defines no copy of anything the server owns.** `capabilities()` returns
the parsed descriptor as a dict rather than a re-declared `Capabilities`, and a
refusal's `reason` stays a string rather than a copied enum. A second model of the
same shape is the drift in miniature, so there is not one.

Version skew is reported, not silent. `Capabilities` carries a `contract_version`,
and the client checks its major component on the first `capabilities()` call. A
consumer pinned to an older package finds out there, rather than from a route that
starts returning 404 a month later.

Three versions, because they skew independently and a single field cannot carry
all three. `contract_version` is the protocol. `version` is the solver's —
materialsframework's, LAMMPS's, TC-Python's — and tracks upstream. `source_ref`
is the instance repo's commit, stamped at build time from a `SOURCE_REF` build
argument, null when the build supplied none.

`source_ref` exists because the other two cannot answer the question a consumer
actually has. An agent that operates a tool is built from the `agent.md` and
knowledge files in that tool's repo, fetched at some commit. Rebuild the image
from newer source — a new operation, a rewritten instruction — and
`contract_version` does not move, because the protocol did not change, and
`version` does not move, because the solver did not. Only `source_ref` does. It
is what lets a consumer notice that the agent it built is older than the server
it is talking to.

It holds no cost model, no scheduling, no retry policy and no opinion about which
calculation is worth running — the same split every other decision here holds.

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

1. The `Capabilities` shape in `compute_server_base/compute_server_base/capabilities.py`
   is the contract between every tool server and every consumer of one — changes to
   it are methodology changes. It stays operations-keyed, and `compute_server_base/`
   stays Python 3.10-compatible while any instance image is.
2. A tool server never consults the dashboard to authenticate a caller. Verification
   is a local signature check plus an audience check, so compute keeps working when
   the dashboard is down — adding a network call or a synced allowlist to that path
   is a methodology change.
3. Nothing a tool server advertises may outrun what its image can do. Operations are
   filtered by live import probe, and a knowledge document is served only if the
   operation it requires is advertised. Adding an advertisement path that skips that
   check — an operation listed from a static table, a document served unreconciled —
   is a methodology change, because every consumer's right to trust the advertisement
   flatly depends on it.
4. The MCP face carries the same two general powers the REST face does: an agent can
   submit code it wrote itself (`submit_code`, mirroring `POST /jobs`) and can read the
   capability descriptor (`get_capabilities`). Named `submit_<operation>` tools are a
   convenience over the same execution path, never a replacement for it — a face that
   only offers named operations cannot serve the use case these servers exist for.
5. A refusal's `reason` vocabulary is closed, and every value in it is mechanically
   checkable by the server. Adding one that requires a judgment about whether a
   calculation is sensible is a methodology change, because it moves experimental
   decisions into the instrument.
6. A consumer reaches a compute server through `compute_interface`, not through HTTP
   it writes itself. The package defines no second copy of a shape the server owns,
   ships from this repository so both halves release together, and reports a major
   `contract_version` mismatch rather than failing quietly later. Reimplementing the
   calls in a consuming project is how `/manifest` outlived its deprecation in two
   projects at once, and it forfeits every guarantee in this list.
