# compute-interface

The consumer half of the compute-server protocol. A consumer imports this rather
than writing its own HTTP: the two hand-rolled clients that came before it both
sat on a deprecated endpoint for months because each reimplemented the calls in
its own project, with nothing connecting them to the contract. It releases
alongside `compute_server_base` so both halves of the protocol change together.

## Install

```bash
pip install ./compute_interface
```

From another project, by git subdirectory:

```bash
pip install 'compute-interface @ git+https://github.com/Tripp5118/compute-service@v0.4.0#subdirectory=compute_interface'
```

## Use

When you only want the numbers back:

```python
from compute_interface import ComputeServerClient, JobFailed, Refused

with ComputeServerClient("http://materials-framework:8000", token) as tool:
    values = tool.run(source, variables={"structure": cif}, timeout_s=3600)
```

`run()` is submit → wait → return values. Set `timeout_s`: the server kills the
job's process group when it expires, and without it a job that hangs occupies the
server's one worker. `tool.jobs()` shows that queue and whether its worker is
alive, which is the first thing to read when nothing is progressing.

The long form, when you want the live log or the files the job wrote:

```python
from compute_interface import ComputeServerClient, Refused

with ComputeServerClient("http://materials-framework:8000", token) as tool:
    try:
        job = tool.submit(source, entrypoint="run", variables={"structure": cif})
    except Refused as refusal:
        report(refusal.reason, refusal.detail)   # a dead end, not a crash
    else:
        for event in tool.stream(job):           # live output while it runs
            log(event.get("message", ""))
        result = tool.wait(job)
        for entry in tool.files(job):
            tool.fetch(job, entry["path"], "out/")
        tool.discard(job)                        # nothing else reclaims the workspace
```

Three outcomes, three types, and telling them apart is the point. `Refused`
means the server will not run this and says why. `JobFailed` — raised by `run()`
— means the calculation itself raised, and carries the server's traceback on
`.job_traceback`. Everything else that goes wrong raises `httpx.HTTPError`.
`wait()` returns a failed result as data rather than raising, because a caller
that streamed the job has usually already seen why.

`refusal.reason` is one of a closed set, so it can be branched on, and
`refusal.detail` is prose — read it, do not match on it. The values are not
re-declared here as an enum: `RefusalReason` in
`compute_server_base/refusal.py` owns them, and a second copy is the drift this
package exists to stop.

The refusal you will actually meet is a stale agent version. A consumer builds
its tool-use agent from `agent.md` and the knowledge base at a release tag and
sends that tag with every job; a server built from a different one refuses,
rather than run real work under instructions already known to be wrong. Re-fetch
the definition at the tag the server names and resubmit.

## What it does not do

No cost model, no scheduling, no retry policy, no opinion about which
calculation is worth running. An instrument reports; the caller decides.

It also asks no server what its tool can do. That question is answered by
`agent.md` and the knowledge files beside it in the tool's repository, fetched
from git — never over the wire.
