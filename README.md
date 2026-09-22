# compute-service

Run code on a machine that has a solver installed on it, and get back what it
produced: the values it returned, the files it wrote, and its output while it is
still running.

Most callers are agents rather than people, so a caller that has never seen the
machine can read what it can do, decide whether a job is possible there, and be
refused in a form it can act on.

Two packages, one protocol, released together:

| Package | Half |
|---|---|
| [`compute_server_base`](compute_server_base/) | The server. A job queue, per-job workspaces, bearer auth, a REST face and an MCP face. An instance supplies a capability descriptor and its own startup probing; everything else comes from here. |
| [`compute_interface`](compute_interface/) | The client. Submit, poll, stream logs, fetch files, cancel — plus the capability descriptor and an MCP session. |

## Install

```bash
pip install "compute-server-base @ git+https://github.com/Tripp5118/compute-service@v0.2.0#subdirectory=compute_server_base"
pip install "compute-interface @ git+https://github.com/Tripp5118/compute-service@v0.2.0#subdirectory=compute_interface"
```

Both need Python 3.10 or newer.

## Using it

A consumer imports `compute_interface` rather than writing its own HTTP. That is
the rule the two packages exist to enforce: the client defines no second copy of
any shape the server owns, and reports a major contract version mismatch on the
first `capabilities()` call. Two earlier hand-rolled clients sat on a deprecated
endpoint for months because each reimplemented the calls in its own project.

```python
from compute_interface import ComputeServerClient

client = ComputeServerClient("http://localhost:8300", token=TOKEN)
print(client.capabilities()["operations"])
values = client.run(SOURCE, variables={"x": 1})
```

Each package's README covers its own surface:
[server](compute_server_base/README.md), [client](compute_interface/README.md).

## What it deliberately does not do

No cost model, no budget, no scheduling, no queue policy, and no opinion about
which calculation is worth running. A server reports what a job took and what
host it ran on. Those decisions belong to the caller.

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
