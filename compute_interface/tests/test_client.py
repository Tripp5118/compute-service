"""The client against a real compute server over a real socket.

Run against a live uvicorn rather than an ASGI transport on purpose: the log
stream is a WebSocket, and an in-process transport cannot exercise one at all.
That is the half most worth testing here, because it is the half neither
hand-rolled client got right.

Run, from `compute_interface/`:

    PYTHONPATH="$PWD/../compute_server_base:$PWD" uv run --with fastapi --with httpx \
        --with websockets --with uvicorn --with pyjwt --with pytest pytest tests -v

PYTHONPATH rather than `--with ../compute_server_base`: uv serves a cached build
of that package keyed on its version, so an un-bumped edit to the server half is
invisible to a test run that installs it — and this suite exists to catch exactly
the client/server disagreements that would hide behind that.
"""

from __future__ import annotations

import os
import socket
import threading
import time
from typing import TYPE_CHECKING, Any

import httpx
import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

os.environ.setdefault("COMPUTE_SERVER_TOKEN", "test-token-for-pytest")

import uvicorn  # noqa: E402 — env var must be set before the server imports
from compute_server_base import create_app  # noqa: E402
from compute_interface import ComputeServerClient, JobFailed, Refused  # noqa: E402

TOKEN = os.environ["COMPUTE_SERVER_TOKEN"]


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _serve(workspace: Path, *, release: str | None = None) -> Iterator[str]:
    """Run one compute server on a loopback port for the life of a test."""
    os.environ["JOB_WORKSPACE_ROOT"] = str(workspace)
    if release is None:
        os.environ.pop("RELEASE", None)
    else:
        os.environ["RELEASE"] = release
    port = _free_port()

    app = create_app(tool_name="stub")
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(200):
        if server.started:
            break
        time.sleep(0.05)
    else:  # pragma: no cover — a server that never starts fails every assertion anyway
        pytest.fail("stub server did not start")
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=10)


@pytest.fixture
def ready_url(tmp_path: Path) -> Iterator[str]:
    """A server that will run jobs."""
    yield from _serve(tmp_path / "ready")


@pytest.fixture
def released_url(tmp_path: Path) -> Iterator[str]:
    """A server built from a known release, for the staleness check."""
    yield from _serve(tmp_path / "released", release="v2.0.0")


def test_health_carries_identity_and_release(released_url: str) -> None:
    """The one unauthenticated call, and the cheap way to see what a server is."""
    with ComputeServerClient(released_url, TOKEN) as client:
        payload = client.health()

    assert payload["status"] == "ok"
    assert payload["tool"] == "stub"
    assert payload["release"] == "v2.0.0"


def test_a_stale_agent_definition_is_refused_rather_than_run(released_url: str) -> None:
    """The drift this package exists to stop, caught at the moment of use.

    A consumer builds its tool-use agent from agent.md at a release; if the
    server has moved past it, the instructions may describe a build this is not.
    """
    with ComputeServerClient(released_url, TOKEN, agent_release="v1.0.0") as client, pytest.raises(Refused) as caught:
        client.submit("def run():\n    return {}\n")

    assert caught.value.reason == "agent_release_stale"
    assert caught.value.context == {"agent_release": "v1.0.0", "server_release": "v2.0.0"}
    # A dead end the caller acts on, not a transport failure to retry.
    assert not isinstance(caught.value, httpx.HTTPError)


def test_a_matching_release_is_not_refused(released_url: str) -> None:
    with ComputeServerClient(released_url, TOKEN, agent_release="v2.0.0") as client:
        assert client.run("def run():\n    return {'ok': 1}\n") == {"ok": 1}


def test_submit_wait_and_read_a_result(ready_url: str) -> None:
    """The path a generated campaign takes, end to end over a real socket."""
    with ComputeServerClient(ready_url, TOKEN) as client:
        job_id = client.submit(
            "def measure(a, b):\n    return {'sum': a + b}\n",
            entrypoint="measure",
            variables={"a": 2, "b": 5},
        )
        result = client.wait(job_id, poll_s=0.1, timeout_s=60)

    assert result["status"] == "succeeded"
    assert result["values"] == {"sum": 7}
    assert result["provenance"]["tool_name"] == "stub"


def test_run_does_submit_wait_and_read_in_one_call(ready_url: str) -> None:
    """The three-call shape is what a caller wanting only the numbers had to write."""
    with ComputeServerClient(ready_url, TOKEN) as client:
        assert client.run("def run(x):\n    return {'doubled': x * 2}\n", variables={"x": 21}, poll_s=0.1) == {
            "doubled": 42
        }


def test_run_raises_a_failure_rather_than_returning_an_empty_result(ready_url: str) -> None:
    """A job that raised must not come back looking like a job that returned nothing.

    `wait()` hands the failed result back as data; `run()` returns only values,
    so it has nowhere to put the error and raises instead.
    """
    with ComputeServerClient(ready_url, TOKEN) as client, pytest.raises(JobFailed) as caught:
        client.run("def run():\n    raise ValueError('no')\n", poll_s=0.1)

    assert "no" in str(caught.value)
    assert "ValueError" in (caught.value.job_traceback or "")


def test_jobs_reports_the_queue_and_its_worker(ready_url: str) -> None:
    """The window that did not exist when jobs were reported as sitting stale."""
    with ComputeServerClient(ready_url, TOKEN) as client:
        client.run("def run():\n    return {}\n", poll_s=0.1)
        queue = client.jobs()

        assert queue["worker_alive"] is True
        assert len(queue["jobs"]) >= 1
        assert queue["jobs"][0]["age_s"] >= 0


def test_files_written_by_a_job_are_listed_fetched_and_discarded(ready_url: str, tmp_path: Path) -> None:
    """The workspace half: a job's relative write is reachable, and only discard reclaims it."""
    with ComputeServerClient(ready_url, TOKEN) as client:
        job_id = client.submit("def run():\n    open('out.txt', 'w').write('hello')\n    return {'wrote': 1}\n")
        client.wait(job_id, poll_s=0.1, timeout_s=60)

        assert [f["path"] for f in client.files(job_id)] == ["out.txt"]
        fetched = client.fetch(job_id, "out.txt", tmp_path / "fetched")
        assert fetched.read_text() == "hello"

        archive = client.archive(job_id, tmp_path / "bundle.tar.gz")
        assert archive.stat().st_size > 0

        client.discard(job_id)
        assert client.files(job_id) == []


def test_log_output_arrives_while_the_job_is_still_running(ready_url: str) -> None:
    """The reason this package carries a WebSocket dependency at all.

    A long run has to be watchable. Nothing else on the protocol returns output
    before the job finishes — the result route 409s until it does.
    """
    with ComputeServerClient(ready_url, TOKEN) as client:
        job_id = client.submit(
            "import time\n"
            "def run():\n"
            "    for i in range(3):\n"
            "        print(f'step {i}', flush=True)\n"
            "        time.sleep(0.2)\n"
            "    return {'steps': 3}\n"
        )
        lines = [event.get("message", "") for event in client.stream(job_id)]

    assert any("step 0" in line for line in lines)
    assert any("step 2" in line for line in lines)


def test_a_refusal_carries_prose_a_caller_can_act_on(released_url: str) -> None:
    """The distinction the whole refusal shape exists for.

    A caller that catches `Refused` is handling a dead end it can report and
    work around. A caller that catches `httpx.HTTPError` is handling breakage.
    They must not arrive as the same type.
    """
    with ComputeServerClient(released_url, TOKEN, agent_release="v1.0.0") as client, pytest.raises(Refused) as caught:
        client.submit("def run():\n    return 1\n")

    assert "stub" in caught.value.detail
    # The release to re-fetch at has to be in the prose, not only the context.
    assert "v2.0.0" in caught.value.detail


def test_a_dropped_read_is_retried_rather_than_surfaced(monkeypatch):
    """A connection reset while fetching a finished job's result is not a failed job.

    On 2026-10-28 a campaign lost three smoke evaluations to
    `ReadError: [Errno 104] Connection reset by peer` while reading results. Every job on
    the server reported `succeeded`; only the wire broke. The caller could not tell the
    difference, reported an infrastructure failure, and the GPU work was thrown away.
    """
    import httpx

    from compute_interface.client import ComputeServerClient

    client = ComputeServerClient("http://example.invalid", "token")

    calls = {"n": 0}

    def flaky(path: str):
        calls["n"] += 1
        if calls["n"] < 3:
            raise httpx.ReadError("Connection reset by peer")
        return httpx.Response(
            200,
            json={"values": {"ok": True}, "error": None},
            request=httpx.Request("GET", "http://example.invalid" + path),
        )

    monkeypatch.setattr(client._client, "get", flaky)
    monkeypatch.setattr("compute_interface.client._READ_BACKOFF_S", 0.0)

    assert client.result("job-1")["values"] == {"ok": True}
    assert calls["n"] == 3, "should have retried twice before succeeding"


def test_submit_is_never_retried_after_the_request_is_sent(monkeypatch):
    """Re-sending a POST whose reply was lost can enqueue the same work twice, and a
    duplicate ten-minute GPU job is worse than an error the caller can see."""
    import httpx
    import pytest as _pytest

    from compute_interface.client import ComputeServerClient

    client = ComputeServerClient("http://example.invalid", "token")

    calls = {"n": 0}

    def always_drops(*args, **kwargs):
        calls["n"] += 1
        raise httpx.ReadError("Connection reset by peer")

    monkeypatch.setattr(client._client, "post", always_drops)

    with _pytest.raises(httpx.ReadError):
        client.submit("def run():\n    return {}")
    assert calls["n"] == 1, "submit must be attempted exactly once"


def test_no_tls_material_configured_leaves_the_defaults_alone(monkeypatch):
    """A tool reached directly over http:// on its own host must keep working untouched —
    the gateway variables exist for callers that cross a machine, not for every caller."""
    from compute_interface.client import _tls_kwargs

    for var in ("COMPUTE_CA_BUNDLE", "COMPUTE_CLIENT_CERT", "COMPUTE_CLIENT_KEY"):
        monkeypatch.delenv(var, raising=False)
    assert _tls_kwargs() == {}


def test_tls_material_becomes_one_ssl_context(monkeypatch):
    """A verify= path plus a cert= pair cannot be combined on every client this is used
    with — httpx2 rejects it outright — so the three variables collapse into one context."""
    import ssl as _ssl

    from compute_interface import client as _client

    seen = {}

    class FakeContext:
        def load_cert_chain(self, cert, key):
            seen["chain"] = (cert, key)

    def fake_default_context(cafile=None):
        seen["cafile"] = cafile
        return FakeContext()

    monkeypatch.setattr(_ssl, "create_default_context", fake_default_context)
    monkeypatch.setenv("COMPUTE_CA_BUNDLE", "/etc/spark-lab/certs/ca.pem")
    monkeypatch.setenv("COMPUTE_CLIENT_CERT", "/etc/spark-lab/certs/client.pem")
    monkeypatch.setenv("COMPUTE_CLIENT_KEY", "/etc/spark-lab/certs/client-key.pem")

    kwargs = _client._tls_kwargs()
    assert seen["cafile"] == "/etc/spark-lab/certs/ca.pem"
    assert seen["chain"] == ("/etc/spark-lab/certs/client.pem", "/etc/spark-lab/certs/client-key.pem")
    assert set(kwargs) == {"verify"}, "cert= must not be passed alongside verify="
    assert isinstance(kwargs["verify"], FakeContext)


def test_half_a_client_certificate_is_an_error_rather_than_a_handshake_failure(monkeypatch):
    """Setting the cert and forgetting the key otherwise surfaces as an opaque TLS error
    from inside httpx, a long way from the one line that caused it."""
    import pytest as _pytest

    from compute_interface.client import _tls_kwargs

    monkeypatch.delenv("COMPUTE_CA_BUNDLE", raising=False)
    monkeypatch.setenv("COMPUTE_CLIENT_CERT", "/etc/spark-lab/certs/client.pem")
    monkeypatch.delenv("COMPUTE_CLIENT_KEY", raising=False)

    with _pytest.raises(ValueError, match="COMPUTE_CLIENT_KEY"):
        _tls_kwargs()


def test_a_gateway_path_prefix_survives_into_the_websocket_url(monkeypatch):
    """Behind a per-Spark gateway the base carries a path, and a socket dialled at the bare
    host misses the prefix the gateway routes on — so the stream 404s while every other
    call on the same client works."""
    from compute_interface.client import ComputeServerClient

    dialled = {}

    class FakeSocket:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def recv(self):
            raise OSError("closed")

    def fake_connect(url, **kwargs):
        dialled["url"] = url
        dialled["ssl"] = kwargs.get("ssl")
        return FakeSocket()

    monkeypatch.setattr("websockets.sync.client.connect", fake_connect)

    client = ComputeServerClient("https://10.125.153.116:8443/materials-framework", "token")
    list(client.stream("job-1"))

    assert dialled["url"] == "wss://10.125.153.116:8443/materials-framework/jobs/job-1/stream"


def test_a_direct_http_base_still_streams_at_the_root(monkeypatch):
    from compute_interface.client import ComputeServerClient

    dialled = {}

    class FakeSocket:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def recv(self):
            raise OSError("closed")

    def fake_connect(url, **kwargs):
        dialled["url"] = url
        dialled["ssl"] = kwargs.get("ssl")
        return FakeSocket()

    monkeypatch.setattr("websockets.sync.client.connect", fake_connect)

    client = ComputeServerClient("http://materials-framework:8000", "token")
    list(client.stream("job-1"))

    assert dialled["url"] == "ws://materials-framework:8000/jobs/job-1/stream"
    assert dialled["ssl"] is None, "a plain ws:// stream must not be handed an SSL context"


def test_health_works_when_a_client_certificate_is_configured(monkeypatch, tmp_path):
    """`httpx.get` has no `cert` parameter, so building the health request through the
    top-level helper raised TypeError as soon as a gateway client cert was set — breaking
    the one call used to decide whether a tool is reachable at all."""
    import httpx as _httpx

    from compute_interface.client import ComputeServerClient

    sentinel = object()
    monkeypatch.setattr("compute_interface.client._tls_kwargs", lambda: {"verify": sentinel})

    seen = {}

    class FakeClient:
        def __init__(self, **kwargs):
            seen.update(kwargs)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, url):
            seen["url"] = url
            return _httpx.Response(200, json={"status": "ok"}, request=_httpx.Request("GET", url))

    monkeypatch.setattr("compute_interface.client.httpx.Client", FakeClient)

    client = ComputeServerClient("https://host:8443/materials-framework", "token")
    assert client.health() == {"status": "ok"}
    assert seen["verify"] is sentinel
    assert "cert" not in seen
    assert seen["url"] == "https://host:8443/materials-framework/health"


def test_tls_material_reaches_the_transport_not_just_the_client(monkeypatch):
    """httpx ignores `verify=` on a Client when an explicit transport is supplied, because the
    transport holds the SSL context. Putting it on the Client left every real request using
    default verification while health() -- which builds a plain client -- still worked, so a
    server read as reachable and then refused every job."""
    from compute_interface.client import ComputeServerClient

    sentinel = object()
    monkeypatch.setattr("compute_interface.client._tls_kwargs", lambda: {"verify": sentinel})

    seen = {}

    class FakeTransport:
        def __init__(self, **kwargs):
            seen.update(kwargs)

    monkeypatch.setattr("compute_interface.client.httpx.HTTPTransport", FakeTransport)

    ComputeServerClient("https://host:8443/materials-framework", "token")

    assert seen.get("verify") is sentinel, "SSL context must be given to the transport"
    assert "retries" in seen, "connection retries must survive alongside the TLS material"
