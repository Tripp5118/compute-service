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
