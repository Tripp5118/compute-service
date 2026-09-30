r"""Coverage for the shared compute-server path, with no real toolchain involved.

The two instance test suites exercise this same code, but only inside their
own images — one needs MaterialsFramework, the other a licensed Thermo-Calc
install. This drives create_app() with plain-Python jobs so the shared half
has a check that runs anywhere.

Run:
    cd compute_server_base && PYTHONPATH=. uv run --with fastapi --with httpx --with pytest pytest tests -v
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("COMPUTE_SERVER_TOKEN", "test-token-for-pytest")

from compute_server_base import create_app  # noqa: E402 — env var must be set before import

TOKEN = os.environ["COMPUTE_SERVER_TOKEN"]
AUTH = {"Authorization": f"Bearer {TOKEN}"}

ECHO = """
def run(x):
    print(f"got {x}")
    return {"doubled": x * 2}
"""

SLOW = """
def run():
    import time
    time.sleep(30)
    return {}
"""

# Writes both pids into the workspace before hanging, so the test can ask the
# OS whether they are really gone rather than trusting the job's status.
SPAWNS_AND_HANGS = """
def run():
    import os, subprocess, time
    child = subprocess.Popen(["sleep", "300"])
    with open("pids.txt", "w") as handle:
        handle.write(f"{os.getpid()}\\n{child.pid}\\n")
    time.sleep(300)
    return {}
"""


# Returns promptly and leaves a process behind holding the inherited stdout —
# a Popen the job never waited on, which is ordinary job code, not abuse.
LEAKS_A_PROCESS = """
def run():
    import subprocess, sys
    subprocess.Popen([sys.executable, "-c", "import time; time.sleep(300)"])
    return {"leaked": True}
"""


app = create_app(tool_name="stub")


def _still_running(pid: int) -> bool:
    """Whether a pid is a live process, reading /proc rather than signalling it.

    `os.kill(pid, 0)` succeeds on a zombie, and a killed grandchild stays one
    until something reaps it — in a container whose pid 1 is pytest, nothing does.
    """
    try:
        state = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()[0]
    except FileNotFoundError:
        return False
    return state != "Z"


def _wait_for_terminal(client: TestClient, job_id: str, timeout_s: float = 30.0) -> dict:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        resp = client.get(f"/jobs/{job_id}", headers=AUTH)
        assert resp.status_code == 200
        if resp.json()["status"] in ("succeeded", "failed", "cancelled"):
            return resp.json()
        time.sleep(0.1)
    raise TimeoutError(f"job {job_id} did not finish within {timeout_s}s")


def test_health_requires_no_auth():
    """/health is the liveness probe, so it must answer before any token exists."""
    with TestClient(app) as client:
        body = client.get("/health").json()
    assert body["status"] == "ok"
    assert body["tool"] == "stub"


@pytest.mark.parametrize("path", ["/jobs", "/jobs/anything"])
def test_authenticated_routes_reject_missing_token(path: str):
    """The bearer token is the only trust boundary — see jobs.py on why."""
    with TestClient(app) as client:
        assert client.get(path).status_code == 401


def test_the_server_describes_no_operations_at_all():
    """The API is job CRUD. Nothing here says what the hosted tool can compute.

    A route naming a call into the library would pin this server to a signature
    upstream can rename, which is the coupling this contract exists without.
    """
    with TestClient(app) as client:
        assert client.get("/capabilities", headers=AUTH).status_code == 404


def test_a_job_from_a_stale_agent_is_refused_rather_than_run(monkeypatch):
    """Work under instructions known to be stale is worse than work not done.

    Nothing downstream can tell a number computed against the wrong build from a
    right one, so the refusal happens before the job is queued.
    """
    monkeypatch.setenv("RELEASE", "v2.0.0")
    with TestClient(app) as client:
        resp = client.post("/jobs", headers=AUTH, json={"code": ECHO, "variables": {"x": 1}, "agent_release": "v1.0.0"})

    assert resp.status_code == 422
    body = resp.json()
    assert body["refused"] is True
    assert body["reason"] == "agent_release_stale"
    # Both sides named, so the caller knows which release to re-fetch at.
    assert body["context"] == {"agent_release": "v1.0.0", "server_release": "v2.0.0"}


def test_a_matching_release_runs(monkeypatch):
    monkeypatch.setenv("RELEASE", "v2.0.0")
    with TestClient(app) as client:
        resp = client.post("/jobs", headers=AUTH, json={"code": ECHO, "variables": {"x": 1}, "agent_release": "v2.0.0"})
    assert resp.status_code == 200


def test_a_caller_with_no_agent_definition_is_not_version_checked(monkeypatch):
    """A script or a smoke test has no agent to be stale, so it is not gated."""
    monkeypatch.setenv("RELEASE", "v2.0.0")
    with TestClient(app) as client:
        resp = client.post("/jobs", headers=AUTH, json={"code": ECHO, "variables": {"x": 1}})
    assert resp.status_code == 200


def test_an_unstamped_build_does_not_refuse_everything(monkeypatch):
    """No RELEASE means unknown, and unknown must not read as a mismatch."""
    monkeypatch.delenv("RELEASE", raising=False)
    with TestClient(app) as client:
        resp = client.post("/jobs", headers=AUTH, json={"code": ECHO, "variables": {"x": 1}, "agent_release": "v1.0.0"})
    assert resp.status_code == 200


def test_submit_poll_result():
    """The whole point of the shared path: a job goes in, values come out."""
    with TestClient(app) as client:
        resp = client.post("/jobs", headers=AUTH, json={"code": ECHO, "variables": {"x": 21}})
        assert resp.status_code == 200
        job_id = resp.json()["job_id"]

        assert _wait_for_terminal(client, job_id)["status"] == "succeeded"

        result = client.get(f"/jobs/{job_id}/result", headers=AUTH).json()
        assert result["values"] == {"doubled": 42}
        assert result["provenance"]["tool_name"] == "stub"
        assert result["provenance"]["wall_time_s"] >= 0


def test_result_409s_before_the_job_finishes():
    """A caller polling too early must get a distinct answer from a job that failed."""
    with TestClient(app) as client:
        job_id = client.post("/jobs", headers=AUTH, json={"code": SLOW}).json()["job_id"]
        assert client.get(f"/jobs/{job_id}/result", headers=AUTH).status_code == 409


def test_cancel_a_queued_job():
    """Cancel is only reliable before the job starts — see jobs.py on the running case."""
    with TestClient(app) as client:
        client.post("/jobs", headers=AUTH, json={"code": SLOW})  # occupies the worker
        job_id = client.post("/jobs", headers=AUTH, json={"code": ECHO, "variables": {"x": 1}}).json()["job_id"]

        assert client.post(f"/jobs/{job_id}/cancel", headers=AUTH).status_code == 200
        assert _wait_for_terminal(client, job_id)["status"] == "cancelled"


def test_timeout_kills_the_job_and_anything_it_spawned():
    """The one that matters: a timed-out job has to stop computing, not just stop being waited on.

    Before jobs ran in a child process, `timeout_s` marked a job failed and left
    it running — 19 cores and 44 GB for 19 hours, in the case that prompted this.
    Asserting the status alone would have passed throughout.
    """
    with TestClient(app) as client:
        job_id = client.post(
            "/jobs",
            headers=AUTH,
            json={
                "code": SPAWNS_AND_HANGS,
                "timeout_s": 3,
            },
        ).json()["job_id"]

        assert _wait_for_terminal(client, job_id)["status"] == "failed"
        pids = client.get(f"/jobs/{job_id}/files/pids.txt", headers=AUTH)
        assert pids.status_code == 200

    # The grandchild is the LAMMPS-shaped case: job code that runs a solver under
    # Popen. Killing only the job's own process would leave the solver running.
    assert [pid for pid in (int(line) for line in pids.text.split()) if _still_running(pid)] == []


def test_a_job_that_leaks_a_process_does_not_stall_the_queue():
    """The stall: a job returned, left a process holding stdout, and the queue died.

    Reading the child's output waits for EOF on its pipe, and EOF needs every
    writer closed. A process the job left running behind it still holds the write
    end, so the worker waited on it forever — and one worker runs one job at a
    time, so every later job stayed queued. `timeout_s` did not help: it guards
    the child's exit, which had already happened.

    Asserting only the leaking job's own status would pass, as it did here: the
    job whose status has to be checked is the one queued behind it.

    No `timeout_s`, which is its default and the case with no way out: a timeout
    would eventually have killed the group and freed the pipe.
    """
    with TestClient(app) as client:
        client.post("/jobs", headers=AUTH, json={"code": LEAKS_A_PROCESS})
        behind = client.post("/jobs", headers=AUTH, json={"code": ECHO, "variables": {"x": 1}}).json()["job_id"]

        assert _wait_for_terminal(client, behind, timeout_s=45)["status"] == "succeeded"
        assert client.get("/jobs", headers=AUTH).json()["worker_alive"] is True


def test_stream_carries_log_and_result_events():
    """print() from job code has to reach a watching caller, not just the container log."""
    with TestClient(app) as client:
        job_id = client.post("/jobs", headers=AUTH, json={"code": ECHO, "variables": {"x": 3}}).json()["job_id"]
        _wait_for_terminal(client, job_id)

        with client.websocket_connect(f"/jobs/{job_id}/stream?token={TOKEN}") as ws:
            events = [ws.receive_json() for _ in range(2)]

    kinds = [e["type"] for e in events]
    assert "log" in kinds
    assert any("got 3" in e.get("message", "") for e in events if e["type"] == "log")


def test_stream_rejects_a_bad_token():
    """WebSockets can't use the header dependency, so this path is checked separately."""
    with TestClient(app) as client:
        job_id = client.post("/jobs", headers=AUTH, json={"code": ECHO, "variables": {"x": 1}}).json()["job_id"]
        # noqa: B017 — starlette closes the socket; the exception type varies by version
        with pytest.raises(Exception, match=".*"), client.websocket_connect(f"/jobs/{job_id}/stream?token=wrong") as ws:  # noqa: B017
            ws.receive_json()


def test_unknown_job_id_404s():
    """A typo'd job id must not look like a job that hasn't started."""
    with TestClient(app) as client:
        assert client.get("/jobs/nope", headers=AUTH).status_code == 404
        assert client.post("/jobs/nope/cancel", headers=AUTH).status_code == 404
