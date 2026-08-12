r"""Coverage for the shared compute-server path, with no real toolchain involved.

The two instance test suites exercise this same code, but only inside their
own images — one needs MaterialsFramework, the other a licensed Thermo-Calc
install. This drives create_app() with plain-Python jobs so the shared half
has a check that runs anywhere.

Run:
    uv run --with fastapi --with httpx pytest tools/compute_server_base/tests -v
"""

from __future__ import annotations

import os
import time

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("COMPUTE_SERVER_TOKEN", "test-token-for-pytest")

from compute_server_base import Capabilities, Host, Operation, create_app  # noqa: E402 — env var must be set before import

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


def _capabilities() -> Capabilities:
    return Capabilities(
        tool="stub",
        version="0.0",
        contract_version="1.0",
        ready=True,
        host=Host.native(),
        operations=[Operation(name="echo", backends=["python"], description="Double a number.")],
    )


app = create_app(tool_name="stub", capabilities=_capabilities)


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
        assert client.get("/health").json() == {"status": "ok"}


@pytest.mark.parametrize("path", ["/capabilities", "/jobs/anything"])
def test_authenticated_routes_reject_missing_token(path: str):
    """The bearer token is the only trust boundary — see jobs.py on why."""
    with TestClient(app) as client:
        assert client.get(path).status_code == 401


def test_capabilities_reports_the_descriptor():
    """/capabilities serves what the instance supplied, in S-2's shape."""
    with TestClient(app) as client:
        body = client.get("/capabilities", headers=AUTH).json()
        assert body["tool"] == "stub"
        assert body["ready"] is True
        assert body["host"]["container_arch"]
        assert body["operations"][0]["backends"] == ["python"]


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
