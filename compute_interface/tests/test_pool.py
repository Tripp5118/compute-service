"""The pool against real compute servers, including ones that stop and come back.

Run the same way as `test_client.py`.
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
from pathlib import Path
from typing import Any

import pytest

os.environ.setdefault("COMPUTE_SERVER_TOKEN", "test-token-for-pytest")

import uvicorn  # noqa: E402
from compute_server_base import create_app  # noqa: E402
from compute_interface import ComputeServerClient, PooledClient, Refused  # noqa: E402

TOKEN = os.environ["COMPUTE_SERVER_TOKEN"]
SLEEP = "def run(t, tag):\n    import time\n    time.sleep(t)\n    return {'tag': tag}\n"


class Stub:
    """One compute server on a loopback port that can be stopped and started again there."""

    def __init__(self, workspace: Path, release: str | None = None) -> None:
        self.workspace, self.release = workspace, release
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            self.port = int(probe.getsockname()[1])
        self.url = f"http://127.0.0.1:{self.port}"
        self.server: uvicorn.Server | None = None

    def start(self) -> Stub:
        os.environ["JOB_WORKSPACE_ROOT"] = str(self.workspace)
        if self.release:
            os.environ["RELEASE"] = self.release
        else:
            os.environ.pop("RELEASE", None)
        config = uvicorn.Config(create_app(tool_name="stub"), host="127.0.0.1", port=self.port, log_level="error")
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        _until(lambda: self.server.started, 10)
        return self

    def stop(self) -> None:
        if self.server:
            self.server.should_exit = True
            self.thread.join(timeout=10)
            self.server = None

    def remote_ids_oldest_first(self) -> list[str]:
        with ComputeServerClient(self.url, TOKEN) as client:
            return [j["job_id"] for j in reversed(client.jobs()["jobs"])]


def _until(predicate: Any, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while not predicate():
        if time.monotonic() > deadline:
            pytest.fail("condition not reached in time")
        time.sleep(0.05)


@pytest.fixture(autouse=True)
def _fast_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COMPUTE_POOL_TICK_S", "0.1")
    monkeypatch.setenv("COMPUTE_POOL_DOWN_AFTER_S", "0.5")


@pytest.fixture
def two(tmp_path: Path) -> Any:
    a, b = Stub(tmp_path / "a").start(), Stub(tmp_path / "b").start()
    yield a, b
    a.stop()
    b.stop()


def test_a_list_of_urls_gives_a_pool_with_the_same_methods(two: Any) -> None:
    a, b = two
    with ComputeServerClient([a.url, b.url], TOKEN) as client:
        assert isinstance(client, PooledClient)
        assert client.run(SLEEP, variables={"t": 0, "tag": "x"}) == {"tag": "x"}


def test_jobs_spread_over_idle_servers(two: Any) -> None:
    a, b = two
    with ComputeServerClient([a.url, b.url], TOKEN) as client:
        handles = [client.submit(SLEEP, variables={"t": 1, "tag": i}) for i in range(2)]
        servers = {client.wait(h)["provenance"]["server_url"] for h in handles}
    assert servers == {a.url, b.url}


def test_a_job_whose_code_raises_is_a_result_and_is_not_resubmitted(two: Any) -> None:
    a, b = two
    with ComputeServerClient([a.url, b.url], TOKEN) as client:
        outcome = client.wait(client.submit("def run():\n    raise ValueError('typo')\n"))
        assert "typo" in str(outcome["error"])
        assert client.jobs()["jobs"][0]["requeued"] == 0
    assert len(a.remote_ids_oldest_first()) + len(b.remote_ids_oldest_first()) == 1


def test_a_job_on_a_server_that_goes_down_runs_next_on_another(two: Any) -> None:
    a, b = two
    with ComputeServerClient([a.url, b.url], TOKEN) as client:
        first = client.submit(SLEEP, variables={"t": 3, "tag": "first"})
        second = client.submit(SLEEP, variables={"t": 1.5, "tag": "second"})
        _until(lambda: client.status(first) == "running" and client.status(second) == "running", 10)
        third = client.submit(SLEEP, variables={"t": 0, "tag": "third"})

        lost = client._jobs[first]
        doomed, survivor = (a, b) if lost.server == a.url else (b, a)
        doomed.stop()

        moved = client.wait(first, timeout_s=30)
        assert moved["values"] == {"tag": "first"}
        assert moved["provenance"]["server_url"] == survivor.url
        client.wait(third, timeout_s=30)

        # The requeued job went ahead of the one that was waiting in the pool.
        order = survivor.remote_ids_oldest_first()
        assert order.index(client._jobs[first].remote_id) < order.index(client._jobs[third].remote_id)


def test_a_server_that_comes_back_takes_jobs_again(two: Any) -> None:
    a, b = two
    with ComputeServerClient([a.url, b.url], TOKEN) as client:
        a.stop()
        _until(lambda: not client.jobs()["servers"][a.url]["up"], 10)
        a.start()
        _until(lambda: client.jobs()["servers"][a.url]["up"], 10)
        handles = [client.submit(SLEEP, variables={"t": 1, "tag": i}) for i in range(2)]
        assert {client.wait(h)["provenance"]["server_url"] for h in handles} == {a.url, b.url}


def test_a_server_added_to_the_pools_file_joins_a_running_client(two: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    a, b = two
    pools = tmp_path / "pools.json"
    pools.write_text(json.dumps({"stub": [a.url]}))
    monkeypatch.setenv("COMPUTE_SERVER_POOLS", str(pools))
    with ComputeServerClient("pool://stub", TOKEN) as client:
        busy = client.submit(SLEEP, variables={"t": 2, "tag": "busy"})
        _until(lambda: client.status(busy) == "running", 10)
        pools.write_text(json.dumps({"stub": [a.url, {"url": b.url}]}))
        os.utime(pools, (time.time() + 5, time.time() + 5))
        joined = client.wait(client.submit(SLEEP, variables={"t": 0, "tag": "new"}), timeout_s=10)
    assert joined["provenance"]["server_url"] == b.url


def test_a_refusal_reaches_the_caller_through_wait(tmp_path: Path) -> None:
    released = Stub(tmp_path / "r", release="v2.0.0").start()
    try:
        with ComputeServerClient([released.url], TOKEN, agent_release="v1.0.0") as client:
            handle = client.submit("def run():\n    return {}\n")
            with pytest.raises(Refused) as caught:
                client.wait(handle, timeout_s=10)
        assert caught.value.reason == "agent_release_stale"
    finally:
        released.stop()


def test_a_job_sent_without_a_timeout_gets_the_servers_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COMPUTE_JOB_TIMEOUT_DEFAULT_S", "86400")
    server = Stub(tmp_path / "t").start()
    try:
        with ComputeServerClient(server.url, TOKEN) as client:
            client.wait(client.submit("def run():\n    return {}\n"))
            assert client.jobs()["jobs"][0]["timeout_s"] == 86400
    finally:
        server.stop()


def test_a_refusal_from_one_server_sends_the_job_to_another(two: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """A machine on another build refuses what its neighbours accept.

    Both stubs share one process, and a server reads RELEASE per request, so the older build's
    refusal is made explicit here rather than by giving the two servers different releases.
    """
    from compute_interface import Refusal
    from compute_interface import pool as pool_module

    a, b = two
    real_submit = pool_module.ComputeServerClient.submit

    def old_build_on_a(client: Any, *args: Any, **kwargs: Any) -> str:
        if client._base == a.url:
            raise Refused(Refusal("agent_release_stale", "built from v1.0.0"))
        return real_submit(client, *args, **kwargs)

    monkeypatch.setattr(pool_module.ComputeServerClient, "submit", old_build_on_a)
    with ComputeServerClient([a.url, b.url], TOKEN) as client:
        accepted = client.wait(client.submit("def run():\n    return {'ok': 1}\n"), timeout_s=10)
        assert accepted["provenance"]["server_url"] == b.url

    with ComputeServerClient([a.url], TOKEN) as client, pytest.raises(Refused):
        client.wait(client.submit("def run():\n    return {}\n"), timeout_s=10)
