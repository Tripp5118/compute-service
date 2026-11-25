"""SlurmJobManager's fallback to plain compute servers, against fake ssh/rsync and a real fallback server.

The fallback is a real compute_server_base app with the in-process JobManager, run by uvicorn in
a thread, so the manager talks to it through compute_interface exactly as it would to the Spark.
"""

from __future__ import annotations

import asyncio
import socket
import threading
import time

import pytest
import uvicorn

from compute_server_base import create_app
from compute_server_base.jobs import JobStatus
from compute_server_base.slurm import SlurmJobManager
from tests.test_slurm import calls, dispatch, plant, set_sacct

# Brings in test_slurm's `fake` fixture: fake ssh and rsync on PATH.
pytest_plugins = ["tests.test_slurm"]

SLEEPY = "def run(x, t=0):\n    import time\n    time.sleep(t)\n    open('made.txt', 'w').write('hi')\n    return {'x': x}\n"


@pytest.fixture
def spark(tmp_path, monkeypatch):
    """A real compute server on a free port, playing a Spark frenkel server."""
    monkeypatch.setenv("COMPUTE_SERVER_TOKEN", "test-token-for-pytest")
    monkeypatch.setenv("JOB_WORKSPACE_ROOT", str(tmp_path / "spark-work"))
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(create_app(tool_name="spark"), host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.05)
    monkeypatch.setenv("JOB_WORKSPACE_ROOT", str(tmp_path / "work"))
    yield f"http://127.0.0.1:{port}", server.config.app.state.jobs
    server.should_exit = True
    thread.join(5)


def manager(spark_url: str, **kw) -> SlurmJobManager:
    return SlurmJobManager(fallback_urls=[spark_url], fallback_after_s=0, fallback_pending_s=3600, **kw)


async def settle(mgr: SlurmJobManager, job, seconds: float = 15) -> None:
    deadline = time.monotonic() + seconds
    while job.status not in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED):
        assert time.monotonic() < deadline, f"job still {job.status}"
        await mgr._poll_once()
        await asyncio.sleep(0.1)


def test_no_fallback_configured_keeps_the_old_behaviour(fake, monkeypatch):
    monkeypatch.delenv("HPRC_FALLBACK_URLS", raising=False)

    async def go():
        mgr = SlurmJobManager()
        job = await dispatch(mgr)
        set_sacct(fake, f"{job.slurm_id}|NODE_FAIL|00:01:00\n")
        await mgr._poll_once()
        return job

    job = asyncio.run(go())
    assert job.status == JobStatus.FAILED
    assert "NODE_FAIL" in job.error


def test_cluster_unreachable_sends_a_queued_job_to_the_fallback(fake, spark, monkeypatch):
    url, _ = spark
    monkeypatch.setenv("FAKE_SSH_FAIL", "1")

    async def go():
        mgr = manager(url)
        job = await mgr.submit(SLEEPY, "run", {"x": 7}, None, 600)
        await settle(mgr, job)
        return job

    job = asyncio.run(go())
    assert job.status == JobStatus.SUCCEEDED
    assert job.values == {"x": 7}
    assert job.fallback_url == url
    assert (job.workdir / "made.txt").read_text() == "hi"
    assert not [c for c in calls(fake) if "sbatch" in c]


def test_unreachable_for_less_than_the_threshold_does_not_fall_back(fake, spark, monkeypatch):
    url, _ = spark
    monkeypatch.setenv("FAKE_SSH_FAIL", "1")

    async def go():
        mgr = SlurmJobManager(fallback_urls=[url], fallback_after_s=3600)
        job = await mgr.submit(SLEEPY, "run", {"x": 1}, None, 600)
        await mgr._poll_once()
        await mgr._poll_once()
        return job

    job = asyncio.run(go())
    assert job.status == JobStatus.QUEUED
    assert job.fallback_id is None


def test_a_job_slurm_loses_is_rerun_on_the_fallback(fake, spark):
    url, _ = spark

    async def go():
        mgr = manager(url)
        job = await mgr.submit(SLEEPY, "run", {"x": 3}, None, 600)
        await mgr._poll_once()
        set_sacct(fake, f"{job.slurm_id}|NODE_FAIL|00:01:00\n")
        await settle(mgr, job)
        return job

    job = asyncio.run(go())
    assert job.status == JobStatus.SUCCEEDED
    assert job.values == {"x": 3}
    assert job.fallback_url == url


def test_a_job_whose_code_fails_on_slurm_is_not_rerun(fake, spark):
    url, spark_jobs = spark

    async def go():
        mgr = manager(url)
        job = await dispatch(mgr)
        plant(fake, job.id, outcome={"ok": False, "error": "boom", "traceback": "tb"})
        set_sacct(fake, f"{job.slurm_id}|COMPLETED|00:01:00\n")
        await mgr._poll_once()
        return job

    job = asyncio.run(go())
    assert job.status == JobStatus.FAILED
    assert job.error == "boom"
    assert not job.fallback
    assert spark_jobs.all_jobs() == []


def test_a_timeout_on_slurm_is_not_rerun(fake, spark):
    url, spark_jobs = spark

    async def go():
        mgr = manager(url)
        job = await dispatch(mgr)
        set_sacct(fake, f"{job.slurm_id}|TIMEOUT|01:30:00\n")
        await mgr._poll_once()
        return job

    job = asyncio.run(go())
    assert job.status == JobStatus.FAILED
    assert "timeout" in job.error
    assert spark_jobs.all_jobs() == []


def test_a_job_pending_too_long_is_cancelled_on_slurm_and_rerun(fake, spark):
    url, _ = spark

    async def go():
        mgr = SlurmJobManager(fallback_urls=[url], fallback_after_s=3600, fallback_pending_s=0)
        job = await mgr.submit(SLEEPY, "run", {"x": 5}, None, 600)
        await mgr._poll_once()
        slurm_id = job.slurm_id
        set_sacct(fake, f"{slurm_id}|PENDING|00:00:00\n")
        await settle(mgr, job)
        return job, slurm_id

    job, slurm_id = asyncio.run(go())
    assert job.status == JobStatus.SUCCEEDED
    assert any(f"scancel {slurm_id}" in c for c in calls(fake))


def test_when_the_fallback_copy_wins_the_slurm_copy_is_cancelled_once_reachable(fake, spark, monkeypatch):
    url, _ = spark

    async def go():
        mgr = manager(url)
        job = await mgr.submit(SLEEPY, "run", {"x": 9}, None, 600)
        await mgr._poll_once()
        slurm_id = job.slurm_id
        monkeypatch.setenv("FAKE_SSH_FAIL", "1")
        await settle(mgr, job)
        monkeypatch.delenv("FAKE_SSH_FAIL")
        set_sacct(fake, f"{slurm_id}|RUNNING|00:05:00\n")
        await mgr._poll_once()
        return job, slurm_id

    job, slurm_id = asyncio.run(go())
    assert job.status == JobStatus.SUCCEEDED
    assert job.values == {"x": 9}
    assert any(f"scancel {slurm_id}" in c for c in calls(fake))


def test_when_the_slurm_copy_wins_the_fallback_copy_is_cancelled(fake, spark, monkeypatch):
    url, spark_jobs = spark

    async def go():
        mgr = manager(url)
        job = await mgr.submit(SLEEPY, "run", {"x": 2, "t": 30}, None, 600)
        await mgr._poll_once()
        monkeypatch.setenv("FAKE_SSH_FAIL", "1")
        await mgr._poll_once()
        assert job.fallback_id is not None
        monkeypatch.delenv("FAKE_SSH_FAIL")
        plant(fake, job.id, outcome={"ok": True, "values": {"x": 2}})
        set_sacct(fake, f"{job.slurm_id}|COMPLETED|00:05:00\n")
        await mgr._poll_once()
        await asyncio.sleep(1)
        return job

    job = asyncio.run(go())
    assert job.status == JobStatus.SUCCEEDED
    remote = spark_jobs.get(job.fallback_id)
    assert remote is not None and remote.status == JobStatus.CANCELLED


def test_a_fallback_server_that_forgets_the_job_gets_it_again(fake, spark, monkeypatch):
    url, spark_jobs = spark
    monkeypatch.setenv("FAKE_SSH_FAIL", "1")

    async def go():
        mgr = manager(url)
        job = await mgr.submit(SLEEPY, "run", {"x": 4, "t": 30}, None, 600)
        await mgr._poll_once()
        first = job.fallback_id
        spark_jobs._jobs.pop(first)
        await mgr._poll_once()
        await mgr._poll_once()
        return job, first

    job, first = asyncio.run(go())
    assert job.fallback_id is not None and job.fallback_id != first


def test_fallback_state_survives_a_restart(fake, spark, monkeypatch):
    url, _ = spark
    monkeypatch.setenv("FAKE_SSH_FAIL", "1")

    async def go():
        mgr = manager(url)
        job = await mgr.submit(SLEEPY, "run", {"x": 6, "t": 1}, None, 600)
        await mgr._poll_once()
        restarted = manager(url)
        restarted._load()
        again = restarted.get(job.id)
        await settle(restarted, again)
        return job, again

    job, again = asyncio.run(go())
    assert again.fallback_id == job.fallback_id
    assert again.status == JobStatus.SUCCEEDED


def test_degraded_stays_set_when_a_pass_makes_no_cluster_call(fake, spark, monkeypatch):
    url, _ = spark
    monkeypatch.setenv("FAKE_SSH_FAIL", "1")

    async def go():
        mgr = SlurmJobManager(fallback_urls=[url], fallback_after_s=0)
        job = await mgr.submit(SLEEPY, "run", {"x": 1, "t": 30}, None, 600)
        await mgr._poll_once()
        assert job.fallback_id is not None
        # The job is on the fallback now, so this pass has nothing to send the cluster.
        await mgr._poll_once()
        return mgr.degraded

    assert asyncio.run(go()) is not None
