"""SlurmJobManager against fake ssh and rsync programs on PATH.

The fakes log their argv, so these tests check the commands the manager actually
builds. Each fake answers the way the cluster would for the few commands the
manager sends. The test writes `sacct.txt` to say what SLURM reports, and plants
`remote/<job id>/` folders to say what a finished job left on the cluster.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from compute_server_base import create_app
from compute_server_base.jobs import Job, JobStatus
from compute_server_base.slurm import SlurmJobManager

REMOTE_ROOT = "/scratch/user/trippr5118/frenkel-hprc"
SIF = "/scratch/user/images/frenkel.sif"
CODE = "def run(x):\n    return {'x': x}\n"

SSH_SHIM = """#!/bin/bash
# Logs its argv, then answers the remote command the way the cluster would.
echo "ssh $*" >> "$FAKE_DIR/calls.log"
if [ -n "$FAKE_SSH_FAIL" ]; then
    echo "Control socket connect(/x): No such file or directory" >&2
    exit 255
fi
cmd="${@: -1}"
case "$cmd" in
    sbatch*)
        n=$(cat "$FAKE_DIR/counter" 2>/dev/null || echo 100)
        n=$((n + 1))
        echo "$n" > "$FAKE_DIR/counter"
        echo "$n"
        ;;
    sacct*)
        cat "$FAKE_DIR/sacct.txt" 2>/dev/null
        ;;
esac
exit 0
"""

RSYNC_SHIM = """#!/bin/bash
# Logs its argv. A pull copies the folder the test planted for that job.
echo "rsync $*" >> "$FAKE_DIR/calls.log"
src="${@: -2:1}"
dst="${@: -1}"
case "$src" in
    *:*)
        id=$(basename "${src#*:}")
        if [ -d "$FAKE_DIR/remote/$id" ]; then
            cp -r "$FAKE_DIR/remote/$id/." "$dst"
        fi
        ;;
esac
exit 0
"""


@pytest.fixture
def fake(tmp_path, monkeypatch) -> Path:
    """Fake ssh and rsync first on PATH, and the HPRC_* settings pointed at them.

    Returns the fake's state folder: calls.log, sacct.txt, counter, remote/.
    """
    state = tmp_path / "fake"
    state.mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in (("ssh", SSH_SHIM), ("rsync", RSYNC_SHIM)):
        shim = bin_dir / name
        shim.write_text(body)
        shim.chmod(0o755)

    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_DIR", str(state))
    monkeypatch.setenv("HPRC_HOST", "grace")
    monkeypatch.setenv("HPRC_REMOTE_ROOT", REMOTE_ROOT)
    monkeypatch.setenv("HPRC_SIF", SIF)
    monkeypatch.setenv("HPRC_POLL_S", "0.05")
    monkeypatch.setenv("JOB_WORKSPACE_ROOT", str(tmp_path / "work"))
    for name in (
        "FAKE_SSH_FAIL",
        "HPRC_PARTITION",
        "HPRC_GRES",
        "HPRC_ACCOUNT",
        "HPRC_CPUS",
        "HPRC_MEM",
        "HPRC_MAX_TIME",
        "HPRC_MAX_INFLIGHT",
        "HPRC_KEEP_REMOTE",
    ):
        monkeypatch.delenv(name, raising=False)
    return state


def calls(fake: Path) -> list[str]:
    log = fake / "calls.log"
    return log.read_text().splitlines() if log.exists() else []


def sbatch_calls(fake: Path) -> list[str]:
    return [line for line in calls(fake) if "sbatch --parsable" in line]


def set_sacct(fake: Path, text: str) -> None:
    (fake / "sacct.txt").write_text(text)


def plant(fake: Path, job_id: str, *, outcome: dict | None = None, slurm_out: str | None = None) -> None:
    """Put what a finished job left on the cluster into the fake's remote folder."""
    folder = fake / "remote" / job_id
    folder.mkdir(parents=True, exist_ok=True)
    if outcome is not None:
        (folder / "outcome.json").write_text(json.dumps(outcome))
    if slurm_out is not None:
        (folder / "slurm.out").write_text(slurm_out)


async def dispatch(mgr: SlurmJobManager, timeout_s: float | None = 5400) -> Job:
    """Submit one job, then run the poll that hands it to SLURM."""
    job = await mgr.submit(CODE, "run", {"x": 1}, None, timeout_s)
    await mgr._poll_once()
    return job


def test_submit_writes_request_and_batch_script_and_touches_no_ssh(fake):
    async def go():
        mgr = SlurmJobManager()
        job = await mgr.submit(CODE, "", {"x": 1}, None, 5400)
        capped = await mgr.submit(CODE, "run", {}, None, 10**6)
        return job, capped

    job, capped = asyncio.run(go())
    assert not (fake / "calls.log").exists()
    assert job.status == JobStatus.QUEUED
    assert job.entrypoint == "run"

    request = json.loads((job.workdir / "request.json").read_text())
    assert request == {
        "job_id": job.id,
        "code": CODE,
        "entrypoint": "run",
        "variables": {"x": 1},
        "workdir": f"{REMOTE_ROOT}/jobs/{job.id}",
        "result_path": f"{REMOTE_ROOT}/jobs/{job.id}/outcome.json",
    }

    script = (job.workdir / "job.sbatch").read_text().splitlines()
    assert f"#SBATCH --job-name=fp-{job.id[:8]}" in script
    assert "#SBATCH --output=slurm.out" in script
    assert "#SBATCH --time=0-01:30:00" in script
    assert "#SBATCH --nodes=1" in script
    assert "#SBATCH --ntasks=1" in script
    assert "#SBATCH --cpus-per-task=8" in script
    assert "#SBATCH --mem=60G" in script
    assert "#SBATCH --partition=gpu" in script
    assert "#SBATCH --gres=gpu:a100:1" in script
    assert not any("--account" in line for line in script)
    assert script[-1] == f'singularity exec --nv "{SIF}" python -m compute_server_base._job_runner < request.json'

    assert "#SBATCH --time=4-00:00:00" in (capped.workdir / "job.sbatch").read_text().splitlines()


def test_first_poll_rsyncs_then_sbatches(fake):
    async def go():
        mgr = SlurmJobManager()
        return mgr, await dispatch(mgr)

    mgr, job = asyncio.run(go())
    lines = calls(fake)
    assert lines[0] == f"ssh -o BatchMode=yes -o ConnectTimeout=20 grace mkdir -p {REMOTE_ROOT}/jobs/{job.id}"
    assert lines[1].startswith("rsync ")
    assert f"{job.id}/" in lines[1]
    assert lines[2].startswith("ssh -o BatchMode=yes -o ConnectTimeout=20 grace")
    assert f"sbatch --parsable --chdir {REMOTE_ROOT}/jobs/{job.id} {REMOTE_ROOT}/jobs/{job.id}/job.sbatch" in lines[2]
    assert job.status == JobStatus.RUNNING
    assert job.slurm_id == "101"
    assert mgr.degraded is None


def test_pending_is_reported_as_running(fake):
    async def go():
        mgr = SlurmJobManager()
        job = await dispatch(mgr)
        set_sacct(fake, "101|PENDING|00:00:00\n")
        await mgr._poll_once()
        return job

    assert asyncio.run(go()).status == JobStatus.RUNNING


def test_completed_with_outcome_succeeds_and_cleans_up_remote(fake):
    async def go():
        mgr = SlurmJobManager()
        job = await dispatch(mgr)
        plant(fake, job.id, outcome={"ok": True, "values": {"x": 1}}, slurm_out="step 1\nstep 2\n")
        set_sacct(fake, "101|COMPLETED|00:10:00\n")
        await mgr._poll_once()
        return job

    job = asyncio.run(go())
    assert job.status == JobStatus.SUCCEEDED
    assert job.values == {"x": 1}
    assert {"type": "result", "values": {"x": 1}} in job.log_history
    assert {"type": "log", "level": "info", "message": "step 1"} in job.log_history
    assert calls(fake)[-1] == f"ssh -o BatchMode=yes -o ConnectTimeout=20 grace rm -rf {REMOTE_ROOT}/jobs/{job.id}"


def test_completed_with_failed_outcome_carries_the_traceback(fake):
    async def go():
        mgr = SlurmJobManager()
        job = await dispatch(mgr)
        plant(fake, job.id, outcome={"ok": False, "error": "boom", "traceback": "tb"})
        set_sacct(fake, "101|COMPLETED|00:10:00\n")
        await mgr._poll_once()
        return job

    job = asyncio.run(go())
    assert job.status == JobStatus.FAILED
    assert job.error == "boom"
    assert job.traceback == "tb"


def test_timeout_without_outcome_uses_the_jobmanager_message(fake):
    async def go():
        mgr = SlurmJobManager()
        job = await dispatch(mgr, timeout_s=5400)
        set_sacct(fake, "101|TIMEOUT|01:30:00\n")
        await mgr._poll_once()
        return job

    job = asyncio.run(go())
    assert job.status == JobStatus.FAILED
    assert job.error == "job exceeded timeout_s=5400s and was killed"


def test_cancel_running_job_sends_scancel_and_settles_as_cancelled(fake):
    async def go():
        mgr = SlurmJobManager()
        job = await dispatch(mgr)
        assert await mgr.cancel(job.id) is True
        set_sacct(fake, "101|CANCELLED by 1|00:00:05\n")
        await mgr._poll_once()
        return job

    job = asyncio.run(go())
    assert any(line.endswith("grace scancel 101") for line in calls(fake))
    assert job.status == JobStatus.CANCELLED


def test_ssh_failure_sets_degraded_and_stops_the_poll(fake, monkeypatch):
    async def go():
        mgr = SlurmJobManager()
        first = await mgr.submit(CODE, "run", {}, None, None)
        second = await mgr.submit(CODE, "run", {}, None, None)

        monkeypatch.setenv("FAKE_SSH_FAIL", "1")
        before = len(calls(fake))
        await mgr._poll_once()
        during = calls(fake)[before:]
        failed_state = (first.status, second.status, mgr.degraded)

        monkeypatch.delenv("FAKE_SSH_FAIL")
        await mgr._poll_once()
        return during, failed_state, mgr.degraded, first

    during, failed_state, degraded_after, first = asyncio.run(go())
    # The mkdir is the first ssh call and it fails; nothing else is tried this poll.
    assert [line.split()[0] for line in during] == ["ssh"]
    assert failed_state == (JobStatus.QUEUED, JobStatus.QUEUED, "Control socket connect(/x): No such file or directory")
    assert degraded_after is None
    assert first.status == JobStatus.RUNNING


def test_max_inflight_holds_the_third_job_until_one_finishes(fake, monkeypatch):
    monkeypatch.setenv("HPRC_MAX_INFLIGHT", "2")

    async def go():
        mgr = SlurmJobManager()
        jobs = [await mgr.submit(CODE, "run", {}, None, None) for _ in range(3)]
        await mgr._poll_once()
        after_first = len(sbatch_calls(fake))

        plant(fake, jobs[0].id, outcome={"ok": True, "values": {}})
        set_sacct(fake, "101|COMPLETED|00:01:00\n102|RUNNING|00:00:10\n")
        await mgr._poll_once()
        after_second = len(sbatch_calls(fake))
        first_state = jobs[0].status

        await mgr._poll_once()
        return after_first, after_second, first_state, jobs[2].status

    after_first, after_second, first_state, third = asyncio.run(go())
    assert after_first == 2
    # The poll that finishes job one refills its slot in the same pass.
    assert after_second == 3
    assert first_state == JobStatus.SUCCEEDED
    assert len(sbatch_calls(fake)) == 3
    assert third == JobStatus.RUNNING


def test_restart_reloads_jobs_and_keeps_polling_their_slurm_ids(fake):
    async def go():
        mgr = SlurmJobManager()
        job = await dispatch(mgr)

        restarted = SlurmJobManager()
        restarted.start()
        restored = restarted.get(job.id)
        set_sacct(fake, "101|RUNNING|00:01:00\n")
        before = len(calls(fake))
        await restarted._poll_once()
        return restored, calls(fake)[before:]

    restored, later = asyncio.run(go())
    assert restored.status == JobStatus.RUNNING
    assert restored.slurm_id == "101"
    assert any("sacct" in line and " -j 101 " in line for line in later)


def test_keep_remote_skips_the_remote_delete_until_discard(fake, monkeypatch):
    monkeypatch.setenv("HPRC_KEEP_REMOTE", "1")

    async def go():
        mgr = SlurmJobManager()
        job = await dispatch(mgr)
        plant(fake, job.id, outcome={"ok": True, "values": {}})
        set_sacct(fake, "101|COMPLETED|00:10:00\n")
        await mgr._poll_once()
        no_remote_delete = not any("rm -rf" in line for line in calls(fake))

        assert mgr.discard_workspace(job.id) is True
        await asyncio.gather(*list(mgr._background))
        return job, no_remote_delete

    job, no_remote_delete = asyncio.run(go())
    assert job.status == JobStatus.SUCCEEDED
    assert no_remote_delete
    assert any(line.endswith(f"rm -rf {REMOTE_ROOT}/jobs/{job.id}") for line in calls(fake))
    assert not job.workdir.exists()


def test_app_serves_the_slurm_manager_and_reports_degraded_and_inflight(fake, monkeypatch):
    monkeypatch.setenv("HPRC_POLL_S", "3600")
    monkeypatch.setenv("COMPUTE_SERVER_TOKEN", "slurm-test-token")
    auth = {"Authorization": "Bearer slurm-test-token"}

    app = create_app(tool_name="stub", manager=SlurmJobManager())
    with TestClient(app) as client:
        job_id = client.post("/jobs", headers=auth, json={"code": CODE, "variables": {"x": 1}}).json()["job_id"]
        listing = client.get("/jobs", headers=auth).json()
        status = client.get(f"/jobs/{job_id}", headers=auth).json()

    assert listing["degraded"] is None
    assert listing["inflight"] == 0
    assert [job["job_id"] for job in listing["jobs"]] == [job_id]
    assert status["status"] == "queued"
