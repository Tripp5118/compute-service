"""Run jobs as SLURM batch jobs on a remote cluster, over an already-open SSH master connection.

Same public surface as jobs.JobManager, so app.py's routes only take the manager
as an argument. A job's Python is the same child process (`_job_runner`), but it
runs as the body of a batch script submitted with `sbatch` rather than as a local
subprocess. Every cluster interaction is an `ssh` or `rsync` call that reuses the
ControlMaster connection in ~/.ssh/config. Nothing here authenticates: the cluster
asks for Duo on each new login, so every call runs with BatchMode=yes and fails
at once instead of prompting, and one failure stops the poll (see `_poll_once`).
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import shlex
import shutil
import time
import traceback
import uuid
from dataclasses import fields
from pathlib import Path
from typing import Any

from compute_server_base.jobs import Job, JobStatus, default_timeout_s, workspace_root

SSH_OPTS = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=20"]
_COMMAND_TIMEOUT_S = 120
_LOG_TAIL_LINES = 200
# A job missing from sacct for more than this many polls is declared lost.
_MAX_MISSING_POLLS = 3
_TERMINAL_SLURM_STATES = frozenset(
    {"COMPLETED", "FAILED", "TIMEOUT", "CANCELLED", "OUT_OF_MEMORY", "NODE_FAIL", "PREEMPTED", "BOOT_FAIL", "DEADLINE"}
)

_SBATCH_TEMPLATE = """\
#!/bin/bash
#SBATCH --job-name=fp-{short_id}
#SBATCH --output=slurm.out
#SBATCH --time={time}
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem={mem}
#SBATCH --partition={partition}
#SBATCH --gres={gres}
{account}
singularity exec --nv "{sif}" python -m compute_server_base._job_runner < request.json
"""


class _Unreachable(Exception):
    """An ssh or rsync call exited non-zero. The message is its last stderr line."""


def _setting(value: Any, env_name: str, default: str | None = None) -> str:
    """A keyword override if given, else the environment variable, else the default."""
    if value is not None:
        return str(value)
    env = os.environ.get(env_name, "").strip()
    if env:
        return env
    if default is None:
        raise RuntimeError(f"{env_name} must be set for SlurmJobManager")
    return default


def _slurm_time_seconds(text: str) -> int:
    """Seconds in a SLURM time limit such as `4-00:00:00` or `01:30:00`."""
    days, _, clock = text.rpartition("-")
    hours, minutes, seconds = (int(part) for part in clock.split(":"))
    return int(days or 0) * 86400 + hours * 3600 + minutes * 60 + seconds


def _last_line(text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return lines[-1] if lines else ""


def _parse_sacct(text: str) -> dict[str, str]:
    """Map each SLURM job id to its state's first word, so `CANCELLED by 123` reads as CANCELLED."""
    states: dict[str, str] = {}
    for line in text.splitlines():
        parts = line.split("|")
        if len(parts) >= 2 and parts[1].split():
            states[parts[0].strip()] = parts[1].split()[0]
    return states


def _tail(path: Path, lines: int) -> list[str]:
    if not path.is_file():
        return []
    return path.read_text(errors="replace").splitlines()[-lines:]


def _read_outcome(path: Path) -> dict[str, Any] | None:
    """The job's outcome.json, or None if it was never written or is unreadable."""
    if not path.is_file() or path.stat().st_size == 0:
        return None
    try:
        outcome = json.loads(path.read_text())
    except ValueError:
        return None
    return outcome if isinstance(outcome, dict) else None


class SlurmJobManager:
    """Runs each submitted job as a SLURM batch job, polled over SSH.

    Attributes:
        degraded: The stderr line of the last ssh or rsync call that failed, or
            None after a poll that reached the cluster. Shown on GET /jobs.
        host: ssh config alias of the cluster login node.
        remote_root: Absolute cluster directory; jobs live under `jobs/<id>/`.
    """

    def __init__(
        self,
        *,
        host: str | None = None,
        remote_root: str | None = None,
        sif: str | None = None,
        partition: str | None = None,
        gres: str | None = None,
        account: str | None = None,
        cpus: int | None = None,
        mem: str | None = None,
        max_time: str | None = None,
        max_inflight: int | None = None,
        poll_s: float | None = None,
        keep_remote: bool | None = None,
    ) -> None:
        """Read the HPRC_* settings once. Keyword arguments override the environment.

        Raises:
            RuntimeError: HPRC_HOST, HPRC_REMOTE_ROOT or HPRC_SIF is not set.
        """
        self.host = _setting(host, "HPRC_HOST")
        self.remote_root = _setting(remote_root, "HPRC_REMOTE_ROOT").rstrip("/")
        self.sif = _setting(sif, "HPRC_SIF")
        self.partition = _setting(partition, "HPRC_PARTITION", "gpu")
        self.gres = _setting(gres, "HPRC_GRES", "gpu:a100:1")
        self.account = _setting(account, "HPRC_ACCOUNT", "")
        self.cpus = int(_setting(cpus, "HPRC_CPUS", "8"))
        self.mem = _setting(mem, "HPRC_MEM", "60G")
        self.max_time_s = _slurm_time_seconds(_setting(max_time, "HPRC_MAX_TIME", "4-00:00:00"))
        self.max_inflight = int(_setting(max_inflight, "HPRC_MAX_INFLIGHT", "20"))
        self.poll_s = float(_setting(poll_s, "HPRC_POLL_S", "60"))
        keep = _setting(None if keep_remote is None else int(keep_remote), "HPRC_KEEP_REMOTE", "0")
        self.keep_remote = keep == "1"

        self.degraded: str | None = None
        self._root = workspace_root()
        self._state_file = self._root / "jobs.json"
        self._jobs: dict[str, Job] = {}
        # Poll count per job id for jobs sacct has no line for; in memory only.
        self._missing: dict[str, int] = {}
        self._poll_task: asyncio.Task | None = None
        self._background: set[asyncio.Task] = set()

    def start(self) -> None:
        """Reload saved jobs and start the poll loop. Call from inside the running event loop."""
        self._load()
        self._poll_task = asyncio.create_task(self._poll_forever())

    async def submit(
        self,
        code: str,
        entrypoint: str,
        variables: dict[str, Any],
        correlation_id: str | None,
        timeout_s: float | None,
    ) -> Job:
        """Write the job's request and batch script locally; the next poll sends them to the cluster.

        Makes no ssh call, so a submit never waits on a login.
        """
        job_id = uuid.uuid4().hex
        workdir = self._root / job_id
        workdir.mkdir(parents=True)
        job = Job(
            id=job_id,
            code=code,
            entrypoint=entrypoint or "run",
            variables=variables or {},
            correlation_id=correlation_id,
            timeout_s=timeout_s if timeout_s is not None else default_timeout_s(),
            workdir=workdir,
        )
        remote = self._remote_dir(job_id)
        request = {
            "job_id": job_id,
            "code": code,
            "entrypoint": job.entrypoint,
            "variables": job.variables,
            "workdir": remote,
            "result_path": f"{remote}/outcome.json",
        }
        (workdir / "request.json").write_text(json.dumps(request))
        (workdir / "job.sbatch").write_text(
            _SBATCH_TEMPLATE.format(
                short_id=job_id[:8],
                time=self._sbatch_time(job.timeout_s),
                cpus=self.cpus,
                mem=self.mem,
                partition=self.partition,
                gres=self.gres,
                account=f"#SBATCH --account={self.account}" if self.account else "",
                sif=self.sif,
            )
        )
        self._jobs[job_id] = job
        job.publish_log("info", f"queued for {self.host}")
        self._save()
        return job

    def get(self, job_id: str) -> Job | None:
        """Look up a job by id, or None if it was never submitted."""
        return self._jobs.get(job_id)

    async def cancel(self, job_id: str) -> bool:
        """Cancel a queued job locally, or send scancel for a running one.

        A running job is marked CANCELLED by the next poll that sees SLURM's
        final state. If scancel fails, the failure is logged on the job and calling
        cancel again sends it again.
        """
        job = self._jobs.get(job_id)
        if job is None:
            return False
        if job.status == JobStatus.QUEUED:
            job.status = JobStatus.CANCELLED
            job.publish_terminal({"type": "error", "message": "cancelled before it started running"})
            self._save()
            return True
        if job.status == JobStatus.RUNNING:
            job.cancel_requested = True
            rc, _, err = await self._run(self._ssh_argv(["scancel", job.slurm_id]))
            if rc != 0:
                job.publish_log("error", f"scancel {job.slurm_id} failed: {_last_line(err) or f'exit {rc}'}")
            self._save()
            return True
        return False

    def all_jobs(self) -> list[Job]:
        """Every job this manager knows about, newest first."""
        return sorted(self._jobs.values(), key=lambda job: job.created_at, reverse=True)

    @property
    def inflight(self) -> int:
        """How many jobs are on SLURM now, counting PENDING ones as running."""
        return sum(1 for job in self._jobs.values() if job.status == JobStatus.RUNNING)

    def worker_alive(self) -> bool:
        """Whether the poll loop is still running. If False, nothing is being sent or collected."""
        return self._poll_task is not None and not self._poll_task.done()

    def discard_workspace(self, job_id: str) -> bool:
        """Delete a job's local folder, and its remote folder if KEEP_REMOTE kept it.

        Synchronous, like JobManager's, because the route calls it without awaiting.
        The remote delete runs as a background task; a failure is reported in
        `degraded`.
        """
        job = self._jobs.get(job_id)
        if job is None or job.workdir is None:
            return False
        shutil.rmtree(job.workdir, ignore_errors=True)
        if self.keep_remote:
            task = asyncio.get_running_loop().create_task(self._remove_remote(job_id))
            self._background.add(task)
            task.add_done_callback(self._background.discard)
        return True

    async def _remove_remote(self, job_id: str) -> None:
        rc, _, err = await self._run(self._ssh_argv(["rm", "-rf", self._remote_dir(job_id)]))
        if rc != 0:
            self.degraded = _last_line(err) or f"ssh exited {rc}"

    async def _poll_forever(self) -> None:
        while True:
            await asyncio.sleep(self.poll_s)
            try:
                await self._poll_once()
            except Exception:  # noqa: BLE001 — the loop must survive any one poll's failure
                traceback.print_exc()

    async def _poll_once(self) -> None:
        """One pass: collect finished jobs, then send queued ones while under MAX_INFLIGHT.

        The first failed ssh or rsync call ends the pass and sets `degraded`. The
        next pass tries again. A pass that reaches the cluster clears `degraded`.
        """
        try:
            # Collect first, so a slot a finished job frees is refilled in the same pass.
            await self._collect()
            await self._submit_queued()
        except _Unreachable as exc:
            self.degraded = str(exc)
        else:
            self.degraded = None

    async def _submit_queued(self) -> None:
        queued = sorted(
            (job for job in self._jobs.values() if job.status == JobStatus.QUEUED),
            key=lambda job: job.created_at,
        )
        for job in queued:
            if self.inflight >= self.max_inflight:
                return
            remote = self._remote_dir(job.id)
            # The cluster's rsync (3.1.3) predates --mkpath.
            await self._ssh(["mkdir", "-p", remote])
            await self._rsync(f"{job.workdir}/", f"{self.host}:{remote}/")
            out = await self._ssh(["sbatch", "--parsable", "--chdir", remote, f"{remote}/job.sbatch"])
            # `--parsable` prints `<id>` or `<id>;<cluster>`.
            job.slurm_id = out.strip().splitlines()[-1].split(";")[0]
            job.status = JobStatus.RUNNING
            job.started_at = time.monotonic()
            job.publish_log("info", f"submitted to SLURM as job {job.slurm_id} on {self.host}")
            self._save()

    async def _collect(self) -> None:
        running = [job for job in self._jobs.values() if job.status == JobStatus.RUNNING]
        if not running:
            return
        ids = ",".join(job.slurm_id for job in running)
        out = await self._ssh(["sacct", "-X", "-n", "-P", "-j", ids, "-o", "JobID,State,Elapsed"])
        states = _parse_sacct(out)
        for job in running:
            state = states.get(job.slurm_id)
            if state is None:
                self._missing[job.id] = self._missing.get(job.id, 0) + 1
                if self._missing[job.id] > _MAX_MISSING_POLLS:
                    message = f"slurm lost job {job.id}"
                    self._settle(job, JobStatus.FAILED, {"type": "error", "message": message}, error=message)
                continue
            self._missing.pop(job.id, None)
            if state in _TERMINAL_SLURM_STATES:
                await self._finish(job, state)

    async def _finish(self, job: Job, state: str) -> None:
        """Pull a job's folder back from the cluster and record its outcome."""
        remote = self._remote_dir(job.id)
        await self._rsync(f"{self.host}:{remote}/", f"{job.workdir}/")
        for line in _tail(job.workdir / "slurm.out", _LOG_TAIL_LINES):
            job.publish_log("info", line)
        outcome = _read_outcome(job.workdir / "outcome.json")

        if job.cancel_requested:
            self._settle(job, JobStatus.CANCELLED, {"type": "error", "message": "cancelled"})
        elif outcome is not None and outcome.get("ok"):
            values = outcome.get("values", {})
            self._settle(job, JobStatus.SUCCEEDED, {"type": "result", "values": values}, values=values)
        elif outcome is not None:
            error = str(outcome.get("error", "job reported a failure"))
            event: dict[str, Any] = {"type": "error", "message": error}
            if outcome.get("traceback"):
                event["traceback"] = outcome["traceback"]
            self._settle(job, JobStatus.FAILED, event, error=error, job_traceback=outcome.get("traceback"))
        elif state == "TIMEOUT":
            message = f"job exceeded timeout_s={job.timeout_s}s and was killed"
            self._settle(job, JobStatus.FAILED, {"type": "error", "message": message}, error=message)
        elif state == "CANCELLED":
            self._settle(job, JobStatus.CANCELLED, {"type": "error", "message": "cancelled"})
        else:
            message = f"slurm job {job.slurm_id} ended {state} without reporting a result"
            self._settle(job, JobStatus.FAILED, {"type": "error", "message": message}, error=message)

        if not self.keep_remote:
            await self._ssh(["rm", "-rf", remote])

    def _settle(
        self,
        job: Job,
        status: JobStatus,
        event: dict[str, Any],
        *,
        values: dict[str, Any] | None = None,
        error: str | None = None,
        job_traceback: str | None = None,
    ) -> None:
        job.status = status
        job.values = values
        job.error = error
        job.traceback = job_traceback
        job.finished_at = time.monotonic()
        job.publish_terminal(event)
        self._missing.pop(job.id, None)
        self._save()

    def _remote_dir(self, job_id: str) -> str:
        return f"{self.remote_root}/jobs/{job_id}"

    def _sbatch_time(self, timeout_s: float | None) -> str:
        """`--time` for a job: its timeout rounded up to whole minutes, capped at HPRC_MAX_TIME."""
        if timeout_s is None:
            seconds = self.max_time_s
        else:
            seconds = min(math.ceil(timeout_s / 60) * 60, self.max_time_s)
        days, rest = divmod(int(seconds), 86400)
        hours, rest = divmod(rest, 3600)
        minutes, secs = divmod(rest, 60)
        return f"{days}-{hours:02}:{minutes:02}:{secs:02}"

    def _ssh_argv(self, remote_command: list[str]) -> list[str]:
        # One string for the remote side: ssh joins its arguments with spaces, and
        # shlex keeps a path with a `$` or a space from being split or expanded there.
        return ["ssh", *SSH_OPTS, self.host, shlex.join(remote_command)]

    async def _ssh(self, remote_command: list[str]) -> str:
        rc, out, err = await self._run(self._ssh_argv(remote_command))
        if rc != 0:
            raise _Unreachable(_last_line(err) or f"ssh exited {rc}")
        return out

    async def _rsync(self, source: str, dest: str) -> None:
        rc, _, err = await self._run(
            ["rsync", "-a", "-e", " ".join(["ssh", *SSH_OPTS]), source, dest]
        )
        if rc != 0:
            raise _Unreachable(_last_line(err) or f"rsync exited {rc}")

    async def _run(self, args: list[str]) -> tuple[int, str, str]:
        """Run one command with no shell. The only place ssh and rsync are launched."""
        try:
            proc = await asyncio.create_subprocess_exec(
                *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
        except OSError as exc:
            return 127, "", str(exc)
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout=_COMMAND_TIMEOUT_S)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return 124, "", f"timed out after {_COMMAND_TIMEOUT_S}s"
        return proc.returncode, out.decode(errors="replace"), err.decode(errors="replace")

    def _save(self) -> None:
        """Rewrite jobs.json through a temp file, so a crash mid-write leaves the old one intact."""
        records = []
        for job in self._jobs.values():
            record = {f.name: getattr(job, f.name) for f in fields(Job) if f.name != "subscribers"}
            record["status"] = job.status.value
            record["workdir"] = str(job.workdir) if job.workdir is not None else None
            records.append(record)
        tmp = self._state_file.with_name(self._state_file.name + ".tmp")
        tmp.write_text(json.dumps(records))
        os.replace(tmp, self._state_file)

    def _load(self) -> None:
        if not self._state_file.is_file():
            return
        for record in json.loads(self._state_file.read_text()):
            record["status"] = JobStatus(record["status"])
            record["workdir"] = Path(record["workdir"]) if record["workdir"] else None
            job = Job(**record)
            self._jobs[job.id] = job
