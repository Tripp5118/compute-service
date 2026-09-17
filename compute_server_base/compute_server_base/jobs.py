"""Generic in-process job queue and executor.

This module is the reusable half of the compute-server-template: it knows
nothing about materials science. A job is a Python source string plus a
dict of input variables; the worker execs the source into a fresh
namespace, calls the named entrypoint function with the variables as
kwargs, and captures whatever dict it returns as the job's result.

Each job also owns a directory on disk (`workspace_root()/<job_id>`), made the
process's cwd while its code runs. For several tools the real output *is*
files rather than a return value — a LAMMPS run writes a log, dump files, a
restart — and those are routinely large enough that carrying them through the
JSON result would be absurd. So the result stays small and carries a manifest
of what was written; the files themselves are fetched over the /jobs/{id}/files
routes in app.py. Nothing prunes the workspace automatically; see
JobManager.discard_workspace.

Trust model: since this executes arbitrary submitted code with the
container's full privileges, the bearer token is the only boundary. That's
an accepted tradeoff for a LAN-only service shared between trusted
first-party backends (MatFlow, [other project]) — not a public API.

Each job runs in a child process (`_job_runner.py`), in its own session, and
`timeout_s` and `cancel` kill that session's process group. Until 2026-09-04
execution lived in a thread pool thread instead, which meant neither could stop
anything: a thread cannot be interrupted, so a timed-out job was marked failed
and went on computing until the container was restarted. One did, for 19 hours,
holding 19 cores and 44 GB. The process group rather than the process alone
because job code may spawn its own subprocesses — LAMMPS jobs run `lmp` under
`Popen`, and killing only the Python child would orphan the solver.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import signal
import sys
import tempfile
import time
import traceback
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

# Each instance's compose file backs this with a volume. Overridable so the
# server still runs outside a container.
_DEFAULT_WORKSPACE_ROOT = "/work/jobs"

# How long the child's pipes are still read after the child itself has exited.
# Only a leaked process holds them open that late, and its output belongs to a
# job that has already finished.
_PUMP_GRACE_S = 5.0

# How often the child is checked for having exited. One job runs at a time, so
# this is one wakeup per interval for the whole server.
_EXIT_POLL_S = 0.05


def workspace_root() -> Path:
    """Directory holding one subdirectory per job.

    Falls back to a temp directory when the configured root cannot be created,
    so a bare `uvicorn` run or a test session gets a working server rather than
    a failure at first submit.
    """
    root = Path(os.environ.get("JOB_WORKSPACE_ROOT", _DEFAULT_WORKSPACE_ROOT))
    try:
        root.mkdir(parents=True, exist_ok=True)
    except OSError:
        root = Path(tempfile.gettempdir()) / "compute-server-jobs"
        root.mkdir(parents=True, exist_ok=True)
    return root


class JobStatus(str, Enum):  # noqa: UP042 — StrEnum needs 3.11; thermocalc's image is 3.10, see below
    """Lifecycle states a Job moves through, queued to one of three terminal states.

    `str, Enum` rather than `StrEnum`: this package is shared with the
    thermocalc image, which is pinned to Python 3.10 for TC-Python and has no
    `enum.StrEnum`. Callers read `.value`, so the two behave identically here.
    """

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_STATUSES = (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED)


@dataclass
class Job:
    """One submitted job and its accumulated state.

    Attributes:
        id: Server-generated job id.
        code: Python source submitted by the caller.
        entrypoint: Name of the function `code` must define; called as
            entrypoint(**variables).
        variables: Input variable bindings passed to the entrypoint as kwargs.
        correlation_id: Caller-supplied id (e.g. MatFlow's task_id) for
            matching this job back to the right subscriber.
        timeout_s: Wall-clock limit. On expiry the job's process group is killed.
        workdir: This job's private directory, and its cwd while it runs.
            Anything it writes with a relative path lands here.
        pgid: Process-group id of the child running this job, while it runs.
            `cancel()` needs it to kill something. The group id rather than the
            process handle because the group outlives the child it was named
            for, and killing the group is what reaches a solver the job spawned.
    """

    id: str
    code: str
    entrypoint: str
    variables: dict[str, Any]
    correlation_id: str | None
    timeout_s: float | None
    workdir: Path | None = None
    pgid: int | None = None
    status: JobStatus = JobStatus.QUEUED
    created_at: float = field(default_factory=time.monotonic)
    started_at: float | None = None
    finished_at: float | None = None
    values: dict[str, Any] | None = None
    error: str | None = None
    traceback: str | None = None
    cancel_requested: bool = False
    log_history: list[dict[str, Any]] = field(default_factory=list)
    subscribers: list[asyncio.Queue] = field(default_factory=list)

    def publish_log(self, level: str, message: str) -> None:
        """Append a log event and fan it out to any live WS subscribers.

        Must only be called from the event loop thread (the worker thread
        schedules this via loop.call_soon_threadsafe).
        """
        event = {"type": "log", "level": level, "message": message}
        self.log_history.append(event)
        for q in self.subscribers:
            q.put_nowait(event)

    def publish_terminal(self, event: dict[str, Any]) -> None:
        """Append the terminal (result/error) event and close out subscribers."""
        self.log_history.append(event)
        for q in self.subscribers:
            q.put_nowait(event)
            q.put_nowait(None)

    def subscribe(self) -> tuple[list[dict[str, Any]], asyncio.Queue]:
        """Atomically snapshot history and register for new events.

        Runs on the event loop thread with no `await` inside it, so it can't
        race with publish_log/publish_terminal (also event-loop-thread-only).
        """
        snapshot = list(self.log_history)
        queue: asyncio.Queue = asyncio.Queue()
        self.subscribers.append(queue)
        return snapshot, queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        """Remove a queue previously returned by subscribe()."""
        if queue in self.subscribers:
            self.subscribers.remove(queue)

    def files(self) -> list[dict[str, Any]]:
        """Everything the job wrote, as {path, size_bytes, modified} entries.

        Path-sorted rather than time-sorted: a consumer reading a manifest
        wants `log.lammps` in the same position on every run.
        """
        if self.workdir is None or not self.workdir.is_dir():
            return []
        entries = []
        for path in sorted(self.workdir.rglob("*")):
            if not path.is_file():
                continue
            stat = path.stat()
            entries.append(
                {
                    "path": path.relative_to(self.workdir).as_posix(),
                    "size_bytes": stat.st_size,
                    "modified": stat.st_mtime,
                }
            )
        return entries

    def wall_time_s(self) -> float | None:
        """Elapsed run time, or None if the job hasn't started or finished yet."""
        if self.started_at is None or self.finished_at is None:
            return None
        return self.finished_at - self.started_at


class JobFailed(Exception):
    """The job's code raised, or its process died. Carries the child's traceback."""

    def __init__(self, message: str, job_traceback: str | None = None) -> None:
        super().__init__(message)
        self.job_traceback = job_traceback


def kill_process_group(pgid: int | None) -> None:
    """SIGKILL a job's whole session, so a solver it spawned dies with it.

    No SIGTERM first: by the time this is called the job's outcome is already
    settled, so nothing is waiting on a graceful exit.

    Takes the group id, not the process handle: it is also called *after* the
    child has been reaped, when `os.getpgid` would raise for the dead leader
    while the group's surviving members still hold cores, memory and pipes.
    """
    if pgid is None:
        return
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(pgid, signal.SIGKILL)


async def _pump(stream: asyncio.StreamReader, job: Job, level: str) -> None:
    """Publish the child's output a line at a time as it arrives.

    Chunked rather than `readline()` because a job that prints one enormous
    line — a whole array, say — would otherwise trip StreamReader's limit and
    lose the rest of its output.
    """
    buffer = ""
    while True:
        chunk = await stream.read(8192)
        if not chunk:
            break
        buffer += chunk.decode("utf-8", errors="replace")
        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            if line:
                job.publish_log(level, line)
    if buffer.strip():
        job.publish_log(level, buffer)


async def _wait_for_exit(process: asyncio.subprocess.Process) -> int:
    """Wait until the child itself exits, whatever is still holding its pipes.

    Deliberately not `process.wait()`: asyncio resolves that only once the
    process has exited *and* every pipe has disconnected, and a pipe stays
    connected while any process holds the write end. So a job that returns while
    leaving a process running behind it — inheriting stdout, as every child does
    — makes `process.wait()` outlive the child indefinitely, and with `timeout_s`
    unset (its default) nothing ever interrupted that. The worker is the queue's
    only consumer, so the whole queue stopped there.

    Polls `returncode`, which the child watcher sets when it reaps the process,
    before any pipe bookkeeping.
    """
    while process.returncode is None:
        await asyncio.sleep(_EXIT_POLL_S)
    return process.returncode


async def _run_job(job: Job) -> dict[str, Any]:
    """Run the job's code in a child process and return the values it produced.

    Raises:
        asyncio.TimeoutError: The job outlived `timeout_s` and was killed.
        JobFailed: The job's code raised, or its process died without reporting.
    """
    handle, result_path = tempfile.mkstemp(prefix=f"job-{job.id}-", suffix=".json")
    os.close(handle)
    request = json.dumps(
        {
            "job_id": job.id,
            "code": job.code,
            "entrypoint": job.entrypoint,
            "variables": job.variables,
            # Job code that names its output files relatively writes them into
            # the workspace without being told to: the child's cwd is the
            # workspace, and WORKDIR names it for code that would rather be
            # explicit. The old in-process path did this with os.chdir, which
            # is process-global and was only ever safe by luck.
            "workdir": str(job.workdir) if job.workdir is not None else None,
            "result_path": result_path,
        }
    )
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "compute_server_base._job_runner",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(job.workdir) if job.workdir is not None else None,
            # Its own session, so one kill reaches anything the job spawned.
            start_new_session=True,
            # Otherwise the child block-buffers into the pipe and the live log
            # stream arrives all at once when the job ends.
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        # start_new_session made the child its own session leader, so its pid is
        # the group id of everything it goes on to spawn.
        job.pgid = process.pid
        pumps = asyncio.gather(
            _pump(process.stdout, job, "info"),
            _pump(process.stderr, job, "error"),
        )
        try:
            process.stdin.write(request.encode())
            await process.stdin.drain()
            process.stdin.close()
            # asyncio.TimeoutError, not the builtin: they are the same class only
            # from 3.11, and this package still runs on thermocalc's 3.10 image.
            await asyncio.wait_for(_wait_for_exit(process), timeout=job.timeout_s)
        finally:
            # Kill the group on *every* path out, not just timeout and cancel. A
            # job that returns while leaving a process running behind it — a
            # Popen it never waited on, a pool it never joined — otherwise leaves
            # that process holding cores and memory nobody is waiting for, the
            # same class of leak as the 19-hour runaway, and holding this pipe.
            kill_process_group(job.pgid)
            # Bounded, because _pump waits for EOF and EOF needs every writer
            # closed. A process that escaped the group above still holds the
            # write end, and waiting for it stalled this worker — and therefore
            # every job queued behind it — permanently. `timeout_s` did not help:
            # it guards the child's exit, which had already happened.
            try:
                await asyncio.wait_for(pumps, timeout=_PUMP_GRACE_S)
            except asyncio.TimeoutError:
                job.publish_log("error", "stopped reading job output: a process it spawned still holds the log pipe")

        outcome_text = Path(result_path).read_text() if Path(result_path).stat().st_size else ""
        if not outcome_text:
            raise JobFailed(f"job process exited with code {process.returncode} without reporting a result")
        outcome = json.loads(outcome_text)
        if not outcome["ok"]:
            raise JobFailed(outcome["error"], outcome.get("traceback"))
        return outcome["values"]
    finally:
        job.pgid = None
        Path(result_path).unlink(missing_ok=True)


class JobManager:
    """Owns the queue, the single worker, and the job registry."""

    def __init__(self) -> None:
        """Set up empty state; the queue itself is created in start()."""
        # Created in start(), not here: it must bind to whichever event loop
        # is actually running at startup, not whatever loop happens to exist
        # (or not) at module-import time.
        self._queue: asyncio.Queue[Job] | None = None
        self._jobs: dict[str, Job] = {}
        self._worker_task: asyncio.Task | None = None

    def start(self) -> None:
        """Create the queue against the running loop and launch the worker."""
        self._queue = asyncio.Queue()
        self._worker_task = asyncio.create_task(self._worker())

    async def submit(
        self,
        code: str,
        entrypoint: str,
        variables: dict[str, Any],
        correlation_id: str | None,
        timeout_s: float | None,
    ) -> Job:
        """Register a new job and enqueue it for the worker."""
        job_id = uuid.uuid4().hex
        workdir = workspace_root() / job_id
        workdir.mkdir(parents=True, exist_ok=True)
        job = Job(
            id=job_id,
            code=code,
            entrypoint=entrypoint or "run",
            variables=variables or {},
            correlation_id=correlation_id,
            timeout_s=timeout_s,
            workdir=workdir,
        )
        self._jobs[job.id] = job
        await self._queue.put(job)
        return job

    def get(self, job_id: str) -> Job | None:
        """Look up a job by id, or None if it was never submitted."""
        return self._jobs.get(job_id)

    async def cancel(self, job_id: str) -> bool:
        """Cancel a queued job outright, or kill a running one."""
        job = self._jobs.get(job_id)
        if job is None:
            return False
        if job.status == JobStatus.QUEUED:
            job.status = JobStatus.CANCELLED
            job.publish_terminal({"type": "error", "message": "cancelled before it started running"})
            return True
        if job.status == JobStatus.RUNNING:
            job.cancel_requested = True
            kill_process_group(job.pgid)
            return True
        return False

    def all_jobs(self) -> list[Job]:
        """Every job this process knows about, newest first.

        The queue had no window onto it, so "my job has been queued for an hour"
        was a report nobody could check against anything.
        """
        return sorted(self._jobs.values(), key=lambda job: job.created_at, reverse=True)

    def worker_alive(self) -> bool:
        """Whether the one consumer of the queue is still running.

        If this is False, every queued job stays queued forever. It is the first
        thing to look at when nothing is progressing.
        """
        return self._worker_task is not None and not self._worker_task.done()

    def discard_workspace(self, job_id: str) -> bool:
        """Delete a job's files once the caller has what it wants.

        ponytail: no TTL sweeper. Retention is the caller's call because only
        the caller knows whether a 40 GB trajectory is still wanted; add a
        sweeper when a disk actually fills.
        """
        job = self._jobs.get(job_id)
        if job is None or job.workdir is None:
            return False
        shutil.rmtree(job.workdir, ignore_errors=True)
        return True

    async def _worker(self) -> None:
        while True:
            job = await self._queue.get()
            try:
                await self._run_one(job)
            except Exception:  # noqa: BLE001 — see below
                # Nothing may end this loop. It is the queue's only consumer, so
                # an exception escaping it leaves every later job queued forever,
                # with the traceback buried in a Task nobody awaits.
                traceback.print_exc()

    async def _run_one(self, job: Job) -> None:
        if job.status == JobStatus.CANCELLED:
            return

        job.status = JobStatus.RUNNING
        job.started_at = time.monotonic()
        job.publish_log("info", f"job {job.id} started")

        try:
            values = await _run_job(job)
        except asyncio.TimeoutError:
            self._fail(job, f"job exceeded timeout_s={job.timeout_s}s and was killed")
        except JobFailed as exc:
            self._fail(job, str(exc), exc.job_traceback)
        except Exception as exc:  # noqa: BLE001 — a failure to launch is this job's outcome, not the worker's
            self._fail(job, str(exc), "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
        else:
            if job.cancel_requested:
                job.status = JobStatus.CANCELLED
                job.publish_terminal({"type": "error", "message": "cancelled"})
            else:
                job.values = values
                job.status = JobStatus.SUCCEEDED
                job.publish_terminal({"type": "result", "values": values})
        finally:
            job.finished_at = time.monotonic()

    @staticmethod
    def _fail(job: Job, message: str, job_traceback: str | None = None) -> None:
        """Record a terminal failure — or a cancellation, if that is what killed it.

        A cancelled job's process dies mid-run and reports nothing, which is
        indistinguishable from a crash except by `cancel_requested`.
        """
        if job.cancel_requested:
            job.status = JobStatus.CANCELLED
            job.publish_terminal({"type": "error", "message": "cancelled"})
            return
        job.status = JobStatus.FAILED
        job.error = message
        job.traceback = job_traceback
        event = {"type": "error", "message": message}
        if job_traceback:
            event["traceback"] = job_traceback
        job.publish_terminal(event)
