"""Generic in-process job queue and executor.

This module is the reusable half of the compute-server-template: it knows
nothing about materials science. A job is a Python source string plus a
dict of input variables; the worker execs the source into a fresh
namespace, calls the named entrypoint function with the variables as
kwargs, and captures whatever dict it returns as the job's result.

Trust model: since this executes arbitrary submitted code with the
container's full privileges, the bearer token is the only boundary. That's
an accepted tradeoff for a LAN-only service shared between trusted
first-party backends (MatFlow, [other project]) — not a public API.

Known v1 limitations, both inherent to running jobs via a thread pool
executor rather than a subprocess:
  - Cancelling a *running* job is best-effort: the underlying thread can't
    be interrupted mid-exec, so cancel only discards its eventual result.
  - A timeout is enforced the same way: past timeout_s the job is marked
    failed, but the thread keeps running in the background until it
    finishes on its own.
"""

from __future__ import annotations

import asyncio
import json
import time
import traceback
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


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
        timeout_s: Soft timeout — see module docstring for what "soft" means here.
    """

    id: str
    code: str
    entrypoint: str
    variables: dict[str, Any]
    correlation_id: str | None
    timeout_s: float | None
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

    def wall_time_s(self) -> float | None:
        """Elapsed run time, or None if the job hasn't started or finished yet."""
        if self.started_at is None or self.finished_at is None:
            return None
        return self.finished_at - self.started_at


class _JobLogWriter:
    """File-like object redirecting a job's stdout/stderr into its log stream."""

    def __init__(self, job: Job, loop: asyncio.AbstractEventLoop, level: str) -> None:
        self._job = job
        self._loop = loop
        self._level = level
        self._buffer = ""

    def write(self, text: str) -> int:
        self._buffer += text
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            if line:
                self._loop.call_soon_threadsafe(self._job.publish_log, self._level, line)
        return len(text)

    def flush(self) -> None:
        pass


def _run_job_code(job: Job, loop: asyncio.AbstractEventLoop) -> dict[str, Any]:
    """Exec the job's source and call its entrypoint. Runs in a worker thread."""
    import contextlib

    stdout_writer = _JobLogWriter(job, loop, "info")
    stderr_writer = _JobLogWriter(job, loop, "error")

    namespace: dict[str, Any] = {}
    with contextlib.redirect_stdout(stdout_writer), contextlib.redirect_stderr(stderr_writer):
        exec(compile(job.code, f"<job:{job.id}>", "exec"), namespace)

        entrypoint = namespace.get(job.entrypoint)
        if entrypoint is None or not callable(entrypoint):
            raise NameError(f"submitted code does not define a callable {job.entrypoint!r} entrypoint")

        result = entrypoint(**job.variables)

    if not isinstance(result, dict):
        raise TypeError(f"entrypoint {job.entrypoint!r} must return a dict, got {type(result).__name__}")
    return result


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
        job = Job(
            id=uuid.uuid4().hex,
            code=code,
            entrypoint=entrypoint or "run",
            variables=variables or {},
            correlation_id=correlation_id,
            timeout_s=timeout_s,
        )
        self._jobs[job.id] = job
        await self._queue.put(job)
        return job

    def get(self, job_id: str) -> Job | None:
        """Look up a job by id, or None if it was never submitted."""
        return self._jobs.get(job_id)

    async def cancel(self, job_id: str) -> bool:
        """Cancel a queued job outright, or flag a running one (best-effort)."""
        job = self._jobs.get(job_id)
        if job is None:
            return False
        if job.status == JobStatus.QUEUED:
            job.status = JobStatus.CANCELLED
            job.publish_terminal({"type": "error", "message": "cancelled before it started running"})
            return True
        if job.status == JobStatus.RUNNING:
            job.cancel_requested = True
            return True
        return False

    async def _worker(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            job = await self._queue.get()
            if job.status == JobStatus.CANCELLED:
                continue

            job.status = JobStatus.RUNNING
            job.started_at = time.monotonic()
            job.publish_log("info", f"job {job.id} started")

            try:
                coro = loop.run_in_executor(None, _run_job_code, job, loop)
                if job.timeout_s:
                    values = await asyncio.wait_for(coro, timeout=job.timeout_s)
                else:
                    values = await coro
            except TimeoutError:
                job.status = JobStatus.FAILED
                job.error = (
                    f"job exceeded timeout_s={job.timeout_s}s (the underlying execution may still be running in the background)"
                )
                job.publish_terminal({"type": "error", "message": job.error})
            except Exception as exc:  # noqa: BLE001 — job code is arbitrary, must not crash the worker
                job.status = JobStatus.FAILED
                job.error = str(exc)
                job.traceback = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
                job.publish_terminal({"type": "error", "message": job.error, "traceback": job.traceback})
            else:
                if job.cancel_requested:
                    job.status = JobStatus.CANCELLED
                    job.publish_terminal({"type": "error", "message": "cancelled"})
                else:
                    try:
                        json.dumps(values)
                    except TypeError as exc:
                        job.status = JobStatus.FAILED
                        job.error = f"entrypoint return value is not JSON-serializable: {exc}"
                        job.publish_terminal({"type": "error", "message": job.error})
                    else:
                        job.values = values
                        job.status = JobStatus.SUCCEEDED
                        job.publish_terminal({"type": "result", "values": values})
            finally:
                job.finished_at = time.monotonic()
