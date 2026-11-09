"""Several servers offering one tool, used as if they were one.

`ComputeServerClient("pool://<tool>", token)` or `ComputeServerClient([url, url], token)`
returns a `PooledClient`. A caller's code does not change: `submit` returns a handle, and
`status`, `wait`, `result`, `run`, `files`, `fetch`, `stream`, `cancel` and `discard` take it.

**The pool holds the queue, not the servers.** A server is handed a job only when it has
nothing queued or running, so the pool's own FIFO decides what runs next. That is what lets a
job from a server that went down go to the front: if the servers held the queue, a requeued job
would land behind whatever they already had.

**Only a server going away requeues a job.** A server that stops answering for
`COMPUTE_POOL_DOWN_AFTER_S` (60 s), or reports its worker dead, is marked down and its
unfinished jobs go back to the front of the FIFO. A job that comes back 404 was lost by a
server that restarted, and is requeued the same way. A job whose own code raised, or that hit
its own `timeout_s`, is a result: it would fail the same way anywhere, and rerunning a
day-long job for a typo costs a day.

**Membership is live.** `pool://<tool>` is read from the JSON file named by
`COMPUTE_SERVER_POOLS` — `{"<tool>": ["http://a:8000", {"url": "...", "token_env": "..."}]}` —
and re-read when it changes, so a server added to the file starts taking jobs from a campaign
that is already running. A down server is probed every tick and takes jobs again once it
answers. A job abandoned on it is cancelled there when it returns, in case it was still running.
"""

from __future__ import annotations

import collections
import json
import logging
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

from compute_interface.client import _TERMINAL, ComputeServerClient, Refused

if TYPE_CHECKING:
    from collections.abc import Iterator

logger = logging.getLogger(__name__)

POOLS_ENV = "COMPUTE_SERVER_POOLS"
POOL_SCHEME = "pool://"


def is_pool(base_url: Any) -> bool:
    """Whether a `base_url` names several servers rather than one."""
    return isinstance(base_url, (list, tuple)) or (isinstance(base_url, str) and base_url.startswith(POOL_SCHEME))


@dataclass
class _Job:
    handle: str
    seq: int
    request: dict[str, Any]
    server: str | None = None
    remote_id: str | None = None
    status: str = "queued"
    outcome: dict[str, Any] | None = None
    refusal: Refused | None = None
    refused_by: dict[str, Refused] = field(default_factory=dict)
    history: list[tuple[str, str]] = field(default_factory=list)


@dataclass
class _Server:
    url: str
    client: ComputeServerClient
    up: bool = True
    failing_since: float | None = None
    busy: bool = False


class PooledClient(ComputeServerClient):
    """`ComputeServerClient` over several servers that run the same tool.

    Args:
        base_url: `pool://<tool>`, resolved through `COMPUTE_SERVER_POOLS`, or a list of URLs.
        token: Bearer token for every server that does not name its own `token_env`.
        timeout: Per-request timeout, as for one server.
        agent_release: Sent with every job, as for one server. A job every server refuses makes
            `status`, `wait` and `run` raise `Refused`; one server refusing sends it to another.
    """

    def __init__(
        self,
        base_url: str | list[Any],
        token: str,
        *,
        timeout: float = 30.0,
        agent_release: str | None = None,
    ) -> None:
        """Resolve the members and start the dispatcher thread."""
        self._spec = base_url
        self._token = token
        self._timeout = timeout
        self._agent_release = agent_release
        self._down_after = float(os.environ.get("COMPUTE_POOL_DOWN_AFTER_S", "60"))
        self._tick_s = float(os.environ.get("COMPUTE_POOL_TICK_S", "2"))
        self._lock = threading.RLock()
        self._servers: dict[str, _Server] = {}
        self._fifo: collections.deque[_Job] = collections.deque()
        self._jobs: dict[str, _Job] = {}
        self._abandoned: dict[str, list[str]] = collections.defaultdict(list)
        self._seq = 0
        self._mtime: float | None = None
        self._closed = threading.Event()
        self._refresh_members()
        self._thread = threading.Thread(target=self._loop, name="compute-pool", daemon=True)
        self._thread.start()

    # ---- the caller's side --------------------------------------------------------

    def submit(
        self,
        code: str,
        *,
        entrypoint: str = "run",
        variables: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        timeout_s: float | None = None,
    ) -> str:
        """Queue a job in the pool and return its handle immediately."""
        with self._lock:
            self._seq += 1
            job = _Job(
                handle=f"pool-{uuid.uuid4().hex}",
                seq=self._seq,
                request={
                    "code": code,
                    "entrypoint": entrypoint,
                    "variables": variables,
                    "correlation_id": correlation_id,
                    "timeout_s": timeout_s,
                },
            )
            self._jobs[job.handle] = job
            self._fifo.append(job)
        return job.handle

    def status(self, job_id: str) -> str:
        """queued (in the pool or on a server), running, succeeded, failed or cancelled."""
        job = self._job(job_id)
        if job.refusal is not None:
            raise job.refusal
        return job.status

    def result(self, job_id: str) -> dict[str, Any]:
        """The finished job's result, with `provenance.server_url` naming the server that ran it."""
        job = self._job(job_id)
        if job.refusal is not None:
            raise job.refusal
        if job.outcome is not None:
            return job.outcome
        server, remote = self._placed(job)
        return server.client.result(remote)

    def cancel(self, job_id: str) -> bool:
        """Drop a job still in the pool, or cancel it on the server running it."""
        job = self._job(job_id)
        with self._lock:
            if job in self._fifo:
                self._fifo.remove(job)
                job.status = "cancelled"
                job.outcome = {"values": None, "error": "cancelled before it was dispatched", "files": []}
                return True
        if job.status in _TERMINAL:
            return False
        server, remote = self._placed(job)
        return server.client.cancel(remote)

    def stream(self, job_id: str) -> Iterator[dict[str, Any]]:
        """The log of the server running the job; waits for it to be dispatched first."""
        job = self._job(job_id)
        while job.server is None and job.status not in _TERMINAL and job.refusal is None:
            time.sleep(self._tick_s)
        if job.server is None:
            return iter(())
        server, remote = self._placed(job)
        return server.client.stream(remote)

    def files(self, job_id: str) -> list[dict[str, Any]]:
        """The workspace manifest on the server that ran the job."""
        server, remote = self._placed(self._job(job_id))
        return server.client.files(remote)

    def fetch(self, job_id: str, path: str, dest: str | Path) -> Path:
        """One file out of the workspace on the server that ran the job."""
        server, remote = self._placed(self._job(job_id))
        return server.client.fetch(remote, path, dest)

    def archive(self, job_id: str, dest: str | Path) -> Path:
        """The workspace on the server that ran the job, as one tar.gz."""
        server, remote = self._placed(self._job(job_id))
        return server.client.archive(remote, dest)

    def discard(self, job_id: str) -> None:
        """Delete the workspace on the server that ran the job."""
        server, remote = self._placed(self._job(job_id))
        server.client.discard(remote)

    def health(self) -> dict[str, Any]:
        """Each member's `/health`, and `ok` if any of them answers."""
        members: dict[str, Any] = {}
        for url, server in list(self._servers.items()):
            try:
                members[url] = server.client.health()
            except httpx.HTTPError as exc:
                members[url] = {"status": "unreachable", "error": str(exc)}
        answering = [m for m in members.values() if m.get("status") == "ok"]
        first = answering[0] if answering else {}
        return {
            "status": "ok" if answering else "unreachable",
            "tool": first.get("tool"),
            "release": first.get("release"),
            "servers": members,
        }

    def jobs(self) -> dict[str, Any]:
        """What the pool holds and where each member stands."""
        with self._lock:
            return {
                "worker_alive": any(s.up for s in self._servers.values()),
                "pending_in_pool": len(self._fifo),
                "servers": {
                    url: {"up": s.up, "busy": s.busy, "failing": s.failing_since is not None}
                    for url, s in self._servers.items()
                },
                "jobs": [
                    {"handle": j.handle, "status": j.status, "server": j.server, "requeued": len(j.history)}
                    for j in self._jobs.values()
                ],
            }

    def close(self) -> None:
        """Stop dispatching and close every member's connection pool."""
        self._closed.set()
        self._thread.join(timeout=self._tick_s + 5)
        for server in self._servers.values():
            server.client.close()

    # ---- the dispatcher -------------------------------------------------------------

    def _loop(self) -> None:
        while not self._closed.is_set():
            try:
                self._tick()
            except Exception:  # noqa: BLE001 — the dispatcher must outlive any one bad tick
                logger.exception("compute pool tick failed")
            self._closed.wait(self._tick_s)

    def _tick(self) -> None:
        self._refresh_members()
        now = time.monotonic()
        for server in list(self._servers.values()):
            self._probe(server, now)
        for job in [j for j in self._jobs.values() if j.server and j.status not in _TERMINAL]:
            self._follow(job)
        self._dispatch()

    def _probe(self, server: _Server, now: float) -> None:
        """Mark a member busy, idle, failing or down from one unretried `/jobs`."""
        try:
            payload = server.client._json(server.client._client.get("/jobs"))
            alive = bool(payload.get("worker_alive", True))
        except (httpx.HTTPError, ValueError):
            alive = False
            payload = {}
        if alive:
            server.busy = bool(payload.get("queued") or payload.get("running"))
            server.failing_since = None
            if not server.up:
                server.up = True
                logger.warning("compute pool: %s answers again and takes jobs", server.url)
                for remote in self._abandoned.pop(server.url, []):
                    try:
                        server.client.cancel(remote)
                    except httpx.HTTPError:
                        pass
            return
        if server.failing_since is None:
            server.failing_since = now
        if server.up and now - server.failing_since >= self._down_after:
            server.up = False
            stranded = [j for j in self._jobs.values() if j.server == server.url and j.status not in _TERMINAL]
            logger.warning(
                "compute pool: %s has not answered for %.0f s; %d job(s) go to the front of the queue",
                server.url, now - server.failing_since, len(stranded),
            )
            self._requeue(stranded)

    def _follow(self, job: _Job) -> None:
        server = self._servers.get(job.server or "")
        if server is None or not server.up:
            return
        try:
            status = server.client._json(server.client._client.get(f"/jobs/{job.remote_id}"))["status"]
            if status in _TERMINAL:
                outcome = server.client.result(job.remote_id or "")
                outcome.setdefault("provenance", {})["server_url"] = server.url
                job.outcome = outcome
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                logger.warning("compute pool: %s no longer knows job %s; requeued", server.url, job.remote_id)
                self._requeue([job])
            return
        except httpx.HTTPError:
            return
        job.status = status

    def _requeue(self, jobs: list[_Job]) -> None:
        with self._lock:
            for job in sorted(jobs, key=lambda j: j.seq, reverse=True):
                if job.server and job.remote_id:
                    job.history.append((job.server, job.remote_id))
                    self._abandoned[job.server].append(job.remote_id)
                job.server = job.remote_id = None
                job.status = "queued"
                self._fifo.appendleft(job)

    def _dispatch(self) -> None:
        """Hand the front of the queue to an idle server, until one or the other runs out.

        A refusal belongs to the server that gave it -- a machine on another build refuses what
        its neighbours accept -- so the job is offered to the others first, and is refused for
        good only once every server in the pool has said no.
        """
        while True:
            with self._lock:
                if not self._fifo:
                    return
                job = self._fifo[0]
                if self._servers and set(self._servers) <= set(job.refused_by):
                    self._fifo.popleft()
                    job.refusal = next(reversed(job.refused_by.values()))
                    job.status = "failed"
                    continue
                idle = [
                    s for s in self._servers.values()
                    if s.up and s.failing_since is None and not s.busy and s.url not in job.refused_by
                ]
                if not idle:
                    return
                self._fifo.popleft()
                server = idle[0]
                server.busy = True
            try:
                remote = ComputeServerClient.submit(server.client, **job.request)
            except Refused as exc:
                logger.warning("compute pool: %s refused a job (%s); offering it elsewhere", server.url, exc)
                with self._lock:
                    server.busy = False
                    job.refused_by[server.url] = exc
                    self._fifo.appendleft(job)
                continue
            except httpx.HTTPError as exc:
                logger.warning("compute pool: submit to %s failed (%s); job stays first in line", server.url, exc)
                with self._lock:
                    server.failing_since = server.failing_since or time.monotonic()
                    self._fifo.appendleft(job)
                continue
            job.server, job.remote_id = server.url, remote

    # ---- membership -----------------------------------------------------------------

    def _members(self) -> list[tuple[str, str]]:
        """(url, token) for every member the spec names right now."""
        if isinstance(self._spec, (list, tuple)):
            entries = list(self._spec)
        else:
            name = self._spec[len(POOL_SCHEME):]
            path = os.environ.get(POOLS_ENV)
            if not path:
                raise ValueError(f"{self._spec} needs {POOLS_ENV} to name the file listing its servers")
            pools = json.loads(Path(path).read_text())
            if name not in pools:
                raise KeyError(f"{POOLS_ENV} file {path} has no pool {name!r}; it has {sorted(pools)}")
            entries = list(pools[name])
        members = []
        for entry in entries:
            if isinstance(entry, str):
                members.append((entry.rstrip("/"), self._token))
            else:
                token = os.environ.get(entry["token_env"], "") if entry.get("token_env") else self._token
                members.append((str(entry["url"]).rstrip("/"), token))
        return members

    def _refresh_members(self) -> None:
        if not isinstance(self._spec, (list, tuple)):
            path = Path(os.environ.get(POOLS_ENV, ""))
            mtime = path.stat().st_mtime if path.is_file() else None
            if self._mtime is not None and mtime == self._mtime:
                return
            self._mtime = mtime
        elif self._servers:
            return
        try:
            wanted = dict(self._members())
        except (OSError, ValueError, KeyError):
            if not self._servers:
                raise
            logger.exception("compute pool: could not re-read %s; keeping the current members", POOLS_ENV)
            return
        with self._lock:
            for url in [u for u in self._servers if u not in wanted]:
                gone = self._servers.pop(url)
                logger.warning("compute pool: %s left the pool", url)
                self._requeue([j for j in self._jobs.values() if j.server == url and j.status not in _TERMINAL])
                gone.client.close()
            for url, token in wanted.items():
                if url not in self._servers:
                    self._servers[url] = _Server(
                        url=url,
                        client=ComputeServerClient(
                            url, token, timeout=self._timeout, agent_release=self._agent_release
                        ),
                    )
                    logger.info("compute pool: %s joined", url)

    # ---- plumbing -------------------------------------------------------------------

    def _job(self, handle: str) -> _Job:
        try:
            return self._jobs[handle]
        except KeyError:
            raise KeyError(f"no job {handle!r} in this pool") from None

    def _placed(self, job: _Job) -> tuple[_Server, str]:
        if job.server is None or job.remote_id is None:
            raise RuntimeError(f"job {job.handle} has not been dispatched to a server yet")
        server = self._servers.get(job.server)
        if server is None:
            raise RuntimeError(f"job {job.handle} ran on {job.server}, which has left the pool")
        return server, job.remote_id
