"""Several servers offering one tool, used as if they were one.

`ComputeServerClient("pool://<tool>", token)` or `ComputeServerClient([url, url], token)`
returns a `PooledClient`. A caller's code does not change: `submit` returns a handle, and
`status`, `wait`, `result`, `run`, `files`, `fetch`, `stream`, `cancel` and `discard` take it.

**The servers hold the queues; the pool only places and watches.** `submit` sends the job at
once to the member with the fewest jobs queued or running. After that the pool looks at the job
only when the caller does -- every `status`, and so every poll of `wait`.

**Only a server going away moves a job.** A server that has not answered for
`COMPUTE_POOL_DOWN_AFTER_S` (60 s) has its job resubmitted to another member; a job a restarted
server no longer knows (404) is resubmitted at once. A job whose own code raised, or that hit its
own `timeout_s`, is a result: it would fail the same way anywhere, and rerunning a day-long job
for a typo costs a day. A refusal belongs to the refusing server (a machine on another build
refuses what its neighbours accept), so the job is offered to the others and is refused only
when every member has said no.

**Membership is live.** `pool://<tool>` is read from the JSON file named by
`COMPUTE_SERVER_POOLS` -- `{"<tool>": ["http://a:8000", {"url": "...", "token_env": "..."}]}` --
and re-read whenever it changes, so a server added to the file takes new jobs from a campaign
already running. A server that was down is tried again every time a job is placed.
"""

from __future__ import annotations

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
    request: dict[str, Any]
    server: str | None = None
    remote_id: str | None = None
    status: str = "queued"
    outcome: dict[str, Any] | None = None
    history: list[tuple[str, str]] = field(default_factory=list)


class PooledClient(ComputeServerClient):
    """`ComputeServerClient` over several servers that run the same tool.

    Args:
        base_url: `pool://<tool>`, resolved through `COMPUTE_SERVER_POOLS`, or a list of URLs.
        token: Bearer token for every server that does not name its own `token_env`.
        timeout: Per-request timeout, as for one server.
        agent_release: Sent with every job, as for one server.
    """

    def __init__(
        self,
        base_url: str | list[Any],
        token: str,
        *,
        timeout: float = 30.0,
        agent_release: str | None = None,
    ) -> None:
        """Resolve the members; nothing is contacted until a job is placed."""
        self._spec = base_url
        self._token = token
        self._timeout = timeout
        self._agent_release = agent_release
        self._down_after = float(os.environ.get("COMPUTE_POOL_DOWN_AFTER_S", "60"))
        self._lock = threading.RLock()
        #: Every member a job may still be on. A member removed from the file stays here, so a
        #: job running on it is still readable, but is no longer in `_offered`.
        self._members: dict[str, ComputeServerClient] = {}
        self._offered: list[str] = []
        self._failing_since: dict[str, float] = {}
        self._jobs: dict[str, _Job] = {}
        self._mtime: float | None = None
        self._refresh()

    # ---- placing ----------------------------------------------------------------------

    def submit(
        self,
        code: str,
        *,
        entrypoint: str = "run",
        variables: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        timeout_s: float | None = None,
    ) -> str:
        """Send the job to the least-loaded member now and return its handle.

        Raises:
            Refused: Every member refused it.
            httpx.ConnectError: No member could be reached.
        """
        job = _Job(
            handle=f"pool-{uuid.uuid4().hex}",
            request={
                "code": code,
                "entrypoint": entrypoint,
                "variables": variables,
                "correlation_id": correlation_id,
                "timeout_s": timeout_s,
            },
        )
        self._place(job)
        with self._lock:
            self._jobs[job.handle] = job
        return job.handle

    def _place(self, job: _Job, avoid: str | None = None) -> None:
        """Submit to the member with the shortest queue; on a refusal, to the next one."""
        self._refresh()
        loads = []
        for url in list(self._offered):
            if url == avoid:
                continue
            client = self._members[url]
            try:
                payload = client._json(client._client.get("/jobs"))
            except (httpx.HTTPError, ValueError):
                continue
            if payload.get("worker_alive", True):
                loads.append((int(payload.get("queued", 0)) + int(payload.get("running", 0)), url))
        refusal: Refused | None = None
        for _, url in sorted(loads):
            try:
                remote = ComputeServerClient.submit(self._members[url], **job.request)
            except Refused as exc:
                logger.warning("compute pool: %s refused a job (%s); offering it elsewhere", url, exc)
                refusal = exc
                continue
            except httpx.HTTPError as exc:
                logger.warning("compute pool: submit to %s failed (%s); trying another", url, exc)
                continue
            if job.server and job.remote_id:
                job.history.append((job.server, job.remote_id))
            job.server, job.remote_id, job.status = url, remote, "queued"
            self._failing_since.pop(url, None)
            return
        if refusal is not None:
            raise refusal
        raise httpx.ConnectError(f"no server in {self._spec} could take the job")

    # ---- watching ---------------------------------------------------------------------

    def status(self, job_id: str) -> str:
        """Ask the job's server; move the job if that server has gone."""
        job = self._job(job_id)
        if job.outcome is not None:
            return job.status
        client = self._members[job.server or ""]
        try:
            status = client._json(client._client.get(f"/jobs/{job.remote_id}"))["status"]
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                raise
            logger.warning("compute pool: %s no longer knows job %s; resubmitting", job.server, job.remote_id)
            self._move(job)
            return job.status
        except (httpx.HTTPError, ValueError):
            since = self._failing_since.setdefault(job.server or "", time.monotonic())
            if time.monotonic() - since >= self._down_after:
                logger.warning(
                    "compute pool: %s has not answered for %.0f s; resubmitting job %s elsewhere",
                    job.server, time.monotonic() - since, job.handle,
                )
                self._move(job)
            return job.status
        self._failing_since.pop(job.server or "", None)
        if status in _TERMINAL:
            outcome = client.result(job.remote_id or "")
            outcome.setdefault("provenance", {})["server_url"] = job.server
            job.outcome = outcome
        job.status = status
        return status

    def _move(self, job: _Job) -> None:
        """Resubmit a job its server lost. If nothing can take it now, try again next poll."""
        try:
            self._place(job, avoid=job.server)
        except httpx.ConnectError:
            logger.warning("compute pool: nowhere to move job %s yet", job.handle)

    def result(self, job_id: str) -> dict[str, Any]:
        """The finished job's result, with `provenance.server_url` naming the server that ran it."""
        job = self._job(job_id)
        if job.outcome is not None:
            return job.outcome
        return self._members[job.server or ""].result(job.remote_id or "")

    def cancel(self, job_id: str) -> bool:
        """Cancel the job on the server holding it."""
        job = self._job(job_id)
        return self._members[job.server or ""].cancel(job.remote_id or "")

    def stream(self, job_id: str) -> Iterator[dict[str, Any]]:
        """The log of the server holding the job."""
        job = self._job(job_id)
        return self._members[job.server or ""].stream(job.remote_id or "")

    def files(self, job_id: str) -> list[dict[str, Any]]:
        """The workspace manifest on the server that ran the job."""
        job = self._job(job_id)
        return self._members[job.server or ""].files(job.remote_id or "")

    def fetch(self, job_id: str, path: str, dest: str | Path) -> Path:
        """One file out of the workspace on the server that ran the job."""
        job = self._job(job_id)
        return self._members[job.server or ""].fetch(job.remote_id or "", path, dest)

    def archive(self, job_id: str, dest: str | Path) -> Path:
        """The workspace on the server that ran the job, as one tar.gz."""
        job = self._job(job_id)
        return self._members[job.server or ""].archive(job.remote_id or "", dest)

    def discard(self, job_id: str) -> None:
        """Delete the workspace on the server that ran the job."""
        job = self._job(job_id)
        self._members[job.server or ""].discard(job.remote_id or "")

    def health(self) -> dict[str, Any]:
        """Each member's `/health`, and `ok` if any of them answers."""
        self._refresh()
        members: dict[str, Any] = {}
        for url in list(self._offered):
            try:
                members[url] = self._members[url].health()
            except (httpx.HTTPError, ValueError) as exc:
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
        """Every member's own queue, and where each of this client's jobs is."""
        self._refresh()
        servers: dict[str, Any] = {}
        for url in list(self._offered):
            client = self._members[url]
            try:
                payload = client._json(client._client.get("/jobs"))
                servers[url] = {k: payload.get(k) for k in ("worker_alive", "queued", "running")}
            except (httpx.HTTPError, ValueError) as exc:
                servers[url] = {"worker_alive": False, "error": str(exc)}
        return {
            "worker_alive": any(s.get("worker_alive") for s in servers.values()),
            "servers": servers,
            "jobs": [
                {"handle": j.handle, "status": j.status, "server": j.server, "moved": len(j.history)}
                for j in self._jobs.values()
            ],
        }

    def close(self) -> None:
        """Close every member's connection pool."""
        for client in self._members.values():
            client.close()

    # ---- membership -------------------------------------------------------------------

    def _wanted(self) -> dict[str, str]:
        """{url: token} for every member the spec names right now."""
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
        wanted = {}
        for entry in entries:
            if isinstance(entry, str):
                wanted[entry.rstrip("/")] = self._token
            else:
                token = os.environ.get(entry["token_env"], "") if entry.get("token_env") else self._token
                wanted[str(entry["url"]).rstrip("/")] = token
        return wanted

    def _refresh(self) -> None:
        if isinstance(self._spec, (list, tuple)):
            if self._offered:
                return
        else:
            path = Path(os.environ.get(POOLS_ENV, ""))
            mtime = path.stat().st_mtime if path.is_file() else None
            if self._offered and mtime == self._mtime:
                return
            self._mtime = mtime
        try:
            wanted = self._wanted()
        except (OSError, ValueError, KeyError):
            if not self._offered:
                raise
            logger.exception("compute pool: could not re-read %s; keeping the current members", POOLS_ENV)
            return
        with self._lock:
            for url, token in wanted.items():
                if url not in self._members:
                    self._members[url] = ComputeServerClient(
                        url, token, timeout=self._timeout, agent_release=self._agent_release
                    )
            for url in set(self._offered) - set(wanted):
                logger.warning("compute pool: %s left the pool; it takes no new jobs", url)
            self._offered = list(wanted)

    # ---- plumbing ---------------------------------------------------------------------

    def _job(self, handle: str) -> _Job:
        try:
            return self._jobs[handle]
        except KeyError:
            raise KeyError(f"no job {handle!r} in this pool") from None
