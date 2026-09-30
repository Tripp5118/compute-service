"""The consumer half of the compute-server protocol.

Two hand-rolled clients existed against this protocol before this package did
— matflow's `ComputeServerClient` and autoBO's `PotentialRunner` — and both
were still calling a deprecated endpoint months after the servers had moved on.
Neither noticed, because a client that ships from a different repository on a
different cadence has nothing tying it to the contract it speaks. So this lives
next to `compute_server_base`, and the two change in one commit or the drift
comes back.

**The protocol is CRUD for jobs.** Send Python, poll it, read its values and
files. There is nothing here that asks a server what its tool can do, because no
server answers that: `agent.md` and the knowledge files in the tool's own
repository do, fetched from git at a release tag.

**It models nothing the server owns.** `result()` hands back the parsed result;
the only shape this package defines is `Refusal`, and it carries the server's
fields through verbatim without an opinion about what the reasons are.

**A refusal is not a transport error.** `Refused` is the one exception that
means "this machine will not do this, and here is why" — a dead end the caller
reports and works around. Everything else that goes wrong raises
`httpx.HTTPError`, which means "this broke" and is worth a retry or a bug.
Catching those separately is the whole reason the refusal shape exists.

**It holds no cost model and no scheduling logic.** An instrument reports.
"""

from __future__ import annotations

import contextlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit, urlunsplit

import httpx

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = ["ComputeServerClient", "JobFailed", "Refusal", "Refused"]

_TERMINAL = ("succeeded", "failed", "cancelled")


@dataclass(frozen=True)
class Refusal:
    """A server declining a job, carried through exactly as it was sent.

    `reason` is deliberately a plain string rather than an enum: the closed set
    lives in `compute_server_base.refusal.RefusalReason` and copying it here
    would create the second definition this package exists to avoid. Compare it
    against that enum's values, or against the literals.
    """

    reason: str
    detail: str
    context: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> Refusal:
        """Build one from a response body that carries `refused: true`."""
        return cls(
            reason=str(payload.get("reason", "")),
            detail=str(payload.get("detail", "")),
            context=dict(payload.get("context") or {}),
        )


class Refused(Exception):  # noqa: N818 — a refusal is an outcome, not an error; the distinction is the point
    """The server will not run this. Catch this separately from `httpx.HTTPError`."""

    def __init__(self, refusal: Refusal) -> None:
        """Carry the refusal so a caller can branch on `reason` without parsing prose."""
        super().__init__(f"{refusal.reason}: {refusal.detail}")
        self.refusal = refusal

    @property
    def reason(self) -> str:
        """Which of the server's closed set applies."""
        return self.refusal.reason

    @property
    def detail(self) -> str:
        """Prose saying what is wrong and, where it can, what would work instead."""
        return self.refusal.detail

    @property
    def context(self) -> dict[str, Any]:
        """Whatever makes it actionable — the backends that are served, for instance."""
        return self.refusal.context


class JobFailed(Exception):
    """The job ran and raised. A third outcome, distinct from the other two.

    `Refused` means the machine will not run this; `httpx.HTTPError` means the
    call broke. This means the calculation itself failed, which is the caller's
    problem to read — hence the server's traceback on `.job_traceback`.

    Only `run()` raises it. `wait()` returns the failed result as data, because
    a caller that submitted and streamed a job has usually already seen why.
    """

    def __init__(self, job_id: str, result: dict[str, Any]) -> None:
        """Build from the server's result payload."""
        super().__init__(f"job {job_id} failed: {result.get('error')}")
        self.job_id = job_id
        self.result = result
        self.job_traceback = result.get("traceback")


class ComputeServerClient:
    """Talks to one compute server: submit, poll, read, fetch, stream.

    Synchronous, because both jobs it has to do are synchronous: a generated
    campaign calling a truth function in a loop, and an agent that submits and
    then waits. The async half of matflow's old client is not reproduced here
    until something is running that needs it.

    Args:
        base_url: The server's root, e.g. `http://materials-framework:8000`.
        token: Bearer token — a dashboard-issued job token or the static
            `COMPUTE_SERVER_TOKEN`. The server cannot tell the two apart.
        timeout: Per-request timeout in seconds. Not a job timeout; pass
            `timeout_s` to `submit()` for that.
        agent_release: The tool repository release this caller built its
            tool-use agent from, sent with every job. A server built from a
            different release refuses the job rather than run work under
            instructions that have moved on. Leave it None for a caller with no
            agent definition to be stale — a script, a smoke test.
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = 30.0,
        agent_release: str | None = None,
    ) -> None:
        """Open the HTTP connection pool; nothing is contacted until a call is made."""
        self._base = base_url.rstrip("/")
        self._token = token
        self._agent_release = agent_release
        self._client = httpx.Client(
            base_url=self._base,
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout,
        )

    def health(self) -> dict[str, Any]:
        """Liveness, the tool's name, and the release it was built from.

        Unauthenticated — the one call that works without a valid token.
        """
        return self._json(httpx.get(f"{self._base}/health", timeout=10.0))

    # ---- running something ------------------------------------------------------

    def submit(
        self,
        code: str,
        *,
        entrypoint: str = "run",
        variables: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        timeout_s: float | None = None,
    ) -> str:
        """Enqueue source code and return its job id immediately.

        `code` must define a function named by `entrypoint`, called with
        `variables` as keyword arguments; whatever it returns, as long as it is
        JSON-serializable, comes back from `result()`. The job runs with its own
        directory as cwd, so a relative path it writes is reachable through
        `files()` and `fetch()`.

        Raises:
            Refused: The server will not run this — read `.reason`. An
                `agent_release_stale` refusal means this caller's tool-use agent
                was built from a different release than the server; re-fetch the
                definition at the release the refusal names and resubmit.
        """
        return str(
            self._json(
                self._client.post(
                    "/jobs",
                    json={
                        "code": code,
                        "entrypoint": entrypoint,
                        "variables": variables or {},
                        "correlation_id": correlation_id,
                        "timeout_s": timeout_s,
                        "agent_release": self._agent_release,
                    },
                )
            )["job_id"]
        )

    def status(self, job_id: str) -> str:
        """One of queued, running, succeeded, failed, cancelled."""
        return str(self._json(self._client.get(f"/jobs/{job_id}"))["status"])

    def result(self, job_id: str) -> dict[str, Any]:
        """Values, error, file manifest and provenance. 409s while the job is unfinished."""
        return self._json(self._client.get(f"/jobs/{job_id}/result"))

    def wait(self, job_id: str, *, poll_s: float = 1.0, timeout_s: float | None = None) -> dict[str, Any]:
        """Poll until the job reaches a terminal status, then return its result.

        A finished job may still have failed — check `result["error"]`. This
        raises only if the wait itself runs out.

        Raises:
            TimeoutError: If `timeout_s` elapses before the job finishes. The
                job keeps running; cancel it if that is not what you want.
        """
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        while True:
            if self.status(job_id) in _TERMINAL:
                return self.result(job_id)
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"job {job_id} still running after {timeout_s}s")
            time.sleep(poll_s)

    def run(
        self,
        code: str,
        *,
        entrypoint: str = "run",
        variables: dict[str, Any] | None = None,
        timeout_s: float | None = None,
        wait_s: float | None = None,
        poll_s: float = 1.0,
    ) -> dict[str, Any]:
        """Submit code, wait for it, and return what it returned.

        The whole of submit → poll → read for a caller that just wants the
        numbers back and has no use for the live log or the files. Use `submit`
        and `wait` separately when you want to watch the job or fetch what it
        wrote; nothing here reclaims a workspace, so call `discard` if the job
        writes files you do not want kept.

        Args:
            code: Source defining `entrypoint`.
            entrypoint: Function in `code` to call.
            variables: Keyword arguments for it.
            timeout_s: Wall-clock limit enforced by the *server*, which kills
                the job's process group when it expires. Worth setting: without
                it a job that hangs occupies the server's one worker.
            wait_s: How long this call waits before giving up. The job keeps
                running; `TimeoutError` carries its id in the message.
            poll_s: Seconds between status checks.

        Returns:
            The `values` dict the entrypoint returned.

        Raises:
            Refused: The server will not run this — read `.reason`.
            JobFailed: The job ran and raised. Carries the server's traceback.
            TimeoutError: `wait_s` elapsed. The job is still running.
        """
        job_id = self.submit(code, entrypoint=entrypoint, variables=variables, timeout_s=timeout_s)
        outcome = self.wait(job_id, poll_s=poll_s, timeout_s=wait_s)
        if outcome.get("error"):
            raise JobFailed(job_id, outcome)
        return dict(outcome.get("values") or {})

    def jobs(self) -> dict[str, Any]:
        """Every job the server knows about, and whether its queue is moving.

        `worker_alive` false, or one job running far longer than it should be,
        is why nothing else is progressing: a server runs one job at a time, so
        a job that cannot finish holds every job behind it.
        """
        return self._json(self._client.get("/jobs"))

    def cancel(self, job_id: str) -> bool:
        """Best-effort. A queued job is dropped; a running one has its result discarded."""
        payload = self._json(self._client.post(f"/jobs/{job_id}/cancel"))
        return payload.get("status") != "not found"

    def stream(self, job_id: str) -> Iterator[dict[str, Any]]:
        """Yield the job's log events: history replayed first, then live until it ends.

        The only way to read output before a job finishes. LAMMPS relays each
        `lmp` stdout line as it arrives, so a long run is watchable rather than
        opaque; a Python job's prints arrive the same way.

        The generator ends when the server closes the socket, which it does at
        the job's terminal event. Break out early and the socket closes with it.
        """
        # Imported here so a consumer that never streams does not pay for it at
        # import time, and so an install without it fails at the call, not at
        # `import compute_interface`.
        from websockets.sync.client import connect

        scheme, netloc, *_ = urlsplit(self._base)
        ws_url = urlunsplit(("wss" if scheme == "https" else "ws", netloc, f"/jobs/{job_id}/stream", "", ""))
        with connect(ws_url, additional_headers={"Authorization": f"Bearer {self._token}"}) as socket:
            while True:
                try:
                    message = socket.recv()
                except Exception:  # noqa: BLE001 — any close ends the stream; the job's status is the truth
                    return
                with contextlib.suppress(json.JSONDecodeError, TypeError):
                    yield json.loads(message)

    # ---- what the job wrote -----------------------------------------------------

    def files(self, job_id: str) -> list[dict[str, Any]]:
        """The workspace manifest — path, size, mtime. No contents."""
        return list(self._json(self._client.get(f"/jobs/{job_id}/files"))["files"])

    def fetch(self, job_id: str, path: str, dest: str | Path) -> Path:
        """Stream one file out of the workspace to `dest`, which may be a directory."""
        target = Path(dest)
        if target.is_dir():
            target = target / Path(path).name
        target.parent.mkdir(parents=True, exist_ok=True)
        with self._client.stream("GET", f"/jobs/{job_id}/files/{path}", timeout=None) as response:
            self._raise(response, read_first=True)
            with target.open("wb") as handle:
                for chunk in response.iter_bytes():
                    handle.write(chunk)
        return target

    def archive(self, job_id: str, dest: str | Path) -> Path:
        """Stream the whole workspace to `dest` as one tar.gz."""
        target = Path(dest)
        if target.is_dir():
            target = target / f"{job_id}.tar.gz"
        target.parent.mkdir(parents=True, exist_ok=True)
        with self._client.stream("GET", f"/jobs/{job_id}/archive", timeout=None) as response:
            self._raise(response, read_first=True)
            with target.open("wb") as handle:
                for chunk in response.iter_bytes():
                    handle.write(chunk)
        return target

    def discard(self, job_id: str) -> None:
        """Delete the workspace. Nothing else reclaims it — no sweeper runs."""
        self._json(self._client.delete(f"/jobs/{job_id}/files"))

    # ---- plumbing ---------------------------------------------------------------

    def close(self) -> None:
        """Close the connection pool."""
        self._client.close()

    def __enter__(self) -> ComputeServerClient:
        """Usable as a context manager; the pool closes on exit."""
        return self

    def __exit__(self, *_exc: object) -> None:
        """Close the pool."""
        self.close()

    def _json(self, response: httpx.Response) -> dict[str, Any]:
        self._raise(response)
        return dict(response.json())

    @staticmethod
    def _raise(response: httpx.Response, *, read_first: bool = False) -> None:
        """Turn a refusal into `Refused` and anything else broken into `httpx.HTTPError`.

        A refusal arrives as 422 with `refused: true`. It is checked before
        `raise_for_status()` so that "this machine will not do this" never
        reaches a caller wearing the same exception type as a dead socket.
        """
        if response.status_code == 422:
            if read_first:
                response.read()
            with contextlib.suppress(ValueError):
                payload = response.json()
                if isinstance(payload, dict) and payload.get("refused"):
                    raise Refused(Refusal.from_payload(payload))
        response.raise_for_status()
