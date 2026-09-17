"""Child process that runs exactly one job's code, then exits.

Job code runs here, in its own process, rather than in a thread of the server
process. A Python thread cannot be interrupted, so when execution lived in the
server's thread pool a `timeout_s` only marked the job failed and left its
compute running until the container was restarted: one materials-framework job
held 19 cores and 44 GB for 19 hours after its result had been discarded as
timed out. A process can be killed, so `timeout_s` and `cancel` mean what they
say.

The request arrives as one JSON object on stdin. The outcome is written to the
file named in it rather than to stdout, because stdout belongs to the job — it
is the live log stream, and job code prints whatever it likes to it.
"""

from __future__ import annotations

import json
import sys
import traceback
from pathlib import Path
from typing import Any


def _outcome(request: dict[str, Any]) -> str:
    namespace: dict[str, Any] = {"WORKDIR": request["workdir"]}
    entrypoint_name = request["entrypoint"]
    try:
        exec(compile(request["code"], f"<job:{request['job_id']}>", "exec"), namespace)  # noqa: S102 — running submitted code is this server's purpose

        entrypoint = namespace.get(entrypoint_name)
        if entrypoint is None or not callable(entrypoint):
            raise NameError(f"submitted code does not define a callable {entrypoint_name!r} entrypoint")

        values = entrypoint(**request["variables"])
    except BaseException as exc:  # noqa: BLE001 — job code is arbitrary; every failure is a job outcome, not a crash
        return json.dumps({"ok": False, "error": str(exc), "traceback": traceback.format_exc()})

    if not isinstance(values, dict):
        return json.dumps(
            {"ok": False, "error": f"entrypoint {entrypoint_name!r} must return a dict, got {type(values).__name__}"}
        )
    try:
        return json.dumps({"ok": True, "values": values})
    except TypeError as exc:
        return json.dumps({"ok": False, "error": f"entrypoint return value is not JSON-serializable: {exc}"})


def main() -> None:
    """Read one job request from stdin, run it, write the outcome to its result file."""
    request = json.loads(sys.stdin.read())
    outcome = _outcome(request)
    sys.stdout.flush()
    Path(request["result_path"]).write_text(outcome)


if __name__ == "__main__":
    main()
