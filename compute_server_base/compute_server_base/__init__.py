"""Shared compute-server template for tool-server instances.

Installed into each tool image. The API is CRUD for jobs: an instance's
`server/main.py` supplies its name and, at most, a startup probe and a route for
its own input format. It does not describe the library the image installs —
`agent.md` and the knowledge files in the instance's own repository do that, and
a consumer fetches them from git at a release tag.
"""

from compute_server_base.app import JobRequest, create_app, release
from compute_server_base.auth import check_ws_token, require_token
from compute_server_base.jobs import TERMINAL_STATUSES, JobManager, JobStatus
from compute_server_base.refusal import Refusal, RefusalReason, Refused, refusal, refuse, stale_agent_detail

__all__ = [
    "TERMINAL_STATUSES",
    "JobManager",
    "JobRequest",
    "JobStatus",
    "Refusal",
    "RefusalReason",
    "Refused",
    "check_ws_token",
    "create_app",
    "refusal",
    "refuse",
    "release",
    "require_token",
    "stale_agent_detail",
]
