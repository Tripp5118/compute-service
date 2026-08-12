"""Shared compute-server template for spark-lab-stack tool instances.

Installed into each tool image (see any tools/*/Dockerfile). An instance's
`server/main.py` supplies a Capabilities descriptor and its own startup
probing; everything else comes from here.
"""

from compute_server_base.app import JobRequest, create_app
from compute_server_base.auth import check_ws_token, require_token
from compute_server_base.capabilities import (
    Capabilities,
    Host,
    Operation,
    available_backends,
    probe_import,
    under_emulation,
)
from compute_server_base.jobs import TERMINAL_STATUSES, JobManager, JobStatus

__all__ = [
    "TERMINAL_STATUSES",
    "Capabilities",
    "Host",
    "JobManager",
    "JobRequest",
    "JobStatus",
    "Operation",
    "available_backends",
    "check_ws_token",
    "create_app",
    "probe_import",
    "require_token",
    "under_emulation",
]
