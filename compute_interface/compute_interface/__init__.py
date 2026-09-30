"""Client for compute servers built on compute-server-base.

The consumer half of the protocol `compute_server_base` implements, kept in the
same repository as the servers so the two cannot version apart — which is how
both previous hand-rolled clients ended up on a deprecated endpoint without
anyone noticing.

The protocol is CRUD for jobs. Nothing here asks a server what its tool can do;
that is answered by `agent.md` and the knowledge files in the tool's own
repository, fetched from git at a release tag.
"""

from compute_interface.client import (
    ComputeServerClient,
    JobFailed,
    Refusal,
    Refused,
)

__all__ = [
    "ComputeServerClient",
    "JobFailed",
    "Refusal",
    "Refused",
]
