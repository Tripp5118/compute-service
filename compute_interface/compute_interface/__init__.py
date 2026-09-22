"""Client for compute servers built on compute-server-base.

The consumer half of the protocol `compute_server_base` implements, kept in the
same repository as the servers so the two cannot version apart — which is how
both previous hand-rolled clients ended up on a deprecated endpoint without
anyone noticing.
"""

from compute_interface.client import (
    CONTRACT_MAJOR,
    ComputeServerClient,
    ContractMismatchError,
    JobFailed,
    Refusal,
    Refused,
)

__all__ = [
    "CONTRACT_MAJOR",
    "ComputeServerClient",
    "ContractMismatchError",
    "JobFailed",
    "Refusal",
    "Refused",
]
