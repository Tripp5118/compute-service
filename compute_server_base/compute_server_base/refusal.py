"""Saying "this instance will not run that" as a value, instead of as a failure.

An agent that gets a traceback cannot tell "this machine cannot do this, and
here is why" from "this broke". Those call for different next actions: the
first means pick a different instance or a different calculation, the second
means retry or report a bug. So a refusal travels as data on both faces — the
same JSON body whether it came back from an MCP tool call or from an HTTP
route (`docs/execution-servers.md`, missing item 3).

`reason` is drawn from a closed set, because the point is that a caller can
branch on it. Every value in that set is something this server can check by
looking, not by judging:

- the instance is not ready to run anything
- the operation is not advertised here
- the named backend does not serve the operation asked for
- the compute is running on an architecture it was not built for

There is deliberately no "precondition not met". A precondition — relax before
elastic constants, equilibrate before measuring — is a scientific judgment, and
a server that starts making those is deciding which calculations are worth
doing, which is the split `docs/execution-servers.md` exists to hold. Those
live in each tool's knowledge pack, where an agent reads them and decides.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class RefusalReason(str, Enum):  # noqa: UP042 — StrEnum needs 3.11; thermocalc's image is 3.10, same as JobStatus
    """Why an instance declined, from a closed set a caller can branch on."""

    INSTANCE_NOT_READY = "instance_not_ready"
    OPERATION_NOT_SERVED = "operation_not_served"
    BACKEND_NOT_SERVED = "backend_not_served"
    ARCHITECTURE_MISMATCH = "architecture_mismatch"


class Refusal(BaseModel):
    """The body a refusal travels in, identical over MCP and over HTTP."""

    # Present and always true so a caller can test one key without first
    # knowing which of the two faces the response arrived on, and without
    # having to distinguish a refusal from a job that ran and failed.
    refused: bool = True
    reason: RefusalReason
    detail: str
    # Whatever makes the refusal actionable — the backends that *are* served,
    # the operations that *are* advertised. Free-form because what helps
    # differs per reason, and a caller reads `detail` when it doesn't
    # recognise a key.
    context: dict[str, Any] = Field(default_factory=dict)


class Refused(Exception):  # noqa: N818 — it is a refusal, not an error; the name is the point
    """Raised inside an HTTP route; the handler turns it into the same body MCP returns."""

    def __init__(self, refusal: Refusal) -> None:
        """Carry the body so the handler has nothing to reconstruct."""
        super().__init__(refusal.detail)
        self.refusal = refusal


def refusal(reason: RefusalReason, detail: str, **context: Any) -> dict[str, Any]:
    """Build the refusal body an MCP tool returns.

    Args:
        reason: Which of the closed set applies.
        detail: Prose for a reader — a language model is the reader, so say
            what is wrong and what would work instead.
        **context: Anything that makes it actionable, e.g. `available=[...]`.
    """
    return Refusal(reason=reason, detail=detail, context=context).model_dump(mode="json")


def refuse(reason: RefusalReason, detail: str, **context: Any) -> Refused:
    """Build the exception an HTTP route raises. Same body, 422 on the wire."""
    return Refused(Refusal(reason=reason, detail=detail, context=context))


def not_ready_detail(tool_name: str) -> str:
    """One wording for the commonest refusal, so the two faces cannot drift apart."""
    return f"{tool_name} is not ready to run jobs on this host — code submitted now would fail on import"
