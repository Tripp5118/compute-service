"""Saying "this instance will not run that" as a value, instead of as a failure.

An agent that gets a traceback cannot tell "this machine cannot do this, and
here is why" from "this broke". Those call for different next actions: the
first means pick a different instance or a different calculation, the second
means retry or report a bug. A refusal therefore travels as structured JSON
from an HTTP route.

`reason` is drawn from a closed set, because the point is that a caller can
branch on it. Every value in that set is something this server can check by
looking, not by judging.

There is deliberately no "precondition not met". A precondition — relax before
elastic constants, equilibrate before measuring — is a scientific judgment, and
a server that starts making those is deciding which calculations are worth
doing, which is the split `docs/execution-servers.md` exists to hold. Those
live in each tool's knowledge base, where an agent reads them and decides.

The reasons that existed when a server described its own operations —
`operation_not_served`, `backend_not_served`, `instance_not_ready`,
`architecture_mismatch` — are gone with that descriptor. A job now either
imports what it needs or fails saying so, which is a job failure and not a
refusal.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class RefusalReason(str, Enum):  # noqa: UP042 — StrEnum needs 3.11; thermocalc's image is 3.10, same as JobStatus
    """Why an instance declined, from a closed set a caller can branch on."""

    AGENT_RELEASE_STALE = "agent_release_stale"


class Refusal(BaseModel):
    """The body a refusal travels in from an HTTP route."""

    # Present and always true so a caller can distinguish a refusal from a job
    # that ran and failed.
    refused: bool = True
    reason: RefusalReason
    detail: str
    # Whatever makes the refusal actionable — the backends that *are* served,
    # the operations that *are* advertised. Free-form because what helps
    # differs per reason, and a caller reads `detail` when it doesn't
    # recognise a key.
    context: dict[str, Any] = Field(default_factory=dict)


class Refused(Exception):  # noqa: N818 — it is a refusal, not an error; the name is the point
    """Raised inside an HTTP route; the handler returns its body as a 422."""

    def __init__(self, refusal: Refusal) -> None:
        """Carry the body so the handler has nothing to reconstruct."""
        super().__init__(refusal.detail)
        self.refusal = refusal


def refusal(reason: RefusalReason, detail: str, **context: Any) -> dict[str, Any]:
    """Build the refusal body returned by an HTTP route.

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


def stale_agent_detail(tool_name: str, agent_release: str, server_release: str) -> str:
    """One wording for the only refusal, so callers receive a stable answer."""
    return (
        f"{tool_name} was built from {server_release}, but this job was submitted by an agent built "
        f"from {agent_release}. Re-fetch agent.md and its knowledge files at {server_release} and "
        f"resubmit — the instructions you are working from may describe a build this is not."
    )
