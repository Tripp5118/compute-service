"""The one shape every compute-server instance answers "what can you do?" in.

Operations, not calculators. A backend is listed under each operation it
actually supports and nowhere else, because non-uniform capability is the
normal case: MEGNet in the materials-framework image is a formation-energy
predictor with no `.relax()` at all, which is why matflow `67a418c` had to
exclude it from the relax node by hand. A contract keyed on "calculator"
reproduces that bug in every consumer; keying on operation makes it
unrepresentable.

`description` on an Operation is read by a language model — autoBO's tool
agents are the consumers, and it is what they decide from. Write it as prose
for a reader, not as a field name.

See docs/infra-cleanup-2026-08.md S-2.
"""

from __future__ import annotations

import importlib
import platform
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class Host(BaseModel):
    """Whether this instance's compute can actually execute on this machine.

    Kept separate from `ready` because "wrong architecture" and "not installed"
    are different failures with different fixes, and a caller picking between
    instances wants to see which one it hit.
    """

    container_arch: str
    solver_arch: str | None = None
    # None, not False, when there is no solver to compare against — absence of
    # an engine is a different failure from a mismatched one.
    arch_match: bool | None = None
    under_emulation: bool = False

    @classmethod
    def native(cls) -> Host:
        """Facts for a tool whose compute is the container itself — no separate solver binary."""
        arch = platform.machine()
        return cls(container_arch=arch, solver_arch=arch, arch_match=True, under_emulation=under_emulation())


class Operation(BaseModel):
    """One thing a caller can invoke, and which backends can serve it."""

    name: str
    backends: list[str] = Field(default_factory=list)
    inputs: dict[str, str] = Field(default_factory=dict)
    returns: dict[str, str] = Field(default_factory=dict)
    description: str = ""


class Capabilities(BaseModel):
    """A compute server's identity, health, and invocable operations."""

    tool: str
    version: str
    # Carried over from the /manifest it replaces: each PROTOCOL.md tells
    # integrators to refuse a major version they don't understand, and dropping
    # the field would quietly take that away from them.
    contract_version: str
    ready: bool
    host: Host
    operations: list[Operation] = Field(default_factory=list)


def under_emulation() -> bool:
    """Best-effort detection of qemu-user, which reports the *guest* architecture.

    Under binfmt_misc the qemu interpreter is mapped into the process, so
    platform.machine() returns the emulated architecture and every naive arch
    check passes on an image that cannot run one instruction of its solver.
    Heuristic, not a guarantee.
    """
    try:
        maps = Path("/proc/self/maps").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    return "qemu" in maps


def probe_import(module_path: str) -> tuple[bool, str | None]:
    """Whether a module actually imports here, so capability is measured rather than claimed."""
    try:
        importlib.import_module(module_path)
    except Exception as exc:  # noqa: BLE001 — reporting import health, not handling one specific error
        return False, f"{type(exc).__name__}: {exc}"
    return True, None


def available_backends(registry: list[dict[str, Any]], operation: str) -> list[str]:
    """Backends in `registry` that serve `operation` and whose probe module imports.

    This is the startup-reconciliation rule in its smallest form: an operation
    advertises only what this image can actually run, so a consumer can trust
    the advertisement flatly instead of holding a precedence rule between a
    document and an endpoint.
    """
    return [entry["id"] for entry in registry if entry.get("operation") == operation and probe_import(entry["probe_module"])[0]]
