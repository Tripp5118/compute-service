"""Coverage for the import probe every advertisement is filtered through.

`probe_import` is the whole basis of methodology_map.md invariant 7 — if it says
yes for something that cannot run, every consumer's right to trust the
advertisement flatly is gone. The namespace-package case below is not
hypothetical: an empty directory left behind on `sys.path` is what a half-removed
install looks like, and plain `import_module` accepts it.

Run:
    cd compute_server_base && PYTHONPATH=. uv run --with fastapi --with pytest pytest tests/test_capabilities.py -v
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from compute_server_base import Capabilities, Host, probe_import

if TYPE_CHECKING:
    from pathlib import Path


def test_real_module_probes_true() -> None:
    """A module with actual code behind it is available."""
    ok, error = probe_import("json")
    assert ok is True
    assert error is None


def test_absent_module_probes_false_with_a_reason() -> None:
    """The reason is surfaced to operators through the deprecated /mlips shim."""
    ok, error = probe_import("no_such_module_at_all")
    assert ok is False
    assert error is not None
    assert "ModuleNotFoundError" in error


def test_builtin_module_probes_true() -> None:
    """A builtin has no `__file__`, which must not be mistaken for a missing install."""
    ok, error = probe_import("sys")
    assert ok is True, error


def test_empty_directory_on_the_path_probes_false(tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
    """An empty package directory imports as a namespace package but can run nothing."""
    (tmp_path / "pretend_calculator").mkdir()
    monkeypatch.syspath_prepend(str(tmp_path))
    sys.modules.pop("pretend_calculator", None)

    ok, error = probe_import("pretend_calculator")
    assert ok is False, "an empty directory must not count as an installed package"
    assert error is not None
    assert "namespace package" in error


def _caps() -> Capabilities:
    return Capabilities(tool="t", version="1", contract_version="0.1", ready=True, host=Host.native())


def test_source_ref_is_none_when_the_build_supplied_nothing(monkeypatch) -> None:  # noqa: ANN001
    """A consumer must be able to tell "not stamped" from a real commit."""
    monkeypatch.delenv("SOURCE_REF", raising=False)
    assert _caps().source_ref is None


def test_empty_source_ref_is_none_rather_than_empty_string(monkeypatch) -> None:  # noqa: ANN001
    """`--build-arg SOURCE_REF=` with nothing after it must not read as a stamped build."""
    monkeypatch.setenv("SOURCE_REF", "")
    assert _caps().source_ref is None


def test_source_ref_reports_the_commit_the_image_was_built_from(monkeypatch) -> None:  # noqa: ANN001
    """This is the only field that moves when the instance repo changes but the solver does not."""
    monkeypatch.setenv("SOURCE_REF", "0123456789abcdef")
    assert _caps().source_ref == "0123456789abcdef"
