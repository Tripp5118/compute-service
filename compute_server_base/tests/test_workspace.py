"""Coverage for the per-job workspace: the manifest, the fetch routes, the escape guard.

The point of the workspace is tools whose real output is files — a LAMMPS run
writes a log, dumps and a restart, none of which can travel through a JSON
result. These tests use plain Python jobs so they run without any toolchain.

Run:
    uv run --with fastapi --with httpx pytest tools/compute_server_base/tests -v
"""

from __future__ import annotations

import os
import tarfile
import tempfile
import time
from pathlib import Path

from fastapi.testclient import TestClient

os.environ.setdefault("COMPUTE_SERVER_TOKEN", "test-token-for-pytest")
os.environ["JOB_WORKSPACE_ROOT"] = tempfile.mkdtemp(prefix="workspace-test-")

from compute_server_base import Capabilities, Host, create_app  # noqa: E402 — env must be set before import

AUTH = {"Authorization": f"Bearer {os.environ['COMPUTE_SERVER_TOKEN']}"}

# Relative paths only: proving the job's cwd is its own workspace, which is
# what makes wrapping a command-line tool work without rewriting its paths.
WRITES_FILES = """
def run():
    from pathlib import Path
    Path("log.txt").write_text("thermo 1 2 3\\n")
    Path("sub").mkdir()
    Path("sub/dump.bin").write_bytes(b"\\x00\\x01\\x02")
    return {"wrote": 2}
"""

ESCAPES = """
def run():
    from pathlib import Path
    Path("escape").symlink_to("/etc/hostname")
    return {}
"""


def _capabilities() -> Capabilities:
    return Capabilities(tool="stub", version="0.0", contract_version="1.0", ready=True, host=Host.native())


app = create_app(tool_name="stub", capabilities=_capabilities)


def _run(client: TestClient, code: str) -> str:
    job_id = client.post("/jobs", json={"code": code}, headers=AUTH).json()["job_id"]
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if client.get(f"/jobs/{job_id}", headers=AUTH).json()["status"] in ("succeeded", "failed", "cancelled"):
            return job_id
        time.sleep(0.05)
    raise TimeoutError(f"job {job_id} never finished")


def test_job_files_are_listed_and_fetchable():
    """A job's files are reported in the result and fetched separately, never inlined."""
    with TestClient(app) as client:
        job_id = _run(client, WRITES_FILES)

        result = client.get(f"/jobs/{job_id}/result", headers=AUTH).json()
        assert result["status"] == "succeeded"
        assert [f["path"] for f in result["files"]] == ["log.txt", "sub/dump.bin"]

        listing = client.get(f"/jobs/{job_id}/files", headers=AUTH).json()
        assert listing["total_bytes"] == sum(f["size_bytes"] for f in listing["files"])

        assert client.get(f"/jobs/{job_id}/files/log.txt", headers=AUTH).text == "thermo 1 2 3\n"
        assert client.get(f"/jobs/{job_id}/files/sub/dump.bin", headers=AUTH).content == b"\x00\x01\x02"


def test_workspace_is_not_a_way_out_of_the_workspace():
    """A symlink the job planted and a traversal path both resolve to a 404.

    The job already runs arbitrary code, so this guard is not about the job —
    it is about a *caller* being able to read the host through the fetch route.
    """
    with TestClient(app) as client:
        job_id = _run(client, ESCAPES)
        assert client.get(f"/jobs/{job_id}/files/escape", headers=AUTH).status_code == 404
        assert client.get(f"/jobs/{job_id}/files/%2e%2e/%2e%2e/etc/hostname", headers=AUTH).status_code == 404


def test_archive_and_discard():
    """The whole-workspace tarball, and the only thing that reclaims the disk."""
    with TestClient(app) as client:
        job_id = _run(client, WRITES_FILES)

        resp = client.get(f"/jobs/{job_id}/archive", headers=AUTH)
        assert resp.status_code == 200
        archive = Path(tempfile.mkdtemp()) / "out.tar.gz"
        archive.write_bytes(resp.content)
        with tarfile.open(archive) as tar:
            assert f"{job_id}/log.txt" in tar.getnames()

        assert client.delete(f"/jobs/{job_id}/files", headers=AUTH).status_code == 200
        assert client.get(f"/jobs/{job_id}/files", headers=AUTH).json()["files"] == []
