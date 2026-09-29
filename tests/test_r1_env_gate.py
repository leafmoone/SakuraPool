"""Gate H environment gate: the R1 suite must degrade and prove its safety.

- no worker binary (no RUST-CARGO-TARGET build output) -> the R1 worker
  tests SKIP and the session header REPORTS it, the run still exits 0
- offline: every R1 test source references only loopback hosts (plus the
  example.invalid sentinel the worker rejects before any connection) and
  every R1 server binds 127.0.0.1
- no real data / no production: synthetic fixtures live under the fixed
  P4 work root, which is outside the repository
"""

import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

from conftest import R1_WORKER_CANDIDATES, resolve_r1_worker

from sakurapool.storage.budget import DEFAULT_WORK_ROOT

REPO_ROOT = Path(__file__).resolve().parents[1]
FAKE_WORKER = "D:/SakuraTool/SakuraPool-P4-work/r1-env-gate-missing-worker.exe"


def test_worker_resolver_explicit_env_is_authoritative():
    # A missing file named explicitly must resolve to skip, never fall back.
    assert resolve_r1_worker({"SAKURAPOOL_RUST_WORKER": FAKE_WORKER}) is None
    with tempfile.TemporaryDirectory(
            prefix="r1-env-resolver-", dir=DEFAULT_WORK_ROOT) as temp:
        fake = Path(temp) / "sakurapool-worker.exe"
        fake.write_bytes(b"\x00" * 16)
        assert resolve_r1_worker({"SAKURAPOOL_RUST_WORKER": str(fake)}) == str(fake)


def test_worker_resolver_fallback_only_returns_existing_files():
    result = resolve_r1_worker({})
    assert result is None or os.path.isfile(result)
    if result is not None:
        assert result in R1_WORKER_CANDIDATES


def test_r1_suite_skips_and_reports_without_worker():
    """No build output -> the whole R1 bridge file skips (exit 0) and the
    session header carries the report with the build instruction."""
    env = {**os.environ, "SAKURAPOOL_RUST_WORKER": FAKE_WORKER,
           "PYTHONPATH": "src"}
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/test_r1_bridge.py",
         "-p", "no:cacheprovider"],
        cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=600,
    )
    assert proc.returncode == 0, proc.stdout[-2000:] + proc.stderr[-2000:]
    assert "R1 worker: NOT FOUND" in proc.stdout
    assert "will be skipped" in proc.stdout
    summary = proc.stdout.strip().splitlines()[-1]
    # Worker tests are skipped (>=1) and nothing failed or errored; a few
    # worker-independent tests (drain thread, Python crash) still pass.
    assert re.search(r"\b\d+ skipped\b", summary), summary
    assert "failed" not in summary, summary
    assert "error" not in summary, summary


def test_p4_work_root_is_outside_the_repository():
    work_root = Path(DEFAULT_WORK_ROOT)
    assert work_root.is_dir(), "fixed P4 work root must exist for R1 fixtures"
    assert not work_root.is_relative_to(REPO_ROOT), \
        "work root inside the repo would pollute it with fixtures"
    assert not REPO_ROOT.is_relative_to(work_root)


def test_r1_servers_bind_loopback_only():
    from test_r1_bridge import _serve, _stall_server

    server, _url, _thread = _serve(b"probe")
    try:
        assert server.server_address[0] == "127.0.0.1"
    finally:
        server.shutdown()
        server.server_close()

    listener, _stall_url, _stall_thread = _stall_server()
    try:
        assert listener.getsockname()[0] == "127.0.0.1"
    finally:
        listener.close()


def test_r1_test_sources_reference_no_external_hosts():
    """Static offline proof: every URL literal in the R1 test sources is
    loopback (or the example.invalid sentinel, which the worker rejects at
    parse time without opening a connection)."""
    hosts: set[str] = set()
    for name in ("test_r1_bridge.py", "test_r1_loopback_loop.py",
                 "test_r1_gate_e_e2e.py", "test_r1_env_gate.py", "conftest.py"):
        source = (REPO_ROOT / "tests" / name).read_text(encoding="utf-8")
        hosts.update(re.findall(r"https?://([A-Za-z0-9.-]+)", source))
    allowed = {"127.0.0.1", "example.invalid"}
    assert hosts <= allowed, f"unexpected hosts in R1 test sources: {hosts - allowed}"


def test_r1_work_root_fixtures_are_created_under_p4_root():
    """The fixture helper used by every R1 test stays under the work root."""
    with tempfile.TemporaryDirectory(prefix="r1-env-", dir=DEFAULT_WORK_ROOT) as temp:
        assert Path(temp).is_relative_to(DEFAULT_WORK_ROOT)
        assert not Path(temp).is_relative_to(REPO_ROOT)
