"""Offline loopback closed loop: scan -> P2 durable v4 -> P3 compile/query ->
budget-gated Rust fetch with byte-level agreement.

Everything runs on synthetic data under the fixed P4 work root and local
loopback HTTP. No external network, no real repository access.
"""

import hashlib
import json
import os
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from test_runtime import build_p2_index

from sakurapool.runtime.compiler import compile_runtime
from sakurapool.runtime.inventory import load_p2_inventory
from sakurapool.runtime.query import RuntimeQuerySpec
from sakurapool.runtime.snapshot import RuntimeSnapshot
from sakurapool.storage.budget import DEFAULT_WORK_ROOT, MIB, BudgetLedger
from sakurapool.storage.rust_bridge import RustWorker, RustWorkerError


def _worker_binary() -> str | None:
    """Explicit binary resolution for tests (the bridge itself has no fallback)."""
    explicit = os.environ.get("SAKURAPOOL_RUST_WORKER")
    if explicit:
        return explicit if os.path.isfile(explicit) else None
    for cand in (
        "D:/SakuraTool/SakuraPool-P4-work/rust-target/release/sakurapool-worker.exe",
        "D:/SakuraTool/SakuraPool-P4-work/rust-target/debug/sakurapool-worker.exe",
    ):
        if os.path.isfile(cand):
            return cand
    return None


_WORKER_BINARY = _worker_binary()

WORKER_JOB_BUDGET = {
    "body": 4 * 1024 * 1024,
    "disk": 4 * 1024 * 1024,
    "inflight": 8 * 1024 * 1024,
    "attempts": 5,
}

requires_worker = pytest.mark.skipif(
    _WORKER_BINARY is None, reason="sakurapool-worker binary not built")


@pytest.fixture
def work_root():
    # Offline budget roots and fixtures must live under the fixed P4 work root.
    with tempfile.TemporaryDirectory(prefix="r1-loop-", dir=DEFAULT_WORK_ROOT) as temp:
        yield Path(temp)


class _RangeHandler(BaseHTTPRequestHandler):
    payload: bytes

    def log_message(self, *args: object) -> None:  # silence
        return None

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        data = type(self).payload
        start, end = 0, len(data) - 1
        range_header = self.headers.get("Range")
        if range_header:
            spec = range_header.removeprefix("bytes=")
            start = int(spec.split("-")[0])
            end = int(spec.split("-")[1])
        part = data[start : end + 1]
        self.send_response(206)
        self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        self.send_header("Content-Length", str(len(part)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(part)


def _serve(payload: bytes) -> tuple[ThreadingHTTPServer, str, threading.Thread]:
    _RangeHandler.payload = payload
    server = ThreadingHTTPServer(("127.0.0.1", 0), _RangeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_address[1]}/a.tar"
    return server, url, thread


@requires_worker
def test_scan_p2_p3_rust_fetch_bytes_agree(work_root: Path) -> None:
    image = bytes((i * 17 + 3) % 251 for i in range(128 * 1024))
    members = {
        "1.jpg": image,
        "1.json": b'{"tags": ["solo"]}',
        "2.jpg": b"second",
        "2.json": b'{"tags": []}',
    }
    index = build_p2_index(work_root, "r1", {"a.tar": members})
    tar_path = work_root / "src-r1" / "a.tar"
    tar_bytes = tar_path.read_bytes()
    expected = hashlib.sha256(tar_bytes).hexdigest()

    # P2 durable v4 declared the real input bytes.
    declared = json.loads((index / "INPUT.json").read_bytes())["inputs"]["a.tar"]["sha256"]
    assert declared == expected

    # P3 compile + query + location resolution.
    runtime_root = work_root / "runtime-r1"
    summary = compile_runtime(load_p2_inventory(index), runtime_root)
    assert summary.rid_count == 2
    with RuntimeSnapshot.open(runtime_root) as rt:
        rids = rt.query(RuntimeQuerySpec(all_tags=[("tags", "solo")])).limit(10)
        assert rids == [0]
        assert rt.location(rids[0])["image_size"] == len(image)

    # Budget-gated Rust fetch over loopback: one durable reservation before the
    # request, output/temp + body + IPC all covered.
    (work_root / "ledger").mkdir()
    ledger = BudgetLedger(
        work_root / "ledger",
        _offline_test=True,
        _test_limits={"body": 2 * MIB, "inflight": 4 * MIB, "attempts": 5},
    )
    server, url, _thread = _serve(tar_bytes)
    try:
        with RustWorker(_WORKER_BINARY, job_budget=WORKER_JOB_BUDGET) as worker:
            reply = worker.fetch_range_gated(
                url, 0, len(tar_bytes) - 1, len(tar_bytes), ledger=ledger,
                disk_reserve=len(tar_bytes),
            )
            assert reply == {"sha256": expected, "bytes": len(tar_bytes)}
            status = ledger.status()
            assert status["attempts"] == 1
            assert status["body"] == len(tar_bytes)
            assert status["records"] == 0
        # Clean rejection: lease settles, the attempt stays charged.
        with RustWorker(_WORKER_BINARY, job_budget=WORKER_JOB_BUDGET) as worker:
            with pytest.raises(RustWorkerError, match="worker rejected request"):
                worker.fetch_range_gated(
                    "http://example.invalid:80/x", 0, 9, 100, ledger=ledger)
            status = ledger.status()
            assert status["attempts"] == 2
            assert status["body"] == len(tar_bytes)
        # Crash-class failure: pending lease is retained, i.e. no refund.
        crashed = RustWorker(_WORKER_BINARY, job_budget=WORKER_JOB_BUDGET, timeout_s=1.0)
        crashed._proc.kill()
        # Crash-class failure (dead pipe / timeout): pending lease retained.
        with pytest.raises((RustWorkerError, OSError)):
            crashed.fetch_range_gated(url, 0, 1023, len(tar_bytes), ledger=ledger)
        status = ledger.status()
        assert status["attempts"] == 3
        assert status["body"] == len(tar_bytes) + 1024
        assert status["inflight"] >= 1024
        crashed.close()
    finally:
        server.shutdown()
        server.server_close()
