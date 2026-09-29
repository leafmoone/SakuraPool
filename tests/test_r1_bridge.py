"""R1 bridge tests: formal NDJSON protocol handshake (type/protocol_version/
capabilities), structured stdout, bounded stderr drain, cancel kill+wait,
explicit binary path without fallback, and Python-owned durable budget with
the Rust worker's in-memory job budget.

All tests run fully offline: loopback TCP servers and temporary synthetic
files under the fixed P4 work root only. No production data, no real
network, no real archives.
"""

import hashlib
import http.server
import json
import os
import tempfile
import threading

import pytest

from sakurapool.storage import rust_bridge as bridge
from sakurapool.storage.budget import (
    DEFAULT_WORK_ROOT,
    BudgetCorrupt,
    BudgetExceeded,
    BudgetLedger,
    Reservation,
)
from sakurapool.storage.rust_bridge import RustWorker, RustWorkerError

RUST_WORKER = os.environ.get("SAKURAPOOL_RUST_WORKER", "")
if not RUST_WORKER:
    for cand in (
        "D:/SakuraTool/SakuraPool-P4-work/rust-target/release/sakurapool-worker.exe",
        "D:/SakuraTool/SakuraPool-P4-work/rust-target/debug/sakurapool-worker.exe",
    ):
        if os.path.isfile(cand):
            RUST_WORKER = cand
            break

NEEDS_WORKER = pytest.mark.skipif(
    not RUST_WORKER, reason="sakurapool-worker binary not built (RUST-CARGO-TARGET required)")


@pytest.fixture
def worker_path():
    assert RUST_WORKER, "no worker binary available"
    return RUST_WORKER


WORKER_JOB_BUDGET = {
    "body": 4 * 1024 * 1024,
    "disk": 4 * 1024 * 1024,
    "inflight": 8 * 1024 * 1024,
    "attempts": 5,
}


@pytest.fixture
def work_root():
    # Offline ledger roots must live under the fixed P4 work root.
    with tempfile.TemporaryDirectory(prefix="r1-bridge-", dir=DEFAULT_WORK_ROOT) as temp:
        yield temp


@pytest.fixture
def ledger(work_root):
    root = os.path.join(work_root, "ledger")
    os.mkdir(root)
    yield BudgetLedger(
        root, _offline_test=True,
        _test_limits={"body": 4 * 1024 * 1024, "inflight": 8 * 1024 * 1024, "attempts": 5})


class _RangeHandler(http.server.BaseHTTPRequestHandler):
    payload: bytes
    status: int = 206

    def log_message(self, *args):
        pass

    def do_GET(self):
        data = type(self).payload
        if type(self).status == 200:
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        start, end = 0, len(data) - 1
        range_header = self.headers.get("Range")
        if range_header:
            spec = range_header.removeprefix("bytes=")
            start = int(spec.split("-")[0])
            end = int(spec.split("-")[1])
        part = data[start:end + 1]
        self.send_response(206)
        self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        self.send_header("Content-Length", str(len(part)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(part)


def _serve(body, status=206):
    _RangeHandler.payload = body
    _RangeHandler.status = status
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _RangeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{server.server_address[1]}/a.tar", thread


@pytest.fixture
def loopback_server():
    body = os.urandom(200_000)
    server, url, _thread = _serve(body)
    yield url, body
    server.shutdown()
    server.server_close()


def _raw_request(worker, request_id, payload, operation="hash_file"):
    worker.send_raw(json.dumps({
        "type": "request", "request_id": request_id, "operation": operation,
        "budget": {"body": 1_000_000, "disk": 0, "inflight": 1_000_000, "attempts": 1},
        "payload": payload,
    }).encode("utf-8") + b"\n")
    return worker.read_raw()


# ------------------------------------------------------------------
# Handshake and binary resolution
# ------------------------------------------------------------------

@NEEDS_WORKER
def test_handshake_reports_protocol_version_and_capabilities(worker_path):
    with RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET) as worker:
        # The constructor already validated protocol_version agreement; the
        # ready line also reports the worker build and its capabilities.
        assert worker.capabilities == ("hash_file", "fetch_range")
        assert worker.worker_version == "0.1.0"
        assert bridge.PROTOCOL_VERSION == 1


@NEEDS_WORKER
def test_protocol_version_mismatch_is_fatal(worker_path, monkeypatch):
    monkeypatch.setattr(bridge, "PROTOCOL_VERSION", 99)
    with pytest.raises(RustWorkerError, match="worker handshake failed"):
        RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET)


def test_explicit_binary_path_is_required_without_fallback(tmp_path):
    missing = tmp_path / "no-such-binary"
    with pytest.raises(RustWorkerError, match="worker binary missing"):
        RustWorker(missing)
    with pytest.raises(RustWorkerError, match="worker binary missing"):
        RustWorker(missing.parent)


# ------------------------------------------------------------------
# Line framing, UTF-8, unknown fields, duplicate ids
# ------------------------------------------------------------------

@NEEDS_WORKER
def test_oversized_line_rejected_and_worker_survives(worker_path):
    worker = RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET)
    try:
        # 64 KiB + 1 byte line: the bridge must reject it before the worker
        # sees it, and the worker must stay healthy afterwards.
        big = json.dumps({
            "type": "request", "request_id": "big", "operation": "hash_file",
            "budget": {"body": 1, "disk": 0, "inflight": 1, "attempts": 1},
            "payload": {"path": "x" * 70_000},
        }).encode("utf-8") + b"\n"
        assert len(big) > bridge.MAX_LINE_BYTES
        with pytest.raises(RustWorkerError, match="line too long"):
            worker.send_raw(big)
        with pytest.raises(RustWorkerError, match="worker rejected request"):
            worker.request("hash_file", payload={"path": "missing-xyz"})
    finally:
        worker.close()


@NEEDS_WORKER
def test_non_utf8_line_rejected_and_worker_survives(worker_path):
    worker = RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET)
    try:
        worker.send_raw(b'{"type":"request","request_id":"x"\xff}\n')
        reply = worker.read_raw()
        assert reply["type"] == "protocol_error"
        assert reply["error"] == "invalid_utf8"
        # The worker survived the malformed line.
        with pytest.raises(RustWorkerError, match="worker rejected request"):
            worker.request("hash_file", payload={"path": "missing-xyz"})
    finally:
        worker.close()


@NEEDS_WORKER
def test_unknown_fields_are_rejected(worker_path):
    worker = RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET)
    try:
        worker.send_raw(json.dumps({
            "type": "request", "request_id": "u1", "operation": "hash_file",
            "extra": True,
            "budget": {"body": 1, "disk": 0, "inflight": 1, "attempts": 1},
            "payload": {"path": "missing-xyz"},
        }).encode("utf-8") + b"\n")
        reply = worker.read_raw()
        assert reply["type"] == "protocol_error"
        assert reply["error"] == "protocol_violation"
        # The worker is still alive and processes normal requests.
        with pytest.raises(RustWorkerError, match="worker rejected request"):
            worker.request("hash_file", payload={"path": "missing-xyz"})
    finally:
        worker.close()


@NEEDS_WORKER
def test_duplicate_request_ids_rejected(worker_path):
    worker = RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET)
    try:
        first = _raw_request(worker, "dup", {"path": "missing-xyz"})
        assert first["ok"] is False
        assert first["error"] == "io_error"
        second = _raw_request(worker, "dup", {"path": "missing-xyz"})
        assert second["ok"] is False
        assert second["error"] == "duplicate_request"
    finally:
        worker.close()


# ------------------------------------------------------------------
# Durable budget gates (Python reserve is the only durable authority)
# ------------------------------------------------------------------

@NEEDS_WORKER
def test_attempt_limit_exhaustion_refuses_next_reserve(worker_path, work_root,
                                                       loopback_server):
    url, body = loopback_server
    root = os.path.join(work_root, "ledger1")
    os.mkdir(root)
    ledger = BudgetLedger(
        root, _offline_test=True,
        _test_limits={"body": 4 * 1024 * 1024, "inflight": 8 * 1024 * 1024, "attempts": 1})
    with RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET) as worker:
        result = worker.fetch_range_gated(url, 0, 511, len(body), ledger=ledger,
                                          disk_reserve=512)
        assert result["bytes"] == 512
    with RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET) as worker:
        with pytest.raises(BudgetExceeded, match="attempts limit reached"):
            worker.fetch_range_gated(url, 0, 511, len(body), ledger=ledger,
                                     disk_reserve=512)


# ------------------------------------------------------------------
# Cancel: kill + wait; stderr bounded tail
# ------------------------------------------------------------------

@NEEDS_WORKER
def test_cancel_kills_and_waits(worker_path):
    worker = RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET)
    pid = worker.pid
    assert pid is not None
    worker.close()

    def gone() -> bool:
        try:
            os.kill(pid, 0)
            return False
        except OSError:
            return True

    assert gone(), "worker process must be killed and reaped by close()"


@NEEDS_WORKER
def test_cancel_is_immediate_kill_and_wait(worker_path):
    worker = RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET)
    pid = worker.pid
    assert pid is not None
    worker.cancel()
    try:
        os.kill(pid, 0)
        alive = True
    except OSError:
        alive = False
    assert not alive, "cancel() must kill the worker and wait for exit"


def test_stderr_tail_is_bounded(worker_path):
    if not RUST_WORKER:
        pytest.skip("sakurapool-worker binary not built")
    worker = RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET)
    try:
        chunk = b"e" * 10_000
        for _ in range(7):
            worker._append_stderr(chunk)
        tail = worker.stderr_tail()
        assert len(tail) == bridge._STDERR_TAIL_BYTES
        assert tail == b"e" * bridge._STDERR_TAIL_BYTES
    finally:
        worker.close()


# ------------------------------------------------------------------
# Budget-gated fetch: durable Python reserve owns the outcome
# ------------------------------------------------------------------

@NEEDS_WORKER
def test_gated_fetch_success_settles(worker_path, ledger, loopback_server):
    url, body = loopback_server
    with RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET) as worker:
        result = worker.fetch_range_gated(url, 0, len(body) - 1, len(body),
                                          ledger=ledger, disk_reserve=len(body))
    expected = hashlib.sha256(body).hexdigest()
    assert result == {"sha256": expected, "bytes": len(body)}
    status = ledger.status()
    assert status["attempts"] == 1
    assert status["body"] == len(body)
    assert status["records"] == 0


@NEEDS_WORKER
def test_gated_fetch_mid_range_settles(worker_path, ledger, loopback_server):
    url, body = loopback_server
    start, end = 7, 10_006  # 10_000 bytes
    with RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET) as worker:
        result = worker.fetch_range_gated(url, start, end, len(body),
                                          ledger=ledger, disk_reserve=end - start + 1)
    assert result == {"sha256": hashlib.sha256(body[start:end + 1]).hexdigest(),
                      "bytes": end - start + 1}
    status = ledger.status()
    assert status["attempts"] == 1
    assert status["body"] == end - start + 1


@NEEDS_WORKER
def test_gated_fetch_clean_rejection_settles(worker_path, ledger):
    # A 200 (full) response is rejected by the range validator.
    body = os.urandom(50_000)
    server, url, _thread = _serve(body, status=200)
    try:
        with RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET) as worker:
            with pytest.raises(RustWorkerError, match="worker rejected request"):
                worker.fetch_range_gated(url, 0, len(body) - 1, len(body),
                                         ledger=ledger, disk_reserve=len(body))
        status = ledger.status()
        assert status["attempts"] == 1
        assert status["body"] == 0
        assert status["records"] == 0
    finally:
        server.shutdown()
        server.server_close()


@NEEDS_WORKER
def test_gated_crash_leaves_lease_pending(worker_path, ledger, loopback_server):
    url, body = loopback_server
    worker = RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET, timeout_s=1.0)
    try:
        worker._proc.kill()  # crash-class failure mid-session
        with pytest.raises(RustWorkerError):
            worker.fetch_range_gated(url, 0, 1023, len(body), ledger=ledger)
    finally:
        worker.close()
    status = ledger.status()
    # The attempt is charged; the body stays pending (no refund on crash).
    assert status["attempts"] == 1
    assert status["body"] == 1024
    assert status["inflight"] >= 1024


@NEEDS_WORKER
def test_restart_sees_durable_pending(worker_path, work_root, loopback_server):
    url, body = loopback_server
    root = os.path.join(work_root, "ledger2")
    os.mkdir(root)
    ledger = BudgetLedger(
        root, _offline_test=True,
        _test_limits={"body": 4 * 1024 * 1024, "inflight": 8 * 1024 * 1024, "attempts": 5})
    # Worker A reserves and dies; the durable ledger must still show the
    # pending reservation to a completely fresh process (worker B).
    a = RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET, timeout_s=1.0)
    a._proc.kill()
    with pytest.raises(RustWorkerError):
        a.fetch_range_gated(url, 0, 2047, len(body), ledger=ledger)
    a.close()
    status = ledger.status()
    assert status["body"] == 2048

    b = RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET)
    try:
        # The surviving session can still operate within the remaining budget.
        result = b.fetch_range_gated(url, 0, 511, len(body), ledger=ledger,
                                     disk_reserve=512)
        assert result == {
            "sha256": hashlib.sha256(body[:512]).hexdigest(), "bytes": 512}
        status = ledger.status()
        assert status["body"] == 2048 + 512
        assert status["attempts"] == 2
    finally:
        b.close()


@NEEDS_WORKER
def test_double_settle_rejected(worker_path, ledger):
    lease = ledger.reserve(Reservation(body=100, disk=0, inflight=200, attempt=True))
    ledger.settle(lease)
    with pytest.raises(BudgetCorrupt, match="already settled"):
        ledger.settle(lease)


def test_gated_fetch_without_ledger_refuses(worker_path):
    if not RUST_WORKER:
        pytest.skip("sakurapool-worker binary not built")
    with RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET) as worker:
        with pytest.raises(TypeError):
            worker.fetch_range_gated("http://127.0.0.1:9/", 0, 10, 100)
