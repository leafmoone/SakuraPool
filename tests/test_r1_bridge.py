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
import socket
import sys
import tempfile
import threading

import pytest
from conftest import resolve_r1_worker

from sakurapool.storage import rust_bridge as bridge
from sakurapool.storage.budget import (
    DEFAULT_WORK_ROOT,
    BudgetCorrupt,
    BudgetExceeded,
    BudgetLedger,
    Reservation,
)
from sakurapool.storage.rust_bridge import RustWorker, RustWorkerError

# Single source of truth: tests/conftest.py (explicit env var is
# authoritative; missing binary -> None -> the suite skips with a report).
RUST_WORKER = resolve_r1_worker()

NEEDS_WORKER = pytest.mark.skipif(
    not RUST_WORKER, reason="sakurapool-worker binary not found (see R1 header report)")


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
    """Loopback test endpoint speaking strict HTTP/1.1 (the worker client
    rejects non-HTTP/1.1 status lines)."""

    payload: bytes
    status: int = 206
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        data = type(self).payload
        if type(self).status == 200:
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Connection", "close")
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


def _stall_server():
    """Loopback TCP server that accepts, swallows the request, then never
    responds. Used to hang a worker mid-fetch so the bridge's receive
    timeout is the thing that fires."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)

    def accept_loop():
        while True:
            try:
                conn, _addr = listener.accept()
            except OSError:
                break
            # Swallow the request bytes, then hold the connection open with
            # no response so a range fetch blocks in read.
            try:
                while True:
                    data = conn.recv(65536)
                    if not data:
                        break
            except OSError:
                pass

    thread = threading.Thread(target=accept_loop, daemon=True)
    thread.start()
    port = listener.getsockname()[1]
    return listener, f"http://127.0.0.1:{port}/stall.bin", thread


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
        assert worker.capabilities == (
            "hash_file", "fetch_range", "scan_tar", "scan_http_tar", "bounded_session_v1")
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

def _retain_live_worker_resources(worker):
    proc = worker._proc
    readers = (worker._reader, worker._stderr_reader)
    pipes = (proc.stdin, proc.stdout, proc.stderr)
    assert proc.returncode is None and proc.poll() is None, "worker must initially be running"
    assert worker.pid == proc.pid
    return proc, readers, pipes


def _assert_worker_exited(worker, resources):
    proc, readers, pipes = resources
    # Observe the retained Popen, not a PID lookup or only a cleared bridge reference.
    returncode = proc.returncode
    polled = proc.poll()
    state = {"returncode_before_poll": returncode, "poll": polled,
             "worker_pid": worker.pid,
             "reader_alive": [reader.is_alive() for reader in readers],
             "pipes_closed": [pipe.closed if pipe is not None else None for pipe in pipes]}
    assert returncode is not None, state
    assert polled is not None, state
    assert worker.pid is None, state
    assert not any(state["reader_alive"]), state
    assert all(pipe is None or pipe.closed for pipe in pipes), state


@NEEDS_WORKER
def test_cancel_kills_and_waits(worker_path):
    worker = RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET)
    resources = _retain_live_worker_resources(worker)
    try:
        worker.close()
        _assert_worker_exited(worker, resources)
    finally:
        worker.close()
        proc = resources[0]
        if proc.poll() is None:
            proc.kill()
            proc.wait()


@NEEDS_WORKER
def test_cancel_is_immediate_kill_and_wait(worker_path, monkeypatch):
    worker = RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET)
    resources = _retain_live_worker_resources(worker)
    proc = resources[0]
    calls = []
    original_kill, original_wait = proc.kill, proc.wait

    def kill():
        calls.append("kill")
        return original_kill()

    def wait(*args, **kwargs):
        calls.append("wait")
        return original_wait(*args, **kwargs)

    monkeypatch.setattr(proc, "kill", kill)
    monkeypatch.setattr(proc, "wait", wait)
    try:
        worker.cancel()
        _assert_worker_exited(worker, resources)
        assert calls[:2] == ["kill", "wait"], calls
    finally:
        worker.close()
        if proc.poll() is None:
            original_kill()
            original_wait()


@NEEDS_WORKER
@pytest.mark.parametrize("shutdown", ["close", "cancel"])
def test_exit_assertion_rejects_live_worker_and_repeated_shutdown(worker_path, shutdown):
    worker = RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET)
    resources = _retain_live_worker_resources(worker)
    try:
        with pytest.raises(AssertionError, match="returncode_before_poll"):
            _assert_worker_exited(worker, resources)
        assert resources[0].poll() is None, "negative assertion must not terminate the worker"
        getattr(worker, shutdown)()
        _assert_worker_exited(worker, resources)
        worker.close()
        worker.cancel()
        worker.close()
        worker.cancel()
        _assert_worker_exited(worker, resources)
    finally:
        worker.close()
        proc = resources[0]
        if proc.poll() is None:
            proc.kill()
            proc.wait()


@NEEDS_WORKER
def test_stderr_tail_is_bounded(worker_path):
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


def test_stderr_drain_thread_keeps_last_64kib_on_real_pipe():
    """Real pipe + drain thread + overflow: the tail keeps exactly the
    last 64 KiB of mixed content and the drain never deadlocks."""
    import subprocess as sp

    expected = bytes(range(256)) * 400  # 100 KiB of distinct bytes
    child = sp.Popen(
        [sys.executable, "-c",
         "import sys; sys.stderr.buffer.write(bytes(range(256))*400); sys.stderr.buffer.flush()"],
        stderr=sp.PIPE,
    )
    worker = RustWorker.__new__(RustWorker)
    worker._proc = child
    worker._stderr_tail = bytearray()
    worker._stderr_lock = threading.Lock()
    drain = threading.Thread(target=worker._drain_stderr)
    drain.start()
    drain.join(timeout=10)
    assert not drain.is_alive(), "stderr drain must not deadlock on overflow"
    tail = worker.stderr_tail()
    assert len(tail) == bridge._STDERR_TAIL_BYTES
    assert tail == expected[-bridge._STDERR_TAIL_BYTES:]
    child.wait(timeout=10)


@NEEDS_WORKER
def test_bridge_timeout_on_hung_worker_retains_lease(worker_path, ledger):
    """A worker stuck mid-fetch never replies; the bridge receive timeout
    fires (crash class) and the durable lease stays pending."""
    listener, url, _thread = _stall_server()
    try:
        worker = RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET,
                            timeout_s=2.0)
        body = 4096
        with pytest.raises(RustWorkerError, match="timed out"):
            worker.fetch_range_gated(url, 0, body - 1, body * 16,
                                     ledger=ledger)
        status = ledger.status()
        assert status["attempts"] == 1
        assert status["body"] == body
        assert status["records"] == 0
        # Reap the still-blocked worker.
        worker.cancel()
        assert worker.pid is None
    finally:
        listener.close()


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


def test_python_crash_keeps_durable_lease_pending(work_root):
    """A hard exit of the owning Python process while a lease is held must
    not refund or corrupt the durable ledger: a fresh process opening the
    same root sees the pending reservation exactly as left behind."""
    import subprocess as sp

    root = os.path.join(work_root, "ledger-crash")
    os.mkdir(root)
    code = (
        "from sakurapool.storage.budget import BudgetLedger, Reservation;"
        "import os;"
        f"ledger = BudgetLedger({root!r}, _offline_test=True,"
        " _test_limits={'body': 4*1024*1024, 'inflight': 8*1024*1024,"
        " 'attempts': 5});"
        "lease = ledger.reserve(Reservation(body=3000, disk=0, inflight=3000,"
        " attempt=True));"
        "os._exit(0)"  # hard exit: no settle, no atexit cleanup
    )
    child = sp.Popen([sys.executable, "-c", code],
                     env={**os.environ, "PYTHONPATH": os.environ.get("PYTHONPATH", "src")},
                     cwd=os.getcwd())
    assert child.wait(timeout=120) == 0

    limits = {"body": 4 * 1024 * 1024, "inflight": 8 * 1024 * 1024, "attempts": 5}
    ledger = BudgetLedger(root, _offline_test=True, _test_limits=limits)
    status = ledger.status()
    # The reservation survives the crash, unrefunded and still pending.
    assert status["attempts"] == 1
    assert status["body"] == 3000
    assert status["inflight"] >= 3000
    # The surviving ledger stays usable within the remaining budget: a
    # consume + settle cycle charges exactly the consumed body, leaving the
    # crashed pending reservation untouched.
    lease = ledger.reserve(Reservation(body=100, disk=0, inflight=100, attempt=True))
    ledger.consume_body(lease, 100)
    ledger.settle(lease)
    status = ledger.status()
    assert status["body"] == 3100
    assert status["attempts"] == 2


@NEEDS_WORKER
def test_double_settle_rejected(worker_path, ledger):
    lease = ledger.reserve(Reservation(body=100, disk=0, inflight=200, attempt=True))
    ledger.settle(lease)
    with pytest.raises(BudgetCorrupt, match="already settled"):
        ledger.settle(lease)


@NEEDS_WORKER
def test_gated_fetch_without_ledger_refuses(worker_path):
    with RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET) as worker:
        with pytest.raises(TypeError):
            worker.fetch_range_gated("http://127.0.0.1:9/", 0, 10, 100)


# ---------------------------------------------------------------------------
# scan_tar: bounded sequential archive audit (offline synthetic archives)
# ---------------------------------------------------------------------------

def _make_scan_fixture(tmp_path):
    import io as _io
    import tarfile

    data_a = os.urandom(1000)
    data_b = os.urandom(2048)
    buf = _io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.GNU_FORMAT) as tf:
        dir_info = tarfile.TarInfo("sub/")
        dir_info.type = tarfile.DIRTYPE
        tf.addfile(dir_info)
        for name, data in [("a.bin", data_a), ("sub/b.bin", data_b), ("sub/empty", b"")]:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, _io.BytesIO(data))
    raw = buf.getvalue()
    tar_path = tmp_path / "scan.tar"
    tar_path.write_bytes(raw)
    return raw


@NEEDS_WORKER
def test_scan_tar_agrees_with_raw_bytes(worker_path, tmp_path):
    raw = _make_scan_fixture(tmp_path)
    with RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET) as worker:
        result = worker.request(
            "scan_tar",
            budget={"body": len(raw), "disk": 0, "inflight": len(raw), "attempts": 1},
            payload={"path": str(tmp_path / "scan.tar")},
        )
    assert result["whole_sha256"] == hashlib.sha256(raw).hexdigest()
    assert result["size"] == len(raw)
    assert result["trailing_bytes"] > 0
    paths = [m["path"] for m in result["members"]]
    assert "a.bin" in paths and "sub/b.bin" in paths and "sub/empty" in paths
    assert "sub/" in paths
    for member in result["members"]:
        assert member["kind"] in ("file", "dir")
        assert member["offset"] % 512 == 0
        if member["kind"] == "file":
            seg = raw[member["offset"]: member["offset"] + member["size"]]
            assert hashlib.sha256(seg).hexdigest() == member["sha256"]


@NEEDS_WORKER
def test_scan_tar_rejections(worker_path, tmp_path):
    raw = _make_scan_fixture(tmp_path)
    budget = {"body": len(raw), "disk": 0, "inflight": len(raw), "attempts": 3}
    with RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET) as worker:
        # Missing file.
        with pytest.raises(RustWorkerError, match="worker rejected request"):
            worker.request("scan_tar", budget=budget,
                           payload={"path": str(tmp_path / "nope.tar")})
        # Member budget exceeded (fixture has 4 members).
        with pytest.raises(RustWorkerError, match="worker rejected request"):
            worker.request("scan_tar", budget=budget,
                           payload={"path": str(tmp_path / "scan.tar"), "max_members": 1})
        # Byte budget exceeded.
        with pytest.raises(RustWorkerError, match="worker rejected request"):
            worker.request("scan_tar", budget=budget,
                           payload={"path": str(tmp_path / "scan.tar"),
                                    "max_bytes": len(raw) - 1})
        # Unknown payload field is a protocol violation, worker survives.
        with pytest.raises(RustWorkerError, match="worker rejected request"):
            worker.request("scan_tar", budget=budget,
                           payload={"path": str(tmp_path / "scan.tar"), "bogus": 1})

@NEEDS_WORKER
def test_scan_http_tar_matches_file_scan_field_for_field(worker_path, tmp_path):
    """Rust HTTP scan and Rust file scan over the same bytes must agree on
    every field (Gate 3). Fully offline: loopback HTTP, synthetic archive."""
    raw = _make_scan_fixture(tmp_path)
    server, url, _thread = _serve(raw, status=200)
    try:
        with RustWorker(worker_path, job_budget=WORKER_JOB_BUDGET) as worker:
            budget = {"body": len(raw), "disk": 0, "inflight": len(raw), "attempts": 1}
            http_result = worker.request(
                "scan_http_tar", budget=budget, payload={"url": url})
            file_result = worker.request(
                "scan_tar", budget=budget,
                payload={"path": str(tmp_path / "scan.tar")})
            # Byte cap is enforced on the transport: one byte short of the
            # archive, server still serving the full body.
            with pytest.raises(RustWorkerError, match="worker rejected request"):
                worker.request(
                    "scan_http_tar", budget=budget,
                    payload={"url": url, "max_bytes": len(raw) - 1})
    finally:
        server.shutdown()
        server.server_close()
    assert http_result == file_result
    assert http_result["whole_sha256"] == hashlib.sha256(raw).hexdigest()
    assert http_result["size"] == len(raw)
