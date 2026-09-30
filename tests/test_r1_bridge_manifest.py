"""Real subprocess regressions for bounded TAR manifest responses."""

import hashlib
import json
import subprocess
import sys
import tarfile
import threading
import time

import pytest
from conftest import resolve_r1_worker

from sakurapool.storage import rust_bridge as bridge
from sakurapool.storage.rust_bridge import RustWorker, RustWorkerError

_FAKE = r'''
import json, sys, time
mode = sys.argv[1]
json.loads(sys.stdin.buffer.readline())
print(json.dumps({"type": "ready", "protocol_version": 1,
                  "worker_version": "fake", "capabilities": ["scan_tar"]}), flush=True)
for line in sys.stdin.buffer:
    request = json.loads(line)
    if mode == "huge":
        # No newline and never finishes: rejecting must kill, not drain to EOF.
        while True:
            sys.stdout.buffer.write(b"x" * 65536)
            sys.stdout.buffer.flush()
    if mode == "invalid":
        sys.stdout.buffer.write(b"x" * 100000 + b"\n")
        sys.stdout.buffer.flush()
        time.sleep(3600)
    count = 100001 if mode == "members" else 1000
    members = [{"path": "member-%06d.bin" % i, "kind": "file", "offset": i * 512,
                "size": 0, "sha256": "0" * 64} for i in range(count)]
    result = {"members": members, "whole_sha256": "0" * 64,
              "size": count * 512, "trailing_bytes": 1024}
    response = {"type": "response", "request_id": request["request_id"],
                "ok": True, "result": result}
    if mode == "error":
        response = {"type": "response", "request_id": request["request_id"],
                    "ok": False, "error": "x" * 100000}
    print(json.dumps(response), flush=True)
    if mode == "sleep":
        time.sleep(3600)
'''


@pytest.fixture
def fake_worker(monkeypatch):
    popen = subprocess.Popen
    workers = []
    timers = []

    def start(mode):
        def spawn(args, **kwargs):
            return popen([sys.executable, "-u", "-c", _FAKE, mode], **kwargs)

        monkeypatch.setattr(bridge.subprocess, "Popen", spawn)
        worker = RustWorker(sys.executable, timeout_s=3600)
        workers.append(worker)
        # A regression must fail promptly, not hang the test suite for an hour.
        timer = threading.Timer(8, worker.cancel)
        timer.daemon = True
        timer.start()
        timers.append(timer)
        return worker

    yield start
    for timer in timers:
        timer.cancel()
    for worker in workers:
        worker.cancel()


@pytest.mark.parametrize("operation", ["scan_tar", "scan_http_tar"])
def test_large_manifest_round_trip_and_control_limit(fake_worker, operation):
    with fake_worker("valid") as worker:
        result = worker.request(operation, payload={"max_members": 100000})
        assert len(json.dumps(result).encode()) > bridge.MAX_LINE_BYTES
        assert len(result["members"]) == 1000
        assert result["members"][-1]["path"] == "member-000999.bin"
        # The next non-scan response still has the original small limit.
        with pytest.raises(RustWorkerError, match="response line too long"):
            worker.request("hash_file")
    assert worker.pid is None
    assert not worker._reader.is_alive()
    assert not worker._stderr_reader.is_alive()


@pytest.mark.parametrize("mode, error", [
    ("huge", "response line too long"),
    ("invalid", "invalid json"),
    ("members", "invalid json"),
    ("error", "response line too long"),
])
def test_bad_manifest_rejects_and_closes_promptly(fake_worker, mode, error):
    worker = fake_worker(mode)
    started = time.monotonic()
    with pytest.raises(RustWorkerError, match=error):
        worker.request("scan_tar")
    worker.close()
    assert time.monotonic() - started < 6
    assert worker.pid is None
    assert not worker._reader.is_alive()
    assert not worker._stderr_reader.is_alive()
    if mode == "huge":
        assert worker._stdout_error == "worker response line too long"


def test_close_has_short_timeout_despite_hour_long_request_timeout(fake_worker):
    worker = fake_worker("sleep")
    worker.request("scan_tar")
    started = time.monotonic()
    worker.close()
    assert time.monotonic() - started < 6
    assert worker.pid is None


def test_scan_member_request_limit_is_bounded(fake_worker):
    with fake_worker("valid") as worker:
        for limit in (100001, -1, True):
            with pytest.raises(RustWorkerError, match="scan member limit invalid"):
                worker.request("scan_tar", payload={"max_members": limit})
        with pytest.raises(RustWorkerError, match="invalid json"):
            worker.request("scan_tar", payload={"max_members": 999})


@pytest.mark.skipif(not resolve_r1_worker(), reason="explicit Rust worker unavailable")
def test_real_worker_emits_manifest_over_64kib(tmp_path):
    path = tmp_path / "many-members.tar"
    with tarfile.open(path, "w", format=tarfile.GNU_FORMAT) as archive:
        for index in range(1000):
            archive.addfile(tarfile.TarInfo(f"member-{index:06d}.bin"))
    raw = path.read_bytes()
    budget = {"body": len(raw), "disk": 0, "inflight": len(raw), "attempts": 1}
    with RustWorker(resolve_r1_worker(), job_budget=budget, timeout_s=3600) as worker:
        result = worker.request("scan_tar", budget=budget,
                                payload={"path": str(path), "max_members": 100000})
    assert len(json.dumps(result).encode()) > bridge.MAX_LINE_BYTES
    assert len(result["members"]) == 1000
    assert result["whole_sha256"] == hashlib.sha256(raw).hexdigest()
    assert result["size"] == len(raw)
