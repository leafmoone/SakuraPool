"""Offline tests for the Python -> Rust worker NDJSON bridge.

No external network: the worker binary runs locally over stdin/stdout and
only performs local file hashing, range validation, and lifecycle ops.
"""

import hashlib
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from sakurapool.storage.rust_bridge import (
    RustWorker,
    RustWorkerError,
    find_worker_binary,
)

try:
    find_worker_binary()
except RustWorkerError:
    _WORKER_AVAILABLE = False
else:
    _WORKER_AVAILABLE = True

requires_worker = pytest.mark.skipif(
    not _WORKER_AVAILABLE,
    reason="sakurapool-worker binary not built",
)


def _write_payload(tmp_path: Path, size: int = 4 * 1024 * 1024) -> tuple[Path, bytes]:
    payload = bytes((i * 31 + 7) % 251 for i in range(size))
    path = tmp_path / "r1-payload.bin"
    path.write_bytes(payload)
    return path, payload


@requires_worker
def test_hash_file_matches_python_streaming_hash(tmp_path: Path) -> None:
    path, payload = _write_payload(tmp_path)
    expected = hashlib.sha256()
    for offset in range(0, len(payload), 64 * 1024):
        expected.update(payload[offset : offset + 64 * 1024])
    with RustWorker() as worker:
        assert worker.hash_file(path) == expected.hexdigest()


@requires_worker
def test_validate_range_ok_and_reject() -> None:
    with RustWorker() as worker:
        ok = worker.call(
            "validate_range",
            start=2,
            end=4,
            total=10,
            content_range="bytes 2-4/10",
            body_len=3,
        )
        assert ok == "valid"
        with pytest.raises(RustWorkerError, match="worker rejected request"):
            worker.call(
                "validate_range",
                start=0,
                end=9,
                total=10,
                content_range="bytes 0-8/10",
                body_len=9,
            )


@requires_worker
def test_lifecycle_and_cancel() -> None:
    with RustWorker() as worker:
        assert "Responding" in worker.call("lifecycle", action="begin")
        assert "Cancelled" in worker.call("lifecycle", action="cancel")
        with pytest.raises(RustWorkerError):
            worker.call("nope")


@requires_worker
def test_missing_file_is_rejected(tmp_path: Path) -> None:
    with RustWorker() as worker:
        with pytest.raises(RustWorkerError):
            worker.hash_file(tmp_path / "absent.bin")


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


@requires_worker
def test_loopback_fetch_range_bytes_match_python() -> None:
    payload = bytes((i * 13 + 5) % 253 for i in range(2 * 1024 * 1024))
    _RangeHandler.payload = payload
    server = ThreadingHTTPServer(("127.0.0.1", 0), _RangeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_address[1]
        url = f"http://127.0.0.1:{port}/data/obj.bin"
        with RustWorker() as worker:
            full = worker.call(
                "fetch_range", url=url, start=0, end=len(payload) - 1, total=len(payload)
            )
            assert full == f"sha256:{hashlib.sha256(payload).hexdigest()}:bytes:{len(payload)}"
            part = payload[1000 : 1000 + 65536]
            part_reply = worker.call(
                "fetch_range", url=url, start=1000, end=1000 + len(part) - 1, total=len(payload)
            )
            assert part_reply == f"sha256:{hashlib.sha256(part).hexdigest()}:bytes:{len(part)}"
            with pytest.raises(RustWorkerError):
                worker.call(
                    "fetch_range", url="http://example.invalid:80/x", start=0, end=9, total=100
                )
    finally:
        server.shutdown()
        server.server_close()


@requires_worker
def test_close_is_idempotent_and_blocks_later_calls() -> None:
    worker = RustWorker()
    worker.close()
    worker.close()
    with pytest.raises(RustWorkerError, match="worker is closed"):
        worker.call("lifecycle", action="begin")
