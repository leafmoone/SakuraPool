"""Offline tests for the Python -> Rust worker NDJSON bridge.

No external network: the worker binary runs locally over stdin/stdout and
only performs local file hashing, range validation, and lifecycle ops.
"""

import hashlib
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
    reason="sakurapool-r1 worker binary not built",
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


@requires_worker
def test_close_is_idempotent_and_blocks_later_calls() -> None:
    worker = RustWorker()
    worker.close()
    worker.close()
    with pytest.raises(RustWorkerError, match="worker is closed"):
        worker.call("lifecycle", action="begin")
