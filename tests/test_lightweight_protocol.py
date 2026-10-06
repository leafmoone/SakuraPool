"""Local worker protocol tests: no public network or production directories."""

import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from sakurapool.capacity import CapacityConfig
from sakurapool.storage.rust_bridge import RustWorker, RustWorkerError


@pytest.fixture(scope="module")
def lightweight_worker():
    path = Path(__file__).parents[1] / "rust/target/debug/sakurapool-worker.exe"
    assert path.is_file(), "build the local debug worker first"
    return path


def test_protocol_one_hash_compatible(lightweight_worker, tmp_path):
    payload = b"synthetic legacy hash"
    source = tmp_path / "source"
    source.write_bytes(payload)
    with RustWorker(lightweight_worker) as worker:
        result = worker.request("hash_file", payload={"path": str(source)})
    assert result == {"sha256": hashlib.sha256(payload).hexdigest(), "bytes": len(payload)}


def test_protocol_two_exact_limits_no_budget(lightweight_worker):
    capacity = CapacityConfig(range_chunk_bytes=1024, http_header_bytes=4096)
    with RustWorker(lightweight_worker, capacity=capacity, lightweight=True) as worker:
        assert worker.protocol_version == 2
        assert "production_download_lightweight_v1" in worker.capabilities
        worker.send_raw(
            b'{"type":"request","request_id":"one","operation":"hash_file","payload":{}}\n'
        )
        response = worker.read_raw()
        assert not response["ok"]
        assert response["result"]["diagnostic"] == {
            "code": "rejected",
            "phase": "worker",
            "recoverable": False,
            "delivery_safe": False,
            "http_status": None,
        }
        assert "accounting" not in json.dumps(response)


@pytest.mark.parametrize("mutation", ["missing_cap", "wrong_limits", "old_protocol"])
def test_actual_handshake_negative_gate(lightweight_worker, monkeypatch, mutation):
    original = RustWorker._receive

    def receive(self, **kwargs):
        result = original(self, **kwargs)
        if result.get("type") == "ready":
            if mutation == "missing_cap":
                result["capabilities"].remove("production_download_lightweight_v1")
            elif mutation == "wrong_limits":
                result["execution_limits"]["range_chunk_bytes"] += 1
            else:
                result["protocol_version"] = 1
        return result

    monkeypatch.setattr(RustWorker, "_receive", receive)
    with pytest.raises(RustWorkerError):
        RustWorker(lightweight_worker, lightweight=True)


def test_bounded_dedup_and_no_legacy_budget(lightweight_worker):
    with RustWorker(lightweight_worker, lightweight=True) as worker:
        for n in range(256):
            worker.send_raw(
                (
                    json.dumps(
                        dict(type="request", request_id=str(n), operation="fetch_range", payload={})
                    )
                    + "\n"
                ).encode()
            )
            assert not worker.read_raw()["ok"]
        worker.send_raw(
            b'{"type":"request","request_id":"overflow","operation":"fetch_range","payload":{}}\n'
        )
        assert not worker.read_raw()["ok"]
    with RustWorker(lightweight_worker, lightweight=True) as worker:
        line = dict(type="request", request_id="same", operation="fetch_range", payload={})
        for _ in range(2):
            worker.send_raw((json.dumps(line) + "\n").encode())
            assert not worker.read_raw()["ok"]
        with pytest.raises(RustWorkerError):
            worker.request("fetch_range", budget={"body": 1}, payload={"production": {}})


def test_v2_hello_refuses_budget(lightweight_worker):
    message = dict(
        type="hello",
        protocol_version=2,
        execution_limits=dict(range_chunk_bytes=1024, http_header_bytes=4096, rpc_line_bytes=65536),
        budget=dict(body=0, disk=0, inflight=0, attempts=0),
    )
    result = subprocess.run(
        [str(lightweight_worker)],
        input=json.dumps(message) + "\n",
        text=True,
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == 0
    assert json.loads(result.stdout)["type"] == "protocol_error"
