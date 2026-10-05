from __future__ import annotations

import hashlib
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from sakurapool.capacity import UINT64_MAX, CapacityConfig, ResourcePolicy
from sakurapool.storage.prepared_fetch import stream_plan
from sakurapool.storage.production_resources import ProductionFootprint, protocolmemory
from sakurapool.storage.publication import PublicationCorrupt
from sakurapool.storage.rust_bridge import RustWorker
from sakurapool.storage.transport import GuardedTransport
from sakurapool.workspace import Workspace


def test_nine_mib_three_mib_chunks_and_tail():
    capacity = CapacityConfig(image_max_bytes=10 << 20, range_chunk_bytes=3 << 20)
    location = dict(
        image_offset=513, image_size=(9 << 20) + 7, metadata_offset=0, metadata_size=0, flags=0
    )
    plan = stream_plan(location, 11 << 20, capacity=capacity)
    assert plan.image_chunks == plan.chunk_count == 4
    assert list(plan.chunks()) == [
        (513, 3 << 20),
        (513 + (3 << 20), 3 << 20),
        (513 + (6 << 20), 3 << 20),
        (513 + (9 << 20), 7),
    ]
    assert plan.saved_bytes == (9 << 20) + 7


def test_larger_than_legacy_range_footprint():
    capacity = CapacityConfig(range_chunk_bytes=12 << 20)
    footprint = ProductionFootprint.admit("range", 9 << 20, capacity=capacity)
    assert footprint.memory == (32 << 20) + 2 * (9 << 20) + protocolmemory(capacity)
    assert footprint.artifacts == 9 << 20
    with pytest.raises(ValueError):
        ProductionFootprint.admit("range", 9 << 20)


def test_checked_extent_before_io():
    location = dict(
        image_offset=UINT64_MAX, image_size=1, metadata_offset=0, metadata_size=0, flags=0
    )
    with pytest.raises(PublicationCorrupt):
        stream_plan(location, UINT64_MAX)


def test_guarded_workspace_capacity_and_clone(tmp_path):
    capacity = CapacityConfig(range_chunk_bytes=12 << 20)
    workspace = Workspace.init(tmp_path / "domain", capacity=capacity)
    ledger = workspace.ledger()
    with GuardedTransport(ledger, trusted_hosts=frozenset({"modelscope.cn"})) as control:
        assert control.capacity == capacity
        assert control.max_range_bytes == 12 << 20
        with control.clone() as clone:
            assert clone.capacity == capacity
    with pytest.raises(ValueError, match="capacity mismatch"):
        GuardedTransport(
            ledger, trusted_hosts=frozenset({"modelscope.cn"}), capacity=CapacityConfig()
        )
    assert ledger.policy.body is None
    ledger.update_policy(ResourcePolicy(body=100))
    assert ledger.inspect_policy()["policy"]["body"] == 100


@pytest.mark.parametrize("header_limit", [65536, 32])
def test_actual_rust_range_over_eight_mib(tmp_path, header_limit):
    binary = os.environ.get("SAKURAPOOL_STREAM_WORKER")
    assert binary and Path(binary).is_file(), "set SAKURAPOOL_STREAM_WORKER to the rebuilt worker"
    expected = Path(os.environ["SAKURAPOOL_EXPECTED_STREAM_WORKER"])
    assert Path(binary).is_absolute() and expected.is_absolute()
    assert Path(binary).resolve() == expected.resolve()
    body = b"x" * ((9 << 20) + 7)
    hits = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            hits.append(self.headers.get("Range"))
            self.send_response(206)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Range", f"bytes 0-{len(body) - 1}/{len(body)}")
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    capacity = CapacityConfig(
        range_chunk_bytes=12 << 20, http_header_bytes=header_limit, rpc_line_bytes=128 << 10
    )
    budget = dict(body=len(body), attempts=1, disk=0, inflight=2 * len(body))
    try:
        with RustWorker(binary, capacity=capacity, job_budget=budget) as worker:
            payload = dict(
                url=f"http://127.0.0.1:{server.server_port}/body",
                start=0,
                end=len(body) - 1,
                total=len(body),
            )
            request = dict(
                type="request",
                request_id="large-line",
                operation="fetch_range",
                budget=budget,
                payload=payload,
            )
            worker.send_raw(b" " * 70000 + json.dumps(request).encode() + bytes([10]))
            response = worker.read_raw()
            assert response["type"] == "response"
            assert response["ok"] is (header_limit != 32)
            if header_limit != 32:
                assert response["result"] == dict(
                    bytes=len(body), sha256=hashlib.sha256(body).hexdigest()
                )
        assert hits == [f"bytes=0-{len(body) - 1}"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
