"""Production protocol working-set tests against the explicitly rebuilt worker."""

import json
import os
import queue
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from sakurapool.capacity import UINT64_MAX, CapacityConfig
from sakurapool.storage import production_resources, rust_bridge
from sakurapool.storage.production_resources import ProductionFootprint, protocolmemory
from sakurapool.storage.rust_bridge import RustWorker, RustWorkerError, _json_peak

ROOT = Path(__file__).resolve().parents[1]


def test_actual_root_and_memory_model():
    assert Path(production_resources.__file__).resolve().is_relative_to(ROOT / "src")
    assert Path(rust_bridge.__file__).resolve().is_relative_to(ROOT / "src")
    assert rust_bridge._STDOUT_QUEUE_LINES == 1
    capacity = CapacityConfig(rpc_line_bytes=16 << 20, http_header_bytes=417_760)
    assert protocolmemory(capacity) == (
        (1 << 20)
        + 13 * capacity.rpc_line_bytes
        + production_resources.PROTOCOL_NODE_BYTES * production_resources.PROTOCOL_MAX_NODES
        + 2 * (4 * production_resources.HTTP_PARSER_BYTES + 128 * 256)
    )
    assert ProductionFootprint.admit("range", 1, capacity).memory == (
        32 << 20
    ) + 2 + protocolmemory(capacity)
    assert ProductionFootprint.admit("remote-stream-scan", 1, capacity).memory == (
        128 << 20
    ) + protocolmemory(capacity)
    with pytest.raises(ValueError):
        ProductionFootprint.admit("download-then-scan", UINT64_MAX)


@pytest.mark.parametrize(
    "raw",
    [
        b"[]",
        b'{"x":' + b"[" * 64 + b"0" + b"]" * 64 + b"}",
        b'{"x":' + b"9" * 1000 + b"}",
        b'{"x":1e999}',
        b'{"x":NaN}',
        b'{"x":18446744073709551616}',
        b'{"x":[' + b"0," * 65536 + b"0]}",
    ],
    ids=["root", "depth", "digits", "exponent", "nan", "integer", "nodes"],
)
def test_rejects_before_loads(raw, monkeypatch):
    worker = RustWorker.__new__(RustWorker)
    worker._line_bytes = 16 << 20
    worker.timeout_s = 1
    worker._stop = threading.Event()
    worker._lines = queue.Queue(maxsize=1)
    worker._lines.put(raw)
    monkeypatch.setattr(
        rust_bridge.json, "loads", lambda *_: pytest.fail("decoded before lexical gate")
    )
    with pytest.raises(RustWorkerError):
        worker._receive()


def test_lexical_string_peak():
    raw = b'{"x":"' + b"\\u1234" * 100 + b'","n":18446744073709551615}'
    assert _json_peak(raw) >= 4 * len(raw)


def _binary():
    configured = Path(os.environ["SAKURAPOOL_STREAM_WORKER"])
    expected = Path(os.environ["SAKURAPOOL_EXPECTED_STREAM_WORKER"])
    assert configured.is_absolute() and expected.is_absolute()
    binary = configured.resolve()
    assert binary == expected.resolve()
    assert binary.is_file()
    return binary


@pytest.mark.parametrize("insufficient", [True, False])
def test_expanded_rpc_header_and_pre_http_admission(tmp_path, insufficient):
    hits = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            hits.append(self.path)
            if self.path.startswith("/api/"):
                self.send_response(302)
                self.send_header("Location", f"http://127.0.0.1:{self.server.server_port}/body")
                self.send_header("Content-Length", "0")
            else:
                self.send_response(206)
                self.send_header("Content-Length", "1")
                self.send_header("Content-Range", "bytes 0-0/1")
                self.send_header("ETag", '"v1"')
            self.send_header("X-Large", "a" * 80000)
            self.end_headers()
            if self.path == "/body":
                self.wfile.write(b"x")

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    capacity = CapacityConfig(rpc_line_bytes=16 << 20, http_header_bytes=417_760)
    footprint = ProductionFootprint.admit("range", 1, capacity)
    budget = dict(body=2, disk=1, attempts=2, inflight=footprint.memory - int(insufficient))
    transfer = dict(
        profile="twohop_test",
        object=dict(
            repo_id="a/b",
            repo_type="modelscope_dataset_legacy",
            origin=f"http://127.0.0.1:{server.server_port}/",
            revision="a" * 40,
            object_path="data.tar",
            object_size=1,
            validator='"v1"',
            cdn_host="127.0.0.1",
        ),
        token=None,
        cookie=None,
        start=0,
        length=1,
        condition="match",
        output_root=str(tmp_path.resolve()),
        output_name="body.bin",
        report_name=None,
        mode="range",
        payload_revision=2,
        range_chunk_bytes=capacity.range_chunk_bytes,
        http_header_bytes=capacity.http_header_bytes,
    )
    try:
        with RustWorker(_binary(), capacity=capacity, job_budget=budget) as worker:
            assert worker._bootstrap_bytes == 65536
            assert worker.protocol_resident_bytes == protocolmemory(capacity)
            request = dict(
                type="request",
                request_id="combo",
                operation="fetch_range",
                budget=budget,
                payload=dict(production=transfer),
            )
            worker.send_raw(b" " * ((16 << 20) - 4096) + json.dumps(request).encode() + b"\n")
            response = worker.read_raw()
            assert response["ok"] is not insufficient
            if insufficient:
                assert response["error"] == "production_budget"
                assert hits == []
                assert not (tmp_path / "body.bin").exists()
            else:
                assert response["result"]["bytes"] == 1
                assert len(hits) == 2
                assert (tmp_path / "body.bin").read_bytes() == b"x"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
