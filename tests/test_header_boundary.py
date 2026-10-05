"""Locked HTTP/1 boundaries; loopback only, explicitly rebuilt worker."""

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from sakurapool.capacity import (
    HTTP_MAX_HEADER_FIELDS,
    HTTP_PARSER_BYTES,
    HTTP_SUPPORTED_HEADER_BYTES,
    CapacityConfig,
)
from sakurapool.storage import rust_bridge
from sakurapool.storage.production_resources import ProductionFootprint
from sakurapool.storage.rust_bridge import RustWorker

ROOT = Path(__file__).resolve().parents[1]


def test_configuration_boundary_before_spawn(monkeypatch):
    assert HTTP_PARSER_BYTES == 417792
    assert HTTP_SUPPORTED_HEADER_BYTES == 417760
    assert HTTP_MAX_HEADER_FIELDS == 100
    assert Path(rust_bridge.__file__).resolve().is_relative_to(ROOT / "src")
    monkeypatch.setattr(rust_bridge.subprocess, "Popen", lambda *_a, **_k: pytest.fail("spawned"))
    for value in (HTTP_SUPPORTED_HEADER_BYTES + 1, 16 << 20):
        with pytest.raises(ValueError, match="http_header_bytes.*417760"):
            CapacityConfig.from_configuration({"http_header_bytes": value})
    for value in (80000, HTTP_SUPPORTED_HEADER_BYTES):
        capacity = CapacityConfig(http_header_bytes=value)
        assert capacity.http_header_bytes == value
        assert CapacityConfig.from_dict(capacity.to_dict()) == capacity
    assert CapacityConfig(rpc_line_bytes=16 << 20).rpc_line_bytes == 16 << 20


@pytest.mark.parametrize(
    "decoded_bytes,field_count,limit,accepted",
    [
        (80000, 4, 80000, True),
        (80000, 4, 79999, False),
        (HTTP_SUPPORTED_HEADER_BYTES, 4, HTTP_SUPPORTED_HEADER_BYTES, True),
        (HTTP_SUPPORTED_HEADER_BYTES, 100, HTTP_SUPPORTED_HEADER_BYTES, True),
        (80000, 101, HTTP_SUPPORTED_HEADER_BYTES, False),
    ],
    ids=["80k", "decoded-over-limit", "maximum-one-value", "maximum-100-fields", "101-fields"],
)
def test_actual_production_header_boundary(tmp_path, decoded_bytes, field_count, limit, accepted):
    binary = Path(os.environ["SAKURAPOOL_STREAM_WORKER"]).resolve()
    assert binary.is_file()
    hits = []
    fields = [("Content-Length", "1"), ("Content-Range", "bytes 0-0/1"), ("ETag", '"v1"')]
    fields.extend((f"X-{i}", "a") for i in range(field_count - 4))
    overhead = sum(len(name) + len(value) + 4 for name, value in fields) + len("X-Large") + 4
    fields.append(("X-Large", "a" * (decoded_bytes - overhead)))
    assert len(fields) == field_count
    assert sum(len(name) + len(value) + 4 for name, value in fields) == decoded_bytes
    wire = (
        b"HTTP/1.1 206 Partial Content\r\n"
        + b"".join(f"{name}: {value}\r\n".encode("ascii") for name, value in fields)
        + b"\r\nx"
    )
    if decoded_bytes == HTTP_SUPPORTED_HEADER_BYTES:
        assert len(wire) - 1 == HTTP_PARSER_BYTES

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            hits.append(self.path)
            if self.path.startswith("/api/"):
                self.send_response(302)
                self.send_header("Location", f"http://127.0.0.1:{self.server.server_port}/body")
                self.send_header("Content-Length", "0")
                self.end_headers()
            else:
                self.wfile.write(wire)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    capacity = CapacityConfig(rpc_line_bytes=16 << 20, http_header_bytes=limit)
    footprint = ProductionFootprint.admit("range", 1, capacity)
    budget = dict(body=2, disk=1, attempts=2, inflight=footprint.memory)
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
        with RustWorker(binary, capacity=capacity, job_budget=budget) as worker:
            request = dict(
                type="request",
                request_id="header",
                operation="fetch_range",
                budget=budget,
                payload=dict(production=transfer),
            )
            worker.send_raw(json.dumps(request).encode() + b"\n")
            response = worker.read_raw()
            assert response["ok"] is accepted
            assert len(hits) == 2
            if accepted:
                assert response["result"]["bytes"] == 1
                assert (tmp_path / "body.bin").read_bytes() == b"x"
            else:
                # The low-level worker creates staging before HTTP; no body was written.
                assert (tmp_path / "body.bin").read_bytes() == b""
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
