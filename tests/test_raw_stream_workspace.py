"""Loopback raw stream lifecycle in a new workspace, no provider access."""

import hashlib
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests

from sakurapool.capacity import CapacityConfig
from sakurapool.storage.budget import Reservation
from sakurapool.storage.transport import RawObjectStream
from sakurapool.workspace import Workspace


@pytest.mark.parametrize("fail_consumer", [False, True])
def test_workspace_raw_stream_lifecycle(tmp_path, fail_consumer):
    body = b"opaque" * 15000

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("ETag", '"v1"')
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    workspace = Workspace.init(tmp_path / "w", capacity=CapacityConfig(range_chunk_bytes=1))
    ledger = workspace.ledger()
    lease = ledger.reserve(Reservation(body=len(body) + 1, inflight=2 * 65536))
    response = requests.get(f"http://127.0.0.1:{server.server_port}/body", stream=True, timeout=5)
    stream = RawObjectStream(response, lease, ledger, len(body))
    try:
        if fail_consumer:
            with pytest.raises(RuntimeError, match="consumer"):
                try:
                    assert stream.read(5) == body[:5]
                    raise RuntimeError("consumer")
                finally:
                    response.close()
            assert workspace.inspect()["pending_count"] == 1
            assert ledger.status()["body"] == len(body) + 1
        else:
            assert stream.read(5) == body[:5]
            assert stream.drain_and_verify() == hashlib.sha256(body).hexdigest()
            response.close()
            ledger.settle(lease)
            assert workspace.inspect()["pending_count"] == 0
            assert ledger.status()["body"] == len(body)
            assert ledger.status()["inflight"] == 0
        assert stream.response.raw.closed
    finally:
        response.close()
        server.shutdown()
        thread.join()
        server.server_close()
