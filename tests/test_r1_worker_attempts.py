"""A worker request must not hide HTTP retries behind one durable attempt."""

import http.server
import threading

import pytest
from conftest import resolve_r1_worker

from sakurapool.storage.rust_bridge import RustWorker, RustWorkerError

WORKER = resolve_r1_worker()


@pytest.mark.skipif(not WORKER, reason="sakurapool-worker binary not found")
@pytest.mark.parametrize("operation", ["fetch_range", "scan_http_tar"])
def test_worker_does_not_retry_503(operation):
    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        hits = 0

        def log_message(self, *_args):
            pass

        def do_GET(self):
            type(self).hits += 1
            self.send_response(503)
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.end_headers()

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_address[1]}/synthetic"
    budget = {"body": 4096, "disk": 0, "inflight": 4096, "attempts": 1}
    payload = {"url": url}
    if operation == "fetch_range":
        payload.update(start=0, end=7, total=32)
    try:
        with RustWorker(WORKER, job_budget=budget) as worker:
            with pytest.raises(RustWorkerError, match="worker rejected request"):
                worker.request(operation, budget=budget, payload=payload)
        assert Handler.hits == 1, "one admitted request must not silently make retry attempts"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
