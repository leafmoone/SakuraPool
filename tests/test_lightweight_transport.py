"""Synthetic loopback coverage for the ledger-free shared production stack."""

import hashlib
import io
import json
import tarfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from sakurapool.capacity import CapacityConfig
from sakurapool.storage.modelscope import ListedFile, ModelScopeDataset
from sakurapool.storage.production import ProviderObject, RustProductionTransport
from sakurapool.storage.transport import BoundObject, GuardedTransport, RemoteIOError


@pytest.fixture
def loopback():
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as tar:
        for name, body in [("one.gif", b"GIF89a synthetic"), ("one.json", b'{"tags":["local"]}')]:
            member = tarfile.TarInfo(name)
            member.size = len(body)
            tar.addfile(member, io.BytesIO(body))
    state = {"raw": archive.getvalue(), "calls": [], "mode": "ok", "statuses": []}

    class CDN(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def do_GET(self):
            state["calls"].append(("cdn", dict(self.headers)))
            if (
                self.headers.get("If-Match") == '"sakurapool-deliberately-wrong-r2"'
                and state["mode"] != "ignore"
            ):
                self.send_response(412)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            ranged = self.headers.get("Range")
            start, end = map(int, ranged[6:].split("-")) if ranged else (0, len(state["raw"]) - 1)
            body = state["raw"][start : end + 1]
            self.send_response(206 if ranged else 200)
            self.send_header("Content-Length", str(len(body)))
            if ranged:
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(state['raw'])}")
            self.send_header("ETag", '"bound"')
            self.send_header("Content-Encoding", "gzip" if state["mode"] == "gzip" else "identity")
            self.end_headers()
            self.wfile.write(body)

    class Origin(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def do_GET(self):
            state["calls"].append(("origin", dict(self.headers)))
            if self.path == "/metadata":
                body = b'{"Code":200,"Data":{}}'
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            status = state["statuses"].pop(0) if state["statuses"] else 302
            self.send_response(status)
            if status == 302:
                target = state["cdn"] + "/object"
                if state["mode"] == "echo":
                    target += "?leak=synthetic-token"
                self.send_header("Location", target)
            self.send_header("Content-Length", "3")
            self.end_headers()
            self.wfile.write(b"ctl")

    servers = [ThreadingHTTPServer(("127.0.0.1", 0), cls) for cls in (Origin, CDN)]
    threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in servers]
    state["origin"], state["cdn"] = [f"http://127.0.0.1:{s.server_port}" for s in servers]
    for thread in threads:
        thread.start()
    try:
        yield state
    finally:
        for server in servers:
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join(2)


def make_transport(tmp_path, state, monkeypatch, **kwargs):
    import sakurapool.storage.budget as budget

    def forbidden(*_args, **_kwargs):
        raise AssertionError("lightweight must not construct ledger or scan physical tree")

    monkeypatch.setattr(budget.BudgetLedger, "__init__", forbidden)
    monkeypatch.setattr(budget, "_disk_usage", forbidden)
    worker = Path(__file__).parents[1] / "rust/target/debug/sakurapool-worker.exe"
    transport = RustProductionTransport(
        None, worker, root=tmp_path, origin=state["origin"], offline_mode=True, **kwargs
    )
    candidate = ProviderObject(
        "owner/dataset",
        "modelscope_dataset_legacy",
        state["origin"],
        "b" * 40,
        "tiny.tar",
        len(state["raw"]),
    )
    return transport, candidate


def test_shared_proof_range_rotation_clone_metadata(tmp_path, loopback, monkeypatch):
    transport, candidate = make_transport(tmp_path, loopback, monkeypatch)
    with transport:
        pid = transport._lane_worker.pid
        assert not loopback["calls"]
        obj = transport.verify_conditions(candidate)
        assert transport._lane_worker.pid == pid
        assert transport.verified_object(candidate) == obj
        bound = BoundObject(
            ModelScopeDataset(transport, obj.origin, obj.repo_id).download_url(
                obj.revision, obj.object_path
            ),
            obj.object_size,
            obj.revision,
            obj.validator,
            repository=obj.repo_id,
        )
        with transport.read_range_owned(bound, 0, 8) as raw:
            assert raw == loopback["raw"][:8]
        for _ in range(251):
            with transport.read_range_owned(bound, 0, 1) as raw:
                assert raw == loopback["raw"][:1]
        assert transport._generation == 1 and transport._lane_worker.pid != pid
        assert transport.verified_object(candidate) == obj
        with transport.clone() as clone:
            assert (
                clone.ledger is None
                and clone.capacity == transport.capacity
                and clone.root == tmp_path
            )
            assert clone.verify_conditions(candidate) == obj
        control = transport.metadata_control()
        assert control.ledger is None and control.offline_mode
        assert json.loads(control.read_metadata(loopback["origin"] + "/metadata"))["Code"] == 200
        entry = ListedFile("tiny.tar", obj.object_size, None, False, obj.revision)
        assert (
            ProviderObject.from_tree(ModelScopeDataset(control, obj.origin, obj.repo_id), entry)
            == candidate
        )
        assert "accounting" not in json.dumps(transport.last_result)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("mode", ["gzip", "ignore", "echo"])
def test_fail_closed_safe_diagnostic(tmp_path, loopback, monkeypatch, mode):
    transport, candidate = make_transport(tmp_path, loopback, monkeypatch, token="synthetic-token")
    loopback["mode"] = mode
    with transport, pytest.raises(RemoteIOError) as caught:
        transport.verify_conditions(candidate)
    diagnostic = caught.value.public_diagnostic()
    assert {"code", "phase", "recoverable", "delivery_safe"} <= diagnostic.keys()
    text = json.dumps(diagnostic) + str(caught.value)
    assert not diagnostic["delivery_safe"]
    assert all(
        word not in text for word in ("accounting", "consumption", "synthetic-token", "http://")
    )
    assert all(
        "Authorization" not in headers and "Cookie" not in headers
        for kind, headers in loopback["calls"]
        if kind == "cdn"
    )


def test_origin_retry_and_full_scan_modes(tmp_path, loopback, monkeypatch):
    transport, candidate = make_transport(tmp_path, loopback, monkeypatch)
    with transport:
        loopback["statuses"] = [400, 403, 302]
        obj = transport.verify_conditions(candidate)
        assert len([call for call in loopback["calls"] if call[0] == "origin"]) == 5
        for mode in ("remote-stream-scan", "download-then-scan"):
            with transport.transfer(obj, mode=mode) as (root, result):
                assert result["sha256"] == hashlib.sha256(loopback["raw"]).hexdigest()
                assert (root / "body").exists() == (mode == "download-then-scan")
    assert not list(tmp_path.iterdir())


def test_metadata_no_ledger_bounds_offline_and_clone(tmp_path, loopback):
    with GuardedTransport(
        None,
        trusted_hosts=frozenset({"127.0.0.1"}),
        offline_mode=True,
        allow_loopback_http=True,
        capacity=CapacityConfig(metadata_max_bytes=64),
    ) as transport:
        with transport.clone() as clone:
            assert clone.ledger is None
        assert transport.read_metadata(loopback["origin"] + "/metadata", max_bytes=64)
        with pytest.raises(RemoteIOError):
            transport.read_metadata(loopback["origin"] + "/metadata", max_bytes=2)
        with pytest.raises(RemoteIOError):
            transport.read_metadata("http://localhost:9/metadata", max_bytes=64)
    with pytest.raises(ValueError):
        GuardedTransport(
            None,
            trusted_hosts=frozenset({"127.0.0.1"}),
            offline_mode=True,
            token="synthetic",
            allow_loopback_http=True,
        )
