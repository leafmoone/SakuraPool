"""Same Rust two-hop path on loopback, no production fallback or real credentials."""

import hashlib
import io
import json
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from contextlib import closing
from dataclasses import asdict, replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from sakurapool.registry import DatasetAdapter
from sakurapool.storage.budget import DEFAULT_WORK_ROOT, BudgetLedger
from sakurapool.storage.modelscope import ModelScopeDataset
from sakurapool.storage.production import ProviderObject, RustProductionTransport, proof_key
from sakurapool.storage.rust_bridge import RustWorkerError
from sakurapool.storage.transport import BoundObject, RemoteIOError


def test_modelscope_legacy_control_plane_exact_identity_and_live_candidates():
    class Control:
        calls = []
        payload = None

        def _host(self, url):
            return "modelscope.cn"

        def read_metadata(self, url):
            self.calls.append(url)
            return json.dumps({"Code": 200, "Data": self.payload}).encode()

    control = Control()
    dataset = ModelScopeDataset(control, "https://modelscope.cn", "leafmoone/game_cg_5M")
    control.payload = {"Namespace": "other", "Name": "game_cg_5M", "Id": 17, "Type": 4}
    with pytest.raises(RemoteIOError):
        dataset.legacy_hub_id()
    with pytest.raises(ValueError):
        dataset.legacy_tree_page(17, "master", root="pre")
    control.payload["Namespace"] = "leafmoone"
    assert dataset.legacy_hub_id() == 17
    control.payload = {
        "Files": [{"Path": "pre/live.tar", "Type": "blob", "Size": 10240, "Revision": REV}],
        "TotalCount": 30,
    }
    files, complete = dataset.legacy_tree_page(17, "master", root="pre")
    assert files[0].revision_candidate == REV and not complete
    assert "/datasets/17/repo/tree?" in control.calls[-1]
    assert "PageSize=20" in control.calls[-1]
    again, complete = dataset.legacy_tree_page(17, REV, root="pre")
    assert again == files and not complete  # echo is candidate metadata, not conditional proof
    for field, invalid in [
        ("Revision", "c" * 40),
        ("Path", "other/a.tar"),
        ("Path", "pre/../outside.tar"),
        ("Size", -1),
    ]:
        entry = control.payload["Files"][0]
        original = entry[field]
        entry[field] = invalid
        with pytest.raises(RemoteIOError):
            dataset.legacy_tree_page(17, REV, root="pre")
        entry[field] = original


TOKEN = "synthetic-r2-secret"
REV = "b" * 40


@pytest.fixture
def twohop():
    from conftest import resolve_r1_worker

    r1_worker = resolve_r1_worker()
    assert r1_worker is not None, "R2 requires an explicitly built release worker"
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        for name, body in [("one.png", b"synthetic image"), ("one.json", b'{"tags":["r2"]}')]:
            entry = tarfile.TarInfo(name)
            entry.size = len(body)
            tar.addfile(entry, io.BytesIO(body))
    state = dict(raw=archive.getvalue(), mode="ok", calls=[], origin=None, cdn=None)

    class CDN(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def do_GET(self):
            state["calls"].append(("cdn", dict(self.headers)))
            wrong = self.headers.get("If-Match") == '"sakurapool-deliberately-wrong-r2"'
            if wrong and state["mode"] != "ignore-condition":
                self.send_response(412)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            ranged = self.headers.get("Range")
            raw = state["raw"]
            if ranged:
                start, end = [int(n) for n in ranged.removeprefix("bytes=").split("-")]
                body = raw[start : end + 1]
                status = 200 if state["mode"] == "200" else 206
            else:
                body = raw
                status = 200
            if state["mode"] == "cdn-redirect":
                self.send_response(302)
                self.send_header("Location", "http://127.0.0.1:9/escape")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(status)
            total = len(raw) + (1 if state["mode"] == "wrong-size" else 0)
            if ranged:
                self.send_header("Content-Range", f"bytes {start}-{end}/{total}")
            length = len(body) + (1 if state["mode"] == "long" else 0)
            self.send_header("Content-Length", str(length))
            if state["mode"] == "duplicate":
                self.send_header("Content-Length", str(length))
            self.send_header("Content-Encoding", "gzip" if state["mode"] == "gzip" else "identity")
            self.send_header(
                "ETag",
                '"independent-cookie-secret"'
                if state["mode"] == "cookie-etag"
                else '"other"'
                if state["mode"] == "etag"
                else '"r2-bound"',
            )
            self.end_headers()
            if state["mode"] == "short":
                self.wfile.write(body[:-1])
                self.close_connection = True
            else:
                self.wfile.write(body)
            self.wfile.flush()

    class Origin(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def do_GET(self):
            state["calls"].append(("origin", dict(self.headers)))
            assert self.headers.get("Authorization") == "Bearer " + TOKEN
            location = state["cdn"] + "/object"
            if state["mode"] == "echo":
                location += "?secret=" + TOKEN
            elif state["mode"] == "cookie-echo":
                location += "?leak=independent-cookie-secret"
            elif state["mode"] == "cookie-echo-encoded":
                location += "?leak=%69ndependent-cookie-secret"
            elif state["mode"] == "expired":
                location += "?Signature=fake&Expires=1"
            elif state["mode"] == "uncertain":
                location += "?Signature=fake"
            elif state["mode"] == "escaped":
                location += "?Signature=a%2Fb%3Dc%2Bd&Expires=" + str(int(time.time()) + 600)
            elif state["mode"] == "userinfo":
                location = location.replace("://", "://user@")
            elif state["mode"] == "nonloopback":
                location = "http://localhost:9/escape"
            elif state["mode"] == "encoded-host":
                location = location.replace("127.0.0.1", "127%2E0%2E0%2E1")
            elif state["mode"] == "fragment":
                location += "#oops"
            elif state["mode"] == "path-escape":
                location += "/%2Fobject"
            self.send_response(302)
            self.send_header("Location", location)
            self.send_header("Content-Length", "0")
            self.end_headers()

    origin = ThreadingHTTPServer(("127.0.0.1", 0), Origin)
    cdn = ThreadingHTTPServer(("127.0.0.1", 0), CDN)
    state["origin"] = f"http://127.0.0.1:{origin.server_port}"
    state["cdn"] = f"http://127.0.0.1:{cdn.server_port}"
    threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in (origin, cdn)]
    for thread in threads:
        thread.start()
    with tempfile.TemporaryDirectory(prefix="offline-r2-", dir=DEFAULT_WORK_ROOT) as temp:
        ledger = BudgetLedger(Path(temp), _offline_test=True)
        transport = RustProductionTransport(
            ledger,
            r1_worker,
            origin=state["origin"],
            token=TOKEN,
            same_origin_cookie="synthetic-cookie=independent-cookie-secret",
            _test=True,
        )
        obj = ProviderObject(
            "leafmoone/game_cg_5M",
            "modelscope_dataset_legacy",
            state["origin"],
            REV,
            "tiny.tar",
            len(state["raw"]),
        )
        yield state, ledger, transport, obj
    for s in (origin, cdn):
        s.shutdown()
        s.server_close()
    for t in threads:
        t.join(2)


def test_rust_auth_stripping_conditional_proof_and_real_bytes(twohop):
    state, ledger, transport, obj = twohop
    verified = transport.verify_conditions(obj)
    assert ledger.condition_proof(proof_key(verified, test=True))
    assert len(state["calls"]) == 6
    for hop, headers in state["calls"]:
        assert {k.lower(): v for k, v in headers.items()}["range"] == "bytes=0-0"
        if hop == "cdn":
            assert not any(k.lower() in ("authorization", "cookie", "referer") for k in headers)
        else:
            assert {k.lower(): v for k, v in headers.items()}[
                "cookie"
            ] == "synthetic-cookie=independent-cookie-secret"
    bound = BoundObject(
        ModelScopeDataset(transport, obj.origin, obj.repo_id).download_url(REV, obj.object_path),
        obj.object_size,
        REV,
        verified.validator,
        repository=obj.repo_id,
    )
    with transport.read_range_owned(bound, 512, 15) as body:
        assert body == state["raw"][512:527]
    assert ledger.status()["body"] == 17
    assert ledger.status()["attempts"] == 8
    assert ledger.status()["inflight"] == 0
    assert not list(ledger.root.glob("rust-transfer-*"))
    assert TOKEN.encode() not in b"".join(p.read_bytes() for p in ledger.slots)


@pytest.mark.parametrize(
    "mode",
    [
        "echo",
        "expired",
        "uncertain",
        "userinfo",
        "nonloopback",
        "encoded-host",
        "fragment",
        "path-escape",
    ],
)
def test_location_fail_closed_before_cdn(twohop, mode):
    state, ledger, transport, obj = twohop
    state["mode"] = mode
    with pytest.raises(RemoteIOError, match="rejected") as error:
        with transport.transfer(obj, condition="observe"):
            pass
    assert TOKEN not in str(error.value)
    assert len(state["calls"]) == 1
    assert ledger.status()["body"] == 0
    assert ledger.status()["inflight"] == 0


def test_approved_encoded_signature_stays_memory_only(twohop):
    state, ledger, transport, obj = twohop
    state["mode"] = "escaped"
    with transport.transfer(obj, condition="observe") as (_, result):
        assert result["bytes"] == 1
        assert "Signature" not in json.dumps(result)
    assert len(state["calls"]) == 2
    assert all(b"Signature" not in p.read_bytes() for p in ledger.slots)


@pytest.mark.parametrize("mode", ["200", "wrong-size", "long", "gzip", "duplicate", "cdn-redirect"])
def test_range_headers_reject_without_body_or_fallback(twohop, mode):
    state, ledger, transport, obj = twohop
    state["mode"] = mode
    with pytest.raises(RemoteIOError):
        with transport.transfer(obj, condition="observe"):
            pass
    assert len(state["calls"]) == 2
    # Nonempty unread rejected bodies are unknown, not zero. Empty 302 is known.
    assert ledger.status()["body"] == (0 if mode == "cdn-redirect" else 2)
    assert ledger.status()["inflight"] == 0
    diagnostic = transport.last_result["diagnostic"]
    assert set(diagnostic) == {"phase", "http_status", "read_bytes", "accounting_complete"}
    assert diagnostic["phase"] == "cdn"
    assert diagnostic["http_status"] == (
        200 if mode == "200" else 302 if mode == "cdn-redirect" else 206
    )
    assert diagnostic["read_bytes"] == 0
    assert diagnostic["accounting_complete"] == (mode == "cdn-redirect")
    public = json.dumps(transport.last_result)
    assert TOKEN not in public and "Signature=" not in public and state["cdn"] not in public


@pytest.mark.parametrize("mode", ["cookie-echo", "cookie-echo-encoded", "cookie-etag"])
def test_independent_cookie_value_echo_never_leaks_from_rust(twohop, mode):
    state, _ledger, transport, obj = twohop
    state["mode"] = mode
    with pytest.raises(RemoteIOError):
        with transport.transfer(obj, condition="observe"):
            pass
    assert len(state["calls"]) == (2 if mode == "cookie-etag" else 1)
    result = transport.last_result
    assert result.get("etag") is None
    assert result["production_error"] in (
        "location_invalid",
        "location_encoding",
        "validator_mismatch",
    )
    assert "synthetic-cookie" not in json.dumps(result) and "leak=" not in json.dumps(result)


def test_production_package_memory_and_validation_precede_credentials(twohop, monkeypatch):
    from sakurapool.storage import production_cli
    from sakurapool.storage.budget import BudgetExceeded
    from sakurapool.storage.package import PackageCorrupt, admitted_package

    state, ledger, _transport, _obj = twohop
    package = ledger.root / "bad-package"
    package.mkdir()
    manifest = package / "index-package.json"
    manifest.write_text("{}")
    monkeypatch.setattr(production_cli, "BudgetLedger", lambda *_: ledger)
    calls = []
    monkeypatch.setattr(production_cli, "load_profile", lambda *_: calls.append(True))
    with pytest.raises(PackageCorrupt):
        production_cli.fetch(None, package, "bad", ledger.root / "output")
    assert not calls and not state["calls"] and ledger.status()["inflight"] == 0
    manifest.write_bytes(b" " * ((1 << 20) + 1))
    with pytest.raises(BudgetExceeded):
        with admitted_package(package, ledger):
            pass
    assert not calls and not state["calls"] and ledger.status()["inflight"] == 0


def test_wrong_etag_and_wrong_size_reject(twohop):
    state, ledger, transport, obj = twohop
    obj = replace(obj, validator='"expected"')
    with pytest.raises(RemoteIOError):
        with transport.transfer(obj, condition="observe"):
            pass
    assert ledger.status()["body"] == 2  # unread response: reservation stays pending
    state["calls"].clear()
    with pytest.raises(RemoteIOError):
        with transport.transfer(
            replace(obj, validator=None, object_size=obj.object_size + 1), condition="observe"
        ):
            pass
    assert len(state["calls"]) == 2


def test_short_body_known_prefix_and_unknown_remainder_retained(twohop):
    state, ledger, transport, obj = twohop
    state["mode"] = "short"
    with pytest.raises(RemoteIOError):
        with transport.transfer(obj, length=10, condition="observe"):
            pass
    # Actual prefix charged; the unobserved remainder is not silently refunded.
    assert ledger.status()["body"] == 11
    assert ledger.status()["inflight"] == 0
    assert ledger.status()["attempts"] == 2


def test_ignore_conditional_does_not_write_production_proof(twohop):
    state, ledger, transport, obj = twohop
    state["mode"] = "ignore-condition"
    with pytest.raises(RemoteIOError):
        transport.verify_conditions(obj)
    candidate = replace(obj, validator='"r2-bound"', cdn_host="127.0.0.1")
    assert ledger.condition_proof(proof_key(candidate, test=True)) is None
    with pytest.raises(RemoteIOError, match="verified"):
        with transport.transfer(candidate):
            pass


def test_fresh_worker_and_ledger_reopen_need_exact_scope(twohop):
    state, ledger, transport, obj = twohop
    bound = transport.verify_conditions(obj)
    fresh_ledger = BudgetLedger(ledger.root, _offline_test=True)
    fresh = RustProductionTransport(
        fresh_ledger, transport.worker, origin=obj.origin, token=TOKEN, _test=True
    )
    fresh.register(bound)
    with fresh.transfer(bound, start=512, length=15) as (root, _):
        assert (root / "body").read_bytes() == state["raw"][512:527]
    for changed in [
        replace(bound, revision="a" * 40),
        replace(bound, object_size=5),
        replace(bound, object_path="other.tar"),
        replace(bound, cdn_host="foreign"),
    ]:
        with pytest.raises((RemoteIOError, ValueError)):
            fresh.register(changed)


@pytest.mark.parametrize("mode", ["download-then-scan", "remote-stream-scan"])
def test_two_admin_modes_same_file_audit_and_stage_contract(twohop, mode):
    state, ledger, transport, obj = twohop
    bound = transport.verify_conditions(obj)
    adapter = DatasetAdapter("r2", "synthetic", storage_id="remote1", image_extensions=(".png",))
    stage = ledger.root / mode
    result = transport.build_stage(bound, adapter, stage, mode=mode)
    assert result.content_sha256 == hashlib.sha256(state["raw"]).hexdigest()
    assert (stage / "stage.complete").is_file()
    with closing(sqlite3.connect(result.database)) as db:
        rows = db.execute("select name,kind,size from members order by name").fetchall()
    assert rows == [("one.json", "json", 15), ("one.png", "image", 15)]
    assert not list(ledger.root.glob("rust-transfer-*"))
    assert ledger.status()["inflight"] == 0
    # Exact same audited P2/P3/package chain in both administrator modes.
    from sakurapool.runtime.compiler import compile_runtime
    from sakurapool.runtime.inventory import load_p2_inventory
    from sakurapool.runtime.query import RuntimeQuerySpec
    from sakurapool.runtime.snapshot import RuntimeSnapshot
    from sakurapool.storage.package import Binding, fetch_from_package, publish_local_package
    from sakurapool.storage.remote_index import write_staged_v4

    package = ledger.root / "package"
    package.mkdir()
    (package / "audit").mkdir()
    provider = ModelScopeDataset(transport, obj.origin, obj.repo_id)
    resolved = BoundObject(
        provider.download_url(REV, obj.object_path),
        obj.object_size,
        REV,
        bound.validator,
        repository=obj.repo_id,
    )
    summary = write_staged_v4(
        ledger,
        [(obj.object_path, resolved, result)],
        package / "durable",
        adapter,
        audit_output=package / "audit/members.json",
    )
    assert summary["samples"] == 1 and summary["errors"] == 0
    compile_runtime(load_p2_inventory(package / "durable"), package / "runtime")
    with RuntimeSnapshot.open(package / "runtime", full_verify=True) as rt:
        assert rt.query(RuntimeQuerySpec(all_tags=[("tags", "r2")])).count() == 1
        ref = rt.object_ref(0)
    binding = Binding(
        adapter.dataset,
        adapter.storage_id,
        ref["object_id"],
        obj.object_path,
        obj.object_size,
        result.content_sha256,
        bound.validator,
        True,
        ledger.condition_proof(proof_key(bound, test=True)),
        False,
    )
    publish_local_package(
        package,
        ledger,
        endpoint=obj.origin,
        data_revision=REV,
        bindings=[binding],
        production_transport=transport,
    )
    audit = json.loads((package / "audit/members.json").read_bytes())
    output = ledger.root / "images"
    output.mkdir()
    dest = fetch_from_package(package, next(iter(audit)), output, transport)
    assert (dest / "image.png").read_bytes() == b"synthetic image"
    assert (dest / "metadata.json").read_bytes() == b'{"tags":["r2"]}'
    assert not list(ledger.root.glob("rust-transfer-*"))
    # New interpreter/worker, same durable proof, no parent Python object reuse.
    output_child = ledger.root / "fresh-images"
    output_child.mkdir()
    payload = {
        "ledger": str(ledger.root),
        "worker": str(transport.worker),
        "object": asdict(bound),
        "token": TOKEN,
        "package": str(package),
        "output": str(output_child),
        "record_id": next(iter(audit)),
    }
    code = """
import json, sys
from pathlib import Path
from sakurapool.storage.budget import BudgetLedger
from sakurapool.storage.production import ProviderObject, RustProductionTransport
from sakurapool.storage.package import fetch_from_package
p=json.load(sys.stdin)
ledger=BudgetLedger(Path(p["ledger"]), _offline_test=True)
obj=ProviderObject(**p["object"])
t=RustProductionTransport(ledger, Path(p["worker"]), origin=obj.origin,
                          token=p["token"], _test=True)
t.register(obj)
output=fetch_from_package(Path(p["package"]),p["record_id"],Path(p["output"]),t)
assert (output/"image.png").read_bytes()==b"synthetic image"
assert (output/"metadata.json").read_bytes()==b'{"tags":["r2"]}'
assert ledger.status()["inflight"]==0
print(json.dumps({"status":"fresh_process_exact_bytes", "python_isolated":sys.flags.isolated}))
"""
    before = len(state["calls"])
    child = subprocess.run(
        [sys.executable, "-I", "-c", code],
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        cwd=ledger.root,
        timeout=180,
    )
    assert child.returncode == 0, child.stderr
    assert json.loads(child.stdout) == {"status": "fresh_process_exact_bytes", "python_isolated": 1}
    assert len(state["calls"]) == before + 2  # one merged Range; no repeat proof/download/scan
    assert not list(ledger.root.glob("rust-transfer-*"))


@pytest.mark.parametrize("mode", ["download-then-scan", "remote-stream-scan"])
def test_administrator_cli_modes_obey_combined_capacity_before_metadata(
    twohop, monkeypatch, capsys, mode
):
    from dataclasses import asdict

    from sakurapool.cli import main
    from sakurapool.storage import production_cli

    state, ledger, transport, obj = twohop
    obj = transport.verify_conditions(obj)
    config = {"profile": "modelscope_https_v1"}
    monkeypatch.setattr(production_cli, "load_profile", lambda _: (config, obj, transport))
    called = []
    monkeypatch.setattr(production_cli, "verify_tree_object", lambda *_: called.append(True))
    ledger.limits["disk"] = min(ledger.limits["disk"], ledger.status()["disk"] + (2 << 30))
    before = len(state["calls"])
    plan = ledger.root / "admin-plan.json"
    adapter = DatasetAdapter("r2", "synthetic")
    stage = ledger.root / "admin-stage"
    plan.write_text(json.dumps({"adapter": asdict(adapter), "stage": str(stage)}))
    durable = ledger.root / "admin-durable"
    assert (
        main(
            [
                "index",
                "scan-remote",
                "--config",
                "explicit.json",
                "--plan",
                str(plan),
                "--output-package",
                str(durable),
                "--mode",
                mode,
            ]
        )
        == 3
    )
    response = json.loads(capsys.readouterr().out)
    assert response["status"] == "BLOCKED"
    assert not called and len(state["calls"]) == before
    assert not durable.exists() and not stage.exists()
    assert ledger.status()["inflight"] == 0


def test_administrator_combined_large_working_set_blocks_before_metadata(twohop, monkeypatch):
    from dataclasses import asdict

    from sakurapool.storage import production_cli

    state, ledger, transport, obj = twohop
    obj = replace(transport.verify_conditions(obj), object_size=8 << 20)
    ledger.record_condition_proof(proof_key(obj, test=True), "0" * 64)
    monkeypatch.setattr(production_cli, "load_profile", lambda _: ({}, obj, transport))
    called = []
    monkeypatch.setattr(production_cli, "verify_tree_object", lambda *_: called.append(True))
    before = len(state["calls"])
    plan = ledger.root / "admin-plan.json"
    plan.write_text(
        json.dumps(
            {
                "adapter": asdict(DatasetAdapter("r2", "synthetic")),
                "stage": str(ledger.root / "stage"),
            }
        )
    )
    with pytest.raises(Exception) as error:
        production_cli.scan(None, plan, ledger.root / "durable", "remote-stream-scan")
    from sakurapool.storage.budget import BudgetExceeded

    assert isinstance(error.value, BudgetExceeded)
    assert not called and len(state["calls"]) == before
    assert not (ledger.root / "durable").exists()


def test_budget_gate_precedes_network_and_artifacts(twohop):
    state, ledger, transport, obj = twohop
    bound = transport.verify_conditions(obj)
    before = len(state["calls"])
    ledger.limits["inflight"] = 65536  # Test-only tightened cap, never production.
    from sakurapool.storage.budget import BudgetExceeded

    with pytest.raises(BudgetExceeded):
        with transport.transfer(bound):
            pass
    assert len(state["calls"]) == before
    assert not list(ledger.root.glob("rust-transfer-*"))


def test_worker_crash_does_not_refund_pending_body(twohop, monkeypatch):
    _, ledger, transport, obj = twohop
    from sakurapool.storage import production

    class Crash:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            raise RustWorkerError("worker died")

        def __exit__(self, *_args):
            pass

    monkeypatch.setattr(production, "RustWorker", Crash)
    with pytest.raises(RustWorkerError):
        with transport.transfer(obj, condition="observe"):
            pass
    assert ledger.status()["body"] == 2
    assert ledger.status()["attempts"] == 2
    assert ledger.status()["inflight"] == 0


@pytest.mark.parametrize("mode", ["download-then-scan", "remote-stream-scan"])
def test_partial_or_invalid_fullstream_never_marks_stage_complete(twohop, mode):
    state, ledger, transport, obj = twohop
    bound = transport.verify_conditions(obj)
    state["raw"] = b"X" * len(state["raw"])
    before = ledger.status()["body"]
    with pytest.raises(RemoteIOError):
        transport.build_stage(
            bound, DatasetAdapter("r2", "synthetic"), ledger.root / "bad-stage", mode=mode
        )
    assert not (ledger.root / "bad-stage/stage.complete").exists()
    assert not list(ledger.root.glob("rust-transfer-*"))
    if mode == "download-then-scan":
        assert ledger.status()["body"] - before == obj.object_size
    else:
        assert ledger.status()["body"] - before == obj.object_size + 1  # early parser stop: unknown


def test_stage_traversal_and_scope_changes_rejected_before_network(twohop):
    state, ledger, transport, obj = twohop
    before = len(state["calls"])
    with pytest.raises(ValueError):
        transport.build_stage(
            obj,
            DatasetAdapter("r2", "synthetic"),
            ledger.root / ".." / "outside",
            mode="remote-stream-scan",
        )
    with pytest.raises(RemoteIOError):
        with transport.transfer(obj):
            pass
    assert len(state["calls"]) == before


def test_proxy_environment_not_used_by_either_hop(twohop, monkeypatch):
    state, _ledger, transport, obj = twohop
    for var in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
        monkeypatch.setenv(var, "http://127.0.0.1:9")
    monkeypatch.setenv("NO_PROXY", "")
    with transport.transfer(obj, condition="observe") as (_, result):
        assert result["bytes"] == 1
    assert len(state["calls"]) == 2
