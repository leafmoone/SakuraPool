"""C2 coordinator ownership and bounded synthetic lane differential."""

import json
import os
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import get_ident

import pytest
from test_r2_production import twohop as _twohop
from test_task_multiple import multiple as _multiple

from sakurapool.tasks.export import export_task
from sakurapool.tasks.runner import run_task
from sakurapool.tasks.store import TaskDB, TaskError

multiple = _multiple
twohop = _twohop


@pytest.mark.parametrize("invalidate", [False, True])
def test_real_lane_warm_range_with_cold_ledger_credit_unavailable(
        multiple, twohop, monkeypatch, invalidate):
    from dataclasses import replace

    from sakurapool.runtime import RuntimeQuerySpec
    from sakurapool.storage import publication_fetch
    from sakurapool.storage.budget import Reservation
    from sakurapool.storage.modelscope import ModelScopeDataset
    from sakurapool.storage.production import RustProductionTransport
    from sakurapool.storage.publication import load_publication
    from sakurapool.tasks.plan import Selection
    from sakurapool.tasks.runner import create_task

    old, ledger, _, _ = multiple
    state, _, rust, template = twohop
    with TaskDB(old, readonly=True) as db:
        pub_path = db.meta("publication_path")
    with load_publication(pub_path, full_verify=True) as pub:
        payload = bytearray(b"x" * 10240)
        for rid in range(pub.runtime.rid_count):
            offset = pub.runtime.location(rid)["image_offset"]
            payload[offset:offset + 5] = b"image"
    state["raw"] = bytes(payload)
    directory = ledger.root / "warm-credit-task"
    with create_task(pub_path, directory, ledger, RuntimeQuerySpec(),
                     Selection(mode="first", limit=2)):
        pass
    bindings = []
    verified_lanes = []
    monkeypatch.setattr(publication_fetch, "exact_provider_lookup",
        lambda control, endpoint, repo, revision, path, size, digest:
        replace(template, repo_id=repo, revision=revision,
                object_path=path, object_size=size))
    monkeypatch.setattr(ModelScopeDataset, "download_url", lambda self, revision, path:
        template.origin + "/object?Revision=" + revision + "&FilePath=" + path)
    original = RustProductionTransport.verify_conditions

    def proof(lane, obj):
        result = original(lane, obj)
        bindings.append((id(lane), lane._generation, obj.object_path))
        verified_lanes.append(lane)
        return result

    monkeypatch.setattr(RustProductionTransport, "verify_conditions", proof)
    predict = RustProductionTransport.predict_warm

    def synthetic_origin(lane, identity, lengths):
        # This fixture's verified publication uses HTTPS while its explicitly
        # test-only byte transport is strict loopback; translate only this fixture.
        return predict(lane, (template.origin, *identity[1:]), lengths)

    monkeypatch.setattr(RustProductionTransport, "predict_warm", synthetic_origin)
    occupied = []

    def hook(event, payload):
        if event == "SETTLED" and not occupied:
            # Real reserved body leaves precisely the next 5-byte Range plus
            # framing byte. Cold metadata/probe topology cannot fit.
            remaining = ledger.limits["body"] - ledger.status()["body"]
            occupied.append(ledger.reserve(Reservation(body=remaining - 6)))
            if invalidate:
                # A proof invalidated in the real transport cannot be inferred
                # warm merely because the publication-side cache still exists.
                verified_lanes[0]._live_proofs.clear()

    transport = RustProductionTransport(ledger, rust.worker, origin=template.origin,
        token=rust._token, same_origin_cookie=rust._cookie, _test=True)
    try:
        if invalidate:
            with pytest.raises(TaskError, match="RESOURCE_BLOCKED"):
                run_task(directory, transport, control=object(), workers=1, fault_hook=hook)
            with TaskDB(directory, readonly=True) as task:
                assert task.inspect()["delivered_confirmed"] == 1
        else:
            result = run_task(directory, transport, control=object(), workers=1, fault_hook=hook)
            assert result["state"] == "COMPLETED" and result["delivered_confirmed"] == 2
        assert len(bindings) == 1 and bindings[0][1] == 0
    finally:
        transport.close()
        for lease in occupied:
            ledger.settle(lease)
    assert ledger.status()["inflight"] == 0


@pytest.mark.parametrize("workers", [1, 2, 4])
def test_real_rust_lanes_with_owner_ledger(multiple, twohop, monkeypatch, workers):
    from dataclasses import replace

    from sakurapool.storage import publication_fetch
    from sakurapool.storage.production import RustProductionTransport

    directory, ledger, _, _ = multiple
    state, _, rust_transport, object_template = twohop
    from sakurapool.storage.publication import load_publication
    with TaskDB(directory, readonly=True) as task:
        pub_path = task.meta("publication_path")
    payload = bytearray(b"x" * 10240)
    with load_publication(pub_path, full_verify=True) as pub:
        for rid in range(pub.runtime.rid_count):
            offset = pub.runtime.location(rid)["image_offset"]
            payload[offset:offset + 5] = b"image"
    state["raw"] = bytes(payload)
    owner = get_ident()
    for name in ("event", "finish_item", "claim"):
        original_task = getattr(TaskDB, name)

        def owned_task(task, *args, _original=original_task, **kwargs):
            assert get_ident() == owner
            return _original(task, *args, **kwargs)

        monkeypatch.setattr(TaskDB, name, owned_task)
    for name in ("reserve", "consume_body", "settle", "status",
                 "condition_proof", "record_condition_proof"):
        original = getattr(ledger, name)

        def owned(*args, _original=original, **kwargs):
            assert get_ident() == owner
            return _original(*args, **kwargs)

        monkeypatch.setattr(ledger, name, owned)

    def lookup(control, endpoint, repo, revision, path, size, digest):
        return replace(object_template, repo_id=repo, revision=revision,
                       object_path=path, object_size=size)

    monkeypatch.setattr(publication_fetch, "exact_provider_lookup", lookup)
    # Publication is synthetic HTTPS; byte-plane binding below remains strict
    # loopback with the same independent conditional checks and artifacts.
    def bound_url(self, revision, path):
        return object_template.origin + "/object?Revision=" + revision + "&FilePath=" + path

    from sakurapool.storage.modelscope import ModelScopeDataset
    monkeypatch.setattr(ModelScopeDataset, "download_url", bound_url)
    transport = RustProductionTransport(ledger, rust_transport.worker,
        origin=object_template.origin, token=rust_transport._token,
        same_origin_cookie=rust_transport._cookie, _test=True)
    proof_calls = []
    verify = RustProductionTransport.verify_conditions

    def counted_proof(lane, obj):
        verified = verify(lane, obj)
        proof_calls.append((obj.object_path, id(lane), lane._generation))
        return verified

    monkeypatch.setattr(RustProductionTransport, "verify_conditions", counted_proof)
    predict = RustProductionTransport.predict_warm
    monkeypatch.setattr(RustProductionTransport, "predict_warm",
        lambda lane, identity, lengths:
        predict(lane, (object_template.origin, *identity[1:]), lengths))
    # Synthetic offsets are even multiples of 4096, yielding repeated image.
    result = run_task(directory, transport, control=object(), workers=workers)
    assert result["state"] == "COMPLETED"
    assert result["delivered_confirmed"] == 3
    assert len(proof_calls) == 2
    assert len({path for path, _, _ in proof_calls}) == 2
    assert all(generation == 0 for _, _, generation in proof_calls)
    assert ledger.status()["inflight"] == 0


@pytest.mark.parametrize("workers", [1, 2, 4])
def test_real_rust_metadata_enabled(multiple, twohop, monkeypatch, tmp_path, workers):
    import hashlib
    from dataclasses import replace

    import pyarrow as pa
    import pyarrow.parquet as pq
    from synthetic_p2 import ObjectSpec, SampleSpec, build_p2_directory

    from sakurapool.indexer import _json
    from sakurapool.runtime import RuntimeQuerySpec, compile_runtime, load_p2_inventory
    from sakurapool.storage import publication_fetch
    from sakurapool.storage.modelscope import ModelScopeDataset
    from sakurapool.storage.production import RustProductionTransport
    from sakurapool.storage.publication import build_publication, load_publication, sha
    from sakurapool.tasks.runner import create_task

    _, ledger, _, _ = multiple
    state, _, rust, template = twohop
    p2 = tmp_path / "metadata-p2"
    build_p2_directory(p2, dataset="meta", source="synthetic", objects=[
        ObjectSpec("meta.tar", [SampleSpec(f"{i}.png", str(i), has_json=True)
                                for i in range(3)], size=10240)])
    contract = json.loads((p2 / "INPUT.json").read_bytes())
    contract["hash_images"] = True
    (p2 / "INPUT.json").write_bytes(_json(contract))
    # Build and verify a genuine publication with both extents, not edited receipts.
    for marker in p2.glob("*.COMMIT"):
        commit = json.loads(marker.read_bytes())
        info = commit["files"]["samples"]
        path = p2 / info["path"]
        table = pq.read_table(path)
        for name, value in (("sha256", hashlib.sha256(b"image").hexdigest()),
                            ("hash_source", "computed:sha256"), ("hash_kind", "sha256")):
            index = table.schema.get_field_index(name)
            table = table.set_column(index, table.schema.field(index),
                                     pa.array([value] * table.num_rows,
                                              type=table.schema.field(index).type))
        index = table.schema.get_field_index("json_offset_data")
        table = table.set_column(index, table.schema.field(index),
                                 pa.array([512] * table.num_rows, type=pa.uint64()))
        pq.write_table(table, path)
        info.update(bytes=path.stat().st_size, sha256=sha(path))
        commit["contract_sha256"] = hashlib.sha256(_json(contract)).hexdigest()
        marker.write_bytes(_json(commit))
    runtime = tmp_path / "metadata-runtime"
    compile_runtime(load_p2_inventory(p2), runtime)
    roots = tmp_path / "metadata-roots.json"
    roots.write_text(json.dumps({"format": "sakurapool-p2-root-list-v1", "roots": [str(p2)]}))
    mapping = tmp_path / "metadata-mapping.jsonl"
    mapping.write_text("\n".join(json.dumps({
        "dataset_id": "meta", "endpoint": "https://modelscope.cn", "repo_id": "synthetic/meta",
        "repo_type": "modelscope_dataset_legacy", "revision_candidate": "b" * 40,
        "object_path": name, "object_size": 10240, "provider_sha256": value["sha256"],
    }) for name, value in contract["inputs"].items()))
    pub_path = tmp_path / "metadata-publication"
    build_publication(runtime, roots, mapping, pub_path)
    # Match the existing synthetic fixture's 5-byte image projection and add
    # a 2-byte metadata projection; this is NOT unmodified runtime evidence.
    from sakurapool.runtime import RuntimeSnapshot
    synthetic_location = RuntimeSnapshot.location
    monkeypatch.setattr(RuntimeSnapshot, "location", lambda self, rid: dict(
        synthetic_location(self, rid), flags=1, metadata_size=2))
    with load_publication(pub_path, full_verify=True) as pub:
        payload = bytearray(b"x" * 10240)
        for rid in range(pub.runtime.rid_count):
            loc = pub.runtime.location(rid)
            payload[loc["image_offset"]:loc["image_offset"] + loc["image_size"]] = b"image"
            payload[loc["metadata_offset"]:loc["metadata_offset"] + 2] = b"{}"
        state["raw"] = bytes(payload)
    directory = ledger.root / "metadata-task"
    with create_task(pub_path, directory, ledger, RuntimeQuerySpec(), metadata=True):
        pass
    monkeypatch.setattr(publication_fetch, "exact_provider_lookup",
        lambda control, endpoint, repo, revision, path, size, digest:
        replace(template, repo_id=repo, revision=revision, object_path=path, object_size=size))
    monkeypatch.setattr(ModelScopeDataset, "download_url",
        lambda self, revision, path:
        template.origin + "/object?Revision=" + revision + "&FilePath=" + path)
    transport = RustProductionTransport(ledger, rust.worker, origin=template.origin,
                                      token=rust._token, same_origin_cookie=rust._cookie,
                                      _test=True)
    result = run_task(directory, transport, workers=workers, control=object())
    assert result["delivered_confirmed"] == 3
    assert len(list((directory / "output").glob("*/metadata.json"))) == 3


@pytest.mark.parametrize("workers", [1, 2, 4])
def test_synthetic_pipeline_owner_and_content(multiple, monkeypatch, workers):
    import weakref

    from sakurapool.storage.prepared_fetch import PreparedFetch

    live = []
    peak = [0]
    prepare = PreparedFetch._prepare

    def observed_prepare(pub, record_id):
        descriptor = prepare(pub, record_id)
        live[:] = [reference for reference in live if reference() is not None]
        live.append(weakref.ref(descriptor))
        peak[0] = max(peak[0], len(live))
        return descriptor

    monkeypatch.setattr(PreparedFetch, "_prepare", observed_prepare)
    directory, ledger, Transport, calls = multiple
    owner = get_ident()
    for name in ("event", "finish_item", "claim"):
        original_task = getattr(TaskDB, name)

        def owned_task(task, *args, _original=original_task, **kwargs):
            assert get_ident() == owner
            return _original(task, *args, **kwargs)

        monkeypatch.setattr(TaskDB, name, owned_task)
    for name in ("reserve", "consume_body", "settle", "status",
                 "condition_proof", "record_condition_proof"):
        original = getattr(ledger, name)

        def owned(*args, _original=original, **kwargs):
            assert get_ident() == owner
            return _original(*args, **kwargs)

        monkeypatch.setattr(ledger, name, owned)
    Transport.clone = lambda self: Transport()
    Transport.close = lambda self: None
    result = run_task(directory, Transport(), control=object(), workers=workers)
    assert result["state"] == "COMPLETED"
    assert result["delivered_confirmed"] == 3
    assert result["unknown_accounting_count"] == 0
    # Active lane descriptors plus previous/current transient preparation.
    # No gc collection is forced, and weakrefs do not retain the descriptors.
    assert peak[0] <= workers + 2


def test_out_of_order_completion_export_stays_frozen(multiple):
    directory, ledger, Transport, calls = multiple
    Transport.clone = lambda self: Transport()
    Transport.close = lambda self: None
    original = Transport.read_range_owned
    completions = []

    @contextmanager
    def delayed(self, bound, offset, size):
        if "one.tar" in bound.url:
            time.sleep(0.1)
        with original(self, bound, offset, size) as payload:
            yield payload

    Transport.read_range_owned = delayed

    def hook(event, payload):
        if event == "CLAIMED":
            completions.append(("claimed", payload["seq"]))
        if event == "SETTLED":
            completions.append(("settled", None))

    result = run_task(directory, Transport(), control=object(), workers=4, fault_hook=hook)
    assert result["delivered_confirmed"] == 3
    export_task(directory, directory / "export.jsonl", ledger)
    rows = [json.loads(line) for line in (directory / "export.jsonl").read_text().splitlines()]
    with TaskDB(directory, readonly=True) as task:
        expected = [row[0] for row in task.db.execute("SELECT record_id FROM items ORDER BY seq")]
    assert [row["record_id"] for row in rows] == expected


def test_persistent_worker_reconnects_after_server_keepalive_close(twohop):
    state, ledger, transport, obj = twohop
    state["close_keepalive"] = True
    transport.enable_persistent()
    verified = transport.verify_conditions(obj)
    child = transport._lane_worker
    pid = child.pid
    for _ in range(3):
        with transport._transfer_owned(verified, start=512, length=16) as (_, _, payload):
            assert payload == state["raw"][512:528]
        assert transport._lane_worker is child and child.pid == pid
    assert transport._lane_requests == 6
    transport.close()
    assert child.pid is None
    assert ledger.status()["inflight"] == 0


@pytest.mark.stress
def test_real_request_threshold_rotates_and_reprobes(twohop):
    _, ledger, transport, obj = twohop
    transport.enable_persistent()
    verified = transport.verify_conditions(obj)
    first = transport._lane_worker
    for _ in range(252):
        with transport.transfer(verified, start=512, length=1):
            pass
    assert transport._lane_requests == 255
    assert transport.verified_object(verified, lengths=[1]) == verified
    assert transport._lane_worker is first and transport._generation == 0
    with transport.transfer(verified, start=512, length=1):
        pass
    assert transport._lane_requests == 256
    with pytest.raises(Exception):
        transport.verified_object(verified, lengths=[1])
    assert first.pid is None
    assert transport._generation == 1
    verified = transport.verify_conditions(obj)
    assert transport._lane_worker is not first
    assert transport._lane_requests == 3
    with transport.transfer(verified, start=512, length=1):
        pass
    assert transport._lane_requests == 4
    transport.close()
    assert ledger.status()["inflight"] == 0


def test_generation_ack_failure_has_no_new_socket(twohop, monkeypatch):
    state, ledger, transport, obj = twohop
    transport.enable_persistent()
    verified = transport.verify_conditions(obj)
    worker = transport._lane_worker
    calls_before = len(state["calls"])
    spawns = []
    popen = subprocess.Popen
    def track(*args, **kwargs):
        spawns.append(True)
        return popen(*args, **kwargs)
    monkeypatch.setattr(subprocess, "Popen", track)
    transport._lane_requests = 256
    marker = RuntimeError("generation admission failure")

    def reject():
        raise marker

    monkeypatch.setattr(ledger, "generation_start", reject, raising=False)
    with pytest.raises(RuntimeError) as caught:
        transport.verified_object(verified)
    assert caught.value is marker
    assert worker.pid is None
    assert transport._lane_worker is None
    assert transport._lane_failed
    assert not transport._live_proofs
    assert not spawns
    assert len(state["calls"]) == calls_before
    transport.close()
    assert ledger.status()["inflight"] == 0


def test_unfinished_lease_cannot_cross_operation_generation(multiple, monkeypatch):
    from sakurapool.storage.budget import Reservation
    from sakurapool.tasks.pipeline import LedgerRPC

    directory, ledger, Transport, calls = multiple
    Transport.clone = lambda self: Transport()
    Transport.close = lambda self: None
    original = LedgerRPC._invoke
    retained = []

    def attack(proxy, method, *args, **kwargs):
        if method == "event" and args[1] == "NETWORK_START" and not retained:
            retained.append(original(proxy, "reserve", Reservation(body=7, attempt=True)))
            proxy._channel._generation = 1
            original(proxy, "consume_body", retained[0], 1)
        return original(proxy, method, *args, **kwargs)

    monkeypatch.setattr(LedgerRPC, "_invoke", attack)
    with pytest.raises(RuntimeError, match="owner (lease lane mismatch|generation unacknowledged)"):
        run_task(directory, Transport(), control=object(), workers=1)
    assert retained
    assert ledger.status()["body"] == 7
    with TaskDB(directory, readonly=True) as task:
        assert task.meta("state") == "BLOCKED"


def test_actual_rotation_cancel_does_not_hold_state_lock(twohop, monkeypatch):
    from sakurapool.storage.rust_bridge import RustWorker

    _, ledger, transport, obj = twohop
    transport.enable_persistent()
    verified = transport.verify_conditions(obj)
    worker = transport._lane_worker
    transport._lane_requests = 256
    entered = threading.Event()
    release = threading.Event()
    original = RustWorker.close
    errors = []

    def delayed(current):
        if current is worker:
            entered.set()
            assert release.wait(timeout=5)
        original(current)

    monkeypatch.setattr(RustWorker, "close", delayed)
    def rotate():
        try:
            transport.verified_object(verified)
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=rotate)
    thread.start()
    try:
        assert entered.wait(timeout=3)
        transport.cancel()
        assert worker.pid is None
    finally:
        release.set()
        thread.join(timeout=5)
    assert not thread.is_alive() and errors
    assert ledger.status()["inflight"] == 0


def test_actual_saturated_pipe_transport_cancel(twohop, monkeypatch):
    from sakurapool.storage.rust_bridge import RustWorker

    _, ledger, transport, obj = twohop
    transport.enable_persistent()
    original = subprocess.Popen
    # Child reads hello only, then proves pipe occupancy without consuming it.
    script = r'''
import sys,json,time,ctypes,msvcrt
from ctypes import wintypes
sys.stdin.buffer.readline()
print(json.dumps({'type':'ready','protocol_version':1,'worker_version':'fault',
'capabilities':['bounded_session_v1']}),flush=True)
k=ctypes.WinDLL('kernel32',use_last_error=True)
h=wintypes.HANDLE(msvcrt.get_osfhandle(sys.stdin.fileno()))
a=wintypes.DWORD()
while True:
 if k.PeekNamedPipe(h,None,0,None,ctypes.byref(a),None) and a.value>=4096:
  print('PIPE_SATURATED',file=sys.stderr,flush=True)
  time.sleep(60)
 time.sleep(.001)
'''

    def child(*args, **kwargs):
        return original([sys.executable, "-I", "-c", script], **kwargs)

    monkeypatch.setattr(subprocess, "Popen", child)
    saturated = threading.Event()
    original_append = RustWorker._append_stderr
    def append(worker, chunk):
        original_append(worker, chunk)
        if b"PIPE_SATURATED" in worker.stderr_tail():
            saturated.set()
    monkeypatch.setattr(RustWorker, "_append_stderr", append)
    original_send = RustWorker._send
    def send(worker, line):
        if b'"request"' in line:
            line = b"x" * 65535 + b"\n"
        return original_send(worker, line)
    monkeypatch.setattr(RustWorker, "_send", send)
    errors = []
    def request():
        try:
            transport.verify_conditions(obj)
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=request)
    thread.start()
    try:
        assert saturated.wait(timeout=10)
        worker = transport._lane_worker
        assert thread.is_alive() and worker._writing.is_set()
        assert ledger.status()["inflight"] >= 32 << 20
        transport.cancel()
        thread.join(timeout=5)
        assert not thread.is_alive()
        assert worker.pid is None
        assert not worker._reader.is_alive() and not worker._stderr_reader.is_alive()
        assert errors
        assert ledger.status()["inflight"] == 0
        assert ledger.status()["body"] > 0  # UNKNOWN reservation survives child death.
    finally:
        transport.cancel()
        thread.join(timeout=5)


@pytest.mark.parametrize("workers", [1, 2, 4])
def test_actual_metadata_sessions_are_lane_owned(multiple, workers):
    from sakurapool.storage.transport import GuardedTransport

    directory, ledger, Transport, calls = multiple
    sessions = []
    reads = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        def log_message(self, *_):
            pass
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"{}")
            self.wfile.flush()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    original = Transport.verify_conditions

    class Lane(Transport):
        def verify_conditions(self, obj):
            if not hasattr(self, "metadata"):
                self.metadata = GuardedTransport(self.ledger,
                    trusted_hosts=frozenset({"127.0.0.1"}), allow_loopback_http=True)
                sessions.append((id(self.metadata.session), get_ident()))
            result = self.metadata.read_metadata(f"http://127.0.0.1:{server.server_port}/exact")
            reads.append(result.accounting_state)
            return original(self, obj)

        def close(self):
            if hasattr(self, "metadata"):
                self.metadata.close()

    Transport.clone = lambda self: Lane()
    try:
        result = run_task(directory, Transport(), control=object(), workers=workers)
        assert result["delivered_confirmed"] == 3
        assert reads and all(value == "CONFIRMED" for value in reads)
        assert len({identity for identity, _ in sessions}) == len(sessions)
        assert ledger.status()["inflight"] == 0
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


@pytest.mark.parametrize("attack", ["lease", "attempt", "receipt", "digest", "old_attempt"])
def test_owner_rejects_cross_lane_authority(multiple, monkeypatch, attack):
    from sakurapool.tasks.pipeline import LedgerRPC

    directory, ledger, Transport, calls = multiple
    Transport.clone = lambda self: Transport()
    Transport.close = lambda self: None
    original = LedgerRPC._invoke
    attempted = []

    old_lease = []

    def forged(proxy, method, *args, **kwargs):
        if attack == "old_attempt":
            if method == "reserve":
                lease = original(proxy, method, *args, **kwargs)
                if not old_lease:
                    old_lease.append(lease)
                return lease
            if method == "settle" and old_lease and args[0] != old_lease[0]:
                attempted.append(attack)
                args = (old_lease[0], *args[1:])
        if not attempted:
            if attack == "lease" and method == "settle":
                attempted.append(attack)
                args = ("0" * 32, *args[1:])
            elif attack == "attempt" and method == "event":
                attempted.append(attack)
                args = ("foreign-attempt", *args[1:])
            elif attack in ("receipt", "digest") and method == "event" and args[1] == "PREPARED":
                attempted.append(attack)
                payload = dict(args[2])
                receipt = {key: dict(value) for key, value in payload["receipt"].items()}
                if attack == "receipt":
                    next(iter(receipt.values()))["bytes"] += 1
                else:
                    next(iter(receipt.values()))["sha256"] = "0" * 64
                payload["receipt"] = receipt
                args = (*args[:2], payload)
        return original(proxy, method, *args, **kwargs)

    monkeypatch.setattr(LedgerRPC, "_invoke", forged)
    with pytest.raises(RuntimeError, match="owner .* mismatch"):
        count = 1 if attack == "old_attempt" else 2
        run_task(directory, Transport(), control=object(), workers=count)
    assert attempted
    with TaskDB(directory, readonly=True) as task:
        assert task.meta("state") == "BLOCKED"


@pytest.mark.parametrize("field", ["object_path", "object_version", "repo_id", "record_id", "rid"])
def test_sql_required_fields_reject_nul_tail_before_materialization(multiple, monkeypatch, field):
    import sqlite3

    from sakurapool.storage.prepared_fetch import PreparedFetch
    from sakurapool.storage.publication import load_publication

    directory, ledger, Transport, calls = multiple
    with TaskDB(directory) as task:
        row = task._pipeline_candidate()
        path = task.meta("publication_path")
        if field in ("record_id", "rid"):
            bad = ("a" * 32 + "\0" + "tail" * 10000) if field == "record_id" else "huge" * 10000
            task.db.execute(f"UPDATE items SET {field}=? WHERE seq=?", (bad, row["seq"]))
            with pytest.raises(TaskError):
                task._pipeline_candidate()
            with pytest.raises(TaskError):
                task._pipeline_claim()
            assert task.db.execute("SELECT count(*) FROM attempts").fetchone()[0] == 0
            return
    with load_publication(path, full_verify=True) as pub:
        original = pub.catalog if field == "repo_id" else pub.runtime._catalog
        database = sqlite3.connect(":memory:")
        original.backup(database)
        prefix = "a" * 64 if field == "object_version" else "x"
        if field == "repo_id":
            database.execute("UPDATE repositories SET repo_id=?", (prefix + "\0" + "tail" * 10000,))
            monkeypatch.setattr(pub, "catalog", database)
        else:
            database.execute(f"UPDATE objects SET {field}=?", (prefix + "\0" + "tail" * 10000,))
            monkeypatch.setattr(pub.runtime, "_catalog", database)
        with pytest.raises(Exception):
            PreparedFetch._prepare(pub, row["record_id"])
        if field == "repo_id":
            monkeypatch.setattr(pub, "catalog", original)
        else:
            monkeypatch.setattr(pub.runtime, "_catalog", original)
        database.close()


def test_sql_projections_do_not_read_unbounded_unused_columns(multiple, monkeypatch):
    import sqlite3

    from sakurapool.storage.prepared_fetch import PreparedFetch
    from sakurapool.storage.publication import load_publication

    directory, ledger, Transport, calls = multiple
    with TaskDB(directory) as task:
        task.db.execute("UPDATE items SET receipt=?", ("unused" * 20000,))
        queries = []
        task.db.set_trace_callback(queries.append)
        item = task._pipeline_candidate()
        claimed = task._pipeline_claim()
        assert set(item) == {"seq", "rid", "record_id"}
        assert "receipt" not in claimed
        assert all("SELECT *" not in sql for sql in queries)
        pub_path = task.meta("publication_path")
    with load_publication(pub_path, full_verify=True) as pub:
        original = pub.runtime._catalog
        database = sqlite3.connect(":memory:")
        original.backup(database)
        database.execute("UPDATE objects SET validator=?,storage_id=?",
                         ("validator" * 20000, "storage" * 20000))
        selected = []
        database.set_trace_callback(selected.append)
        monkeypatch.setattr(pub.runtime, "_catalog", database)
        descriptor = PreparedFetch._prepare(pub, item["record_id"])
        assert set(descriptor.object_ref) == {"object_path", "object_size", "object_version"}
        assert not any("validator" in sql or "storage_id" in sql for sql in selected)
        database.execute("UPDATE objects SET object_path=?", ("x" * 2049,))
        with pytest.raises(Exception):
            pub.runtime._prepared_object_ref(descriptor.location["object_idx"])
        monkeypatch.setattr(pub.runtime, "_catalog", original)
        database.close()


def test_factory_drops_unneeded_object_and_location_fields(multiple, monkeypatch):
    from sakurapool.runtime import RuntimeSnapshot
    from sakurapool.storage.prepared_fetch import PreparedFetch
    from sakurapool.storage.publication import load_publication
    from sakurapool.tasks.pipeline import _lane_item

    directory, ledger, Transport, calls = multiple
    original_ref = RuntimeSnapshot.object_ref
    original_loc = RuntimeSnapshot.location
    extra = "private-unused" * 10000
    monkeypatch.setattr(RuntimeSnapshot, "object_ref", lambda self, index:
                        dict(original_ref(self, index), validator=extra, backend=extra))
    monkeypatch.setattr(RuntimeSnapshot, "location", lambda self, rid:
                        dict(original_loc(self, rid), unused=extra))
    with TaskDB(directory, readonly=True) as task:
        path = task.meta("publication_path")
        row = dict(task.db.execute("SELECT * FROM items LIMIT 1").fetchone())
    with load_publication(path, full_verify=True) as pub:
        descriptor = PreparedFetch._prepare(pub, row["record_id"])
        assert set(descriptor.object_ref) == {"object_path", "object_size", "object_version"}
        assert "unused" not in descriptor.location
    row.update(attempt_id="a" * 32, operation_id="b" * 32, receipt=extra)
    assert set(_lane_item(row)) == {"seq", "rid", "record_id", "attempt_id", "operation_id"}


def test_rpc_request_near_boundaries():
    import queue
    from threading import get_ident
    from types import SimpleNamespace

    from sakurapool.tasks.pipeline import LedgerRPC, OwnerAuthorityError, _envelope_bytes

    payload = ("x" * 3999,)
    assert _envelope_bytes(payload) + _envelope_bytes({}) == 16380
    assert _envelope_bytes(("x" * 4000,)) + _envelope_bytes({}) == 16384
    class Capture(queue.Queue):
        def put(self, call, *args, **kwargs):
            super().put(call, *args, **kwargs)
            call.reply.put((True, "bounded-reply"))
    calls = Capture(maxsize=1)
    ledger = SimpleNamespace(root=None, limits={}, offline_mode=False)
    rpc = LedgerRPC(get_ident() + 1, calls, ledger, 0, SimpleNamespace())
    assert rpc._invoke("condition_proof", "x" * 4000) == "bounded-reply"
    calls.get()
    with pytest.raises(OwnerAuthorityError):
        rpc._invoke("condition_proof", "x" * 4001)
    assert calls.empty()


def test_exact_completion_queue_full_while_owner_pumps(multiple, monkeypatch):
    from sakurapool.tasks import pipeline

    directory, ledger, Transport, calls = multiple
    Transport.clone = lambda self: Transport()
    Transport.close = lambda self: None
    original = pipeline.queue.Queue
    created = []
    observed = []
    class CompletionQueue(original):
        released = threading.Event()
        def put(self, item, *args, **kwargs):
            super().put(item, *args, **kwargs)
            if not self.released.is_set():
                if self.qsize() == self.maxsize:
                    observed.append(self.qsize())
                    self.released.set()
                assert self.released.wait(timeout=15)
        def empty(self):
            return True if not self.released.is_set() else super().empty()
    def factory(*args, **kwargs):
        # run_pipeline constructs calls(2W), then completions(W), before replies.
        cls = CompletionQueue if len(created) == 1 else original
        value = cls(*args, **kwargs)
        created.append(value)
        return value
    monkeypatch.setattr(pipeline.queue, "Queue", factory)
    result = run_task(directory, Transport(), workers=2, control=object())
    assert created[1].maxsize == 2
    assert observed == [2]
    assert result["delivered_confirmed"] == 3
    assert ledger.status()["inflight"] == 0


@pytest.mark.parametrize("length,accepted", [(4064, True), (4065, False)])
def test_owner_reply_size_boundary(multiple, monkeypatch, length, accepted):
    from sakurapool.tasks import pipeline

    directory, ledger, Transport, calls = multiple
    assert pipeline._envelope_bytes("x" * length) == 128 + length * 4
    original = ledger.record_condition_proof
    def reply(*args, **kwargs):
        original(*args, **kwargs)
        return "x" * length
    monkeypatch.setattr(ledger, "record_condition_proof", reply)
    Transport.clone = lambda self: Transport()
    original_verify = Transport.verify_conditions
    def verify(self, obj):
        self.ledger.record_condition_proof("a" * 64, "b" * 64)
        return original_verify(self, obj)
    monkeypatch.setattr(Transport, "verify_conditions", verify)
    Transport.close = lambda self: None
    if accepted:
        assert run_task(directory, Transport(), workers=2, control=object())["state"] == "COMPLETED"
    else:
        with pytest.raises(pipeline.OwnerAuthorityError):
            run_task(directory, Transport(), workers=2, control=object())
    assert ledger.status()["inflight"] == 0


@pytest.mark.parametrize("length,accepted", [(2048, True), (2049, False)])
def test_prepared_factory_identity_field_boundary(multiple, monkeypatch, length, accepted):
    from types import SimpleNamespace

    from sakurapool.storage.prepared_fetch import PreparedFetch
    from sakurapool.storage.publication import PublicationCorrupt, load_publication

    directory, ledger, Transport, calls = multiple
    with TaskDB(directory, readonly=True) as task:
        pub_path = task.meta("publication_path")
        record = task.db.execute("SELECT record_id FROM items LIMIT 1").fetchone()[0]
    with load_publication(pub_path, full_verify=True) as pub:
        catalog = pub.catalog
        class Catalog:
            def execute(self, sql, parameters):
                row = list(catalog.execute(sql, parameters).fetchone())
                row[1] = "x" * length
                return SimpleNamespace(fetchone=lambda: tuple(row))
        monkeypatch.setattr(pub, "catalog", Catalog())
        if accepted:
            descriptor = PreparedFetch._prepare(pub, record)
            assert len(descriptor.catalog_row[1]) == 2048
        else:
            with pytest.raises(PublicationCorrupt):
                PreparedFetch._prepare(pub, record)
        monkeypatch.setattr(pub, "catalog", catalog)


def test_slow_disk_owner_pump_and_full_reply_queue(multiple, monkeypatch):
    from sakurapool.storage import publication_fetch
    from sakurapool.tasks import pipeline

    directory, ledger, Transport, calls = multiple
    Transport.clone = lambda self: Transport()
    Transport.close = lambda self: None
    original_fsync = publication_fetch.os.fsync
    def slow(fd):
        time.sleep(0.01)
        return original_fsync(fd)
    monkeypatch.setattr(publication_fetch.os, "fsync", slow)
    original_queue = pipeline.queue.Queue
    occupancies = []
    class ObservedQueue(original_queue):
        def put(self, item, *args, **kwargs):
            result = super().put(item, *args, **kwargs)
            occupancies.append((self.maxsize, self.qsize()))
            return result
    monkeypatch.setattr(pipeline.queue, "Queue", ObservedQueue)
    result = run_task(directory, Transport(), workers=2, control=object())
    assert result["delivered_confirmed"] == 3
    assert all(size <= cap for cap, size in occupancies)
    assert any(cap == 1 and size == 1 for cap, size in occupancies)
    assert ledger.status()["inflight"] == 0


def test_completed_bookkeeping_is_bounded(tmp_path, monkeypatch):
    import test_task_multiple
    from synthetic_p2 import ObjectSpec, SampleSpec

    original_builder = test_task_multiple.build_p2_directory
    def many(*args, **kwargs):
        kwargs["objects"] = [ObjectSpec("one.tar", [
            SampleSpec(f"{i}.png", str(i), has_json=False) for i in range(96)], size=10240)]
        return original_builder(*args, **kwargs)
    monkeypatch.setattr(test_task_multiple, "build_p2_directory", many)
    fixture = _multiple.__wrapped__(tmp_path, monkeypatch)
    directory, ledger, Transport, calls = next(fixture)
    Transport.clone = lambda self: Transport()
    Transport.close = lambda self: None
    samples = []
    def hook(event, payload):
        if event == "BOOKKEEPING":
            samples.append(payload)
    result = run_task(directory, Transport(), workers=2, control=object(), fault_hook=hook)
    assert result["delivered_confirmed"] == 96
    assert samples
    assert max(row["completed"] for row in samples) <= 2
    assert max(row["active"] for row in samples) <= 2
    fixture.close()


def test_workers_four_admits_only_one_resident_lane(multiple):
    from sakurapool.storage.budget import Reservation

    directory, ledger, Transport, calls = multiple
    ledger.limits["inflight"] = (33 << 20) + (1 << 19)
    resident = []
    class Lane(Transport):
        def enable_persistent(self):
            self._lane_lease = self.ledger.reserve(Reservation(inflight=32 << 20))
            resident.append(self._lane_lease)
        def close(self):
            if hasattr(self, "_lane_lease"):
                self._closed = True
                self._operation_active = False
                self.ledger.settle_resident(self._lane_lease)
    Transport.clone = lambda self: Lane()
    result = run_task(directory, Transport(), workers=4, control=object())
    assert result["delivered_confirmed"] == 3
    assert len(resident) == 1
    assert ledger.status()["inflight"] == 0


def test_empty_resume_does_not_admit_or_clone_lanes(multiple):
    directory, ledger, Transport, calls = multiple
    run_task(directory, Transport(), control=object())
    Transport.clone = lambda self: (_ for _ in ()).throw(AssertionError("empty clone"))
    before = ledger.status()
    result = run_task(directory, Transport(), workers=4, control=object(), resume=True)
    assert result["delivered_confirmed"] == 3
    assert ledger.status() == before


@pytest.fixture
def three_distinct_objects(tmp_path, monkeypatch):
    import test_task_multiple
    from synthetic_p2 import ObjectSpec, SampleSpec

    build = test_task_multiple.build_p2_directory

    def distinct_build(path, **kwargs):
        kwargs["objects"] = [ObjectSpec(f"{i}.tar", [SampleSpec(f"{i}.png", str(i),
                            has_json=False)], size=10240) for i in range(3)]
        return build(path, **kwargs)

    monkeypatch.setattr(test_task_multiple, "build_p2_directory", distinct_build)
    yield from _multiple.__wrapped__(tmp_path, monkeypatch)


def test_settled_hold_is_not_counted_twice(three_distinct_objects, monkeypatch):
    from sakurapool.storage import publication_fetch as pipeline

    directory, ledger, Transport, calls = three_distinct_objects
    original = pipeline._fetch_publication_sample
    release = threading.Event()
    settled = threading.Event()
    first = []
    lock = threading.Lock()
    def delayed(*args, **kwargs):
        with lock:
            is_first = not first
            first.append(True)
        result = original(*args, **kwargs)
        if is_first:
            settled.set()
            assert release.wait(timeout=10)
        return result
    monkeypatch.setattr(pipeline, "_fetch_publication_sample", delayed)
    claimed = []
    def hook(event, payload):
        if event == "CLAIMED":
            claimed.append(payload["seq"])
            if len(claimed) == 3:
                assert settled.is_set()
                release.set()
    Transport.clone = lambda self: Transport()
    Transport.close = lambda self: None
    with TaskDB(directory) as task:
        with task.transaction() as db:
            task.set_meta(db, "max_output_bytes", 15)
    try:
        result = run_task(directory, Transport(), workers=2, control=object(), fault_hook=hook)
        assert settled.is_set() and len(claimed) == 3
        assert result["delivered_confirmed"] == 3
        assert ledger.status()["saved_bytes"] == 15
    finally:
        release.set()


@pytest.mark.parametrize("message", ["settlement-fault", "owner fake-authority"])
def test_rename_then_settlement_failure_preserves_public_delivery(multiple, monkeypatch, message):
    directory, ledger, Transport, calls = multiple
    Transport.clone = lambda self: Transport()
    Transport.close = lambda self: None
    original = ledger.settle

    def failed_settlement(lease, **kwargs):
        if kwargs.get("saved_samples"):
            raise RuntimeError(message)
        return original(lease, **kwargs)

    monkeypatch.setattr(ledger, "settle", failed_settlement)
    with pytest.raises(TaskError) as caught:
        run_task(directory, Transport(), workers=2, control=object())
    diagnostic = caught.value.public_diagnostic()
    assert diagnostic["delivery"] == "PUBLISHED"
    assert diagnostic["accounting"] == "UNKNOWN"
    assert diagnostic["output_lease"] == "UNKNOWN"
    assert diagnostic["cleanup"] == "PRESERVED"
    with TaskDB(directory, readonly=True) as task:
        assert task.meta("state") == "BLOCKED"


@pytest.mark.parametrize("fault", ["diagnostic", "submit", "finish", "completion"])
def test_bounded_child_secondary_drains(multiple, monkeypatch, fault):
    if os.environ.get("SAKURAPOOL_C2_FAULT_CHILD") != fault:
        result = subprocess.run([sys.executable, "-m", "pytest",
            __file__ + f"::test_bounded_child_secondary_drains[{fault}]", "-q",
            "-p", "no:cacheprovider", "--tb=short"],
            env={**os.environ, "SAKURAPOOL_C2_FAULT_CHILD": fault},
            capture_output=True, text=True, timeout=120)
        assert result.returncode == 0, result.stdout + result.stderr
        return
    from concurrent.futures import ThreadPoolExecutor

    from sakurapool.storage.transport import RemoteIOError

    directory, ledger, Transport, calls = multiple
    Transport.clone = lambda self: Transport()
    Transport.close = lambda self: None
    if fault == "diagnostic":
        marker = RemoteIOError("original")
        marker.public_diagnostic = lambda: (_ for _ in ()).throw(RuntimeError("pack"))
        @contextmanager
        def fail(self, *args):
            raise marker
            yield
        Transport.read_range_owned = fail
        # Inject at pre-materialization boundary so publication wrapping cannot
        # conceal the faulty diagnostic callback.
        from sakurapool.tasks import pipeline
        monkeypatch.setattr(pipeline, "_safe_diagnostic",
                            lambda error: (_ for _ in ()).throw(RuntimeError("pack")))
    elif fault == "submit":
        original = ThreadPoolExecutor.submit
        count = []
        def submit(pool, fn, *args, **kwargs):
            if fn.__name__ == "execute":
                count.append(True)
                if len(count) == 2:
                    raise RuntimeError("partial-submit")
            return original(pool, fn, *args, **kwargs)
        monkeypatch.setattr(ThreadPoolExecutor, "submit", submit)
    elif fault == "completion":
        from sakurapool.tasks import pipeline
        original_queue = pipeline.queue.Queue
        class BrokenCompletion(original_queue):
            broken = False
            def put(self, item, *args, **kwargs):
                if (type(item) is tuple and len(item) == 3 and type(item[0]) is int
                        and not self.broken):
                    self.broken = True
                    raise RuntimeError("completion-publication")
                return super().put(item, *args, **kwargs)
        monkeypatch.setattr(pipeline.queue, "Queue", BrokenCompletion)
    else:
        monkeypatch.setattr(TaskDB, "finish_item",
                            lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("finish")))
    with pytest.raises(BaseException):
        run_task(directory, Transport(), control=object(), workers=2)
    assert ledger.status()["inflight"] == 0


@pytest.mark.parametrize("nested", [False, True])
def test_actual_rust_interrupt_retains_existing_payload_lease(twohop, nested):
    _, ledger, transport, obj = twohop
    transport.enable_persistent()
    verified = transport.verify_conditions(obj)
    marker = KeyboardInterrupt()
    try:
        with pytest.raises(KeyboardInterrupt) as caught:
            with transport._transfer_owned(verified, start=512, length=4096) as (_, _, payload):
                marker.payload = {"nested": [payload]} if nested else payload
                raise marker
        assert caught.value is marker
        transport.close()
        assert ledger.status()["inflight"] == 8192
        held = marker.payload["nested"][0] if nested else marker.payload
        assert len(held) == 4096
        del held
    finally:
        if hasattr(marker, "payload"):
            del marker.payload
        if hasattr(marker, "production_payload_lease"):
            ledger.settle(marker.production_payload_lease)
    assert ledger.status()["inflight"] == 0


def test_real_http_exact_provider_identity(twohop):
    from urllib.parse import parse_qs, urlsplit

    from sakurapool.storage.publication_fetch import exact_provider_lookup
    from sakurapool.storage.transport import GuardedTransport

    _, ledger, _, _ = twohop
    requests = []
    revision = "b" * 40
    digest = "c" * 64
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        def log_message(self, *_):
            pass
        def do_GET(self):
            requests.append(self.path)
            parsed = urlsplit(self.path)
            if parsed.path.endswith("/repo/tree"):
                assert parse_qs(parsed.query)["Revision"] == [revision]
                data = {"Files": [{"Type": "blob", "Path": "folder/object.tar", "Size": 10240,
                                    "Revision": revision, "Sha256": digest}], "TotalCount": 1}
            else:
                data = {"Id": 17, "Namespace": "synthetic", "Name": "repo", "Type": 4}
            body = json.dumps({"Code": 200, "Success": True, "Data": data}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    control = GuardedTransport(ledger, trusted_hosts=frozenset({"127.0.0.1"}),
                               allow_loopback_http=True)
    endpoint = f"http://127.0.0.1:{server.server_port}"
    try:
        obj = exact_provider_lookup(control, endpoint, "synthetic/repo", revision,
                                    "folder/object.tar", 10240, digest)
        assert obj.origin == endpoint and obj.revision == revision
        assert obj.object_path == "folder/object.tar"
        assert len(requests) == 2
        assert ledger.status()["metadata"] > 0
    finally:
        control.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_interrupt_payload_keeps_explicit_ownership_lease(multiple):
    from sakurapool.storage.budget import Reservation

    directory, ledger, Transport, calls = multiple
    Transport.clone = lambda self: Transport()
    Transport.close = lambda self: None
    marker = KeyboardInterrupt()
    ownership = []

    @contextmanager
    def interrupted(self, *args):
        # Deliberately retain a live payload: quota must remain with its owner.
        lease = self.ledger.reserve(Reservation(inflight=4096))
        ownership.append(lease)
        marker.payload = b"x" * 4096
        raise marker
        yield

    Transport.read_range_owned = interrupted
    try:
        with pytest.raises(KeyboardInterrupt) as caught:
            run_task(directory, Transport(), control=object(), workers=1)
        assert caught.value is marker
        assert len(marker.payload) == 4096
        assert ledger.status()["inflight"] == 4096
    finally:
        if hasattr(marker, "payload"):
            del marker.payload
        for lease in ownership:
            ledger.settle(lease)
    assert ledger.status()["inflight"] == 0


def test_rpc_envelope_rejects_payload_and_unbounded_shapes():
    from sakurapool.tasks.pipeline import _envelope_bytes, _rpc_error

    assert _envelope_bytes({"lease": "a" * 32, "body": 5}) < 16384
    for value in (b"payload", "x" * 4097, 1 << 100, [0] * 33, object()):
        with pytest.raises(RuntimeError):
            _envelope_bytes(value)
    marker = SystemExit(9)
    marker.task_secondary = ("TASK_STATE_PERSIST_FAILED",)
    reply = _rpc_error(marker)
    assert reply is not marker
    assert marker.code == 9
    assert marker.task_secondary == ("TASK_STATE_PERSIST_FAILED",)
    assert reply.args == ("coordinator process-control interruption",)


def test_public_fetch_rejects_internal_projection(multiple):
    from types import SimpleNamespace

    from sakurapool.storage.publication import PublicationCorrupt
    from sakurapool.storage.publication_fetch import fetch_publication_sample

    directory, _, Transport, _ = multiple
    with pytest.raises(PublicationCorrupt):
        fetch_publication_sample(SimpleNamespace(_closed=False, full_verified=True),
                                 "0" * 32, Transport(), directory / "fake", control=object())


def test_finish_db_fault_does_not_deadlock_other_lane(multiple, monkeypatch):
    if os.environ.get("SAKURAPOOL_C2_DRAIN_CHILD") != "1":
        result = subprocess.run(
            [sys.executable, "-m", "pytest", __file__ +
             "::test_finish_db_fault_does_not_deadlock_other_lane", "-q",
             "-p", "no:cacheprovider", "--tb=short"],
            env={**os.environ, "SAKURAPOOL_C2_DRAIN_CHILD": "1"},
            capture_output=True, text=True, timeout=120)
        assert result.returncode == 0, result.stdout + result.stderr
        return
    directory, ledger, Transport, calls = multiple
    Transport.clone = lambda self: Transport()
    Transport.close = lambda self: (_ for _ in ()).throw(RuntimeError("close-fault"))
    original = TaskDB.finish_item
    marker = RuntimeError("persist-primary")
    failed = []

    def finish(task, seq, **kwargs):
        if kwargs.get("state") == "DONE" and not failed:
            failed.append(seq)
            raise marker
        return original(task, seq, **kwargs)

    monkeypatch.setattr(TaskDB, "finish_item", finish)
    with pytest.raises(RuntimeError) as caught:
        run_task(directory, Transport(), control=object(), workers=2)
    assert caught.value is marker
    assert "LANE_CLOSE_FAILED" in marker.task_secondary
    with TaskDB(directory, readonly=True) as task:
        assert task.inspect()["delivered_confirmed"] >= 1
        assert task.meta("state") == "BLOCKED"


@pytest.mark.parametrize("marker", [KeyboardInterrupt(), SystemExit(9)])
def test_lane_baseexception_remains_original(multiple, marker):
    directory, ledger, Transport, calls = multiple
    Transport.clone = lambda self: Transport()
    Transport.close = lambda self: None

    @contextmanager
    def fail(self, *args):
        raise marker
        yield

    Transport.read_range_owned = fail
    with pytest.raises(type(marker)) as caught:
        run_task(directory, Transport(), control=object(), workers=2)
    assert caught.value is marker


@pytest.mark.parametrize("workers", [2, 4])
def test_pipeline_aggregate_output_holds_block_before_claim(multiple, workers):
    directory, ledger, Transport, calls = multiple
    Transport.clone = lambda self: Transport()
    Transport.close = lambda self: None
    with TaskDB(directory) as task:
        with task.transaction() as db:
            task.set_meta(db, "max_output_bytes", 7)
    with pytest.raises(TaskError, match="RESOURCE_BLOCKED"):
        run_task(directory, Transport(), control=object(), workers=workers)
    with TaskDB(directory, readonly=True) as task:
        assert task.inspect()["delivered_confirmed"] == 1
        assert task.db.execute("SELECT count(*) FROM attempts").fetchone()[0] == 1
    assert ledger.status()["saved_bytes"] == 5


@pytest.mark.parametrize("corrupt", [False, True])
def test_reconcile_mixed_confirmed_and_unknown_no_redownload(multiple, corrupt):
    from sakurapool.tasks.runner import reconcile

    directory, ledger, Transport, calls = multiple
    run_task(directory, Transport(), control=object())
    before = list(calls)
    with TaskDB(directory) as task:
        rows = list(task.db.execute("SELECT seq,record_id,attempt_id FROM items ORDER BY seq"))
        with task.transaction() as db:
            db.execute("UPDATE items SET state='IN_PROGRESS',accounting='UNKNOWN' WHERE seq=?",
                       (rows[0]["seq"],))
            db.execute("UPDATE attempts SET accounting='UNKNOWN',phase='PUBLISHED' "
                       "WHERE attempt_id=?",
                       (rows[0]["attempt_id"],))
            db.execute("UPDATE items SET state='IN_PROGRESS' WHERE seq=?", (rows[1]["seq"],))
        if corrupt:
            image = directory / "output" / rows[0]["record_id"] / "image.png"
            image.write_bytes(b"other")
        with pytest.raises(TaskError):
            reconcile(task)
        assert task.db.execute("SELECT state FROM items WHERE seq=?",
                               (rows[1]["seq"],)).fetchone()[0] == "DONE"
        assert task.db.execute("SELECT accounting FROM items WHERE seq=?",
                               (rows[0]["seq"],)).fetchone()[0] == "UNKNOWN"
    assert calls == before


@pytest.mark.parametrize("window", ["CLAIMED", "REQUEST", "PREPARED", "PUBLISHED",
                                    "BEFORE_SETTLED", "SETTLED"])
def test_pipeline_actual_child_crash_windows(multiple, window):
    directory, ledger, Transport, calls = multiple
    code = r'''
import os,sys
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager
from sakurapool.runtime import RuntimeSnapshot
from sakurapool.storage import publication_fetch
from sakurapool.storage.budget import BudgetLedger,Reservation
from sakurapool.tasks.runner import run_task
root,directory,window=sys.argv[1:]
original=RuntimeSnapshot.location
RuntimeSnapshot.location=lambda self,rid: dict(
 original(self,rid),image_size=5,flags=0,metadata_size=0)
ledger=BudgetLedger(root,_offline_test=True)
publication_fetch.exact_provider_lookup=lambda *args: SimpleNamespace(
 repo_type='modelscope_dataset_legacy',origin='https://modelscope.cn',repo_id='synthetic/test',
 revision='b'*40,object_path=args[4],validator='"fresh"')
class Transport:
 max_range_bytes=8<<20
 def __init__(self): self.ledger=ledger
 def clone(self): return Transport()
 def close(self): pass
 def _host(self,url): return 'modelscope.cn'
 def verify_conditions(self,obj): return obj
 @contextmanager
 def read_range_owned(self,*args):
  lease=self.ledger.reserve(Reservation(body=5,attempt=True))
  self.ledger.consume_body(lease,2 if window=='REQUEST' else 5)
  if window=='REQUEST': os._exit(71)
  yield b'image'
  self.ledger.settle(lease)
def crash(event,payload):
 if event==window: os._exit(71)
run_task(directory,Transport(),workers=2,control=object(),fault_hook=crash)
'''
    child = subprocess.run([sys.executable, "-c", code, str(ledger.root), str(directory), window],
                           capture_output=True, text=True, timeout=120)
    assert child.returncode == 71, child.stdout + child.stderr
    Transport.clone = lambda self: Transport()
    Transport.close = lambda self: None
    if window == "CLAIMED":
        result = run_task(directory, Transport(), workers=2, control=object(), resume=True)
        assert result["state"] == "COMPLETED"
        assert result["delivered_confirmed"] == 3
    elif window == "SETTLED":
        try:
            run_task(directory, Transport(), workers=2, control=object(), resume=True)
        except TaskError:
            pass  # Independent other lane may be unresolved at the actual crash.
        with TaskDB(directory, readonly=True) as task:
            assert task.inspect()["delivered_confirmed"] >= 1
            done = task.db.execute("SELECT count(*) FROM items WHERE state='DONE'").fetchone()[0]
            assert done >= 1
    else:
        with pytest.raises(TaskError):
            run_task(directory, Transport(), workers=2, control=object(), resume=True)
        with TaskDB(directory, readonly=True) as task:
            assert task.meta("state") == "BLOCKED"
            assert task.inspect()["unknown_accounting_count"] >= 1


@pytest.mark.parametrize("action", ["PAUSE", "CANCEL"])
@pytest.mark.parametrize("workers", [1, 2, 4])
def test_pipeline_control_drains_admitted_only(multiple, action, workers):
    directory, ledger, Transport, calls = multiple
    Transport.clone = lambda self: Transport()
    Transport.close = lambda self: None

    def hook(event, payload):
        if event == "SETTLED":
            with TaskDB(directory) as task:
                task.request(action)

    result = run_task(directory, Transport(), control=object(), workers=workers, fault_hook=hook)
    assert result["state"] == ("PAUSED" if action == "PAUSE" else "CANCELLED")
    assert result["delivered_confirmed"] <= workers
