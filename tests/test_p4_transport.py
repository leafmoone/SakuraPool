"""Offline HTTP fixture: no target repo, no external network."""

import hashlib
import io
import json
import os
import pickle
import subprocess
import sys
import tarfile
import tempfile
import threading
import traceback
from contextlib import closing
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest
import requests
from urllib3.exceptions import ProtocolError, ReadTimeoutError

from sakurapool import indexer
from sakurapool.registry import DatasetAdapter
from sakurapool.runtime.compiler import compile_runtime
from sakurapool.runtime.inventory import load_p2_inventory
from sakurapool.runtime.snapshot import RuntimeSnapshot
from sakurapool.storage.budget import DEFAULT_WORK_ROOT, BudgetExceeded, BudgetLedger
from sakurapool.storage.modelscope import ModelScopeDataset
from sakurapool.storage.package import (
    Binding,
    PackageCorrupt,
    load_package,
    publish_local_package,
)
from sakurapool.storage.remote_index import (
    StagedArchive,
    open_completed_stage,
    stage_tar,
    write_staged_v4,
)
from sakurapool.storage.retrieval import (
    AuditedSample,
    Extent,
    fetch_bound_sample,
    fetch_bounded_samples,
)
from sakurapool.storage.transport import (
    READ_CHUNK,
    BoundObject,
    GuardedTransport,
    RemoteIOError,
    condition_binding_key,
)

DATA = b"abcdefghijklmnopqrst"
ETAG = '"frozen-etag"'
TAR_BUFFER = io.BytesIO()
with tarfile.open(fileobj=TAR_BUFFER, mode="w:") as archive:
    info = tarfile.TarInfo("nested/1.json")
    info.size = len(DATA)
    archive.addfile(info, io.BytesIO(DATA))
TAR = TAR_BUFFER.getvalue()
MULTI_BUFFER = io.BytesIO()
with tarfile.open(fileobj=MULTI_BUFFER, mode="w:") as archive:
    for name in ("dir/1.JPG", "dir/1.json", "dir/2.jpg", "dir/2.json"):
        content = b"{}" if name.endswith(".json") else b"abc"
        info = tarfile.TarInfo(name)
        info.size = len(content)
        archive.addfile(info, io.BytesIO(content))
TAR_MULTI = MULTI_BUFFER.getvalue()


class Handler(BaseHTTPRequestHandler):
    calls = []
    fixture_data = b""

    def log_message(self, *_args):
        pass

    def do_GET(self):
        mode = urlsplit(self.path).path.strip("/")
        self.calls.append((mode, self.headers.get("Range"),
                           self.headers.get("Authorization")))
        if mode == "api/v1/datasets/leafmoone/game_cg_5M/repo":
            body = TAR_MULTI
            requested = self.headers.get("Range")
            if self.headers.get("If-Match") not in (None, ETAG):
                self.send_response(412)
                self.end_headers()
                return
            if requested:
                start, end = map(int, requested.removeprefix("bytes=").split("-"))
                body = body[start:end + 1]
                self.send_response(206)
                self.send_header("Content-Range",
                                 f"bytes {start}-{end}/{len(TAR_MULTI)}")
            else:
                self.send_response(200)
            self.send_header("ETag", ETAG)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if mode.startswith("api/v1/datasets/leafmoone/game_cg_5M/"):
            payload = ({"Data": {"RevisionMap": {"Tags": [], "Branches": [
                {"Revision": "master", "CommitId": "a" * 40}]}}}
                       if mode.endswith("/revisions") else
                       {"Data": {"Total": 1,
                                 "Files": [{"Path": "example.tar", "Size": 100,
                                            "Type": "blob"}]}})
            data = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        if mode == "redirect":
            self.send_response(302)
            self.send_header("Location", "/good")
            self.end_headers()
            return
        if mode == "redirect-retry":
            self.send_response(302)
            self.send_header("Location", "/retry")
            self.end_headers()
            return
        if mode == "cross-host":
            self.send_response(302)
            self.send_header("Location", f"http://127.0.0.1:{self.server.server_port}/good")
            self.end_headers()
            return
        if mode == "if-match-enforced" and self.headers.get("If-Match") != ETAG:
            self.send_response(412)
            self.end_headers()
            return
        if mode in {"auth401", "auth403"}:
            self.send_response(401 if mode == "auth401" else 403)
            self.end_headers()
            return
        if mode == "evil":
            self.send_response(302)
            self.send_header("Location", "https://evil.invalid/object?token=SECRET")
            self.end_headers()
            return
        if mode in {"retry", "retry-long"}:
            self.send_response(429)
            self.send_header("Retry-After", "0" if mode == "retry" else "1000")
            self.end_headers()
            return
        if mode in {"full", "full-multi", "full-custom", "full-short", "full-overlong"}:
            payload = (Handler.fixture_data if mode == "full-custom" else
                       TAR_MULTI if mode == "full-multi" else
                       TAR[:-1] if mode == "full-short" else
                       TAR + b"x" if mode == "full-overlong" else TAR)
            self.send_response(200)
            if mode == "full":
                self.send_header("Content-Length", str(len(payload)))
            self.send_header("ETag", ETAG)
            self.end_headers()
            self.wfile.write(payload)
            return
        if mode == "meta":
            payload = json.dumps({"items": ["demo"]}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        sent_range = self.headers.get("Range", "bytes=0-3")
        start, end = map(int, sent_range.removeprefix("bytes=").split("-"))
        payload = DATA[start:end + 1]
        if mode == "short":
            payload = payload[:-1]
        elif mode == "overlong":
            payload += b"x"
        self.send_response(200 if mode == "ignore" else 206)
        if mode != "no-cr":
            cr = f"bytes {start}-{end}/{len(DATA)}"
            if mode == "wrong-cr":
                cr = f"bytes {start+1}-{end}/{len(DATA)}"
            self.send_header("Content-Range", cr)
        if mode not in {"no-cl", "short", "overlong"}:
            self.send_header("Content-Length", str(len(payload)))
        if mode == "encode":
            self.send_header("Content-Encoding", "gzip")
        if mode == "multipart":
            self.send_header("Content-Type", "multipart/byteranges")
        self.send_header("ETag", '"changed"' if mode == "changed" else ETAG)
        self.end_headers()
        self.wfile.write(payload)


@pytest.fixture
def http_and_budget():
    Handler.calls = []
    Handler.fixture_data = b""
    with tempfile.TemporaryDirectory(prefix="offline-transport-", dir=DEFAULT_WORK_ROOT) as work, \
            pytest.MonkeyPatch.context() as domain:
        from sakurapool.storage import budget, package

        domain.setattr(budget, "DEFAULT_WORK_ROOT", Path(work))
        domain.setattr(package, "DEFAULT_WORK_ROOT", Path(work))
        root = Path(work) / "ledger"
        root.mkdir()
        ledger = BudgetLedger(root, _offline_test=True)
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_port}"
            with GuardedTransport(ledger, trusted_hosts=frozenset({"127.0.0.1"}),
                                  allow_loopback_http=True) as transport:
                yield base, ledger, transport
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


def bound(base, name="good"):
    return BoundObject(f"{base}/{name}?token=SECRET", len(DATA), strong_etag=ETAG)


@pytest.mark.parametrize("merged", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
def test_atomic_sample_retrieval_merged_and_separate(http_and_budget, merged, reverse):
    base, ledger, client = http_and_budget
    positions = (8, 0) if reverse else (0, 8)
    image = Extent(positions[0], 4, hashlib.sha256(DATA[positions[0]:
                                                       positions[0]+4]).hexdigest())
    meta = Extent(positions[1], 4, hashlib.sha256(DATA[positions[1]:
                                                      positions[1]+4]).hexdigest())
    sample = AuditedSample("a"*32, image, meta, ".jpg")
    output = ledger.root / "delivered"
    output.mkdir()
    final = fetch_bound_sample(client, ledger, bound(base), sample, output, merged=merged)
    assert (final / "image.jpg").read_bytes() == DATA[positions[0]:positions[0]+4]
    assert (final / "metadata.json").read_bytes() == DATA[positions[1]:positions[1]+4]
    assert list(output.iterdir()) == [final]
    assert ledger.status()["saved_samples"] == 1
    assert ledger.status()["saved_bytes"] == 8
    assert ledger.status()["inflight"] == 0
    assert ledger.status()["attempts"] == (1 if merged else 2)
    with pytest.raises(FileExistsError):
        fetch_bound_sample(client, ledger, bound(base), sample, output)
    assert ledger.status()["attempts"] == (1 if merged else 2)


def test_corrupt_member_hash_never_delivers_partial_pair(http_and_budget):
    base, ledger, client = http_and_budget
    output = ledger.root / "delivered"
    output.mkdir()
    sample = AuditedSample("f"*32, Extent(0, 4, "0"*64),
                           Extent(8, 4, hashlib.sha256(DATA[8:12]).hexdigest()), ".jpg")
    with pytest.raises(RemoteIOError, match="SHA"):
        fetch_bound_sample(client, ledger, bound(base), sample, output)
    assert list(output.iterdir()) == []
    assert ledger.status()["saved_samples"] == 0
    assert ledger.status()["inflight"] > 0  # failed consumer retains owned lease


@pytest.mark.parametrize("workers", [1, 4, 8])
def test_bounded_workers_only_return_paths_no_pending_payload(http_and_budget, workers):
    base, ledger, client = http_and_budget
    output = ledger.root / "delivered"
    output.mkdir()
    imagesha = hashlib.sha256(DATA[:3]).hexdigest()
    metasha = hashlib.sha256(DATA[8:10]).hexdigest()
    samples = [AuditedSample(f"{i:032x}", Extent(0, 3, imagesha),
                             Extent(8, 2, metasha), ".jpg") for i in range(8)]
    delivered = fetch_bounded_samples(client, ledger, bound(base), samples,
                                      output, workers=workers)
    assert len(delivered) == 8 and len(set(delivered)) == 8
    assert all((path / "image.jpg").read_bytes() == DATA[:3] for path in delivered)
    assert ledger.status()["attempts"] == 8  # merged, one range each
    assert ledger.status()["inflight"] == 0
    assert ledger.status()["saved_samples"] == 8
    with pytest.raises(ValueError, match="worker"):
        fetch_bounded_samples(client, ledger, bound(base), [], output, workers=9)


def test_range_ownership_ack_holds_and_releases_inflight(http_and_budget):
    base, ledger, client = http_and_budget
    with client.read_range_owned(bound(base), 0, 4) as body:
        assert body == DATA[:4]
        assert ledger.status()["inflight"] == 15 + 2 * READ_CHUNK
        assert ledger.status()["body"] == 5  # 4 received + 1 reserved probe
    assert ledger.status()["inflight"] == 0
    assert ledger.status()["body"] == 4
    with pytest.raises(RuntimeError, match="consumer fault"):
        with client.read_range_owned(bound(base), 4, 4):
            raise RuntimeError("consumer fault")
    assert ledger.status()["inflight"] == 15 + 2 * READ_CHUNK  # failed handoff pending
    assert ledger.status()["attempts"] == 2


def test_correct_ranges_and_zero_no_network(http_and_budget):
    base, ledger, client = http_and_budget
    assert client.read_range(bound(base), 0, 0) == b""
    assert ledger.status()["attempts"] == 0
    assert client.read_range(bound(base), 0, 1) == DATA[:1]
    assert client.read_range(bound(base, "no-cl"), len(DATA)-1, 1) == DATA[-1:]
    assert client.read_range(bound(base), 3, 5) == DATA[3:8]
    assert ledger.status()["attempts"] == 3
    assert ledger.status()["body"] == 7
    assert all(auth is None for _, _, auth in Handler.calls)


@pytest.mark.parametrize("mode", ["ignore", "wrong-cr", "no-cr", "short",
                                   "overlong", "encode", "multipart", "changed"])
def test_refuse_bad_range_without_full_download(http_and_budget, mode):
    base, ledger, client = http_and_budget
    with pytest.raises(RemoteIOError) as caught:
        client.read_range(bound(base, mode), 1, 4)
    assert "SECRET" not in str(caught.value)
    assert ledger.status()["attempts"] == 1
    if mode in {"ignore", "wrong-cr", "no-cr", "encode", "multipart", "changed"}:
        assert ledger.status()["body"] == 0  # reject HEADERS before body
    else:
        assert ledger.status()["body"] in {3, 5}


def test_redirect_and_retry_share_attempt_ceiling(http_and_budget, monkeypatch):
    base, ledger, client = http_and_budget
    monkeypatch.setattr("sakurapool.storage.transport.time.sleep", lambda _: None)
    assert client.read_range(bound(base, "redirect"), 0, 2) == DATA[:2]
    assert ledger.status()["attempts"] == 2
    with pytest.raises(RemoteIOError, match="retry attempts exhausted"):
        client.read_range(bound(base, "retry"), 0, 2)
    assert ledger.status()["attempts"] == 5
    with pytest.raises(RemoteIOError, match="Retry-After exceeds"):
        client.read_range(bound(base, "retry-long"), 0, 2)
    assert ledger.status()["attempts"] == 6


def test_retry_and_redirect_share_one_three_attempt_ceiling(http_and_budget, monkeypatch):
    base, ledger, client = http_and_budget
    monkeypatch.setattr("sakurapool.storage.transport.time.sleep", lambda _: None)
    with pytest.raises(RemoteIOError, match="retry attempts exhausted"):
        client.read_range(bound(base, "redirect-retry"), 0, 1)
    assert ledger.status()["attempts"] == 3
    assert [name for name, _, _ in Handler.calls] == ["redirect-retry", "retry", "retry"]


def test_auth_401_403_single_attempt_and_redacted_socket_exception(http_and_budget,
                                                                       monkeypatch):
    base, ledger, client = http_and_budget
    for code in ("auth401", "auth403"):
        with pytest.raises(RemoteIOError, match="not 206"):
            client.read_range(bound(base, code), 0, 1)
    assert ledger.status()["attempts"] == 2
    def fail(*_args, **_kwargs):
        raise requests.Timeout("https://host.example/?token=SECRET")
    monkeypatch.setattr(client.session, "get", fail)
    with pytest.raises(RemoteIOError) as caught:
        client.read_range(bound(base), 0, 1)
    assert "SECRET" not in str(caught.value)
    assert ledger.status()["attempts"] == 5  # 2 auth + 3 bounded socket retries
    assert ledger.status()["body"] == 6  # all 3 ambiguous attempts conservatively pending


def test_approved_cross_host_strips_auth(http_and_budget):
    base, ledger, client = http_and_budget
    assert client.read_range(bound(base, "cross-host"), 0, 1) == DATA[:1]
    assert Handler.calls[0][2] is None
    assert Handler.calls[1][2] is None
    assert ledger.status()["attempts"] == 2


def test_production_credentials_on_origin_and_redirects_without_socket(monkeypatch):
    """Exercise real transport routing with a typed ledger/session HTTP boundary."""
    import traceback
    from dataclasses import dataclass, field

    @dataclass
    class MockProductionBudget:
        offline_mode: bool = False
        attempts: list = field(default_factory=list)
        def reserve(self, request):
            self.attempts.append(request)
            return str(len(self.attempts))
        def settle(self, _lease):
            pass

    @dataclass
    class MockResponse:
        status_code: int
        headers: dict
        def close(self):
            pass

    ledger = MockProductionBudget()
    origin = "https://modelscope.cn"
    cdn = "https://cdn.example.invalid"
    token = "SECRET_PRODUCTION_TEST_TOKEN"
    calls = []
    with GuardedTransport(ledger, trusted_hosts=frozenset({"modelscope.cn",
                                                            "cdn.example.invalid"}),
                          token=token, credential_origin=origin) as client:
        def fake_get(url, *, headers, stream, allow_redirects, timeout):
            assert stream and not allow_redirects and timeout == (10, 60)
            calls.append((url, headers.get("Authorization")))
            if url.endswith("/origin-cdn"):
                return MockResponse(302, {"Location": cdn + "/asset"})
            if url.endswith("/cdn-origin"):
                return MockResponse(302, {"Location": origin + "/returned"})
            if url.endswith("/origin-origin"):
                return MockResponse(302, {"Location": origin + "/same"})
            return MockResponse(204, {})
        monkeypatch.setattr(client.session, "get", fake_get)
        for url in (origin + "/plain", cdn + "/direct",
                    origin + "/origin-cdn", cdn + "/cdn-origin",
                    origin + "/origin-origin"):
            response, lease = client._response(url, max_body=4, metadata=False,
                                               inflight=0, headers={})
            assert response.status_code == 204
            ledger.settle(lease)
        assert calls == [
            (origin + "/plain", "Bearer " + token),
            (cdn + "/direct", None),
            (origin + "/origin-cdn", "Bearer " + token),
            (cdn + "/asset", None),
            (cdn + "/cdn-origin", None),
            (origin + "/returned", "Bearer " + token),
            (origin + "/origin-origin", "Bearer " + token),
            (origin + "/same", "Bearer " + token)]
        assert len(ledger.attempts) == len(calls)
        signed = origin + "/signed?signature=SECRET_SIGNATURE"
        def fake_error(*_args, **_kwargs):
            raise requests.Timeout(signed + "&token=" + token)
        monkeypatch.setattr(client.session, "get", fake_error)
        monkeypatch.setattr("sakurapool.storage.transport.time.sleep", lambda _: None)
        with pytest.raises(RemoteIOError) as failure:
            client._response(signed, max_body=4, metadata=False,
                             inflight=0, headers={})
        public = (str(failure.value) + repr(failure.value) +
                  "".join(traceback.format_exception(failure.value)) +
                  json.dumps({"error": str(failure.value)}))
        assert "SECRET_SIGNATURE" not in public and token not in public
        assert failure.value.__context__ is None
        assert len(ledger.attempts) == len(calls) + 3


def test_untrusted_redirect_never_leaks_signed_url_or_token(http_and_budget):
    base, ledger, client = http_and_budget
    with pytest.raises(RemoteIOError) as caught:
        client.read_range(bound(base, "evil"), 0, 2)
    assert "SECRET" not in str(caught.value)
    assert "PRIVATE" not in str(caught.value)
    assert len(Handler.calls) == 1
    assert ledger.status()["attempts"] == 1
    with pytest.raises(ValueError, match="weak"):
        BoundObject(base + "/good", len(DATA), strong_etag='W/"weak"')
    with pytest.raises(ValueError, match="floating"):
        BoundObject(base + "/good?Revision=master", len(DATA), immutable_revision="master")
    with pytest.raises(ValueError, match="strong validator"):
        BoundObject(base + "/good?Revision=" + "a"*40, len(DATA),
                    immutable_revision="a"*40)


def test_stage_single_stream_hashes_raw_tar_and_member_extents(http_and_budget):
    import sqlite3
    base, ledger, client = http_and_budget
    bound_tar = BoundObject(base + "/full-multi", len(TAR_MULTI), strong_etag=ETAG)
    destination = ledger.root / "completed-stage"
    result = stage_tar(client, ledger, bound_tar, destination,
                       DatasetAdapter("d", "s"), max_records=2)
    assert result.content_sha256 == hashlib.sha256(TAR_MULTI).hexdigest()
    assert result.members == 4 and result.potential_records == 2
    assert (destination / "stage.complete").exists()
    with closing(sqlite3.connect(result.database)) as db:
        rows = db.execute("SELECT name,offset_data,size,sha256 FROM members "
                          "ORDER BY name").fetchall()
    for name, offset, length, digest in rows:
        with tarfile.open(fileobj=io.BytesIO(TAR_MULTI), mode="r:") as archive:
            original = archive.getmember(name)
        assert offset == original.offset_data and length == original.size
        assert hashlib.sha256(TAR_MULTI[offset:offset+length]).hexdigest() == digest
    assert ledger.status()["body"] == len(TAR_MULTI)
    assert ledger.status()["disk"] >= result.database.stat().st_size
    assert open_completed_stage(destination, bound_tar, DatasetAdapter("d", "s")) == result
    scope = indexer._SpoolScope()
    rows = indexer._scan_shard_impl(
        None, "example.tar", DatasetAdapter("d", "s"), True,
        {"header_seconds": 0., "json_seconds": 0.},
        {"sha256": result.content_sha256, "size": result.object_size,
         "strength": "strong:sha256"}, scope=scope,
        staged_archive=StagedArchive(result), spool_directory=ledger.root)
    try:
        samples = list(rows.iter_rows("samples"))
        assert len(samples) == 2
        assert all(row["sha256"] in {
            hashlib.sha256(b"abc").hexdigest()} for row in samples)
        assert all(row["status"] == "indexed" for row in samples)
        assert next(rows.iter_rows("objects"))["backend"] == "modelscope"
    finally:
        rows.close()
    with pytest.raises(RemoteIOError, match="validator"):
        open_completed_stage(destination, BoundObject(base + "/full-multi",
                             len(TAR_MULTI), strong_etag='"changed"'),
                             DatasetAdapter("d", "s"))
    with pytest.raises(RemoteIOError, match="validator"):
        open_completed_stage(destination, bound_tar, DatasetAdapter("other", "s"))
    with pytest.raises(FileExistsError):
        stage_tar(client, ledger, bound_tar, destination,
                  DatasetAdapter("d", "s"), max_records=2)
    assert ledger.status()["attempts"] == 1


def test_staged_remote_v4_compiles_original_p3_and_queries(http_and_budget):
    base, ledger, client = http_and_budget
    bound_tar = BoundObject(base + "/full-multi", len(TAR_MULTI), strong_etag=ETAG)
    adapter = DatasetAdapter("demo", "synthetic", storage_id="modelscope:dataset")
    stage = stage_tar(client, ledger, bound_tar, ledger.root / "stage",
                      adapter, max_records=2)
    durable = ledger.root / "durable"
    audit_dir = ledger.root / "audit"
    audit_dir.mkdir()
    output = write_staged_v4(ledger, [("dir/example.tar", bound_tar, stage)],
                             durable, adapter, audit_output=audit_dir / "members.json")
    audited = json.loads((audit_dir / "members.json").read_bytes())
    assert len(audited) == 2
    assert all(item["image"]["sha256"] == hashlib.sha256(b"abc").hexdigest()
               for item in audited.values())
    assert output == {"objects": 1, "samples": 2, "annotations": 2, "errors": 0}
    inventory = load_p2_inventory(durable)
    assert len(inventory.objects) == 1
    assert ledger.status()["records"] == 2
    assert ledger.status()["attempts"] == 1  # no TAR re-fetch for v4/P3
    result = compile_runtime(inventory, ledger.root / "runtime")
    with RuntimeSnapshot.open(ledger.root / "runtime", full_verify=True) as snapshot:
        assert snapshot.snapshot_id == result.snapshot_id
        assert snapshot.query().count() == 2
        assert snapshot.object_ref(0)["backend"] == "modelscope"
    assert ledger.status()["attempts"] == 1


def test_real_p2_p3_package_builder_and_audit_roundtrip_offline(http_and_budget):
    base, ledger, client = http_and_budget
    bound_tar = BoundObject(base + "/full-multi", len(TAR_MULTI), strong_etag=ETAG)
    adapter = DatasetAdapter("demo", "synthetic", storage_id="modelscope:dataset")
    stage = stage_tar(client, ledger, bound_tar, ledger.root / "stage",
                      adapter, max_records=2)
    package_root = ledger.root / "package"
    package_root.mkdir()
    audit_dir = package_root / "audit"
    audit_dir.mkdir()
    durable = package_root / "durable"
    write_staged_v4(ledger, [("dir/example.tar", bound_tar, stage)], durable,
                    adapter, audit_output=audit_dir / "members.json")
    # Synthetic fixture ONLY: production P4 compiler is deliberately blocked
    # until its full physical disk bound is independently established.
    compile_runtime(load_p2_inventory(durable), package_root / "runtime")
    snapshot = RuntimeSnapshot.open(package_root / "runtime", full_verify=True)
    try:
        object_ref = snapshot.object_ref(0)
        record_id = next(snapshot.query().iter_record_batches()).record_id[0]
    finally:
        snapshot.close()
    endpoint = base  # local HTTP only; never a ModelScope request
    revision = "a" * 40
    binding = Binding(adapter.dataset, adapter.storage_id, object_ref["object_id"],
                      "dir/example.tar", len(TAR_MULTI), stage.content_sha256,
                      ETAG, True, hashlib.sha256(TAR_MULTI[:1]).hexdigest(), False)
    with pytest.raises(PackageCorrupt, match="missing app-owned"):
        publish_local_package(package_root, ledger, endpoint=endpoint,
                              data_revision=revision, bindings=[binding])
    # Synthetic ledger fixture only; this test is NOT evidence of target HTTP
    # conditional capability. The real path records this after guarded 206/412.
    key = condition_binding_key(endpoint=endpoint, repository="leafmoone/game_cg_5M",
                                revision=revision, path=binding.path, size=binding.size,
                                strong_etag=binding.strong_etag)
    ledger.record_condition_proof(key, binding.condition_probe_sha256)
    manifest = publish_local_package(package_root, ledger, endpoint=endpoint,
                                     data_revision=revision, bindings=[binding])
    assert manifest.exists()
    loaded = load_package(package_root, allow_offline_loopback=True)
    with RuntimeSnapshot.open(package_root / "runtime", full_verify=True) as snapshot:
        actual_binding, sample = loaded.sample(snapshot, record_id)
        assert actual_binding.content_sha256 == stage.content_sha256
        assert sample.image.sha256 == hashlib.sha256(b"abc").hexdigest()
    assert ledger.status()["attempts"] == 1  # no target or re-scan
    config = ledger.root / "offline-cli-profile.json"
    config.write_text(json.dumps({"repo_id": "leafmoone/game_cg_5M",
                                  "endpoint": endpoint, "revision": revision,
                                  "trusted_hosts": ["127.0.0.1"],
                                  "work_root": str(ledger.root)}), encoding="utf-8")
    env = os.environ.copy()
    source_path = str(Path(__file__).resolve().parents[1] / "src")
    env["PYTHONPATH"] = (os.environ["PYTHONPATH"]
                         if os.environ.get("SAKURAPOOL_TEST_EXCLUSIVE_ROOT") else source_path)
    plan = ledger.root / "inspect-plan.json"
    inspect = subprocess.run([sys.executable, "-m", "sakurapool", "remote", "inspect",
                              "--config", str(config), "--output", str(plan),
                              "--offline-fixture"], capture_output=True, text=True,
                             env=env, timeout=30, check=False)
    assert inspect.returncode == 0, (inspect.stdout, inspect.stderr)
    plan_data = json.loads(plan.read_text(encoding="utf-8"))
    assert plan_data["files_complete"] is True
    assert plan_data["source_and_tag_mapping"].startswith("unconfirmed")
    assert plan_data["repository"] == "leafmoone/game_cg_5M"
    delivered_root = ledger.root / "cli-delivered"
    delivered_root.mkdir()
    fetched = subprocess.run([sys.executable, "-m", "sakurapool", "remote", "fetch",
                              "--config", str(config), "--package", str(package_root),
                              "--record-id", record_id, "--output", str(delivered_root),
                              "--offline-fixture"], capture_output=True, text=True,
                             env=env, timeout=30, check=False)
    assert fetched.returncode == 0, (fetched.stdout, fetched.stderr)
    assert json.loads(fetched.stdout)["record_id"] == record_id
    delivered = delivered_root / record_id
    assert (delivered / "image.jpg").read_bytes() == b"abc"
    assert len([p for p in delivered.iterdir() if p.name.endswith(".json")]) == 1
    assert not (ledger.root / "local-tar-cache").exists()
    assert all(auth is None for _, _, auth in Handler.calls)


def test_cli_bootstrap_revisions_then_select_candidate_offline(http_and_budget):
    base, ledger, _client = http_and_budget
    config_path = ledger.root / "profile.json"
    config = {"repo_id": "leafmoone/game_cg_5M", "endpoint": base,
              "revision": None, "trusted_hosts": ["127.0.0.1"],
              "work_root": str(ledger.root)}
    config_path.write_text(json.dumps(config), encoding="utf-8")
    env = os.environ.copy()
    source_path = str(Path(__file__).resolve().parents[1] / "src")
    env["PYTHONPATH"] = (os.environ["PYTHONPATH"]
                         if os.environ.get("SAKURAPOOL_TEST_EXCLUSIVE_ROOT") else source_path)
    plan_path = ledger.root / "bootstrap-plan.json"
    def run():
        return subprocess.run([sys.executable, "-m", "sakurapool", "remote",
                               "inspect", "--config", str(config_path),
                               "--output", str(plan_path), "--offline-fixture"],
                              capture_output=True, text=True, timeout=25,
                              env=env, check=False)
    first = run()
    assert first.returncode == 0, (first.stdout, first.stderr)
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    assert plan["requested_revision"] is None
    assert plan["revision_candidates"] == ["a" * 40]
    assert not plan["files_complete"] and not plan["version_capability_verified"]
    assert [path for path, *_ in Handler.calls] == [
        "api/v1/datasets/leafmoone/game_cg_5M/revisions"]
    plan_path.unlink()
    config["revision"] = "master"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    bad = run()
    assert bad.returncode == 2 and not plan_path.exists()
    assert len(Handler.calls) == 1  # no floating alias ever sent
    config["revision"] = "a" * 40
    config_path.write_text(json.dumps(config), encoding="utf-8")
    selected = run()
    assert selected.returncode == 0, (selected.stdout, selected.stderr)
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    assert plan["requested_revision"] == "a" * 40
    assert plan["files_complete"] and not plan["version_capability_verified"]
    assert len(Handler.calls) == 3  # revisions + selected listing


def test_production_scan_build_package_block_before_any_http_or_artifact():
    ledger = SimpleNamespace(root=DEFAULT_WORK_ROOT.absolute())
    class NoNetwork:
        def stream_object(self, *_args):
            pytest.fail("production scan must stop before HTTP")
    bound_tar = BoundObject("https://modelscope.cn/repo", 10240,
                            strong_etag=ETAG)
    with pytest.raises(BudgetExceeded, match="BLOCKED"):
        stage_tar(NoNetwork(), ledger, bound_tar,
                  ledger.root / "must-not-create-stage", DatasetAdapter("d", "s"))
    with pytest.raises(BudgetExceeded, match="BLOCKED"):
        write_staged_v4(ledger, [], ledger.root / "must-not-create-durable",
                        DatasetAdapter("d", "s"))
    with pytest.raises(BudgetExceeded, match="BLOCKED"):
        publish_local_package(ledger.root / "must-not-create-package", ledger,
                              endpoint="https://modelscope.cn",
                              data_revision="a" * 40, bindings=[])
    for name in ("must-not-create-stage", "must-not-create-durable",
                 "must-not-create-package"):
        assert not (ledger.root / name).exists()


def test_offline_mode_external_initial_redirect_ledger_and_proxy_denial(http_and_budget):
    base, ledger, client = http_and_budget
    external = "https://modelscope.cn/leafmoone/game_cg_5M"
    before = ledger.status()["attempts"]
    with pytest.raises(ValueError, match="literal IPv4 loopback"):
        GuardedTransport(ledger, trusted_hosts=frozenset({"127.0.0.1", "modelscope.cn"}),
                         allow_loopback_http=True)
    with pytest.raises(ValueError, match="credentials"):
        GuardedTransport(ledger, trusted_hosts=frozenset({"127.0.0.1"}),
                         allow_loopback_http=True, token="PRIVATE")
    with pytest.raises(RemoteIOError):
        stage_tar(client, ledger, BoundObject(external, len(TAR), strong_etag=ETAG),
                  ledger.root / "external-stage", DatasetAdapter("d", "s"))
    assert Handler.calls == [] and ledger.status()["attempts"] == before
    assert not (ledger.root / "external-stage").exists()
    with pytest.raises(RemoteIOError):
        client.read_range(bound(base, "evil"), 0, 1)
    assert [name for name, *_ in Handler.calls] == ["evil"]
    assert ledger.status()["attempts"] == before + 1
    with tempfile.TemporaryDirectory(prefix="offline-mismatch-", dir=ledger._physical_root) as temp:
        alternate = BudgetLedger(Path(temp), _offline_test=True)
        with pytest.raises(ValueError, match="identical"):
            stage_tar(client, alternate, bound(base, "full"),
                      alternate.root / "mixed-stage", DatasetAdapter("d", "s"))
        assert not (alternate.root / "mixed-stage").exists()
    assert [name for name, *_ in Handler.calls] == ["evil"]
    client.session.trust_env = True
    with pytest.raises(RemoteIOError, match="credentials/proxy"):
        client.read_range(bound(base), 0, 1)
    client.session.trust_env = False
    client.session.proxies["http"] = "http://untrusted.invalid:1234"
    with pytest.raises(RemoteIOError, match="credentials/proxy"):
        client.read_range(bound(base), 0, 1)
    assert [name for name, *_ in Handler.calls] == ["evil"]


def test_record_cap_aborts_without_marker_and_without_finishing_tail(http_and_budget):
    base, ledger, client = http_and_budget
    bound_tar = BoundObject(base + "/full-multi", len(TAR_MULTI), strong_etag=ETAG)
    destination = ledger.root / "incomplete-stage"
    with pytest.raises(RemoteIOError, match="record cap"):
        stage_tar(client, ledger, bound_tar, destination,
                  DatasetAdapter("d", "s"), max_records=1)
    assert not (destination / "stage.complete").exists()
    assert ledger.status()["body"] == len(TAR_MULTI) + 1  # unfinished body pending


def test_stage_rejects_link_and_preserves_no_completed_marker(http_and_budget):
    base, ledger, client = http_and_budget
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:") as archive:
        info = tarfile.TarInfo("nested/link.jpg")
        info.type = tarfile.SYMTYPE
        info.linkname = "other.jpg"
        archive.addfile(info)
    Handler.fixture_data = output.getvalue()
    target = ledger.root / "bad-tar-stage"
    with pytest.raises(RemoteIOError, match="unsupported TAR member"):
        stage_tar(client, ledger, BoundObject(base + "/full-custom",
                  len(Handler.fixture_data), strong_etag=ETAG), target,
                  DatasetAdapter("d", "s"), max_records=2)
    assert not (target / "stage.complete").exists()


@pytest.mark.parametrize("format", [tarfile.PAX_FORMAT, tarfile.GNU_FORMAT])
def test_stage_accepts_pax_long_member_with_true_raw_extent(http_and_budget, format):
    import sqlite3
    base, ledger, client = http_and_budget
    name = "nested/" + "a" * 120 + "/1.JPG"
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:", format=format) as archive:
        info = tarfile.TarInfo(name)
        info.size = len(DATA)
        archive.addfile(info, io.BytesIO(DATA))
    Handler.fixture_data = output.getvalue()
    result = stage_tar(client, ledger,
                       BoundObject(base + "/full-custom", len(Handler.fixture_data),
                                   strong_etag=ETAG), ledger.root / "pax-stage",
                       DatasetAdapter("d", "s"), max_records=1)
    with closing(sqlite3.connect(result.database)) as db:
        stored = db.execute("SELECT offset_data,size,sha256 FROM members WHERE name=?",
                            (name,)).fetchone()
    assert stored is not None
    offset, size, digest = stored
    assert Handler.fixture_data[offset:offset + size] == DATA
    assert digest == hashlib.sha256(DATA).hexdigest()


@pytest.mark.parametrize("member_type", [tarfile.CHRTYPE, tarfile.BLKTYPE,
                                         tarfile.FIFOTYPE, tarfile.LNKTYPE,
                                         tarfile.GNUTYPE_SPARSE])
def test_stage_refuses_noncontiguous_or_special_tar_member(http_and_budget, member_type):
    base, ledger, client = http_and_budget
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w:", format=tarfile.GNU_FORMAT) as archive:
        info = tarfile.TarInfo("nested/1.jpg")
        info.type = member_type
        info.linkname = "nested/other.jpg" if member_type == tarfile.LNKTYPE else ""
        archive.addfile(info)
    Handler.fixture_data = data.getvalue()
    destination = ledger.root / "special-not-published"
    with pytest.raises(RemoteIOError):
        stage_tar(client, ledger, BoundObject(base + "/full-custom",
                  len(Handler.fixture_data), strong_etag=ETAG), destination,
                  DatasetAdapter("d", "s"), max_records=1)
    assert not (destination / "stage.complete").exists()
    assert ledger.status()["attempts"] == 1


def test_full_stream_tarfile_drains_tail_and_charges_once(http_and_budget):
    base, ledger, client = http_and_budget
    bound_tar = BoundObject(base + "/full", len(TAR), strong_etag=ETAG)
    with client.stream_object(bound_tar) as stream:
        with tarfile.open(fileobj=stream, mode="r|") as archive:
            member = next(iter(archive))
            assert member.name == "nested/1.json"
            assert archive.extractfile(member).read() == DATA
    assert stream.count == len(TAR)
    assert stream._hash.hexdigest() == hashlib.sha256(TAR).hexdigest()
    assert ledger.status()["body"] == len(TAR)
    assert ledger.status()["attempts"] == 1


@pytest.mark.parametrize("name", ["full-short", "full-overlong"])
def test_full_stream_size_failure_keeps_pending(http_and_budget, name):
    base, ledger, client = http_and_budget
    with pytest.raises(RemoteIOError):
        with client.stream_object(BoundObject(base + "/" + name, len(TAR),
                                              strong_etag=ETAG)) as stream:
            with tarfile.open(fileobj=stream, mode="r|") as archive:
                next(iter(archive))
    assert ledger.status()["body"] == len(TAR) + 1  # conservative pending


def test_provider_metadata_uses_same_transport_and_ledger(http_and_budget):
    base, ledger, client = http_and_budget
    provider = ModelScopeDataset(client, base, "leafmoone/game_cg_5M")
    assert provider.revisions() == ["a" * 40]
    files, complete = provider.list_files("a" * 40)
    assert complete and files[0].path == "example.tar"
    assert ledger.status()["attempts"] == 2
    assert ledger.status()["body"] == ledger.status()["metadata"] > 0


@pytest.mark.parametrize("exc", [
    ProtocolError("SECRET_URL?token=HIDDEN"),
    ReadTimeoutError(None, "/SECRET_URL?token=HIDDEN", "secret"),
    OSError("SECRET_URL?token=HIDDEN"),
])
@pytest.mark.parametrize("operation", ["range", "metadata", "stream"])
def test_raw_failures_keep_pending_and_cannot_leak_through_context(
        http_and_budget, monkeypatch, exc, operation):
    base, ledger, client = http_and_budget
    class Raw:
        def read(self, *_args, **_kwargs):
            raise exc
    class Response:
        status_code = 200 if operation != "range" else 206
        headers = {"ETag": ETAG, "Content-Range": f"bytes 0-1/{len(DATA)}",
                   "Content-Length": "2"}
        raw = Raw()
        def close(self):
            pass
    monkeypatch.setattr(client.session, "get", lambda *_args, **_kwargs: Response())
    with pytest.raises(RemoteIOError) as caught:
        if operation == "range":
            client.read_range(bound(base), 0, 2)
        elif operation == "metadata":
            client.read_metadata(base + "/meta", max_bytes=8)
        else:
            with client.stream_object(BoundObject(base + "/full", 2,
                                                 strong_etag=ETAG)) as stream:
                stream.read(2)
    emitted = "".join(traceback.format_exception(caught.value))
    assert "SECRET_URL" not in emitted and "HIDDEN" not in emitted
    assert caught.value.__context__ is None
    assert ledger.status()["attempts"] == 1
    expected = 3 if operation == "range" else 9 if operation == "metadata" else 3
    assert ledger.status()["body"] == expected  # reservation retained, no refund


def test_bound_object_repr_and_serialization_hide_signed_url(http_and_budget):
    base, ledger, client = http_and_budget
    b = BoundObject(base + "/good?token=SECRET", len(DATA), strong_etag='"SECRET_ETAG"')
    assert "SECRET" not in repr(b)
    with pytest.raises(TypeError):
        asdict(b)
    with pytest.raises(TypeError):
        json.dumps(b)
    with pytest.raises(TypeError, match="cannot be serialized"):
        pickle.dumps(b)
    with pytest.raises(AttributeError):
        b.url = "http://localhost/other"
    # Offline transport never receives/forwards credentials; localhost is
    # rejected rather than allowing DNS resolution to widen the scope.
    other = base.replace("127.0.0.1", "localhost")
    with pytest.raises(RemoteIOError):
        client.read_range(bound(other), 0, 1)
    assert client.read_range(bound(base), 0, 1) == DATA[:1]
    assert Handler.calls[-1][2] is None
    assert ledger.status()["attempts"] == 1


def test_condition_proof_persisted_only_for_exact_binding(http_and_budget):
    base, ledger, client = http_and_budget
    revision = "a" * 40
    b = BoundObject(base + "/if-match-enforced?Revision=" + revision,
                    len(DATA), revision, ETAG)
    options = dict(endpoint=base, repository="leafmoone/game_cg_5M",
                   path="example.tar",
                   expected_probe_sha256=hashlib.sha256(DATA[:1]).hexdigest())
    assert client.ensure_verified_condition(b, **options) == options["expected_probe_sha256"]
    assert ledger.status()["attempts"] == 2
    reopened = BudgetLedger(ledger.root, _offline_test=True)
    with GuardedTransport(reopened, trusted_hosts=frozenset({"127.0.0.1"}),
                          allow_loopback_http=True) as other:
        other.ensure_verified_condition(b, **options)
    assert ledger.status()["attempts"] == 2
    with pytest.raises(RemoteIOError, match="manifest/ledger"):
        client.ensure_verified_condition(b, **{**options,
            "expected_probe_sha256": "f"*64})
    assert ledger.status()["attempts"] == 2
    with pytest.raises(RemoteIOError, match="not 206"):
        changed = BoundObject(b.url, len(DATA), revision, '"changed"')
        client.ensure_verified_condition(changed, **options)
    assert ledger.status()["attempts"] == 3


def test_validated_if_match_pair_and_ignored_condition_refused(http_and_budget):
    base, ledger, client = http_and_budget
    assert client.verify_if_match(bound(base, "if-match-enforced")) == hashlib.sha256(
        DATA[:1]).hexdigest()
    assert ledger.status()["attempts"] == 2
    with pytest.raises(RemoteIOError, match="did not enforce"):
        client.verify_if_match(bound(base, "good"))
    assert ledger.status()["attempts"] == 4


def test_retry_after_http_date_nonfinite_and_invalid_never_retry_early(http_and_budget):
    _, _, client = http_and_budget
    date = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=1), usegmt=True)
    assert 0 <= client._retry_delay(date, 0) <= client.max_retry_wait_s
    for invalid in ("nan", "inf", "-inf", "SECRET_INVALID_DATE", "9999"):
        with pytest.raises(RemoteIOError) as caught:
            client._retry_delay(invalid, 0)
        assert "SECRET_" not in "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize("scenario,expected_attempts", [
    ("signed_unknown403", 2), ("signed_permission403", 2),
    ("signed_quota403", 2), ("direct403", 1)])
def test_signed_redirect_403_never_refreshes_without_proven_expiry_signal(
        http_and_budget, monkeypatch, scenario, expected_attempts):
    base, ledger, client = http_and_budget
    revision = "a" * 40
    source = (base + "/api/v1/datasets/leafmoone/game_cg_5M/repo?"
              + "Revision=" + revision + "&FilePath=example.tar")
    secret = "SENSITIVE_SIGNED_QUERY_TOKEN"
    signed = base + "/signed?signature=" + secret
    bound = BoundObject(source, len(DATA), revision, ETAG)
    class Raw:
        def __init__(self, data): self.data = io.BytesIO(data)
        def read(self, n, *, decode_content):
            assert decode_content is False
            return self.data.read(n)
    class Response:
        def __init__(self, status, headers=None, data=b""):
            self.status_code = status
            self.headers = headers or {}
            self.raw = Raw(data)
        def close(self): pass
    calls = []
    def fake_get(url, *, headers, stream, allow_redirects, timeout):
        calls.append((url, headers.copy()))
        assert stream and not allow_redirects and timeout == (10, 60)
        assert headers.get("If-Match") == ETAG
        assert headers.get("Range") == "bytes=0-1"
        if scenario == "direct403":
            return Response(403)
        if url == signed:
            # Server-generated permission/quota/unknown bodies MUST NOT be
            # parsed as an invented expiry signal or written into diagnostics.
            return Response(403, {"X-Error-Kind": scenario},
                            b"SENSITIVE_SIGNED_QUERY_TOKEN")
        return Response(302, {"Location": signed})
    monkeypatch.setattr(client.session, "get", fake_get)
    with pytest.raises(RemoteIOError) as failure:
        client.read_range(bound, 0, 2)
    assert secret not in (str(failure.value) + repr(failure.value)
                          + "".join(traceback.format_exception(failure.value)))
    assert failure.value.__context__ is None
    assert len(calls) == ledger.status()["attempts"] == expected_attempts
    assert ledger.status()["body"] == 0
    assert all(request_headers.get("Authorization") is None
               for _url, request_headers in calls)
    assert [url for url, _headers in calls] == (
        [source] if scenario == "direct403" else [source, signed])


def test_refresh_origin_requires_one_exact_provider_path_and_pin(http_and_budget):
    base, _ledger, client = http_and_budget
    rev = "a" * 40
    good = (base + "/api/v1/datasets/leafmoone/game_cg_5M/repo?"
            + "Revision=" + rev + "&FilePath=example.tar")
    # The refresh origin is rebuilt from the PERSISTENT repository identity
    # sealed into the bound object, not from any hardcoded repository.
    assert client._pinned_refresh_origin(BoundObject(
        good, len(DATA), rev, ETAG,
        repository="leafmoone/game_cg_5M")) == good
    # A bound object without a repository configuration refuses refresh.
    assert client._pinned_refresh_origin(BoundObject(good, len(DATA), rev, ETAG)) is None
    # A provider repo route carrying a DIFFERENT owner/name than the sealed
    # repository identity is refused at binding time.
    other_route = good.replace("/leafmoone/game_cg_5M/",
                               "/other-owner/other-name/")
    with pytest.raises(ValueError, match="repository route mismatch"):
        BoundObject(other_route, len(DATA), rev, ETAG,
                    repository="leafmoone/game_cg_5M")
    # A SWAPPED owner/name is not accepted either: the route must equal the
    # configured (owner, name) exactly, in order; there is no swapped branch.
    swapped_route = good.replace("/leafmoone/game_cg_5M/",
                                 "/game_cg_5M/leafmoone/")
    with pytest.raises(ValueError, match="repository route mismatch"):
        BoundObject(swapped_route, len(DATA), rev, ETAG,
                    repository="leafmoone/game_cg_5M")
    # The correct route still binds, so the refusal is the mismatch, not the
    # route shape itself.
    assert client._pinned_refresh_origin(BoundObject(
        good, len(DATA), rev, ETAG,
        repository="leafmoone/game_cg_5M")) == good
    # Malformed repository configuration is refused by the bound object too.
    with pytest.raises(ValueError, match="owner/name"):
        BoundObject(good, len(DATA), rev, ETAG, repository="a..b/c")
    for url in (good + "&FilePath=changed.tar", good + "&token=SECRET",
                good + "#fragment", good.replace("&FilePath=example.tar", ""),
                good.replace("example.tar", "../escape.tar")):
        try:
            candidate = BoundObject(url, len(DATA), rev, ETAG,
                                    repository="leafmoone/game_cg_5M")
        except ValueError:
            continue  # bound object can fail before the resolver
        assert client._pinned_refresh_origin(candidate) is None


@pytest.mark.parametrize("mode,status", [("auth401", 401), ("auth403", 403)])
def test_metadata_header_rejection_reports_known_status_without_body(http_and_budget,
                                                                      mode, status):
    base, ledger, client = http_and_budget
    secret = "SIGNED_URL_TOKEN_SUPER_SECRET"
    url = f"{base}/{mode}?signature={secret}"
    with pytest.raises(RemoteIOError) as failure:
        client.read_metadata(url)
    assert failure.value.public_diagnostic() == {
        "code": "http_status", "phase": "metadata_headers", "http_status": status}
    public = (str(failure.value) + repr(failure.value)
              + "".join(traceback.format_exception(failure.value)))
    assert secret not in public and failure.value.__context__ is None
    assert ledger.status()["attempts"] == 1 and ledger.status()["metadata"] == 0


def test_metadata_uses_same_ledger(http_and_budget):
    base, ledger, client = http_and_budget
    result = client.read_metadata(base + "/meta", max_bytes=128)
    assert json.loads(result) == {"items": ["demo"]}
    assert ledger.status()["attempts"] == 1
    assert ledger.status()["body"] == len(result)
    assert ledger.status()["metadata"] == len(result)
