"""Production-profile delivery; local IO, real workspace accounting."""
import hashlib
import json
import sqlite3
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from sakurapool.capacity import CapacityConfig, ResourcePolicy
from sakurapool.storage import publication_fetch as fetch
from sakurapool.storage.budget import Reservation
from sakurapool.storage.production import ProviderObject, RustProductionTransport, proof_key
from sakurapool.storage.transport import RemoteIOError
from sakurapool.workspace import Workspace

MIB = 1 << 20


class LocalProduction(RustProductionTransport):
    def __init__(self, ledger, image, metadata, fault):
        super().__init__(ledger, Path("unused-worker"), origin="https://modelscope.cn")
        self.image, self.metadata, self.fault = image, metadata, fault
        self.calls, self.pending = [], []

    def verify_conditions(self, obj):
        obj = replace(obj, validator='"fixed"', cdn_host="cdn.modelscope.cn")
        self.ledger.record_condition_proof(proof_key(obj), hashlib.sha256(b"i").hexdigest())
        self.register(obj)
        return obj

    def _call(self, obj, root, *, start=0, length=1, **kwargs):
        self.calls.append((start, length, obj.validator))
        lease = self.ledger.reserve(Reservation(body=length + 1, attempt=True))
        if self.fault == "unknown" and len(self.calls) == 2:
            self.ledger.consume_body(lease, 11)
            self.pending.append(lease)
            raise RemoteIOError("unknown worker exit")
        raw = (self.image[start:start + length] if start < len(self.image)
               else self.metadata[start - len(self.image):start - len(self.image) + length])
        if self.fault == "short" and len(self.calls) == 2:
            raw = raw[:-1]
        self.ledger.consume_body(lease, len(raw))
        self.ledger.settle(lease)
        (root / "body").write_bytes(raw)
        return {"status": 412 if self.fault == "412" else 200 if self.fault == "200" else 206,
                "etag": '"changed"' if self.fault == "etag" else obj.validator,
                "cdn_host": obj.cdn_host, "sha256": hashlib.sha256(raw).hexdigest()}


@contextmanager
def delivery(tmp_path, monkeypatch, fault="none"):
    image = b"i" * (9 * MIB + 7)
    metadata = b'{"ok":true}' if fault != "json" else b"PRIVATE_INVALID_JSON"
    capacity = replace(CapacityConfig(), image_max_bytes=len(image), range_chunk_bytes=3 * MIB)
    workspace = Workspace.init(tmp_path / "workspace", capacity=capacity,
                               policy=ResourcePolicy(body=None, attempts=None, saved_bytes=None))
    transport = LocalProduction(workspace.ledger(), image, metadata, fault)
    size, digest = len(image) + len(metadata), hashlib.sha256(image).digest()
    obj = ProviderObject("synthetic/test", "modelscope_dataset_legacy", transport.origin,
                         "b" * 40, "gc5m/one.tar", size)
    monkeypatch.setattr(fetch, "exact_provider_lookup", lambda *args: obj)
    db = sqlite3.connect(":memory:")
    db.executescript("CREATE TABLE repositories(repo_idx,endpoint,repo_id,repo_type);"
                     "CREATE TABLE objects(repo_idx,object_idx,revision_candidate,object_path,"
                     "object_size,content_sha256,provider_sha256,fetchable);")
    db.execute("INSERT INTO repositories VALUES(0,?,?,?)", (obj.origin, obj.repo_id, obj.repo_type))
    db.execute("INSERT INTO objects VALUES(0,0,?,?,?,?,?,1)",
               (obj.revision, obj.object_path, size.to_bytes(8, "big"), digest, digest))
    record = "a" * 32
    pub = SimpleNamespace(_closed=False, full_verified=True, catalog=db,
                          content_digest="content", _verified=OrderedDict(),
                          expected_image_sha=lambda rid: bytes(32) if fault == "sha" else digest)
    pub.runtime = SimpleNamespace(
        snapshot_id="snapshot", resolve_record=lambda r: SimpleNamespace(rid=0),
        location=lambda rid: dict(object_idx=0, image_offset=0, image_size=len(image),
                                  metadata_offset=len(image), metadata_size=len(metadata),
                                  flags=1, format_id=0),
        image_format=lambda fid: "jpg",
        object_ref=lambda idx: dict(object_path=obj.object_path, object_size=size,
                                   object_version=digest.hex()))
    try:
        yield pub, record, transport, workspace
    finally:
        transport.close()
        db.close()


def test_complete_stream_receipt_once(tmp_path, monkeypatch):
    with delivery(tmp_path, monkeypatch) as (pub, record, transport, workspace):
        events, opens = [], []
        original_open = Path.open

        def tracked_open(path, mode="r", *args, **kwargs):
            if path.name == "image.jpg":
                opens.append(mode)
            return original_open(path, mode, *args, **kwargs)

        monkeypatch.setattr(Path, "open", tracked_open)
        result = fetch._fetch_publication_sample(pub, record, transport, workspace.tmp,
                    metadata=True, attempt_hook=lambda event, value: events.append((event, value)))
        assert opens == ["xb"]
        assert (result / "image.jpg").stat().st_size == 9 * MIB + 7
        actual_sha = hashlib.sha256((result / "image.jpg").read_bytes()).digest()
        assert actual_sha == pub.expected_image_sha(0)
        assert json.loads((result / "metadata.json").read_bytes()) == {"ok": True}
        assert [n for _, n, _ in transport.calls] == [3 * MIB] * 3 + [7, 11]
        assert {etag for _, _, etag in transport.calls} == {'"fixed"'}
        assert [event for event, _ in events].count("PUBLISHED") == 1
        receipt = dict(events)["PREPARED"]["receipt"]
        assert receipt["image.jpg"]["sha256"] == pub.expected_image_sha(0).hex()
        assert receipt["metadata.json"]["verification"] == "BOUNDED_JSON_NO_PUBLICATION_SHA"
        status = transport.ledger.status()
        assert status["saved_samples"] == 1
        assert status["saved_bytes"] == 9 * MIB + 18


@pytest.mark.parametrize(
    "fault", ["sha", "short", "412", "200", "etag", "unknown", "json", "settle"]
)
def test_fault_delivery_conservative(tmp_path, monkeypatch, fault):
    with delivery(tmp_path, monkeypatch, fault) as (pub, record, transport, workspace):
        if fault == "settle":
            original = transport.ledger.settle

            def settle(lease, **kwargs):
                if kwargs.get("saved_samples"):
                    raise OSError("unconfirmed saved settlement")
                return original(lease, **kwargs)

            monkeypatch.setattr(transport.ledger, "settle", settle)
        with pytest.raises(fetch.PublicationFetchError) as caught:
            fetch._fetch_publication_sample(pub, record, transport, workspace.tmp, metadata=True)
        assert (workspace.tmp / record).exists() == (fault == "settle")
        assert caught.value.delivery_published == (fault == "settle")
        with transport.ledger._locked():
            _, (_, _, pending, _) = transport.ledger._read_pair()
        if fault == "unknown":
            assert pending[transport.pending[0]]["consumed_body"] == 11
        if fault == "json":
            from sakurapool.storage.bounded_json import VALIDATION_INFLIGHT_BYTES

            assert any(p["inflight"] == VALIDATION_INFLIGHT_BYTES for p in pending.values())
            assert "PRIVATE_INVALID_JSON" not in str(caught.value)
        if fault == "settle":
            assert any(p["saved_samples"] == 1 for p in pending.values())


def test_generation_credit_refresh_and_warm(tmp_path):
    capacity = replace(CapacityConfig(), range_chunk_bytes=40 * MIB, metadata_max_bytes=2 * MIB)
    workspace = Workspace.init(tmp_path / "workspace", capacity=capacity,
                               policy=ResourcePolicy(body=None, attempts=None))
    transport = LocalProduction(workspace.ledger(), b"i", b"{}", "none")
    obj = transport.verify_conditions(ProviderObject("synthetic/test", "modelscope_dataset_legacy",
                              transport.origin, "b" * 40, "one.tar", 3))
    transport.enable_persistent()
    transport._lane_worker = SimpleNamespace()
    transport._lane_failed = False
    transport._live_proofs.add(proof_key(obj))
    transport._lane_body = 8 << 30
    transport._lane_requests = 1
    transport._lane_attempts = 2
    identity = (obj.origin, obj.repo_id, obj.repo_type, obj.revision,
                obj.object_path, obj.object_size)
    assert transport.predict_warm(identity, [1])
    workspace.update_policy(replace(workspace.policy, disk=3 << 30))
    assert transport._generation_budget()["disk"] == 3 << 30
    assert transport.ledger.limits["body"] is None
    assert transport.ledger.limits["attempts"] is None
    transport._lane_requests = 256
    assert not transport.predict_warm(identity, [1])
    transport._persistent = False
    transport.ledger.settle(transport._lane_lease)
    transport.close()


@pytest.mark.parametrize("changed", [False, True])
def test_chunk_generation_reproof_same_validator(tmp_path, monkeypatch, changed):
    with delivery(tmp_path, monkeypatch) as (pub, record, transport, workspace):
        obj = transport.verify_conditions(ProviderObject(
            "synthetic/test", "modelscope_dataset_legacy", transport.origin,
            "b" * 40, "gc5m/one.tar", len(transport.image) + len(transport.metadata)
        ))
        from sakurapool.storage.modelscope import ModelScopeDataset
        from sakurapool.storage.transport import BoundObject

        bound = BoundObject(ModelScopeDataset(transport, obj.origin, obj.repo_id).download_url(
            obj.revision, obj.object_path), obj.object_size, obj.revision, obj.validator,
            repository=obj.repo_id)
        transport._persistent = True
        proofs = []

        def rotate(*args):
            transport._live_proofs.clear()
            transport._objects.clear()

        def reproof(candidate):
            proofs.append(candidate)
            refreshed = replace(candidate, validator='"changed"') if changed else candidate
            transport.register(refreshed) if not changed else None
            transport._live_proofs.add(proof_key(refreshed))
            return refreshed

        @contextmanager
        def local_transfer(candidate, **kwargs):
            assert candidate == obj
            yield None, {}, b"i"

        monkeypatch.setattr(transport, "_admit_generation", rotate)
        monkeypatch.setattr(transport, "verify_conditions", reproof)
        monkeypatch.setattr(transport, "_transfer_owned", local_transfer)
        try:
            if changed:
                with pytest.raises(RemoteIOError, match="validator changed"):
                    with transport.read_range_owned(bound, 0, 1):
                        pytest.fail("changed validator delivered")
            else:
                with transport.read_range_owned(bound, 0, 1) as raw:
                    assert raw == b"i"
            assert proofs == [obj]
        finally:
            transport._persistent = False


def test_range_exit_preserves_unknown_lease(tmp_path, monkeypatch):
    with delivery(tmp_path, monkeypatch) as (pub, record, transport, workspace):
        original = transport.read_range_owned
        lease = transport.ledger.reserve(Reservation(body=31, attempt=True))
        transport.ledger.consume_body(lease, 3)

        @contextmanager
        def exit_unknown(*args):
            with original(*args) as payload:
                yield payload
            raise RemoteIOError("range exit UNKNOWN")

        monkeypatch.setattr(transport, "read_range_owned", exit_unknown)
        with pytest.raises(fetch.PublicationFetchError) as caught:
            fetch._fetch_publication_sample(pub, record, transport, workspace.tmp)
        assert caught.value.accounting_state == "UNKNOWN"
        assert not (workspace.tmp / record).exists()
        with transport.ledger._locked():
            _, (_, _, pending, _) = transport.ledger._read_pair()
        assert pending[lease]["body"] == 31
        assert pending[lease]["consumed_body"] == 3
