import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import numpy as np
import pytest
from test_publication import inputs as publication_inputs

from sakurapool.runtime.errors import SnapshotCorruptError
from sakurapool.runtime.snapshot import RuntimeSnapshot
from sakurapool.storage.publication import (
    PublicationCorrupt,
    build_publication,
    fetchable_rids,
    load_publication,
)
from sakurapool.storage.publication_session import PublicationSession


@pytest.fixture
def inputs(tmp_path):
    return publication_inputs.__wrapped__(tmp_path)


def test_manifest_digest_and_session_lifetime(inputs):
    rt, roots, mapping, out, _ = inputs
    build_publication(rt, roots, mapping, out)
    raw = (out / "PUBLICATION.json").read_bytes()
    transport = SimpleNamespace(ledger=object())
    with PublicationSession(out, transport) as session:
        pub = session.publication
        assert pub.content_digest == hashlib.sha256(raw).hexdigest()
        assert pub.full_verified
        assert pub.expected_image_sha(0) == hashlib.sha256(b"image").digest()
        with ThreadPoolExecutor(1) as pool:
            with pytest.raises(PublicationCorrupt):
                pool.submit(session._check).result()
        transport.ledger = object()
        with pytest.raises(PublicationCorrupt):
            session._check()
        transport.ledger = session.ledger
    with pytest.raises(PublicationCorrupt):
        pub.expected_image_sha(0)
    assert not pub._verified
    pub.close()


@pytest.mark.parametrize("path", ["../outside", "/outside", "snapshots/../outside"])
def test_runtime_current_containment(inputs, path):
    rt, *_ = inputs
    current = json.loads((rt / "current.json").read_bytes())
    current["path"] = path
    (rt / "current.json").write_text(json.dumps(current))
    with pytest.raises(SnapshotCorruptError):
        RuntimeSnapshot.open(rt)


def test_bad_object_idx_bounded():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE objects(object_idx,fetchable)")
    db.execute("INSERT INTO objects VALUES(4294967295,1)")
    loc = np.zeros(2, dtype=[("object_idx", "<u4")])
    with pytest.raises(PublicationCorrupt):
        fetchable_rids(db, loc, 1)
    db.execute("DELETE FROM objects")
    db.execute("INSERT INTO objects VALUES(0,1)")
    loc[0]["object_idx"] = 4294967295
    with pytest.raises(PublicationCorrupt):
        fetchable_rids(db, loc, 1)
    db.close()


def test_same_snapshot_different_manifest_digest(inputs):
    rt, roots, mapping, out, row = inputs
    build_publication(rt, roots, mapping, out)
    other = out.with_name("publication-null")
    row["provider_sha256"] = None
    mapping.write_text(json.dumps(row) + "\n")
    build_publication(rt, roots, mapping, other)
    with (
        load_publication(out, full_verify=True) as a,
        load_publication(other, full_verify=True) as b,
    ):
        assert a.manifest["publication_id"] == b.manifest["publication_id"]
        assert a.content_digest != b.content_digest


@pytest.mark.parametrize("ready", [b"", b"0" * 64, b"a" * 67])
def test_standalone_ready_strict(inputs, ready):
    rt, *_ = inputs
    current = json.loads((rt / "current.json").read_bytes())
    (rt / current["path"] / "READY").write_bytes(ready)
    with pytest.raises(SnapshotCorruptError):
        RuntimeSnapshot.open(rt)


def test_query_variable_limit_and_batch_types(inputs):
    rt, *_ = inputs
    with RuntimeSnapshot.open(rt) as snapshot:
        result = snapshot.query()
        snapshot._catalog.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 1)
        assert sum(len(b.rid) for b in result.iter_record_batches(99999)) == snapshot.rid_count
        for size in (True, 0, -1, 1.5, "2"):
            with pytest.raises(ValueError):
                list(result.iter_record_batches(size))
            with pytest.raises(ValueError):
                list(result.iter_location_batches(size))
        record = next(iter(result.iter_record_batches())).record_id[0]
        assert snapshot.resolve_record(record).rid == 0
        assert len(list(snapshot.iter_objects())) == 1


def test_atomic_no_replace_actual(tmp_path):
    from sakurapool.storage.retrieval import _publish_directory

    stage, final = tmp_path / "stage", tmp_path / "final"
    stage.mkdir()
    final.mkdir()
    with pytest.raises(OSError):
        _publish_directory(stage, final)
    assert stage.is_dir() and final.is_dir()


def test_session_full_verify_once(inputs, monkeypatch):
    from sakurapool.storage import publication_session as module

    rt, roots, mapping, out, _ = inputs
    build_publication(rt, roots, mapping, out)
    original = module.load_publication
    calls = []

    def load(*a, **kw):
        calls.append(kw)
        return original(*a, **kw)

    monkeypatch.setattr(module, "load_publication", load)
    monkeypatch.setattr(module, "fetch_publication_sample", lambda *a, **kw: "offline-call")
    with module.PublicationSession(out, SimpleNamespace(ledger=object())) as session:
        assert session.fetch("a" * 32, out) == "offline-call"
        assert session.fetch("b" * 32, out) == "offline-call"
    assert calls == [{"full_verify": True}]


def test_session_same_ledger_transport_tamper(inputs):
    rt, roots, mapping, out, _ = inputs
    build_publication(rt, roots, mapping, out)
    transport = SimpleNamespace(ledger=object())
    session = PublicationSession(out, transport)
    original = session.publication
    session.transport = SimpleNamespace(ledger=transport.ledger)
    with pytest.raises(PublicationCorrupt):
        session._check()
    session.close()
    assert original._closed


def test_owned_cleanup_preserves_unknown_and_replaced(tmp_path):
    from sakurapool.fs_safety import cleanup_owned_tree, owned_tree_identity

    stage = tmp_path / "owned"
    stage.mkdir()
    file = stage / "own"
    file.write_bytes(b"owned")
    identity = owned_tree_identity(stage)
    unknown = stage / "unknown"
    unknown.write_bytes(b"external")
    assert not cleanup_owned_tree(stage, identity)
    assert file.read_bytes() == b"owned" and unknown.read_bytes() == b"external"
    unknown.unlink()  # This test itself created it.
    assert cleanup_owned_tree(stage, identity)
    assert not stage.exists()


def test_stage_unknown_injection_does_not_mask_error(inputs, monkeypatch):
    from sakurapool.storage import publication as module

    rt, roots, mapping, out, _ = inputs
    original = module.load_publication
    inserted = []

    def fail(root, **kw):
        if str(root).find(".publication-") != -1:
            unknown = root / "external"
            unknown.write_bytes(b"not owned")
            inserted.append(unknown)
            raise RuntimeError("primary error")
        return original(root, **kw)

    monkeypatch.setattr(module, "load_publication", fail)
    with pytest.raises(RuntimeError, match="primary error"):
        build_publication(rt, roots, mapping, out)
    assert inserted[0].read_bytes() == b"not owned"
    assert not out.exists()


def test_production_structural_cache_bound_and_credentials():
    from dataclasses import replace

    from sakurapool.storage.production import ProviderObject, RustProductionTransport
    from sakurapool.storage.transport import RemoteIOError

    # Pure identity unit test: no budget root, no HTTP.
    ledger = SimpleNamespace(condition_proof=lambda key: "proof", offline_mode=False)
    transport = RustProductionTransport(ledger, "unused-worker", origin="https://modelscope.cn")
    obj = ProviderObject(
        origin="https://modelscope.cn",
        repo_id="owner/repo",
        repo_type="modelscope_dataset_legacy",
        revision="b" * 40,
        object_path="one.tar",
        object_size=10240,
        validator='"strong"',
        cdn_host="cdn.example.com",
    )
    for n in range(65):
        transport.register(replace(obj, repo_id=f"owner/repo{n}"))
    assert len(transport._objects) == 64
    assert all(value.repo_id != "owner/repo0" for value in transport._objects.values())
    transport.register(replace(obj, revision="c" * 40))
    transport.register(obj)
    assert obj in transport._objects.values()
    assert replace(obj, revision="c" * 40) in transport._objects.values()
    assert transport.verified_object(obj) == obj
    assert transport.verified_object(replace(obj, validator=None, cdn_host=None)) == obj
    for field, value in (
        ("origin", "https://other.example.com"),
        ("repo_id", "other/repo"),
        ("revision", "d" * 40),
        ("object_path", "other.tar"),
        ("object_size", 10241),
        ("validator", '"other"'),
    ):
        with pytest.raises((RemoteIOError, ValueError)):
            transport.verified_object(replace(obj, **{field: value}))
    with pytest.raises(ValueError):
        transport.verified_object(replace(obj, repo_type="unsupported"))
    ledger.condition_proof = lambda key: None
    with pytest.raises(RemoteIOError, match="proof unavailable"):
        transport.verified_object(obj)
    ledger.condition_proof = lambda key: "proof"
    transport.register(replace(obj, validator='"alternate"'))
    with pytest.raises(RemoteIOError, match="ambiguous"):
        transport.verified_object(replace(obj, validator=None))
    assert transport.verified_object(obj) == obj
    with pytest.raises(ValueError):
        transport.register(replace(obj, repo_type="unsupported"))
    credential = RustProductionTransport(
        ledger, "unused-worker", origin="https://modelscope.cn", token="test-only-token"
    )
    assert credential._objects is not transport._objects
    clone = transport.clone()
    assert not clone._objects and clone.ledger is transport.ledger
    with pytest.raises(RemoteIOError):
        clone.verified_object(obj)
    clone.close()
    with pytest.raises(RemoteIOError):
        clone.verified_object(obj)
    with pytest.raises(RemoteIOError):
        clone.clone()
    credential.close()
    assert len(transport._objects) == 64 and not credential._objects
    with pytest.raises(RemoteIOError):
        credential.register(obj)
    transport.close()
    assert not transport._objects


def test_runtime_reparse_attribute_fail_closed(inputs, monkeypatch):
    from sakurapool import fs_safety

    rt, *_ = inputs
    original = fs_safety.Path.lstat
    target = rt / "current.json"

    def lstat(path, *a, **kw):
        info = original(path, *a, **kw)
        if path == target:
            return SimpleNamespace(st_mode=info.st_mode, st_file_attributes=0x400)
        return info

    monkeypatch.setattr(fs_safety.Path, "lstat", lstat)
    with pytest.raises(SnapshotCorruptError):
        RuntimeSnapshot.open(rt)


def test_runtime_windows_junction_actual(inputs, tmp_path):
    import os
    import subprocess

    if os.name != "nt":
        pytest.skip("Windows junction integration")
    rt, *_ = inputs
    junction = tmp_path / "junction"
    result = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(junction), str(rt)], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    try:
        with pytest.raises(SnapshotCorruptError):
            RuntimeSnapshot.open(junction)
    finally:
        junction.rmdir()  # Owned junction only, never traverse target.


def test_owned_root_replacement_preserved(tmp_path):
    from sakurapool.fs_safety import cleanup_owned_tree, owned_tree_identity

    root = tmp_path / "stage"
    root.mkdir()
    identity = owned_tree_identity(root)
    displaced = tmp_path / "displaced"
    root.rename(displaced)
    root.mkdir()
    (root / "unknown").write_bytes(b"replacement")
    assert not cleanup_owned_tree(root, identity)
    assert (root / "unknown").read_bytes() == b"replacement"
    assert displaced.exists()


@pytest.mark.parametrize("failure", ["early_unknown", "root_replace", "load", "fsync", "map"])
def test_builder_creation_ownership_failures(inputs, monkeypatch, failure):
    from sakurapool import fs_safety
    from sakurapool.storage import publication as module

    rt, roots, mapping, out, _ = inputs
    captured = []
    original_create = fs_safety.OwnedStage.create

    def create(self, relative, **kw):
        if not captured:
            captured.append(self.root)
            if failure == "early_unknown":
                (self.root / "unknown").write_bytes(b"external")
            elif failure == "root_replace":
                self.root.rename(self.root.with_name(self.root.name + "-displaced"))
                self.root.mkdir()
                (self.root / "unknown").write_bytes(b"replacement")
        return original_create(self, relative, **kw)

    monkeypatch.setattr(fs_safety.OwnedStage, "create", create)
    if failure == "load":

        def load(*a, **kw):
            raise RuntimeError("load failure")

        monkeypatch.setattr(module, "load_publication", load)
    elif failure == "fsync":

        def fsync(*a):
            raise OSError("fsync failure")

        monkeypatch.setattr(module.os, "fsync", fsync)
    elif failure in ("map", "early_unknown"):
        mapping.write_text("invalid-json\n")
    with pytest.raises((ValueError, RuntimeError, OSError)):
        build_publication(rt, roots, mapping, out)
    assert not out.exists()
    if failure in ("early_unknown", "root_replace"):
        assert (captured[0] / "unknown").exists()
    else:
        assert not captured[0].exists()


def test_performance_harness_actual_small_phases(inputs):
    import subprocess
    import sys
    from pathlib import Path

    rt, roots, mapping, out, _ = inputs
    repo = Path(__file__).resolve().parents[1]
    tree = subprocess.check_output(["git", "-C", str(repo), "write-tree"], text=True).strip()
    result = subprocess.run(
        [
            sys.executable,
            str(repo / "reports/P5A/publication_perf.py"),
            "--implementation-tree",
            tree,
            "--runtime",
            str(rt),
            "--p2-roots",
            str(roots),
            "--remote-map",
            str(mapping),
            "--output",
            str(out),
            "--expected-rids",
            "1",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    data = json.loads(result.stdout)
    assert data["implementation_tree"] == tree
    phases = data["phase_wall"]
    assert set(phases) == {
        "preflight",
        "mapping",
        "hash_sidecar",
        "fetchability",
        "manifest",
        "build_full_verify",
        "standalone_full_verify",
    }
    assert all(seconds > 0 for seconds in phases.values())
    assert phases["standalone_full_verify"] == data["full_verify_wall"]
    assert (
        abs(sum(v for k, v in phases.items() if k != "standalone_full_verify") - data["build_wall"])
        < 1e-6
    )
    assert any(
        row["phase"] == "hash_sidecar" and "EXECUTEMANY INSERT INTO hashes" in row["sql"]
        for row in data["sql_execute"]
    )
