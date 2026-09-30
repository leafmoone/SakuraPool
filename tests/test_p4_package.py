"""Offline-only package inventory and record-to-binding authorization tests."""

import hashlib
import json
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pytest
from test_p4_transport import DATA, ETAG, TAR_MULTI
from test_p4_transport import http_and_budget as _synthetic_http

from sakurapool.registry import DatasetAdapter
from sakurapool.runtime.compiler import compile_runtime
from sakurapool.runtime.inventory import load_p2_inventory
from sakurapool.runtime.snapshot import RuntimeSnapshot
from sakurapool.storage import package as package_module
from sakurapool.storage.budget import DEFAULT_WORK_ROOT, BudgetExceeded, BudgetLedger
from sakurapool.storage.cli_ops import fetch as cli_fetch
from sakurapool.storage.package import (
    Binding,
    PackageCorrupt,
    fetch_from_package,
    load_package,
    publish_local_package,
)
from sakurapool.storage.remote_index import stage_tar, write_staged_v4
from sakurapool.storage.retrieval import (
    AuditedSample,
    Extent,
    fetch_bound_sample,
    fetch_bounded_samples,
)
from sakurapool.storage.transport import BoundObject, condition_binding_key

SHA = "a" * 64
RECORD = "b" * 32
PATH = "dir/example.tar"
OBJECT = f"{PATH}@sha256-{SHA}"


class FakeCatalog:
    def __init__(self, matches=True):
        self.matches = matches

    def execute(self, sql, params):
        assert "JOIN datasets" in sql
        assert params == (bytes.fromhex(RECORD),)
        return self

    def fetchone(self):
        return (0, "dataset1") if self.matches else None


@dataclass
class FakeSnapshot:
    snapshot_id: str = SHA
    _catalog: FakeCatalog = FakeCatalog()
    flags: int = 1
    metadata_size: int = 2

    def location(self, rid):
        assert rid == 0
        return dict(object_idx=0, image_offset=512, image_size=4,
                    metadata_offset=1024, metadata_size=self.metadata_size,
                    flags=self.flags)

    def object_ref(self, idx):
        assert idx == 0
        return dict(storage_id="remote1", object_id=OBJECT, object_path=PATH,
                    object_size=2048, object_version=SHA, validator=SHA,
                    backend="modelscope", repo_type="dataset", archive_format="tar")


@pytest.fixture
def package_root(monkeypatch):
    # Manifest/sample unit tests use a fake snapshot. The real durable/runtime
    # source-chain gate is covered separately by offline integration tests.
    monkeypatch.setattr(package_module, "_validate_package_chain", lambda *_args: None)
    with tempfile.TemporaryDirectory(prefix="offline-package-", dir=DEFAULT_WORK_ROOT) as temp:
        root = Path(temp) / "package"
        root.mkdir()
        (root / "runtime").mkdir()
        (root / "durable").mkdir()
        (root / "audit").mkdir()
        for name in ("runtime/SNAPSHOT.json", "runtime/catalog.sqlite",
                     "runtime/locations.npy", "durable/INPUT.json"):
            (root / name).write_bytes(b"small synthetic package entry")
        audit = {RECORD: {"identity": ["dataset1", "remote1", OBJECT],
                          "image_path": "nested/1.jpg", "json_path": "nested/1.json",
                          "image": {"offset": 512, "size": 4,
                                    "sha256": hashlib.sha256(b"jpeg").hexdigest()},
                          "json": {"offset": 1024, "size": 2,
                                   "sha256": hashlib.sha256(b"{}").hexdigest()},
                          "suffix": ".jpg"}}
        (root / "audit/members.json").write_text(json.dumps(audit))
        files = []
        (root / "audit/scan-config.json").write_text("{}", encoding="utf-8")
        for path in root.rglob("*"):
            if path.is_file():
                raw = path.read_bytes()
                files.append(dict(path=path.relative_to(root).as_posix(),
                                  bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()))
        manifest = dict(package_format_version=1, repo_id="leafmoone/game_cg_5M",
                        repo_type="dataset", endpoint="https://modelscope.cn",
                        producer_code_version="0.1.0", binding_mode="strong_etag_if_match",
                        mtime_provenance="unavailable; P2 mtime_ns=0",
                        durable="durable", scan_config="audit/scan-config.json",
                        scan_config_sha256="0" * 64,
                        scan_evidence="audit/members.json",
                        data_revision="c" * 40, snapshot_id=SHA,
                        producer="sakurapool-p4-remote-stream",
                        coverage={"mode": "canary", "object_count": 1,
                                  "full_repository": False},
                        publication_status="local_only", runtime="runtime",
                        audit="audit/members.json", files=files,
                        bindings=[dict(dataset_id="dataset1", storage_id="remote1",
                                       object_id=OBJECT, path=PATH, size=2048,
                                       content_sha256=SHA, strong_etag='"observed"',
                                       conditional_verified=True,
                                       condition_probe_sha256="f" * 64,
                                       legacy_local_binding_verified=False)])
        (root / "index-package.json").write_text(json.dumps(manifest))
        yield root, manifest


def test_package_inventory_and_exact_runtime_record_binding(package_root):
    root, manifest = package_root
    package = load_package(root)
    binding, sample = package.sample(FakeSnapshot(), RECORD)
    assert binding.path == PATH and sample.image.offset == 512
    assert sample.json_member.offset == 1024
    with pytest.raises(PackageCorrupt, match="snapshot"):
        package.sample(FakeSnapshot(snapshot_id="f"*64), RECORD)
    with pytest.raises(PackageCorrupt, match="record not present"):
        package.sample(FakeSnapshot(_catalog=FakeCatalog(False)), RECORD)
    assert not any("token" in json.dumps(item).lower() for item in manifest["files"])


def test_frozen_package_schema_rejects_heterogeneous_repository(package_root):
    root, manifest = package_root
    foreign = dict(manifest, repo_id="leafmoone/konachan_full")
    (root / "index-package.json").write_text(json.dumps(foreign))
    # This is a frozen P2 schema boundary, not migration support: foreign
    # repositories are rejected before provider/network/fetch code is reached.
    with pytest.raises(PackageCorrupt, match="unapproved repository"):
        load_package(root)
    malformed = dict(manifest, repo_id="a..b/invalid")
    (root / "index-package.json").write_text(json.dumps(malformed))
    with pytest.raises(PackageCorrupt, match="unapproved repository"):
        load_package(root)


@pytest.mark.parametrize("change", ["size", "hash", "missing", "escape", "duplicate",
                                     "binding", "snapshot", "symlink", "directory"])
def test_package_fails_closed_on_invalid_inventory_or_binding(package_root, change):
    root, manifest = package_root
    victim = root / "runtime/catalog.sqlite"
    if change == "size":
        manifest["files"][1]["bytes"] += 1
    elif change == "hash":
        victim.write_bytes(b"changed")
    elif change == "missing":
        victim.unlink()
    elif change == "escape":
        manifest["files"][0]["path"] = "../outside"
    elif change == "duplicate":
        manifest["files"].append(manifest["files"][0])
    elif change == "binding":
        manifest["bindings"][0]["content_sha256"] = "f" * 64
    elif change == "snapshot":
        manifest["snapshot_id"] = "evil"
    elif change == "symlink":
        try:
            (root / "test-symlink-probe").symlink_to(root / "runtime/SNAPSHOT.json")
        except OSError as exc:
            pytest.skip(f"host lacks symlink privilege: {type(exc).__name__}")
        (root / "test-symlink-probe").unlink()
        victim.unlink()
        victim.symlink_to(root / "runtime/SNAPSHOT.json")
    else:
        victim.unlink()
        victim.mkdir()
    (root / "index-package.json").write_text(json.dumps(manifest))
    with pytest.raises((PackageCorrupt, OSError)):
        load_package(root)


def test_fetch_facade_verifies_package_snapshot_audit_before_network(package_root,
                                                                       monkeypatch):
    root, manifest = package_root
    ledger = BudgetLedger(root.parent, _offline_test=True)
    output_root = root.parent / "delivered"
    output_root.mkdir()
    class FakeRemote:
        # Offline-only fake bytes with a real nested budget ledger.
        def __init__(self):
            self.ledger = ledger
            self.requests = []
            self.data = bytearray(2048)
            self.data[512:516] = b"jpeg"
            self.data[1024:1026] = b"{}"
        def _host(self, url):
            assert url == "https://modelscope.cn"
        def ensure_verified_condition(self, bound, **kwargs):
            self.requests.append(("proof", bound.size, bound.strong_etag))
            assert bound.url.endswith("FilePath=dir%2Fexample.tar")
            assert kwargs["expected_probe_sha256"] == "f"*64
        @contextmanager
        def read_range_owned(self, bound, offset, size):
            self.requests.append(("range", offset, size))
            yield bytes(self.data[offset:offset+size])
    class Snapshot(FakeSnapshot):
        def __enter__(self):
            return self
        def __exit__(self, *_args):
            pass
    def opened(path, *, full_verify):
        assert path == root / "runtime" and full_verify
        return Snapshot()
    monkeypatch.setattr("sakurapool.storage.package.RuntimeSnapshot.open", opened)
    client = FakeRemote()
    delivered = fetch_from_package(root, RECORD, output_root, client)
    assert (delivered / "image.jpg").read_bytes() == b"jpeg"
    assert (delivered / "metadata.json").read_bytes() == b"{}"
    assert client.requests == [("proof", 2048, '"observed"'), ("range", 512, 514)]
    assert ledger.status()["saved_samples"] == 1
    client.requests.clear()
    manifest["snapshot_id"] = "f" * 64
    (root / "index-package.json").write_text(json.dumps(manifest))
    with pytest.raises(PackageCorrupt, match="snapshot"):
        fetch_from_package(root, RECORD, output_root, client)
    assert client.requests == []


@pytest.mark.parametrize("present,flag,size", [
    (True, 1, 2), (False, 0, 0), (True, 1, 0),
    (True, 0, 0), (False, 1, 0),
])
def test_snapshot_metadata_presence_uses_flags_not_size(package_root, present, flag, size):
    root, manifest = package_root
    original = manifest["bindings"][0]
    package = load_package(root)
    member = package.audit[RECORD]
    if not present:
        member["json"] = member["json_path"] = None
    elif size == 0:
        member["json"]["size"] = 0
        member["json"]["sha256"] = hashlib.sha256(b"").hexdigest()
    snapshot = FakeSnapshot(flags=flag, metadata_size=size)
    if bool(flag) == present:
        binding, sample = package.sample(snapshot, RECORD)
        assert binding.object_id == original["object_id"]
        assert (sample.json_member is not None) is present
        if present:
            assert sample.json_member.size == size
    else:
        with pytest.raises(PackageCorrupt, match="presence"):
            package.sample(snapshot, RECORD)


@pytest.fixture
def real_package():
    """Real offline TAR -> staged P2 v4 -> P3 -> local package (no provider)."""
    yield from _real_package_fixture()


@pytest.fixture
def two_object_package():
    yield from _real_package_fixture(two_objects=True)


def _real_package_fixture(*, two_objects=False):
    fixture = _synthetic_http.__wrapped__()
    base, ledger, client = next(fixture)
    try:
        bound = BoundObject(base + "/full-multi", len(TAR_MULTI), strong_etag=ETAG)
        adapter = DatasetAdapter("demo", "synthetic", storage_id="modelscope:dataset")
        stage = stage_tar(client, ledger, bound, ledger.root / "stage", adapter,
                          max_records=2)
        frozen = [("dir/example.tar", bound, stage)]
        if two_objects:
            second = stage_tar(client, ledger, bound, ledger.root / "stage-second",
                               adapter, max_records=2)
            frozen.append(("dir/other.tar", bound, second))
        package_root = ledger.root / "package"
        package_root.mkdir()
        (package_root / "audit").mkdir()
        durable = package_root / "durable"
        audit_path = package_root / "audit" / "members.json"
        write_staged_v4(ledger, frozen, durable, adapter,
                        audit_output=audit_path)
        compile_runtime(load_p2_inventory(durable), package_root / "runtime")
        bindings = []
        with RuntimeSnapshot.open(package_root / "runtime", full_verify=True) as snapshot:
            for idx, (path, _, completed) in enumerate(frozen):
                obj = snapshot.object_ref(idx)
                assert obj["object_path"] == path
                binding = Binding(adapter.dataset, adapter.storage_id, obj["object_id"],
                                  path, len(TAR_MULTI), completed.content_sha256,
                                  ETAG, True, hashlib.sha256(TAR_MULTI[:1]).hexdigest(), False)
                bindings.append(binding)
                key = condition_binding_key(endpoint=base, repository="leafmoone/game_cg_5M",
                                            revision="a" * 40, path=binding.path,
                                            size=binding.size, strong_etag=binding.strong_etag)
                ledger.record_condition_proof(key, binding.condition_probe_sha256)
        publish_local_package(package_root, ledger, endpoint=base,
                              data_revision="a" * 40, bindings=bindings)
        yield package_root, ledger, client
    finally:
        try:
            next(fixture)
        except StopIteration:
            pass


@pytest.mark.parametrize("change", ["durable_all_removed", "durable_fragment_removed",
                                     "config_changed", "audit_member_changed",
                                     "binding_extra", "runtime_source_changed"])
def test_real_package_chain_rejects_disconnected_inputs(real_package, change):
    root, ledger, _client = real_package
    manifest_path = root / "index-package.json"
    manifest = json.loads(manifest_path.read_bytes())
    if change == "durable_all_removed":
        # Even a self-consistent rehashed manifest without any durable files
        # cannot pass simply by changing its editable inventory list.
        for file in (root / "durable").iterdir():
            file.unlink()
        manifest["files"] = [row for row in manifest["files"]
                             if not row["path"].startswith("durable/")]
    elif change == "durable_fragment_removed":
        row = next(item for item in manifest["files"]
                   if item["path"].endswith(".samples.parquet"))
        (root / row["path"]).unlink()
        manifest["files"].remove(row)
    elif change == "config_changed":
        config = root / manifest["scan_config"]
        data = json.loads(config.read_bytes())
        data["inputs"] = {}
        raw = json.dumps(data).encode()
        config.write_bytes(raw)
        for row in manifest["files"]:
            if row["path"] == manifest["scan_config"]:
                row.update(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
        manifest["scan_config_sha256"] = hashlib.sha256(raw).hexdigest()
    elif change == "audit_member_changed":
        audit = root / manifest["audit"]
        data = json.loads(audit.read_bytes())
        first = next(iter(data.values()))
        first["image_path"] = "different/path.jpg"
        raw = json.dumps(data).encode()
        audit.write_bytes(raw)
        for row in manifest["files"]:
            if row["path"] == manifest["audit"]:
                row.update(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
    elif change == "binding_extra":
        manifest["bindings"].append(dict(manifest["bindings"][0]))
        manifest["bindings"][1]["dataset_id"] = "other"
        manifest["coverage"]["object_count"] = 2
    elif change == "runtime_source_changed":
        snap = root / "runtime" / "snapshots"
        folder = next(snap.iterdir())
        snapshot_file = folder / "SNAPSHOT.json"
        document = json.loads(snapshot_file.read_bytes())
        document["source_fingerprint"] = "f" * 64
        raw = json.dumps(document).encode()
        snapshot_file.write_bytes(raw)
        for row in manifest["files"]:
            if row["path"] == snapshot_file.relative_to(root).as_posix():
                row.update(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises((PackageCorrupt, OSError)):
        load_package(root, allow_offline_loopback=True)
    assert ledger.status()["attempts"] == 1


def test_real_package_chain_verified_consistent(real_package):
    root, ledger, _client = real_package
    package = load_package(root, allow_offline_loopback=True)
    config = json.loads((root / "audit/scan-config.json").read_bytes())
    assert config["adapter"]["storage_id"] == "modelscope:dataset"
    assert package.durable == "durable"
    assert package.runtime == "runtime" and len(package.audit) == 2
    assert ledger.status()["attempts"] == 1


def _rehash_manifest_file(root, manifest, relative):
    raw = (root / relative).read_bytes()
    entry = next(item for item in manifest["files"] if item["path"] == relative)
    entry.update(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())


def test_audit_image_digest_mutation_rehashed_inventory_refused_before_network(real_package):
    root, ledger, client = real_package
    manifest_path = root / "index-package.json"
    manifest = json.loads(manifest_path.read_bytes())
    audit_path = root / manifest["audit"]
    audit = json.loads(audit_path.read_bytes())
    target = next(iter(audit))
    audit[target]["image"]["sha256"] = "f" * 64
    audit_path.write_text(json.dumps(audit), encoding="utf-8")
    _rehash_manifest_file(root, manifest, manifest["audit"])
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    baseline = ledger.status()["attempts"]
    with pytest.raises(PackageCorrupt, match="durable P2 sample"):
        load_package(root, allow_offline_loopback=True)
    with pytest.raises(PackageCorrupt):
        fetch_from_package(root, target, ledger.root, client)
    assert ledger.status()["attempts"] == baseline


def test_oversized_rehashed_durable_input_refused_before_p2_parser(real_package,
                                                                     monkeypatch):
    root, ledger, _client = real_package
    manifest_path = root / "index-package.json"
    manifest = json.loads(manifest_path.read_bytes())
    input_path = root / "durable/INPUT.json"
    input_path.write_bytes(b"x" * ((1 << 20) + 1))
    _rehash_manifest_file(root, manifest, "durable/INPUT.json")
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    def forbidden(*_args, **_kwargs):
        raise AssertionError("unbounded P2 parser must not see excessive INPUT")
    monkeypatch.setattr(package_module, "load_p2_inventory", forbidden)
    with pytest.raises(PackageCorrupt, match="bounded|exceeds"):
        load_package(root, allow_offline_loopback=True)
    assert ledger.status()["attempts"] == 1


def test_oversized_rehashed_commit_refused_before_p2_parser_by_load_and_publish(
        real_package, monkeypatch):
    root, ledger, _client = real_package
    manifest_path = root / "index-package.json"
    manifest = json.loads(manifest_path.read_bytes())
    marker = next(row["path"] for row in manifest["files"]
                  if row["path"].endswith(".COMMIT"))
    (root / marker).write_bytes(b"x" * ((1 << 20) + 1))
    _rehash_manifest_file(root, manifest, marker)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    def forbidden(*_args, **_kwargs):
        raise AssertionError("unbounded P2 parser must not see excessive COMMIT")

    monkeypatch.setattr(package_module, "load_p2_inventory", forbidden)
    baseline = ledger.status()["attempts"]
    with pytest.raises(PackageCorrupt, match="bounded|exceeds"):
        load_package(root, allow_offline_loopback=True)
    # Publishing has its own preflight, independent of load_package's
    # inventoried-file check. Rebuild is intentionally never attempted.
    manifest_path.unlink()
    bindings = [Binding(**row) for row in manifest["bindings"]]
    with pytest.raises(PackageCorrupt, match="durable COMMIT exceeds"):
        publish_local_package(root, ledger, endpoint=manifest["endpoint"],
                              data_revision=manifest["data_revision"],
                              bindings=bindings)
    assert ledger.status()["attempts"] == baseline


@pytest.mark.parametrize("target_idx", [1, 999999])
def test_runtime_location_cross_object_or_out_of_range_rehashed_refused(
        two_object_package, target_idx):
    root, ledger, _client = two_object_package
    manifest_path = root / "index-package.json"
    manifest = json.loads(manifest_path.read_bytes())
    snapshot_dir = next((root / "runtime/snapshots").iterdir())
    location = snapshot_dir / "locations.npy"
    array = np.load(location, mmap_mode="r+")
    assert array[0]["object_idx"] == 0
    array[0]["object_idx"] = target_idx
    array.flush()
    del array
    relative = location.relative_to(root).as_posix()
    snap_json = snapshot_dir / "SNAPSHOT.json"
    snapshot_doc = json.loads(snap_json.read_bytes())
    snapshot_doc["files"]["locations.npy"].update(
        bytes=location.stat().st_size, sha256=hashlib.sha256(location.read_bytes()).hexdigest())
    snap_json.write_text(json.dumps(snapshot_doc), encoding="utf-8")
    _rehash_manifest_file(root, manifest, relative)
    _rehash_manifest_file(root, manifest, snap_json.relative_to(root).as_posix())
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(PackageCorrupt, match="runtime record points|chain"):
        load_package(root, allow_offline_loopback=True)
    assert ledger.status()["attempts"] == 2


def test_production_fetch_four_entrypoints_block_before_network_or_output(
        monkeypatch, tmp_path):
    # Preserve the real BudgetLedger class and its production-mode property,
    # but NEVER initialize or inspect the real production ledger/work root.
    ledger = object.__new__(BudgetLedger)
    ledger._offline_mode = False
    ledger.root = tmp_path
    def forbidden(*_args, **_kwargs):
        raise AssertionError("production ledger/network/output must not be used")
    monkeypatch.setattr(ledger, "reserve", forbidden)
    monkeypatch.setattr(ledger, "status", forbidden)
    monkeypatch.setattr(ledger, "settle", forbidden)
    class NoNetwork:
        def __init__(self):
            self.ledger = ledger
        def clone(self):
            return forbidden()
        def ensure_verified_condition(self, *_args, **_kwargs):
            return forbidden()
        def read_range_owned(self, *_args, **_kwargs):
            return forbidden()
    client = NoNetwork()
    bound = BoundObject("https://modelscope.cn/never", len(DATA),
                        strong_etag='"verified-only-offline"')
    sample = AuditedSample("a" * 32,
                           Extent(0, 1, hashlib.sha256(DATA[:1]).hexdigest()),
                           None, ".jpg")
    for call in (
        lambda: fetch_bound_sample(client, ledger, bound, sample, tmp_path),
        lambda: fetch_bounded_samples(client, ledger, bound, [sample], tmp_path),
        lambda: fetch_from_package(tmp_path / "no-package", sample.record_id,
                                   tmp_path, client),
    ):
        with pytest.raises(BudgetExceeded, match="BLOCKED"):
            call()
    # R2 removes the perpetual CLI offline-only gate, but missing package still
    # refuses before config/token loading or implicit network/indexing.
    with pytest.raises(ValueError, match="package unavailable"):
        cli_fetch(tmp_path / "no-package", tmp_path / "no-config", sample.record_id, tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_present_zero_length_metadata_writes_empty_file_without_second_http():
    fixture = _synthetic_http.__wrapped__()
    base, ledger, client = next(fixture)
    try:
        image = DATA[:3]
        sample = AuditedSample("a" * 32,
            Extent(0, 3, hashlib.sha256(image).hexdigest()),
            Extent(7, 0, hashlib.sha256(b"").hexdigest()), ".jpg")
        bound = BoundObject(base + "/good", len(DATA), strong_etag=ETAG)
        delivered = fetch_bound_sample(client, ledger, bound, sample,
                                       ledger.root, merged=True)
        assert (delivered / "image.jpg").read_bytes() == image
        assert (delivered / "metadata.json").read_bytes() == b""
        assert ledger.status()["attempts"] == 1
    finally:
        try:
            next(fixture)
        except StopIteration:
            pass


def test_legacy_object_cannot_be_silently_rebound(package_root):
    root, manifest = package_root
    package = load_package(root)
    class LocalSnapshot(FakeSnapshot):
        def object_ref(self, idx):
            return {**super().object_ref(idx), "backend": "local", "repo_type": "local"}
    with pytest.raises(PackageCorrupt, match="legacy/local"):
        package.sample(LocalSnapshot(), RECORD)
    manifest["bindings"][0]["legacy_local_binding_verified"] = True
    (root / "index-package.json").write_text(json.dumps(manifest))
    binding, sample = load_package(root).sample(LocalSnapshot(), RECORD)
    assert binding.content_sha256 == SHA and sample.record_id == RECORD
