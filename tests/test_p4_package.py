"""Offline-only package inventory and record-to-binding authorization tests."""

import hashlib
import json
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pytest

from sakurapool.storage.budget import DEFAULT_WORK_ROOT, BudgetLedger
from sakurapool.storage.package import PackageCorrupt, fetch_from_package, load_package

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

    def location(self, rid):
        assert rid == 0
        return dict(object_idx=0, image_offset=512, image_size=4,
                    metadata_offset=1024, metadata_size=2)

    def object_ref(self, idx):
        assert idx == 0
        return dict(storage_id="remote1", object_id=OBJECT, object_path=PATH,
                    object_size=2048, object_version=SHA, validator=SHA,
                    backend="modelscope", repo_type="dataset", archive_format="tar")


@pytest.fixture
def package_root():
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
                          "image": {"offset": 512, "size": 4,
                                    "sha256": hashlib.sha256(b"jpeg").hexdigest()},
                          "json": {"offset": 1024, "size": 2,
                                   "sha256": hashlib.sha256(b"{}").hexdigest()},
                          "suffix": ".jpg"}}
        (root / "audit/members.json").write_text(json.dumps(audit))
        files = []
        for path in root.rglob("*"):
            if path.is_file():
                raw = path.read_bytes()
                files.append(dict(path=path.relative_to(root).as_posix(),
                                  bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()))
        manifest = dict(package_format_version=1, repo_id="leafmoone/game_cg_5M",
                        repo_type="dataset", endpoint="https://modelscope.cn",
                        data_revision="c" * 40, snapshot_id=SHA,
                        producer="sakurapool-p4-remote-stream", coverage="canary",
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
    class SimulatedBudget:
        # Fully mocked facade path; this is NOT an offline GuardedTransport.
        offline_mode = False
        root = ledger.root
        reserve = ledger.reserve
        settle = ledger.settle
    class FakeRemote:
        def __init__(self):
            self.ledger = SimulatedBudget()
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
