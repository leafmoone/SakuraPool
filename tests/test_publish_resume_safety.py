"""Fail-closed regression tests for prepublication verification."""
import hashlib
import json
import sqlite3
from pathlib import Path

import pytest
from synthetic_p2 import ObjectSpec, SampleSpec, build_p2_directory

from sakurapool.runtime import RuntimeSnapshot, compile_runtime, compiler, load_p2_inventory
from sakurapool.runtime.errors import SnapshotCorruptError


@pytest.fixture
def interrupted(tmp_path, monkeypatch):
    p2, root = tmp_path / "p2", tmp_path / "rt"
    build_p2_directory(p2, dataset="ds", source="src", objects=[
        ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("t", None)])])])
    inv = load_p2_inventory(p2)
    rename = compiler.os.rename

    def fail(src, dst):
        raise OSError("one-shot rename fault")

    monkeypatch.setattr(compiler.os, "rename", fail)
    with pytest.raises(OSError, match="one-shot"):
        compile_runtime(inv, root)
    monkeypatch.setattr(compiler.os, "rename", rename)
    return inv, root, next(root.glob(".staging-*"))


def contents(staging):
    return {p.name: p.read_bytes() for p in staging.iterdir() if p.is_file()}


@pytest.mark.parametrize("damage", ["ready", "hash", "schema", "path", "extra"])
@pytest.mark.parametrize("missing_ready", [False, True])
def test_invalid_staging_is_never_repaired(interrupted, damage, missing_ready):
    inv, root, staging = interrupted
    if missing_ready:
        (staging / "READY").unlink()
    if damage == "ready":
        (staging / "READY").write_text("wrong-id", encoding="utf-8")
    elif damage == "extra":
        (staging / "foreign.txt").write_bytes(b"preserve")
    elif damage == "hash":
        with (staging / "locations.npy").open("ab") as f:
            f.write(b"corrupt")
    else:
        manifest_path = staging / "SNAPSHOT.json"
        manifest = json.loads(manifest_path.read_text())
        if damage == "schema":
            p = staging / "catalog.sqlite"
            db = sqlite3.connect(p)
            try:
                db.execute("DROP TABLE tags")
                db.commit()
            finally:
                db.close()
            manifest["files"][p.name]["bytes"] = p.stat().st_size
            manifest["files"][p.name]["sha256"] = hashlib.sha256(p.read_bytes()).hexdigest()
        else:
            manifest["files"]["catalog.sqlite"]["path"] = "../outside.sqlite"
        manifest_path.write_text(json.dumps(manifest))
    before = contents(staging)
    with pytest.raises((SnapshotCorruptError, sqlite3.Error)):
        compile_runtime(inv, root)
    assert contents(staging) == before
    assert not (root / "current.json").exists()


def test_public_open_cannot_override_directory_identity(interrupted):
    _inv, _root, staging = interrupted
    with pytest.raises(SnapshotCorruptError):
        RuntimeSnapshot.open(staging)
    with pytest.raises(TypeError):
        RuntimeSnapshot.open(staging, snapshot_id=staging.name.removeprefix(".staging-"))


@pytest.mark.parametrize("name", ["READY", "OWNER.json", "SNAPSHOT.json", "catalog.sqlite"])
def test_entry_audit_precedes_any_external_read(interrupted, monkeypatch, name):
    inv, root, staging = interrupted
    # A directory under a known name is a portable nonregular-file attack.
    entry = staging / name
    entry.unlink()
    entry.mkdir()
    sentinel = entry / "user.txt"
    sentinel.write_bytes(b"untouched")
    original = Path.read_text

    def guard(path, *args, **kwargs):
        if path.parent == staging:
            pytest.fail("read happened before type audit")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", guard)
    with pytest.raises(SnapshotCorruptError):
        compile_runtime(inv, root)
    assert sentinel.read_bytes() == b"untouched"


def test_resume_full_verify_and_byte_identity(interrupted):
    inv, root, staging = interrupted
    before = contents(staging)
    result = compile_runtime(inv, root)
    final = root / "snapshots" / result.snapshot_id
    assert not staging.exists()
    assert contents(final) == before
    with RuntimeSnapshot.open(root, full_verify=True) as snapshot:
        assert snapshot.query(namespace="tags", all_tags=["t"]).count() == 1
