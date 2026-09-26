"""Additional boundaries missing from the initial P2 submission."""

import json

import pytest
from test_indexer import make_tar, read_rows

from sakurapool import indexer
from sakurapool.records import IdentityConflictError, RecordKey
from sakurapool.registry import AdapterRegistry, DatasetAdapter


@pytest.mark.parametrize("change", ["touch", "add", "remove", "config"])
def test_input_changes(tmp_path, change):
    source = tmp_path / "input"
    source.mkdir()
    path = source / "a.tar"
    make_tar(path, {"1.jpg": b"x", "1.json": b"{}"})
    out = tmp_path / "out"
    indexer.scan(source, out)
    if change == "touch":
        import os
        stat = path.stat()
        os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1000000))
    elif change == "add":
        make_tar(source / "b.tar", {})
    elif change == "remove":
        path.unlink()
    with pytest.raises(ValueError, match="input validator mismatch|no TAR"):
        indexer.scan(source, out, hash_images=change == "config")


def test_conflict_does_not_publish(tmp_path, monkeypatch):
    path = tmp_path / "a.tar"
    make_tar(path, {"1.jpg": b"x", "1.json": b"{}", "2.jpg": b"y", "2.json": b"{}"})
    monkeypatch.setattr(RecordKey, "record_id", property(lambda self: "collision"))
    with pytest.raises(IdentityConflictError, match="record_identity_conflict"):
        indexer.scan(path, tmp_path / "out")
    assert not list((tmp_path / "out").glob("*.COMMIT"))
    assert not list((tmp_path / "out").glob("*.parquet"))


@pytest.mark.parametrize("members,code", [
    ([("1.jpg", b"x"), ("1.json", b"{}"), ("1.json", b"{}")],
     "ambiguous_metadata_member"),
    ([("x.jpg", b"x"), ("x.json", b"{}")], "invalid_post_id"),
])
def test_explicit_error_codes(tmp_path, members, code):
    path = tmp_path / "a.tar"
    make_tar(path, members)
    registry = AdapterRegistry()
    registry.register(DatasetAdapter("d", "s", numeric_post_id=True))
    out = tmp_path / "out"
    assert indexer.scan(path, out, dataset="d", registry=registry)["errors"] == 1
    assert [r["code"] for r in read_rows(out, "errors")] == [code]


def test_full_paths_computed_hash(tmp_path):
    import hashlib
    path = tmp_path / "a.tar"
    members = {"a/1.jpg": b"x", "a/1.json": b"{}", "b/1.jpg": b"y", "b/1.json": b"{}"}
    make_tar(path, members)
    out = tmp_path / "out"
    assert indexer.scan(path, out, hash_images=True)["samples"] == 2
    for row in read_rows(out, "samples"):
        assert row["hash_source"] == "computed:sha256"
        assert row["sha256"] == hashlib.sha256(members[row["image_path"]]).hexdigest()
    commit = json.loads(next(out.glob("*.COMMIT")).read_bytes())
    assert commit["builder"] == indexer.BUILDER
    assert commit["schema"] == indexer.FORMAT_VERSION
    assert commit["created_at"]
    assert commit["files"]["objects"]["rows"] == 1
    assert commit["files"]["samples"]["rows"] == 2
