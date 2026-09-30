"""A-v2 category metadata does not change value-based tag identity."""

import hashlib
import json
import sqlite3

import pytest
from synthetic_p2 import ObjectSpec, SampleSpec, build_p2_directory
from test_indexer import make_tar
from test_nested_metadata import adapter, metadata

from sakurapool import indexer
from sakurapool.registry import AdapterRegistry
from sakurapool.runtime import RUNTIME_COMPILER, RUNTIME_FORMAT_VERSION
from sakurapool.runtime.compiler import _catalog, _stage1, _stage2, compile_runtime
from sakurapool.runtime.errors import (
    CorruptInputError,
    SnapshotClosedError,
    SnapshotCorruptError,
    UnknownQueryValueError,
)
from sakurapool.runtime.identity import snapshot_id
from sakurapool.runtime.inventory import load_p2_inventory
from sakurapool.runtime.snapshot import RuntimeSnapshot


def file_hashes(root):
    return {p.relative_to(root): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob("*") if p.is_file()}


def test_real_nested_sano_and_tiger(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    members = {}
    for post_id in range(42, 48):
        value = metadata()
        value["id"] = post_id
        value["tags"] = {"general": [], "artist": [], "character": [], "copyright": []}
        if post_id == 42:
            value["tags"]["general"] = ["sano"]
            value["tags"]["artist"] = ["sano"]
        else:
            value["tags"]["character" if post_id < 46 else "general"] = ["tiger"]
        members[f"{post_id}.jpg"] = b"image"
        members[f"{post_id}.json"] = json.dumps(value).encode()
    make_tar(source / "a.tar", members)
    registry = AdapterRegistry()
    registry.register(adapter())
    p2 = tmp_path / "p2"
    indexer.scan(source, p2, dataset="danbooru_v3", registry=registry)
    before = file_hashes(p2)
    assert json.loads((p2 / "INPUT.json").read_text())["format_version"] == 4
    inv = load_p2_inventory(p2)
    stage = tmp_path / "stage"
    stage.mkdir()
    _stage1(inv, stage)
    _stage2(inv, stage, 1)
    counts = json.loads((stage / "STAGE2-COUNTS.json").read_text())
    assert counts["tag_occurrences"] == 7
    assert "memberships" not in counts
    pairs = json.loads((stage / "CATEGORIES.json").read_text())
    assert pairs == sorted(pairs)
    assert len(pairs) == 4
    summary = compile_runtime(inv, tmp_path / "runtime", chunk_size=1)
    assert (summary.tag_count, summary.tag_memberships) == (2, 6)
    with RuntimeSnapshot.open(summary.path, full_verify=True) as rt:
        assert rt.tag_categories("tags", "sano") == ("artist", "general")
        assert rt.tag_categories("tags", "tiger") == ("character", "general")
        assert rt.query(namespace="tags", all_tags=["sano"]).count() == 1
        assert rt.query(namespace="tags", all_tags=["tiger"]).count() == 5
        assert rt.manifest["tag_memberships"] == 6
        assert rt.manifest["runtime_format_version"] == 2
        assert rt.manifest["compiler"] == "sakurapool-p3-v2"
    assert file_hashes(p2) == before


@pytest.mark.parametrize("tags,expected,occurrences,memberships", [
    ([[('t', None)]], (), 1, 1),
    ([[('t', 'general')]], ('general',), 1, 1),
    ([[('t', 'general'), ('t', 'artist')]], ('artist', 'general'), 2, 1),
    ([[('t', 'artist'), ('t', 'artist')]], ('artist',), 2, 1),
    ([[('t', None)], [('t', 'artist')]], ('artist',), 2, 2),
    ([[('t', 'general')], [('t', 'artist')]], ('artist', 'general'), 2, 2),
])
def test_category_union_and_occurrence_counts(tmp_path, tags, expected, occurrences, memberships):
    idx = tmp_path / "p2"
    build_p2_directory(idx, dataset="ds", source="src", objects=[ObjectSpec("a.tar", [
        SampleSpec(f"{i}.jpg", str(i), row) for i, row in enumerate(tags)])])
    inv = load_p2_inventory(idx)
    stage = tmp_path / "stage"
    stage.mkdir()
    _stage1(inv, stage)
    _stage2(inv, stage, 1)
    assert json.loads((stage / "STAGE2-COUNTS.json").read_text())["tag_occurrences"] == occurrences
    summary = compile_runtime(inv, tmp_path / "rt", chunk_size=1)
    assert (summary.tag_count, summary.tag_memberships) == (1, memberships)
    with RuntimeSnapshot.open(summary.path) as rt:
        assert rt.tag_categories("tags", "t") == expected
        assert rt.query(namespace="tags", all_tags=["t"]).count() == memberships
        for ns, value in (("tags", "absent"), ("absent", "t")):
            with pytest.raises(UnknownQueryValueError):
                rt.tag_categories(ns, value)
    with pytest.raises(SnapshotClosedError, match="closed"):
        rt.tag_categories("tags", "t")


def test_namespaces_remain_separate(tmp_path):
    idx = tmp_path / "p2"
    build_p2_directory(idx, dataset="ds", source="src", objects=[
        ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("t", "artist")])], namespace="a"),
        ObjectSpec("b.tar", [SampleSpec("2.jpg", "2", [("t", "general")])], namespace="b"),
    ])
    summary = compile_runtime(load_p2_inventory(idx), tmp_path / "rt")
    assert (summary.tag_count, summary.tag_memberships) == (2, 2)
    with RuntimeSnapshot.open(summary.path) as rt:
        assert rt.tag_categories("a", "t") == ("artist",)
        assert rt.tag_categories("b", "t") == ("general",)


def test_category_references_validated_without_fk_enforcement(tmp_path):
    idx = tmp_path / "p2"
    build_p2_directory(idx, dataset="ds", source="src", objects=[
        ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("t", "general")])])])
    inv = load_p2_inventory(idx)
    stage = tmp_path / "stage"
    stage.mkdir()
    _stage1(inv, stage)
    _stage2(inv, stage, 1)
    (stage / "CATEGORIES.json").write_text(json.dumps([[999, "general"]]))
    with pytest.raises(CorruptInputError, match="unknown tag_id"):
        _catalog(inv, stage, "a" * 64)


def test_v2_identity_schema_and_v1_rejection(tmp_path):
    idx = tmp_path / "p2"
    build_p2_directory(idx, dataset="ds", source="src", objects=[
        ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("t", "general")])])])
    inv = load_p2_inventory(idx)
    summary = compile_runtime(inv, tmp_path / "rt")
    assert (RUNTIME_FORMAT_VERSION, RUNTIME_COMPILER) == (2, "sakurapool-p3-v2")
    assert summary.snapshot_id == snapshot_id(inv.source_fingerprint, {"chunk_size": 500_000},
                                               RUNTIME_COMPILER, RUNTIME_FORMAT_VERSION)
    assert summary.snapshot_id != snapshot_id(inv.source_fingerprint, {"chunk_size": 500_000},
                                               "sakurapool-p3-v1", 1)
    con = sqlite3.connect(summary.path / "catalog.sqlite")
    try:
        assert "category" not in [r[1] for r in con.execute("PRAGMA table_info(tags)")]
        assert [r[1] for r in con.execute("PRAGMA table_info(tag_categories)")] == [
            "tag_id", "category"]
        assert con.execute("PRAGMA foreign_key_list(tag_categories)").fetchone()[2] == "tags"
        plan = con.execute("EXPLAIN QUERY PLAN SELECT tag_id FROM tag_categories "
                           "WHERE category = ?", ("general",)).fetchall()
        assert "tag_categories_category_tag" in str(plan)
    finally:
        con.close()
    manifest_path = summary.path / "SNAPSHOT.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["runtime_format_version"] = 1
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(SnapshotCorruptError, match="unsupported runtime_format_version"):
        RuntimeSnapshot.open(summary.path)


@pytest.mark.parametrize("version", [1, 2])
def test_p2_historical_runtime_provenance_stays_readable(version):
    from sakurapool.local_builder import validate_build_metadata

    value = dict(sakurapool_code_sha="a" * 40, rust_worker_version="worker-v1",
                 rust_worker_sha256="b" * 64, runtime_format_version=version,
                 durable_format_version=4, adapter_contract_version="nested_json_v1")
    validate_build_metadata(value)
    with pytest.raises(ValueError, match="unsupported build format provenance"):
        validate_build_metadata(dict(value, runtime_format_version=3))


def test_old_partition_p2_compiles_under_v2(tmp_path, monkeypatch):
    from test_local_partition_builder import FakeWorker, fixture

    import sakurapool.local_builder as builder

    manifest, adapter, _ = fixture(tmp_path)
    worker = tmp_path / "worker.bin"
    worker.write_bytes(b"fixture worker identity")
    p2 = tmp_path / "p2"
    with monkeypatch.context() as old:
        old.setattr(builder, "RUNTIME_FORMAT_VERSION", 1)
        old.setattr(builder, "RustWorker", FakeWorker)
        builder.build_partition(manifest, adapter, worker, tmp_path / "work", p2,
                                code_sha="a" * 40)
    before = file_hashes(p2)
    assert json.loads((p2 / "INPUT.json").read_text())["build_metadata"][
        "runtime_format_version"] == 1
    summary = compile_runtime(load_p2_inventory(p2), tmp_path / "runtime")
    with RuntimeSnapshot.open(summary.path) as rt:
        assert rt.manifest["runtime_format_version"] == 2
        assert rt.query(namespace="tags", all_tags=["blue"]).count() == 1
    assert file_hashes(p2) == before
