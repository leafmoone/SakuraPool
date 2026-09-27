import json
import os
from pathlib import Path

import pytest
from test_indexer import make_tar

from sakurapool import cli, indexer
from sakurapool.runtime.errors import (
    AmbiguousRecordError,
    CorruptInputError,
    SnapshotClosedError,
    SnapshotCorruptError,
    UnknownQueryValueError,
)
from sakurapool.runtime.identity import assign_rids, check_rid_capacity
from sakurapool.runtime.inventory import (
    combine_inventories,
    load_p2_inventory,
)
from sakurapool.runtime.query import RuntimeQuerySpec
from sakurapool.runtime.snapshot import RuntimeSnapshot

SRC = Path(__file__).resolve().parents[1] / "src"


def build_p2_index(tmp_path, name, tars, dataset="local"):
    source = tmp_path / f"src-{name}"
    source.mkdir(exist_ok=True)
    for tar_name, members in tars.items():
        make_tar(source / tar_name, members)
    output = tmp_path / f"idx-{name}"
    if not (output / "INPUT.json").exists():
        registry = None
        if dataset != "local":
            from sakurapool.registry import AdapterRegistry, DatasetAdapter
            registry = AdapterRegistry()
            registry.register(DatasetAdapter(
                dataset=dataset, source=f"src-{dataset}"))
        indexer.scan(source, output, dataset=dataset, registry=registry)
    return output


def meta(tags):
    return json.dumps({"tags": tags}).encode()


def compile_small(tmp_path, name="a", tars=None):
    if tars is None:
        tars = {"a.tar": {
            "1.jpg": b"img1",
            "1.json": meta(["1girl", "solo"]),
            "2.jpg": b"img2",
            "2.json": meta(["1girl"]),
            "3.jpg": b"img3",
            "3.json": b"{}",
        }}
    index = build_p2_index(tmp_path, name, tars)
    runtime_root = tmp_path / f"runtime-{name}"
    from sakurapool.runtime.compiler import compile_runtime
    summary = compile_runtime(load_p2_inventory(index), runtime_root)
    return summary, runtime_root


def test_compile_snapshot_and_query(tmp_path):
    summary, root = compile_small(tmp_path)
    assert summary.rid_count == 3
    assert summary.tag_count == 2
    assert summary.tag_memberships == 3
    rt = RuntimeSnapshot.open(root)
    assert rt.snapshot_id == summary.snapshot_id
    assert sorted(rt.query(
        RuntimeQuerySpec(all_tags=[("tags", "1girl")])).limit(10)) == [0, 1]
    assert rt.query(
        RuntimeQuerySpec(namespace="tags", all_tags=["solo"])).limit(10) == [0]
    # none excludes only within the known namespace: rid 2 has no tags at all.
    assert rt.query(
        RuntimeQuerySpec(namespace="tags", none_tags=["1girl"])).limit(10) == []
    union = rt.query(RuntimeQuerySpec(any_of=[
        RuntimeQuerySpec(namespace="tags", all_tags=["solo"]),
        RuntimeQuerySpec(sources=("local",)),
    ]))
    assert union.count() == 3
    # no terms = universe
    assert rt.query(RuntimeQuerySpec()).count() == 3
    record = rt.resolve_one("local", "2")
    assert record.rid == 1
    location = rt.location(record.rid)
    assert location["image_size"] == 4
    assert location["metadata_size"] > 0
    batches = [b.rid.tolist() for b in union.iter_location_batches(2)]
    assert batches == [[0, 1], [2]]
    records = [b.post_id for b in union.iter_record_batches(2)]
    assert records == [["1", "2"], ["3"]]
    for rid in union.iter_rids():
        assert 0 <= rid < 3
    assert rt.cache.misses >= 2 and rt.cache.resident_bytes() > 0
    rt.close()
    with pytest.raises(SnapshotClosedError):
        rt.query(RuntimeQuerySpec())


def test_query_unknown_values_raise(tmp_path):
    _, root = compile_small(tmp_path)
    with RuntimeSnapshot.open(root) as rt:
        with pytest.raises(UnknownQueryValueError):
            rt.query(RuntimeQuerySpec(namespace="tags", all_tags=["nope"]))
        with pytest.raises(UnknownQueryValueError):
            rt.query(RuntimeQuerySpec(all_tags=[("missing", "1girl")]))
        with pytest.raises(UnknownQueryValueError):
            rt.query(RuntimeQuerySpec(sources=("missing",)))
        with pytest.raises(UnknownQueryValueError):
            rt.query(RuntimeQuerySpec(datasets=("missing",)))
        with pytest.raises(UnknownQueryValueError):
            rt.resolve_one("local", "missing")
        with pytest.raises(UnknownQueryValueError):
            rt.query(RuntimeQuerySpec(all_tags=["1girl"]))


def test_ambiguous_record_raises(tmp_path):
    tars = {
        "a.tar": {"2.jpg": b"a", "2.json": meta(["t"])},
        "b.tar": {"2.jpg": b"b", "2.json": meta(["t"])},
    }
    compile_small(tmp_path, tars=tars)
    root = tmp_path / "runtime-a"
    with RuntimeSnapshot.open(root) as rt:
        with pytest.raises(AmbiguousRecordError):
            rt.resolve_one("local", "2")


def test_snapshot_verify_and_corruption(tmp_path):
    summary, root = compile_small(tmp_path)
    RuntimeSnapshot.open(root, full_verify=True)
    catalog = root / "snapshots" / summary.snapshot_id / "catalog.sqlite"
    data = bytearray(catalog.read_bytes())
    data[-1] ^= 1
    catalog.write_bytes(bytes(data))
    with pytest.raises(SnapshotCorruptError, match="sha256"):
        RuntimeSnapshot.open(root, full_verify=True)
    summary2 = summary
    catalog = root / "snapshots" / summary2.snapshot_id / "catalog.sqlite"
    catalog.write_bytes(catalog.read_bytes()[:-1])
    with pytest.raises(SnapshotCorruptError, match="size"):
        RuntimeSnapshot.open(root)
    ready = root / "snapshots" / summary.snapshot_id / "READY"
    ready.unlink()
    with pytest.raises(SnapshotCorruptError, match="READY"):
        RuntimeSnapshot.open(root)


def test_compile_crash_resume(tmp_path, monkeypatch):
    import sakurapool.runtime.compiler as compiler

    def flaky_bitmaps(staging):
        flaky_bitmaps.calls += 1
        if flaky_bitmaps.calls == 1:
            raise OSError("injected crash at bitmaps")
        return compiler._real_bitmaps(staging)

    flaky_bitmaps.calls = 0
    compiler._real_bitmaps = compiler._bitmaps
    monkeypatch.setattr(compiler, "_bitmaps", flaky_bitmaps)
    with pytest.raises(OSError, match="injected"):
        compile_small(tmp_path)
    monkeypatch.setattr(compiler, "_bitmaps", compiler._real_bitmaps)
    summary, root = compile_small(tmp_path)
    with RuntimeSnapshot.open(root) as rt:
        assert rt.rid_count == 3


def test_compile_is_idempotent(tmp_path):
    summary, root = compile_small(tmp_path)
    import sakurapool.runtime.compiler as compiler
    again = compiler.compile_runtime(load_p2_inventory(
        tmp_path / "idx-a"), root)
    assert again.snapshot_id == summary.snapshot_id


def test_two_input_order_independent(tmp_path):
    import sakurapool.runtime.compiler as compiler

    tars_a = {"a.tar": {"1.jpg": b"a", "1.json": meta(["shared"])}}
    tars_b = {"b.tar": {"2.jpg": b"b", "2.json": meta(["shared"])}}
    index_a = build_p2_index(tmp_path, "ord-a", tars_a, dataset="ds_a")
    index_b = build_p2_index(tmp_path, "ord-b", tars_b, dataset="ds_b")
    inv_a = load_p2_inventory(index_a)
    inv_b = load_p2_inventory(index_b)
    id_a = compiler.compile_runtime(
        combine_inventories([inv_a, inv_b]), tmp_path / "ord-ab").snapshot_id
    id_b = compiler.compile_runtime(
        combine_inventories([inv_b, inv_a]), tmp_path / "ord-ba").snapshot_id
    assert id_a == id_b


def test_rebuilt_inputs_same_logical_results(tmp_path):
    import sakurapool.runtime.compiler as compiler

    tars_a = {"a.tar": {"1.jpg": b"a", "1.json": meta(["shared", "x"])}}
    tars_b = {"b.tar": {"2.jpg": b"b", "2.json": meta(["shared"])}}

    def logical(tag):
        index_a = build_p2_index(tmp_path, f"rb-a-{tag}", tars_a, dataset="ds_a")
        index_b = build_p2_index(tmp_path, f"rb-b-{tag}", tars_b, dataset="ds_b")
        combined = combine_inventories([
            load_p2_inventory(index_a), load_p2_inventory(index_b)])
        root = tmp_path / f"rb-{tag}"
        summary = compiler.compile_runtime(combined, root)
        with RuntimeSnapshot.open(root) as rt:
            shared_rids = rt.query(
                RuntimeQuerySpec(namespace="tags",
                                 all_tags=["shared"])).limit(10)
            order = rt._catalog.execute(  # noqa: SLF001 - test boundary
                "SELECT d.name, r.post_id FROM records r"
                " JOIN datasets d ON d.dataset_id = r.dataset_id"
                " ORDER BY r.rid").fetchall()
            tag_ids = rt._catalog.execute(  # noqa: SLF001 - test boundary
                "SELECT value, tag_id FROM tags WHERE value IN ('shared','x')"
                " ORDER BY value").fetchall()
        return summary.rid_count, shared_rids, order, tag_ids

    (c1, s1, o1, t1) = logical("one")
    (c2, s2, o2, t2) = logical("two")
    assert (c1, s1, o1, t1) == (c2, s2, o2, t2)
    assert c1 == 2
    assert s1 == [0, 1]


def test_duplicate_dataset_names_rejected(tmp_path):
    index_a = build_p2_index(tmp_path, "da", {"a.tar": {"1.jpg": b"a", "1.json": meta(["t"])}})
    index_b = build_p2_index(tmp_path, "db", {"b.tar": {"2.jpg": b"b", "2.json": meta(["t"])}})
    with pytest.raises(CorruptInputError, match="duplicate dataset"):
        combine_inventories([load_p2_inventory(index_a),
                             load_p2_inventory(index_b)])


def test_cli_runtime(tmp_path, capsys):
    summary, root = compile_small(tmp_path)
    assert cli.main([
        "runtime", "inspect", str(root)]) == 0
    assert cli.main([
        "runtime", "verify", str(root), "--full-verify"]) == 0
    assert cli.main([
        "runtime", "lookup", str(root),
        "--source", "local", "--post-id", "1"]) == 0
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["rid"] == 0 and out["image_size"] == 4
    assert cli.main([
        "runtime", "query", str(root),
        "--namespace", "tags", "--all-tag", "solo"]) == 0
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["rids"] == [0] and out["count"] == 1
    assert cli.main([
        "runtime", "query", str(root),
        "--namespace", "tags", "--all-tag", "missing"]) == 2
    with pytest.raises(SystemExit):
        cli.main(["runtime", "compile"])


def test_committed_p2_inventory_and_fingerprint(tmp_path):
    source = tmp_path / "input"
    source.mkdir()
    make_tar(source / "a.tar", {"1.jpg": b"x", "1.json": b"{}"})
    output = tmp_path / "index"
    indexer.scan(source, output)
    inventory = load_p2_inventory(output)
    assert len(inventory.objects) == 1
    assert {fragment.name for fragment in inventory.fragments} == {
        "objects", "samples", "annotations", "errors"
    }
    relocated = tmp_path / "relocated"
    relocated.mkdir()
    for path in output.iterdir():
        path.rename(relocated / path.name)
    assert load_p2_inventory(relocated).source_fingerprint == inventory.source_fingerprint


def test_commit_allow_set_is_derived_not_globbered(tmp_path):
    source = tmp_path / "input"
    source.mkdir()
    make_tar(source / "a.tar", {"1.jpg": b"x", "1.json": b"{}"})
    output = tmp_path / "index"
    indexer.scan(source, output)
    load_p2_inventory(output)
    (output / ("deadbeef" + "0" * 56 + ".COMMIT")).write_text(json.dumps(
        {"schema": 4, "builder": "sakurapool-p2-v4", "dataset_id": "local",
         "object_id": "x", "input": {}, "files": {}, "contract_sha256": "0" * 64,
         "created_at": "now"}))
    with pytest.raises(CorruptInputError, match="COMMIT marker set"):
        load_p2_inventory(output)


def test_cli_compile_positional_syntax(tmp_path, capsys):
    index = build_p2_index(tmp_path, "cli", {"a.tar": {
        "1.jpg": b"a", "1.json": meta(["t"])}})
    root = tmp_path / "cli-runtime"
    assert cli.main(["runtime", "compile", str(index), str(root)]) == 0
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["rid_count"] == 1
    assert (root / "current.json").exists()


def _synthetic_tag_profile(runtime_root, snapshot_id):
    """(dataset, post_id) per rid + tag value→rids + tag_id map."""
    with RuntimeSnapshot.open(runtime_root) as rt:
        order = rt._catalog.execute(  # noqa: SLF001 - test boundary
            "SELECT d.name, r.post_id FROM records r"
            " JOIN datasets d ON d.dataset_id = r.dataset_id"
            " ORDER BY r.rid").fetchall()
        tag_rows = rt._catalog.execute(  # noqa: SLF001 - test boundary
            "SELECT value, tag_id FROM tags ORDER BY value").fetchall()
        memberships = {}
        for value, _ in tag_rows:
            rids = rt.query(
                RuntimeQuerySpec(all_tags=[("tags", value)])
            ).limit(1 << 30)
            memberships[value] = rids
        return order, tag_rows, memberships


def test_dual_compile_determinism(tmp_path):
    """Reversed input dir order and shuffled annotation rows must give the
    same tag_id mapping and per-rid results."""
    import random

    from synthetic_p2 import ObjectSpec, SampleSpec, build_p2_directory

    import sakurapool.runtime.compiler as compiler

    rng = random.Random(7)
    pool = ["1girl", "solo", "blue", "night", "rain"]

    def make_samples(n, offset):
        return [SampleSpec(f"{i}.jpg", str(offset + i),
                           [(pool[rng.randrange(len(pool))], None)
                            for i in range(rng.randint(0, 2))])
                for i in range(n)]

    specs_a = {"o1": make_samples(6, 0), "o2": make_samples(4, 100)}
    specs_b = {"o3": make_samples(5, 200)}
    profiles = {}
    variants = (  # (suffix, shuffle_a, shuffle_b, reverse_dir_order)
        ("base", False, False, False),
        ("shuf", True, True, False),
        ("revd", True, False, True),
    )
    for suffix, shuffle_a, shuffle_b, reverse in variants:
        inventories = []
        for dataset, specs, source, shuffle in (
                ("ds_a", specs_a, "src_a", shuffle_a),
                ("ds_b", specs_b, "src_b", shuffle_b)):
            shuffled = {rel: (list(reversed(samples)) if shuffle else samples)
                        for rel, samples in specs.items()}
            build_p2_directory(
                tmp_path / f"p2-{dataset}-{suffix}", dataset=dataset,
                source=source,
                objects=[ObjectSpec(rel, samples, namespace="tags")
                         for rel, samples in shuffled.items()],
                created_at="2025-01-01T00:00:00+00:00")
            inventories.append(load_p2_inventory(
                tmp_path / f"p2-{dataset}-{suffix}"))
        if reverse:
            inventories.reverse()
        combined = combine_inventories(inventories)
        root = tmp_path / f"rt-{suffix}"
        compiler.compile_runtime(combined, root)
        profiles[suffix] = _synthetic_tag_profile(root, None)
    first = next(iter(profiles.values()))
    for value in profiles.values():
        assert value == first


def test_tag_category_resolution_and_conflict(tmp_path):
    from synthetic_p2 import ObjectSpec, SampleSpec, build_p2_directory

    import sakurapool.runtime.compiler as compiler

    def build(name, samples):
        idx = tmp_path / name
        build_p2_directory(
            idx, dataset="ds", source="src",
            objects=[ObjectSpec("a.tar", samples)],
            created_at="2025-01-01T00:00:00+00:00")
        return compiler.compile_runtime(load_p2_inventory(idx),
                                        tmp_path / f"rt-{name}")

    # all null -> NULL
    build("cat-null", [SampleSpec("1.jpg", "1", [("t", None)]),
                       SampleSpec("2.jpg", "2", [("t", None)])])
    # one non-null wins over nulls
    build("cat-one", [SampleSpec("1.jpg", "1", [("t", None)]),
                      SampleSpec("2.jpg", "2", [("t", "male")])])
    # different non-null values -> compile failure
    with pytest.raises(CorruptInputError, match="TAG_CATEGORY_CONFLICT"):
        build("cat-conflict", [SampleSpec("1.jpg", "1", [("t", "male")]),
                               SampleSpec("2.jpg", "2", [("t", "female")])])
    import sqlite3
    catalog = tmp_path / "rt-cat-one" / "snapshots"
    catalog = next(catalog.iterdir()) / "catalog.sqlite"
    con = sqlite3.connect(str(catalog))
    assert con.execute("SELECT category FROM tags").fetchone()[0] == "male"
    con.close()


def test_multi_origin_union_and_namespace_split(tmp_path):
    """§18/§44: same tag across origins unions; namespace is not source."""
    from synthetic_p2 import ObjectSpec, SampleSpec, build_p2_directory

    import sakurapool.runtime.compiler as compiler

    idx_a = tmp_path / "p2-a"
    idx_b = tmp_path / "p2-b"
    build_p2_directory(idx_a, dataset="ds_a", source="sourceA", objects=[
        ObjectSpec("a.tar", [
            SampleSpec("1.jpg", "1", [("1girl", None)], "known",
        ), SampleSpec("2.jpg", "2", [("solo", None)], "known")],
        namespace="danbooru", origin="originA"),
    ], created_at="2025-01-01T00:00:00+00:00")
    build_p2_directory(idx_b, dataset="ds_b", source="sourceB", objects=[
        ObjectSpec("b.tar", [
            SampleSpec("1.jpg", "3", [("1girl", None)], "known",
        ), SampleSpec("2.jpg", "4", [("blue", None)], "known")],
        namespace="danbooru", origin="originB"),
    ], created_at="2025-01-01T00:00:00+00:00")
    combined = combine_inventories([load_p2_inventory(idx_a),
                                    load_p2_inventory(idx_b)])
    root = tmp_path / "rt-origins"
    compiler.compile_runtime(combined, root)
    with RuntimeSnapshot.open(root) as rt:
        # §44: one namespace, two sources/origins both have 1girl
        both = sorted(rt.query(RuntimeQuerySpec(
            all_tags=[("danbooru", "1girl")])).limit(10))
        assert both == [0, 2]
        # OR both sources AND 1girl: still both match
        mixed = sorted(rt.query(RuntimeQuerySpec(
            sources=("sourceA", "sourceB"),
            all_tags=[("danbooru", "1girl")])).limit(10))
        assert mixed == [0, 2]
        # source-only term narrows to sourceA's object
        only_a = sorted(rt.query(RuntimeQuerySpec(
            sources=("sourceA",),
            all_tags=[("danbooru", "1girl")])).limit(10))
        assert only_a == [0]
        # namespace isolation: solo/blue are danbooru-only values
        with pytest.raises(UnknownQueryValueError):
            rt.query(RuntimeQuerySpec(all_tags=[("tags", "1girl")]))
        blue = sorted(rt.query(RuntimeQuerySpec(
            all_tags=[("danbooru", "blue")])).limit(10))
        assert blue == [3]


def test_multi_dataset_same_source_post(tmp_path):
    """§45: two datasets, same source+post -> two rids; +dataset -> one."""
    from synthetic_p2 import ObjectSpec, SampleSpec, build_p2_directory

    idx_old = tmp_path / "p2-old"
    idx_new = tmp_path / "p2-new"
    build_p2_directory(idx_old, dataset="dataset_old", source="danbooru",
                       objects=[ObjectSpec("a.tar", [
                           SampleSpec("1.jpg", "123", [("t", None)])])],
                       created_at="2025-01-01T00:00:00+00:00")
    build_p2_directory(idx_new, dataset="dataset_new", source="danbooru",
                       objects=[ObjectSpec("b.tar", [
                           SampleSpec("1.jpg", "123", [("t", None)])])],
                       created_at="2025-01-01T00:00:00+00:00")
    combined = combine_inventories([load_p2_inventory(idx_old),
                                    load_p2_inventory(idx_new)])
    from sakurapool.runtime.compiler import compile_runtime
    compile_runtime(combined, tmp_path / "rt-multi-ds")
    with RuntimeSnapshot.open(tmp_path / "rt-multi-ds") as rt:
        assert rt.lookup_rids("danbooru", "123") == [0, 1]
        # canonical rid order: dataset_new sorts before dataset_old
        assert rt.lookup_rids("danbooru", "123", "dataset_new") == [0]
        record = rt.resolve_one("danbooru", "123", "dataset_old")
        assert record.rid == 1


def _compile_with_crash(tmp_path, monkeypatch, crash_stage, partial):
    """Run compile once crashing in crash_stage, then retry to completion."""
    import sakurapool.runtime.compiler as compiler

    real = getattr(compiler, crash_stage)
    calls = {"n": 0}

    def wrapper(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            partial()
            raise OSError(f"injected crash in {crash_stage}")
        return real(*args, **kwargs)

    monkeypatch.setattr(compiler, crash_stage, wrapper)
    tars = {"a.tar": {"1.jpg": b"a", "1.json": meta(["t"])}}
    index = build_p2_index(tmp_path, "crash", tars)
    root = tmp_path / "rt-crash"
    with pytest.raises(OSError, match="injected"):
        compiler.compile_runtime(load_p2_inventory(index), root)
    monkeypatch.setattr(compiler, crash_stage, real)
    return compiler.compile_runtime(load_p2_inventory(index), root)


def test_crash_resume_before_catalog(tmp_path, monkeypatch):
    """A: crash before catalog completes; retry rebuilds from stage1."""
    root = tmp_path / "rt-crash"

    def partial():
        staging = next(root.glob(".staging-*"))
        (staging / "catalog.sqlite").write_bytes(b"\x00" * 16)

    summary = _compile_with_crash(
        tmp_path, monkeypatch, "_catalog", partial)
    with RuntimeSnapshot.open(root) as rt:
        assert rt.rid_count == 1
    assert summary.rid_count == 1


def test_crash_resume_after_catalog_before_bitmaps(tmp_path, monkeypatch):
    """B: catalog done, crash at bitmaps start; retry reuses catalog."""
    def partial():
        pass  # catalog.sqlite already written by the real _catalog

    summary = _compile_with_crash(
        tmp_path, monkeypatch, "_bitmaps", partial)
    with RuntimeSnapshot.open(tmp_path / "rt-crash") as rt:
        assert rt.rid_count == 1
    assert summary.rid_count == 1


def test_crash_resume_partial_locations(tmp_path, monkeypatch):
    """C: partial locations.npy left behind; retry rebuilds it."""
    root = tmp_path / "rt-crash"

    def partial():
        staging = next(root.glob(".staging-*"))
        (staging / "locations.npy").write_bytes(b"\x93NUMPY\x00partial")

    summary = _compile_with_crash(
        tmp_path, monkeypatch, "_locations", partial)
    with RuntimeSnapshot.open(root) as rt:
        assert rt.rid_count == 1
    assert summary.rid_count == 1


def test_crash_resume_after_bitmaps(tmp_path, monkeypatch):
    """D: bitmaps done, crash before SNAPSHOT; retry continues."""
    summary = _compile_with_crash(
        tmp_path, monkeypatch, "_snapshot", lambda: None)
    with RuntimeSnapshot.open(tmp_path / "rt-crash") as rt:
        assert rt.rid_count == 1
    assert summary.rid_count == 1


def test_crash_resume_ready_rename(tmp_path, monkeypatch):
    """E: SNAPSHOT written, READY written, rename failed; retry publishes."""
    import os

    import sakurapool.runtime.compiler as compiler

    tars = {"a.tar": {"1.jpg": b"a", "1.json": meta(["t"])}}
    index = build_p2_index(tmp_path, "crash", tars)
    root = tmp_path / "rt-crash"
    inv = load_p2_inventory(index)
    original_rename = os.rename
    calls = {"n": 0}

    def flaky_rename(src, dst):
        calls["n"] += 1
        if calls["n"] == 1 and Path(src).name.startswith(".staging-"):
            raise OSError("injected crash at rename")
        return original_rename(src, dst)

    monkeypatch.setattr(os, "rename", flaky_rename)
    with pytest.raises(OSError, match="injected crash at rename"):
        compiler.compile_runtime(inv, root)
    monkeypatch.setattr(os, "rename", original_rename)
    summary = compiler.compile_runtime(inv, root)
    with RuntimeSnapshot.open(root) as rt:
        assert rt.rid_count == 1
    assert summary.rid_count == 1


def test_ready_snapshot_is_reused(tmp_path):
    """F: READY done; recompiling reuses the published snapshot."""
    import sakurapool.runtime.compiler as compiler

    tars = {"a.tar": {"1.jpg": b"a", "1.json": meta(["t"])}}
    index = build_p2_index(tmp_path, "crash", tars)
    root = tmp_path / "rt-crash"
    inv = load_p2_inventory(index)
    first = compiler.compile_runtime(inv, root)
    second = compiler.compile_runtime(inv, root)
    assert second.snapshot_id == first.snapshot_id
    assert not list(root.glob(".staging-*"))


def test_fresh_process_open_and_query(tmp_path):
    """§46/§47: fresh Python process verifies mmap/SQLite/bitmap/query."""
    import subprocess
    import sys

    tars = {"a.tar": {
        "1.jpg": b"a", "1.json": meta(["t", "u"]),
        "2.jpg": b"b", "2.json": meta(["t"]),
    }}
    index = build_p2_index(tmp_path, "fresh", tars)
    root = tmp_path / "rt-fresh"
    import sakurapool.runtime.compiler as compiler
    summary = compiler.compile_runtime(load_p2_inventory(index), root)
    code = (
        "import json, sys;"
        "from sakurapool.runtime.snapshot import RuntimeSnapshot;"
        "from sakurapool.runtime.query import RuntimeQuerySpec;"
        "rt = RuntimeSnapshot.open(sys.argv[1]);"
        "r = rt.query(RuntimeQuerySpec(namespace='tags', all_tags=['t']));"
        "rids = sorted(r.limit(10));"
        "batch = next(r.iter_location_batches(8));"
        "print(json.dumps({'rids': rids, 'count': r.count(),"
        " 'first_rid': int(batch.rid[0]),"
        " 'first_size': int(batch.image_size[0]),"
        " 'snapshot': rt.snapshot_id, 'cache_misses': rt.cache.misses}))"
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC) + os.pathsep + env.get("PYTHONPATH", "")
    out = subprocess.run(
        [sys.executable, "-c", code, str(root)], capture_output=True,
        text=True, env=env, check=True).stdout.strip()
    result = json.loads(out)
    assert result["count"] == 2
    assert result["rids"] == [0, 1]
    assert result["snapshot"] == summary.snapshot_id
    assert result["first_size"] == 1  # b"a" payload
    assert result["cache_misses"] >= 1


def test_catalog_query_plans_use_indexes(tmp_path):
    """EXPLAIN QUERY PLAN: every runtime catalog lookup must use an index."""
    from synthetic_p2 import ObjectSpec, SampleSpec, build_p2_directory

    idx = tmp_path / "p2"
    build_p2_directory(idx, dataset="ds", source="src", objects=[
        ObjectSpec("a.tar", [SampleSpec(f"{i}.jpg", str(i),
                                         [(f"t{i % 7}", None)])
                                    for i in range(20)]),
        ObjectSpec("b.tar", [SampleSpec(f"{i}.jpg", str(i),
                                         [(f"t{i % 5}", None)])
                                    for i in range(20)],
                  namespace="danbooru"),
    ], created_at="2025-01-01T00:00:00+00:00")
    import sakurapool.runtime.compiler as compiler
    summary = compiler.compile_runtime(load_p2_inventory(idx),
                                       tmp_path / "rt")
    import sqlite3
    con = sqlite3.connect(str(summary.path / "catalog.sqlite"))
    plans = {
        "namespace": ("SELECT namespace_id FROM namespaces WHERE namespace = ?",
                      ("x",)),
        "tag": ("SELECT t.tag_id FROM tags t JOIN namespaces n "
                "ON n.namespace_id = t.namespace_id WHERE n.namespace = ? "
                "AND t.value = ?", ("x", "y")),
        "source": ("SELECT source_id FROM sources WHERE name = ?", ("x",)),
        "dataset": ("SELECT dataset_id FROM datasets WHERE name = ?", ("x",)),
        "records_source_post": ("SELECT rid FROM records WHERE source_id = ? "
                                "AND post_id = ? ORDER BY rid", (0, "p")),
        "records_dataset_post": ("SELECT rid FROM records WHERE source_id = ? "
                                 "AND post_id = ? AND dataset_id = ? "
                                 "ORDER BY rid", (0, "p", 0)),
        "record_by_rid": ("SELECT record_id, source_id, dataset_id, post_id "
                          "FROM records WHERE rid = ?", (0,)),
        "records_batch": ("SELECT rid, record_id, source_id, dataset_id, "
                          "post_id FROM records WHERE rid IN (?,?,?) "
                          "ORDER BY rid", (0, 1, 2)),
    }
    for name, (sql, params) in plans.items():
        rows = con.execute("EXPLAIN QUERY PLAN " + sql, params).fetchall()
        text = "\n".join(row[3] for row in rows)
        assert "SCAN" not in text, f"{name}: full scan detected:\n{text}"
        assert "USING" in text, f"{name}: no index used:\n{text}"
    con.close()


def test_not_semantics_states(tmp_path):
    """§43: NOT monochrome excludes only known-with-monochrome."""
    from synthetic_p2 import ObjectSpec, SampleSpec, build_p2_directory

    import sakurapool.runtime.compiler as compiler

    samples = [
        SampleSpec("1.jpg", "1", [("monochrome", None)], "known"),   # A
        SampleSpec("2.jpg", "2", [("solo", None)], "known"),         # B
        SampleSpec("3.jpg", "3", [], "empty"),                       # C
        SampleSpec("4.jpg", "4", [], "missing"),                     # D
        SampleSpec("5.jpg", "5", [], "invalid"),                     # E
    ]
    idx = tmp_path / "p2-not"
    build_p2_directory(idx, dataset="ds", source="src",
                       objects=[ObjectSpec("a.tar", samples)],
                       created_at="2025-01-01T00:00:00+00:00")
    compiler.compile_runtime(load_p2_inventory(idx), tmp_path / "rt-not")
    with RuntimeSnapshot.open(tmp_path / "rt-not") as rt:
        assert sorted(rt.query(
            RuntimeQuerySpec(namespace="tags",
                             none_tags=["monochrome"])).limit(10)) == [1, 2]


def test_rids_are_canonical_and_bounded():
    rows = [
        {"dataset_id": "d", "object_id": "b", "sample_path": "2", "record_id": "f"},
        {"dataset_id": "d", "object_id": "a", "sample_path": "2", "record_id": "e"},
        {"dataset_id": "d", "object_id": "a", "sample_path": "1", "record_id": "d"},
    ]
    assigned = assign_rids(rows)
    assert [row["record_id"] for row in assigned] == ["d", "e", "f"]
    assert [row["rid"] for row in assigned] == [0, 1, 2]


@pytest.mark.parametrize("count", [2**32 - 1, 2**32])
def test_rid_capacity_accepts_boundary_counts_without_allocation(count):
    check_rid_capacity(count)


@pytest.mark.parametrize("count", [2**32 + 1, -(2**32)])
def test_rid_capacity_rejects_overflow_and_negative(count):
    with pytest.raises(ValueError, match="uint32"):
        check_rid_capacity(count)


def test_assign_rids_checks_capacity_before_sorting():
    class SmallRows:
        def __len__(self):
            return 2**32 + 1

        def __iter__(self):
            raise AssertionError("sort must never run past the capacity check")

    with pytest.raises(ValueError, match="uint32"):
        assign_rids(SmallRows())
