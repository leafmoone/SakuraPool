import json

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
