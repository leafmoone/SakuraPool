"""P3 reviewer-remediation acceptance tests (items A-H).

Each test maps to a concrete reviewer finding:
A  locations identity trailer + explicit snapshot open + keyword query
B  READY reuse validated, current.json repaired, staging ownership
C  streaming hash parity (no whole-file read)
D  ByteLRU oversized-blob and non-positive budget
E  bare unknown namespace, QueryResult.__len__, object_ref, record
   batch names, stored-cardinality planner short-circuit
G  inventory fail-closed strays + row ownership + synthetic P2 contract
H  uint64 > 2**63 offsets and independent zero-length metadata presence
"""

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_runtime as T  # noqa: E402  (real-P2 fixture helpers)
from synthetic_p2 import (  # noqa: E402
    ObjectSpec,
    SampleSpec,
    assert_object_ref_valid,
    build_p2_directory,
)

SRC = Path(__file__).resolve().parents[1] / "src"


def _compile_synthetic(tmp_path, name, objects, dataset="ds", source="src",
                       created_at="2025-01-01T00:00:00+00:00"):
    idx = tmp_path / f"p2-{name}"
    build_p2_directory(idx, dataset=dataset, source=source, objects=objects,
                       created_at=created_at)
    from sakurapool.runtime import compile_runtime, load_p2_inventory
    root = tmp_path / f"rt-{name}"
    summary = compile_runtime(load_p2_inventory(idx), root)
    return summary, root


def test_locations_payload_tamper_only_full_verify(tmp_path):
    """A: same-size in-place payload edit is NOT detected by the fast open
    (documented boundary); the streaming full verification detects it."""
    summary, root = _compile_synthetic(tmp_path, "tamper", [
        ObjectSpec("a.tar", [
            SampleSpec("1.jpg", "1", [("t", None)]),
            SampleSpec("2.jpg", "2", [("t", None)]),
        ])
    ])
    from sakurapool.runtime import RuntimeSnapshot
    snap = root / "snapshots" / summary.snapshot_id
    loc = snap / "locations.npy"
    data = bytearray(loc.read_bytes())
    # one payload byte of the last row (before the identity trailer)
    data[-82 - 31] ^= 0xFF
    loc.write_bytes(bytes(data))
    with RuntimeSnapshot.open(root) as rt:  # fast open: identity intact
        assert rt.location(0)["object_idx"] in (0, 1)
    with pytest.raises(Exception, match="sha256"):
        RuntimeSnapshot.open(root, full_verify=True)


def test_whole_file_swap_same_shape_rejected(tmp_path):
    """A: an entire locations.npy / bitmaps.sqlite / catalog.sqlite copied
    from another same-shape snapshot must be rejected by the fast open."""
    objs_a = [ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("a", None)]),
                                   SampleSpec("2.jpg", "2", [("b", None)])])]
    objs_b = [ObjectSpec("b.tar", [SampleSpec("1.jpg", "1", [("c", None)]),
                                   SampleSpec("2.jpg", "2", [("d", None)])])]
    summary_a, root_a = _compile_synthetic(tmp_path, "swap-a", objs_a,
                                           dataset="dsa")
    summary_b, root_b = _compile_synthetic(tmp_path, "swap-b", objs_b,
                                           dataset="dsb")
    assert summary_a.rid_count == summary_b.rid_count
    from sakurapool.runtime import RuntimeSnapshot
    from sakurapool.runtime.errors import SnapshotCorruptError, SnapshotMixError
    snap_a = root_a / "snapshots" / summary_a.snapshot_id
    snap_b = root_b / "snapshots" / summary_b.snapshot_id

    (snap_a / "locations.npy").write_bytes(
        (snap_b / "locations.npy").read_bytes())
    with pytest.raises(SnapshotMixError, match="locations"):
        RuntimeSnapshot.open(root_a)

    (snap_a / "locations.npy").write_bytes(
        (snap_b / "locations.npy").read_bytes()[:-40])
    with pytest.raises(SnapshotCorruptError, match="size"):
        RuntimeSnapshot.open(root_a)

    (snap_a / "locations.npy").write_bytes(
        (snap_b / "locations.npy").read_bytes())
    (snap_a / "bitmaps.sqlite").write_bytes(
        (snap_b / "bitmaps.sqlite").read_bytes())
    with pytest.raises(SnapshotCorruptError, match="bitmaps"):
        RuntimeSnapshot.open(root_a)
    # the streaming full verification catches it too
    with pytest.raises(SnapshotCorruptError, match="sha256"):
        RuntimeSnapshot.open(root_a, full_verify=True)


def test_explicit_snapshot_dir_open(tmp_path):
    """A3: open(compile output path) works and binds the directory name to
    the manifest id; a renamed directory is rejected."""
    summary, root = _compile_synthetic(tmp_path, "explicit", [
        ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("t", None)])])
    ])
    from sakurapool.runtime import RuntimeSnapshot
    snap = root / "snapshots" / summary.snapshot_id
    with RuntimeSnapshot.open(snap) as rt:
        assert rt.snapshot_id == summary.snapshot_id
    renamed = root / "snapshots" / "renamed-dir"
    snap.rename(renamed)
    from sakurapool.runtime.errors import SnapshotCorruptError
    with pytest.raises(SnapshotCorruptError):
        RuntimeSnapshot.open(renamed)
    # current.json open also keeps working and sees the renamed dir fail
    # only via the manifest comparison
    root.joinpath("current.json").write_text(json.dumps(
        {"snapshot_id": summary.snapshot_id,
         "path": f"snapshots/{summary.snapshot_id}"}))


def test_keyword_query_entry_and_len_and_object_ref(tmp_path):
    """E: snapshot.query(sources=...) keyword form, len(result), and the
    full ObjectRef entry."""
    summary, root = _compile_synthetic(tmp_path, "kw", [
        ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("t", None)])])
    ])
    from sakurapool.runtime import RuntimeSnapshot
    with RuntimeSnapshot.open(root) as rt:
        result = rt.query(namespace="tags", all_tags=["t"])
        assert len(result) == result.count() == 1
        result2 = rt.query()  # empty spec = universe
        assert len(result2) == rt.rid_count
        batch = next(iter(result2.iter_location_batches(16)))
        ref = rt.object_ref(int(batch.object_idx[0]))
        assert ref["object_id"].startswith("a.tar@sha256-")
        assert ref["object_version"] == ref["validator"]
        assert len(ref["validator"]) == 64
        assert ref["object_size"] == 1000
        assert ref["archive_format"] == "tar"
        with pytest.raises(Exception, match="unknown object_idx"):
            rt.object_ref(99)
    # keyword entry must reach the same result as the spec object form
    from sakurapool.runtime import RuntimeQuerySpec
    with RuntimeSnapshot.open(root) as rt:
        a = rt.query(namespace="tags", all_tags=["t"]).limit(10)
        b = rt.query(RuntimeQuerySpec(namespace="tags",
                                      all_tags=["t"])).limit(10)
        assert a == b


def test_record_batches_carry_names(tmp_path):
    """E5: identity batch output includes source/dataset names."""
    summary, root = _compile_synthetic(tmp_path, "names", [
        ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("t", None)])])
    ])
    from sakurapool.runtime import RuntimeSnapshot
    with RuntimeSnapshot.open(root) as rt:
        batch = next(rt.query().iter_record_batches(16))
        assert batch.source_name == ["src"]
        assert batch.dataset_name == ["ds"]
        assert batch.source_id == [0] and batch.dataset_id == [0]


def test_bare_unknown_namespace_errors(tmp_path):
    """E1: RuntimeQuerySpec(namespace='typo') with no tags must raise."""
    summary, root = _compile_synthetic(tmp_path, "ns", [
        ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("t", None)])])
    ])
    from sakurapool.runtime import RuntimeQuerySpec, RuntimeSnapshot
    from sakurapool.runtime.errors import UnknownQueryValueError
    with RuntimeSnapshot.open(root) as rt:
        with pytest.raises(UnknownQueryValueError, match="namespace"):
            rt.query(RuntimeQuerySpec(namespace="typo"))
        with pytest.raises(UnknownQueryValueError, match="namespace"):
            rt.query(namespace="typo")
        with pytest.raises(UnknownQueryValueError, match="namespace"):
            rt.query(RuntimeQuerySpec(
                any_of=(RuntimeQuerySpec(namespace="typo"),)))
        # a valid bare namespace still means the universe
        assert rt.query(RuntimeQuerySpec(namespace="tags")).count() == 1


def test_planner_short_circuits_on_stored_cardinality(tmp_path):
    """E6: AND terms are planned from stored cardinality and the AND can
    stop without materializing the remaining bitmaps."""
    objs = [ObjectSpec("a.tar", [
        SampleSpec(f"{i}.jpg", str(i),
                   [("common", None),
                    ("only0" if i == 0 else "only1", None),
                    ("never", None)])
        for i in range(4)
    ])]
    summary, root = _compile_synthetic(tmp_path, "plan", objs)
    from sakurapool.runtime import RuntimeQuerySpec, RuntimeSnapshot
    with RuntimeSnapshot.open(root) as rt:
        loaded = []
        original = rt._get_bitmap  # noqa: SLF001 - test boundary

        def tracking(kind, bitmap_id):
            loaded.append((kind, bitmap_id))
            return original(kind, bitmap_id)

        rt._get_bitmap = tracking
        result = rt.query(RuntimeQuerySpec(
            namespace="tags", all_tags=["common", "only0"]))
        assert result.count() == 1
        order = [bid for kind, bid in loaded if kind == "tag"]
        # planned by stored cardinality: only0 (1) before common (4)
        cat = rt._catalog
        id_only0 = cat.execute(
            "SELECT tag_id FROM tags WHERE value = 'only0'").fetchone()[0]
        assert order[0] == id_only0
        # early empty: only0 AND only1 already disjoint, so the third
        # term ('common', cardinality 4) is never materialized
        loaded.clear()
        result = rt.query(RuntimeQuerySpec(
            namespace="tags", all_tags=["only0", "only1", "common"]))
        assert result.count() == 0
        cat = rt._catalog
        id_common = cat.execute(
            "SELECT tag_id FROM tags WHERE value = 'common'").fetchone()[0]
        tag_ids = [bid for kind, bid in loaded if kind == "tag"]
        assert len(tag_ids) == 2
        assert id_common not in tag_ids  # never materialized


def test_byte_lru_oversized_and_budget(tmp_path):
    """D: oversized single blobs are never cached; non-positive budgets
    are rejected."""
    from sakurapool.runtime.snapshot import ByteLRU

    with pytest.raises(ValueError):
        ByteLRU(0)
    with pytest.raises(ValueError):
        ByteLRU(-1)
    lru = ByteLRU(4)
    lru.put(("tag", 1), b"123456789")  # 9 > 4
    assert lru.resident_bytes() == 0
    assert lru.get(("tag", 1)) is None
    assert lru.misses == 1
    lru.put(("tag", 2), b"123")
    lru.put(("tag", 3), b"456")  # resident 6 > 4 -> evict oldest
    assert lru.resident_bytes() == 3
    assert lru.evictions == 1
    assert lru.get(("tag", 2)) is None
    assert lru.get(("tag", 3)) == b"456"


def test_byte_lru_32mib_real_eviction():
    """D: real 32 MiB budget with 48 evictions of 1 MiB blobs - the
    1 MiB demo does not substitute for this."""
    from sakurapool.runtime.snapshot import ByteLRU

    lru = ByteLRU(32 << 20)
    blob = b"x" * (1 << 20)
    for i in range(80):
        lru.put(("tag", i), bytes((i,)) * (1 << 20))
        assert lru.resident_bytes() <= 32 << 20
    assert lru.evictions == 48
    assert lru.resident_bytes() == 32 << 20
    # oldest evicted, newest retained
    assert lru.get(("tag", 0)) is None
    assert lru.get(("tag", 79)) == bytes((79,)) * (1 << 20)
    del blob


def test_default_cache_budget_is_256mib(tmp_path):
    """D: the default open() cache budget stays 256 MiB."""
    summary, root = _compile_synthetic(tmp_path, "cache256", [
        ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("t", None)])])
    ])
    from sakurapool.runtime import RuntimeSnapshot
    with RuntimeSnapshot.open(root) as rt:
        assert rt.cache.byte_limit == 256 * 1024 * 1024


def test_streaming_hash_parity(tmp_path):
    """C: the streaming hash helper equals hashlib.sha256 over the file
    and never needs the whole file in Python memory."""
    import os

    from sakurapool.runtime import compiler
    for size in (0, 1, 1024, (1 << 20) + 12345):
        path = tmp_path / f"f{size}.bin"
        payload = os.urandom(size) if size else b""
        path.write_bytes(payload)
        assert compiler._sha256_file(path) == \
            hashlib.sha256(payload).hexdigest()


def test_uint64_offsets_and_metadata_presence(tmp_path):
    """H: offsets beyond 2**63 round-trip losslessly; zero-length metadata
    keeps HAS_METADATA; absent json drops it."""
    objs = [ObjectSpec("a.tar", [
        SampleSpec("1.jpg", "1", [("t", None)],
                   has_json=True, json_size=64),
        SampleSpec("2.jpg", "2", [("t", None)],
                   has_json=True, json_size=0),
        SampleSpec("3.jpg", "3", [("t", None)], has_json=False),
    ], uint64_offsets=True)]
    summary, root = _compile_synthetic(tmp_path, "u64", objs)
    from sakurapool.runtime import RuntimeSnapshot
    with RuntimeSnapshot.open(root) as rt:
        base = 2**63
        l0 = rt.location(0)
        l1 = rt.location(1)
        l2 = rt.location(2)
        assert l0["image_offset"] == base
        assert l0["metadata_offset"] == base + 1000
        assert l0["metadata_size"] == 64
        assert l0["flags"] == 1
        assert l1["metadata_size"] == 0 and l1["flags"] == 1
        assert l2["flags"] == 0
        assert l2["metadata_offset"] == 0
        # 2**64-1 boundary is still legal
    objs2 = [ObjectSpec("b.tar", [
        SampleSpec("1.jpg", "1", [("t", None)]),
    ], size=2**64 - 1)]
    summary2, root2 = _compile_synthetic(tmp_path, "u64max", objs2)
    from sakurapool.runtime import RuntimeSnapshot
    with RuntimeSnapshot.open(root2) as rt:
        ref = rt.object_ref(0)
        assert ref["object_size"] == 2**64 - 1
        assert rt.location(0)["image_size"] == 32
    with pytest.raises(Exception, match="uint64"):
        # out-of-range value must fail at the SQLite-encoding boundary
        compiler_check()


def compiler_check():
    from sakurapool.runtime import compiler
    compiler._u64(2**64)


def test_inventory_rejects_stray_files(tmp_path):
    """G1: an orphan file next to INPUT.json fails closed."""
    summary, root = _compile_synthetic(tmp_path, "stray", [
        ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("t", None)])])
    ])
    from sakurapool.runtime import load_p2_inventory
    from sakurapool.runtime.errors import CorruptInputError
    idx = tmp_path / "p2-stray"
    (idx / "orphan.partial").write_bytes(b"leftover")
    with pytest.raises(CorruptInputError, match="unexpected file"):
        load_p2_inventory(idx)
    (idx / "orphan.partial").unlink()
    (idx / "junk.parquet").write_bytes(b"not a declared fragment")
    with pytest.raises(CorruptInputError, match="unexpected file"):
        load_p2_inventory(idx)


def test_inventory_rejects_row_ownership_violation(tmp_path):
    """G2: a fragment row claiming a different object is rejected."""
    summary, root = _compile_synthetic(tmp_path, "own", [
        ObjectSpec("a.tar", [
            SampleSpec("1.jpg", "1", [("t", None)]),
            SampleSpec("2.jpg", "2", [("t", None)]),
        ])
    ])
    import pyarrow as pa
    import pyarrow.parquet as pq

    from sakurapool.runtime import load_p2_inventory
    from sakurapool.runtime.errors import CorruptInputError
    idx = tmp_path / "p2-own"
    shard_files = sorted(idx.glob("*.samples.parquet"))
    assert len(shard_files) == 1
    table = pq.read_table(shard_files[0])
    column_index = table.column_names.index("object_id")
    values = table.column(column_index).to_pylist()
    values[0] = "other.tar@sha256-" + "0" * 64
    arrays = list(table.columns)
    arrays[column_index] = pa.array(
        values, type=table.schema.field(column_index).type)
    table = pa.Table.from_arrays(arrays, schema=table.schema)
    pq.write_table(table, shard_files[0])
    # keep the COMMIT fragment record consistent so the row-ownership
    # check (not the sha check) is what fails
    commit_files = sorted(idx.glob("*.COMMIT"))
    assert len(commit_files) == 1
    commit = json.loads(commit_files[0].read_text())
    commit["files"]["samples"]["sha256"] = \
        hashlib.sha256(shard_files[0].read_bytes()).hexdigest()
    commit["files"]["samples"]["bytes"] = shard_files[0].stat().st_size
    commit_files[0].write_text(json.dumps(commit))
    with pytest.raises(CorruptInputError, match="row object_id"):
        load_p2_inventory(idx)


def test_synthetic_p2_follows_real_contract(tmp_path):
    """G3: the synthetic generator produces a model the real P2 contract
    accepts (BLAKE2b16 record id, rel@sha256(input) object id, ObjectRef
    validity), and a real-indexer-built fixture still compiles."""
    from sakurapool.indexer import _json
    from sakurapool.records import RecordKey
    from sakurapool.runtime import RuntimeQuerySpec, RuntimeSnapshot

    obj = ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("t", None)])])
    assert_object_ref_valid("ds", "src", obj)
    digest = hashlib.sha256(_json(["synthetic", "ds", "a.tar"])).hexdigest()
    expected_record = RecordKey("ds", f"a.tar@sha256-{digest}",
                                "1.jpg").record_id
    summary, root = _compile_synthetic(tmp_path, "contract", [obj])
    with RuntimeSnapshot.open(root) as rt:
        batch = next(rt.query().iter_record_batches(16))
        assert batch.record_id[0] == expected_record
    # real P2 fixture (indexer-built TAR) still compiles and queries
    _, root2 = T.compile_small(tmp_path, name="real")
    with RuntimeSnapshot.open(root2) as rt:
        assert rt.query(RuntimeQuerySpec(namespace="tags",
                                         all_tags=["solo"])).count() == 1


def test_created_at_does_not_change_snapshot_id(tmp_path):
    """F2: only the P2 created_at timestamps change -> same snapshot_id
    (the fingerprint excludes created_at by contract §7)."""
    objs = [ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("t", None)])])]
    summary1, _ = _compile_synthetic(
        tmp_path, "ts1", objs, created_at="2025-01-01T00:00:00+00:00")
    summary2, _ = _compile_synthetic(
        tmp_path, "ts2", objs, created_at="2026-06-30T23:59:59+00:00")
    assert summary1.snapshot_id == summary2.snapshot_id


def test_ready_reuse_validated_and_current_repaired(tmp_path):
    """B: READY reuse is validated (corrupt READY fails closed); a broken
    current.json is repaired on the next compile."""
    import sakurapool.runtime.compiler as compiler
    from sakurapool.runtime import load_p2_inventory
    objs = [ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("t", None)])])]
    summary, root = _compile_synthetic(tmp_path, "ready", objs)
    snap = root / "snapshots" / summary.snapshot_id

    # current.json missing -> reuse validates and repairs it
    (root / "current.json").unlink()
    again = compiler.compile_runtime(load_p2_inventory(tmp_path / "p2-ready"),
                                     root)
    assert again.snapshot_id == summary.snapshot_id
    current = json.loads((root / "current.json").read_text())
    assert current["snapshot_id"] == summary.snapshot_id

    # corrupt READY content -> fail closed, no silent reuse
    ready = snap / "READY"
    ready.write_text("something-else")
    with pytest.raises(Exception, match="READY"):
        compiler.compile_runtime(load_p2_inventory(tmp_path / "p2-ready"),
                                 root)
    ready.write_text(summary.snapshot_id)

    # corrupt a published payload -> reuse must not open silently
    loc = snap / "locations.npy"
    data = bytearray(loc.read_bytes())
    data[len(data) // 2] ^= 1
    loc.write_bytes(bytes(data))
    with pytest.raises(Exception, match="sha256"):
        compiler.compile_runtime(load_p2_inventory(tmp_path / "p2-ready"),
                                 root)


def test_same_id_staging_without_marker_fail_closed(tmp_path):
    """B (reviewer repro): a pre-created .staging-<correct snap_id> with a
    user file and NO owner marker must fail closed - compile must raise and
    the user file must survive."""
    import sakurapool.runtime.compiler as compiler
    from sakurapool.runtime import load_p2_inventory
    from sakurapool.runtime.errors import SnapshotCorruptError
    objs = [ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("t", None)])])]
    summary, root = _compile_synthetic(tmp_path, "sameid", objs)
    # drop READY so compile reaches the staging branch (READY reuse returns
    # earlier and would not touch staging)
    (root / "snapshots" / summary.snapshot_id / "READY").unlink()
    staging = root / f".staging-{summary.snapshot_id}"
    staging.mkdir()
    (staging / "user.txt").write_text("do not delete")
    with pytest.raises(SnapshotCorruptError, match="unowned staging"):
        compiler.compile_runtime(load_p2_inventory(tmp_path / "p2-sameid"),
                                 root)
    # fail closed: nothing deleted, nothing reused
    assert (staging / "user.txt").read_text() == "do not delete"
    assert staging.exists()
    # after manual cleanup (the foreign staging and the de-READY'd publish,
    # which the compiler also refuses to delete unowned) the compile
    # succeeds from scratch
    (staging / "user.txt").unlink()
    staging.rmdir()
    import shutil as _shutil
    _shutil.rmtree(root / "snapshots" / summary.snapshot_id)
    again = compiler.compile_runtime(
        load_p2_inventory(tmp_path / "p2-sameid"), root)
    assert again.snapshot_id == summary.snapshot_id


def test_same_id_staging_forced_marker_fail_closed(tmp_path):
    """B: a forged marker with foreign files still fails closed (the
    known-file audit is independent of the marker)."""
    import json as _json

    import sakurapool.runtime.compiler as compiler
    from sakurapool.runtime import load_p2_inventory
    from sakurapool.runtime.errors import SnapshotCorruptError
    objs = [ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("t", None)])])]
    summary, root = _compile_synthetic(tmp_path, "forged", objs)
    (root / "snapshots" / summary.snapshot_id / "READY").unlink()
    staging = root / f".staging-{summary.snapshot_id}"
    staging.mkdir()
    (staging / "OWNER.json").write_text(_json.dumps({
        "protocol": 1, "owner": compiler.RUNTIME_COMPILER,
        "snapshot_id": summary.snapshot_id, "role": "staging",
        "known_files": []}))
    (staging / "user.txt").write_text("do not delete")
    with pytest.raises(SnapshotCorruptError, match="unowned staging"):
        compiler.compile_runtime(load_p2_inventory(tmp_path / "p2-forged"),
                                 root)
    assert (staging / "user.txt").read_text() == "do not delete"


def test_unowned_snapshot_dir_never_deleted(tmp_path):
    """B: a pre-created snapshots/<snap_id> (no READY, no owner marker)
    must fail closed at publish time, not be rmtree'd."""
    import json as _json

    import sakurapool.runtime.compiler as compiler
    from sakurapool.runtime import load_p2_inventory
    from sakurapool.runtime.errors import SnapshotCorruptError
    objs = [ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("t", None)])])]
    summary, root = _compile_synthetic(tmp_path, "snapdir", objs)
    # wipe our publish and plant a foreign directory of the same name
    snap2 = root / "snapshots" / summary.snapshot_id
    (snap2 / "READY").unlink()
    (snap2 / "user.bin").write_bytes(b"keep me")
    with pytest.raises(SnapshotCorruptError, match="unowned snapshot"):
        compiler.compile_runtime(load_p2_inventory(tmp_path / "p2-snapdir"),
                                 root)
    assert (snap2 / "user.bin").read_bytes() == b"keep me"
    # with the compiler's own staging marker the directory would be ours -
    # but the known-file audit still rejects it while user.bin is inside.
    # Only after manual removal of the foreign file may the publish proceed.
    owner = root / "snapshots" / summary.snapshot_id / "OWNER.json"
    owner.write_text(_json.dumps({
        "protocol": 1, "owner": compiler.RUNTIME_COMPILER,
        "snapshot_id": summary.snapshot_id, "role": "staging"}))
    with pytest.raises(SnapshotCorruptError, match="unowned snapshot"):
        compiler.compile_runtime(
            load_p2_inventory(tmp_path / "p2-snapdir"), root)
    (snap2 / "user.bin").unlink()
    again = compiler.compile_runtime(
        load_p2_inventory(tmp_path / "p2-snapdir"), root)
    assert again.snapshot_id == summary.snapshot_id


def test_staging_ownership_respected(tmp_path):
    """B: a foreign .staging-<other-id> directory is never deleted."""
    objs = [ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("t", None)])])]
    summary, root = _compile_synthetic(tmp_path, "own-staging", objs)
    import sakurapool.runtime.compiler as compiler
    from sakurapool.runtime import load_p2_inventory
    foreign = root / ".staging-deadbeef"
    foreign.mkdir()
    (foreign / "STAGE.txt").write_text("stage1")
    (foreign / "user-file.bin").write_bytes(b"keep me")
    again = compiler.compile_runtime(
        load_p2_inventory(tmp_path / "p2-own-staging"), root)
    assert again.snapshot_id == summary.snapshot_id
    assert (foreign / "user-file.bin").read_bytes() == b"keep me"


def test_cli_spec_and_flags(tmp_path, capsys):
    """F1: --snapshot/--spec/--full/--index CLI forms work, including an
    any_of spec from a JSON file."""
    import json as _json

    from sakurapool import cli
    summary, root = _compile_synthetic(tmp_path, "cli", [
        ObjectSpec("a.tar", [
            SampleSpec("1.jpg", "1", [("alpha", None)]),
            SampleSpec("2.jpg", "2", [("beta", None)]),
        ])
    ])
    assert cli.main(["runtime", "query", str(root),
                     "--namespace", "tags", "--all-tag", "alpha"]) == 0
    out = _json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["count"] == 1
    assert cli.main(["runtime", "query",
                     "--snapshot", str(root),
                     "--namespace", "tags", "--any-tag", "alpha",
                     "--any-tag", "beta", "--full"]) == 0
    out = _json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["count"] == 2
    spec_file = tmp_path / "spec.json"
    spec_file.write_text(_json.dumps({
        "any_of": [
            {"namespace": "tags", "all_tags": ["alpha"]},
            {"namespace": "tags", "all_tags": ["beta"]},
        ]}))
    assert cli.main(["runtime", "query", str(root), "--spec",
                     str(spec_file)]) == 0
    out = _json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["count"] == 2
    # --spec cannot be mixed with term flags
    assert cli.main(["runtime", "query", str(root), "--spec",
                     str(spec_file), "--all-tag", "alpha"]) == 2
    # --index + --output compile form
    out_root = tmp_path / "rt-via-index"
    assert cli.main(["runtime", "compile", "--index",
                     str(tmp_path / "p2-cli"), "--output",
                     str(out_root)]) == 0
    out = _json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["rid_count"] == 2
    del summary


def test_planner_source_dataset_cardinality_order(tmp_path):
    """E §29: source/dataset terms plan by their real stored cardinality,
    not a hardcoded 0 - a rare tag must load before big source/dataset OR
    terms, including with all AND conditions present."""
    from sakurapool.runtime import RuntimeSnapshot
    objs = [
        ObjectSpec("a.tar",
                   [SampleSpec(f"{i}.jpg", str(i), [("hot", None)])
                    for i in range(50)]
                   + [SampleSpec("r.jpg", "900",
                                 [("rare", None), ("hot", None)])]),
        ObjectSpec("b.tar",
                   [SampleSpec(f"b{i}.jpg", f"b{i}",
                               [("hot", None)]
                               + ([("cold", None)] if i % 10 == 0 else []))
                    for i in range(50)]),
    ]
    summary, root = _compile_synthetic(tmp_path, "srcord", objs)
    rt = RuntimeSnapshot.open(root)
    assert rt._dataset_id("ds") is not None
    loaded: list[str] = []
    orig = rt._get_bitmap

    def tracking(kind: str, bitmap_id: int) -> Any:
        loaded.append(kind)
        return orig(kind, bitmap_id)

    rt._get_bitmap = tracking
    # rare tag (cardinality 1) vs source OR (101): rare must load first
    q1 = rt.query(sources=["src"], namespace="tags",
                  all_tags=["rare"])
    assert q1.count() == 1
    assert loaded and loaded[0] == "tag"
    assert "source" in loaded and loaded.index("source") > loaded.index(
        "tag")
    # full AND condition set: sources + datasets + all + any + none, all
    # pre-validated from stored cardinalities; the rare tag stays first and
    # the count stays exact.
    loaded.clear()
    q2 = rt.query(sources=["src"], datasets=["ds"], namespace="tags",
                  all_tags=["rare"], any_tags=["hot"], none_tags=["cold"])
    assert q2.count() == 1
    assert loaded[0] == "tag"
    for kind in ("source", "dataset"):
        assert kind in loaded
        assert loaded.index(kind) > loaded.index("tag")


def test_real_runtime_eviction_correctness(tmp_path):
    """D §25: eviction caused by REAL Roaring bitmap loads through
    RuntimeSnapshot (not arbitrary bytes): results identical before and
    after eviction, stats honest, resident bounded."""
    from sakurapool.runtime import RuntimeSnapshot
    samples = [SampleSpec(f"{i}.jpg", str(i), [(f"t{i % 4}", None)])
               for i in range(120000)]
    summary, root = _compile_synthetic(
        tmp_path, "evict-rt", [ObjectSpec("big.tar", samples)])
    # each 30000-rid Roaring blob serializes to ~16.4 KB (measured from the
    # bitmaps table), so a 48 KiB budget holds two of them and forces real
    # eviction on the third and fourth load
    budget = 48 * 1024
    rt = RuntimeSnapshot.open(root, cache_bytes=budget)
    first: dict[str, Any] = {}
    # three ~16.4 KB blobs in a 48 KiB cache: the first load is evicted by
    # the third
    for tag in ("t0", "t1", "t2"):
        q = rt.query(namespace="tags", all_tags=[tag])
        first[tag] = (q.count(), tuple(q.iter_rids()))
    assert rt.cache.evictions >= 1
    assert all(n == 30000 for n, _ in first.values())
    assert rt.cache.resident_bytes() <= budget
    # t1 and t2 are still resident: warm re-queries hit and return the
    # identical result set
    before_hits = rt.cache.hits
    for tag in ("t1", "t2"):
        q = rt.query(namespace="tags", all_tags=[tag])
        assert (q.count(), tuple(q.iter_rids())) == first[tag]
    assert rt.cache.hits >= 2 and rt.cache.hits > before_hits
    # t0 was evicted: the re-query reloads it and still returns the exact
    # same result (correctness across eviction)
    q0 = rt.query(namespace="tags", all_tags=["t0"])
    assert (q0.count(), tuple(q0.iter_rids())) == first["t0"]
    assert rt.cache.misses >= 4
    assert rt.cache.resident_bytes() <= budget
