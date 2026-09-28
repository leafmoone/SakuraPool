"""Fast benchmark-contract tests before expensive scale runs."""
import math
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import bench_runtime as bench  # noqa: E402


def test_anchor_sampling_frequency_and_correlations():
    import random
    rng = random.Random(bench.SEED_BASE + len("100k"))
    tags = bench.vocab("100k")
    samples = [bench.sample_tags(rng, tags["hot"], tags["medium"], tags["rare"])
               for _ in range(40000)]
    known = [set(values) for values, state in samples if state == "known"]
    counts = {tag: sum(tag in values for values in known) for tag in bench.ANCHORS}
    manifest = dict(known_records=len(known), anchor_counts=counts,
                    anchor_frequencies={tag: n / len(known) for tag, n in counts.items()})
    bench._validate_frequencies(manifest)
    assert all(counts.values())
    assert any(set(bench.ANCHORS) <= values for values in known)
    assert sum(set(bench.CORR_TAGS[:2]) <= values for values in known) > 1000
    assert all(bench.CORR_TAGS[1] not in values or
               {bench.ANCHORS[0], bench.CORR_TAGS[0]} <= values
               for values in known)
    assert all(not set(bench.EXCLUSIVE) <= values for values in known)
    with pytest.raises(ValueError):
        bench._validate_frequencies({**manifest, "anchor_counts": {**counts, bench.ANCHORS[0]: 1}})


def test_percentiles_and_workload_specs():
    numbers = list(range(1, 101))
    t = bench._timings(numbers)
    assert t["n"] == 100 and t["p50_ms"] == 50000
    assert t["p95_ms"] == 95000 and t["p99_ms"] == 99000
    assert t["max_ms"] == 100000
    assert t["percentile_algorithm"] == "nearest_rank"
    specs = bench._family_specs({})
    assert specs["three_tag_and"]["all_tags"] == ("hot_anchor", "corr_a", "corr_b")
    assert specs["source_2tag"]["sources"] == ("src_b",)
    assert specs["two_source_2tag"]["sources"] == ("src_a", "src_b")
    assert specs["empty_boundary"]["all_tags"] == bench.EXCLUSIVE
    assert bench.WARM_REPEATS >= 100


@pytest.fixture(scope="module")
def tiny_workload(tmp_path_factory):
    # 40k is light yet has reliable .1% rare frequency; compile only once.
    path = tmp_path_factory.mktemp("bench-workload")
    bench.SCALE_PARAMS["tiny"] = dict(objects_per_dir=1, samples_per_object=20000)
    try:
        manifest = bench.phase_gen(path, "tiny")
        bench.phase_compile(path, "tiny")
        yield path, manifest
    finally:
        bench.SCALE_PARAMS.pop("tiny", None)


def test_tiny_two_source_workload_and_capacity(tiny_workload):
    import pyarrow.parquet as pq
    from pyroaring import BitMap

    from sakurapool.runtime import RuntimeSnapshot

    path, manifest = tiny_workload
    # Independent P2 durable-source audit: count actual known rows/anchor
    # membership without reusing generator's manifest/helper math.
    known = 0
    anchors = {name: 0 for name in bench.ANCHORS}
    for _, dataset in bench.DIRS:
        shards = list((path / "p2" / dataset).glob("*.samples.parquet"))
        assert shards
        for shard in shards:
            for batch in pq.ParquetFile(shard).iter_batches(
                    batch_size=4096, columns=["tags_state", "tags"]):
                for state, tags in zip(batch.column(0).to_pylist(),
                                       batch.column(1).to_pylist(), strict=True):
                    if state != "known":
                        continue
                    known += 1
                    values = {t["value"] for t in tags}
                    for anchor in anchors:
                        anchors[anchor] += anchor in values
    assert known == manifest["known_records"] > 35000
    assert anchors == manifest["anchor_counts"]
    for anchor, count in anchors.items():
        lo, hi = bench.FREQUENCY_BANDS[anchor]
        assert lo <= count / known <= hi
        assert math.isclose(count / known, manifest["anchor_frequencies"][anchor])
    assert manifest["frequency_denominator"] == "danbooru tags_state=known records"
    with RuntimeSnapshot.open(path / "rt") as rt:
        specs = bench._family_specs(manifest)
        counts = {name: rt.query(bench._spec_from_parts(parts)).count()
                  for name, parts in specs.items()}
        assert all(counts[name] > 0 for name in bench._FAMILY_ORDER), counts
        assert counts["empty_boundary"] == 0
        assert counts["hot"] == manifest["anchor_counts"]["hot_anchor"]
        assert rt.query(bench._spec_from_parts(specs["source_2tag"])).count() > 0
        capacity = bench._capacity(rt, rt.path)
        # Independently deserialize each stored tag Roaring payload and
        # compare catalog row count, true BLOB byte lengths and cardinality.
        assert (rt._catalog.execute("SELECT COUNT(*) FROM tags").fetchone()[0]
                == capacity["unique_tags"])
        rows = rt._bitmaps.execute(
            "SELECT cardinality, blob FROM bitmaps WHERE kind='tag'").fetchall()
        independent_memberships = independent_payload = 0
        for cardinality, blob in rows:
            assert len(BitMap.deserialize(blob)) == cardinality
            independent_memberships += cardinality
            independent_payload += len(blob)
        assert capacity["tag_memberships"] == independent_memberships
        assert capacity["tag_bitmap_blob_bytes"] == independent_payload
        assert capacity["rid_count"] == 40000
        assert capacity["unique_tags"] > 700
        assert capacity["tag_memberships"] > 0
        assert capacity["tag_bitmap_blob_bytes"] > 0
        assert capacity["uint32_baseline_bytes"] == capacity["tag_memberships"] * 4
        assert math.isclose(capacity["bitmap_compression_ratio"],
                            capacity["tag_bitmap_blob_bytes"] /
                            (capacity["tag_memberships"] * 4))
        assert math.isclose(capacity["bytes_per_tag_membership"],
                            capacity["tag_bitmap_blob_bytes"] / capacity["tag_memberships"])
        assert set(capacity["snapshot_file_bytes"]) == {
            "catalog.sqlite", "bitmaps.sqlite", "locations.npy"}
        result = rt.query(bench._spec_from_parts(specs["rare"]))
        duration, rows = bench._drain_10k(result)
        assert duration >= 0 and rows == counts["rare"] < 10000


def test_integrated_timings_materialization_and_fail_closed_gates(tiny_workload):
    path, _ = tiny_workload
    result = bench.phase_query(path, "tiny", "tiny-unit-run")
    assert result["empty_boundary"]["result_cardinality"] == 0
    assert min(result["any_of_2_branch_cardinalities"]) > 0
    for name, family in result["families"].items():
        assert family["untimed_warmup"] is True, name
        assert family["warm_repeats"] == 100
        for key in ("warm_plan", "warm_first128_e2e", "warm_materialization"):
            assert family[key]["n"] == 100
            assert all(k in family[key] for k in ("p50_ms", "p95_ms", "p99_ms", "max_ms"))
        assert family["warm_materialization"]["requested_rows"] == 10000
        assert family["warm_materialization"]["actual_rows"] == min(
            family["result_cardinality"], 10000)
    for name in ("source_only", "hot"):
        assert result["families"][name]["warm_materialization"]["actual_rows"] == 10000
    assert "warm_10k" not in result["families"]["rare"]

    # This synthetic label tests gate logic ONLY; no tiny timings become a
    # 5M performance claim or enter benchmark.json.
    result["scale"] = "5M"
    result["locations100k"] = {"rows": 100000}
    assert all(bench._gates(result).values())
    result["families"]["hot"]["warm_repeats"] = 99
    with pytest.raises(ValueError, match="nonempty and timed"):
        bench._gates(result)
    result["families"]["hot"]["warm_repeats"] = 100
    for family in result["families"].values():
        family["warm_materialization"]["actual_rows"] = 9999
    with pytest.raises(ValueError, match="true 10k"):
        bench._gates(result)


def test_drain_does_not_request_extra_batch():
    class Fake:
        calls = 0
        def iter_location_batches(self, batch_size):
            assert batch_size == 2000
            for n in range(6):
                self.calls += 1
                if n == 5:
                    pytest.fail("requested an extra batch after 10000")
                yield type("Batch", (), {"rid": range(2000)})()
    fake = Fake()
    seconds, actual = bench._drain_10k(fake)
    assert seconds >= 0 and actual == 10000 and fake.calls == 5


def test_preflight_rejects_empty_any_of_branch(tiny_workload, monkeypatch):
    path, _ = tiny_workload
    original = bench._family_specs
    def broken(manifest):
        specs = original(manifest)
        a, _ = specs["any_of_2"]["any_of"]
        specs["any_of_2"] = dict(any_of=(
            a, dict(namespace="danbooru", all_tags=bench.EXCLUSIVE)))
        return specs
    monkeypatch.setattr(bench, "_family_specs", broken)
    with pytest.raises(ValueError, match="empty branch"):
        bench.phase_query(path, "tiny", "bad-branch")
