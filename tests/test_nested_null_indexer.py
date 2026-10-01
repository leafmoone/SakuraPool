import hashlib
import io
import itertools
import json
import tarfile

import pyarrow.parquet as pq
import pytest
from test_nested_metadata import adapter, metadata

from sakurapool import indexer
from sakurapool.metadata import MISSING
from sakurapool.registry import AdapterRegistry
from sakurapool.runtime.compiler import compile_runtime
from sakurapool.runtime.inventory import load_p2_inventory
from sakurapool.runtime.snapshot import RuntimeSnapshot


@pytest.mark.parametrize("hash_images", [False, True])
def test_unknown_dimensions_p2_p3_roundtrip(tmp_path, hash_images):
    source = tmp_path / "nulls.tar"
    payload = b"not a decodable image: dimensions must come only from metadata"
    cases = list(itertools.product([MISSING, None, 0, 123], [MISSING, None, 0, 456]))
    invalid = [(name, bad) for name in ("width", "height")
               for bad in (-1, 2**32, True, False, "123", "", 1.5, [], {})]
    expected = {}
    with tarfile.open(source, "w") as archive:
        for post_id, case in enumerate(cases + invalid, 1):
            value = metadata()
            value["id"] = post_id
            value["captions"] = None if post_id % 2 else {"nl2": None}
            value["tags"]["artist"] = None
            value["tags"]["general"] = [None, "blue"]
            if post_id <= len(cases):
                for name, dimension in zip(("width", "height"), case):
                    if dimension is MISSING:
                        del value["image"][name]
                    else:
                        value["image"][name] = dimension
                expected[str(post_id)] = tuple(None if v is MISSING else v for v in case)
            else:
                name, bad = case
                value["image"][name] = bad
            for name, data in ((f"{post_id}.jpg", payload),
                               (f"{post_id}.json", json.dumps(value).encode())):
                info = tarfile.TarInfo(name)
                info.size = len(data)
                archive.addfile(info, io.BytesIO(data))
    registry = AdapterRegistry()
    registry.register(adapter())
    out = tmp_path / "out"
    result = indexer.scan(source, out, dataset=adapter().dataset, registry=registry,
                          hash_images=hash_images, verify_offsets=True)

    def rows(table):
        return [row for path in out.glob(f"*.{table}.parquet")
                for row in pq.read_table(path).to_pylist()]

    assert result["samples"] == len(cases)
    samples = rows("samples")
    assert {row["post_id"] for row in samples} == set(expected)
    expected_tags = [{"value": "blue", "category": "general"},
                     {"value": "series", "category": "copyright"}]
    for row in samples:
        assert (row["width"], row["height"]) == expected[row["post_id"]]
        assert row["image_format"] == "jpg"
        assert row["text"] is None
        assert row["tags_state"] == "known"
        assert row["tags"] == expected_tags
        assert row["status"] == "indexed"
        assert row["sha256"] == (hashlib.sha256(payload).hexdigest() if hash_images else None)
        assert row["hash_source"] == ("computed:sha256" if hash_images else "missing")
    for path in out.glob("*.samples.parquet"):
        schema = pq.read_schema(path)
        assert schema == indexer.SAMPLES_SCHEMA
        assert schema.field("width").nullable and schema.field("height").nullable
    assert all(row["tags"] == expected_tags for row in rows("annotations"))
    errors = rows("errors")
    assert len(errors) == len(invalid)
    assert {row["post_id"] for row in errors} == {
        str(i) for i in range(len(cases) + 1, len(cases) + len(invalid) + 1)}
    assert all(row["code"] == "metadata_invalid" for row in errors)
    assert all("must be a uint32-compatible int" in row["detail"] for row in errors)

    before = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
              for path in out.iterdir() if path.is_file()}
    assert json.loads((out / "INPUT.json").read_text())["format_version"] == 4
    summary = compile_runtime(load_p2_inventory(out), tmp_path / "runtime", chunk_size=1)
    by_id = {row["record_id"]: row for row in samples}
    with RuntimeSnapshot.open(summary.path, full_verify=True) as runtime:
        query = runtime.query(namespace="tags", all_tags=["blue"])
        assert query.count() == len(cases)
        assert runtime.query(datasets=[adapter().dataset]).count() == len(cases)
        record_ids = [rid for batch in query.iter_record_batches(batch_size=3)
                      for rid in batch.record_id]
        assert set(record_ids) == set(by_id)
        locations = [
            (int(rid), int(offset), int(size), int(meta_offset), int(meta_size))
            for batch in query.iter_location_batches(batch_size=3)
            for rid, offset, size, meta_offset, meta_size in zip(
                batch.rid, batch.image_offset, batch.image_size,
                batch.metadata_offset, batch.metadata_size)
        ]
        with source.open("rb") as handle:
            for rid, offset, size, meta_offset, meta_size in locations:
                row = by_id[record_ids[rid]]
                assert (offset, size) == (row["offset_data"], row["size"])
                assert (meta_offset, meta_size) == (row["json_offset_data"], row["json_size"])
                handle.seek(offset)
                assert handle.read(size) == payload
                handle.seek(meta_offset)
                assert str(json.loads(handle.read(meta_size))["id"]) == row["post_id"]
    assert before == {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                      for path in out.iterdir() if path.is_file()}
