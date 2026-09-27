"""Original P2 fixtures and physical-record v2 contract regression tests."""

import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sakurapool import indexer
from sakurapool.cli import main
from sakurapool.records import (
    IdentityConflictError,
    MemberRef,
    ObjectRef,
    RecordKey,
    register_identity,
)
from sakurapool.registry import AdapterRegistry, DatasetAdapter


def make_tar(path, members, **kwargs):
    with tarfile.open(path, "w", **kwargs) as archive:
        for name, data in members.items() if isinstance(members, dict) else members:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))


def read_rows(output, name):
    return [row for p in sorted(output.glob(f"*.{name}.parquet"))
            for row in pq.read_table(p).to_pylist()]


CASES = [
    ("A", {"1.jpg": b"one", "1.json": b'{"tags":["a"]}',
           "2.webp": b"two", "2.json": b'{"tags":[]}'}, 2, []),
    ("B", {"3.jpg": b"three"}, 1, ["missing_metadata"]),
    ("C", {"4.json": b"{}"}, 0, ["missing_image"]),
    ("D", {"5.jpg": b"a", "5.webp": b"b", "5.json": b"{}"},
     0, ["ambiguous_image_member"]),
    ("E", {"6.jpg": b"six", "6.json": b"invalidJSON"}, 0, ["metadata_invalid"]),
    ("F", {"images/7.jpg": b"seven", "meta/7.json": b"{}"}, 1, []),
    ("G", {"8.jpg": b"eight", "8.json": b'{"source":"B"}'}, 0, ["source_mismatch"]),
    ("H", {"README.md": b"read", "manifest.json": b"{}", "foo.bin": b"aux",
           ".hidden.jpg": b"hidden", ".hidden.json": b"{}"}, 0, []),
]


@pytest.mark.parametrize("case,members,samples,codes", CASES)
def test_original_fixtures(tmp_path, case, members, samples, codes):
    source = tmp_path / f"{case}.tar"
    make_tar(source, members)
    registry = AdapterRegistry()
    registry.register(DatasetAdapter("demo", "A", image_prefix="images/", metadata_prefix="meta/"))
    out = tmp_path / "out"
    result = indexer.scan(source, out, dataset="demo", registry=registry)
    assert (result["objects_seen"], result["objects_committed"], result["samples"]) == (
        1, 1, samples)
    assert len(read_rows(out, "objects")) == 1
    assert sorted(r["code"] for r in read_rows(out, "errors")) == codes
    if case == "B":
        missing = read_rows(out, "samples")[0]
        assert missing["json_path"] is None
        assert missing["text"] is None
        assert missing["tags_state"] == "missing"
        missing_error = read_rows(out, "errors")[0]
        assert missing_error["post_id"] == "3"
        assert missing_error["record_id"] == missing["record_id"]
    for name, schema in indexer.SCHEMAS.items():
        assert pq.read_table(next(out.glob(f"*.{name}.parquet"))).schema == schema
    obj = read_rows(out, "objects")[0]
    ref = ObjectRef(
        obj["storage_id"], obj["object_id"], obj["object_path"], obj["object_size"],
        obj["object_version"], obj["validator"], backend=obj["backend"],
        repo_type=obj["repo_type"], validator_kind=obj["validator_kind"],
        validator_strength=obj["validator_strength"],
    )
    with tarfile.open(source, "r:") as tar, source.open("rb") as stream:
        for row in read_rows(out, "samples"):
            key = RecordKey(row["dataset_id"], row["object_id"], row["sample_path"])
            assert row["record_id"] == key.record_id
            for member_path, offset, size in (
                (row["image_path"], row["offset_data"], row["size"]),
                (row["json_path"], row["json_offset_data"], row["json_size"]),
            ):
                if member_path is None:
                    assert offset is None and size is None
                    continue
                member = MemberRef(ref, member_path, offset, size)
                stream.seek(member.offset_data)
                payload = stream.read(member.size)
                assert payload == members[member.path] == tar.extractfile(member.path).read()
                assert hashlib.sha256(payload).digest() == hashlib.sha256(
                    members[member.path]).digest()
    resumed = indexer.scan(source, out, dataset="demo", registry=registry)
    assert (resumed["objects_skipped"], resumed["objects_committed"], resumed["samples"]) == (
        1, 0, samples)


@pytest.mark.parametrize("stage", list("ABCDE"))
def test_crash_recovery(tmp_path, stage):
    source = tmp_path / "a.tar"
    make_tar(source, {"1.jpg": b"x", "1.json": b"{}", "2.jpg": b"y", "2.json": b"{}"})
    out = tmp_path / "out"

    def crash(actual):
        if actual == stage:
            raise RuntimeError(stage)

    with pytest.raises(RuntimeError):
        indexer.scan(source, out, checkpoint=crash)
    partials = list(out.glob("*.parquet.partial"))
    finals = list(out.glob("*.parquet"))
    markers = list(out.glob("*.COMMIT"))
    assert (len(partials), len(finals), len(markers)) == {
        "A": (2, 0, 0), "B": (4, 0, 0), "C": (3, 1, 0), "D": (0, 4, 0), "E": (0, 4, 1),
    }[stage]
    unknown = out / "user.partial"
    unknown.write_bytes(b"keep")
    result = indexer.scan(source, out)
    assert result["samples"] == 2
    assert result["objects_skipped"] == int(stage == "E")
    assert unknown.read_bytes() == b"keep"


@pytest.mark.parametrize("name", [*indexer.SCHEMAS, "COMMIT"])
def test_committed_corruption(tmp_path, name):
    source = tmp_path / "a.tar"
    make_tar(source, {"1.jpg": b"x", "1.json": b"{}"})
    out = tmp_path / "out"
    indexer.scan(source, out)
    fragment = next(out.glob("*.COMMIT" if name == "COMMIT" else f"*.{name}.parquet"))
    fragment.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="CORRUPT_COMMIT"):
        indexer.scan(source, out)
    assert fragment.read_bytes() == b"corrupt"


@pytest.mark.parametrize("mode", ["w:gz", "w:bz2", "w:xz"])
def test_compressed_disguised(tmp_path, mode):
    source = tmp_path / "a.tar"
    with tarfile.open(source, mode):
        pass
    with pytest.raises(indexer.UnsupportedArchiveError):
        indexer.scan(source, tmp_path / "out")


def test_member_offset_re_read_verification():
    class UnstableFile:
        def __init__(self):
            self.reads = 0

        def seek(self, offset):
            assert offset == 512

        def read(self, size):
            assert size == 3
            self.reads += 1
            return b"one" if self.reads == 1 else b"two"

    member = tarfile.TarInfo("1.jpg")
    member.offset_data = 512
    member.size = 3
    archive = SimpleNamespace(fileobj=UnstableFile())
    with pytest.raises(ValueError, match="offset verification failed"):
        indexer._read_member(archive, member, verify_offsets=True)


def test_same_path_different_content_changes_object_and_record_ids(tmp_path):
    source = tmp_path / "same.tar"
    first_out = tmp_path / "first-out"
    second_out = tmp_path / "second-out"
    make_tar(source, {"1.jpg": b"one", "1.json": b"{}"})
    registry = AdapterRegistry()
    registry.register(DatasetAdapter("demo", "A"))
    indexer.scan(source, first_out, dataset="demo", registry=registry)
    make_tar(source, {"1.jpg": b"two", "1.json": b"{}"})
    indexer.scan(source, second_out, dataset="demo", registry=registry)
    first_object = read_rows(first_out, "objects")[0]
    second_object = read_rows(second_out, "objects")[0]
    first_sample = read_rows(first_out, "samples")[0]
    second_sample = read_rows(second_out, "samples")[0]
    assert first_object["object_id"] != second_object["object_id"]
    assert first_sample["record_id"] != second_sample["record_id"]


def test_four_tables_share_object_and_record_references(tmp_path):
    source = tmp_path / "a.tar"
    make_tar(source, {"1.jpg": b"one", "1.json": b'{"tags":["a"]}'})
    output = tmp_path / "out"
    indexer.scan(source, output)
    objects = {(r["dataset_id"], r["object_id"]) for r in read_rows(output, "objects")}
    for table in ("samples", "annotations", "errors"):
        assert all((r["dataset_id"], r["object_id"]) in objects for r in read_rows(output, table))
    samples = {(r["record_id"], r["dataset_id"], r["object_id"], r["sample_path"])
               for r in read_rows(output, "samples")}
    assert all((r["record_id"], r["dataset_id"], r["object_id"], r["sample_path"]) in samples
               for r in read_rows(output, "annotations"))
    annotations = read_rows(output, "annotations")
    assert len({(r["record_id"], r["namespace"], r["origin"])
                for r in annotations}) == len(annotations)
    annotation_fields = {field.name: field for field in indexer.ANNOTATIONS_SCHEMA}
    assert annotation_fields["tags_state"].type == indexer.pa.string()
    assert annotation_fields["tags"].type == indexer.TAG_TYPE
    assert annotations[0]["tags"] == [dict(value="a", category="general")]
    assert {field.name for field in indexer.SAMPLES_SCHEMA} >= {
        "image_format", "width", "height", "has_alpha", "hash_kind", "status"
    }


def test_bounded_cross_batch_writer_staging(tmp_path, monkeypatch):
    source = tmp_path / "a.tar"
    members = {}
    for index in range(indexer.BATCH_SIZE * 2 + 3):
        members[f"{index}.jpg"] = b"x"
        members[f"{index}.json"] = b"{}"
    make_tar(source, members)
    output = tmp_path / "out"
    observed = []
    original = indexer._table_from_rows

    def observe(rows, schema):
        observed.append(len(rows))
        return original(rows, schema)

    monkeypatch.setattr(indexer, "_table_from_rows", observe)
    result = indexer.scan(source, output)
    assert result["samples"] == indexer.BATCH_SIZE * 2 + 3
    assert max(observed) <= indexer.BATCH_SIZE


def test_identity_formula_conflict_uint64(tmp_path, monkeypatch):
    key = RecordKey("数据", "目录/a.tar", "图像/1")
    expected = hashlib.blake2b(
        '["sakurapool-record-v1","数据","目录/a.tar","图像/1"]'.encode(), digest_size=16,
    ).hexdigest()
    assert key.record_id == expected
    seen = {}
    register_identity(seen, key)
    monkeypatch.setattr(RecordKey, "record_id", property(lambda self: expected))
    with pytest.raises(IdentityConflictError):
        register_identity(seen, RecordKey("other", "a.tar", "1"))
    assert seen == {expected: key}
    schema = pa.schema([("offset_data", pa.uint64()), ("size", pa.uint64())])
    row = {"offset_data": 2**63 + 1, "size": 2**63 + 2}
    path = tmp_path / "large.parquet"
    pq.write_table(pa.Table.from_pylist([row], schema), path)
    assert pq.read_table(path).to_pylist() == [row]
    MemberRef(ObjectRef("local", "a.tar", "a.tar", 2**64 - 1, "a" * 64, "a" * 64),
              "1.jpg", offset_data=1, size=2)


def test_optional_metadata_tags_hash_no_decode(tmp_path, monkeypatch):
    source = tmp_path / "a.tar"
    members = {"0.jpg": b"no metadata"}
    for i, meta in enumerate([{}, {"tags": []}, {"tags": ["x"]}, {"tags": None},
                              {"tags": [3]}, {"sha256": "A" * 64}], 1):
        members[f"{i}.jpg"] = b"not decodable"
        members[f"{i}.json"] = json.dumps(meta).encode()
    make_tar(source, members)
    real = indexer._read_member

    def guarded(tar, member):
        assert member.name.endswith((".json", ".jpg"))
        return real(tar, member)

    monkeypatch.setattr(indexer, "_read_member", guarded)
    registry = AdapterRegistry()
    registry.register(DatasetAdapter("d", "s", metadata_required=False))
    out = tmp_path / "out"
    indexer.scan(source, out, dataset="d", registry=registry)
    rows = read_rows(out, "samples")
    assert rows[0]["json_path"] is None and rows[0]["text"] is None
    assert [r["tags_state"] for r in rows] == [
        "missing", "missing", "empty", "known", "invalid", "invalid", "missing"]
    assert rows[-1]["hash_source"] == "declared:json.sha256"
    assert rows[3]["tags"] == [dict(value="x", category="general")]


def test_png_header_dimensions_are_indexed_without_decode(tmp_path):
    source = tmp_path / "a.tar"
    png = bytearray(b"\x89PNG\r\n\x1a\n")
    png += b"\x00\x00\x00\x0dIHDR" + (12).to_bytes(4, "big") + (9).to_bytes(4, "big")
    png += b"\x08\x06" + b"\x00\x00\x00\x00" + b"\x00" * 20
    make_tar(source, {"1.png": bytes(png), "1.json": b'{"width":99,"height":88,"has_alpha":false}'})
    out = tmp_path / "out"
    indexer.scan(source, out)
    row = read_rows(out, "samples")[0]
    assert (row["width"], row["height"], row["has_alpha"]) == (99, 88, False)
    assert "metadata" not in row


def test_tar_member_cache_is_bounded(tmp_path, monkeypatch):
    source = tmp_path / "a.tar"
    make_tar(source, {f"{i}.jpg": b"x" for i in range(2051)})
    observed = []
    real_next = indexer.tarfile.TarFile.next
    def observe(archive):
        member = real_next(archive)
        observed.append(len(archive.members))
        return member
    monkeypatch.setattr(indexer.tarfile.TarFile, "next", observe)
    indexer.scan(source, tmp_path / "out")
    assert max(observed) <= 1


def test_member_lookup_uses_indexed_plan():
    spool = indexer._MemberSpool()
    rows = indexer._SpoolRows()
    try:
        plan = spool.db.execute(
            "EXPLAIN QUERY PLAN SELECT name FROM members "
            "WHERE key=? AND is_image=? ORDER BY name LIMIT 2", ("x", 1)
        ).fetchall()
        detail = " ".join(row[3] for row in plan)
        assert "members_lookup" in detail
        assert "SCAN" not in detail.upper()
        join_plan = rows.db.execute(
            "EXPLAIN QUERY PLAN SELECT a.record_id FROM rows AS a "
            "LEFT JOIN rows AS s ON s.kind='samples' AND s.record_id=a.record_id "
            "WHERE a.kind='annotations' AND s.record_id IS NULL LIMIT 1"
        ).fetchall()
        join_detail = " ".join(row[3] for row in join_plan)
        assert "rows_record_lookup" in join_detail
        assert "payload" not in join_detail
    finally:
        spool.close()
        rows.close()


def test_default_scan_does_not_read_image_payload(tmp_path, monkeypatch):
    source = tmp_path / "a.tar"
    make_tar(source, {"1.jpg": b"payload", "1.json": b'{"width":99,"height":88}'})
    real = indexer._read_member
    def guarded(archive, member, verify_offsets=False):
        assert member.name.endswith(".json")
        return real(archive, member, verify_offsets)
    monkeypatch.setattr(indexer, "_read_member", guarded)
    indexer.scan(source, tmp_path / "out")


def test_resume_verification_does_not_read_full_parquet(tmp_path, monkeypatch):
    source = tmp_path / "a.tar"
    make_tar(source, {f"{i}.jpg": b"x" for i in range(1025)})
    out = tmp_path / "out"
    indexer.scan(source, out)
    monkeypatch.setattr(indexer.pq, "read_table", lambda *args: pytest.fail("full parquet read"))
    assert indexer.scan(source, out)["skipped"] == 1


def test_utf8_error_detail_is_bounded(tmp_path):
    source = tmp_path / "a.tar"
    make_tar(source, {"x.jpg": b"x", "x.json": b"invalid"})
    out = tmp_path / "out"
    original = indexer._truncate_utf8
    indexer._truncate_utf8 = lambda value: original("中" * 5000)
    try:
        indexer.scan(source, out)
    finally:
        indexer._truncate_utf8 = original
    detail = read_rows(out, "errors")[0]["error_detail"]
    assert len(detail.encode("utf-8")) <= 4096
    detail.encode("utf-8").decode("utf-8")


@pytest.mark.parametrize("count", [0, 1, 1023, 1024, 1025, 2051])
def test_all_table_row_boundaries_and_resume(tmp_path, count):
    members = {}
    for index in range(count):
        members[f"{index}.jpg"] = b"x"
        members[f"{index}.json"] = b'{"tags": ["x"]}'
    source = tmp_path / "a.tar"
    make_tar(source, members)
    out = tmp_path / "out"
    result = indexer.scan(source, out)
    assert result["samples"] == count
    assert len(read_rows(out, "samples")) == count
    assert len(read_rows(out, "annotations")) == count
    assert indexer.scan(source, out)["skipped"] == 1


@pytest.mark.parametrize("count", [1025, 2051])
def test_errors_cross_batch_roundtrip_and_resume(tmp_path, count):
    members = {f"{index}.jpg": b"x" for index in range(count)}
    source = tmp_path / "a.tar"
    make_tar(source, members)
    out = tmp_path / "out"
    indexer.scan(source, out)
    errors = read_rows(out, "errors")
    assert len(errors) == count
    assert all(row["post_id"] is not None for row in errors)
    assert len(read_rows(out, "errors")) == count
    assert indexer.scan(source, out)["skipped"] == 1


def test_v4_contract_constants_and_storage_roundtrip():
    readme = Path(__file__).parents[1] / "README.md"
    text = readme.read_text(encoding="utf-8")
    assert f"FORMAT_VERSION={indexer.FORMAT_VERSION}" in text
    assert indexer.BUILDER in text
    assert "P4 MUST NOT use remote full-object SHA rereads" in text


def test_object_ref_roundtrip_and_bounds():
    ref = ObjectRef("modelscope-main", "same.tar@sha256-" + "a" * 64,
                    "same.tar", 100, "a" * 64, "a" * 64)
    payload = {"storage_id": ref.storage_id, "object_id": ref.object_id,
               "object_path": ref.object_path, "object_size": ref.object_size,
               "object_version": ref.object_version, "validator": ref.validator}
    assert ObjectRef(**payload) == ObjectRef(**payload)
    with pytest.raises(ValueError, match="exceeds"):
        MemberRef(ref, "x.jpg", offset_data=99, size=2)


def test_avif_pairing_and_payload_extent(tmp_path):
    source = tmp_path / "a.tar"
    payload = b"avif-not-decoded"
    make_tar(source, {"42.avif": payload, "42.json": b'{"tags":[]}'})
    out = tmp_path / "out"
    indexer.scan(source, out)
    row = read_rows(out, "samples")[0]
    assert row["image_format"] == "avif"
    with source.open("rb") as stream:
        stream.seek(row["offset_data"])
        assert stream.read(row["size"]) == payload


def test_spools_are_removed_after_failure(tmp_path):
    source = tmp_path / "a.tar"
    make_tar(source, {"1.jpg": b"x", "1.json": b"invalid"})
    user_temp = tmp_path / "user.partial"
    user_temp.write_bytes(b"keep")
    def fail(stage):
        raise RuntimeError(stage)
    with pytest.raises(RuntimeError):
        indexer.scan(source, tmp_path / "out", checkpoint=fail)
    assert not indexer._ACTIVE_SPOOLS
    assert user_temp.read_bytes() == b"keep"


def test_storage_profile_is_stable_but_object_id_is_not(tmp_path):
    registry = AdapterRegistry()
    registry.register(DatasetAdapter("one", "A", storage_id="modelscope-main"))
    registry.register(DatasetAdapter("two", "A", storage_id="modelscope-main"))
    first = tmp_path / "one.tar"
    second = tmp_path / "two.tar"
    make_tar(first, {"1.jpg": b"a", "1.json": b"{}"})
    make_tar(second, {"1.jpg": b"b", "1.json": b"{}"})
    first_out, second_out = tmp_path / "one-out", tmp_path / "two-out"
    indexer.scan(first, first_out, dataset="one", registry=registry)
    indexer.scan(second, second_out, dataset="two", registry=registry)
    first_object = read_rows(first_out, "objects")[0]
    second_object = read_rows(second_out, "objects")[0]
    assert first_object["storage_id"] == second_object["storage_id"] == "modelscope-main"
    assert first_object["object_id"] != second_object["object_id"]


def test_input_same_size_mtime_changed(tmp_path):
    source = tmp_path / "a.tar"
    make_tar(source, {"1.jpg": b"one", "1.json": b"{}"})
    out = tmp_path / "out"
    indexer.scan(source, out)
    stat = source.stat()
    source.write_bytes(source.read_bytes().replace(b"one", b"two"))
    os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    with pytest.raises(ValueError, match="input validator mismatch"):
        indexer.scan(source, out)


def test_fresh_process_roundtrip_determinism(tmp_path):
    source = tmp_path / "a.tar"
    stem = "nested/" + "x" * 120
    make_tar(source, {stem + ".jpg": b"x", stem + ".json": b"{}"}, format=tarfile.PAX_FORMAT)
    outputs = [tmp_path / "one", tmp_path / "two"]
    env = dict(os.environ)
    if os.environ.get("SAKURAPOOL_EXPECT_INSTALLED") != "1":
        env["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
    code = (
        "import os,sys,pathlib,sakurapool;"
        "assert os.environ.get('SAKURAPOOL_EXPECT_INSTALLED') != '1' or "
        "str(pathlib.Path(sys.prefix)) in str(pathlib.Path(sakurapool.__file__).resolve());"
        "from pathlib import Path;import pyarrow.parquet as pq;"
        "from sakurapool.indexer import scan,SCHEMAS;"
        "o=Path(sys.argv[2]);r=scan(Path(sys.argv[1]),o);"
        "assert r['samples']==1;"
        "assert all(pq.read_table(next(o.glob('*.'+n+'.parquet'))).schema==s "
        "for n,s in SCHEMAS.items())"
    )
    for out in outputs:
        result = subprocess.run([sys.executable, "-c", code, str(source), str(out)],
                                env=env, capture_output=True)
        assert result.returncode == 0, result.stderr
    for name in indexer.SCHEMAS:
        assert read_rows(outputs[0], name) == read_rows(outputs[1], name)
    assert indexer.scan(source, outputs[0])["objects_skipped"] == 1


def test_cli_config(tmp_path, capsys):
    config = tmp_path / "config.json"
    config.write_text('{"datasets":{"demo":{"source":"canonical"}}}')
    assert main(["config", "validate", "--config", str(config)]) == 0
    assert json.loads(capsys.readouterr().out)["valid"]
    assert main(["index", "scan"]) == 2
