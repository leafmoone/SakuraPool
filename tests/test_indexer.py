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
    ("B", {"3.jpg": b"three"}, 0, ["missing_metadata"]),
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
    for name, schema in indexer.SCHEMAS.items():
        assert pq.read_table(next(out.glob(f"*.{name}.parquet"))).schema == schema
    obj = read_rows(out, "objects")[0]
    ref = ObjectRef(obj["dataset_id"], obj["object_id"], obj["path"], obj["sha256"])
    with tarfile.open(source, "r:") as tar, source.open("rb") as stream:
        for row in read_rows(out, "samples"):
            key = RecordKey(row["dataset_id"], row["object_id"], row["sample_path"])
            assert row["record_id"] == key.record_id
            for member_path, offset, size in (
                (row["image_path"], row["offset_data"], row["size"]),
                (row["json_path"], row["json_offset_data"], row["json_size"]),
            ):
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
        indexer._read_member(archive, member)


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
    MemberRef(ObjectRef("d", "a.tar", "a.tar", "a" * 64), "1.jpg", **row)


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
        assert member.name.endswith(".json")
        return real(tar, member)

    monkeypatch.setattr(indexer, "_read_member", guarded)
    registry = AdapterRegistry()
    registry.register(DatasetAdapter("d", "s", metadata_required=False))
    out = tmp_path / "out"
    indexer.scan(source, out, dataset="d", registry=registry)
    rows = read_rows(out, "samples")
    assert rows[0]["metadata"] is None and rows[0]["json_path"] is None
    assert [r["tags_state"] for r in rows] == [
        "missing", "missing", "empty", "known", "invalid", "invalid", "missing"]
    assert rows[-1]["hash_source"] == "declared:json.sha256"
    assert rows[3]["tags"] == [dict(value="x", namespace="tags",
                                     origin="declared:json.tags", category="general")]


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
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).parents[1] / "src"))
    code = (
        "import sys;from pathlib import Path;import pyarrow.parquet as pq;"
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
