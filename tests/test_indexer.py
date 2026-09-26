"""Synthetic A-H fixtures; no network, image libraries, or production data."""

import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sakurapool import indexer
from sakurapool.cli import main
from sakurapool.indexer import OBJECTS_SCHEMA, SCHEMAS, UnsupportedArchiveError, scan
from sakurapool.registry import AdapterRegistry


def make_tar(path: Path, members: dict[str, bytes] | list[tuple[str, bytes]], **kwargs) -> None:
    with tarfile.open(path, "w", **kwargs) as archive:
        for name, data in members.items() if isinstance(members, dict) else members:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))


def read_rows(output: Path, name: str) -> list[dict]:
    return [
        row
        for p in sorted(output.glob(f"*.{name}.parquet"))
        for row in pq.read_table(p).to_pylist()
    ]


def test_scan_offsets_pairing_states_and_resume(tmp_path: Path) -> None:
    source = tmp_path / "input"
    source.mkdir()
    make_tar(
        source / "a.tar",
        {
            "one.jpg": b"image-one",
            "one.json": b'{"tags":[],"text":"hello"}',
            "two.jpg": b"image-two",
            "two.json": b'{"tags":["x"]}',
            "three.jpg": b"image-three",
        },
    )
    output = tmp_path / "index"
    summary = scan(source, output, hash_images=True)
    assert summary["objects"] == 2
    assert summary["errors"] == 1
    table = pq.read_table(next(output.glob("*.objects.parquet")))
    assert table.schema == OBJECTS_SCHEMA
    assert table["offset_data"].type == pa.uint64()
    assert sorted(table["tags_state"].to_pylist()) == ["empty", "known"]
    for row in table.to_pylist():
        with (source / row["shard"]).open("rb") as stream:
            stream.seek(row["offset_data"])
            payload = stream.read(row["size"])
        assert payload == {"one.jpg": b"image-one", "two.jpg": b"image-two"}[row["image_path"]]
        assert row["sha256"] == hashlib.sha256(payload).hexdigest()
    resumed = scan(source, output, hash_images=True)
    assert (resumed["skipped"], resumed["objects"], resumed["errors"]) == (1, 2, 1)
    for name, schema in SCHEMAS.items():
        assert pq.read_table(next(output.glob(f"*.{name}.parquet"))).schema == schema


@pytest.mark.parametrize(
    "case,members,objects,codes",
    [
        ("A-flat", {"x.jpg": b"not an image", "x.json": b"{}"}, 1, []),
        (
            "B-nested",
            {"a/x.jpg": b"a", "a/x.json": b"{}", "b/x.jpg": b"b", "b/x.json": b"{}"},
            2,
            [],
        ),
        ("C-missing", {"x.jpg": b"x", "y.json": b"{}"}, 0, ["missing_json", "orphan_json"]),
        ("D-invalid", {"x.jpg": b"x", "x.json": b"[]"}, 0, ["invalid_metadata"]),
        (
            "E-ambiguous",
            {"x.jpg": b"x", "x.png": b"x", "x.json": b"{}"},
            0,
            ["ambiguous_pair", "ambiguous_pair"],
        ),
        ("F-source", {"x.jpg": b"x", "x.json": b'{"source":"other"}'}, 0, ["source_mismatch"]),
        (
            "G-duplicate",
            [("x.jpg", b"x"), ("x.jpg", b"y"), ("x.json", b"{}")],
            0,
            ["duplicate_member", "duplicate_member", "orphan_json"],
        ),
        ("H-unsafe", {"../x.jpg": b"x", "/x.json": b"{}"}, 0, ["unsafe_member", "unsafe_member"]),
    ],
)
def test_fixtures_a_h(tmp_path, case, members, objects, codes):
    source = tmp_path / case
    source.mkdir()
    make_tar(source / "a.tar", members)
    out = tmp_path / "out"
    result = scan(source, out)
    assert result["objects"] == objects
    assert sorted(r["code"] for r in read_rows(out, "errors")) == sorted(codes)
    assert len(list(out.glob("*.parquet"))) == 4
    assert len(list(out.glob("*.COMMIT"))) == 1


@pytest.mark.parametrize(
    "mode,extension", [("w:gz", ".tar"), ("w:bz2", ".tar"), ("w:xz", ".tar"), ("w:gz", ".tar.gz")]
)
def test_compressed_tar_rejected(tmp_path, mode, extension):
    source = tmp_path / "input"
    source.mkdir()
    with tarfile.open(source / ("compressed" + extension), mode):
        pass
    with pytest.raises(UnsupportedArchiveError):
        scan(source, tmp_path / "out")


@pytest.mark.parametrize("stage", list("ABCDE"))
def test_crash_recovery(tmp_path, stage):
    source = tmp_path / "input"
    source.mkdir()
    make_tar(source / "a.tar", {"x.jpg": b"x", "x.json": b"{}"})
    output = tmp_path / "out"

    def crash(actual):
        if actual == stage:
            raise RuntimeError(f"crash {stage}")

    with pytest.raises(RuntimeError, match="crash"):
        scan(source, output, checkpoint=crash)
    result = scan(source, output)
    assert result["objects"] == 1
    assert result["skipped"] == (1 if stage in "DE" else 0)
    assert not list(output.glob("*.partial"))
    assert len(list(output.glob("*.parquet"))) == 4


@pytest.mark.parametrize("name", list(SCHEMAS) + ["COMMIT"])
def test_committed_corruption(tmp_path, name):
    source = tmp_path / "input"
    source.mkdir()
    make_tar(source / "a.tar", {"x.jpg": b"x", "x.json": b"{}"})
    output = tmp_path / "out"
    scan(source, output)
    fragment = next(output.glob("*.COMMIT" if name == "COMMIT" else f"*.{name}.parquet"))
    fragment.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="corrupt committed"):
        scan(source, output)


@pytest.mark.parametrize("change", ["content", "touch", "add", "remove", "config"])
def test_input_mismatch(tmp_path, change):
    source = tmp_path / "input"
    source.mkdir()
    archive = source / "a.tar"
    make_tar(archive, {"x.jpg": b"x", "x.json": b"{}"})
    output = tmp_path / "out"
    scan(source, output)
    if change == "content":
        stat = archive.stat()
        data = archive.read_bytes().replace(b"x.jpg", b"y.jpg", 1)
        archive.write_bytes(data)
        os.utime(archive, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    elif change == "touch":
        stat = archive.stat()
        os.utime(archive, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1000000))
    elif change == "add":
        make_tar(source / "b.tar", {})
    elif change == "remove":
        archive.unlink()
    with pytest.raises(ValueError, match="input validator mismatch|no TAR"):
        scan(source, output, hash_images=change == "config")


def test_tags_hash_adapter_and_no_image_read(tmp_path, monkeypatch):
    source = tmp_path / "input"
    source.mkdir()
    metas = [
        {},
        {"labels": []},
        {"labels": ["tag"]},
        {"labels": None},
        {"labels": [3]},
        {"sha256": "A" * 64},
        {"sha256": 123},
    ]
    members = {}
    for i, meta in enumerate(metas):
        members[f"{i}.bin"] = b"invalid image bytes"
        members[f"{i}.json"] = json.dumps(meta).encode()
    make_tar(source / "a.tar", members)
    registry = AdapterRegistry.from_dict(
        {
            "datasets": {
                "demo": {
                    "source": "canonical",
                    "image_extensions": [".bin"],
                    "tags_field": "labels",
                }
            }
        }
    )
    real = indexer._read_member

    def guarded(archive, member):
        assert member.name.endswith(".json"), "default scan read image payload"
        return real(archive, member)

    monkeypatch.setattr(indexer, "_read_member", guarded)
    output = tmp_path / "out"
    scan(source, output, dataset="demo", registry=registry)
    rows = read_rows(output, "objects")
    assert [r["tags_state"] for r in rows[:5]] == [
        "missing",
        "empty",
        "known",
        "invalid",
        "invalid",
    ]
    assert rows[5]["hash_source"] == "declared:json.sha256"
    assert rows[5]["sha256"] == "a" * 64
    assert rows[6]["sha256"] is None
    assert {r["source"] for r in rows} == {"canonical"}
    assert not any(n in sys.modules for n in ("PIL.Image", "cv2", "torch"))


def subprocess_scan(source, output):
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).parents[1] / "src"))
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "sakurapool",
            "index",
            "scan",
            "--input",
            str(source),
            "--output",
            str(output),
        ],
        env=env,
        capture_output=True,
        text=True,
    )


def test_fresh_process_roundtrip_determinism(tmp_path):
    source = tmp_path / "input"
    source.mkdir()
    long_name = "nested/" + "x" * 120
    make_tar(
        source / "a.tar",
        {long_name + ".jpg": b"synthetic", long_name + ".json": b"{}"},
        format=tarfile.PAX_FORMAT,
    )
    outputs = [tmp_path / "first", tmp_path / "second"]
    for output in outputs:
        result = subprocess_scan(source, output)
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)["objects"] == 1
    assert {p.name: p.read_bytes() for p in outputs[0].iterdir()} == {
        p.name: p.read_bytes() for p in outputs[1].iterdir()
    }
    result = subprocess_scan(source, outputs[0])
    assert result.returncode == 0
    assert json.loads(result.stdout)["skipped"] == 1
    row = read_rows(outputs[0], "objects")[0]
    with (source / "a.tar").open("rb") as stream:
        stream.seek(row["offset_data"])
        assert stream.read(row["size"]) == b"synthetic"


def test_cli_config_and_failures(tmp_path, capsys):
    source = tmp_path / "input"
    source.mkdir()
    make_tar(source / "a.tar", {"x.jpg": b"x", "x.json": b"{}"})
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"datasets": {"demo": {"source": "canonical"}}}))
    args = [
        "index",
        "scan",
        "--input",
        str(source),
        "--output",
        str(tmp_path / "out"),
        "--config",
        str(config),
        "--dataset",
        "demo",
    ]
    assert main(args) == 0
    assert json.loads(capsys.readouterr().out)["objects"] == 1
    assert main(args[:-1] + ["unknown"]) == 2
    assert "error" in json.loads(capsys.readouterr().out)
    assert main(["index", "scan"]) == 2
    assert "error" in json.loads(capsys.readouterr().out)


def test_uint64_contract():
    row = dict(
        object_id="x",
        source="local",
        dataset="local",
        shard="a.tar",
        image_path="x",
        offset_data=2**63 + 1,
        size=2**63 + 2,
        tags_state="missing",
        hash_source="missing",
    )
    table = pa.Table.from_pylist([row], schema=OBJECTS_SCHEMA)
    assert table["offset_data"][0].as_py() == 2**63 + 1
