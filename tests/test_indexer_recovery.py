"""Process-death and storage-error regression coverage for shard publication."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from test_indexer import make_tar, read_rows

from sakurapool import indexer


@pytest.mark.parametrize("stage", list("ABCDE"))
def test_process_death(tmp_path, stage):
    source = tmp_path / "input"
    source.mkdir()
    make_tar(source / "a.tar", {"x.jpg": b"x", "x.json": b"{}"})
    output = tmp_path / "out"
    env = dict(os.environ)
    if os.environ.get("SAKURAPOOL_EXPECT_INSTALLED") != "1":
        env["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
    code = (
        "import os,pathlib,sys,sakurapool;"
        "assert os.environ.get('SAKURAPOOL_EXPECT_INSTALLED') != '1' or "
        "str(pathlib.Path(sys.prefix)) in str(pathlib.Path(sakurapool.__file__).resolve());"
        "import os,sys;from pathlib import Path;from sakurapool.indexer import scan;"
        "scan(Path(sys.argv[1]),Path(sys.argv[2]),"
        "checkpoint=lambda s: os._exit(73) if s==sys.argv[3] else None)"
    )
    result = subprocess.run(
        [sys.executable, "-c", code, str(source), str(output), stage],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 73, result.stderr
    assert indexer.scan(source, output)["objects"] == 1
    assert not list(output.glob("*.partial"))
    assert len(read_rows(output, "objects")) == 1


def test_write_failure_is_not_archive_error(tmp_path, monkeypatch):
    source = tmp_path / "input"
    source.mkdir()
    make_tar(source / "a.tar", {"x.jpg": b"x", "x.json": b"{}"})
    output = tmp_path / "out"
    real = indexer.os.fsync
    calls = 0

    def failing(fd):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise OSError("synthetic storage failure")
        real(fd)

    with monkeypatch.context() as patch:
        patch.setattr(indexer.os, "fsync", failing)
        with pytest.raises(OSError, match="synthetic storage failure"):
            indexer.scan(source, output)
    assert not list(output.glob("*.COMMIT"))
    assert indexer.scan(source, output)["objects"] == 1


def test_changed_during_scan_not_committed(tmp_path):
    source = tmp_path / "input"
    source.mkdir()
    path = source / "a.tar"
    make_tar(path, {"x.jpg": b"x", "x.json": b"{}"})
    output = tmp_path / "out"

    def change(stage):
        if stage == "C":
            with path.open("ab") as stream:
                stream.write(b"changed")

    with pytest.raises(ValueError, match="input validator mismatch"):
        indexer.scan(source, output, checkpoint=change)
    assert not list(output.glob("*.COMMIT"))


def test_readable_parquet_tampering_rejected(tmp_path):
    source = tmp_path / "input"
    source.mkdir()
    make_tar(source / "a.tar", {"x.jpg": b"x", "x.json": b'{"text":"original"}'})
    output = tmp_path / "out"
    indexer.scan(source, output)
    fragment = next(output.glob("*.samples.parquet"))
    table = pq.read_table(fragment)
    rows = table.to_pylist()
    rows[0]["text"] = "tampered"
    pq.write_table(indexer.pa.Table.from_pylist(rows, schema=table.schema), fragment)
    with pytest.raises(ValueError, match="output hash mismatch"):
        indexer.scan(source, output)


def test_two_shards_and_empty_annotations(tmp_path):
    source = tmp_path / "input"
    source.mkdir()
    for name in ("a", "b"):
        make_tar(source / f"{name}.tar", {"x.jpg": b"x", "x.json": b"{}"})
    output = tmp_path / "out"
    assert indexer.scan(source, output)["objects"] == 2
    assert len({r["object_id"] for r in read_rows(output, "objects")}) == 2
    annotations = read_rows(output, "annotations")
    assert len(annotations) == 2
    assert {row["tags_state"] for row in annotations} == {"missing"}
    assert indexer.scan(source, output)["skipped"] == 2
    for marker in output.glob("*.COMMIT"):
        commit = json.loads(marker.read_bytes())
        assert set(commit["files"]) == set(indexer.SCHEMAS)
        assert len(commit["input"]["sha256"]) == 64


def test_cli_row_error_nonzero(tmp_path):
    from sakurapool.cli import main

    source = tmp_path / "input"
    source.mkdir()
    make_tar(source / "a.tar", {"x.jpg": b"x"})
    assert main(["index", "scan", str(source), str(tmp_path / "out")]) == 1
