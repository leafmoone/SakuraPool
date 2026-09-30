"""Integrated v2 semantics and shared stages; synthetic local/loopback bytes only."""

import hashlib
import io
import json
import sqlite3
import tarfile
from contextlib import closing
from dataclasses import replace
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from test_local_partition_builder import FakeWorker, fixture
from test_partition_inventory import make
from test_r2_production import REV
from test_r2_production import twohop as _r2_twohop

from sakurapool import indexer
from sakurapool.local_builder import build_partition
from sakurapool.partition import FORMAT, PartitionManifest
from sakurapool.registry import DatasetAdapter
from sakurapool.runtime.compiler import compile_runtime
from sakurapool.runtime.errors import AmbiguousRecordError, CorruptInputError
from sakurapool.runtime.inventory import load_p2_inventory
from sakurapool.runtime.snapshot import RuntimeSnapshot
from sakurapool.storage.modelscope import ModelScopeDataset
from sakurapool.storage.remote_index import write_staged_v4
from sakurapool.storage.rust_bridge import RustWorker
from sakurapool.storage.transport import BoundObject

# Reuse the same fixed-root fixture; no separate transport implementation.
twohop = _r2_twohop


@pytest.mark.parametrize(
    "field,value,match",
    [
        ("declared_size", 1024, "validator mismatch"),
        ("provider_sha256", "f" * 64, "validator mismatch"),
        ("provider_sha256", None, "validator mismatch"),
        ("local_path", "hidden-machine-path.tar", "local_path"),
    ],
)
def test_partition_provenance_bound_to_committed_input(tmp_path, monkeypatch, field, value, match):
    manifest, adapter, _ = fixture(tmp_path)
    monkeypatch.setattr("sakurapool.local_builder.RustWorker", FakeWorker)
    worker = tmp_path / "worker.bin"
    worker.write_bytes(b"synthetic worker")
    output = tmp_path / "p2"
    build_partition(manifest, adapter, worker, tmp_path / "work", output, code_sha="a" * 40)
    contract = json.loads((output / "INPUT.json").read_bytes())
    contract["partition_manifest"]["objects"][0][field] = value
    packed = indexer._json(contract)
    (output / "INPUT.json").write_bytes(packed)
    # Re-sign enclosing contract: exercise semantic admission, not stale hash rejection.
    for marker in output.glob("*.COMMIT"):
        commit = json.loads(marker.read_bytes())
        commit["contract_sha256"] = hashlib.sha256(packed).hexdigest()
        marker.write_bytes(indexer._json(commit))
    with pytest.raises(CorruptInputError, match=match):
        load_p2_inventory(output)


def test_same_dataset_multipart_runtime_keeps_duplicate_posts(tmp_path):
    a = make(tmp_path / "part-000000", "danbooru/a.tar")
    b = make(tmp_path / "part-000001", "danbooru/b.tar")
    snapshots = []
    for i, roots in enumerate(([a.root, b.root], [b.root, a.root])):
        result = compile_runtime(load_p2_inventory(roots), tmp_path / f"runtime-{i}")
        with RuntimeSnapshot.open(result.path, full_verify=True) as rt:
            assert rt.manifest["runtime_format_version"] == 2
            assert rt.query(namespace="tags", all_tags=["blue"]).count() == 2
            rids = rt.lookup_rids("danbooru", "42", "danbooru_v3")
            assert len(rids) == 2
            with pytest.raises(AmbiguousRecordError):
                rt.resolve_one("danbooru", "42", "danbooru_v3")
            assert {
                rt.object_ref(rt.location(rid)["object_idx"])["object_path"] for rid in rids
            } == {"danbooru/a.tar", "danbooru/b.tar"}
            snapshots.append(result.snapshot_id)
    assert snapshots[0] == snapshots[1]


@pytest.mark.parametrize(
    "source,value,category",
    [
        ("gamecg_native", "sano_toshihide", "artist"),
        ("zerochan_native", "tiger", "character"),
    ],
)
def test_local_remote_stage_equivalence_v2(twohop, tmp_path, monkeypatch, source, value, category):
    state, ledger, transport, original_obj = twohop
    metadata = {
        "schema_version": 1,
        "id": 42,
        "source": {"dataset": source},
        "image": {"width": 12, "height": 34, "format": "gif"},
        "captions": {"nl2": "synthetic caption"},
        "tags": {"general": [value], "artist": [], "character": [], "copyright": []},
    }
    metadata["tags"][category] = [value]
    members = [
        ("nested/42.gif", b"GIF89a-synthetic"),
        ("nested/42.json", json.dumps(metadata).encode()),
    ]
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        for name, data in members:
            entry = tarfile.TarInfo(name)
            entry.size = len(data)
            tar.addfile(entry, io.BytesIO(data))
    state["raw"] = archive.getvalue()
    obj = replace(original_obj, object_size=len(state["raw"]))
    path = tmp_path / "already-downloaded.tar"
    path.write_bytes(state["raw"])
    digest = hashlib.sha256(state["raw"]).hexdigest()
    adapter = DatasetAdapter(
        "danbooru_v3",
        source,
        storage_id="same-semantic-storage",
        metadata_mode="nested_json_v1",
        numeric_post_id=True,
        allowed_provenance=(source,),
        image_extensions=(".gif", ".avif"),
    )
    manifest = PartitionManifest.from_dict(
        {
            "format": FORMAT,
            "repository": obj.repo_id,
            "dataset": adapter.dataset,
            "source": source,
            "partition": "part-000000",
            "objects": [
                {
                    "repository_path": obj.object_path,
                    "local_path": str(path),
                    "declared_size": len(state["raw"]),
                    "provider_sha256": digest,
                }
            ],
        }
    )
    captured = {}

    class RecordingWorker(RustWorker):
        def request(self, *args, **kwargs):
            result = super().request(*args, **kwargs)
            captured["scan"] = result
            captured["calls"] = captured.get("calls", 0) + 1
            return result

    import sakurapool.local_builder as builder

    original_stage = builder.build_stage_from_file_scan

    def capture_stage(*args, **kwargs):
        stage = original_stage(*args, **kwargs)
        with closing(sqlite3.connect(stage.database)) as db:
            captured["rows"] = db.execute("select * from members order by name").fetchall()
        return stage

    original_read = Path.read_bytes

    def no_raw_tar_buffer(self):
        assert self != path, "Python must not materialize local TAR"
        return original_read(self)

    with monkeypatch.context() as patch:
        patch.setattr(builder, "RustWorker", RecordingWorker)
        patch.setattr(builder, "build_stage_from_file_scan", capture_stage)
        patch.setattr(Path, "read_bytes", no_raw_tar_buffer)
        receipt = build_partition(
            manifest,
            adapter,
            transport.worker,
            tmp_path / "work",
            tmp_path / "local-p2",
            code_sha="a" * 40,
        )
    assert captured["calls"] == 1
    assert captured["scan"]["whole_sha256"] == digest
    assert receipt["build_metadata"]["runtime_format_version"] == 2
    bound = transport.verify_conditions(obj)
    remote = transport.build_stage(
        bound, adapter, ledger.root / "remote-stage", mode="remote-stream-scan"
    )
    with closing(sqlite3.connect(remote.database)) as db:
        assert db.execute("select * from members order by name").fetchall() == captured["rows"]
    assert remote.content_sha256 == digest
    provider = ModelScopeDataset(transport, obj.origin, obj.repo_id)
    resolved = BoundObject(
        provider.download_url(REV, obj.object_path),
        obj.object_size,
        REV,
        bound.validator,
        repository=obj.repo_id,
    )
    remote_p2 = ledger.root / "remote-p2"
    summary = write_staged_v4(ledger, [(obj.object_path, resolved, remote)], remote_p2, adapter)
    assert summary["samples"] == 1 and summary["errors"] == 0
    for table in indexer.SCHEMAS:
        local_rows = pq.read_table(
            next((tmp_path / "local-p2").glob(f"*.{table}.parquet"))
        ).to_pylist()
        remote_rows = pq.read_table(next(remote_p2.glob(f"*.{table}.parquet"))).to_pylist()
        if table == "objects":
            assert local_rows[0]["backend"] == "local"
            assert remote_rows[0]["backend"] == "modelscope"
            for rows in (local_rows, remote_rows):
                for row in rows:
                    row.pop("backend")
                    row.pop("repo_type")
        assert local_rows == remote_rows
    runtime = compile_runtime(load_p2_inventory(tmp_path / "local-p2"), tmp_path / "runtime")
    with RuntimeSnapshot.open(runtime.path, full_verify=True) as rt:
        assert rt.manifest["runtime_format_version"] == 2
        assert rt.manifest["compiler"] == "sakurapool-p3-v2"
        assert rt.tag_categories("tags", value) == tuple(sorted((category, "general")))
        assert rt.query(namespace="tags", all_tags=[value]).count() == 1
        record = rt.resolve_one(source, "42", "danbooru_v3")
        location = rt.location(record.rid)
        ref = rt.object_ref(location["object_idx"])
        assert ref["object_path"] == obj.object_path
        assert ref["backend"] == "local"
        for role, body in (("image", members[0][1]), ("metadata", members[1][1])):
            start, size = location[f"{role}_offset"], location[f"{role}_size"]
            with path.open("rb") as handle:
                handle.seek(start)
                actual = handle.read(size)
            assert actual == body
            assert hashlib.sha256(actual).hexdigest() == hashlib.sha256(body).hexdigest()
