import hashlib
import io
import json
import tarfile
from pathlib import Path

import pytest

from sakurapool import indexer
from sakurapool.local_builder import build_partition
from sakurapool.partition import FORMAT, PartitionManifest, partition_objects
from sakurapool.registry import DatasetAdapter
from sakurapool.runtime.inventory import load_p2_inventory


class FakeWorker:
    calls = 0
    worker_version = "test-worker"

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def request(self, operation, *, budget, payload):
        assert operation == "scan_tar"
        type(self).calls += 1
        path = Path(payload["path"])
        with path.open("rb") as handle:
            whole = hashlib.file_digest(handle, "sha256").hexdigest()
        with tarfile.open(path) as archive:
            members = [{"path": m.name, "kind": "file", "offset": m.offset_data,
                        "size": m.size,
                        "sha256": hashlib.sha256(archive.extractfile(m).read()).hexdigest()}
                       for m in archive if m.isfile()]
        return {"size": path.stat().st_size, "whole_sha256": whole, "members": members}


def fixture(tmp_path, *, provider=True):
    path = tmp_path / "input.tar"
    metadata = {"schema_version": 1, "id": 42, "source": {"dataset": "danbooru"},
                "image": {"width": 12, "height": 34, "format": "gif"},
                "tags": {"general": ["blue"], "artist": ["alice"],
                         "character": [], "copyright": []}, "captions": {"nl2": ""}}
    with tarfile.open(path, "w") as archive:
        for name, data in [("danbooru/42.gif", b"GIF89a-test"),
                           ("danbooru/42.json", json.dumps(metadata).encode())]:
            m = tarfile.TarInfo(name)
            m.size = len(data)
            archive.addfile(m, io.BytesIO(data))
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    obj = {"repository_path": "danbooru/a.tar", "local_path": str(path),
           "declared_size": path.stat().st_size, "provider_sha256": digest if provider else None}
    manifest = PartitionManifest.from_dict({"format": FORMAT, "repository": "ns/repo",
        "dataset": "danbooru_v3", "source": "danbooru", "partition": "part-000000",
        "objects": [obj]})
    adapter = DatasetAdapter(dataset="danbooru_v3", source="danbooru",
        metadata_mode="nested_json_v1", numeric_post_id=True, allowed_provenance=("danbooru",),
        image_extensions=(".jpg", ".jpeg", ".png", ".webp", ".avif", ".gif"))
    return manifest, adapter, path


@pytest.mark.parametrize("provider", [True, False])
def test_single_pass_and_resume_without_tar(tmp_path, monkeypatch, provider):
    manifest, adapter, path = fixture(tmp_path, provider=provider)
    monkeypatch.setattr("sakurapool.local_builder.RustWorker", FakeWorker)
    original = Path.read_bytes

    def no_tar_bytes(self):
        assert self != path, "production Python must not load raw TAR bytes"
        return original(self)

    monkeypatch.setattr(Path, "read_bytes", no_tar_bytes)
    FakeWorker.calls = 0
    kwargs = dict(code_sha="a" * 40)
    output = tmp_path / "durable"
    work = tmp_path / "work"
    worker = tmp_path / "worker.bin"
    worker.write_bytes(b"fixture worker identity")
    first = build_partition(manifest, adapter, worker, work, output, **kwargs)
    assert first["counts"]["samples"] == 1 and first["counts"]["errors"] == 0
    assert first["objects"][0]["SAFE_TO_RELEASE_LOCAL_OBJECT"] is True
    assert FakeWorker.calls == 1
    # Admin release is simulated only in fixture; builder never deletes TAR.
    assert path.exists()
    path.unlink()
    second = build_partition(manifest, adapter, worker, work, output, **kwargs)
    assert second["objects"][0]["reused_commit"] is True
    assert second["objects"][0]["SAFE_TO_RELEASE_LOCAL_OBJECT"] is False
    assert second["objects"][0]["local_content_verification"] == "NOT_AVAILABLE"
    assert FakeWorker.calls == 1
    assert second["counts"] == first["counts"]


def test_uncommitted_object_resume(tmp_path, monkeypatch):
    manifest, adapter, path = fixture(tmp_path)
    monkeypatch.setattr("sakurapool.local_builder.RustWorker", FakeWorker)
    worker = tmp_path / "worker.bin"
    worker.write_bytes(b"fixture worker identity")
    output, work = tmp_path / "durable", tmp_path / "work"

    def interrupt(phase):
        if phase == "C":
            raise RuntimeError("injected crash")

    with pytest.raises(RuntimeError, match="injected"):
        build_partition(manifest, adapter, worker, work, output, code_sha="a" * 40,
                        checkpoint=interrupt)
    result = build_partition(manifest, adapter, worker, work, output, code_sha="a" * 40)
    assert result["counts"]["samples"] == 1
    assert len(load_p2_inventory(output).objects) == 1
    (output / "unknown.txt").write_text("not ours")
    with pytest.raises(ValueError, match="unknown/unsafe"):
        build_partition(manifest, adapter, worker, work, output, code_sha="a" * 40)
    assert (output / "unknown.txt").exists()
    assert path.exists()


def test_provider_hash_mismatch(tmp_path, monkeypatch):
    manifest, adapter, path = fixture(tmp_path)
    value = manifest.to_dict()
    value["objects"][0]["provider_sha256"] = "f" * 64
    manifest = PartitionManifest.from_dict(value)
    monkeypatch.setattr("sakurapool.local_builder.RustWorker", FakeWorker)
    with pytest.raises(ValueError, match="provider validator"):
        build_partition(manifest, adapter, path, tmp_path / "work", tmp_path / "durable",
                        code_sha="a" * 40)
    assert not list((tmp_path / "durable").glob("*.COMMIT"))


def test_stable_32_tar_partitioning_and_identity(tmp_path):
    objects = [{"repository_path": f"danbooru/{i:04d}.tar",
                "local_path": str(tmp_path / f"{i}.tar"),
                "declared_size": 1024, "provider_sha256": "a" * 64}
               for i in reversed(range(65))]
    partitions = partition_objects("ns/repo", "danbooru_v3", "danbooru", objects)
    assert [len(p.objects) for p in partitions] == [32, 32, 1]
    assert [p.partition for p in partitions] == ["part-000000", "part-000001", "part-000002"]
    assert "local_path" not in json.dumps(partitions[0].durable_plan())
    with pytest.raises(ValueError, match="exactly one"):
        partition_objects("ns/repo", "danbooru_v3", "danbooru", objects + objects[:1])
    assert indexer.FORMAT_VERSION


@pytest.fixture
def builder_case(tmp_path, monkeypatch):
    manifest, adapter, path = fixture(tmp_path)
    monkeypatch.setattr("sakurapool.local_builder.RustWorker", FakeWorker)
    FakeWorker.calls = 0
    worker = tmp_path / "worker.bin"
    worker.write_bytes(b"fixture worker identity")
    output, work = tmp_path / "durable", tmp_path / "work"

    def run(**kwargs):
        return build_partition(manifest, adapter, worker, work, output,
                               code_sha="a" * 40, **kwargs)

    return run, manifest, adapter, path, worker, output, work


def test_crash_during_atomic_commit_recovers_exact_partial(builder_case, monkeypatch):
    run, manifest, adapter, path, worker, output, work = builder_case
    original = indexer.os.replace

    def crash(source, destination):
        if Path(destination).suffix == ".COMMIT":
            raise RuntimeError("COMMIT rename interrupted")
        return original(source, destination)

    with monkeypatch.context() as patch:
        patch.setattr(indexer.os, "replace", crash)
        with pytest.raises(RuntimeError, match="COMMIT rename"):
            run()
    partial = next(output.glob("*.COMMIT.partial"))
    assert partial.exists() and not list(output.glob("*.COMMIT"))
    # Even a torn marker is discarded, never promoted to COMMIT.
    partial.write_bytes(b"{torn")
    assert run()["counts"]["samples"] == 1
    assert FakeWorker.calls == 2 and not partial.exists()
    unknown = output / ("f" * 64 + ".COMMIT.partial")
    unknown.write_bytes(b"not owned")
    with pytest.raises(ValueError, match="unknown/unsafe"):
        run()
    assert unknown.read_bytes() == b"not owned"
    assert path.exists()


def test_destination_lock_is_shared_across_workdirs(builder_case):
    run, manifest, adapter, path, worker, output, work = builder_case
    other = work.with_name("other-work")
    checked = []

    def concurrent(phase):
        if phase == "COMMIT":
            with pytest.raises(OSError):
                build_partition(manifest, adapter, worker, other, output, code_sha="a" * 40)
            checked.append(True)

    run(checkpoint=concurrent)
    assert checked == [True]
    lock = output.parent / ("." + output.name + ".local-builder.lock")
    assert lock.is_file() and lock.parent != work and lock.parent != output
    # Persistent lock files do not represent live owners after OS lock release.
    assert run()["objects"][0]["reused_commit"]


@pytest.mark.parametrize("changed", [False, True])
def test_reused_tar_never_authorizes_unverified_release(builder_case, monkeypatch, changed):
    run, manifest, adapter, path, worker, output, work = builder_case
    first = run()["objects"][0]
    if changed:
        with path.open("r+b") as handle:
            handle.seek(512)
            handle.write(b"changed image bytes")
    original_open = Path.open

    def no_tar_open(self, *args, **kwargs):
        assert self != path, "resume must not reread any TAR bytes"
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", no_tar_open)
    receipt = run()["objects"][0]
    assert FakeWorker.calls == 1
    assert receipt["SAFE_TO_RELEASE_LOCAL_OBJECT"] is False
    assert receipt["local_content_verification"] == "UNKNOWN"
    assert receipt["content_sha256"] == first["content_sha256"]
    assert receipt["content_sha256_scope"] == "committed_object"


def test_local_staged_object_storage_identity(builder_case):
    import dataclasses

    import pyarrow.parquet as pq

    run, manifest, adapter, path, worker, output, work = builder_case
    adapter = dataclasses.replace(adapter, storage_id="my-local-storage")
    build_partition(manifest, adapter, worker, work, output, code_sha="a" * 40)
    row = pq.read_table(next(output.glob("*.objects.parquet"))).to_pylist()[0]
    assert (row["storage_id"], row["backend"], row["repo_type"]) == (
        "my-local-storage", "local", "local")


def test_repeated_stage_interruptions_are_bounded_and_resume_cleans(builder_case):
    run, manifest, adapter, path, worker, output, work = builder_case

    def interrupt(phase):
        if phase == "C":
            raise RuntimeError("stage interruption")

    stage_names = []
    for _ in range(3):
        with pytest.raises(RuntimeError, match="stage interruption"):
            run(checkpoint=interrupt)
        stages = list(work.glob("object-temporary-*"))
        assert len(stages) == 1
        stage_names.append(stages[0].name)
    assert len(set(stage_names)) == 1
    # SQLite rollback journals are exact owned artifacts after a hard crash.
    (stages[0] / "members.sqlite-journal").write_bytes(b"crash journal")

    def commit_interrupt(phase):
        if phase == "COMMIT":
            raise RuntimeError("committed interruption")

    with pytest.raises(RuntimeError, match="committed interruption"):
        run(checkpoint=commit_interrupt)
    assert list(work.glob("object-temporary-*"))
    calls = FakeWorker.calls
    assert run()["objects"][0]["reused_commit"] is True
    assert FakeWorker.calls == calls
    assert not list(work.glob("object-temporary-*"))


def test_unknown_stage_files_are_retained(builder_case):
    run, manifest, adapter, path, worker, output, work = builder_case

    def interrupt(phase):
        if phase == "C":
            raise RuntimeError("interruption")

    with pytest.raises(RuntimeError):
        run(checkpoint=interrupt)
    stage = next(work.glob("object-temporary-*"))
    unknown = stage / "user-file.txt"
    unknown.write_bytes(b"preserve me")
    before = {p.name for p in stage.iterdir()}
    with pytest.raises(ValueError, match="unknown/unsafe temporary"):
        run()
    assert {p.name for p in stage.iterdir()} == before
    assert unknown.read_bytes() == b"preserve me"


def test_new_scan_changed_before_receipt_is_not_safe(builder_case):
    run, manifest, adapter, path, worker, output, work = builder_case

    def change(phase):
        if phase == "COMMIT":
            with path.open("r+b") as handle:
                handle.seek(512)
                handle.write(b"changed after COMMIT")

    receipt = run(checkpoint=change)["objects"][0]
    assert not receipt["SAFE_TO_RELEASE_LOCAL_OBJECT"]
    assert receipt["local_content_verification"] == "UNKNOWN"


@pytest.mark.parametrize("phase", ["C", "COMMIT"])
def test_unknown_stage_blocks_incomplete_and_committed_resume(builder_case, phase):
    run, manifest, adapter, path, worker, output, work = builder_case

    def interrupt(current):
        if current == phase:
            raise RuntimeError("interrupted")

    with pytest.raises(RuntimeError):
        run(checkpoint=interrupt)
    stage = next(work.glob("object-temporary-*"))
    unknown = stage / "unknown.partial"
    unknown.write_bytes(b"preserve")
    with pytest.raises(ValueError, match="unknown/unsafe temporary"):
        run()
    assert unknown.read_bytes() == b"preserve"


def test_lock_symlink_is_rejected(builder_case):
    run, manifest, adapter, path, worker, output, work = builder_case
    target = output.parent / "user-lock-target"
    target.write_bytes(b"user data")
    lock = output.parent / ("." + output.name + ".local-builder.lock")
    try:
        lock.symlink_to(target)
    except OSError as error:
        pytest.skip(f"symlink creation unavailable: {error}")
    with pytest.raises(ValueError, match="symlink/junction"):
        run()
    assert target.read_bytes() == b"user data"
