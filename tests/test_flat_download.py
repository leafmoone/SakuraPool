"""Flat v4 contracts and crash safety, exclusively small synthetic/offline data."""
# ruff: noqa: F811

import json
import sqlite3
import threading
from contextlib import contextmanager

import pytest
from test_lightweight_tasks import lightweight  # noqa: F401,F811

from sakurapool.cli import main
from sakurapool.download_naming import FLAT_POLICY, FilenameConfig, stem_key
from sakurapool.storage import flat_delivery
from sakurapool.storage.publication import load_publication
from sakurapool.storage.publication_fetch import fetch_publication_sample
from sakurapool.tasks.export import export_task
from sakurapool.tasks.plan import SelectedRecord, Selection, canonical, plan_digest
from sakurapool.tasks.runner import create_task, reconcile, run_task
from sakurapool.tasks.store import TaskDB, TaskError


def test_naming_defaults_positive_tags_and_unicode():
    assert FilenameConfig.resolve({"all_tags": ["1girl"]}).stem(0) == "1girl_1"
    assert FilenameConfig.resolve({"none_tags": ["unsafe"]}).stem(9) == "image_10"
    query = {"all_tags": ["z", "a"], "any_tags": ["a"],
             "any_of": [{"all_tags": [["ns", "猫/图"]]}]}
    assert FilenameConfig.resolve(query).stem(1) == "a_ns_猫_图_z_2"
    assert FilenameConfig.resolve({"all_tags": ["CON", "a:b"]}).stem(0) == "CON_a_b_1"
    assert FilenameConfig.resolve({"all_tags": ["e\u0301"]}).stem(0) == "é_1"
    assert stem_key("É_1") == stem_key("é_1")
    assert FilenameConfig.resolve(template="{index}_{tag}", prefix="猫").stem(0) == "1_猫"


@pytest.mark.parametrize("template", ["{tag}", "{index}{index}", "{index:04}", "{index!r}",
                                      "{tag.a}_{index}", "{bad}_{index}", "../{index}",
                                      "{tag}_{index}.jpg", "CON", "{index}/x"])
def test_bad_templates(template):
    with pytest.raises(ValueError):
        FilenameConfig.resolve(template=template)


@pytest.mark.parametrize("prefix", ["CON", "NUL.jpg", "COM1", "LPT¹", "a/b", "a\\b", "C:x",
                                    "a..b", "tail.", "tail ", "a\x00b", "{index}", "e\u0301",
                                    "猫" * 81])
def test_bad_prefixes(prefix):
    with pytest.raises(ValueError):
        FilenameConfig.resolve(prefix=prefix)


def test_flat_task_metadata_receipt_export_and_other_output(lightweight):
    env = lightweight
    with create_task(env.publication, env.directory, env.workspace, env.query,
                     metadata=True) as task:
        assert task.version == 4
        assert task.meta("header")["output_policy"] == FLAT_POLICY
        assert task.meta("header")["filename_config"]["index_base"] == 1
    unrelated = env.directory / "output" / "user.txt"
    unrelated.write_bytes(b"preserve")
    result = run_task(env.directory, env.transport(), workers=4, control=object())
    assert result["delivered_verified"] == 4
    output = env.directory / "output"
    assert sorted(p.name for p in output.iterdir()) == sorted(
        [f"1girl_{i}{ext}" for i in range(1, 5) for ext in (".jpg", ".json")] + ["user.txt"]
    )
    before = list(env.calls)
    with TaskDB(env.directory) as task:
        reconcile(task)
        for row in task.db.execute("SELECT * FROM items"):
            receipt = json.loads(row["receipt"])
            assert receipt["stem"] == row["output_stem"]
            assert receipt["layout"] == FLAT_POLICY
            assert len(receipt["receipt"]) == 2
    assert env.calls == before
    manifest = env.directory / "flat.jsonl"
    assert export_task(env.directory, manifest)["exported"] == 4
    rows = [json.loads(line) for line in manifest.read_text().splitlines()]
    assert {p["path"] for r in rows for p in r["files"]} == {
        f"output/1girl_{i}{ext}" for i in range(1, 5) for ext in (".jpg", ".json")
    }
    assert unrelated.read_bytes() == b"preserve"


@pytest.mark.parametrize("stage_state", ["absent", "owned_empty", "owned_unknown",
                                         "replaced_empty", "replaced_unknown"])
def test_done_private_stage_boundary_for_verify_export_and_reconcile(
    lightweight, monkeypatch, stage_state
):
    from pathlib import Path

    from sakurapool.tasks.runner import verify_delivery

    env = lightweight
    with create_task(env.publication, env.directory, env.workspace, env.query,
                     Selection("first", 1), metadata=True):
        pass
    # Leave the original empty stage after normal DONE without forging its receipt identity.
    original_rmdir = Path.rmdir

    def retain_private_stage(path):
        if path.name.startswith(".publication-fetch-"):
            return None
        return original_rmdir(path)

    monkeypatch.setattr(Path, "rmdir", retain_private_stage)
    run_task(env.directory, env.transport(), control=object())
    monkeypatch.setattr(Path, "rmdir", original_rmdir)
    output = env.directory / "output"
    unrelated = output / "other-record.txt"
    unrelated.write_bytes(b"not owned by this operation")
    with TaskDB(env.directory) as task:
        row = task.db.execute("SELECT * FROM items").fetchone()
        assert row["state"] == "DONE" and row["delivery"] == "VERIFIED"
        receipt = json.loads(row["receipt"])
        stage = output / receipt["stage_name"]
        info = stage.stat()
        assert [info.st_dev, info.st_ino] == receipt["stage_identity"]
        assert not list(stage.iterdir())
    if stage_state == "absent":
        stage.rmdir()
    elif stage_state.startswith("replaced"):
        stage.rename(output / "saved-original-stage")
        stage.mkdir()
    if stage_state.endswith("unknown"):
        (stage / "unknown.txt").write_bytes(b"foreign private-stage entry")
    file_bytes = {name: (output / name).read_bytes() for name in receipt["receipt"]}
    before = list(env.calls)
    manifest = env.directory / "completed-stage.jsonl"
    valid = stage_state in ("absent", "owned_empty")
    if valid:
        with TaskDB(env.directory, readonly=True) as task:
            row = task.db.execute("SELECT * FROM items").fetchone()
            assert verify_delivery(task, row)["layout"] == FLAT_POLICY
        assert export_task(env.directory, manifest)["exported"] == 1
        with TaskDB(env.directory) as task:
            reconcile(task)
            assert task.inspect()["delivered_verified"] == 1
    else:
        with TaskDB(env.directory, readonly=True) as task:
            row = task.db.execute("SELECT * FROM items").fetchone()
            with pytest.raises(TaskError, match="OUTPUT_CONFLICT"):
                verify_delivery(task, row)
        with pytest.raises(TaskError, match="OUTPUT_CONFLICT"):
            export_task(env.directory, manifest)
        assert not manifest.exists() or not manifest.read_bytes()
        with TaskDB(env.directory) as task:
            with pytest.raises(TaskError, match="OUTPUT_CONFLICT"):
                reconcile(task)
            assert task.db.execute("SELECT state FROM items").fetchone()[0] == "BLOCKED"
        assert stage.is_dir()
        if stage_state.endswith("unknown"):
            assert (stage / "unknown.txt").read_bytes() == b"foreign private-stage entry"
        if stage_state.startswith("replaced"):
            saved_info = (output / "saved-original-stage").stat()
            assert [saved_info.st_dev, saved_info.st_ino] == receipt["stage_identity"]
    assert env.calls == before
    assert {name: (output / name).read_bytes() for name in receipt["receipt"]} == file_bytes
    assert unrelated.read_bytes() == b"not owned by this operation"


@pytest.mark.parametrize("event", ["PREPARED", "PUBLISH_INTENT", "MEMBER_PUBLISHED",
                                   "SECOND_MEMBER", "PUBLISHED", "DONE"])
def test_two_file_failure_fresh_db_resume_without_network(lightweight, event):
    env = lightweight
    with create_task(env.publication, env.directory, env.workspace, env.query,
                     Selection("first", 1), metadata=True):
        pass

    members = 0

    def fault(name, _item):
        nonlocal members
        if name == "MEMBER_PUBLISHED":
            members += 1
        if name == event or event == "SECOND_MEMBER" and members == 2:
            raise RuntimeError("synthetic interruption")

    with pytest.raises(Exception):
        run_task(env.directory, env.transport(), control=object(), fault_hook=fault)
    calls = list(env.calls)
    with TaskDB(env.directory) as fresh:
        reconcile(fresh)
        row = fresh.db.execute("SELECT * FROM items").fetchone()
        assert row["state"] == "DONE" and row["delivery"] == "VERIFIED"
    assert env.calls == calls
    assert (env.directory / "output" / "1girl_1.jpg").is_file()
    assert (env.directory / "output" / "1girl_1.json").is_file()


def test_crash_between_rename_and_sql_ack_recovers_only_intent(lightweight, monkeypatch):
    env = lightweight
    with create_task(env.publication, env.directory, env.workspace, env.query,
                     Selection("first", 1), metadata=True):
        pass
    original = flat_delivery._publish_directory
    count = 0

    def move_then_crash(source, target):
        nonlocal count
        original(source, target)
        count += 1
        if count == 1:
            raise RuntimeError("after unacknowledged rename")

    monkeypatch.setattr(flat_delivery, "_publish_directory", move_then_crash)
    with pytest.raises(Exception):
        run_task(env.directory, env.transport(), control=object())
    with TaskDB(env.directory) as task:
        row = task.db.execute("SELECT * FROM items").fetchone()
        assert row["phase"] == "PUBLISH_INTENT"
        assert json.loads(row["published_members"]) == []
    monkeypatch.setattr(flat_delivery, "_publish_directory", original)
    before = list(env.calls)
    with TaskDB(env.directory) as task:
        reconcile(task)
        assert task.inspect()["delivered_verified"] == 1
    assert env.calls == before


def test_existing_final_without_intent_never_adopted(lightweight):
    env = lightweight
    with create_task(env.publication, env.directory, env.workspace, env.query,
                     Selection("first", 1)):
        pass

    def stop(name, _item):
        if name == "PREPARED":
            raise RuntimeError("before durable intent")

    with pytest.raises(Exception):
        run_task(env.directory, env.transport(), control=object(), fault_hook=stop)
    with TaskDB(env.directory) as task:
        row = task.db.execute("SELECT * FROM items").fetchone()
        stage = env.directory / "output" / json.loads(row["stage"])["name"]
        final = env.directory / "output" / "1girl_1.jpg"
        (stage / "image.jpg").rename(final)
        saved = final.read_bytes()
        with pytest.raises(TaskError, match="OUTPUT_UNCERTAIN"):
            reconcile(task)
        assert final.read_bytes() == saved and stage.exists()
        assert task.db.execute("SELECT state FROM items").fetchone()[0] == "BLOCKED"


@pytest.mark.parametrize("mutation", ["unknown_final", "unknown_stage",
                                      "replacement_stage", "moved_back"])
def test_partial_unknowns_preserved_blocked(lightweight, mutation):
    env = lightweight
    with create_task(env.publication, env.directory, env.workspace, env.query,
                     Selection("first", 1), metadata=True):
        pass

    def stop(name, _item):
        if name == "MEMBER_PUBLISHED":
            raise RuntimeError("after first file")

    with pytest.raises(Exception):
        run_task(env.directory, env.transport(), control=object(), fault_hook=stop)
    output = env.directory / "output"
    with TaskDB(env.directory) as task:
        row = task.db.execute("SELECT * FROM items").fetchone()
        receipt = json.loads(row["receipt"])
        stage = output / receipt["stage_name"]
        if mutation == "unknown_final":
            survivor = output / "1girl_1.json"
            survivor.write_bytes(b"unknown")
        elif mutation == "unknown_stage":
            survivor = stage / "unknown"
            survivor.write_bytes(b"unknown")
        elif mutation == "replacement_stage":
            stage.rename(output / "saved-owned-stage")
            stage.mkdir()
            survivor = stage / "unknown"
            survivor.write_bytes(b"unknown")
        else:
            survivor = stage / "image.jpg"
            (output / "1girl_1.jpg").rename(survivor)
        saved = survivor.read_bytes()
        before = list(env.calls)
        with pytest.raises(TaskError):
            reconcile(task)
        assert task.db.execute("SELECT state FROM items").fetchone()[0] == "BLOCKED"
        assert survivor.read_bytes() == saved
        assert env.calls == before


def test_publish_race_no_overwrite(lightweight, monkeypatch):
    env = lightweight
    output = env.workspace.root / "direct"
    output.mkdir()
    original = flat_delivery._publish_directory

    def race(source, target):
        target.write_bytes(b"other-writer")
        return original(source, target)

    monkeypatch.setattr(flat_delivery, "_publish_directory", race)
    with load_publication(env.publication, full_verify=True) as pub:
        record = pub.runtime.resolve_one("synthetic", "0", "small").record_id
        with pytest.raises(Exception):
            fetch_publication_sample(pub, record, env.transport(), output, control=object())
    assert (output / "image_1.jpg").read_bytes() == b"other-writer"
    stages = list(output.glob(".publication-fetch-*"))
    assert len(stages) == 1 and (stages[0] / "image.jpg").is_file()


@pytest.mark.parametrize("lightweight", [6], indirect=True)
def test_six_lane_reverse_completion_frozen_names(lightweight):
    env = lightweight
    with create_task(env.publication, env.directory, env.workspace, env.query) as task:
        seq_by_object = {}
        with load_publication(env.publication, full_verify=True) as pub:
            for row in task.db.execute("SELECT * FROM items"):
                loc = pub.runtime.location(row["rid"])
                seq_by_object[pub.runtime.object_ref(loc["object_idx"])["object_path"]] = row["seq"]
    gate = threading.Barrier(6)
    lock = threading.Lock()
    release = [threading.Event() for _ in range(6)]
    release[5].set()
    active = peak = 0
    completion = []

    class Delayed(env.transport):
        def clone(self):
            return Delayed()

        @contextmanager
        def read_range_owned(self, obj, offset, length):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            gate.wait(timeout=10)
            seq = next(value for path, value in seq_by_object.items() if path in obj.url)
            assert release[seq].wait(10)
            try:
                with super().read_range_owned(obj, offset, length) as body:
                    yield body
            finally:
                with lock:
                    active -= 1
                    completion.append(seq)
                    if seq:
                        release[seq - 1].set()

    result = run_task(env.directory, Delayed(), workers=6, control=object())
    assert result["delivered_verified"] == 6 and peak == 6
    with TaskDB(env.directory) as task:
        assert len(task.candidates(limit=12)) == 0
        for row in task.db.execute("SELECT * FROM items"):
            assert row["output_stem"] == f"1girl_{row['seq'] + 1}"
            assert (env.directory / "output" / (row["output_stem"] + ".jpg")).is_file()
    assert completion == [5, 4, 3, 2, 1, 0]


def test_v3_inspect_export_db_readonly_execution_gate(lightweight, capsys):
    env = lightweight
    with create_task(env.publication, env.directory, env.workspace, env.query,
                     Selection("first", 1)):
        pass
    run_task(env.directory, env.transport(), control=object())
    # Synthetic fixture only: represent a previously completed v3 recorddir archive.
    with TaskDB(env.directory) as task:
        row = task.db.execute("SELECT * FROM items").fetchone()
        flat = json.loads(row["receipt"])
        folder = env.directory / "output" / row["record_id"]
        folder.mkdir()
        (env.directory / "output" / "1girl_1.jpg").rename(folder / "image.jpg")
        info = folder.stat()
        legacy = {"task_id": task.meta("task_id"), "plan_digest": None,
                  "operation_id": row["operation_id"],
                  "directory_identity": [info.st_dev, info.st_ino],
                  "receipt": {"image.jpg": flat["receipt"]["1girl_1.jpg"]}}
        header = task.meta("header")
        header["format"] = "sakurapool-task-v3"
        header["output_policy"] = "task-relative-no-overwrite-v1"
        del header["filename_config"], header["filename_digest"]
        digest = plan_digest(header)
        legacy["plan_digest"] = digest
        task.db.execute("UPDATE meta SET value=? WHERE key='header'", (canonical(header).decode(),))
        task.db.execute("UPDATE meta SET value=? WHERE key='plan_digest'",
                        (canonical(digest).decode(),))
        task.db.execute("UPDATE items SET receipt=?", (canonical(legacy).decode(),))
        task.db.execute("PRAGMA user_version=3")
    original = (env.directory / "task.sqlite").read_bytes()
    with TaskDB(env.directory, readonly=True) as task:
        assert task.inspect()["read_only_archive"]
    assert export_task(env.directory, env.directory / "v3.jsonl")["exported"] == 1
    assert (env.directory / "task.sqlite").read_bytes() == original
    for action in ("run", "resume", "pause", "cancel", "update"):
        argv = ["task", action, str(env.directory)]
        if action in ("run", "resume"):
            argv += ["--profile", str(env.directory / "nonexistent-profile.json")]
        if action == "update":
            argv += ["--expected-settings-version", "0", "--image-extensions", ".jpg,.png"]
        assert main(argv) == 2
        assert json.loads(capsys.readouterr().out)["code"] == "LEGACY_TASK_MIGRATION_REQUIRED"
    with pytest.raises(TaskError, match="MIGRATION_REQUIRED"):
        TaskDB(env.directory)
    assert (env.directory / "task.sqlite").read_bytes() == original
    assert not list(env.directory.glob("task.sqlite-*"))


def test_naming_tamper_rejected_before_writable_db(lightweight):
    env = lightweight
    with create_task(env.publication, env.directory, env.workspace, env.query) as task:
        task.db.execute("UPDATE items SET output_stem='other_1' WHERE seq=0")
    before = (env.directory / "task.sqlite").read_bytes()
    with pytest.raises(TaskError, match="FILENAME_CONFIG_INVALID"):
        TaskDB(env.directory)
    assert (env.directory / "task.sqlite").read_bytes() == before


@pytest.mark.parametrize("lightweight", [{"count": 3, "formats": ["jpg", "png", "gif"]}],
                         indirect=True)
def test_true_suffix_mixed_formats_custom_template(lightweight):
    from sakurapool.image_formats import SUPPORTED_IMAGE_EXTENSIONS
    from sakurapool.tasks.runner import delivery_mapping

    env = lightweight
    with create_task(env.publication, env.directory, env.workspace, env.query,
                     filename_template="{index}_{tag}", filename_prefix="batch",
                     metadata=True, image_extensions=SUPPORTED_IMAGE_EXTENSIONS):
        pass
    assert run_task(env.directory, env.transport(), workers=4,
                    control=object())["delivered_verified"] == 3
    with TaskDB(env.directory) as task, load_publication(env.publication, full_verify=True) as pub:
        suffixes = set()
        for row in task.db.execute("SELECT * FROM items"):
            mapping = delivery_mapping(task, row, pub)
            suffixes.add(mapping.image_suffix)
            assert mapping.stem == f"{row['seq'] + 1}_batch"
            assert all((env.directory / "output" / name).is_file() for name in mapping.names)
        assert suffixes == {".jpg", ".png", ".gif"}


def test_freeze_casefold_duplicate_stem_rejected(tmp_path, monkeypatch):
    rows = [SelectedRecord(i, f"{i:032x}", "synthetic", "small", str(i)) for i in range(2)]
    monkeypatch.setattr(FilenameConfig, "stem", lambda _config, seq: "É_1" if seq == 0 else "é_1")
    directory = tmp_path / "duplicate"
    with pytest.raises(TaskError, match="FILENAME_COLLISION_OR_INVALID"):
        TaskDB.create(directory, None, {}, rows, publication_path=tmp_path / "unused")
    assert not (directory / "output").exists()


def test_path_length_fails_freeze(tmp_path):
    directory = tmp_path / ("x" * 160) / "task"
    directory.parent.mkdir()
    with pytest.raises(TaskError, match="FILENAME_COLLISION_OR_INVALID"):
        TaskDB.create(directory, None, {}, [SelectedRecord(0, "a" * 32, "s", "d", "p")],
                      publication_path=tmp_path / "unused")
    assert not (directory / "output").exists()


@pytest.mark.parametrize("lightweight", [6], indirect=True)
def test_six_lane_sql_failure_drains_and_closes_all(lightweight, monkeypatch):
    import time

    env = lightweight
    with create_task(env.publication, env.directory, env.workspace, env.query):
        pass
    gate = threading.Barrier(6)
    closed = []

    class Lane(env.transport):
        def clone(self):
            return Lane()

        def close(self):
            closed.append(id(self))

        @contextmanager
        def read_range_owned(self, obj, offset, length):
            gate.wait(timeout=10)
            with super().read_range_owned(obj, offset, length) as body:
                yield body

    original = TaskDB.event
    once = False
    owner = threading.get_ident()

    def fail_sql(task, operation, name, payload, **kwargs):
        nonlocal once
        assert threading.get_ident() == owner
        if name == "PREPARED" and not once:
            once = True
            task.db.execute("SELECT * FROM nonexistent_table")
        return original(task, operation, name, payload, **kwargs)

    monkeypatch.setattr(TaskDB, "event", fail_sql)
    start = time.monotonic()
    with pytest.raises(Exception):
        run_task(env.directory, Lane(), workers=6, control=object())
    assert time.monotonic() - start < 15
    assert len(set(closed)) == 6 and once


def test_six_lane_memory_cap_rejects_large_chunk_without_clone(tmp_path):
    from types import SimpleNamespace

    from sakurapool.capacity import CapacityConfig
    from sakurapool.tasks.pipeline import run_pipeline

    class Transport:
        def clone(self):
            raise AssertionError("memory admission must precede lane creation")

    with pytest.raises(TaskError, match="DOWNLOAD_MEMORY_LIMIT"):
        run_pipeline(SimpleNamespace(capacity=CapacityConfig(range_chunk_bytes=16 << 20)),
                     object(), Transport(), workers=6, metadata=False)


def test_intent_and_member_sql_failure_do_not_move_or_delete_data(lightweight):
    env = lightweight
    with create_task(env.publication, env.directory, env.workspace, env.query,
                     Selection("first", 1), metadata=True) as task:
        task.db.execute(
            "CREATE TRIGGER reject_intent BEFORE UPDATE OF phase ON items "
            "WHEN NEW.phase='PUBLISH_INTENT' "
            "BEGIN SELECT RAISE(ABORT,'synthetic intent fault'); END"
        )
    with pytest.raises(Exception):
        run_task(env.directory, env.transport(), control=object())
    before = list(env.calls)
    with TaskDB(env.directory) as task:
        row = task.db.execute("SELECT * FROM items").fetchone()
        assert row["phase"] == "PREPARED" and row["receipt"]
        stage = env.directory / "output" / json.loads(row["stage"])["name"]
        assert (stage / "image.jpg").is_file() and (stage / "metadata.json").is_file()
        assert not (env.directory / "output" / "1girl_1.jpg").exists()
        task.db.execute("DROP TRIGGER reject_intent")
        task.db.execute(
            "CREATE TRIGGER reject_member BEFORE UPDATE OF published_members ON items "
            "BEGIN SELECT RAISE(ABORT,'synthetic member fault'); END"
        )
        with pytest.raises(sqlite3.IntegrityError, match="member fault"):
            reconcile(task)
        row = task.db.execute("SELECT * FROM items").fetchone()
        assert row["phase"] == "PUBLISH_INTENT" and json.loads(row["published_members"]) == []
        assert (env.directory / "output" / "1girl_1.jpg").is_file()
        assert (stage / "metadata.json").is_file()
        task.db.execute("DROP TRIGGER reject_member")
        reconcile(task)
        assert task.inspect()["delivered_verified"] == 1
    assert env.calls == before
