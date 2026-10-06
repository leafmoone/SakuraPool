"""Focused local capacity and workspace regression checks."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from sakurapool.capacity import CapacityConfig
from sakurapool.cli import main
from sakurapool.tasks.context import chunk_plan
from sakurapool.tasks.plan import SelectedRecord, Selection, selected_records
from sakurapool.tasks.store import TaskDB, TaskError
from sakurapool.workspace import Workspace


def test_workspace_reopen_conflict_and_frozen_capacity(tmp_path, monkeypatch):
    capacity = replace(
        CapacityConfig(),
        freeze_count=12,
        sample_heap_count=8,
        record_batch=2,
        task_db_bytes=1 << 20,
        task_journal_bytes=2 << 20,
    )
    ws = Workspace.init(tmp_path / "owned", capacity=capacity)
    rows = [SelectedRecord(i, f"{i:032x}", "source", "dataset", str(i)) for i in range(3)]
    directory = ws.tasks / "task"
    with TaskDB.create(
        directory, ws, {"metadata": False}, rows, publication_path=tmp_path / "publication"
    ) as task:
        assert task.version == 4
        assert task.meta("header")["effective_capacity"] == capacity.to_dict()
        item = task.claim()
        task.event(item["operation_id"], "NETWORK_START", {})
    monkeypatch.chdir(tmp_path)
    with TaskDB(directory, readonly=True) as task:
        assert task.workspace.identity == ws.identity
        assert task.capacity == capacity
        assert task.db.execute("SELECT phase FROM items").fetchone()[0] == "NETWORK_START"
        assert not list(ws.state.iterdir())
    other = Workspace.init(tmp_path / "other")
    before = (directory / "task.sqlite").read_bytes()
    with pytest.raises(TaskError, match="TASK_WORKSPACE_CONFLICT"):
        TaskDB(directory, workspace=other)
    assert (directory / "task.sqlite").read_bytes() == before


def test_selection_and_chunk_arithmetic():
    capacity = replace(CapacityConfig(), freeze_count=20, sample_heap_count=12, range_chunk_bytes=3)
    Selection("sample", 12, "seed").validate(capacity)
    with pytest.raises(ValueError):
        Selection("sample", 13, "seed").validate(capacity)
    assert chunk_plan(10, 4, capacity) == (6, 3)


def test_heap_memory_is_an_actual_admission_bound():
    from sakurapool.runtime import RuntimeQuerySpec

    batch = SimpleNamespace(
        rid=[0], record_id=["0" * 32], source_name=["s" * 1000], dataset_name=["d"], post_id=["0"]
    )
    runtime = SimpleNamespace(
        query=lambda query: SimpleNamespace(iter_record_batches=lambda size: iter([batch]))
    )
    with pytest.raises(ValueError, match="heap memory"):
        list(
            selected_records(
                runtime,
                RuntimeQuerySpec(),
                Selection("sample", 1, "seed"),
                replace(CapacityConfig(), heap_memory_bytes=100),
            )
        )


def test_doctor_does_not_read_credentials(tmp_path, capsys):
    import json

    ws = Workspace.init(tmp_path / "workspace")
    worker = tmp_path / "worker"
    worker.write_bytes(b"not executed")
    profile = tmp_path / "profile.json"
    profile.write_text(
        json.dumps(
            {
                "format": "sakurapool-task-connection-v1",
                "origin": "https://modelscope.cn",
                "repositories": ["owner/repo"],
                "worker": str(worker),
                "credential_ref": {"file": str(tmp_path / "missing-token")},
            }
        )
    )
    assert main(["doctor", "--workspace", str(ws.root), "--profile", str(profile)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["credentials"] == "NOT_READ"
    assert result["network"] == "NOT_STARTED"
