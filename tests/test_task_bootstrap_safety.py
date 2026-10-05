"""Bounded, read-only task bootstrap regression tests."""

import json
import sqlite3
from dataclasses import replace

import pytest

from sakurapool.capacity import CapacityConfig, ResourcePolicy
from sakurapool.tasks import store
from sakurapool.tasks.plan import FORMAT, canonical, plan_digest
from sakurapool.tasks.store import TaskDB, TaskError
from sakurapool.workspace import Workspace


def minimal_db(directory, header, version=1):
    directory.mkdir()
    with sqlite3.connect(directory / "task.sqlite") as db:
        db.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        db.execute(f"PRAGMA user_version={version}")
        db.execute("INSERT INTO meta VALUES('header',?)", (header,))
        db.execute("INSERT INTO meta VALUES('plan_digest',?)", ('"unused"',))
    return directory


def test_oversized_db_before_connect(tmp_path, monkeypatch):
    directory = minimal_db(tmp_path / "task", "{}")
    with (directory / "task.sqlite").open("r+b") as stream:
        stream.truncate(store.LEGACY_CAPACITY.task_db_bytes + 1)
    monkeypatch.setattr(store, "_connect", lambda *a, **kw: pytest.fail("opened oversized DB"))
    with pytest.raises(TaskError, match="RESOURCE_BLOCKED"):
        TaskDB(directory)


@pytest.mark.parametrize("suffix", ["-journal", "-wal", "-shm"])
def test_sidecar_before_connect(tmp_path, monkeypatch, suffix):
    directory = minimal_db(tmp_path / "task", "{}")
    with (directory / ("task.sqlite" + suffix)).open("wb") as stream:
        stream.truncate(store.LEGACY_CAPACITY.task_journal_bytes + 1)
    monkeypatch.setattr(store, "_connect", lambda *a, **kw: pytest.fail("opened unsafe sidecar"))
    with pytest.raises(TaskError, match="TASKDB_SIDECAR_CONFLICT"):
        TaskDB(directory)


@pytest.mark.parametrize(
    "header",
    [
        "x" * 65537,
        sqlite3.Binary(b"{}"),
        "{}\x00" + "x" * 65536,
        '"' + "é" * 32768 + '"',
        "[" * 65 + "]" * 65,
    ],
    ids=["oversized", "blob", "nul", "utf8-bytes", "depth"],
)
def test_header_sql_and_depth_gate(tmp_path, monkeypatch, header):
    directory = minimal_db(tmp_path / "task", header)

    def forbidden(*args, **kwargs):
        pytest.fail("untrusted header reached decoder")

    monkeypatch.setattr(store.json, "loads", forbidden)
    with pytest.raises(TaskError, match="TASK_HEADER_LIMIT"):
        TaskDB(directory)


@pytest.mark.parametrize("header", ["[]", "null", "1", '"text"'])
def test_header_must_be_dictionary(tmp_path, header):
    directory = minimal_db(tmp_path / "task", header)
    with pytest.raises(TaskError, match="TASK_HEADER_INVALID"):
        TaskDB(directory)


def test_wrong_workspace_before_sql(tmp_path, monkeypatch):
    ws = Workspace.init(tmp_path / "owned")
    other = Workspace.init(tmp_path / "other")
    directory = minimal_db(ws.tasks / "task", "{}", version=2)
    monkeypatch.setattr(store, "_connect", lambda *a, **kw: pytest.fail("opened wrong binding"))
    with pytest.raises(TaskError, match="TASK_WORKSPACE_CONFLICT"):
        TaskDB(directory, workspace=other)


def test_durable_binding_before_header(tmp_path, monkeypatch):
    ws = Workspace.init(tmp_path / "owned")
    directory = minimal_db(ws.tasks / "task", "x" * 100000, version=2)
    binding = ws.task_binding(directory)
    binding["workspace_id"] = "0" * 32
    with sqlite3.connect(directory / "task.sqlite") as db:
        db.execute("INSERT INTO meta VALUES('workspace_binding',?)", (json.dumps(binding),))
    original = store._bounded_meta

    def checked(db, key, limit, **kwargs):
        assert key != "header", "wrong binding reached header materialization"
        return original(db, key, limit, **kwargs)

    monkeypatch.setattr(store, "_bounded_meta", checked)
    with pytest.raises(TaskError, match="TASK_WORKSPACE_CONFLICT"):
        TaskDB(directory)


def test_frozen_capacity_and_old_v2(tmp_path):
    capacity = replace(CapacityConfig(), task_header_bytes=100000)
    ws = Workspace.init(tmp_path / "owned", capacity=capacity)
    directory = ws.tasks / "task"
    with TaskDB.create(
        directory,
        ws.ledger(),
        {"padding": "x" * 70000},
        [],
        publication_path=tmp_path / "publication",
    ) as task:
        assert task.meta("workspace_binding") == ws.task_binding(directory)
    ws.update_policy(replace(ResourcePolicy(), disk=128 << 20, body=0))
    with TaskDB(directory, readonly=True) as task:
        assert task.capacity == capacity
    with sqlite3.connect(directory / "task.sqlite") as db:
        db.execute("DELETE FROM meta WHERE key='workspace_binding'")
    with TaskDB(directory, readonly=True) as task:
        assert len(task.meta("header")["padding"]) == 70000


def test_legacy_decode_default(tmp_path):
    header = {"format": FORMAT, "padding": "x" * 10000}
    directory = minimal_db(tmp_path / "task", canonical(header).decode())
    with sqlite3.connect(directory / "task.sqlite") as db:
        db.execute(
            "UPDATE meta SET value=? WHERE key='plan_digest'", (json.dumps(plan_digest(header)),)
        )
    with TaskDB(directory, readonly=True) as task:
        assert task.capacity == CapacityConfig()
        assert task.meta("header") == header


def test_header_cannot_raise_its_own_capacity(tmp_path):
    capacity = replace(CapacityConfig(), task_header_bytes=4096)
    ws = Workspace.init(tmp_path / "owned", capacity=capacity)
    header = {
        "format": "sakurapool-task-v2",
        "workspace_binding": ws.task_binding(ws.tasks / "task"),
        "effective_capacity": replace(capacity, task_header_bytes=100000).to_dict(),
        "padding": "x" * 5000,
    }
    directory = minimal_db(ws.tasks / "task", canonical(header).decode(), version=2)
    with pytest.raises(TaskError, match="TASK_HEADER_LIMIT"):
        TaskDB(directory)


def test_short_nul_is_rejected_before_decode(tmp_path, monkeypatch):
    directory = minimal_db(tmp_path / "task", "{}\x00")

    def forbidden(*args, **kwargs):
        pytest.fail("NUL reached decoder")

    monkeypatch.setattr(store.json, "loads", forbidden)
    with pytest.raises(TaskError, match="TASK_HEADER_LIMIT"):
        TaskDB(directory)
