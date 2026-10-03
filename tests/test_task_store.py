"""Durable task state using isolated offline budget and synthetic identities."""

import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from sakurapool.storage.budget import DEFAULT_WORK_ROOT, BudgetLedger
from sakurapool.tasks.plan import SelectedRecord
from sakurapool.tasks.store import TaskDB, TaskError


@pytest.fixture
def task():
    with tempfile.TemporaryDirectory(dir=DEFAULT_WORK_ROOT, prefix="offline-task-store-") as temp:
        ledger = BudgetLedger(temp, _offline_test=True)
        rows = [SelectedRecord(i, f"{i:032x}", "source", "dataset", str(i)) for i in range(5)]
        directory = Path(temp) / "task#中文"
        with TaskDB.create(directory, ledger, {"publication_digest": "a" * 64,
                                             "snapshot_id": "snapshot", "metadata": False},
                           iter(rows), publication_path="publication") as db:
            yield db, ledger, rows


def test_frozen_reopen_and_atomic_rollback(task):
    db, _, rows = task
    header = db.validate_plan()
    assert header["selection_count"] == 5
    task_id = db.meta("task_id")
    with pytest.raises(RuntimeError):
        with db.transaction() as connection:
            db.set_meta(connection, "state", "BROKEN")
            raise RuntimeError("injected")
    assert db.meta("state") == "READY"
    with TaskDB(db.directory, readonly=True) as reopened:
        assert reopened.meta("task_id") == task_id
        assert reopened.validate_plan() == header
        assert [row[0] for row in reopened.db.execute("SELECT rid FROM items ORDER BY seq")] == [
            row.rid for row in rows
        ]
    assert db.db.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    assert db.db.execute("PRAGMA synchronous").fetchone()[0] == 2


def test_control_request_is_not_stopped_claim(task):
    db, _, _ = task
    with db.transaction() as connection:
        db.set_meta(connection, "state", "RUNNING")
    assert db.request("PAUSE") == {"requested": "PAUSE", "state": "RUNNING"}
    assert db.request("CANCEL") == {"requested": "CANCEL", "state": "RUNNING"}


@pytest.mark.parametrize("field,value", [("record_id", "f" * 32), ("source", "foreign"),
                                         ("dataset", "foreign"), ("seq", 999)])
def test_plan_drift_rejected(task, field, value):
    db, _, _ = task
    with db.transaction() as connection:
        connection.execute(f"UPDATE items SET {field}=? WHERE seq=0", (value,))
    with pytest.raises(TaskError, match="PLAN_IDENTITY_MISMATCH"):
        db.validate_plan()


def test_settlement_event_cannot_skip_attempt_order(task):
    db, _, _ = task
    item = db.claim()
    with pytest.raises(TaskError, match="ATTEMPT_EVENT_ORDER_INVALID"):
        db.event(item["attempt_id"], "SETTLED", {"output_lease": "CONFIRMED"})
    assert db.db.execute("SELECT accounting FROM attempts").fetchone()[0] == "UNKNOWN"
    assert db.meta("confirmed_output_bytes") == 0


def test_transaction_rollback_failure_preserves_primary(task):
    db, _, _ = task
    original = db.db
    marker = TaskError("RESOURCE_BLOCKED", "preflight")
    marker.resources = {"required": {"saved_bytes": 5}, "remaining": {"saved_bytes": 4}}

    class BrokenRollback:
        in_transaction = True

        def execute(self, sql, *args):
            if sql == "ROLLBACK":
                raise KeyboardInterrupt()
            return original.execute(sql, *args)

    db.db = BrokenRollback()
    try:
        with pytest.raises(TaskError) as caught:
            with db.transaction():
                raise marker
        assert caught.value is marker
        assert marker.public_diagnostic()["resources"]["required"]["saved_bytes"] == 5
        assert marker.public_diagnostic()["secondary"] == ["TASK_ROLLBACK_FAILED"]
    finally:
        db.db = original
        if original.in_transaction:
            original.execute("ROLLBACK")


def test_create_rollback_and_close_cannot_replace_primary(task, monkeypatch):
    import sakurapool.tasks.store as store

    db, ledger, _ = task
    original = store._connect
    connections = []
    marker = TaskError("PRIMARY_CREATE", "create")

    class BrokenFinalizers:
        def __init__(self, connection):
            self.connection = connection

        @property
        def in_transaction(self):
            return self.connection.in_transaction

        def execute(self, sql, *args):
            if sql == "ROLLBACK":
                raise SystemExit(73)
            return self.connection.execute(sql, *args)

        def executescript(self, sql):
            return self.connection.executescript(sql)

        def close(self):
            self.connection.close()
            raise OSError("SECRET_CLOSE")

    def connect(*args, **kwargs):
        connection = original(*args, **kwargs)
        connections.append(connection)
        return BrokenFinalizers(connection)

    def rows():
        raise marker
        yield

    monkeypatch.setattr(store, "_connect", connect)
    with pytest.raises(TaskError) as caught:
        TaskDB.create(db.directory.parent / "failed-create", ledger, {}, rows(),
                      publication_path="publication")
    assert caught.value is marker
    assert marker.public_diagnostic()["secondary"] == ["TASK_ROLLBACK_FAILED", "TASK_CLOSE_FAILED"]


def test_sqlite_capacity_becomes_resource_blocked(task):
    db, _, _ = task
    page_count = db.db.execute("PRAGMA page_count").fetchone()[0]
    db.db.execute(f"PRAGMA max_page_count={page_count}")
    with pytest.raises(TaskError, match="RESOURCE_BLOCKED"):
        with db.transaction() as connection:
            connection.execute("INSERT INTO meta VALUES('overflow',?)", ("x" * 100000,))
    assert not db.db.in_transaction
    assert db.meta("state") == "READY"


def test_os_lock_conflict_and_release(task):
    db, _, _ = task
    with TaskDB(db.directory) as other:
        with db.runner_lock():
            with pytest.raises(TaskError, match="RUNNER_BUSY"):
                with other.runner_lock():
                    pytest.fail("second owner acquired")
        with other.runner_lock():
            pass


def test_two_real_process_lock_competition_and_release(task):
    db, _, _ = task
    owner_code = """
import sys
from sakurapool.tasks.store import TaskDB
with TaskDB(sys.argv[1]) as task:
    with task.runner_lock():
        print('LOCKED', flush=True)
        sys.stdin.readline()
"""
    contender_code = """
import sys
from sakurapool.tasks.store import TaskDB, TaskError
try:
    with TaskDB(sys.argv[1]) as task:
        with task.runner_lock():
            print('ACQUIRED')
except TaskError as error:
    print(error.code)
    sys.exit(23)
"""
    owner = subprocess.Popen([sys.executable, "-c", owner_code, str(db.directory)],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True)
    try:
        assert owner.stdout.readline().strip() == "LOCKED"
        blocked = subprocess.run([sys.executable, "-c", contender_code, str(db.directory)],
                                 capture_output=True, text=True, timeout=20)
        assert blocked.returncode == 23 and blocked.stdout.strip() == "RUNNER_BUSY"
        owner.communicate("release\n", timeout=20)
        assert owner.returncode == 0
        acquired = subprocess.run([sys.executable, "-c", contender_code, str(db.directory)],
                                  capture_output=True, text=True, timeout=20)
        assert acquired.returncode == 0 and acquired.stdout.strip() == "ACQUIRED"
    finally:
        if owner.poll() is None:
            owner.kill()
            owner.wait(timeout=10)
        for pipe in (owner.stdin, owner.stdout, owner.stderr):
            pipe.close()
