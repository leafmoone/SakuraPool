"""Synthetic end-to-end task attempt basis and explicit recovery."""
import sqlite3
from contextlib import contextmanager

import pytest
from test_task_runner import setup  # noqa: F401, F811

from sakurapool.runtime import RuntimeQuerySpec
from sakurapool.storage.budget import Reservation
from sakurapool.storage.production import _CONSERVATIVE_FINALIZED
from sakurapool.storage.transport import RemoteIOError
from sakurapool.tasks.runner import create_task, run_task
from sakurapool.tasks.store import TaskDB, TaskError


def test_confirmed_failure_explicit_resume_one_saved(setup):  # noqa: F811
    pub, directory, ledger, transport_type, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass

    class Transient(transport_type):
        @contextmanager
        def read_range_owned(self, *args):
            lease = ledger.reserve(Reservation(body=6, attempt=True))
            ledger.consume_body(lease, 6)
            ledger.settle(lease)
            error = RemoteIOError("synthetic terminal", code="origin_timeout", phase="origin",
                                  accounting="CONFIRMED")
            error.accounting_basis = "CONSERVATIVE_MAX_CHARGE"
            error.actual_consumption = "UNKNOWN"
            error.accounted = "CONSERVATIVE_MAX"
            error._conservative_finalized = _CONSERVATIVE_FINALIZED
            raise error
            yield  # pragma: no cover

    with pytest.raises(TaskError) as caught:
        run_task(directory, Transient(), control=object())
    assert caught.value.recoverable
    assert caught.value.public_diagnostic()["accounting_basis"] == "CONSERVATIVE_MAX_CHARGE"
    with TaskDB(directory, readonly=True) as task:
        row = task.db.execute("SELECT * FROM attempts").fetchone()
        old_attempt = row["attempt_id"]
        assert row["accounting"] == "CONFIRMED" and row["receipt"] is None
        assert task.meta("accounting_basis:" + old_attempt)["actual_consumption"] == "UNKNOWN"
        assert task.inspect()["unknown_accounting_count"] == 0
    assert ledger.status()["saved_samples"] == 0
    before = ledger.status()
    result = run_task(directory, transport_type(), control=object(), resume=True)
    assert result["delivered_confirmed"] == 1
    assert ledger.status()["saved_samples"] == 1
    assert ledger.status()["body"] >= before["body"] == 6
    with TaskDB(directory, readonly=True) as task:
        assert task.db.execute("SELECT count(*) FROM attempts").fetchone()[0] == 2
        assert task.meta("accounting_basis:" + old_attempt)["accounted"] == "CONSERVATIVE_MAX"


def test_basis_conflict_rolls_back_attempt(setup):  # noqa: F811
    pub, directory, ledger, transport_type, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()) as task:
        item = task.claim()
        key = "accounting_basis:" + item["attempt_id"]
        with task.transaction() as db:
            db.execute("INSERT INTO meta(key,value) VALUES(?,?)", (key, '"existing"'))
        before = tuple(task.db.execute("SELECT * FROM attempts").fetchone())
        with pytest.raises(sqlite3.IntegrityError):
            task.finish_item(item["seq"], state="READY", code="origin_timeout",
                             accounting="CONFIRMED", accounting_basis="CONSERVATIVE_MAX_CHARGE")
        assert tuple(task.db.execute("SELECT * FROM attempts").fetchone()) == before
        assert task.meta(key) == "existing"
        assert task.db.execute("SELECT state FROM items").fetchone()[0] == "IN_PROGRESS"
