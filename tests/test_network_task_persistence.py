"""TaskDB commit failure must not leak a confirmed/recoverable CLI result."""
from contextlib import contextmanager

import pytest
from test_task_runner import setup  # noqa: F401, F811

from sakurapool.runtime import RuntimeQuerySpec
from sakurapool.storage.production import _CONSERVATIVE_FINALIZED
from sakurapool.storage.transport import RemoteIOError
from sakurapool.tasks.runner import create_task, run_task
from sakurapool.tasks.store import TaskDB, TaskError


def test_taskdb_failure_downgrades_public_diagnostic(setup, monkeypatch):  # noqa: F811
    pub, directory, ledger, transport_type, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass

    class Transient(transport_type):
        @contextmanager
        def read_range_owned(self, *args):
            error = RemoteIOError("synthetic", code="origin_timeout", phase="origin",
                                  accounting="CONFIRMED")
            error.accounting_basis = "CONSERVATIVE_MAX_CHARGE"
            error.actual_consumption = "UNKNOWN"
            error.accounted = "CONSERVATIVE_MAX"
            error._conservative_finalized = _CONSERVATIVE_FINALIZED
            raise error
            yield  # pragma: no cover

    original = TaskDB.finish_item

    def failed(self, *args, **kwargs):
        if kwargs.get("accounting_basis"):
            raise OSError("synthetic TaskDB commit failure")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(TaskDB, "finish_item", failed)
    with pytest.raises(TaskError) as caught:
        run_task(directory, Transient(), control=object())
    error = caught.value
    safe = error.public_diagnostic()
    assert not error.recoverable and not safe["recoverable"]
    assert safe["accounting"] == "UNKNOWN"
    assert "accounting_basis" not in safe
    assert "TASK_STATE_PERSIST_FAILED" in getattr(error, "task_secondary", ())
    assert ledger.status()["saved_samples"] == 0
    with TaskDB(directory, readonly=True) as task:
        assert task.db.execute("SELECT accounting FROM attempts").fetchone()[0] == "UNKNOWN"
