"""Global legacy limits remain authoritative independent of task limits."""

import pytest
from test_task_runner import setup as runner_setup

from sakurapool.runtime import RuntimeQuerySpec
from sakurapool.storage.budget import LIMITS, Reservation
from sakurapool.tasks.runner import create_task, run_task
from sakurapool.tasks.store import TaskError


@pytest.fixture
def setup(tmp_path, monkeypatch):
    yield from runner_setup.__wrapped__(tmp_path, monkeypatch)


def test_prepared_warm_preflight_real_ledger_range_only_credit(setup):
    from sakurapool.storage.prepared_fetch import PreparedFetch
    from sakurapool.storage.publication import load_publication
    from sakurapool.tasks.runner import preflight
    from sakurapool.tasks.store import TaskDB

    pub, directory, ledger, Transport, _ = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    with TaskDB(directory) as task, load_publication(pub, full_verify=True) as publication:
        item = task._pipeline_candidate()
        prepared = PreparedFetch._prepare(publication, item["record_id"])
        length = prepared.location["image_size"]
        # Tighten this real synthetic ledger: exactly next Range's two hops,
        # and insufficient mandatory cold metadata/probe topology.
        status = ledger.status()
        ledger.limits["attempts"] = status["attempts"] + 2
        ledger.limits["body"] = status["body"] + length + 1
        transport = Transport()
        assert preflight(task, publication, item, transport, prepared=prepared,
                         proof_warm=True) == directory / "output"
        with pytest.raises(TaskError, match="RESOURCE_BLOCKED"):
            preflight(task, publication, item, transport, prepared=prepared,
                      proof_warm=False)
        lease = ledger.reserve(Reservation(body=length + 1, attempt=True))
        second = ledger.reserve(Reservation(attempt=True))
        ledger.consume_body(lease, length)
        ledger.settle(lease)
        ledger.settle(second)
        assert ledger.status()["attempts"] == status["attempts"] + 2


def test_task_growth_cleanup_preserves_primary_and_pending(setup, monkeypatch):
    from sakurapool.tasks.runner import admit_task_growth

    pub, directory, ledger, _, _ = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    marker = TaskError("PRIMARY_SENTINEL", "preflight")
    monkeypatch.setattr(ledger, "settle", lambda *args, **kwargs: (_ for _ in ()).throw(
        OSError("SECRET_SECONDARY")))
    with pytest.raises(TaskError) as caught:
        with admit_task_growth(directory, ledger):
            raise marker
    assert caught.value is marker
    diagnostic = marker.public_diagnostic()
    assert diagnostic["secondary"] == ["TASK_RESOURCE_SETTLEMENT_UNKNOWN"]
    assert "SECRET" not in str(diagnostic)
    with ledger._locked():
        _, (_, _, pending, _) = ledger._read_pair()
    assert any(row["disk"] > 0 for row in pending.values())


def test_state_persist_secondary_does_not_replace_primary(setup, monkeypatch):
    from sakurapool.tasks.store import TaskDB

    pub, directory, ledger, transport, _ = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    marker = TaskError("PIN_FAILURE", "plan")
    original = TaskDB.set_meta

    def update(task, db, key, value):
        if key == "state" and value == "BLOCKED":
            raise OSError("SECRET_STATE_WRITE")
        return original(task, db, key, value)

    def crash(event, payload):
        if event == "CLAIMED":
            raise marker

    monkeypatch.setattr(TaskDB, "set_meta", update)
    with pytest.raises(TaskError) as caught:
        run_task(directory, transport(), control=object(), fault_hook=crash)
    assert caught.value is marker
    assert marker.public_diagnostic()["secondary"] == ["TASK_STATE_PERSIST_FAILED"]


def test_preflight_ready_write_failure_preserves_resource_diagnostic(setup, monkeypatch):
    from sakurapool.tasks.store import TaskDB

    pub, directory, ledger, transport, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec(), max_output_bytes=4):
        pass
    monkeypatch.setattr(TaskDB, "finish_item", lambda *args, **kwargs: (_ for _ in ()).throw(
        OSError("SECRET_READY_WRITE")))
    with pytest.raises(TaskError, match="RESOURCE_BLOCKED") as caught:
        run_task(directory, transport(), control=object())
    diagnostic = caught.value.public_diagnostic()
    assert diagnostic["resources"]["required"]["saved_bytes"] == 5
    assert diagnostic["secondary"] == ["TASK_STATE_PERSIST_FAILED"]
    assert calls == [] and "SECRET" not in str(diagnostic)


@pytest.mark.parametrize("primary_kind", ["resource", "keyboard", "system"])
def test_run_unwind_unlock_and_close_keep_primary(setup, monkeypatch, primary_kind):
    import os

    from sakurapool.storage.publication_session import PublicationSession
    from sakurapool.tasks.store import TaskDB

    pub, directory, ledger, transport, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec(), max_output_bytes=4):
        pass
    marker = KeyboardInterrupt() if primary_kind == "keyboard" else SystemExit(73)
    original_close = TaskDB.close
    session_close = PublicationSession.close

    def close(task):
        original_close(task)
        raise OSError("SECRET_DB_CLOSE")

    def close_session(session):
        session_close(session)
        raise OSError("SECRET_SESSION_CLOSE")

    lock_identity = (directory / "runner.lock").stat()

    def is_runner(fd):
        info = os.fstat(fd)
        return (info.st_dev, info.st_ino) == (lock_identity.st_dev, lock_identity.st_ino)

    if os.name == "nt":
        import msvcrt

        original_unlock = msvcrt.locking

        def unlock(fd, mode, length):
            original_unlock(fd, mode, length)
            if mode == msvcrt.LK_UNLCK and is_runner(fd):
                raise OSError("SECRET_UNLOCK")

        monkeypatch.setattr(msvcrt, "locking", unlock)
    else:
        import fcntl

        original_unlock = fcntl.flock

        def unlock(fd, mode):
            original_unlock(fd, mode)
            if mode == fcntl.LOCK_UN and is_runner(fd):
                raise OSError("SECRET_UNLOCK")

        monkeypatch.setattr(fcntl, "flock", unlock)
    monkeypatch.setattr(TaskDB, "close", close)
    monkeypatch.setattr(PublicationSession, "close", close_session)

    def fault(event, payload):
        if primary_kind != "resource" and event == "CLAIMED":
            raise marker

    expected = TaskError if primary_kind == "resource" else type(marker)
    with pytest.raises(expected) as caught:
        run_task(directory, transport(), control=object(), fault_hook=fault)
    primary = caught.value
    assert primary.task_secondary == ("PUBLICATION_SESSION_CLOSE_FAILED", "TASK_UNLOCK_FAILED",
                                      "TASK_CLOSE_FAILED")
    if primary_kind == "resource":
        assert primary.code == "RESOURCE_BLOCKED"
        assert primary.public_diagnostic()["resources"]["required"]["saved_bytes"] == 5
        assert "SECRET" not in str(primary.public_diagnostic())
    else:
        assert primary is marker
    assert calls == []


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
def test_run_first_close_interrupt_is_not_swallowed(setup, monkeypatch, interrupt):
    from sakurapool.tasks.plan import Selection
    from sakurapool.tasks.store import TaskDB

    pub, directory, ledger, transport, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec(), Selection("first", 0)):
        pass
    original = TaskDB.close
    marker = interrupt()

    def close(task):
        original(task)
        raise marker

    monkeypatch.setattr(TaskDB, "close", close)
    with pytest.raises(interrupt) as caught:
        run_task(directory, transport(), control=object())
    assert caught.value is marker and calls == []


def test_foreign_write_entry_refused_before_sqlite_open(setup, tmp_path, monkeypatch):
    import sakurapool.tasks.store as store
    from sakurapool.tasks.runner import admit_task_growth

    opened = []
    monkeypatch.setattr(store, "_connect", lambda *args, **kwargs: opened.append(args))
    _, _, ledger, _, calls = setup
    with pytest.raises(ValueError):
        with admit_task_growth(tmp_path, ledger), store.TaskDB(tmp_path):
            pytest.fail("foreign writable task admitted")
    assert opened == [] and calls == []


def test_control_disk_admission_refuses_before_rw_open(setup, monkeypatch, capsys):
    import sakurapool.tasks.store as store
    from sakurapool.cli import main
    from sakurapool.storage.budget import BudgetExceeded
    from sakurapool.tasks import cli

    pub, directory, ledger, _, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    opened = []
    original = store._connect

    def connect(path, *, readonly=False):
        opened.append(readonly)
        return original(path, readonly=readonly)

    monkeypatch.setattr(store, "_connect", connect)
    monkeypatch.setattr(cli, "BudgetLedger", lambda root: ledger)
    monkeypatch.setattr(ledger, "reserve", lambda request: (_ for _ in ()).throw(
        BudgetExceeded("disk cap")))
    before = (directory / "task.sqlite").read_bytes()
    assert main(["task", "pause", str(directory)]) == 2
    assert "RESOURCE_BLOCKED" in capsys.readouterr().out
    assert opened == [] and calls == []
    assert (directory / "task.sqlite").read_bytes() == before


def test_non_task_manifest_base_is_refused_without_creation(setup):
    from sakurapool.tasks.export import export_task

    pub, directory, ledger, _, _ = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    manifest = directory.parent / "wrong-base.jsonl"
    with pytest.raises(TaskError, match="EXPORT_BASE_MISMATCH"):
        export_task(directory, manifest, ledger)
    assert not manifest.exists()


@pytest.mark.parametrize("resource,remaining", [("attempts", 9), ("body", 65536),
                                               ("metadata", 1 << 20)])
def test_cold_required_steps_rejected_before_provider(setup, monkeypatch, resource, remaining):
    pub, directory, ledger, transport, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    original = ledger.status

    def constrained():
        status = original()
        status[resource] = ledger.limits[resource] - remaining
        return status

    monkeypatch.setattr(ledger, "status", constrained)
    with pytest.raises(TaskError, match="RESOURCE_BLOCKED") as caught:
        run_task(directory, transport(), control=object())
    diagnostic = caught.value.public_diagnostic()
    assert diagnostic["resources"]["required"][resource] > remaining
    assert calls == []


def test_existing_global_saved_cap_precedes_provider(setup):
    pub, directory, ledger, transport, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    lease = ledger.reserve(Reservation(saved_samples=LIMITS["saved_samples"]))
    ledger.settle(lease, saved_samples=LIMITS["saved_samples"])
    with pytest.raises(TaskError, match="RESOURCE_BLOCKED") as caught:
        run_task(directory, transport(), control=object())
    diagnostic = caught.value.public_diagnostic()
    assert diagnostic["resources"]["remaining"]["saved_samples"] == 0
    assert diagnostic["resources"]["required"]["saved_samples"] == 1
    assert calls == []
    assert ledger.status()["saved_samples"] == LIMITS["saved_samples"]


def test_unrelated_historical_pending_does_not_block_confirmed_task(setup):
    pub, directory, ledger, transport, calls = setup
    unknown = ledger.reserve(Reservation(body=7, attempt=True))
    ledger.consume_body(unknown, 3)
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    result = run_task(directory, transport(), control=object())
    assert result["state"] == "COMPLETED" and result["delivered_confirmed"] == 1
    with ledger._locked():
        _, (_, _, pending, _) = ledger._read_pair()
    assert pending[unknown]["body"] == 7 and pending[unknown]["consumed_body"] == 3
    assert calls == ["proof", "range"]
