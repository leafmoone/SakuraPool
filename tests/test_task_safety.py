"""Pinned task identity, filesystem conflicts and task-profile boundaries."""

import json
import os

import pytest
from test_task_runner import setup as runner_setup

from sakurapool.runtime import RuntimeQuerySpec
from sakurapool.tasks.plan import plan_digest
from sakurapool.tasks.profile import FORMAT, read_profile
from sakurapool.tasks.runner import create_task, run_task
from sakurapool.tasks.store import TaskDB, TaskError


@pytest.fixture
def setup(tmp_path, monkeypatch):
    yield from runner_setup.__wrapped__(tmp_path, monkeypatch)


def test_publication_pin_mismatch_is_before_provider(setup):
    pub, directory, ledger, transport, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()) as task:
        header = task.meta("header")
        header["publication_digest"] = "f" * 64
        with task.transaction() as db:
            task.set_meta(db, "header", header)
            task.set_meta(db, "plan_digest", plan_digest(header))
    with pytest.raises(TaskError, match="PUBLICATION_IDENTITY_MISMATCH"):
        run_task(directory, transport(), control=object())
    assert calls == []


def test_foreign_output_preserved_before_provider(setup):
    pub, directory, ledger, transport, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()) as task:
        identity = task.db.execute("SELECT record_id FROM items").fetchone()[0]
    final = directory / "output" / identity
    final.mkdir()
    sentinel = final / "foreign"
    sentinel.write_bytes(b"must remain")
    with pytest.raises(TaskError, match="OUTPUT_CONFLICT"):
        run_task(directory, transport(), control=object())
    assert sentinel.read_bytes() == b"must remain" and calls == []


def test_replaced_delivery_is_not_confirmed_by_same_content(setup):
    pub, directory, ledger, transport, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    run_task(directory, transport(), control=object())
    image = next((directory / "output").glob("*/image.*"))
    replacement = image.parent / "replacement"
    replacement.write_bytes(image.read_bytes())
    os.replace(replacement, image)
    before = list(calls)
    with pytest.raises(TaskError, match="OUTPUT_CORRUPT"):
        run_task(directory, transport(), control=object(), resume=True)
    assert calls == before


def test_settled_receipt_is_bound_to_own_attempt(setup):
    pub, directory, ledger, transport, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    run_task(directory, transport(), control=object())
    with TaskDB(directory) as task:
        with task.transaction() as db:
            db.execute("UPDATE attempts SET operation_id=?", ("f" * 32,))
    before = list(calls)
    with pytest.raises(TaskError, match="OUTPUT_CONFLICT"):
        run_task(directory, transport(), control=object(), resume=True)
    assert calls == before


def test_linked_task_journal_rejected(setup, tmp_path):
    pub, directory, ledger, _, _ = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    target = tmp_path / "foreign-journal"
    target.write_bytes(b"foreign")
    journal = directory / "task.sqlite-journal"
    try:
        journal.symlink_to(target)
    except OSError:
        pytest.skip("symlink privilege unavailable")
    with pytest.raises(ValueError, match="reparse"):
        TaskDB(directory)
    assert target.read_bytes() == b"foreign"


@pytest.mark.parametrize("changes", [
    {"origin": "https://foreign.invalid"}, {"token": "SECRET"},
    {"repositories": []}, {"credential_ref": {"env": "x", "file": "y"}},
    {"format": "sakurapool-production-profile-v1"},
])
def test_new_profile_rejects_scope_and_embedded_credentials(tmp_path, changes):
    worker = tmp_path / "worker.exe"
    worker.write_bytes(b"synthetic file")
    profile = {"format": FORMAT, "origin": "https://modelscope.cn",
               "repositories": ["synthetic/test"], "worker": str(worker), **changes}
    path = tmp_path / "profile.json"
    path.write_text(json.dumps(profile))
    with pytest.raises(TaskError):
        read_profile(path)
