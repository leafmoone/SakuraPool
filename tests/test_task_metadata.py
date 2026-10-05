"""Metadata delivery receipt is content proof, not a P2 metadata SHA sidecar."""

import json
from contextlib import contextmanager

import pytest
from test_task_runner import setup as runner_setup

from sakurapool.runtime import RuntimeQuerySpec, RuntimeSnapshot
from sakurapool.tasks.export import export_task
from sakurapool.tasks.runner import create_task, run_task
from sakurapool.tasks.store import TaskDB, TaskError


@pytest.fixture
def setup(tmp_path, monkeypatch):
    yield from runner_setup.__wrapped__(tmp_path, monkeypatch)


@pytest.mark.parametrize("metadata_size", [0, 2])
def test_metadata_actual_delivered_receipt(setup, monkeypatch, metadata_size):
    pub, directory, ledger, base, calls = setup
    original = RuntimeSnapshot.location
    monkeypatch.setattr(RuntimeSnapshot, "location", lambda self, rid: dict(
        original(self, rid), flags=1, metadata_size=metadata_size,
    ))

    class Transport(base):
        @contextmanager
        def read_range_owned(self, bound, offset, size):
            calls.append("range")
            yield b"image" if size == 5 else b"{}"

    with create_task(pub, directory, ledger, RuntimeQuerySpec(), metadata=True):
        pass
    manifest = directory / "export.jsonl"
    if metadata_size == 0:
        # Flagged metadata must be a JSON object; an empty extent is not one.
        with pytest.raises(TaskError) as caught:
            run_task(directory, Transport(), control=object())
        assert caught.value.code == "publication_metadata"
        assert caught.value.safe_details["accounting"] == "UNKNOWN"
        assert caught.value.safe_details["delivery"] == "NOT_PUBLISHED"
        with TaskDB(directory, readonly=True) as task:
            item = task.db.execute("SELECT * FROM items").fetchone()
            assert item["state"] == "BLOCKED" and item["accounting"] == "UNKNOWN"
            assert not (directory / "output" / item["record_id"]).exists()
        assert export_task(directory, manifest, ledger)["exported"] == 0
        assert ledger.status()["saved_bytes"] == ledger.status()["saved_samples"] == 0
        assert calls == ["proof", "range"]
        return
    assert run_task(directory, Transport(), control=object())["state"] == "COMPLETED"
    assert export_task(directory, manifest, ledger)["exported"] == 1
    receipt = json.loads(manifest.read_bytes())
    metadata = next(row for row in receipt["files"] if row["path"].endswith("metadata.json"))
    assert metadata["bytes"] == metadata_size
    assert (directory / metadata["path"]).read_bytes() == b"{}"
    assert ledger.status()["saved_bytes"] == 5 + metadata_size
    assert len(calls) == 3
