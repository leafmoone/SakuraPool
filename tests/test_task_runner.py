"""Serial task events with real synthetic publication/SQLite/isolated ledger."""

import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_publication import inputs as publication_inputs

from sakurapool.runtime import RuntimeQuerySpec, RuntimeSnapshot
from sakurapool.storage import publication_fetch
from sakurapool.storage.budget import DEFAULT_WORK_ROOT, BudgetLedger
from sakurapool.storage.publication import build_publication
from sakurapool.tasks.plan import Selection
from sakurapool.tasks.runner import create_task, run_task
from sakurapool.tasks.store import TaskDB, TaskError


@pytest.fixture
def setup(tmp_path, monkeypatch):
    rt, roots, mapping, pub, _ = publication_inputs.__wrapped__(tmp_path)
    build_publication(rt, roots, mapping, pub)
    original = RuntimeSnapshot.location
    monkeypatch.setattr(RuntimeSnapshot, "location", lambda self, rid: dict(
        original(self, rid), image_size=5, flags=0, metadata_size=0,
    ))
    calls = []
    monkeypatch.setattr(publication_fetch, "exact_provider_lookup", lambda *a: SimpleNamespace(
        repo_type="modelscope_dataset_legacy", origin="https://modelscope.cn",
        repo_id="synthetic/test", revision="b" * 40, object_path="gc5m/one.tar",
        validator='"fresh"',
    ))
    with tempfile.TemporaryDirectory(dir=DEFAULT_WORK_ROOT, prefix="offline-task-run-") as raw:
        ledger = BudgetLedger(raw, _offline_test=True)

        class Transport:
            max_range_bytes = 8 << 20

            def __init__(self):
                self.ledger = ledger

            def _host(self, url):
                return "modelscope.cn"

            def verify_conditions(self, obj):
                calls.append("proof")
                return obj

            @contextmanager
            def read_range_owned(self, *args):
                calls.append("range")
                yield b"image"

        yield pub, Path(raw) / "task", ledger, Transport, calls


def test_serial_events_reopen_no_duplicate_delivery(setup):
    pub, directory, ledger, transport, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    assert not calls
    result = run_task(directory, transport(), control=object())
    assert result["state"] == "COMPLETED"
    assert calls == ["proof", "range"]
    with TaskDB(directory, readonly=True) as task:
        row = task.db.execute("SELECT * FROM attempts").fetchone()
        assert row["phase"] == "SETTLED" and row["accounting"] == "CONFIRMED"
        assert task.db.execute("SELECT state FROM items").fetchone()[0] == "DONE"
    before = ledger.status()["saved_samples"]
    assert run_task(directory, transport(), control=object(), resume=True)["state"] == "COMPLETED"
    assert ledger.status()["saved_samples"] == before == 1
    assert calls == ["proof", "range"]


def test_verified_subset_export_rejects_corrupt_output(setup):
    import json

    from sakurapool.tasks.export import export_task

    pub, directory, ledger, transport, _ = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    run_task(directory, transport(), control=object())
    manifest = directory / "subset.jsonl"
    assert export_task(directory, manifest, ledger)["exported"] == 1
    row = json.loads(manifest.read_bytes())
    image = directory / row["files"][0]["path"]
    assert image.read_bytes() == b"image" and row["source"]
    image.write_bytes(b"wrong")
    with pytest.raises(TaskError, match="OUTPUT_CORRUPT"):
        export_task(directory, directory / "corrupt.jsonl", ledger)


@pytest.mark.parametrize(
    "window", ["CLAIMED", "REQUEST", "PREPARED", "PUBLISHED", "BEFORE_SETTLED", "SETTLED"]
)
def test_real_process_crash_reconciliation(setup, window):
    pub, directory, ledger, transport, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    code = r'''
import os,sys
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager
from sakurapool.runtime import RuntimeSnapshot
from sakurapool.storage import publication_fetch
from sakurapool.storage.budget import BudgetLedger,Reservation
from sakurapool.tasks.runner import run_task
root, directory, window = sys.argv[1:]
ledger = BudgetLedger(root, _offline_test=True)
original = RuntimeSnapshot.location
RuntimeSnapshot.location = lambda self,rid: dict(
    original(self,rid),image_size=5,flags=0,metadata_size=0)
publication_fetch.exact_provider_lookup = lambda *args: SimpleNamespace(
    repo_type='modelscope_dataset_legacy',origin='https://modelscope.cn',repo_id='synthetic/test',
    revision='b'*40,object_path='gc5m/one.tar',validator='"fresh"')
class Transport:
    max_range_bytes=8<<20
    def __init__(self): self.ledger=ledger
    def _host(self,url): return 'modelscope.cn'
    def verify_conditions(self,obj): return obj
    @contextmanager
    def read_range_owned(self,*args):
        lease=ledger.reserve(Reservation(body=5,attempt=True))
        ledger.consume_body(lease,2 if window=='REQUEST' else 5)
        if window=='REQUEST': os._exit(71)
        yield b'image'
        ledger.settle(lease)
def crash(event,payload):
    if event==window: os._exit(71)
run_task(directory,Transport(),control=object(),fault_hook=crash)
'''
    child = subprocess.run([sys.executable, "-c", code, str(ledger.root), str(directory), window],
                           capture_output=True, text=True, timeout=30)
    assert child.returncode == 71, child.stderr
    with TaskDB(directory, readonly=True) as task:
        item = task.db.execute("SELECT * FROM items").fetchone()
        final = directory / "output" / item["record_id"]
        assert final.exists() == (window in ("PUBLISHED", "BEFORE_SETTLED", "SETTLED"))
    if window == "CLAIMED":
        result = run_task(directory, transport(), control=object(), resume=True)
        assert result["state"] == "COMPLETED"
        assert calls == ["proof", "range"]
    elif window == "SETTLED":
        result = run_task(directory, transport(), control=object(), resume=True)
        assert result["state"] == "COMPLETED"
        assert calls == [] and ledger.status()["saved_samples"] == 1
    else:
        with pytest.raises(TaskError, match="ACCOUNTING"):
            run_task(directory, transport(), control=object(), resume=True)
        assert calls == []
        with ledger._locked():
            _, (_, used, pending, _) = ledger._read_pair()
        assert used["saved_samples"] == (1 if window == "BEFORE_SETTLED" else 0)
        assert sum(row["saved_samples"] for row in pending.values()) == (
            0 if window == "BEFORE_SETTLED" else 1
        )
        if window == "REQUEST":
            with ledger._locked():
                _, (_, _, pending, _) = ledger._read_pair()
            assert any(row["consumed_body"] == 2 and row["body"] == 5
                       for row in pending.values())


def test_cli_create_inspect_control_no_network_or_token(setup, tmp_path, monkeypatch, capsys):
    import json

    from sakurapool.cli import main
    from sakurapool.tasks import cli as task_cli

    pub, directory, ledger, _, calls = setup
    monkeypatch.setattr(task_cli, "BudgetLedger", lambda root: ledger)
    monkeypatch.setenv("MODELSCOPE_API_TOKEN", "SECRET_NOT_READ")
    query = tmp_path / "query.json"
    query.write_text("{}")
    assert main(["task", "create", "--publication", str(pub), "--query", str(query),
                 "--task-dir", str(directory), "--selection", "first", "--limit", "0"]) == 0
    created = json.loads(capsys.readouterr().out)
    assert created["header"]["selection_count"] == 0
    assert main(["task", "inspect", str(directory)]) == 0
    assert "SECRET_NOT_READ" not in capsys.readouterr().out
    assert main(["task", "cancel", str(directory)]) == 0
    assert json.loads(capsys.readouterr().out)["requested"] == "CANCEL"
    assert calls == []


def test_task_allowlist_rejects_before_network(setup):
    pub, directory, ledger, transport, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    with pytest.raises(TaskError, match="PROFILE_SCOPE_MISMATCH"):
        run_task(directory, transport(), control=object(), connection_profile={
            "origin": "https://modelscope.cn", "repositories": ["foreign/repo"],
        })
    assert calls == []


def test_range_exit_settlement_failure_never_confirmed(setup):
    from sakurapool.storage.budget import Reservation

    pub, directory, ledger, base, calls = setup

    class BadSettlement(base):
        @contextmanager
        def read_range_owned(self, *args):
            lease = ledger.reserve(Reservation(body=5, attempt=True))
            ledger.consume_body(lease, 5)
            yield b"image"
            raise OSError("SECRET_RANGE_SETTLE_FAILURE")

    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    with pytest.raises(TaskError, match="publication_range") as caught:
        run_task(directory, BadSettlement(), control=object())
    diagnostic = caught.value.public_diagnostic()
    assert diagnostic["phase"] == "publication_fetch"
    assert diagnostic["accounting"] == "UNKNOWN" and diagnostic["cleanup"] == "SAFE"
    assert diagnostic["output_lease"] == "CONFIRMED" and "secondary" in diagnostic
    assert "SECRET" not in str(diagnostic)
    with TaskDB(directory, readonly=True) as task:
        item = task.db.execute("SELECT * FROM items").fetchone()
        attempt = task.db.execute("SELECT * FROM attempts").fetchone()
        assert item["accounting"] == attempt["accounting"] == "UNKNOWN"
        assert attempt["phase"] != "SETTLED"
        assert not (directory / "output" / item["record_id"]).exists()
        assert task.inspect()["unknown_accounting_count"] == 1
    with ledger._locked():
        _, (_, _, pending, _) = ledger._read_pair()
    assert any(row["consumed_body"] == 5 for row in pending.values())


def test_task_output_resource_gate_before_provider(setup):
    pub, directory, ledger, transport, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec(), max_output_bytes=4):
        pass
    with pytest.raises(TaskError, match="RESOURCE_BLOCKED") as raised:
        run_task(directory, transport(), control=object())
    diagnostic = raised.value.public_diagnostic()
    assert diagnostic["resources"]["required"]["saved_bytes"] == 5
    assert diagnostic["resources"]["task_output_remaining"] == 4
    assert calls == []


def test_control_request_between_loop_and_claim_is_not_completion(setup, monkeypatch):
    pub, directory, ledger, transport, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    original = TaskDB.claim
    raced = []

    def claim(task):
        if not raced:
            raced.append(task.request("CANCEL"))
        return original(task)

    monkeypatch.setattr(TaskDB, "claim", claim)
    result = run_task(directory, transport(), control=object())
    assert result["state"] == "CANCELLED" and result["delivered_confirmed"] == 0
    assert result["requested_count"] == 1 and calls == []


def test_empty_task_zero_network(setup):
    pub, directory, ledger, transport, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec(), Selection("first", 0)):
        pass
    assert run_task(directory, transport(), control=object())["state"] == "COMPLETED"
    assert calls == [] and ledger.status()["saved_samples"] == 0


def test_pause_request_then_explicit_resume(setup):
    pub, directory, ledger, transport, calls = setup
    with create_task(pub, directory, ledger, RuntimeQuerySpec()) as task:
        assert task.request("PAUSE")["requested"] == "PAUSE"
    assert run_task(directory, transport(), control=object())["state"] == "PAUSED"
    assert calls == []
    with pytest.raises(TaskError, match="EXPLICIT_RESUME_REQUIRED"):
        run_task(directory, transport(), control=object())
    assert run_task(directory, transport(), control=object(), resume=True)["state"] == "COMPLETED"
