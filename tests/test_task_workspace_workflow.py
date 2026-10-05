import json
import queue
from dataclasses import replace
from threading import get_ident
from types import SimpleNamespace

import pytest

from sakurapool.capacity import CapacityConfig, ResourcePolicy
from sakurapool.cli import main
from sakurapool.storage.retrieval import _real_output_root
from sakurapool.tasks.context import effective_capacity, remaining_limits
from sakurapool.tasks.export import export_task
from sakurapool.tasks.pipeline import LedgerRPC, OwnerAuthorityError
from sakurapool.tasks.plan import SelectedRecord
from sakurapool.tasks.store import TaskDB
from sakurapool.workspace import Workspace


def test_output_gate_physical_domain_and_legacy(tmp_path):
    ws = Workspace.init(tmp_path / "w")
    ledger = ws.ledger()
    assert not ledger.offline_mode
    assert ledger.root == ws.state
    assert _real_output_root(ws.tasks, ledger) == ws.tasks
    outside = tmp_path / "outside"
    outside.mkdir()
    with pytest.raises(ValueError):
        _real_output_root(outside, ledger)
    with pytest.raises(ValueError):
        _real_output_root(ws.tasks / ".." / "tasks", ledger)
    legacy = SimpleNamespace(root=outside)
    assert _real_output_root(outside, legacy) == outside
    with pytest.raises(ValueError):
        _real_output_root(ws.tasks, legacy)


def test_capacity_api_rpc_and_fresh_policy(tmp_path):
    capacity = replace(CapacityConfig(), rpc_line_bytes=1024)
    ws = Workspace.init(tmp_path / "w", capacity=capacity, policy=ResourcePolicy(body=0))
    old = ws.ledger()
    channel = SimpleNamespace(capacity=capacity)
    assert effective_capacity(channel) == capacity
    proxy = LedgerRPC(get_ident(), queue.Queue(), old, 0, channel)
    assert proxy.workspace == ws and proxy.capacity == capacity
    ws.update_policy(ResourcePolicy(body=None))
    assert proxy.limits["body"] is None
    assert remaining_limits(old, {"body": 1})["body"] > 1
    proxy._owner = -1
    with pytest.raises(OwnerAuthorityError, match="envelope"):
        proxy._invoke("event", "a" * 400)


def test_control_export_explicit_conflict_prewrite(tmp_path, capsys):
    capacity = replace(CapacityConfig(), task_db_bytes=1 << 20, task_journal_bytes=2 << 20)
    ws = Workspace.init(tmp_path / "w", capacity=capacity)
    other = Workspace.init(tmp_path / "other")
    directory = ws.tasks / "task"
    with TaskDB.create(directory, ws.ledger(), {},
                       [SelectedRecord(0, "0" * 32, "s", "d", "0")],
                       publication_path=tmp_path / "pub"):
        pass
    before = (directory / "task.sqlite").read_bytes()
    for action in ("inspect", "pause", "cancel"):
        assert main(["task", action, str(directory), "--workspace", str(other.root)]) == 2
        assert json.loads(capsys.readouterr().out)["code"] == "TASK_WORKSPACE_CONFLICT"
        assert (directory / "task.sqlite").read_bytes() == before
    for action in ("pause", "cancel"):
        assert main(["task", action, str(directory), "--workspace", str(ws.root)]) == 0
        assert json.loads(capsys.readouterr().out)["requested"] == action.upper()
    assert export_task(directory, directory / "empty.jsonl", ws.ledger())["exported"] == 0


def test_offline_help_and_epoch_update(tmp_path, capsys):
    ws = Workspace.init(tmp_path / "w")
    config = tmp_path / "policy.json"
    config.write_text('{"body":100}')
    assert main(["workspace", "update", str(ws.root), "--config", str(config),
                 "--expected-epoch", "1"]) == 0
    assert json.loads(capsys.readouterr().out)["update_semantics"] == "EXPLICIT_EPOCH_CAS"
    assert main(["workspace", "update", str(ws.root), "--config", str(config),
                 "--expected-epoch", "1"]) == 2
    capsys.readouterr()
    assert main(["workspace", "update", str(ws.root), "--config", str(config)]) == 0
    assert json.loads(capsys.readouterr().out)["update_semantics"] == "LAST_WRITER_WINS"
    for args in (["task", "--help"], ["workspace", "update", "--help"], ["doctor", "--help"]):
        with pytest.raises(SystemExit) as error:
            main(args)
        assert error.value.code == 0
        capsys.readouterr()


def test_local_workspace_run_resume_and_unknown_network(tmp_path, monkeypatch):
    from contextlib import contextmanager

    from test_publication import inputs as publication_inputs

    from sakurapool.runtime import RuntimeQuerySpec, RuntimeSnapshot
    from sakurapool.storage import publication_fetch
    from sakurapool.storage.budget import Reservation
    from sakurapool.storage.production import RustProductionTransport
    from sakurapool.storage.publication import build_publication
    from sakurapool.tasks.runner import create_task, run_task
    from sakurapool.tasks.store import TaskError

    rt, roots, mapping, pub, _ = publication_inputs.__wrapped__(tmp_path)
    build_publication(rt, roots, mapping, pub)
    original = RuntimeSnapshot.location
    monkeypatch.setattr(RuntimeSnapshot, "location", lambda self, rid: dict(
        original(self, rid), image_size=5, flags=0, metadata_size=0))
    monkeypatch.setattr(publication_fetch, "exact_provider_lookup", lambda *a: SimpleNamespace(
        repo_type="modelscope_dataset_legacy", origin="https://modelscope.cn",
        repo_id="synthetic/test", revision="b" * 40, object_path="gc5m/one.tar",
        validator='"fresh"'))
    ws = Workspace.init(tmp_path / "w")
    ledger = ws.ledger()
    calls = []

    class LocalTransport(RustProductionTransport):
        production_profile = True
        _persistent = True
        origin = "https://modelscope.cn"
        _token = None
        _cookie = None
        capacity = ws.capacity
        max_range_bytes = capacity.range_chunk_bytes

        def __init__(self):
            self.ledger = ledger

        @property
        def clone(self):
            raise AttributeError("serial mock")

        def metadata_control(self):
            return SimpleNamespace(close=lambda: None)

        def verified_object(self, obj, **kwargs):
            return obj

        def _host(self, url):
            return "modelscope.cn"

        def verify_conditions(self, obj):
            calls.append("proof")
            return obj

        @contextmanager
        def read_range_owned(self, *args):
            calls.append("range")
            lease = ledger.reserve(Reservation(body=5, attempt=True))
            ledger.consume_body(lease, 5)
            yield b"image"
            ledger.settle(lease)

    directory = ws.tasks / "task"
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    assert not ledger.offline_mode
    for request, state in (("PAUSE", "PAUSED"), ("CANCEL", "CANCELLED")):
        with TaskDB(directory) as task:
            task.request(request)
        assert run_task(directory, LocalTransport(), resume=request == "CANCEL")["state"] == (
            "COMPLETED" if request == "CANCEL" else state)
        if request == "PAUSE":
            assert calls == []
            with pytest.raises(TaskError, match="EXPLICIT_RESUME_REQUIRED"):
                run_task(directory, LocalTransport())
    ws.update_policy(replace(ResourcePolicy(), body=5))
    assert run_task(directory, LocalTransport(), resume=True)["state"] == "COMPLETED"
    assert calls == ["proof", "range"]
    assert ledger.status()["body"] == 5
    manifest = directory / "subset.jsonl"
    assert export_task(directory, manifest, ledger)["exported"] == 1
    exported_image = directory / json.loads(manifest.read_bytes())["files"][0]["path"]
    assert exported_image.read_bytes() == b"image"
    blocked = ws.tasks / "unknown"
    with create_task(pub, blocked, ledger, RuntimeQuerySpec()) as task:
        item = task.claim()
        task.event(item["attempt_id"], "NETWORK_START", {})
    with pytest.raises(TaskError, match="NETWORK_ACCOUNTING_UNKNOWN"):
        run_task(blocked, LocalTransport(), resume=True)
    assert calls == ["proof", "range"]
