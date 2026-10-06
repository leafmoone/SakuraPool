"""Synthetic/offline lightweight lifecycle and no-consumption-ledger contracts."""

import hashlib
import json
import os
import subprocess
import sys
import threading
from contextlib import contextmanager
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from synthetic_p2 import ObjectSpec, SampleSpec, build_p2_directory

from sakurapool.indexer import _json
from sakurapool.runtime import RuntimeQuerySpec, compile_runtime, load_p2_inventory
from sakurapool.storage import budget, publication_fetch
from sakurapool.storage.publication import build_publication, sha
from sakurapool.tasks.plan import Selection, canonical, plan_digest
from sakurapool.tasks.runner import create_task, run_task
from sakurapool.tasks.store import TaskDB, TaskError
from sakurapool.workspace import Workspace


@pytest.fixture
def lightweight(tmp_path, monkeypatch):
    payload = b"x" * 128
    metadata = b'{"hello":"safe"}'
    p2 = tmp_path / "p2"
    objects = [
        ObjectSpec(
            f"object{i}.tar",
            [SampleSpec(f"{i}.jpg", str(i), tags=[("1girl", None)], json_size=len(metadata))],
            namespace="danbooru_native",
            size=4096,
        )
        for i in range(4)
    ]
    build_p2_directory(p2, dataset="small", source="synthetic", objects=objects)
    contract = json.loads((p2 / "INPUT.json").read_bytes())
    contract["hash_images"] = True
    (p2 / "INPUT.json").write_bytes(_json(contract))
    for marker in p2.glob("*.COMMIT"):
        commit = json.loads(marker.read_bytes())
        info = commit["files"]["samples"]
        path = p2 / info["path"]
        table = pq.read_table(path)
        for name, value in (
            ("size", len(payload)),
            ("json_offset_data", 256),
            ("sha256", hashlib.sha256(payload).hexdigest()),
            ("hash_source", "computed:sha256"),
            ("hash_kind", "sha256"),
        ):
            index = table.schema.get_field_index(name)
            table = table.set_column(
                index,
                table.schema.field(index),
                pa.array([value], type=table.schema.field(index).type),
            )
        pq.write_table(table, path)
        info.update(bytes=path.stat().st_size, sha256=sha(path))
        commit["contract_sha256"] = hashlib.sha256(_json(contract)).hexdigest()
        marker.write_bytes(_json(commit))
    runtime = tmp_path / "runtime"
    compile_runtime(load_p2_inventory(p2), runtime)
    roots = tmp_path / "roots.json"
    roots.write_text(json.dumps({"format": "sakurapool-p2-root-list-v1", "roots": [str(p2)]}))
    mapping = tmp_path / "mapping.jsonl"
    mapping.write_text(
        "\n".join(
            json.dumps(
                {
                    "dataset_id": "small",
                    "endpoint": "https://modelscope.cn",
                    "repo_id": "synthetic/small",
                    "repo_type": "modelscope_dataset_legacy",
                    "revision_candidate": "b" * 40,
                    "object_path": obj.rel,
                    "object_size": obj.size,
                    "provider_sha256": contract["inputs"][obj.rel]["sha256"],
                }
            )
            for obj in objects
        )
    )
    pub = tmp_path / "publication"
    build_publication(runtime, roots, mapping, pub)
    workspace = Workspace.init(tmp_path / "workspace")

    def forbidden(*args, **kwargs):
        raise AssertionError("lightweight download touched BudgetLedger/physical scan")

    monkeypatch.setattr(budget.BudgetLedger, "__init__", forbidden)
    monkeypatch.setattr(budget, "_disk_usage", forbidden)
    monkeypatch.setattr(Workspace, "ledger", forbidden)
    monkeypatch.setattr(
        publication_fetch,
        "exact_provider_lookup",
        lambda *args: SimpleNamespace(
            repo_type="modelscope_dataset_legacy",
            object_path=args[4],
            origin="https://modelscope.cn",
            repo_id="synthetic/small",
            revision="b" * 40,
            validator='"fresh"',
        ),
    )
    calls = []
    owners = []

    class Transport:
        ledger = None
        offline_mode = True
        _token = None
        _cookie = None
        _persistent = True

        def metadata_control(self):
            return SimpleNamespace(capacity=self.capacity)

        capacity = workspace.capacity
        root = workspace.root
        max_range_bytes = capacity.range_chunk_bytes

        def clone(self):
            return Transport()

        def close(self):
            pass

        def _host(self, url):
            return "modelscope.cn"

        def verify_conditions(self, obj):
            return obj

        @contextmanager
        def read_range_owned(self, obj, offset, length):
            calls.append((offset, length))
            owners.append(threading.get_ident())
            yield metadata if offset == 256 else payload[:length]

    directory = workspace.tasks / "small"
    query = RuntimeQuerySpec(
        sources=("synthetic",), namespace="danbooru_native", all_tags=("1girl",)
    )
    return SimpleNamespace(
        publication=pub,
        directory=directory,
        workspace=workspace,
        transport=Transport,
        query=query,
        calls=calls,
        owners=owners,
    )


@pytest.mark.parametrize("workers", [1, 4])
def test_metadata_selection_and_no_ledger(lightweight, workers):
    env = lightweight
    with create_task(
        env.publication,
        env.directory,
        env.workspace,
        env.query,
        Selection("sample", 3, "fixed-seed"),
        metadata=True,
    ) as task:
        before = task.meta("plan_digest")
        assert task.meta("header")["query"]["namespace"] == "danbooru_native"
    result = run_task(env.directory, env.transport(), workers=workers, control=object())
    assert result["delivered_verified"] == 3
    assert result["plan_digest"] == before
    assert len(env.calls) == 6
    assert threading.get_ident() not in env.owners
    with TaskDB(env.directory, readonly=True) as task:
        assert {
            row[0] for row in task.db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        } == {"meta", "items"}
        columns = {row[1] for row in task.db.execute("PRAGMA table_info(items)")}
        assert "accounting" not in columns
        assert not any(
            "consumption" in row[0] or "output_bytes" in row[0]
            for row in task.db.execute("SELECT key FROM meta")
        )
    from sakurapool.tasks.export import export_task

    assert export_task(env.directory, env.directory / "export.jsonl")["exported"] == 3
    assert not list(env.workspace.state.iterdir())


def test_published_fault_resume_verifies_not_redownloads(lightweight):
    env = lightweight
    with create_task(
        env.publication, env.directory, env.workspace, env.query, Selection("first", 1)
    ):
        pass

    def fault(event, item):
        if event == "PUBLISHED":
            raise RuntimeError("SECRET_TOKEN")

    with pytest.raises(publication_fetch.LightweightFetchError):
        run_task(env.directory, env.transport(), control=object(), fault_hook=fault)
    with TaskDB(env.directory, readonly=True) as task:
        row = task.db.execute("SELECT * FROM items").fetchone()
        assert row["state"] == "BLOCKED"
        assert task.meta("state") == "FAILED"
        details = task.failure_diagnostic(0)
        assert details["delivery"] == "PUBLISHED"
        assert details["operation_phase"] == "PUBLISHED"
        assert "SECRET" not in json.dumps(details)
        assert not any("account" in key or "bytes" in key for key in details)
    before = len(env.calls)
    result = run_task(env.directory, env.transport(), control=object(), resume=True)
    assert result["delivered_verified"] == 1
    assert len(env.calls) == before


def test_claimed_fault_resume_owned_state_only(lightweight):
    env = lightweight
    with create_task(
        env.publication, env.directory, env.workspace, env.query, Selection("first", 1)
    ):
        pass

    def fault(event, item):
        if event == "CLAIMED":
            raise RuntimeError("synthetic crash")

    with pytest.raises(RuntimeError):
        run_task(env.directory, env.transport(), control=object(), fault_hook=fault)
    assert not env.calls
    result = run_task(env.directory, env.transport(), control=object(), resume=True)
    assert result["delivered_verified"] == 1


def test_unknown_protocol_not_blindly_retried(lightweight):
    env = lightweight
    with create_task(
        env.publication, env.directory, env.workspace, env.query, Selection("first", 1)
    ):
        pass
    original = env.transport.read_range_owned

    @contextmanager
    def fail(self, *_args):
        from sakurapool.storage.transport import RemoteIOError

        raise RemoteIOError("SECRET_URL", code="network_ambiguous", phase="transport")
        yield

    env.transport.read_range_owned = fail
    with pytest.raises(publication_fetch.LightweightFetchError):
        run_task(env.directory, env.transport(), control=object())
    with TaskDB(env.directory, readonly=True) as task:
        assert task.failure_diagnostic(0)["cause_code"] == "network_ambiguous"
    env.transport.read_range_owned = original
    with pytest.raises(TaskError, match="OUTPUT_UNCERTAIN"):
        run_task(env.directory, env.transport(), control=object(), resume=True)
    assert not env.calls


def test_transient_recovery_limit_survives_fresh_runners(lightweight):
    env = lightweight
    with create_task(
        env.publication, env.directory, env.workspace, env.query, Selection("first", 1)
    ):
        pass

    @contextmanager
    def fail(self, *_args):
        from sakurapool.storage.transport import RemoteIOError

        raise RemoteIOError(
            "fixed transient", code="origin_timeout", phase="origin", lightweight=True
        )
        yield

    env.transport.read_range_owned = fail
    for index in range(3):
        with pytest.raises(publication_fetch.LightweightFetchError):
            run_task(env.directory, env.transport(), control=object(), resume=index > 0)
        with TaskDB(env.directory, readonly=True) as task:
            assert task.db.execute("SELECT recovery_retries FROM items").fetchone()[0] == index
    with pytest.raises(TaskError, match="RECOVERY_RETRY_LIMIT"):
        run_task(env.directory, env.transport(), control=object(), resume=True)


def test_unknown_output_no_overwrite(lightweight):
    env = lightweight
    with create_task(
        env.publication, env.directory, env.workspace, env.query, Selection("first", 1)
    ) as task:
        record = task.db.execute("SELECT record_id FROM items").fetchone()[0]
    target = env.directory / "output" / record
    target.mkdir()
    sentinel = target / "image.jpg"
    sentinel.write_bytes(b"unknown user file")
    with pytest.raises(TaskError, match="OUTPUT_CONFLICT"):
        run_task(env.directory, env.transport(), control=object())
    assert sentinel.read_bytes() == b"unknown user file"
    assert not env.calls


def test_legacy_write_rejected_before_rw_connection(tmp_path, monkeypatch):
    directory = tmp_path / "legacy"
    header = {
        "publication_digest": "0" * 64,
        "snapshot_id": "1" * 64,
        "query": {},
        "selection": {},
        "metadata": False,
    }
    with TaskDB.create(directory, None, header, [], publication_path=tmp_path / "unused") as task:
        frozen = task.meta("header")
        frozen["format"] = "sakurapool-task-v1"
        frozen.pop("effective_capacity")
        frozen.pop("workspace_binding")
        task.db.execute("PRAGMA user_version=1")
        task.db.execute("UPDATE meta SET value=? WHERE key='header'", (canonical(frozen).decode(),))
        task.db.execute(
            "UPDATE meta SET value=? WHERE key='plan_digest'",
            (canonical(plan_digest(frozen)).decode(),),
        )
    from sakurapool.tasks import store

    original = store._connect
    calls = []

    def guarded(path, **kwargs):
        calls.append(kwargs.get("readonly", False))
        assert kwargs.get("readonly", False)
        return original(path, **kwargs)

    monkeypatch.setattr(store, "_connect", guarded)
    before = (directory / "task.sqlite").read_bytes()
    with pytest.raises(TaskError, match="LEGACY_TASK_MIGRATION_REQUIRED"):
        TaskDB(directory)
    assert calls == [True]
    assert before == (directory / "task.sqlite").read_bytes()
    assert not list(directory.glob("task.sqlite-*"))


def test_failure_transaction_survives_hard_exit(tmp_path):
    directory = tmp_path / "crash"
    code = """
import os,sys
from types import SimpleNamespace
from sakurapool.tasks.store import TaskDB
h={'publication_digest':'0'*64,'snapshot_id':'1'*64,'query':{},'selection':{},'metadata':False}
r=SimpleNamespace(rid=0,record_id='2'*32,source='synthetic',dataset='small',post_id='1')
t=TaskDB.create(sys.argv[1],None,h,[r],publication_path=sys.argv[2])
i=t.claim()
t.finish_item(0,state='FAILED',code='publication_range',operation=i['operation_id'],diagnostic={'code':'publication_range','phase':'publication_fetch','cause_code':'origin_timeout','cause_phase':'transport','recoverable':False,'url':'SECRET_TOKEN'})
os._exit(9)
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(directory), str(tmp_path / "unused")],
        env={**os.environ, "PYTHONPATH": "src"},
        timeout=20,
    )
    assert result.returncode == 9
    with TaskDB(directory, readonly=True) as task:
        assert task.meta("state") == "FAILED"
        assert task.db.execute("SELECT state FROM items").fetchone()[0] == "FAILED"
        details = task.failure_diagnostic(0)
        assert details["cause_code"] == "origin_timeout"
        assert details["operation_phase"] == "CLAIMED"
        assert "SECRET" not in json.dumps(details)
