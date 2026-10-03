"""Multiple-item serial session and graceful current-item boundary tests."""

import hashlib
import json
import tempfile
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from synthetic_p2 import ObjectSpec, SampleSpec, build_p2_directory

from sakurapool.indexer import _json
from sakurapool.runtime import RuntimeQuerySpec, RuntimeSnapshot, compile_runtime, load_p2_inventory
from sakurapool.storage import publication_fetch
from sakurapool.storage.budget import DEFAULT_WORK_ROOT, BudgetLedger
from sakurapool.storage.publication import build_publication, sha
from sakurapool.tasks.runner import create_task, run_task
from sakurapool.tasks.store import TaskDB


@pytest.fixture
def multiple(tmp_path, monkeypatch):
    p2 = tmp_path / "p2"
    build_p2_directory(p2, dataset="multi", source="synthetic", objects=[
        ObjectSpec("one.tar", [SampleSpec(f"{i}.png", str(i), has_json=False)
                               for i in range(2)], size=10240),
        ObjectSpec("two.tar", [SampleSpec("2.png", "2", has_json=False)], size=10240),
    ])
    contract = json.loads((p2 / "INPUT.json").read_bytes())
    contract["hash_images"] = True
    (p2 / "INPUT.json").write_bytes(_json(contract))
    for marker in p2.glob("*.COMMIT"):
        commit = json.loads(marker.read_bytes())
        info = commit["files"]["samples"]
        path = p2 / info["path"]
        table = pq.read_table(path)
        for name, value in (("sha256", hashlib.sha256(b"image").hexdigest()),
                            ("hash_source", "computed:sha256"), ("hash_kind", "sha256")):
            index = table.schema.get_field_index(name)
            table = table.set_column(index, table.schema.field(index),
                                     pa.array([value] * table.num_rows,
                                              type=table.schema.field(index).type))
        pq.write_table(table, path)
        info.update(bytes=path.stat().st_size, sha256=sha(path))
        commit["contract_sha256"] = hashlib.sha256(_json(contract)).hexdigest()
        marker.write_bytes(_json(commit))
    runtime = tmp_path / "runtime"
    compile_runtime(load_p2_inventory(p2), runtime)
    roots = tmp_path / "roots.json"
    roots.write_text(json.dumps({"format": "sakurapool-p2-root-list-v1", "roots": [str(p2)]}))
    mapping = tmp_path / "mapping.jsonl"
    mapping.write_text("\n".join(json.dumps({
        "dataset_id": "multi", "endpoint": "https://modelscope.cn", "repo_id": "synthetic/multi",
        "repo_type": "modelscope_dataset_legacy", "revision_candidate": "b" * 40,
        "object_path": name, "object_size": 10240, "provider_sha256": value["sha256"],
    }) for name, value in contract["inputs"].items()))
    pub = tmp_path / "publication"
    build_publication(runtime, roots, mapping, pub)
    original = RuntimeSnapshot.location
    monkeypatch.setattr(RuntimeSnapshot, "location", lambda self, rid: dict(
        original(self, rid), image_size=5, flags=0, metadata_size=0,
    ))
    calls = []

    def lookup(control, endpoint, repo, revision, path, size, digest):
        calls.append(("lookup", path))
        return SimpleNamespace(repo_type="modelscope_dataset_legacy", object_path=path,
                               origin=endpoint, repo_id=repo, revision=revision,
                               validator='"fresh"')

    monkeypatch.setattr(publication_fetch, "exact_provider_lookup", lookup)
    with tempfile.TemporaryDirectory(dir=DEFAULT_WORK_ROOT, prefix="offline-task-multi-") as raw:
        ledger = BudgetLedger(raw, _offline_test=True)

        class Transport:
            max_range_bytes = 8 << 20

            def __init__(self):
                self.ledger = ledger

            def _host(self, url):
                return "modelscope.cn"

            def verify_conditions(self, obj):
                calls.append(("proof", obj.object_path))
                return obj

            @contextmanager
            def read_range_owned(self, obj, offset, size):
                calls.append(("range", obj.url))
                yield b"image"

        directory = Path(raw) / "task"
        with create_task(pub, directory, ledger, RuntimeQuerySpec()):
            pass
        yield directory, ledger, Transport, calls


def test_multiple_objects_and_same_object_session_reuse(multiple):
    directory, ledger, transport, calls = multiple
    result = run_task(directory, transport(), control=object())
    assert result["state"] == "COMPLETED" and result["delivered_confirmed"] == 3
    assert sum(kind == "proof" for kind, path in calls) == 2
    assert sum(kind == "range" for kind, path in calls) == 3
    assert ledger.status()["saved_samples"] == 3


@pytest.mark.parametrize("control_request,state", [("PAUSE", "PAUSED"), ("CANCEL", "CANCELLED")])
def test_control_finishes_current_then_no_new_claim(multiple, control_request, state):
    directory, ledger, transport, calls = multiple
    requested = []

    def control_hook(event, payload):
        if event == "PREPARED":
            with TaskDB(directory) as task:
                requested.append(task.request(control_request))

    result = run_task(directory, transport(), control=object(), fault_hook=control_hook)
    assert result["state"] == state and result["delivered_confirmed"] == 1
    assert requested == [{"requested": control_request, "state": "RUNNING"}]
    with TaskDB(directory, readonly=True) as task:
        assert task.db.execute("SELECT count(*) FROM attempts").fetchone()[0] == 1
        plan = task.meta("plan_digest")
    assert ledger.status()["saved_samples"] == 1
    result = run_task(directory, transport(), control=object(), resume=True)
    assert result["state"] == "COMPLETED" and result["delivered_confirmed"] == 3
    with TaskDB(directory, readonly=True) as task:
        assert task.meta("plan_digest") == plan
    assert sum(kind == "range" for kind, path in calls) == 3
