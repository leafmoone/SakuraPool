"""Offline verified-publication clone-route regression, no provider requests."""
import hashlib
import json
from contextlib import contextmanager
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from synthetic_p2 import ObjectSpec, SampleSpec, build_p2_directory

from sakurapool.capacity import CapacityConfig, ResourcePolicy
from sakurapool.indexer import _json
from sakurapool.runtime import RuntimeQuerySpec, compile_runtime, load_p2_inventory
from sakurapool.storage import publication_fetch
from sakurapool.storage.publication import build_publication, sha
from sakurapool.tasks.runner import create_task, run_task
from sakurapool.workspace import Workspace


@pytest.mark.parametrize("workers", [1, 4])
def test_large_image_clone_route(tmp_path, monkeypatch, workers):
    payload = b"x" * ((9 << 20) + 7)
    size = 12 << 20
    p2 = tmp_path / "p2"
    build_p2_directory(p2, dataset="large", source="synthetic", objects=[
        ObjectSpec("large.tar", [SampleSpec("one.png", "1", has_json=False)], size=size)])
    contract = json.loads((p2 / "INPUT.json").read_bytes())
    contract["hash_images"] = True
    (p2 / "INPUT.json").write_bytes(_json(contract))
    for marker in p2.glob("*.COMMIT"):
        commit = json.loads(marker.read_bytes())
        info = commit["files"]["samples"]
        path = p2 / info["path"]
        table = pq.read_table(path)
        for name, value in (("size", len(payload)),
                            ("sha256", hashlib.sha256(payload).hexdigest()),
                            ("hash_source", "computed:sha256"), ("hash_kind", "sha256")):
            index = table.schema.get_field_index(name)
            table = table.set_column(index, table.schema.field(index),
                                     pa.array([value], type=table.schema.field(index).type))
        pq.write_table(table, path)
        info.update(bytes=path.stat().st_size, sha256=sha(path))
        commit["contract_sha256"] = hashlib.sha256(_json(contract)).hexdigest()
        marker.write_bytes(_json(commit))
    runtime = tmp_path / "runtime"
    compile_runtime(load_p2_inventory(p2), runtime)
    roots = tmp_path / "roots.json"
    roots.write_text(json.dumps({"format": "sakurapool-p2-root-list-v1", "roots": [str(p2)]}))
    mapping = tmp_path / "mapping.jsonl"
    mapping.write_text(json.dumps({"dataset_id": "large", "endpoint": "https://modelscope.cn",
        "repo_id": "synthetic/large", "repo_type": "modelscope_dataset_legacy",
        "revision_candidate": "b" * 40, "object_path": "large.tar", "object_size": size,
        "provider_sha256": contract["inputs"]["large.tar"]["sha256"]}))
    pub = tmp_path / "publication"
    build_publication(runtime, roots, mapping, pub)
    capacity = CapacityConfig(image_max_bytes=32 << 20)
    workspace = Workspace.init(tmp_path / "workspace", capacity=capacity,
                               policy=ResourcePolicy(inflight=512 << 20, disk=4 << 30))
    ledger = workspace.ledger()
    from sakurapool.storage.budget import BudgetLedger

    monkeypatch.setattr(BudgetLedger, "offline_mode", property(lambda self: True))
    chunks = []
    monkeypatch.setattr(publication_fetch, "exact_provider_lookup",
        lambda *args: SimpleNamespace(repo_type="modelscope_dataset_legacy",
            object_path="large.tar", origin="https://modelscope.cn", repo_id="synthetic/large",
            revision="b" * 40, validator='"fresh"'))

    class Transport:
        def __init__(self):
            self.capacity = capacity
            self.ledger = ledger
            self.max_range_bytes = 8 << 20

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
            chunks.append(length)
            yield b"x" * length

    directory = workspace.root / "tasks" / "large"
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    result = run_task(directory, Transport(), workers=workers, control=object())
    assert result["delivered_confirmed"] == 1
    assert result["unknown_accounting_count"] == 0
    assert chunks == [8 << 20, (1 << 20) + 7]
    image = next((directory / "output").glob("*/image.jpg"))
    assert image.read_bytes() == payload
    assert ledger.status()["saved_samples"] == 1
