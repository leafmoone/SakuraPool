"""Offline verified-publication clone-route regression, no provider requests."""

import hashlib
import json
from contextlib import contextmanager
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from synthetic_p2 import ObjectSpec, SampleSpec, build_p2_directory

from sakurapool.capacity import CapacityConfig
from sakurapool.indexer import _json
from sakurapool.runtime import RuntimeQuerySpec, compile_runtime, load_p2_inventory
from sakurapool.storage import publication_fetch
from sakurapool.storage.publication import build_publication, sha
from sakurapool.tasks.runner import create_task, run_task
from sakurapool.workspace import Workspace


@pytest.mark.parametrize("workers", [1, 4])
@pytest.mark.parametrize("image_format", ["jpg", "gif"])
def test_large_image_clone_route(tmp_path, monkeypatch, workers, image_format):
    payload = b"x" * ((9 << 20) + 7)
    size = 12 << 20
    p2 = tmp_path / "p2"
    build_p2_directory(
        p2,
        dataset="large",
        source="synthetic",
        objects=[
            ObjectSpec(
                "large.tar", [SampleSpec("one." + image_format, "1", has_json=False)], size=size
            )
        ],
    )
    contract = json.loads((p2 / "INPUT.json").read_bytes())
    contract["hash_images"] = True
    (p2 / "INPUT.json").write_bytes(_json(contract))
    for marker in p2.glob("*.COMMIT"):
        commit = json.loads(marker.read_bytes())
        info = commit["files"]["samples"]
        path = p2 / info["path"]
        table = pq.read_table(path)
        for name, value in (
            ("image_format", image_format),
            ("size", len(payload)),
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
        json.dumps(
            {
                "dataset_id": "large",
                "endpoint": "https://modelscope.cn",
                "repo_id": "synthetic/large",
                "repo_type": "modelscope_dataset_legacy",
                "revision_candidate": "b" * 40,
                "object_path": "large.tar",
                "object_size": size,
                "provider_sha256": contract["inputs"]["large.tar"]["sha256"],
            }
        )
    )
    pub = tmp_path / "publication"
    build_publication(runtime, roots, mapping, pub)
    capacity = CapacityConfig(image_max_bytes=32 << 20)
    workspace = Workspace.init(tmp_path / "workspace", capacity=capacity)
    from sakurapool.storage.budget import BudgetLedger

    def forbidden(*args, **kwargs):
        raise AssertionError("download touched consumption ledger")

    monkeypatch.setattr(BudgetLedger, "__init__", forbidden)
    monkeypatch.setattr(Workspace, "ledger", forbidden)
    chunks = []
    monkeypatch.setattr(
        publication_fetch,
        "exact_provider_lookup",
        lambda *args: SimpleNamespace(
            repo_type="modelscope_dataset_legacy",
            object_path="large.tar",
            origin="https://modelscope.cn",
            repo_id="synthetic/large",
            revision="b" * 40,
            validator='"fresh"',
        ),
    )

    class Transport:
        def __init__(self):
            self.capacity = capacity
            self.ledger = None
            self.offline_mode = True
            self.root = workspace.root
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
    from sakurapool.image_formats import SUPPORTED_IMAGE_EXTENSIONS

    with create_task(
        pub, directory, workspace, RuntimeQuerySpec(), image_extensions=SUPPORTED_IMAGE_EXTENSIONS
    ):
        pass
    result = run_task(directory, Transport(), workers=workers, control=object())
    assert result["delivered_verified"] == 1
    assert "unknown_accounting_count" not in result
    assert chunks == [8 << 20, (1 << 20) + 7]
    image = next((directory / "output").glob("*/image." + image_format))
    assert image.read_bytes() == payload
    assert not list(workspace.state.iterdir())
    from sakurapool.storage.publication import PublicationCorrupt, load_publication
    from sakurapool.storage.publication_fetch import fetch_publication_sample
    from sakurapool.storage.publication_session import PublicationSession
    from sakurapool.tasks.export import export_task
    from sakurapool.tasks.runner import reconcile
    from sakurapool.tasks.store import TaskDB

    with TaskDB(directory) as task:
        record_id = task.db.execute("select record_id from items").fetchone()[0]
        reconcile(task)
    assert export_task(directory, directory / "manifest.jsonl")["exported"] == 1
    from sakurapool.tasks.runner import verify_delivery
    from sakurapool.tasks.store import TaskError

    with TaskDB(directory) as task:
        item = dict(task.db.execute("select * from items").fetchone())
        receipt = json.loads(item["receipt"])
        proof = receipt["receipt"].pop("image." + image_format)
        wrong = "image.png"
        receipt["receipt"][wrong] = proof
        image.rename(image.with_name(wrong))
        item["receipt"] = json.dumps(receipt)
        task.db.execute("update items set receipt=?", (item["receipt"],))
        with pytest.raises(TaskError, match="OUTPUT_CONFLICT"):
            verify_delivery(task, item)
    if image_format == "gif":
        (workspace.root / "direct").mkdir()
        (workspace.root / "session").mkdir()
        with load_publication(pub, full_verify=True) as publication:
            with pytest.raises(PublicationCorrupt, match="image format"):
                fetch_publication_sample(
                    publication, record_id, Transport(), workspace.root / "rejected"
                )
            fetch_publication_sample(
                publication,
                record_id,
                Transport(),
                workspace.root / "direct",
                image_extensions=SUPPORTED_IMAGE_EXTENSIONS,
                control=object(),
            )
        with PublicationSession(
            pub, Transport(), image_extensions=SUPPORTED_IMAGE_EXTENSIONS, control=object()
        ) as session:
            session.fetch(record_id, workspace.root / "session")
        assert (workspace.root / "direct" / record_id / "image.gif").read_bytes() == payload
        assert (workspace.root / "session" / record_id / "image.gif").read_bytes() == payload
