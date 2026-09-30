"""Streaming sidecar integrity, hard caps and conservative failure ownership."""

import hashlib
import io
import json
import tarfile
from dataclasses import replace
from pathlib import Path

import pytest
from test_r2_production import twohop as _twohop

from sakurapool.registry import DatasetAdapter
from sakurapool.storage.production_stage import build_stage_from_sidecars
from sakurapool.storage.rust_index import RustScanAuditError
from sakurapool.storage.transport import BoundObject, RemoteIOError

twohop = _twohop


@pytest.mark.parametrize("mutation", ["footer", "overlap", "duplicate", "unsafe", "metadata"])
def test_sidecar_resigned_semantic_corruption_no_stage_marker(twohop, monkeypatch, mutation):
    state, ledger, transport, obj = twohop
    bound = transport.verify_conditions(obj)
    from sakurapool.storage import production

    worker_type = production.RustWorker

    class Corrupt(worker_type):
        def send_raw(self, line):
            self.transfer_root = Path(json.loads(line)["payload"]["production"]["output_root"])
            return super().send_raw(line)

        def read_raw(self):
            message = super().read_raw()
            result = message.get("result", {})
            if "format" not in result:
                return message
            root = self.transfer_root
            report = root / "scan.json"
            rows = [json.loads(line) for line in report.read_bytes().splitlines()]
            if mutation == "footer":
                rows[-1]["member_count"] += 1
            elif mutation == "overlap":
                rows[1]["offset"] = rows[0]["offset"]
            elif mutation == "duplicate":
                rows[1]["path"] = rows[0]["path"]
            elif mutation == "unsafe":
                rows[0]["path"] = "../escape.png"
            else:
                (root / "metadata.bin").write_bytes(b"x" * result["metadata_bytes"])
                result["metadata_sha256"] = hashlib.sha256(
                    (root / "metadata.bin").read_bytes()
                ).hexdigest()
            packed = b"".join(json.dumps(row).encode() + b"\n" for row in rows)
            report.write_bytes(packed)
            result["report_bytes"] = len(packed)
            result["report_sha256"] = hashlib.sha256(packed).hexdigest()
            return message

    monkeypatch.setattr(production, "RustWorker", Corrupt)
    stage = ledger.root / "bad-stage"
    with pytest.raises(RustScanAuditError):
        transport.build_stage(
            bound, DatasetAdapter("test", "synthetic"), stage, mode="remote-stream-scan"
        )
    assert not (stage / "stage.complete").exists()
    assert not list(ledger.root.glob("rust-transfer-*"))
    assert ledger.status()["body"] >= len(state["raw"])
    assert ledger.status()["inflight"] == 0


@pytest.mark.parametrize("mode", ["download-then-scan", "remote-stream-scan"])
def test_json_declared_limit_rejects_before_metadata_write(twohop, mode):
    state, ledger, transport, obj = twohop
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as tar:
        entry = tarfile.TarInfo("large.json")
        entry.size = (1 << 20) + 1
        tar.addfile(entry, io.BytesIO(b" " * entry.size))
    state["raw"] = archive.getvalue()
    obj = replace(obj, object_size=len(state["raw"]))
    bound = transport.verify_conditions(obj)
    with pytest.raises(RemoteIOError):
        transport.build_stage(
            bound, DatasetAdapter("test", "synthetic"), ledger.root / "stage", mode=mode
        )
    assert not (ledger.root / "stage/stage.complete").exists()
    retained = list(ledger.root.glob("rust-transfer-*"))
    if mode == "download-then-scan":
        assert len(retained) == 1 and (retained[0] / "body").stat().st_size == obj.object_size
        assert (retained[0] / "metadata.bin").stat().st_size == 0
        assert transport._retained_download
    else:
        assert not retained
    assert ledger.status()["inflight"] == 0


def test_uncommitted_download_release_refuses_and_retains(twohop):
    _, ledger, transport, obj = twohop
    bound = transport.verify_conditions(obj)
    transport.build_stage(
        bound, DatasetAdapter("test", "synthetic"), ledger.root / "stage", mode="download-then-scan"
    )
    roots = list(ledger.root.glob("rust-transfer-*"))
    assert len(roots) == 1
    before = (roots[0] / "body").stat().st_size
    from sakurapool.runtime.errors import CorruptInputError

    with pytest.raises(CorruptInputError):
        transport.release_committed_downloads(ledger.root / "nonexistent-p2")
    assert (roots[0] / "body").stat().st_size == before


def test_manual_sidecar_audit_rejects_digest_before_db(twohop):
    _, ledger, transport, obj = twohop
    bound = transport.verify_conditions(obj)
    with transport.transfer(bound, mode="remote-stream-scan") as (root, result):
        result["report_sha256"] = "f" * 64
        with pytest.raises(RustScanAuditError):
            build_stage_from_sidecars(
                root,
                result,
                BoundObject(
                    f"http://127.0.0.1/object?Revision={bound.revision}",
                    bound.object_size,
                    bound.revision,
                    bound.validator,
                    repository=bound.repo_id,
                ),
                DatasetAdapter("test", "synthetic"),
                ledger.root / "no-db",
                None,
                stage_lease=None,
            )
        assert not (ledger.root / "no-db").exists()


def test_settlement_failure_still_registers_owned_download_and_preserves_primary(
    twohop, monkeypatch
):
    state, ledger, transport, obj = twohop
    from sakurapool.storage.budget import BudgetExceeded, BudgetLedger

    def failed_call(_obj, root, **_kwargs):
        (root / "body").write_bytes(state["raw"])
        raise RemoteIOError("original controlled rejection")

    monkeypatch.setattr(transport, "_call", failed_call)
    original_settle = BudgetLedger.settle
    hit = []

    def settlement(self, lease, **kwargs):
        if self is ledger and not hit:
            hit.append(lease)
            raise BudgetExceeded("injected settlement refusal")
        return original_settle(self, lease, **kwargs)

    monkeypatch.setattr(BudgetLedger, "settle", settlement)
    with pytest.raises(RemoteIOError, match="original controlled rejection") as error:
        with transport.transfer(obj, condition="observe", mode="download-then-scan", retain=True):
            pytest.fail("unexpected success")
    assert error.value.__context__ is None
    assert hit and transport._retained_download
    assert len(list(ledger.root.glob("rust-transfer-*/body"))) == 1


def test_hardlinked_owned_artifact_never_registered_or_deleted(twohop, monkeypatch):
    state, ledger, transport, obj = twohop
    import os

    def failed_call(_obj, root, **_kwargs):
        (root / "body").write_bytes(state["raw"])
        os.link(root / "body", ledger.root / "second-link")
        raise RemoteIOError("original hardlink rejection")

    monkeypatch.setattr(transport, "_call", failed_call)
    with pytest.raises(RemoteIOError, match="original hardlink rejection"):
        with transport.transfer(obj, condition="observe", mode="download-then-scan", retain=True):
            pytest.fail("unexpected success")
    assert not getattr(transport, "_retained_download", {})
    assert len(list(ledger.root.glob("rust-transfer-*/body"))) == 1
    assert (ledger.root / "second-link").read_bytes() == state["raw"]


def test_release_requires_exact_intended_commit_root_and_unchanged_owned_identity(twohop):
    _, ledger, transport, obj = twohop
    import shutil

    from sakurapool.storage.modelscope import ModelScopeDataset
    from sakurapool.storage.remote_index import write_staged_v4

    bound = transport.verify_conditions(obj)
    adapter = DatasetAdapter("test", "synthetic")
    stage = transport.build_stage(bound, adapter, ledger.root / "stage", mode="download-then-scan")
    provider = ModelScopeDataset(transport, obj.origin, obj.repo_id)
    resolved = BoundObject(
        provider.download_url(obj.revision, obj.object_path),
        obj.object_size,
        obj.revision,
        bound.validator,
        repository=obj.repo_id,
    )
    durable = ledger.root / "intended-p2"
    write_staged_v4(
        ledger,
        [(obj.object_path, resolved, stage)],
        durable,
        adapter,
        production_transport=transport,
    )
    copied = ledger.root / "other-p2"
    shutil.copytree(durable, copied)
    root = next(ledger.root.glob("rust-transfer-*"))
    transport.release_committed_downloads(copied)
    assert (
        root / "body"
    ).exists()  # Same object/contract at another root is not publication intent.
    (root / "body").rename(root / "unexpected-name")
    with pytest.raises(RemoteIOError, match="changed"):
        transport.release_committed_downloads(durable)
    assert (root / "unexpected-name").exists() and transport._retained_download


def test_fragment_budget_shared_across_objects_and_extent_reader_has_no_sort(twohop, monkeypatch):
    _, ledger, transport, obj = twohop
    from sakurapool import indexer
    from sakurapool.storage import remote_index
    from sakurapool.storage.modelscope import ModelScopeDataset

    adapter = DatasetAdapter("test", "synthetic")
    frozen = []
    for number in range(2):
        candidate = transport.verify_conditions(replace(obj, object_path=f"part-{number}.tar"))
        stage = transport.build_stage(
            candidate, adapter, ledger.root / f"stage-{number}", mode="remote-stream-scan"
        )
        with remote_index.StagedArchive(stage, bounded=True) as archive:
            plan = archive.db.execute(
                "EXPLAIN QUERY PLAN SELECT name,offset_data,size FROM members "
                "ORDER BY offset_data,name"
            ).fetchall()
            assert not any("TEMP B-TREE" in step[-1] for step in plan)
            assert archive.db.execute("PRAGMA cache_size").fetchone()[0] == -2048
            assert archive.db.execute("PRAGMA mmap_size").fetchone()[0] == 0
        provider = ModelScopeDataset(transport, candidate.origin, candidate.repo_id)
        resolved = BoundObject(
            provider.download_url(candidate.revision, candidate.object_path),
            candidate.object_size,
            candidate.revision,
            candidate.validator,
            repository=candidate.repo_id,
        )
        frozen.append((candidate.object_path, resolved, stage))
    original = indexer._write_fragments
    limits = []

    def small_shared(files, rows, checkpoint, byte_limit):
        limits.append(byte_limit)
        result = original(files, rows, checkpoint, byte_limit=byte_limit)
        if len(limits) == 1:
            monkeypatch.setattr(
                remote_index,
                "OFFLINE_FRAGMENT_CAP",
                sum(info["bytes"] for info in result.values()) + 1,
            )
        return result

    monkeypatch.setattr(indexer, "_write_fragments", small_shared)
    output = ledger.root / "limited-two-object-p2"
    with pytest.raises(ValueError, match="fragment output byte cap"):
        remote_index.write_staged_v4(
            ledger, frozen, output, adapter, production_transport=transport
        )
    assert len(limits) == 2 and limits[1] == 1
    assert not list(output.glob("*.COMMIT"))  # All resource checks precede publication.
    from sakurapool.runtime.errors import CorruptInputError
    from sakurapool.runtime.inventory import load_p2_inventory

    with pytest.raises(CorruptInputError):
        load_p2_inventory(output)
    assert ledger.status()["inflight"] == 0
