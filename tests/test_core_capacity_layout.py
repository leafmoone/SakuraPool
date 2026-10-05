"""P6 core regressions; no network."""

import json
import shutil
from dataclasses import replace

import pytest

from sakurapool.capacity import (
    CAPACITY_VERSION,
    HTTP_SUPPORTED_HEADER_BYTES,
    PROTOCOL_SAFETY_BYTES,
    CapacityConfig,
    ResourcePolicy,
)
from sakurapool.storage.budget import LIMITS, BudgetCorrupt, BudgetExceeded, Reservation
from sakurapool.workspace import Workspace


def test_defaults_and_configuration():
    config = CapacityConfig()
    assert config.explicit_records_bytes == 4 << 20
    assert config.image_max_bytes == config.metadata_max_bytes == 8 << 20
    assert config.http_header_bytes == config.rpc_line_bytes == 65536
    assert config.ledger_slot_bytes == 1 << 20
    assert config.pending_leases == config.condition_proofs == 2048
    policy = ResourcePolicy.from_configuration({"disk": 8 << 30})
    assert all(
        policy.to_dict()[name] is None
        for name in ("body", "metadata", "attempts", "records", "saved_samples", "saved_bytes")
    )
    assert policy.disk == 8 << 30
    assert LIMITS == {
        "body": 8 << 30,
        "metadata": 64 << 20,
        "attempts": 2000,
        "disk": 4 << 30,
        "records": 100_000,
        "saved_samples": 1000,
        "saved_bytes": 512 << 20,
        "inflight": 256 << 20,
    }
    assert (
        CapacityConfig.from_configuration({"image_max_bytes": 64 << 20}).image_max_bytes == 64 << 20
    )
    for cls in (CapacityConfig, ResourcePolicy):
        assert cls.from_configuration() == cls()
        with pytest.raises(ValueError):
            cls.from_configuration({"unknown": 1})
        with pytest.raises(ValueError):
            cls.from_dict({})
        with pytest.raises(ValueError):
            cls.from_dict({**cls().to_dict(), "unknown": 1})


def test_layout_and_struct_boundaries():
    config = CapacityConfig(pending_leases=1, condition_proofs=1, ledger_slot_bytes=401)
    assert config.ledger_layout_bytes == 401
    with pytest.raises(ValueError, match="cannot contain"):
        replace(config, ledger_slot_bytes=400)
    for name in ("pending_leases", "condition_proofs"):
        with pytest.raises(ValueError, match="implementation boundary"):
            replace(config, **{name: 65536})
    with pytest.raises(ValueError):
        replace(config, ledger_slot_bytes=128 + (1 << 32))
    # HTTP is bounded by the pinned parser; RPC has a separate safety cap.
    assert HTTP_SUPPORTED_HEADER_BYTES == 417_760
    for name, maximum in (("http_header_bytes", HTTP_SUPPORTED_HEADER_BYTES),
                          ("rpc_line_bytes", PROTOCOL_SAFETY_BYTES)):
        assert replace(config, **{name: maximum})
        with pytest.raises(ValueError):
            replace(config, **{name: maximum + 1})


def test_consumed_layout_reopen(tmp_path):
    config = CapacityConfig(pending_leases=1, condition_proofs=1, ledger_slot_bytes=401)
    workspace = Workspace.init(tmp_path / "w", capacity=config)
    ledger = workspace.ledger()
    assert all(path.stat().st_size == 401 for path in ledger.slots)
    lease = ledger.reserve(Reservation(body=10))
    with pytest.raises(BudgetExceeded, match="lease capacity"):
        ledger.reserve(Reservation(body=1))
    ledger.record_condition_proof("a" * 64, "b" * 64)
    with pytest.raises(BudgetExceeded, match="proof capacity"):
        ledger.record_condition_proof("c" * 64, "b" * 64)
    ledger.consume_body(lease, 3)
    ledger.settle(lease)
    reopened = Workspace.open(workspace.root).ledger()
    assert reopened.status()["body"] == 3
    assert reopened.condition_proof("a" * 64) == "b" * 64
    assert reopened.inspect_policy()["ledger_capacity"]["slot_bytes"] == 401


def test_old_layout_defaults_not_caps(tmp_path):
    config = CapacityConfig(ledger_slot_bytes=2 << 20, pending_leases=4096, condition_proofs=4096)
    workspace = Workspace.init(tmp_path / "w", capacity=config)
    assert workspace.ledger().max_pending_leases == 4096
    assert workspace.ledger().max_proofs == 4096
    assert all(path.stat().st_size == 2 << 20 for path in workspace.ledger().slots)


def test_bootstrap_disk_fail_closed(tmp_path):
    with pytest.raises(BudgetExceeded, match="bootstrap room"):
        Workspace.init(tmp_path / "w", policy=ResourcePolicy(disk=1))
    with pytest.raises(BudgetCorrupt, match="never reinitialize"):
        Workspace.open(tmp_path / "w").ledger()


def test_copy_nested_recreate_binding(tmp_path):
    original = Workspace.init(tmp_path / "w")
    binding = original.task_binding(original.tasks / "task")
    assert binding["capacity_version"] == CAPACITY_VERSION
    assert binding["ledger_lineage"] == original.ledger_lineage
    shutil.copytree(original.root, tmp_path / "copy")
    with pytest.raises(ValueError, match="physical domain"):
        Workspace.open(tmp_path / "copy")
    shutil.copytree(tmp_path / "copy", original.tasks / "nested")
    with pytest.raises(ValueError, match="nested workspace"):
        Workspace.open(original.tasks / "nested")
    old_slots = [path.read_bytes() for path in original.ledger().slots]
    shutil.rmtree(original.root)
    recreated = Workspace.init(tmp_path / "w")
    assert recreated.identity != original.identity
    assert recreated.ledger_lineage != original.ledger_lineage
    with pytest.raises(ValueError, match="binding conflict"):
        recreated.validate_task_binding(binding, recreated.tasks / "task")
    for path, raw in zip(recreated.ledger().slots, old_slots, strict=True):
        path.write_bytes(raw)
    with pytest.raises(BudgetCorrupt, match="identity"):
        recreated.ledger()


def test_version_lineage_tampering(tmp_path):
    workspace = Workspace.init(tmp_path / "w")
    path = workspace.root / "workspace.json"
    data = json.loads(path.read_bytes())
    data["capacity_version"] += 1
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="version"):
        Workspace.open(workspace.root)
    data["capacity_version"] = CAPACITY_VERSION
    data["ledger_lineage"] = "c" * 32
    path.write_text(json.dumps(data))
    with pytest.raises(BudgetCorrupt, match="identity"):
        Workspace.open(workspace.root).ledger()
