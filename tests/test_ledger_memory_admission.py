"""Serial synthetic ledger working-set and reference lifetime tests."""

import sys
from dataclasses import replace
from pathlib import Path

import pytest

from sakurapool.capacity import CapacityConfig, ResourcePolicy
from sakurapool.storage.budget import BudgetCorrupt, BudgetExceeded, Reservation
from sakurapool.workspace import Workspace


def test_model_counts_python_rows_and_configured_slots():
    small = CapacityConfig(pending_leases=1, condition_proofs=1, ledger_slot_bytes=4096)
    rows = replace(small, pending_leases=2, condition_proofs=2)
    slots = replace(small, ledger_slot_bytes=8192)
    assert rows.ledger_effective_headroom > small.ledger_effective_headroom
    assert slots.ledger_effective_headroom - small.ledger_effective_headroom == 8 * 4096
    assert small.ledger_effective_headroom == sum(small.ledger_memory_model.values())
    actual = sys.getsizeof("f" * 32) + sys.getsizeof(dict.fromkeys(range(9)))
    actual += 9 * sys.getsizeof((1 << 64) - 1)
    assert small.ledger_memory_model["two_decoded_states"] > 2 * actual


def test_near_four_gib_rejects_before_creation(tmp_path):
    cap = CapacityConfig(ledger_slot_bytes=128 + (1 << 32) - 1)
    with pytest.raises(ValueError, match="ledger working memory"):
        Workspace.init(tmp_path / "huge", capacity=cap)
    assert not (tmp_path / "huge").exists()


def test_pending_not_double_charged(tmp_path):
    cap = CapacityConfig(pending_leases=4, condition_proofs=4, ledger_slot_bytes=4096)
    headroom = cap.ledger_effective_headroom
    ws = Workspace.init(
        tmp_path / "w", capacity=cap, policy=ResourcePolicy(inflight=headroom + 100)
    )
    ledger = ws.ledger()
    first = ledger.reserve(Reservation(inflight=40))
    second = ledger.reserve(Reservation(inflight=60))
    assert ws.ledger().status()["inflight"] == 100
    assert ws.inspect()["effective_headroom"] == headroom
    assert ws.inspect()["worker_available_inflight"] == 0
    with pytest.raises(BudgetExceeded, match="inflight"):
        ledger.reserve(Reservation(inflight=1))
    with pytest.raises(BudgetExceeded, match="inflight"):
        ws.update_policy(ResourcePolicy(inflight=headroom + 99))
    ledger.settle(first)
    ledger.consume_body(second, 0)
    assert ws.inspect()["worker_available_inflight"] == 40
    ledger.settle(second)


@pytest.mark.parametrize("action", ["status", "inspect_policy", "consume", "settle", "update"])
def test_preflight_precedes_whole_slot_read(tmp_path, monkeypatch, action):
    ws = Workspace.init(tmp_path / "w")
    ledger = ws.ledger()
    lease = ledger.reserve(Reservation(inflight=5))
    # Authenticated incompatible prior admission: must not reset/refund it.
    with ledger._locked():
        index, (generation, used, leases, proofs) = ledger._read_pair()
        ledger.limits["inflight"] = ledger.effective_headroom + 4
        ledger._commit(index, generation, used, leases, proofs)
    original = Path.read_bytes

    def forbidden(path):
        if path in ledger.slots:
            pytest.fail("whole-slot allocation before memory admission")
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", forbidden)
    with pytest.raises(BudgetExceeded, match="active pending"):
        if action == "consume":
            ledger.consume_body(lease, 0)
        elif action == "settle":
            ledger.settle(lease)
        elif action == "update":
            ledger.update_policy(ResourcePolicy())
        else:
            getattr(ledger, action)()
    with pytest.raises(BudgetExceeded, match="active pending"):
        ws.ledger()


def test_retained_exception_has_no_large_completed_frames(tmp_path, monkeypatch):
    ledger = Workspace.init(tmp_path / "w").ledger()
    errors = []

    def broken_encode(*args):
        large_buffer = bytes(ledger.slot_bytes)
        raise OSError("synthetic encode failure", len(large_buffer))

    monkeypatch.setattr(ledger, "_encode", broken_encode)
    for _ in range(3):
        try:
            ledger.reserve(Reservation(inflight=1))
        except OSError as error:
            errors.append(error)
    for error in errors:
        frame = error.__traceback__
        while frame:
            if frame.tb_frame.f_code.co_name in ("broken_encode", "reserve", "_write_slot"):
                assert not frame.tb_frame.f_locals
            frame = frame.tb_next
    assert ledger.status()["inflight"] == 0


def test_corruption_never_bypasses_preflight(tmp_path):
    ledger = Workspace.init(tmp_path / "w").ledger()
    with ledger.slots[1].open("r+b") as stream:
        stream.seek(-1, 2)
        stream.write(b"x")
    with pytest.raises(BudgetCorrupt, match="checksum"):
        ledger.status()
