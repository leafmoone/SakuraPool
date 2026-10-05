"""Cross-process admission shares persisted pending, not standby charges."""

import os
import subprocess
import sys

from sakurapool.capacity import CapacityConfig, ResourcePolicy
from sakurapool.storage.budget import Reservation
from sakurapool.workspace import Workspace


def test_process_reopen_shares_one_headroom(tmp_path):
    cap = CapacityConfig(pending_leases=4, condition_proofs=4, ledger_slot_bytes=4096)
    ws = Workspace.init(
        tmp_path / "w",
        capacity=cap,
        policy=ResourcePolicy(inflight=cap.ledger_effective_headroom + 100),
    )
    parent = ws.ledger()
    parent.reserve(Reservation(inflight=60))
    script = """
import sys
from sakurapool.workspace import Workspace
from sakurapool.storage.budget import Reservation, BudgetExceeded
ledger = Workspace.open(sys.argv[1]).ledger()
ledger.reserve(Reservation(inflight=40))
assert ledger.inspect_policy()['worker_available_inflight'] == 0
try:
    ledger.reserve(Reservation(inflight=1))
except BudgetExceeded:
    pass
else:
    raise AssertionError('cross-process pending over-admitted')
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(ws.root)],
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert parent.status()["inflight"] == 100
    assert parent.inspect_policy()["worker_available_inflight"] == 0
