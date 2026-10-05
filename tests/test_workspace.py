import multiprocessing
from dataclasses import replace

import pytest

from sakurapool.capacity import UINT64_MAX, ResourcePolicy
from sakurapool.storage.budget import (
    SLOT_BYTES,
    BudgetCorrupt,
    BudgetExceeded,
    BudgetLedger,
    Reservation,
)
from sakurapool.workspace import Workspace


def _race(root, queue):
    ledger = Workspace.open(root).ledger()
    try:
        ledger.reserve(Reservation(body=14, attempt=True))
        queue.put("won")
    except BudgetExceeded:
        queue.put("blocked")


def test_workspace_identity_paths_binding_and_nooverwrite(tmp_path):
    workspace = Workspace.init(tmp_path / "中文 空格 #")
    assert Workspace.open(workspace.root) == workspace
    assert all(path.is_dir() for path in (workspace.state, workspace.tasks, workspace.tmp))
    binding = workspace.task_binding(workspace.tasks / "任务 #")
    workspace.validate_task_binding(binding, workspace.tasks / "任务 #")
    with pytest.raises(ValueError):
        workspace.validate_task_binding(binding, workspace.tasks / "other")
    with pytest.raises(ValueError):
        workspace.task_binding(tmp_path / "outside")
    with pytest.raises(FileExistsError):
        Workspace.init(workspace.root)
    assert all(path.stat().st_size == SLOT_BYTES for path in workspace.ledger().slots)


def test_two_workspaces_and_cross_binding(tmp_path):
    first, second = [Workspace.init(tmp_path / name) for name in ("one", "two")]
    first.ledger().reserve(Reservation(body=10, attempt=True))
    assert second.ledger().status()["body"] == 0
    with pytest.raises(ValueError):
        second.validate_task_binding(
            first.task_binding(first.tasks / "task"), second.tasks / "task"
        )
    with pytest.raises(ValueError):
        BudgetLedger(first.root)


def test_authoritative_policy_update_preserves_pending_and_stale_handles(tmp_path):
    workspace = Workspace.init(tmp_path / "w", policy=ResourcePolicy(body=20))
    old = workspace.ledger()
    lease = old.reserve(Reservation(body=14, attempt=True))
    old.consume_body(lease, 3)
    with pytest.raises(BudgetExceeded):
        workspace.update_policy(replace(workspace.policy, body=13))
    assert workspace.policy_version == 1
    assert workspace.update_policy(replace(workspace.policy, body=None), expected_version=1) == 2
    assert old.status()["body"] == 14
    assert old.limits["body"] is None
    assert old.policy_version == 2
    old.settle(lease)
    assert workspace.ledger().status()["body"] == 3
    assert workspace.ledger().status()["attempts"] == 1
    with pytest.raises(ValueError, match="version conflict"):
        workspace.update_policy(ResourcePolicy(), expected_version=1)


def test_null_quota_overflow_is_still_bounded(tmp_path):
    workspace = Workspace.init(tmp_path / "w", policy=ResourcePolicy(body=None, records=None))
    ledger = workspace.ledger()
    ledger.reserve(Reservation(body=UINT64_MAX, records=UINT64_MAX))
    with pytest.raises(BudgetExceeded, match="integer implementation"):
        ledger.reserve(Reservation(body=1))
    with pytest.raises(BudgetExceeded, match="integer implementation"):
        ledger.reserve(Reservation(records=1))
    assert Workspace.open(workspace.root).ledger().status()["body"] == UINT64_MAX


def test_policy_changes_cannot_be_bypassed_by_limits_mutation(tmp_path):
    ledger = Workspace.init(tmp_path / "w", policy=ResourcePolicy(body=1)).ledger()
    ledger.limits["body"] = None
    with pytest.raises(BudgetExceeded, match="body"):
        ledger.reserve(Reservation(body=2))


def test_multiprocess_same_domain_race(tmp_path):
    workspace = Workspace.init(tmp_path / "w", policy=ResourcePolicy(body=20))
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    children = [context.Process(target=_race, args=(workspace.root, queue)) for _ in range(2)]
    for child in children:
        child.start()
    for child in children:
        child.join(20)
        assert child.exitcode == 0
    assert sorted(queue.get(timeout=2) for _ in children) == ["blocked", "won"]
    assert workspace.ledger().status()["body"] == 14


def test_invalid_slot_blocks_policy_and_pending_reopen(tmp_path):
    workspace = Workspace.init(tmp_path / "w")
    ledger = workspace.ledger()
    ledger.reserve(Reservation(body=4, attempt=True))
    with ledger.slots[0].open("r+b") as stream:
        stream.seek(300)
        stream.write(b"!")
    with pytest.raises(BudgetCorrupt, match="checksum"):
        workspace.ledger()


def test_link_task_path_rejected(tmp_path):
    workspace = Workspace.init(tmp_path / "w")
    outside = tmp_path / "outside"
    outside.mkdir()
    link = workspace.tasks / "link"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("symlink privilege unavailable")
    with pytest.raises(ValueError, match="reparse"):
        workspace.task_binding(link / "task")
