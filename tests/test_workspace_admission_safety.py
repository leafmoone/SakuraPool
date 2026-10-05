import stat
from pathlib import Path
from types import SimpleNamespace

import pytest

from sakurapool.capacity import CapacityConfig, ResourcePolicy
from sakurapool.storage.budget import BudgetExceeded
from sakurapool.workspace import Workspace


def test_grandparent_reparse_rejected_before_create(monkeypatch, tmp_path):
    parent = tmp_path / "parent"
    parent.mkdir()
    original = Path.lstat

    def inspect(path, *args, **kwargs):
        if path == tmp_path:
            return SimpleNamespace(st_mode=stat.S_IFDIR, st_file_attributes=0x400)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "lstat", inspect)
    with pytest.raises(ValueError, match="reparse"):
        Workspace.init(parent / "new")
    assert not (parent / "new").exists()


def test_slot_peak_memory_rejected_before_create(tmp_path):
    with pytest.raises(ValueError, match="ledger working memory"):
        Workspace.init(tmp_path / "new", capacity=CapacityConfig(ledger_slot_bytes=32 << 20),
                       policy=ResourcePolicy(inflight=128 << 20))
    assert not (tmp_path / "new").exists()


def test_policy_shrink_includes_ledger_peak(tmp_path):
    ws = Workspace.init(tmp_path / "owned")
    with pytest.raises(BudgetExceeded, match="ledger working memory"):
        ws.update_policy(ResourcePolicy(inflight=4 << 20), expected_version=1)
    assert ws.policy_version == 1
