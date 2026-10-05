"""Names never authorize ignoring a disappearing physical-domain entry."""

import pytest

from sakurapool.storage import budget


@pytest.mark.parametrize("relative", [
    "user-file", "rust-transfer-" + "a" * 32,
    "userdir/rust-transfer-" + "a" * 32 + "/body",
    "tasks/.publication-fetch-" + "a" * 32 + "/image",
])
def test_disappearance_without_owner_blocks(monkeypatch, tmp_path, relative):
    calls = []

    def scan(root):
        calls.append(root)
        raise FileNotFoundError(2, "disappeared", str(root / relative))

    monkeypatch.setattr(budget, "_disk_usage_once", scan)
    with pytest.raises(budget.BudgetCorrupt, match="entry disappeared"):
        budget._disk_usage(tmp_path)
    assert len(calls) == 1


def test_link_error_never_retried(monkeypatch, tmp_path):
    def scan(root):
        raise budget.BudgetCorrupt("work root contains a reparse point")

    monkeypatch.setattr(budget, "_disk_usage_once", scan)
    with pytest.raises(budget.BudgetCorrupt, match="reparse"):
        budget._disk_usage(tmp_path)
