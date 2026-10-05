import pytest

from sakurapool.capacity import UINT64_MAX, ResourcePolicy
from sakurapool.storage.budget import BudgetCorrupt
from sakurapool.workspace import Workspace


@pytest.mark.parametrize(
    "name", ["body", "metadata", "attempts", "records", "saved_samples", "saved_bytes"]
)
def test_explicit_null_no_user_quota(name):
    policy = ResourcePolicy(**{name: None})
    assert ResourcePolicy.from_dict(policy.to_dict()) == policy
    totals = {key: 0 for key in policy.to_dict()}
    totals[name] = UINT64_MAX
    policy.admit(totals)
    totals[name] += 1
    with pytest.raises(ValueError, match="implementation boundary"):
        policy.admit(totals)


@pytest.mark.parametrize("name", ["disk", "inflight"])
@pytest.mark.parametrize("value", [None, True, 0, -1, 1 << 64])
def test_disk_and_inflight_must_be_bounded(name, value):
    with pytest.raises(ValueError):
        ResourcePolicy(**{name: value})


@pytest.mark.parametrize("value", [True, -1, 1 << 64, 1.5])
def test_invalid_cumulative_quotas(value):
    with pytest.raises(ValueError):
        ResourcePolicy(body=value)


def test_zero_quota_and_strict_fields():
    policy = ResourcePolicy(body=0)
    totals = {key: 0 for key in policy.to_dict()}
    totals["body"] = 1
    with pytest.raises(ValueError, match="quota"):
        policy.admit(totals)
    with pytest.raises(ValueError):
        ResourcePolicy.from_dict({"body": None})


def test_lost_accounting_never_bootstraps_again(tmp_path):
    workspace = Workspace.init(tmp_path / "w")
    ledger = workspace.ledger()
    for path in (*ledger.slots, ledger.lock_path):
        path.unlink()
    with pytest.raises(BudgetCorrupt, match="never reinitialize"):
        Workspace.open(workspace.root).ledger()
    assert not ledger.lock_path.exists()


def test_nested_domain_rejected_before_creation(tmp_path):
    workspace = Workspace.init(tmp_path / "w")
    nested = workspace.tasks / "nested"
    with pytest.raises(ValueError, match="nested workspace"):
        Workspace.init(nested)
    assert not nested.exists()
