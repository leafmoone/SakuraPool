"""Actual workspace-v3 quota and historical-pending coexistence, synthetic only."""
import pytest
from test_network_finalization import terminal

from sakurapool.capacity import ResourcePolicy
from sakurapool.storage.budget import BudgetExceeded, Reservation
from sakurapool.storage.production import ProviderObject, RustProductionTransport
from sakurapool.storage.transport import RemoteIOError
from sakurapool.workspace import Workspace


def test_workspace_conservative_preserves_old_pending_and_quota(tmp_path, monkeypatch):
    ws = Workspace.init(tmp_path / "workspace")
    ledger = ws.ledger()
    historical = ledger.reserve(Reservation(body=7, attempt=True))
    before = ledger.inspect_policy()
    transport = RustProductionTransport(ledger, tmp_path / "worker",
                                        origin="https://modelscope.cn")
    obj = ProviderObject("owner/repo", "modelscope_dataset_legacy", transport.origin,
                         "a" * 40, "tiny.tar", 1024)
    terminal(monkeypatch)
    with pytest.raises(RemoteIOError) as caught:
        with transport.transfer(obj, length=1, condition="observe"):
            pass
    assert caught.value.accounting_state == "CONFIRMED"
    after = ledger.inspect_policy()
    assert after["pending_count"] == before["pending_count"] == 1
    with ledger._locked():
        _, (_, _, leases, _) = ledger._read_pair()
        assert historical in leases and leases[historical]["body"] == 7
    assert after["usage"]["body"] == before["usage"]["body"] + 262148
    policy = dict(after["policy"])
    policy["body"] = after["usage"]["body"]
    ws.update_policy(ResourcePolicy(**policy), expected_version=after["policy_version"])
    charged = ledger.inspect_policy()["usage"]
    with pytest.raises(BudgetExceeded):
        ledger.reserve(Reservation(body=1, attempt=True))
    assert ledger.inspect_policy()["usage"] == charged
    assert ledger.inspect_policy()["pending_count"] == 1
