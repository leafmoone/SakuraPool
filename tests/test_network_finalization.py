"""Small offline faults for terminal-only conservative quota reconciliation."""
import json
from contextlib import contextmanager

import pytest

from sakurapool.storage import budget, production
from sakurapool.storage.budget import BudgetLedger
from sakurapool.storage.production import ProviderObject, RustProductionTransport
from sakurapool.storage.publication_fetch import PublicationFetchError
from sakurapool.storage.transport import RemoteIOError


@pytest.fixture
def channel(tmp_path, monkeypatch):
    monkeypatch.setattr(budget, "DEFAULT_WORK_ROOT", tmp_path)
    root = tmp_path / "ledger"
    root.mkdir()
    ledger = BudgetLedger(root, _offline_test=True)
    transport = RustProductionTransport(
        ledger, tmp_path / "synthetic-worker", origin="http://127.0.0.1:12345", _test=True
    )
    obj = ProviderObject("owner/repo", "modelscope_dataset_legacy", transport.origin,
                         "a" * 40, "tiny.tar", 1024)
    return ledger, transport, obj


def snapshot(ledger):
    with ledger._locked():
        _, (_, used, leases, _) = ledger._read_pair()
        return {"usage": ledger._totals(used, leases), "pending_count": len(leases)}


def terminal(monkeypatch, phase="origin", code="origin_connect", observed=0, crash=False,
             live=False):
    class Worker:
        _proc = object() if live else None

        def send_raw(self, raw):
            self.request = json.loads(raw)

        def read_raw(self):
            if crash:
                raise production.RustWorkerError("synthetic crash")
            accounting = {"body": observed, "attempts": 1 if phase == "origin" else 2,
                          "complete": False}
            diagnostic = {"phase": phase, "http_status": None,
                          "attempts": accounting["attempts"], "body_bytes_observed": observed,
                          "accounting_complete": False,
                          **{name: False for name in production._DIAGNOSTIC_FLAGS}}
            return {"type": "response", "request_id": self.request["request_id"], "ok": False,
                    "result": {"accounting": accounting, "diagnostic": diagnostic,
                               "production_error": code}}

    @contextmanager
    def execution(self, worker_type, job_budget):
        worker = Worker()
        self._request_worker = worker
        yield worker

    monkeypatch.setattr(RustProductionTransport, "_execution_worker", execution)


@pytest.mark.parametrize("phase,code", [("origin", "origin_connect"),
                                        ("origin", "origin_timeout"),
                                        ("cdn", "cdn_connect"), ("cdn", "cdn_timeout")])
def test_terminal_full_original_charge(channel, monkeypatch, phase, code):
    ledger, transport, obj = channel
    terminal(monkeypatch, phase, code, observed=1 if phase == "cdn" else 0)
    with pytest.raises(RemoteIOError) as caught:
        with transport.transfer(obj, length=1, condition="observe"):
            pytest.fail("failed request must not deliver")
    error = caught.value
    assert error.accounting_state == "CONFIRMED"
    assert error.public_diagnostic()["accounting_basis"] == "CONSERVATIVE_MAX_CHARGE"
    state = snapshot(ledger)
    assert state["usage"]["body"] == 2
    assert state["usage"]["attempts"] == 2
    assert state["pending_count"] == 0
    assert state["usage"]["saved_samples"] == 0


def test_crash_no_envelope_stays_unknown(channel, monkeypatch):
    ledger, transport, obj = channel
    terminal(monkeypatch, crash=True)
    with pytest.raises(RemoteIOError) as caught:
        with transport.transfer(obj, condition="observe"):
            pass
    assert caught.value.accounting_state == "UNKNOWN"
    assert snapshot(ledger)["pending_count"] == 2


@pytest.mark.parametrize("failure", ["consume", "consume_after_commit", "second_settle", "cleanup"])
def test_failed_finalization_never_confirms(channel, monkeypatch, failure):
    ledger, transport, obj = channel
    terminal(monkeypatch)
    if failure in {"consume", "consume_after_commit"}:
        original = ledger.consume_body

        def consume(lease, amount, **kwargs):
            if amount:
                if failure == "consume_after_commit":
                    original(lease, amount, **kwargs)
                raise OSError("synthetic persistence failure")
            return original(lease, amount, **kwargs)
        monkeypatch.setattr(ledger, "consume_body", consume)
    elif failure == "second_settle":
        original = ledger.settle
        network_settles = []

        def settle(lease, **kwargs):
            candidate = getattr(transport, "_conservative_candidate", None)
            if candidate and lease in candidate["leases"]:
                network_settles.append(lease)
                if len(network_settles) == 2:
                    raise OSError("synthetic second settlement failure")
            return original(lease, **kwargs)
        monkeypatch.setattr(ledger, "settle", settle)
    else:
        monkeypatch.setattr(transport, "_delete_owned",
                            lambda *args: (_ for _ in ()).throw(OSError("synthetic cleanup")))
    with pytest.raises(RemoteIOError) as caught:
        with transport.transfer(obj, condition="observe"):
            pass
    assert caught.value.accounting_state == "UNKNOWN"
    state = snapshot(ledger)
    assert state["pending_count"] == (1 if failure == "second_settle" else 3
                                     if failure == "cleanup" else 2)
    if failure in {"second_settle", "consume_after_commit"}:
        assert transport._conservative_candidate["used"]
        assert state["usage"]["body"] == 2
        assert state["usage"]["attempts"] == 2


@pytest.mark.parametrize("delivered,safe,output,secondary", [
    (True, True, "CONFIRMED", []), (False, False, "CONFIRMED", []),
    (False, True, "UNKNOWN", []), (False, True, "CONFIRMED", ["ACCOUNTING_UNKNOWN"]),
])
def test_outer_failure_does_not_wash_network(delivered, safe, output, secondary):
    cause = RemoteIOError("synthetic", code="origin_timeout", phase="origin",
                          accounting="CONFIRMED")
    cause.accounting_basis = "CONSERVATIVE_MAX_CHARGE"
    cause.actual_consumption = "UNKNOWN"
    cause.accounted = "CONSERVATIVE_MAX"
    cause._conservative_finalized = production._CONSERVATIVE_FINALIZED
    error = PublicationFetchError("publication_range", delivered=delivered,
                                  cleanup_safe=safe, output_lease=output,
                                  secondary=secondary, underlying=cause)
    assert error.accounting_state == "UNKNOWN"
    assert not error.public_diagnostic()["recoverable"]


def test_soak_safe_basis_fields():
    import importlib.util
    from pathlib import Path

    path = Path(__file__).parents[1] / "reports" / "P6A" / "origin_soak.py"
    spec = importlib.util.spec_from_file_location("p6_origin_soak", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    cause = RemoteIOError("synthetic", code="origin_timeout", phase="origin",
                          accounting="CONFIRMED")
    cause.accounting_basis = "CONSERVATIVE_MAX_CHARGE"
    cause.actual_consumption = "UNKNOWN"
    cause.accounted = "CONSERVATIVE_MAX"
    assert module.safe_accounting_basis(cause) == {
        "accounting_basis": "CONSERVATIVE_MAX_CHARGE",
        "actual_consumption": "UNKNOWN", "accounted": "CONSERVATIVE_MAX"}
    cause.accounting_basis = "secret URL"
    assert module.safe_accounting_basis(cause) == {}


def test_outer_finalized_positive():
    cause = RemoteIOError("synthetic", code="origin_timeout", phase="origin",
                          accounting="CONFIRMED")
    cause.accounting_basis = "CONSERVATIVE_MAX_CHARGE"
    cause.actual_consumption = "UNKNOWN"
    cause.accounted = "CONSERVATIVE_MAX"
    cause._conservative_finalized = production._CONSERVATIVE_FINALIZED
    error = PublicationFetchError("publication_range", delivered=False,
                                  cleanup_safe=True, output_lease="CONFIRMED",
                                  secondary=[], underlying=cause)
    assert error.accounting_state == "CONFIRMED"
    assert error.public_diagnostic()["recoverable"]


@pytest.mark.parametrize("failure", ["incomplete", "consume", "settle"])
def test_capability_unknown_latches_later_candidate(channel, monkeypatch, failure):
    ledger, transport, obj = channel
    terminal(monkeypatch, phase="cdn", code="cdn_timeout", observed=1)
    original_consume = ledger.consume_body
    original_settle = ledger.settle
    if failure == "consume":
        monkeypatch.setattr(ledger, "consume_body",
                            lambda *a, **kw: (_ for _ in ()).throw(OSError("synthetic consume")))
    elif failure == "settle":
        monkeypatch.setattr(ledger, "settle",
                            lambda *a, **kw: (_ for _ in ()).throw(OSError("synthetic settle")))
    with pytest.raises((RemoteIOError, OSError)):
        with transport._capability_match(obj):
            pass
    assert transport._unresolved_network
    assert snapshot(ledger)["pending_count"] >= 2
    monkeypatch.setattr(ledger, "consume_body", original_consume)
    monkeypatch.setattr(ledger, "settle", original_settle)
    terminal(monkeypatch)
    with pytest.raises(RemoteIOError) as caught:
        with transport.transfer(obj, condition="observe"):
            pass
    assert getattr(caught.value, "_conservative_finalized", None) is None
    outer = PublicationFetchError("publication_range", delivered=False, cleanup_safe=True,
                                  output_lease="CONFIRMED", secondary=[], underlying=caught.value)
    assert outer.accounting_state == "UNKNOWN"
    assert not outer.public_diagnostic()["recoverable"]


def test_persistent_live_worker_terminal_request_finalizes(channel, monkeypatch):
    ledger, transport, obj = channel
    transport.enable_persistent()
    resident = snapshot(ledger)
    terminal(monkeypatch, live=True)
    with pytest.raises(RemoteIOError) as caught:
        with transport.transfer(obj, condition="observe"):
            pass
    assert caught.value.accounting_state == "CONFIRMED"
    assert transport._request_worker._proc is not None
    assert snapshot(ledger)["pending_count"] == resident["pending_count"] == 1
    assert snapshot(ledger)["usage"]["inflight"] == resident["usage"]["inflight"]
    assert snapshot(ledger)["usage"]["body"] == 2


def test_early_unknown_blocks_later_finalized_evidence(channel, monkeypatch):
    ledger, transport, obj = channel
    terminal(monkeypatch, crash=True)
    with pytest.raises(RemoteIOError):
        with transport.transfer(obj, condition="observe"):
            pass
    terminal(monkeypatch)
    with pytest.raises(RemoteIOError) as caught:
        with transport.transfer(obj, condition="observe"):
            pass
    assert snapshot(ledger)["pending_count"] == 2
    error = PublicationFetchError("publication_range", delivered=False,
                                  cleanup_safe=True, output_lease="CONFIRMED",
                                  secondary=[], underlying=caught.value)
    assert error.accounting_state == "UNKNOWN"
    assert not error.public_diagnostic()["recoverable"]
