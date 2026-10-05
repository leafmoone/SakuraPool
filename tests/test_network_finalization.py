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
             live=False, wrapper=False):
    class Worker:
        _proc = object() if live else None

        capabilities = {"bounded_session_v1"}
        cancelled = False

        def cancel(self):
            self.cancelled = True
            self._proc = None

        def close(self):
            self._proc = None

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
        if wrapper:
            if self._lane_worker is None:
                self._lane_worker = Worker()
            self._request_worker = self._lane_worker
            with self._persistent_worker(Worker, job_budget) as worker:
                yield worker
        else:
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


def soak_module():
    import importlib.util
    from pathlib import Path

    path = Path(__file__).parents[1] / "reports" / "P6A" / "origin_soak.py"
    spec = importlib.util.spec_from_file_location("p6_origin_soak", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_soak_safe_basis_fields():
    module = soak_module()
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


@pytest.mark.parametrize("code,expected", [("origin_status", "origin_status"),
                                           ("secret URL", "rejected"), (None, "rejected")])
def test_unknown_row_preserves_only_safe_code_and_removes_basis(code, expected):
    module = soak_module()
    row = {"safe_code": code, "accounting": "CONFIRMED",
           "accounting_basis": "CONSERVATIVE_MAX_CHARGE",
           "actual_consumption": "UNKNOWN", "accounted": "CONSERVATIVE_MAX"}
    module.unknown_row(row)
    assert row == {"safe_code": expected, "accounting": "UNKNOWN", "result": "TRUE_UNKNOWN"}


def test_positive_capability_wrapper_never_defers(channel, monkeypatch):
    ledger, transport, obj = channel
    transport.enable_persistent()
    terminal(monkeypatch, live=True, wrapper=True)
    with pytest.raises(RemoteIOError):
        with transport._capability_match(obj):
            pass
    assert transport._lane_worker.cancelled
    assert transport._lane_worker._proc is None
    assert transport._lane_failed and transport._unresolved_network
    assert getattr(transport, "_deferred_network_error", None) is None


@pytest.mark.parametrize("unknown", [False, True])
def test_soak_real_api_iteration_and_transient_gate(tmp_path, monkeypatch, unknown):
    module = soak_module()
    calls = []
    channels = []

    class Channel:
        _generation = 0
        _request_worker = None
        last_result = {}

        def __init__(self, ledger):
            self.ledger = ledger
            self.closed = False
            channels.append(self)

        def enable_persistent(self):
            return self

        def verify_conditions(self, obj):
            calls.append(self)
            if len(calls) == 1:
                cause = RemoteIOError("synthetic", code="origin_timeout", phase="origin",
                                      accounting="UNKNOWN" if unknown else "CONFIRMED")
                if not unknown:
                    cause._conservative_finalized = production._CONSERVATIVE_FINALIZED
                raise cause

        def close(self):
            self.closed = True

    monkeypatch.setattr(module, "connect_profile", lambda profile, ledger: Channel(ledger))
    result = module.mode_run({}, tmp_path / "soak", object(), "persistent", 2, "post-fix", extra=3)
    assert result["completed"] == (1 if unknown else 5)
    assert result["true_unknown"] == int(unknown)
    assert result["confirmed_transient"] == int(not unknown)
    assert len(channels) == 1 and channels[0].closed
    assert result["samples"][0]["proof_steps"] == "INCOMPLETE"
    assert all("leases" not in row for row in result["samples"])


@pytest.mark.parametrize("failure", ["pending", "saved", "artifact", "close", "connect"])
def test_harness_success_and_terminal_safety_faults(tmp_path, monkeypatch, failure):
    module = soak_module()

    class Channel:
        _request_worker = None
        last_result = {}

        def __init__(self, ledger):
            self.ledger = ledger

        def enable_persistent(self):
            return self

        def verify_conditions(self, obj):
            if failure == "pending":
                self.ledger.reserve(production.Reservation(body=1))
            elif failure == "saved":
                lease = self.ledger.reserve(production.Reservation(saved_samples=1))
                self.ledger.settle(lease, saved_samples=1)
            elif failure == "artifact":
                (self.ledger.root / "rust-transfer-synthetic").mkdir()

        def close(self):
            if failure == "close":
                raise OSError("secret raw failure")

    def connect(profile, ledger):
        if failure == "connect":
            raise OSError("secret raw failure")
        return Channel(ledger)

    monkeypatch.setattr(module, "connect_profile", connect)
    result = module.mode_run({}, tmp_path / "fault", object(), "cold", 2, "post-fix")
    assert result["true_unknown"] == 1
    assert result["completed"] == 1 and result["pass_count"] == 0
    assert result["samples"][0]["accounting"] == "UNKNOWN"
    assert "secret" not in json.dumps(result)


@pytest.mark.parametrize("failure", ["close", "inspect"])
def test_tree_finalization_preserves_completed_samples(tmp_path, monkeypatch, failure):
    from types import SimpleNamespace

    module = soak_module()
    class Ledger:
        calls = 0

        def inspect_policy(self):
            self.calls += 1
            if failure == "inspect" and self.calls >= 3:
                raise OSError("secret inspect")
            return {"usage": {"attempts": self.calls}, "pending_count": 0}

    ledger = Ledger()
    ws = SimpleNamespace(ledger=lambda: ledger)
    monkeypatch.setattr(module.Workspace, "init", lambda root: ws)

    @contextmanager
    def control():
        yield SimpleNamespace(session=SimpleNamespace(headers={}),
                              _host=lambda url: "modelscope.cn")

    @contextmanager
    def transport(profile, ledger):
        yield SimpleNamespace(metadata_control=control)
        if failure == "close":
            raise OSError("secret close")

    monkeypatch.setattr(module, "connect_profile", transport)
    monkeypatch.setattr(module.ModelScopeDataset, "_data", lambda *a, **kw: {"Files": []})
    result = module.tree_differential({}, tmp_path, SimpleNamespace(origin="https://modelscope.cn"),
                                      repeats=1)
    assert result["samples"][0]["result"] == "PASS"
    assert result["samples"][-1]["accounting"] == "UNKNOWN"
    assert "secret" not in json.dumps(result)


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


@pytest.mark.parametrize("failure", [None, "consume_after_commit", "cleanup", "crash"])
def test_persistent_wrapper_continuation_and_failure(channel, monkeypatch, failure):
    ledger, transport, obj = channel
    transport.enable_persistent()
    terminal(monkeypatch, live=True, wrapper=True, crash=failure == "crash")
    if failure == "consume_after_commit":
        original = ledger.consume_body

        def consume(lease, amount, **kwargs):
            value = original(lease, amount, **kwargs)
            if amount:
                raise OSError("synthetic after commit")
            return value
        monkeypatch.setattr(ledger, "consume_body", consume)
    elif failure == "cleanup":
        monkeypatch.setattr(transport, "_delete_owned",
                            lambda *a: (_ for _ in ()).throw(KeyboardInterrupt()))
    with pytest.raises(BaseException):
        transport.verify_conditions(obj)
    worker = transport._lane_worker
    if failure is None:
        assert not worker.cancelled and not transport._lane_failed
        assert transport._lane_requests == 1
        with pytest.raises(RemoteIOError) as caught:
            transport.verify_conditions(obj)
        assert caught.value.accounting_state == "CONFIRMED"
        assert transport._lane_worker is worker
        assert transport._lane_requests == 2
        assert transport._lane_body == 4 and transport._lane_attempts == 4
    else:
        assert worker.cancelled and transport._lane_failed
        assert getattr(transport, "_unresolved_network", False)
    transport.close()


def test_terminal_failure_at_generation_boundary_rotates_preserving_resident(channel, monkeypatch):
    ledger, transport, obj = channel
    transport.enable_persistent()
    resident = transport._lane_lease
    terminal(monkeypatch, live=True, wrapper=True)
    with pytest.raises(RemoteIOError):
        transport.verify_conditions(obj)
    old = transport._lane_worker
    transport._lane_requests = 255
    transport._lane_body = 0
    transport._lane_attempts = 0
    with pytest.raises(RemoteIOError):
        with transport._operation():
            with transport._transfer_owned_body(obj, condition="observe"):
                pass
    assert transport._lane_requests == 256
    with pytest.raises(RemoteIOError):
        transport.verify_conditions(obj)
    assert old._proc is None
    assert transport._lane_worker is not old
    assert transport._lane_requests == 1
    assert transport._lane_lease == resident
    assert snapshot(ledger)["pending_count"] == 1
    transport.close()
    assert snapshot(ledger)["pending_count"] == 0


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
