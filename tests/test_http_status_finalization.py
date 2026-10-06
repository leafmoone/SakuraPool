"""Bounded status accounting, synthetic only."""

from pathlib import Path

import pytest
from test_network_finalization import channel as status_channel
from test_network_finalization import snapshot, terminal

from sakurapool.storage import production
from sakurapool.storage.production_resources import (
    CONTROL_RESPONSE_BODY_CAP,
    UINT64_MAX,
    checked_body_add,
    checked_body_mul,
    chunked_body_budget,
    network_body_budget,
)
from sakurapool.storage.transport import RemoteIOError

channel = status_channel


@pytest.mark.parametrize("persistent", [False, True])
def test_stale_worker_capability_before_network(channel, monkeypatch, persistent):
    ledger, transport, obj = channel
    children = []

    class Stale:
        capabilities = {"bounded_session_v1", "production_transfer_v2"}

        def __init__(self, *args, **kwargs):
            self._proc = object()
            children.append(self)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()

        def close(self):
            self._proc = None

        cancel = close

        def send_raw(self, raw):
            pytest.fail("stale worker must not receive production request")

    monkeypatch.setattr(production, "RustWorker", Stale)
    if persistent:
        transport.enable_persistent()
    with pytest.raises(RemoteIOError) as caught:
        with transport.transfer(obj, condition="observe"):
            pass
    assert caught.value.code == "WORKER_CAPABILITY_UNAVAILABLE"
    transport.close()
    assert all(child._proc is None for child in children)
    state = snapshot(ledger)
    assert state["pending_count"] == 0
    assert state["usage"]["attempts"] == 0
    assert state["usage"]["body"] == 0
    assert state["usage"]["inflight"] == 0


@pytest.mark.parametrize("failed_index", [2, 3, 4])
def test_four_lease_admission_failure_no_network(channel, monkeypatch, failed_index):
    ledger, transport, obj = channel
    terminal(monkeypatch, complete=True)
    original = ledger.reserve
    count = 0

    def reserve(spec):
        nonlocal count
        if spec.attempt:
            count += 1
            if count == failed_index:
                raise OSError("synthetic admission failure")
        return original(spec)

    monkeypatch.setattr(ledger, "reserve", reserve)
    with pytest.raises(OSError, match="synthetic admission failure"):
        with transport.transfer(obj, condition="observe"):
            pass
    state = snapshot(ledger)
    assert state["pending_count"] == 0
    assert state["usage"]["body"] == 0
    assert state["usage"]["attempts"] == failed_index - 1


@pytest.mark.parametrize("failed_index", [1, 2, 3, 4])
def test_four_lease_partial_settlement_unknown(channel, monkeypatch, failed_index):
    ledger, transport, obj = channel
    terminal(monkeypatch, "origin", "origin_status", observed=7, complete=True, status=400)
    original_reserve = ledger.reserve
    original_settle = ledger.settle
    network = []
    settled = 0

    def reserve(spec):
        lease = original_reserve(spec)
        if spec.attempt:
            network.append(lease)
        return lease

    def settle(lease, **kwargs):
        nonlocal settled
        if lease in network:
            settled += 1
            if settled == failed_index:
                raise OSError("synthetic settlement failure")
        return original_settle(lease, **kwargs)

    monkeypatch.setattr(ledger, "reserve", reserve)
    monkeypatch.setattr(ledger, "settle", settle)
    with pytest.raises(RemoteIOError) as caught:
        with transport.transfer(obj, condition="observe"):
            pass
    assert caught.value.accounting_state == "UNKNOWN"
    state = snapshot(ledger)
    assert state["pending_count"] == 5 - failed_index
    assert state["usage"]["attempts"] == 4
    assert state["usage"]["saved_samples"] == 0
    assert getattr(transport, "_http_status_candidate", None) is None
    assert transport._unresolved_network


def test_protocol_and_checked_budgets():
    source = (Path(__file__).parents[1] / "rust/src/production.rs").read_text()
    assert "CONTROL_RESPONSE_BODY_CAP: u64 = 65_536" in source
    assert CONTROL_RESPONSE_BODY_CAP == 65536
    assert network_body_budget(1) == 262148
    assert network_body_budget(1, condition="wrong") == 262148
    assert network_body_budget(8 << 20) == 8585220
    assert checked_body_mul(UINT64_MAX, 0) == 0
    assert checked_body_mul(UINT64_MAX, 1) == UINT64_MAX
    assert chunked_body_budget(1, 0, UINT64_MAX) == 262148
    for call in (
        lambda: checked_body_mul(UINT64_MAX, 2),
        lambda: checked_body_add(UINT64_MAX, 1),
        lambda: network_body_budget(UINT64_MAX),
        lambda: chunked_body_budget(UINT64_MAX, 0, 1),
    ):
        with pytest.raises(ValueError):
            call()


@pytest.mark.parametrize(
    "phase,status,condition",
    [
        ("origin", 400, "observe"),
        ("cdn", 404, "observe"),
        ("cdn", 200, "wrong"),
        ("cdn", 206, "wrong"),
        ("cdn", 403, "wrong"),
    ],
)
def test_known_status_actual_charge(channel, monkeypatch, phase, status, condition):
    ledger, transport, obj = channel
    terminal(monkeypatch, phase, phase + "_status", observed=7, complete=True, status=status)
    with pytest.raises(RemoteIOError) as caught:
        with transport.transfer(obj, length=1, condition=condition):
            pytest.fail("status must not deliver")
    error = caught.value
    assert error.accounting_state == "CONFIRMED"
    assert error._http_status_finalized is production._HTTP_STATUS_FINALIZED
    assert "accounting_basis" not in error.public_diagnostic()
    assert snapshot(ledger)["pending_count"] == 0
    assert snapshot(ledger)["usage"]["body"] == 7


@pytest.mark.parametrize("fault", ["consume", "settle", "cleanup", "prior", "incomplete"])
def test_status_finalization_faults(channel, monkeypatch, fault):
    ledger, transport, obj = channel
    terminal(
        monkeypatch,
        "origin",
        "origin_status",
        observed=7,
        complete=fault != "incomplete",
        status=400,
    )
    if fault == "consume":
        monkeypatch.setattr(
            ledger, "consume_body", lambda *a, **k: (_ for _ in ()).throw(OSError())
        )
    if fault == "settle":
        monkeypatch.setattr(ledger, "settle", lambda *a, **k: (_ for _ in ()).throw(OSError()))
    if fault == "cleanup":
        monkeypatch.setattr(transport, "_delete_owned", lambda *a: (_ for _ in ()).throw(OSError()))
    if fault == "prior":
        transport._unresolved_network = True
    with pytest.raises(BaseException) as caught:
        with transport.transfer(obj, condition="observe"):
            pass
    assert getattr(caught.value, "accounting_state", "UNKNOWN") == "UNKNOWN"
    assert getattr(caught.value, "_http_status_finalized", None) is None


@pytest.mark.parametrize("positive", [False, True])
def test_persistent_completed_status_continue(channel, monkeypatch, positive):
    ledger, transport, obj = channel
    transport.enable_persistent()
    terminal(
        monkeypatch,
        "origin",
        "origin_status",
        observed=3,
        complete=True,
        status=400,
        live=True,
        wrapper=True,
    )
    for _ in range(2):
        with pytest.raises(RemoteIOError) as caught:
            if positive:
                with transport._capability_match(obj):
                    pass
            else:
                transport.verify_conditions(obj)
        assert caught.value.accounting_state == "CONFIRMED"
        assert caught.value._http_status_finalized is production._HTTP_STATUS_FINALIZED
        assert not transport._lane_failed and not transport._lane_worker.cancelled
        assert snapshot(ledger)["pending_count"] == 1
    transport.close()
    assert snapshot(ledger)["pending_count"] == 0


@pytest.mark.parametrize(
    "statuses", [[400, 200], [403, 200], [400, 400, 200], [400, 400, 400], [403, 403, 403], [401]]
)
def test_tree_retry_ownership(channel, monkeypatch, statuses):
    import io

    from sakurapool.storage.transport import MAX_ATTEMPTS, GuardedTransport

    ledger, _, obj = channel
    client = GuardedTransport(
        ledger, trusted_hosts=frozenset({"127.0.0.1"}), allow_loopback_http=True
    )
    responses = []

    class Raw(io.BytesIO):
        def read(self, n, **kwargs):
            return super().read(n)

    class Response:
        headers = {"Content-Length": "2"}
        raw = None
        closed = 0

        def __init__(self, status):
            self.status_code = status
            self.raw = Raw(b"{}")

        def close(self):
            self.closed += 1
            assert self.closed == 1

    def get(*a, **k):
        response = Response(statuses[min(len(responses), len(statuses) - 1)])
        responses.append(response)
        return response

    monkeypatch.setattr(client.session, "get", get)
    url = obj.origin + "/api/v1/datasets/1/repo/tree"
    if statuses[-1] == 200:
        assert client.read_metadata(url, _validated_tree_retry=True) == b"{}"
    else:
        with pytest.raises(RemoteIOError) as caught:
            client.read_metadata(url, _validated_tree_retry=True)
        assert caught.value.accounting_state == "CONFIRMED"
        assert caught.value.http_status == statuses[-1]
    assert len(responses) == min(len(statuses), MAX_ATTEMPTS)
    assert all(r.closed == 1 for r in responses)
    assert snapshot(ledger)["pending_count"] == 0


@pytest.mark.parametrize("gate", ["safe", "delivered", "cleanup", "output", "secondary"])
def test_publication_status_outer_gates(gate):
    from sakurapool.storage.publication_fetch import PublicationFetchError
    error = RemoteIOError("status", code="origin_status", accounting="CONFIRMED")
    error._http_status_finalized = production._HTTP_STATUS_FINALIZED
    outer = PublicationFetchError(
        "publication_range", delivered=gate == "delivered", cleanup_safe=gate != "cleanup",
        output_lease="UNKNOWN" if gate == "output" else "CONFIRMED",
        underlying=error, secondary=("failure",) if gate == "secondary" else (),
    )
    assert outer.accounting_state == ("CONFIRMED" if gate == "safe" else "UNKNOWN")
    assert "accounting_basis" not in outer.public_diagnostic()


@pytest.mark.parametrize("kind", ["known", "transient", "unknown", "pending"])
def test_harness_classification_and_terminal_counts(tmp_path, monkeypatch, kind):
    from test_network_finalization import soak_module
    module = soak_module()

    class Channel:
        _request_worker = None
        last_result = {}
        calls = 0

        def __init__(self, ledger):
            self.ledger = ledger

        def enable_persistent(self):
            pass

        def verify_conditions(self, obj):
            self.calls += 1
            if self.calls != 1:
                return
            if kind == "pending":
                self.ledger.reserve(production.Reservation(body=1))
            error = RemoteIOError("bounded", code="origin_status" if kind == "known"
                                  else "origin_timeout", accounting="CONFIRMED")
            if kind == "known":
                error._http_status_finalized = production._HTTP_STATUS_FINALIZED
            if kind == "transient":
                error._conservative_finalized = production._CONSERVATIVE_FINALIZED
                error.accounting_basis = "CONSERVATIVE_MAX_CHARGE"
            raise error

        def close(self):
            pass

    monkeypatch.setattr(module, "connect_profile", lambda p, ledger: Channel(ledger))
    result = module.mode_run({}, tmp_path / kind, object(), "persistent", 2, "post-fix", extra=1)
    assert result["completed"] == (3 if kind in {"known", "transient"} else 1)
    assert result["extra_completed"] == (1 if kind in {"known", "transient"} else 0)
    assert result["true_unknown"] == int(kind in {"unknown", "pending"})
    assert result["known_status_rejection"] == int(kind == "known")
    assert result["confirmed_transient"] == int(kind == "transient")
    assert result["mode_safe"] == (kind in {"known", "transient"})
    assert bool(result["terminal_failures"]) == (kind == "pending")


@pytest.mark.parametrize("fault", ["ambiguous", "close", "settle", "redirect"])
def test_tree_unknown_and_redirect_not_retried(channel, monkeypatch, fault):
    from sakurapool.storage.transport import GuardedTransport, _AmbiguousRead
    ledger, _, obj = channel
    client = GuardedTransport(ledger, trusted_hosts=frozenset({"127.0.0.1"}),
                              allow_loopback_http=True)
    calls = []

    class Response:
        status_code = 302 if fault == "redirect" else 400
        headers = {"Location": obj.origin + "/different"}

        def close(self):
            if fault == "close":
                raise OSError("close failed")

    def get(*args, **kwargs):
        calls.append(args[0])
        if fault == "ambiguous":
            raise _AmbiguousRead("synthetic")
        response = Response()
        if len(calls) > 1:
            response.status_code = 403
        return response

    monkeypatch.setattr(client.session, "get", get)
    if fault == "settle":
        monkeypatch.setattr(ledger, "settle", lambda *a, **k: (_ for _ in ()).throw(OSError()))
    with pytest.raises(BaseException) as caught:
        client.read_metadata(obj.origin + "/tree", _validated_tree_retry=True)
    assert len(calls) == (2 if fault == "redirect" else 1)
    if fault == "redirect":
        assert caught.value.accounting_state == "CONFIRMED"
        assert snapshot(ledger)["pending_count"] == 0
    else:
        assert getattr(caught.value, "accounting_state", "UNKNOWN") == "UNKNOWN"
        assert snapshot(ledger)["pending_count"] == 1
