"""C1 actual single-lane lifecycle regression using synthetic two-hop bytes."""
import hashlib
import json
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

import pytest
from test_r2_production import twohop as _twohop

from sakurapool.storage.rust_bridge import RustWorker, RustWorkerError
from sakurapool.storage.transport import GuardedTransport, RemoteIOError

twohop = _twohop


def test_persistent_worker_single_pid_and_idle_reservation(twohop, monkeypatch):
    state, ledger, transport, obj = twohop
    connections = []
    accept = ThreadingHTTPServer.get_request

    def observed_accept(server):
        result = accept(server)
        connections.append(server.server_address)
        return result

    monkeypatch.setattr(ThreadingHTTPServer, "get_request", observed_accept)
    transport.enable_persistent()
    assert ledger.status()["inflight"] == 32 << 20
    verified = transport.verify_conditions(obj)
    worker = transport._lane_worker
    pid = worker.pid
    assert "bounded_session_v1" in worker.capabilities
    for start in (512, 513, 514):
        with transport.transfer(verified, start=start, length=7) as (root, result):
            body = (root / "body").read_bytes()
            assert body == state["raw"][start:start + 7]
            assert hashlib.sha256(body).hexdigest() == result["sha256"]
            assert transport._lane_worker is worker and worker.pid == pid
            assert ledger.status()["inflight"] > 32 << 20
        assert ledger.status()["inflight"] == 32 << 20
    assert transport._lane_requests == 6
    assert len(connections) == 2  # one actual accepted origin TCP + one CDN TCP
    transport.close()
    assert worker.pid is None
    assert ledger.status()["inflight"] == 0


def test_warm_range_credit_does_not_require_cold_probe_credit(twohop):
    _, _, transport, obj = twohop
    transport.enable_persistent()
    try:
        verified = transport.verify_conditions(obj)
        worker = transport._lane_worker
        identity = (obj.origin, obj.repo_id, obj.repo_type, obj.revision,
                    obj.object_path, obj.object_size)
        transport._lane_requests = 254
        assert transport.predict_warm(identity, [1])
        assert transport.verified_object(verified, lengths=[1]) == verified
        assert transport._lane_worker is worker
        assert transport._generation == 0
        changed = (*identity[:3], "c" * 40, *identity[4:])
        assert not transport.predict_warm(changed, [1])
        transport._lane_failed = True
        assert not transport.predict_warm(identity, [1])
        transport._lane_failed = False
        transport._lane_requests = 256
        assert not transport.predict_warm(identity, [1])
        with pytest.raises(RemoteIOError):
            transport.verified_object(verified, lengths=[1])
        assert transport._generation == 1
        assert not transport.predict_warm(identity, [1])
        assert worker.pid is None
    finally:
        transport.close()
    assert not transport.predict_warm(identity, [1])


def test_actual_worker_seen_ids_bounded_and_cumulative_budget(twohop, tmp_path):
    _, _, transport, _ = twohop
    path = tmp_path / "one"
    path.write_bytes(b"x")
    budget = {"body": 512, "attempts": 512, "disk": 0, "inflight": 0}
    request_budget = {"body": 1, "attempts": 1, "disk": 0, "inflight": 0}

    def send(worker, identity):
        worker.send_raw((json.dumps(dict(type="request", request_id=identity,
            operation="hash_file", budget=request_budget,
            payload={"path": str(path)})) + "\n").encode())
        return worker.read_raw()

    with RustWorker(transport.worker, job_budget=budget) as worker:
        assert send(worker, "x" * 65)["error"] == "request_id_invalid"
        assert send(worker, "first")["ok"]
        assert send(worker, "first")["error"] == "duplicate_request"
        for i in range(255):
            assert send(worker, str(i))["ok"]
        assert send(worker, "boundary")["error"] == "session_exhausted"
    budget["body"] = budget["attempts"] = 2
    with RustWorker(transport.worker, job_budget=budget) as worker:
        assert send(worker, "a")["ok"]
        assert send(worker, "b")["ok"]
        assert send(worker, "c")["error"] == "budget_exceeded"
        with pytest.raises(RustWorkerError):
            worker.request("hash_file", budget=request_budget, payload={"path": str(path)})


def test_control_session_owned_for_channel_lifetime(twohop):
    _, ledger, transport, _ = twohop
    transport.origin = "https://modelscope.cn"
    transport.enable_persistent()
    ledger._offline_mode = False
    try:
        control = transport.metadata_control()
    finally:
        ledger._offline_mode = True
    assert transport.metadata_control() is control
    assert control.session.trust_env is False
    transport.close()
    assert transport._control is None
    assert ledger.status()["inflight"] == 0
    with pytest.raises(RemoteIOError):
        transport.metadata_control()


def test_old_capability_rejected_after_real_spawn(twohop, monkeypatch):
    state, ledger, transport, obj = twohop
    transport.enable_persistent()
    handshake = RustWorker._handshake
    spawned = []

    def old_handshake(worker, budget):
        handshake(worker, budget)
        spawned.append(worker)
        worker.capabilities = tuple(c for c in worker.capabilities if c != "bounded_session_v1")

    monkeypatch.setattr(RustWorker, "_handshake", old_handshake)
    with pytest.raises(RemoteIOError):
        transport.verify_conditions(obj)
    assert len(spawned) == 1 and spawned[0].pid is None
    assert not state["calls"]
    transport.close()
    assert ledger.status()["inflight"] == 0


def test_mismatched_response_poisoned_actual_lane(twohop, monkeypatch):
    state, _, transport, obj = twohop
    transport.enable_persistent()
    verified = transport.verify_conditions(obj)
    receive = RustWorker.read_raw

    def mismatch(worker):
        response = receive(worker)
        response["request_id"] = "old-generation"
        return response

    monkeypatch.setattr(RustWorker, "read_raw", mismatch)
    with pytest.raises(RemoteIOError):
        with transport.transfer(verified, start=512, length=7):
            pytest.fail("mismatched response accepted")
    count = len(state["calls"])
    with pytest.raises(RemoteIOError):
        with transport.transfer(verified, start=512, length=7):
            pytest.fail("poisoned channel reused")
    assert len(state["calls"]) == count
    transport.close()


def test_actual_metadata_session_tcp_reuse_and_operation_accounting(twohop):
    _, ledger, _, _ = twohop
    connections = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def do_GET(self):
            if self.path == "/short":
                self.send_response(200)
                self.send_header("Content-Length", "8")
                self.end_headers()
                self.wfile.write(b"x")
                self.close_connection = True
            else:
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")
            self.wfile.flush()

    class Server(ThreadingHTTPServer):
        def get_request(self):
            result = super().get_request()
            connections.append(result[1])
            return result

    server = Server(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    control = GuardedTransport(ledger, trusted_hosts=frozenset({"127.0.0.1"}),
                               allow_loopback_http=True)
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        assert control.read_metadata(base + "/a").accounting_state == "CONFIRMED"
        assert control.read_metadata(base + "/b").accounting_state == "CONFIRMED"
        assert len(connections) == 1
        with pytest.raises(RemoteIOError) as caught:
            control.read_metadata(base + "/short")
        assert caught.value.accounting_state == "UNKNOWN"
        assert control.read_metadata(base + "/c").accounting_state == "CONFIRMED"
    finally:
        control.close()
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.mark.parametrize("failure", ["eof", "timeout"])
def test_actual_process_failure_cannot_reassign_response(twohop, failure):
    state, ledger, transport, obj = twohop
    transport.enable_persistent()
    verified = transport.verify_conditions(obj)
    worker = transport._lane_worker
    if failure == "eof":
        worker._proc.kill()
        worker._proc.wait(timeout=2)
    else:
        worker.timeout_s = 0
    with pytest.raises(RemoteIOError):
        with transport.transfer(verified, start=512, length=7):
            pytest.fail("failed process delivered")
    assert worker.pid is None
    count = len(state["calls"])
    with pytest.raises(RemoteIOError):
        with transport.transfer(verified, start=512, length=7):
            pytest.fail("failed lane reused")
    assert len(state["calls"]) == count
    assert ledger.status()["inflight"] == 32 << 20
    transport.close()
    assert ledger.status()["inflight"] == 0


def test_actual_delayed_response_timeout_never_reused(twohop, monkeypatch):
    state, ledger, transport, obj = twohop
    transport.enable_persistent()
    verified = transport.verify_conditions(obj)
    worker = transport._lane_worker
    worker.timeout_s = 0.05
    entered = threading.Event()
    release = threading.Event()
    original = BaseHTTPRequestHandler.end_headers
    origin_port = urlsplit(obj.origin).port

    def delayed(handler):
        if handler.server.server_port == origin_port:
            entered.set()
            release.wait(timeout=1)
        return original(handler)

    monkeypatch.setattr(BaseHTTPRequestHandler, "end_headers", delayed)
    try:
        with pytest.raises(RemoteIOError):
            with transport.transfer(verified, start=512, length=7):
                pytest.fail("delayed response delivered")
        assert entered.is_set()
        assert worker.pid is None
        count = len(state["calls"])
        with pytest.raises(RemoteIOError):
            with transport.transfer(verified, start=512, length=7):
                pytest.fail("late response reassigned")
        assert len(state["calls"]) == count
    finally:
        release.set()
        transport.close()
    assert ledger.status()["inflight"] == 0


def test_spawn_thread_start_failure_reaps_actual_child(twohop, monkeypatch):
    _, _, transport, _ = twohop
    children = []
    import subprocess

    popen = subprocess.Popen

    def tracked(*args, **kwargs):
        proc = popen(*args, **kwargs)
        children.append(proc)
        return proc

    def fail_start(thread):
        raise RuntimeError("thread-start-fault")

    monkeypatch.setattr(subprocess, "Popen", tracked)
    monkeypatch.setattr(threading.Thread, "start", fail_start)
    with pytest.raises(RuntimeError, match="thread-start-fault"):
        RustWorker(transport.worker)
    assert len(children) == 1 and children[0].poll() is not None


def test_positive_probe_uses_resident_plus_payload_once(twohop, monkeypatch):
    _, ledger, transport, obj = twohop
    transport.enable_persistent()
    peaks = []
    reserve = ledger.reserve

    def observe(request):
        lease = reserve(request)
        peaks.append(ledger.status()["inflight"])
        return lease

    monkeypatch.setattr(ledger, "reserve", observe)
    transport.verify_conditions(obj)
    assert max(peaks) == (32 << 20) + 2
    assert ledger.status()["inflight"] == 32 << 20
    transport.close()


def test_close_serializes_with_first_spawn(twohop, monkeypatch):
    _, ledger, transport, obj = twohop
    transport.enable_persistent()
    entered = threading.Event()
    release = threading.Event()
    completed = threading.Event()
    handshake = RustWorker._handshake
    failures = []

    def delayed(worker, budget):
        entered.set()
        assert release.wait(timeout=3)
        handshake(worker, budget)

    def invoke():
        try:
            transport.verify_conditions(obj)
        except BaseException as error:
            failures.append(error)

    def close():
        transport.close()
        completed.set()

    monkeypatch.setattr(RustWorker, "_handshake", delayed)
    thread = threading.Thread(target=invoke)
    thread.start()
    assert entered.wait(timeout=3)
    closer = threading.Thread(target=close)
    closer.start()
    assert completed.wait(timeout=1)
    assert ledger.status()["inflight"] >= 32 << 20
    release.set()
    thread.join(timeout=5)
    closer.join(timeout=5)
    assert not thread.is_alive() and not closer.is_alive()
    assert completed.is_set()
    assert transport._lane_worker.pid is None
    assert ledger.status()["inflight"] == 0


def test_constructor_and_cancel_fault_preserve_primary(twohop, monkeypatch):
    _, ledger, transport, obj = twohop
    transport.enable_persistent()
    original_cancel = RustWorker.cancel

    def bad_handshake(worker, budget):
        raise RustWorkerError("handshake-primary")

    def bad_cancel(worker):
        raise RustWorkerError("cancel-secondary")

    monkeypatch.setattr(RustWorker, "_handshake", bad_handshake)
    monkeypatch.setattr(RustWorker, "cancel", bad_cancel)
    try:
        with pytest.raises(RemoteIOError):
            transport.verify_conditions(obj)
        assert transport._lane_failed
        assert transport._lane_worker.pid is not None
        assert ledger.status()["inflight"] >= 32 << 20
    finally:
        original_cancel(transport._lane_worker)
        transport.close()


def test_live_cancel_failure_preserves_payload_artifact(twohop, monkeypatch):
    _, ledger, transport, obj = twohop
    transport.enable_persistent()
    verified = transport.verify_conditions(obj)
    worker = transport._lane_worker
    worker.timeout_s = 0.05
    original_cancel = RustWorker.cancel
    original_headers = BaseHTTPRequestHandler.end_headers
    release = threading.Event()
    origin_port = urlsplit(obj.origin).port
    roots = []
    owned = transport._owned_dir

    def root():
        result = owned()
        roots.append(result)
        return result

    def delayed(handler):
        if handler.server.server_port == origin_port:
            release.wait(timeout=2)
        return original_headers(handler)

    def fail_cancel(worker):
        raise RustWorkerError("cancel fault")

    monkeypatch.setattr(transport, "_owned_dir", root)
    monkeypatch.setattr(BaseHTTPRequestHandler, "end_headers", delayed)
    monkeypatch.setattr(RustWorker, "cancel", fail_cancel)
    original_close = RustWorker.close
    monkeypatch.setattr(RustWorker, "close", fail_cancel)
    try:
        with pytest.raises(RemoteIOError):
            with transport.transfer(verified, start=512, length=7):
                pytest.fail("uncertain body delivered")
        assert worker.pid is not None
        assert ledger.status()["inflight"] == (32 << 20) + 14
        assert roots[0].exists() and (roots[0] / "body").exists()
        with pytest.raises(RustWorkerError):
            transport.cancel()
        assert roots[0].exists()
        assert ledger.status()["inflight"] == (32 << 20) + 14
    finally:
        release.set()
        original_cancel(worker)
        monkeypatch.setattr(RustWorker, "close", original_close)
        transport.close()
    assert ledger.status()["inflight"] == 14  # retained request, not refunded on death


def test_close_from_consumer_retains_payload_until_exit(twohop):
    _, ledger, transport, obj = twohop
    transport.enable_persistent()
    verified = transport.verify_conditions(obj)
    with transport.transfer(verified, start=512, length=7) as (root, _):
        transport.close()
        assert root.exists()
        assert ledger.status()["inflight"] == (32 << 20) + 14
        with pytest.raises(RemoteIOError):
            with transport.transfer(verified, start=512, length=7):
                pytest.fail("second outstanding operation")
    assert ledger.status()["inflight"] == 0


@pytest.mark.parametrize("boundary", ["resource", "execution"])
def test_close_barrier_before_dispatch_has_no_new_spawn(twohop, monkeypatch, boundary):
    state, ledger, transport, obj = twohop
    transport.enable_persistent()
    verified = transport.verify_conditions(obj)
    count = len(state["calls"])
    worker = transport._lane_worker
    if boundary == "resource":
        reserve = ledger.reserve

        def race(request):
            lease = reserve(request)
            if request.inflight:
                transport.close()
            return lease

        monkeypatch.setattr(ledger, "reserve", race)
    else:
        from contextlib import contextmanager
        execution = transport._execution_worker

        @contextmanager
        def race(*args):
            transport.close()
            with execution(*args) as result:
                yield result

        monkeypatch.setattr(transport, "_execution_worker", race)
    with pytest.raises(RemoteIOError):
        with transport.transfer(verified, start=512, length=7):
            pytest.fail("closed channel dispatched")
    assert len(state["calls"]) == count
    assert worker.pid is None
    assert ledger.status()["inflight"] == 0


@pytest.mark.parametrize("marker", [RuntimeError("body"), KeyboardInterrupt(), SystemExit(7)])
def test_transport_context_preserves_body_on_close_failure(twohop, monkeypatch, marker):
    _, _, transport, _ = twohop

    def failed_close():
        raise RuntimeError("close")

    monkeypatch.setattr(transport, "close", failed_close)
    with pytest.raises(type(marker)) as caught:
        with transport:
            raise marker
    assert caught.value is marker
    assert marker.finalization_secondary == ("transport_close",)


def test_oneshot_active_cannot_enable_persistent(twohop):
    _, _, transport, obj = twohop
    verified = transport.verify_conditions(obj)
    with transport.transfer(verified, start=512, length=7):
        with pytest.raises(RemoteIOError):
            transport.enable_persistent()
    assert not getattr(transport, "_persistent", False)


def test_close_registered_before_bridge_initialization(twohop, monkeypatch):
    _, ledger, transport, obj = twohop
    transport.enable_persistent()
    original = RustWorker.__init__
    children = []

    def barrier(worker, *args, **kwargs):
        transport.close()
        assert ledger.status()["inflight"] >= 32 << 20
        original(worker, *args, **kwargs)
        children.append(worker)

    monkeypatch.setattr(RustWorker, "__init__", barrier)
    with pytest.raises(RemoteIOError):
        transport.verify_conditions(obj)
    assert children and all(child.pid is None for child in children)
    assert ledger.status()["inflight"] == 0


def test_actual_full_stdin_pipe_cancel_kills_and_joins(twohop, monkeypatch):
    _, _, transport, _ = twohop
    popen = subprocess.Popen
    script = ("import sys,json,time;sys.stdin.buffer.readline();"
              "print(json.dumps({'type':'ready','protocol_version':1,'worker_version':'fault',"
              "'capabilities':['bounded_session_v1']}),flush=True);time.sleep(60)")

    def fault_process(*args, **kwargs):
        return popen([sys.executable, "-I", "-c", script], **kwargs)

    monkeypatch.setattr(subprocess, "Popen", fault_process)
    worker = RustWorker(transport.worker)
    errors = []

    def flood():
        try:
            for _ in range(128):
                worker.send_raw(b"x" * 65535 + b"\n")
        except BaseException as error:
            errors.append(error)

    writer = threading.Thread(target=flood)
    writer.start()
    assert worker._writing.wait(timeout=2)
    worker.cancel()
    writer.join(timeout=3)
    assert not writer.is_alive()
    assert worker.pid is None
    assert not worker._reader.is_alive() and not worker._stderr_reader.is_alive()
    assert errors


def test_persistent_lane_unknown_is_not_reusable(twohop):
    state, ledger, transport, obj = twohop
    transport.enable_persistent()
    verified = transport.verify_conditions(obj)
    state["mode"] = "short"
    with pytest.raises(RemoteIOError):
        with transport.transfer(verified, start=512, length=7):
            pytest.fail("ambiguous body delivered")
    count = len(state["calls"])
    state["mode"] = "ok"
    with pytest.raises(RemoteIOError):
        with transport.transfer(verified, start=512, length=7):
            pytest.fail("dead lane reused")
    assert len(state["calls"]) == count
    transport.close()


def test_persistent_idle_rotation_has_fresh_process(twohop):
    _, ledger, transport, obj = twohop
    transport.enable_persistent()
    verified = transport.verify_conditions(obj)
    worker = transport._lane_worker
    transport._lane_requests = 256
    with pytest.raises(RemoteIOError):
        with transport.transfer(verified, start=512, length=7):
            pytest.fail("old generation proof reused")
    assert worker.pid is None
    verified = transport.verify_conditions(obj)
    with transport.transfer(verified, start=512, length=7):
        assert worker.pid is None
        assert transport._lane_worker is not worker
    transport.close()
    assert ledger.status()["inflight"] == 0
