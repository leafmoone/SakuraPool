"""Formal Rust binding gates on synthetic loopback; no production credentials."""

import hashlib
import importlib.util
import json
import subprocess
import sys
import tempfile
import threading
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from conftest import resolve_r1_worker
from test_r2_production import twohop as _twohop

from sakurapool.storage.budget import DEFAULT_WORK_ROOT, BudgetLedger
from sakurapool.storage.modelscope import ModelScopeDataset
from sakurapool.storage.production import proof_key
from sakurapool.storage.transport import BoundObject, RemoteIOError

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "binding_prep", ROOT / "reports/R2C1B/binding_probe.py"
)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
ETAG = '"offline-sensitive-validator-A"'
TOKEN = "offline-sensitive-token"
COOKIE = "offline-sensitive-cookie"
twohop = _twohop


@pytest.fixture
def binding_loop():
    worker = resolve_r1_worker()
    assert worker, "explicit release worker required"
    state = {"failure": None, "calls": [], "cdn_ops": []}

    class Server(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def do_GET(self):
            headers = {k.lower(): v for k, v in self.headers.items()}
            origin = self.server is servers[0]
            state["calls"].append((origin, headers))
            if origin:
                self.send_response(302)
                self.send_header(
                    "Location",
                    cdn
                    + "/object?Signature=offline-secret&Expires="
                    + str(__import__("time").time_ns() // 10**9 + 600),
                )
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            condition = headers.get("if-match")
            phase = (
                "observe"
                if condition is None
                else "wrong"
                if condition == ('"sakurapool-deliberately-wrong-r2"')
                else "match"
            )
            state["cdn_ops"].append(phase)
            target, mode = state["failure"].split(":") if state["failure"] else (None, None)
            mode = mode if target == phase else None
            code = 412 if phase == "wrong" else 206
            if mode in ("200", "403", "412", "206"):
                code = int(mode)
            raw = b"" if phase == "wrong" else b"A"
            if mode in ("nonempty", "206", "200") and phase == "wrong":
                raw = b"A"
            if mode == "byte":
                raw = b"B"
            self.send_response(code)
            self.send_header("Content-Length", str(len(raw)))
            if phase != "wrong":
                self.send_header(
                    "Content-Range", "bytes 0-0/" + str(1025 if mode == "total" else 1024)
                )
                if mode != "missing":
                    self.send_header(
                        "ETag",
                        "W/" + ETAG
                        if mode == "weak"
                        else '"changed-validator"'
                        if mode == "validator"
                        else ETAG,
                    )
            self.end_headers()
            if raw:
                self.wfile.write(raw)
            self.wfile.flush()

    servers = [ThreadingHTTPServer(("127.0.0.1", 0), Server) for _ in range(2)]
    origin, cdn = [f"http://127.0.0.1:{s.server_port}" for s in servers]
    threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in servers]
    for thread in threads:
        thread.start()
    with tempfile.TemporaryDirectory(prefix="offline-binding-", dir=DEFAULT_WORK_ROOT) as temp:
        ledger = BudgetLedger(Path(temp), _offline_test=True)
        transport = runner.audited_transport(
            ledger,
            worker,
            origin=origin,
            token=TOKEN,
            same_origin_cookie="session=" + COOKIE,
            _test=True,
        )
        candidate = replace(runner.candidate_identity(), origin=origin, object_size=1024)
        yield state, ledger, transport, candidate
    for server in servers:
        server.shutdown()
        server.server_close()
    for thread in threads:
        thread.join(2)


def proofs(ledger):
    with ledger._locked():
        return ledger._read_pair()[1][3]


def test_default_hold_has_no_side_effects(monkeypatch, capsys, tmp_path):
    def forbidden(*_args, **_kwargs):
        pytest.fail("default mode touched side effect")

    monkeypatch.setattr(runner, "future_real_round", forbidden)
    monkeypatch.setattr(runner, "TOKEN_FILE", tmp_path / "absent-secret")
    monkeypatch.setattr(runner, "STARTED", tmp_path / "absent-sentinel")
    monkeypatch.setattr(runner, "WORKER", tmp_path / "absent-worker")
    assert runner.main([]) == 0
    assert capsys.readouterr().out == "HOLD_FOR_USER_AUTHORIZATION\n"
    assert not list(tmp_path.iterdir())
    result = subprocess.run(
        [sys.executable, "-I", "-S", str(ROOT / "reports/R2C1B/binding_probe.py")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0 and result.stdout == "HOLD_FOR_USER_AUTHORIZATION\n"
    assert result.stderr == ""


def test_offline_sentinel_exclusive_only_temp(tmp_path):
    path = tmp_path / "offline-only.started"
    runner.claim_round(path)
    with pytest.raises(FileExistsError):
        runner.claim_round(path)
    assert not runner.STARTED.exists()


def test_verify_conditions_loopback_proof_and_exact_package_lookup(binding_loop):
    state, ledger, transport, candidate = binding_loop
    report = runner.verify_round(transport, candidate)
    assert report["VERSION_BINDING"] == "PASS"
    assert state["cdn_ops"] == ["observe", "match", "wrong"]
    assert ledger.status()["attempts"] == 6 and ledger.status()["body"] == 2
    assert report["budget_after"]["pending_leases"] == 0
    assert ledger.status()["inflight"] == 0
    for origin, headers in state["calls"]:
        assert headers["user-agent"] == "SakuraMoon/1"
        assert headers["accept"] == "application/json, application/octet-stream"
        assert headers["range"] == "bytes=0-0"
        if origin:
            assert headers["authorization"] == "Bearer " + TOKEN
            assert headers["cookie"] == "session=" + COOKIE
            assert "if-match" not in headers
        else:
            assert not any(k in headers for k in ("authorization", "cookie", "referer"))
    bound = transport._objects[candidate.object_path]
    key = proof_key(bound, test=True)
    digest = hashlib.sha256(b"A").hexdigest()
    assert proofs(ledger) == {key: digest}
    assert transport._correct_worker._proc is None  # Real base shutdown reaped before RAM release.
    package_bound = BoundObject(
        ModelScopeDataset(transport, bound.origin, bound.repo_id).download_url(
            bound.revision, bound.object_path
        ),
        bound.object_size,
        bound.revision,
        bound.validator,
        repository=bound.repo_id,
    )
    transport.ensure_verified_condition(
        package_bound,
        endpoint=bound.origin,
        repository=bound.repo_id,
        path=bound.object_path,
        expected_probe_sha256=digest,
    )
    with transport.read_range_owned(package_bound, 0, 1) as raw:
        assert raw == b"A"
    with pytest.raises(RemoteIOError):
        transport.ensure_verified_condition(
            package_bound,
            endpoint=bound.origin,
            repository=bound.repo_id,
            path=bound.object_path,
            expected_probe_sha256="0" * 64,
        )
    for field, value in (
        ("repo_id", "other/repo"),
        ("object_path", "other.tar"),
        ("object_size", 1025),
        ("revision", "a" * 40),
        ("validator", '"different"'),
        ("cdn_host", "other.example"),
        ("origin", "http://127.0.0.1:1"),
    ):
        changed = replace(bound, **{field: value})
        assert proof_key(changed, test=True) != key
        with pytest.raises(RemoteIOError):
            transport.register(changed)
    persisted = json.dumps(report).encode() + b"".join(p.read_bytes() for p in ledger.slots)
    for forbidden in (
        ETAG.encode(),
        TOKEN.encode(),
        COOKIE.encode(),
        b"Signature=",
        b"offline-secret",
        b"/object?",
    ):
        assert forbidden not in persisted


@pytest.mark.parametrize(
    "failure",
    [
        "observe:200",
        "observe:403",
        "observe:weak",
        "observe:missing",
        "observe:total",
        "match:200",
        "match:403",
        "match:412",
        "match:byte",
        "match:validator",
        "wrong:206",
        "wrong:200",
        "wrong:403",
        "wrong:nonempty",
    ],
)
def test_binding_negative_matrix_fail_closed(binding_loop, failure):
    state, ledger, transport, candidate = binding_loop
    state["failure"] = failure
    report = runner.verify_round(transport, candidate)
    assert report["VERSION_BINDING"] == "BLOCKED"
    phase = failure.split(":")[0]
    expected = {
        "observe": ["observe"],
        "match": ["observe", "match"],
        "wrong": ["observe", "match", "wrong"],
    }[phase]
    assert state["cdn_ops"] == expected
    assert not proofs(ledger) and not transport._objects
    assert ledger.status()["attempts"] == len(expected) * 2
    assert ledger.status()["inflight"] == 0
    if phase == "observe":
        assert report["REAL_IF_MATCH_POSITIVE"] == report["REAL_IF_MATCH_NEGATIVE"] == "NOT_RUN"
    elif phase == "match":
        assert report["REAL_RANGE_CAPABILITY"] == "PASS"
        assert report["REAL_IF_MATCH_NEGATIVE"] == "NOT_RUN"
    else:
        assert report["REAL_IF_MATCH_POSITIVE"] == "PASS"
    assert ETAG not in json.dumps(report) and TOKEN not in json.dumps(report)


@pytest.mark.parametrize("failure", ["death", "malformed", "incomplete", "production_error"])
@pytest.mark.parametrize("phase", ["observe", "match", "wrong"])
def test_worker_fault_matrix_retains_unknown_and_no_proof(
    binding_loop, monkeypatch, failure, phase
):
    from sakurapool.storage import production
    from sakurapool.storage.rust_bridge import RustWorkerError

    _state, ledger, transport, candidate = binding_loop
    actual_worker = production.RustWorker

    class Fault:
        def __init__(self, *args, **kwargs):
            self.args, self.kwargs, self.real = args, kwargs, None

        def __enter__(self):
            if transport.binding_phase != phase:
                self.real = actual_worker(*self.args, **self.kwargs)
                return self.real.__enter__()
            if failure == "death":
                raise RustWorkerError(TOKEN)
            return self

        def __exit__(self, *args):
            if self.real is not None:
                return self.real.__exit__(*args)

        def send_raw(self, line):
            self.request_id = json.loads(line)["request_id"]

        def read_raw(self):
            complete = failure == "production_error"
            body = 0 if phase == "wrong" else 1
            accounting = {"body": body, "attempts": 2, "complete": complete}
            result = {
                "accounting": accounting,
                "diagnostic": {
                    "phase": "cdn",
                    "http_status": 403,
                    "attempts": 2,
                    "body_bytes_observed": body,
                    "accounting_complete": complete,
                    **{
                        k: False
                        for k in (
                            "content_length_present",
                            "content_range_present",
                            "etag_present",
                            "etag_is_strong",
                            "content_encoding_present",
                        )
                    },
                },
            }
            if failure == "malformed":
                accounting["body"] = "not-a-number"
            if failure == "production_error":
                result["production_error"] = "cdn_status"
                result["provider_raw"] = TOKEN
            return {"type": "response", "request_id": self.request_id, "ok": True, "result": result}

    monkeypatch.setattr(production, "RustWorker", Fault)
    report = runner.verify_round(transport, candidate)
    assert report["VERSION_BINDING"] == "BLOCKED"
    assert not proofs(ledger) and not transport._objects
    assert ledger.status()["inflight"] == 0
    if transport.binding_phase is None:
        transport.verify_conditions(candidate)  # Surface unexpected offline prerequisite failure.
    assert transport.binding_phase == phase, report
    assert report["budget_after"]["pending_leases"] == (
        0 if failure == "production_error" else 2
    ), report
    assert TOKEN not in json.dumps(report)
    assert not list(ledger.root.glob("rust-transfer-*"))


@pytest.mark.parametrize("artifacts", ["empty", "partial", "unsafe"])
def test_correct_cleanup_ownership_and_dead_worker_memory(binding_loop, monkeypatch, artifacts):
    _state, ledger, transport, candidate = binding_loop
    candidate = replace(candidate, validator=ETAG, cdn_host="127.0.0.1")

    def dead_call(obj, root, **kwargs):
        if artifacts == "partial":
            (root / "body").write_bytes(b"partial")
        elif artifacts == "unsafe":
            (root / "unknown").write_bytes(b"untouched")
        raise RemoteIOError("synthetic primary failure")

    monkeypatch.setattr(transport, "_call", dead_call)
    with pytest.raises(RemoteIOError, match="synthetic primary failure"):
        with transport._capability_match(candidate):
            pytest.fail("failed worker continued")
    assert ledger.status()["inflight"] == 0
    retained = list(ledger.root.glob("rust-transfer-*"))
    assert bool(retained) == (artifacts == "unsafe")
    assert len(runner.snapshot(ledger)["known_settled"]) > 0
    if retained:
        assert (retained[0] / "unknown").read_bytes() == b"untouched"
        assert runner.snapshot(ledger)["pending_leases"] == 1
    else:
        assert runner.snapshot(ledger)["pending_leases"] == 0
    assert not proofs(ledger)


def test_correct_settle_failure_still_cleans_and_preserves_primary(binding_loop, monkeypatch):
    _state, ledger, transport, candidate = binding_loop
    candidate = replace(candidate, validator=ETAG, cdn_host="127.0.0.1")

    def dead_call(obj, root, **kwargs):
        raise RemoteIOError("synthetic primary failure")

    actual_settle = ledger.settle
    calls = []

    def fail_first(lease, **kwargs):
        calls.append(lease)
        if len(calls) == 1:
            raise ValueError("synthetic memory settle failure")
        return actual_settle(lease, **kwargs)

    monkeypatch.setattr(transport, "_call", dead_call)
    monkeypatch.setattr(ledger, "settle", fail_first)
    with pytest.raises(RemoteIOError, match="synthetic primary failure"):
        with transport._capability_match(candidate):
            pytest.fail("failed worker continued")
    assert len(calls) == 2
    assert not list(ledger.root.glob("rust-transfer-*"))
    assert runner.snapshot(ledger)["pending_leases"] == 1
    assert not proofs(ledger)


def test_correct_second_admission_failure_releases_unused_disk(binding_loop, monkeypatch):
    from sakurapool.storage.budget import BudgetExceeded

    _state, ledger, transport, candidate = binding_loop
    candidate = replace(candidate, validator=ETAG, cdn_host="127.0.0.1")
    actual = ledger.reserve

    def fail_memory(reservation):
        if reservation.inflight:
            raise BudgetExceeded("offline memory admission rejected")
        return actual(reservation)

    monkeypatch.setattr(ledger, "reserve", fail_memory)
    with pytest.raises(BudgetExceeded):
        with transport._capability_match(candidate):
            pytest.fail("admission-rejected worker continued")
    assert ledger.status()["inflight"] == 0
    assert runner.snapshot(ledger)["pending_leases"] == 0
    assert not list(ledger.root.glob("rust-transfer-*")) and not proofs(ledger)


def test_correct_cleanup_failure_retains_disk_not_ram_and_preserves_primary(
    binding_loop, monkeypatch
):
    _state, ledger, transport, candidate = binding_loop
    candidate = replace(candidate, validator=ETAG, cdn_host="127.0.0.1")

    def dead_call(obj, root, **kwargs):
        (root / "body").write_bytes(b"partial")
        raise RemoteIOError("synthetic primary failure")

    def cleanup_failure(*args):
        raise OSError("offline cleanup failed")

    monkeypatch.setattr(transport, "_call", dead_call)
    monkeypatch.setattr(transport, "_delete_owned", cleanup_failure)
    with pytest.raises(RemoteIOError, match="synthetic primary failure"):
        with transport._capability_match(candidate):
            pytest.fail("failed worker continued")
    assert ledger.status()["inflight"] == 0
    assert runner.snapshot(ledger)["pending_leases"] == 1
    assert not proofs(ledger)
    assert (next(ledger.root.glob("rust-transfer-*")) / "body").read_bytes() == b"partial"


@pytest.mark.parametrize("worker_state", ["no_spawn", "real_dead", "shutdown_unknown"])
def test_correct_worker_termination_confirmation(binding_loop, monkeypatch, worker_state):
    from types import SimpleNamespace

    from sakurapool.storage import production
    from sakurapool.storage.rust_bridge import RustWorkerError

    _state, ledger, transport, candidate = binding_loop
    candidate = replace(candidate, validator=ETAG, cdn_host="127.0.0.1")
    real_worker = production.RustWorker
    captured = []
    if worker_state == "no_spawn":

        class FailureWorker:
            def __init__(self, *args, **kwargs):
                raise RustWorkerError("offline no spawn")
    elif worker_state == "real_dead":

        class FailureWorker(real_worker):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                captured.append(self._proc)

            def read_raw(self):
                raise RustWorkerError("offline owned worker read failure")
    else:

        class FailureWorker:
            def __init__(self, *args, **kwargs):
                self._proc = SimpleNamespace(poll=lambda: None)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                raise TimeoutError("offline unknown shutdown wait")

            def send_raw(self, line):
                pass

            def read_raw(self):
                raise RustWorkerError("offline worker read failure")

    monkeypatch.setattr(production, "RustWorker", FailureWorker)
    with pytest.raises((RemoteIOError, TimeoutError)):
        with transport._capability_match(candidate):
            pytest.fail("failed worker continued")
    pending = runner.snapshot(ledger)["pending_leases"]
    if worker_state == "shutdown_unknown":
        assert ledger.status()["inflight"] > 0 and pending == 4
        assert list(ledger.root.glob("rust-transfer-*"))
    else:
        assert ledger.status()["inflight"] == 0 and pending == 2
        assert not list(ledger.root.glob("rust-transfer-*"))
    if captured:
        assert captured[0].poll() is not None
    assert not proofs(ledger)


def test_correct_real_child_shutdown_failure_retains_until_confirmed(binding_loop, monkeypatch):
    from types import SimpleNamespace

    from sakurapool.storage import production
    from sakurapool.storage.rust_bridge import RustWorkerError

    _state, ledger, transport, candidate = binding_loop
    candidate = replace(candidate, validator=ETAG, cdn_host="127.0.0.1")
    actual_worker = production.RustWorker
    owned = []

    def failed_close():
        raise OSError("offline owned stdin-close failure")

    def failed_wait(*args, **kwargs):
        raise OSError("offline owned wait failure")

    class ShutdownFailure(actual_worker):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            owned.append((self, self._proc, self._proc.stdin, self._proc.wait))

        def send_raw(self, line):
            pass  # Idle owned child only: no HTTP command sent.

        def read_raw(self):
            raise RustWorkerError("offline primary read failure")

        def close(self):
            self._proc.stdin = SimpleNamespace(close=failed_close)
            self._proc.wait = failed_wait
            super().close()  # Real shutdown path fails before clearing actual Popen.

    monkeypatch.setattr(production, "RustWorker", ShutdownFailure)
    try:
        with pytest.raises(OSError, match="owned wait failure"):
            with transport._capability_match(candidate):
                pytest.fail("shutdown-failed worker continued")
        assert owned and owned[0][1].poll() is None
        assert transport._correct_worker._proc is owned[0][1]
        assert ledger.status()["inflight"] > 0
        assert runner.snapshot(ledger)["pending_leases"] == 4
        assert list(ledger.root.glob("rust-transfer-*")) and not proofs(ledger)
    finally:
        # Restore/reap ONLY the child this fixture created, even if an assertion fails.
        for worker, proc, stdin, wait in owned:
            proc.stdin, proc.wait = stdin, wait
            actual_worker.close(worker)
            assert proc.poll() is not None


def test_c2_append_only_and_transient_classification(tmp_path):
    spec = importlib.util.spec_from_file_location("c2", ROOT / "reports/R2C2/real_binding.py")
    c2 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(c2)
    n1, h1 = c2.numbered_evidence(tmp_path)
    c2.finish(h1, {"synthetic": True})
    n2, h2 = c2.numbered_evidence(tmp_path)
    c2.finish(h2, {"synthetic": False})
    assert (n1, n2) == (1, 2)
    assert json.loads((tmp_path / "round-0001.json").read_text()) == {"synthetic": True}
    report = {
        "VERSION_BINDING": "BLOCKED",
        "REAL_RANGE_CAPABILITY": "PASS",
        "REAL_IF_MATCH_POSITIVE": "BLOCKED",
        "observations": {"match": {"cdn_http_status": 403, "accounting_complete": True}},
    }
    assert c2.classify(report) == "TRANSIENT_OR_UNVERIFIED"
    report["observations"]["match"]["cdn_http_status"] = 412
    assert c2.classify(report) == "TRANSIENT_OR_UNVERIFIED"
    report["observations"]["match"].update(
        same_scope_verified=True, same_validator_verified=True, provider_transient_excluded=True
    )
    assert c2.classify(report) == "SEMANTIC_FAIL"
    report["observations"]["match"]["accounting_complete"] = False
    assert c2.classify(report) == "TRANSIENT_OR_UNVERIFIED"
    report["REAL_IF_MATCH_POSITIVE"] = "PASS"
    report["observations"]["wrong"] = {
        "accounting_complete": True,
        "cdn_http_status": 200,
        "same_scope_verified": True,
        "wrong_condition_accepted": True,
    }
    assert c2.classify(report) == "TRANSIENT_OR_UNVERIFIED"
    report["observations"]["wrong"]["cdn_http_status"] = 206
    assert c2.classify(report) == "SEMANTIC_FAIL"


def test_c2_evidence_finally_exception_and_source_mismatch(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "c2_evidence", ROOT / "reports/R2C2/real_binding.py"
    )
    c2 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(c2)
    identity = {"code_commit": "synthetic", "code_tree": "synthetic"}
    monkeypatch.setattr(c2, "code_identity", lambda: identity)
    monkeypatch.setattr(c2, "safe_snapshot", lambda _: {"synthetic": True})

    def failed_action():
        raise ValueError(TOKEN + ETAG + "Signature=offline-secret")

    _, report = c2.execute_evidenced(
        None, "offline-only", identity, failed_action, directory=tmp_path
    )
    assert report["operation_failed"] is True
    assert TOKEN not in (tmp_path / "round-0001.json").read_text()
    monkeypatch.setattr(c2, "code_identity", lambda: {"code_commit": "changed"})
    called = []
    _, report = c2.execute_evidenced(
        None, "offline-only", identity, lambda: called.append(True), directory=tmp_path
    )
    assert not called and report["identity_mismatch"] is True
    assert report["VERSION_BINDING"] == "BLOCKED"
    assert len(list(tmp_path.glob("*.json"))) == 2
    # Pre-mismatch is sticky even if source is restored before the finally check.
    calls = iter(({"code_commit": "changed"}, identity))
    monkeypatch.setattr(c2, "code_identity", lambda: next(calls))
    _, report = c2.execute_evidenced(
        None, "offline-only", identity, lambda: called.append(True), directory=tmp_path
    )
    assert not called and report["identity_mismatch"] is True
    monkeypatch.setattr(c2, "code_identity", lambda: identity)
    snapshots = iter((None, {"recovered": True}))
    monkeypatch.setattr(c2, "safe_snapshot", lambda _: next(snapshots))
    _, report = c2.execute_evidenced(
        None, "offline-only", identity, lambda: called.append(True), directory=tmp_path
    )
    assert not called and report["ledger_snapshot_failed"] is True
    assert report["status"] == "SOURCE_OR_LEDGER_BLOCKED"
    assert report["origin_status"] == "BLOCKED"
    assert report["budget_before"] is None and report["budget_after"] == {"recovered": True}


def test_c2_small_canary_uses_own_proof_and_both_formal_builders(twohop, monkeypatch):
    from sakurapool.registry import DatasetAdapter

    _state, ledger, transport, candidate = twohop
    transport.verify_conditions(candidate)
    spec = importlib.util.spec_from_file_location(
        "c2_canary_scheduler", ROOT / "reports/R2C2/real_binding.py"
    )
    scheduler = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(scheduler)
    spec = importlib.util.spec_from_file_location("c2_canary", ROOT / "reports/R2C2/canary.py")
    canary = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(canary)
    identity = {"code_commit": "offline-canary", "code_tree": "offline-canary"}
    monkeypatch.setattr(scheduler, "code_identity", lambda: identity)
    # Only test evidence path redirected: ledger/job/HTTP remain formal loopback profile.
    original_evidenced = scheduler.execute_evidenced

    def evidenced(*args, **kwargs):
        return original_evidenced(*args, **kwargs, directory=ledger.root)

    monkeypatch.setattr(scheduler, "execute_evidenced", evidenced)
    adapter = DatasetAdapter("c2", "synthetic", storage_id="remote1", image_extensions=(".png",))
    remote, report = canary.build_one(
        transport, candidate, adapter, "remote-stream-scan", scheduler, identity
    )
    assert report["status"] == "PASS", report
    assert report["runtime_verified"] and report["json_content_sha_independently_verified"]
    assert report["whole_tar_spool"] is False
    download, report = canary.build_one(
        transport, candidate, adapter, "download-then-scan", scheduler, identity
    )
    assert report["status"] == "PASS", report
    assert canary.equivalent(ledger, remote, download)["status"] == "PASS"
    for field, changed in (
        ("object_size", candidate.object_size + 1),
        ("repo_id", "other/repo"),
        ("revision", "a" * 40),
    ):
        with pytest.raises(ValueError, match="own verified proof scope"):
            canary.build_one(
                transport,
                replace(candidate, **{field: changed}),
                adapter,
                "remote-stream-scan",
                scheduler,
                identity,
            )


def load_c2_helpers():
    modules = []
    for name in ("real_binding", "canary"):
        spec = importlib.util.spec_from_file_location(
            "offline_c2_" + name, ROOT / "reports/R2C2" / (name + ".py")
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        modules.append(module)
    return modules


@pytest.mark.parametrize(
    "phase",
    ["discovery", "schema_identification", "remote_builder", "download_builder", "equivalence"],
)
def test_c2_closure_source_failure_propagates_original_status(monkeypatch, phase):
    from types import SimpleNamespace

    scheduler, canary = load_c2_helpers()
    initial = SimpleNamespace(ledger=None)
    candidate = SimpleNamespace(repo_id="offline/repo")
    blocked = {
        "status": "SOURCE_OR_LEDGER_BLOCKED",
        "origin_status": "PASS",
        "identity_mismatch": True,
        "ledger_snapshot_failed": False,
    }
    monkeypatch.setattr(scheduler, "proof_use", lambda *args: {"status": "PASS"})
    monkeypatch.setattr(scheduler, "schedule", lambda *args: (initial, scheduler.outcome("PASS")))
    monkeypatch.setattr(
        canary,
        "discovery",
        lambda *args, **kwargs: (
            (None, blocked) if phase == "discovery" else (candidate, {"status": "PASS"})
        ),
    )
    monkeypatch.setattr(
        canary,
        "identify_adapter",
        lambda *args: (
            (None, blocked) if phase == "schema_identification" else (None, {"status": "PASS"})
        ),
    )
    calls = []

    def build(*args):
        mode = args[3]
        calls.append(mode)
        current = "remote_builder" if mode == "remote-stream-scan" else "download_builder"
        return (None, blocked) if phase == current else ("offline-p2", {"status": "PASS"})

    monkeypatch.setattr(canary, "build_one", build)
    monkeypatch.setattr(scheduler, "execute_evidenced", lambda *args: (None, blocked))
    result = canary.closure(initial, candidate, "offline-only", scheduler, {})
    assert result["status"] == "SOURCE_OR_LEDGER_BLOCKED"
    assert result["origin_status"] == "PASS" and result["origin_phase"] == phase
    assert not result["resumable"]
    if phase == "remote_builder":
        assert calls == ["remote-stream-scan"]
    if phase in ("discovery", "schema_identification"):
        assert not calls


def test_c2_own_binding_pause_remains_resumable(monkeypatch):
    from types import SimpleNamespace

    scheduler, canary = load_c2_helpers()
    initial = SimpleNamespace(ledger=None)
    candidate = SimpleNamespace(repo_id="offline/repo")
    calls = []

    def range_use(*args):
        calls.append("range")
        return {"status": "PASS"}

    monkeypatch.setattr(scheduler, "proof_use", range_use)
    monkeypatch.setattr(
        canary, "discovery", lambda *args, **kwargs: (candidate, {"status": "PASS"})
    )
    monkeypatch.setattr(
        scheduler,
        "schedule",
        lambda *args: (None, scheduler.outcome("PAUSED_FOR_DIAGNOSTICS_NOT_VERIFIED")),
    )
    result = canary.closure(initial, candidate, "offline-only", scheduler, {})
    assert result["status"] == "PAUSED_FOR_DIAGNOSTICS_NOT_VERIFIED"
    assert result["resumable"] and not result["protocol_conclusion"]
    assert result["origin_phase"] == "own_binding" and calls == ["range"]
    assert result["remote"] == result["download"] == "NOT_RUN"


@pytest.mark.parametrize(
    "terminal,exit_code",
    [("PAUSED_FOR_DIAGNOSTICS_NOT_VERIFIED", 4), ("SOURCE_OR_LEDGER_BLOCKED", 3)],
)
def test_c2_main_closure_preserves_terminal_exit_without_network(monkeypatch, terminal, exit_code):
    from types import SimpleNamespace

    from sakurapool.storage import budget

    scheduler, _ = load_c2_helpers()
    monkeypatch.setitem(sys.modules, scheduler.__name__, scheduler)
    monkeypatch.setattr(scheduler, "code_identity", lambda: {})
    monkeypatch.setattr(budget, "BudgetLedger", lambda: None)
    monkeypatch.setattr(scheduler, "load_token", lambda: "offline-only")
    monkeypatch.setattr(scheduler, "schedule", lambda *args: (None, scheduler.outcome("PASS")))
    result = {
        **scheduler.outcome(terminal),
        "remote": "NOT_RUN",
        "download": "NOT_RUN",
        "equivalence": "NOT_AVAILABLE",
    }
    monkeypatch.setattr(scheduler, "execute_evidenced", lambda *args: (None, args[3]()[1]))
    monkeypatch.setattr(
        scheduler.importlib.util,
        "spec_from_file_location",
        lambda *args: SimpleNamespace(loader=SimpleNamespace(exec_module=lambda module: None)),
    )
    monkeypatch.setattr(
        scheduler.importlib.util,
        "module_from_spec",
        lambda spec: SimpleNamespace(closure=lambda *args: result),
    )
    assert scheduler.main(["--run-authorized-c2"]) == exit_code


def test_c2_settlement_failure_additive_primary_and_evidenced(tmp_path, monkeypatch):
    from types import SimpleNamespace

    scheduler, canary = load_c2_helpers()

    def settle_failure(*args):
        raise OSError(TOKEN + ETAG)

    ledger = SimpleNamespace(reserve=lambda reservation: "offline-lease", settle=settle_failure)
    details = {"status": "BLOCKED", "failure_kind": "scan_or_webdataset_audit"}
    canary.settle_preserving(ledger, "offline-lease", details)
    assert details["primary_status"] == "BLOCKED"
    assert details["failure_kind"] == "scan_or_webdataset_audit" and details["settlement_failed"]
    monkeypatch.setattr(canary, "_equivalent_rows", lambda *args: False)
    compared = canary.equivalent(ledger, None, None)
    assert compared["status"] == "SETTLEMENT_BLOCKED" and compared["primary_status"] == "FAIL"
    assert compared["equivalent"] is False
    monkeypatch.setattr(scheduler, "code_identity", lambda: {})
    monkeypatch.setattr(scheduler, "safe_snapshot", lambda ledger: {"offline_only": True})
    _, report = scheduler.execute_evidenced(
        ledger, "offline_settlement", {}, lambda: (None, details), directory=tmp_path
    )
    assert report["failure_kind"] == "scan_or_webdataset_audit" and report["settlement_failed"]
    saved = (tmp_path / "round-0001.json").read_text()
    assert TOKEN not in saved and ETAG not in saved
    assert json.loads(saved)["primary_status"] == "BLOCKED"


def test_c2_build_settlement_failure_preserves_scan_primary(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from sakurapool.registry import DatasetAdapter
    from sakurapool.storage import remote_index
    from sakurapool.storage.rust_index import RustScanAuditError

    scheduler, canary = load_c2_helpers()

    def settle_failure(*args):
        raise OSError(TOKEN + ETAG)

    def scan_failure(*args):
        raise RustScanAuditError(TOKEN + ETAG)

    ledger = SimpleNamespace(
        root=tmp_path, reserve=lambda reservation: "offline-validation", settle=settle_failure
    )
    candidate = replace(runner.candidate_identity(), object_size=1024)
    bound = replace(candidate, validator=ETAG, cdn_host="offline.example")

    def offline_host(origin):
        assert origin == candidate.origin
        return "modelscope.cn"  # Constructor-only validation; no request API in this fake.

    transport = SimpleNamespace(
        ledger=ledger,
        _host=offline_host,
        _objects={bound.object_path: bound},
        last_result={},
        build_stage=lambda *args, **kwargs: None,
        release_committed_downloads=lambda *args: None,
    )
    monkeypatch.setattr(
        remote_index,
        "write_staged_v4",
        lambda *args, **kwargs: {"objects": 1, "samples": 1, "annotations": 0, "errors": 0},
    )
    monkeypatch.setattr(canary, "validate_canary_rows", scan_failure)
    monkeypatch.setattr(scheduler, "code_identity", lambda: {})
    monkeypatch.setattr(scheduler, "safe_snapshot", lambda ledger: {"offline_only": True})
    original_evidenced = scheduler.execute_evidenced
    monkeypatch.setattr(
        scheduler,
        "execute_evidenced",
        lambda *args, **kwargs: original_evidenced(*args, **kwargs, directory=tmp_path),
    )
    _, report = canary.build_one(
        transport,
        candidate,
        DatasetAdapter("offline", "synthetic"),
        "remote-stream-scan",
        scheduler,
        {},
    )
    assert report["status"] == "SETTLEMENT_BLOCKED" and report["primary_status"] == "BLOCKED"
    assert report["failure_kind"] == "scan_or_webdataset_audit" and report["settlement_failed"]
    saved = (tmp_path / "round-0001.json").read_text()
    assert TOKEN not in saved and ETAG not in saved
    assert json.loads(saved)["failure_kind"] == "scan_or_webdataset_audit"
