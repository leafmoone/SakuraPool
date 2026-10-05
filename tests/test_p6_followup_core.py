"""Focused follow-up contracts; no provider traffic."""

import os
import subprocess
import sys
from pathlib import Path
from threading import RLock

import pytest
from conftest import resolve_r1_worker

from sakurapool.storage import production
from sakurapool.storage.prepared_fetch import StreamPlan
from sakurapool.storage.publication_fetch import PublicationFetchError
from sakurapool.storage.transport import RemoteIOError
from sakurapool.tasks.pipeline import _safe_diagnostic


def test_worker_aliases_are_one_resolver(tmp_path):
    binary = tmp_path / "worker"
    binary.write_bytes(b"fixture")
    primary = {"SAKURAPOOL_RUST_WORKER": str(binary)}
    assert Path(resolve_r1_worker(primary)) == binary
    assert resolve_r1_worker(dict(primary, SAKURAPOOL_STREAM_WORKER=str(binary))) == str(binary)
    with pytest.raises(ValueError, match="disagree"):
        resolve_r1_worker(dict(primary, SAKURAPOOL_STREAM_WORKER=str(tmp_path / "other")))


def test_lazy_chunk_generation_credit_and_stale_proof(monkeypatch):
    plan = StreamPlan(0, 9, 9, 7, 3)
    assert plan.chunk_count == 6
    assert list(plan.lengths()) == [3, 3, 3, 3, 3, 1]
    assert plan.generation_body == 128
    assert plan.generation_attempts == 24
    identity = ("origin", "repo", "type", "revision", "path", 16)
    obj = object()
    monkeypatch.setattr(production, "proof_key", lambda *a, **k: "proof")
    lane = production.RustProductionTransport.__new__(production.RustProductionTransport)
    lane._lane_lock = RLock()
    lane._closed = lane._lane_failed = lane._rotating = False
    lane._persistent = True
    lane._lane_worker = object()
    lane._objects = {identity: obj}
    lane._live_proofs = {"proof"}
    lane._test = True
    lane._lane_requests, lane._lane_body, lane._lane_attempts = 250, 0, 0
    lane._generation_budget = lambda **kwargs: {"body": 128, "attempts": 24}
    assert lane.predict_warm(identity, plan)
    for index in range(6):
        changed = list(identity)
        changed[index] = 17 if index == 5 else "changed"
        assert not lane.predict_warm(tuple(changed), plan)
    lane._lane_requests = 251
    assert not lane.predict_warm(identity, plan)
    lane._lane_requests = 250
    lane._generation_budget = lambda **kwargs: {"body": 128, "attempts": 23}
    assert not lane.predict_warm(identity, plan)
    lane._generation_budget = lambda **kwargs: {"body": 128, "attempts": 24}
    lane._live_proofs.clear()
    assert not lane.predict_warm(identity, plan)


def test_coordinator_prediction_does_not_replace_fresh_workspace_reserve(tmp_path, monkeypatch):
    from dataclasses import replace
    from types import SimpleNamespace

    from sakurapool.capacity import CapacityConfig, ResourcePolicy
    from sakurapool.storage.budget import BudgetExceeded, Reservation
    from sakurapool.workspace import Workspace

    workspace = Workspace.init(tmp_path / "w", policy=ResourcePolicy(body=100))
    ledger = workspace.ledger()
    identity = ("origin", "repo", "type", "revision", "path", 16)
    lane = production.RustProductionTransport.__new__(production.RustProductionTransport)
    lane.capacity = CapacityConfig()
    lane._lane_lock = RLock()
    lane._closed = lane._lane_failed = lane._rotating = False
    lane._persistent = True
    lane._lane_worker = object()
    lane._objects = {identity: object()}
    lane._live_proofs = {"proof"}
    lane._test = True
    lane._lane_requests = lane._lane_body = lane._lane_attempts = 0
    monkeypatch.setattr(production, "proof_key", lambda *a, **k: "proof")
    # A coordinator-side proxy may expose limits but must not send lane RPC.
    lane.ledger = SimpleNamespace(
        limits=ledger.limits,
        effective_headroom=0,
        status=lambda: pytest.fail("prediction invoked lane RPC"),
    )
    plan = StreamPlan(0, 9, 9, 7, 3)
    assert lane.predict_warm(identity, plan)
    workspace.update_policy(replace(workspace.policy, body=1))
    assert lane.predict_warm(identity, plan)
    with pytest.raises(BudgetExceeded):
        ledger.reserve(Reservation(body=3))
    assert workspace.inspect()["pending_count"] == 0
    lane._lane_attempts = 512
    assert not lane.predict_warm(identity, plan)


def test_generation_rotation_invalidates_proof_before_reproof():
    from types import SimpleNamespace

    lane = production.RustProductionTransport.__new__(production.RustProductionTransport)
    lane._persistent = True
    lane._closed = lane._lane_failed = lane._rotating = False
    lane._operation_active = lane._proof_group = False
    lane._generation = 1
    lane._lane_requests, lane._lane_body, lane._lane_attempts = 250, 0, 493
    lane._live_proofs = {"stale"}
    lane._objects = {"object": object()}
    worker = SimpleNamespace(_proc=object())
    worker.close = lambda: setattr(worker, "_proc", None)
    lane._lane_worker = worker
    lane._lane_lock = RLock()
    lane.ledger = SimpleNamespace()
    lane._generation_budget = lambda **kwargs: {"body": (16 << 20) + 32, "attempts": 512}
    # A tiny payload fits; its mandatory cold proof topology does not.
    with lane._lane_lock:
        lane._admit_generation((16 << 20) + 16, 20, 5)
    assert worker._proc is None
    assert not lane._live_proofs and not lane._objects
    assert lane._generation == 2
    assert lane._lane_requests == lane._lane_body == lane._lane_attempts == 0


def test_workspace_chunk_accounting_across_fresh_processes(tmp_path):
    from sakurapool.workspace import Workspace

    workspace = Workspace.init(tmp_path / "workspace")
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"))
    code = """
import sys
from sakurapool.workspace import Workspace
from sakurapool.storage.budget import Reservation
from sakurapool.storage.prepared_fetch import StreamPlan
w=Workspace.open(sys.argv[1]); l=w.ledger()
for _,length in StreamPlan(0,9,9,7,3).chunks():
    lease=l.reserve(Reservation(body=length+1,attempt=True))
    l.consume_body(lease,length); l.settle(lease)
lease=l.reserve(Reservation(body=5,attempt=True)); l.consume_body(lease,2)
"""
    subprocess.run(
        [sys.executable, "-c", code, str(workspace.root)], cwd=tmp_path, env=env, check=True
    )
    assert workspace.inspect()["pending_count"] == 1
    assert workspace.ledger().status()["body"] == 14
    subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys
from sakurapool.workspace import Workspace
w=Workspace.open(sys.argv[1]); assert w.inspect()["pending_count"]==1
assert w.ledger().status()["body"]==14
""",
            str(workspace.root),
        ],
        cwd=tmp_path,
        env=env,
        check=True,
    )
    assert workspace.inspect()["pending_count"] == 1


def test_operation_unknown_preserves_only_safe_underlying():
    cause = RemoteIOError(
        "secret https://signed.invalid/?token=x",
        code="cdn_status",
        phase="cdn",
        http_status=403,
        accounting="CONFIRMED",
    )
    error = PublicationFetchError(
        "publication_range",
        delivered=False,
        cleanup_safe=True,
        output_lease="CONFIRMED",
        secondary=(),
        underlying=cause,
    )
    details = _safe_diagnostic(error)
    assert details["accounting"] == "UNKNOWN"
    assert details["cause_accounting"] == "CONFIRMED"
    assert details["cause_code"] == "cdn_status"
    assert details["cause_http_status"] == 403
    assert "secret" not in str(details) and "signed.invalid" not in str(details)
    metadata = PublicationFetchError(
        "publication_metadata",
        delivered=False,
        cleanup_safe=True,
        output_lease="CONFIRMED",
        secondary=(),
    )
    assert _safe_diagnostic(metadata)["code"] == "publication_metadata"
