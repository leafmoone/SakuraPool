"""Local-only worker failure/cancellation lifetime checks; no network requests."""

import hashlib
import tempfile
import threading
from pathlib import Path

import pytest
from test_p4_transport import DATA, ETAG
from test_p4_transport import http_and_budget as _synthetic_http

from sakurapool.storage.budget import DEFAULT_WORK_ROOT, BudgetLedger
from sakurapool.storage.retrieval import (
    AuditedSample,
    Extent,
    fetch_bound_sample,
    fetch_bounded_samples,
)
from sakurapool.storage.transport import BoundObject, RemoteIOError


@pytest.fixture
def local_fixture():
    yield from _synthetic_http.__wrapped__()


def test_failed_hash_never_publishes_partial_sample_and_keeps_unknown_inflight(
        local_fixture):
    base, ledger, client = local_fixture
    record = "c" * 32
    wrong_image_sha = hashlib.sha256(b"NOT THE IMAGE").hexdigest()
    sample = AuditedSample(record, Extent(0, 3, wrong_image_sha), None, ".jpg")
    bound = BoundObject(base + "/good", len(DATA), strong_etag=ETAG)
    with pytest.raises(RemoteIOError, match="SHA"):
        fetch_bound_sample(client, ledger, bound, sample, ledger.root)
    assert ledger.status()["attempts"] == 1
    assert ledger.status()["inflight"] > 0  # failed handoff is conservatively pending
    assert ledger.status()["saved_samples"] == 0
    assert not (ledger.root / record).exists()
    assert not any(p.name.startswith(".sakurapool-") for p in ledger.root.iterdir())


def test_bounded_worker_failure_awaits_running_workers_without_pending_payload(monkeypatch):
    from sakurapool.storage import retrieval

    with tempfile.TemporaryDirectory(prefix="offline-worker-fault-",
                                     dir=DEFAULT_WORK_ROOT) as temp:
        root = Path(temp)
        ledger = BudgetLedger(root, _offline_test=True)
        stage = root / "samples"
        stage.mkdir()
        ready = threading.Event()
        observed = []
        class FakeTransport:
            def __init__(self):
                self.ledger = ledger
            def clone(self):
                return self
            def __enter__(self):
                return self
            def __exit__(self, *_exc):
                observed.append("closed")
        failure_id = "a" * 32
        second_id = "b" * 32
        digest = hashlib.sha256(b"x").hexdigest()
        samples = [AuditedSample(value, Extent(0, 1, digest), None, ".jpg")
                   for value in (failure_id, second_id)]
        bound = BoundObject("http://127.0.0.1:1/never-requested", 1,
                            strong_etag='"fixture"')
        def failing_worker(_transport, _ledger, _bound, sample, _output, *, merged):
            if sample.record_id == failure_id:
                assert ready.wait(timeout=5)
                raise RuntimeError("synthetic worker fault")
            ready.set()
            return stage / sample.record_id  # futures contain only a path
        monkeypatch.setattr(retrieval, "fetch_bound_sample", failing_worker)
        with pytest.raises(RuntimeError, match="synthetic worker fault"):
            fetch_bounded_samples(FakeTransport(), ledger, bound, samples, stage,
                                  workers=2)
        assert len(observed) == 2  # both session contexts have exited
        assert not list(stage.iterdir())  # synthetic worker published nothing
        assert ledger.status()["attempts"] == ledger.status()["body"] == 0
        with ledger._locked():
            _, (_, _, leases, _) = ledger._read_pair()
            assert not leases
