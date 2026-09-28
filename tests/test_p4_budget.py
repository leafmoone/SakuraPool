"""Offline-only P4 budget tests; never contact ModelScope or spend live quota."""

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

from sakurapool.storage.budget import (
    DEFAULT_WORK_ROOT,
    LIMITS,
    MAX_PENDING_LEASES,
    SLOT_BYTES,
    BudgetCorrupt,
    BudgetExceeded,
    BudgetLedger,
    Reservation,
)


@pytest.fixture
def ledger():
    # Two-slot synthetic ledgers and fixtures live under the one P4 work root;
    # no test subroot can reset the production accounting domain.
    with tempfile.TemporaryDirectory(prefix="offline-budget-", dir=DEFAULT_WORK_ROOT) as temp:
        yield BudgetLedger(Path(temp), _offline_test=True,
                           _test_limits={"body": 20, "metadata": 9, "attempts": 3,
                                         "records": 2, "saved_samples": 1,
                                         "saved_bytes": 5, "inflight": 9})


def test_attempt_is_permanent_and_metadata_charges_raw_body(ledger):
    call = ledger.reserve(Reservation(body=8, metadata=8, attempt=True))
    assert ledger.status()["attempts"] == 1
    with pytest.raises(ValueError, match="metadata calls"):
        ledger.consume_body(call, 1)
    ledger.consume_body(call, 3, metadata=True)
    ledger.settle(call)
    assert ledger.status()["body"] == 3
    assert ledger.status()["metadata"] == 3
    assert ledger.status()["attempts"] == 1
    next_call = ledger.reserve(Reservation(body=8, metadata=5, attempt=True))
    with pytest.raises(BudgetExceeded, match="metadata"):
        ledger.reserve(Reservation(body=2, metadata=2, attempt=True))
    assert ledger.status()["attempts"] == 2  # failed reservation didn't send a request
    ledger.settle(next_call)
    ledger.reserve(Reservation(attempt=True))
    with pytest.raises(BudgetExceeded, match="attempts"):
        ledger.reserve(Reservation(attempt=True))


def test_body_pending_crash_and_restart_without_refund(ledger):
    root = ledger.root
    code = """import sys
from pathlib import Path
from sakurapool.storage.budget import BudgetLedger, Reservation
b=BudgetLedger(Path(sys.argv[1]), _offline_test=True,
 _test_limits={'body':20,'metadata':9,'attempts':3,'records':2,
 'saved_samples':1,'saved_bytes':5,'inflight':9})
lease=b.reserve(Reservation(body=19,attempt=True))
b.consume_body(lease,1)
print(lease,flush=True)
"""
    result = subprocess.run([sys.executable, "-c", code, str(root)],
                            capture_output=True, text=True, check=True)
    assert result.stdout.strip()
    reopened = BudgetLedger(root, _offline_test=True,
                            _test_limits={"body": 20, "metadata": 9, "attempts": 3,
                                          "records": 2, "saved_samples": 1,
                                          "saved_bytes": 5, "inflight": 9})
    assert reopened.status()["body"] == 19  # 1 actual + 18 unavailable pending
    with pytest.raises(BudgetExceeded, match="body"):
        reopened.reserve(Reservation(body=2, attempt=True))
    assert reopened.status()["attempts"] == 1


def test_kill_retains_pending_and_inflight_requires_explicit_release(ledger):
    script = """import sys,time
from pathlib import Path
from sakurapool.storage.budget import BudgetLedger, Reservation
b=BudgetLedger(Path(sys.argv[1]), _offline_test=True,
 _test_limits={'body':20,'metadata':9,'attempts':3,'records':2,
 'saved_samples':1,'saved_bytes':5,'inflight':9})
print(b.reserve(Reservation(body=18,inflight=8,attempt=True)),flush=True)
time.sleep(30)
"""
    child = subprocess.Popen([sys.executable, "-c", script, str(ledger.root)],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip()
        child.kill()
        child.communicate(timeout=5)
        reopened = BudgetLedger(ledger.root, _offline_test=True,
                                _test_limits={"body": 20, "metadata": 9, "attempts": 3,
                                              "records": 2, "saved_samples": 1,
                                              "saved_bytes": 5, "inflight": 9})
        assert reopened.status()["body"] == 18
        assert reopened.status()["inflight"] == 8
        with pytest.raises(BudgetExceeded, match="inflight"):
            reopened.reserve(Reservation(inflight=2))
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate(timeout=5)


def test_atomic_cross_process_race(ledger):
    # Two independent processes competing for 20 reserved bytes; exactly one
    # succeeds and outstanding quota remains after both exit.
    code = """import sys
from pathlib import Path
from sakurapool.storage.budget import BudgetLedger, BudgetExceeded, Reservation
b=BudgetLedger(Path(sys.argv[1]), _offline_test=True,
 _test_limits={'body':20,'metadata':9,'attempts':3,'records':2,
 'saved_samples':1,'saved_bytes':5,'inflight':9})
try:
 b.reserve(Reservation(body=14,attempt=True)); print('won',flush=True)
except BudgetExceeded:
 print('blocked',flush=True)
"""
    children = [subprocess.Popen([sys.executable, "-c", code, str(ledger.root)],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 text=True) for _ in range(2)]
    outcomes = []
    for child in children:
        out, err = child.communicate(timeout=20)
        assert child.returncode == 0, err
        outcomes.append(out.strip())
    assert sorted(outcomes) == ["blocked", "won"]
    assert ledger.status()["body"] == 14
    assert ledger.status()["attempts"] == 1


def test_all_quotas_are_atomic_and_disk_includes_unrelated_fixture(ledger):
    first = ledger.reserve(Reservation(body=7, records=2, saved_samples=1,
                                       saved_bytes=5, inflight=9))
    for name in ("records", "saved_samples", "saved_bytes", "inflight"):
        with pytest.raises(BudgetExceeded, match=name):
            ledger.reserve(Reservation(**{name: 1}))
    with pytest.raises(BudgetExceeded, match="body"):
        ledger.consume_body(first, 8)
    ledger.consume_body(first, 7)
    ledger.settle(first, records=2, saved_samples=1, saved_bytes=5)
    assert ledger.status()["records"] == 2
    assert ledger.status()["saved_samples"] == 1
    assert ledger.status()["saved_bytes"] == 5
    assert ledger.status()["inflight"] == 0
    fixture = ledger.root / "user-fixture.bin"
    fixture.write_bytes(b"x" * 1024)
    assert ledger.status()["disk"] >= fixture.stat().st_size
    assert fixture.read_bytes() == b"x" * 1024


def test_disk_reservation_counts_slots_and_cluster_rounded_fixture(ledger):
    measured = ledger.status()["disk"]
    ledger.limits["disk"] = measured + 64
    with pytest.raises(BudgetExceeded, match="disk"):
        ledger.reserve(Reservation(disk=128))
    assert ledger.status()["disk"] >= measured


def test_two_slots_are_exact_and_pending_is_bounded(ledger):
    assert all(path.stat().st_size == SLOT_BYTES for path in ledger.slots)
    assert ledger.lock_path.read_bytes() == b"L"
    with ledger._locked():
        _index, (generation, *_state) = ledger._read_pair()
    assert generation == 0
    assert MAX_PENDING_LEASES * 88 < SLOT_BYTES
    with pytest.raises(ValueError, match="empty"):
        ledger.reserve(Reservation())


def test_near_ceiling_refuses_slot_creation_before_lock_write():
    from sakurapool.storage.budget import _cluster_bytes, _disk_usage
    with tempfile.TemporaryDirectory(prefix="offline-budget-", dir=DEFAULT_WORK_ROOT) as temp:
        root = Path(temp)
        keep = root / "user-fixture.bin"
        keep.write_bytes(b"USER_CONTENT")
        near = (_disk_usage(DEFAULT_WORK_ROOT) + 2 * SLOT_BYTES
                + _cluster_bytes(DEFAULT_WORK_ROOT) - 1)
        with pytest.raises(BudgetExceeded, match="disk"):
            BudgetLedger(root, _offline_test=True, _test_limits={"disk": near})
        assert keep.read_bytes() == b"USER_CONTENT"
        assert not any(path.exists() for path in root.glob("p4-budget.*"))


@pytest.mark.parametrize("cut", ["body-fsync", "header-short", "header-fsync",
                                "after-reserve"])
def test_slot_interruption_never_refunds_attempt_or_silently_resets(ledger, cut):
    code = """import os,sys
from pathlib import Path
from sakurapool.storage.budget import BudgetLedger,Reservation
b=BudgetLedger(Path(sys.argv[1]),_offline_test=True,
 _test_limits={'body':20,'metadata':9,'attempts':3,'records':2,
 'saved_samples':1,'saved_bytes':5,'inflight':9})
cut=sys.argv[2]
real_fsync=os.fsync
n=[0]
def fsync(fd):
 n[0]+=1
 if cut=='body-fsync' and n[0]==1:os._exit(77)
 if cut=='header-fsync' and n[0]==2:os._exit(79)
 real_fsync(fd)
if cut in ('body-fsync','header-fsync'):os.fsync=fsync
if cut=='header-short':
 real_write=b._write_exact
 nwrite=[0]
 def write(stream,data):
  nwrite[0]+=1
  if nwrite[0]==2:
   stream.write(data[:16]);stream.flush();os._exit(78)
  real_write(stream,data)
 b._write_exact=write
b.reserve(Reservation(body=10,attempt=True))
if cut=='after-reserve':os._exit(80)
"""
    result = subprocess.run([sys.executable, "-c", code, str(ledger.root), cut],
                            capture_output=True, text=True, timeout=20, check=False)
    assert result.returncode == {"body-fsync": 77, "header-short": 78,
                                 "header-fsync": 79, "after-reserve": 80}[cut], result.stderr
    if cut == "after-reserve":
        reopened = BudgetLedger(ledger.root, _offline_test=True)
        assert reopened.status()["attempts"] == 1
        assert reopened.status()["body"] == 10  # pending, not refunded
    elif cut == "header-fsync":
        # The kernel may already have written the full header before an
        # interrupted fsync. Either charge the new state or block; never zero.
        try:
            reopened = BudgetLedger(ledger.root, _offline_test=True)
        except BudgetCorrupt:
            pass
        else:
            assert reopened.status()["attempts"] == 1
    else:
        with pytest.raises(BudgetCorrupt):
            BudgetLedger(ledger.root, _offline_test=True)


def test_a_single_invalid_slot_blocks_even_when_other_is_valid(ledger):
    lease = ledger.reserve(Reservation(body=3, attempt=True))
    with ledger.slots[0].open("r+b") as stream:
        stream.seek(200)
        byte = stream.read(1)
        stream.seek(200)
        stream.write(bytes([byte[0] ^ 1]))
    with pytest.raises(BudgetCorrupt, match="checksum"):
        BudgetLedger(ledger.root, _offline_test=True)
    assert lease


def test_full_proof_slot_is_fixed_and_refuses_extra_proof(ledger):
    proofs = {f"{i:064x}": "f" * 64 for i in range(2048)}
    with ledger._locked():
        index, (generation, used, leases, _) = ledger._read_pair()
        assert len(ledger._encode(generation + 1, used, leases, proofs)) == SLOT_BYTES
        ledger._write_slot(ledger.slots[1-index], generation + 1, used, leases, proofs)
    with pytest.raises(BudgetExceeded, match="proof count"):
        ledger.record_condition_proof("e" * 64, "a" * 64)
    reopened = BudgetLedger(ledger.root, _offline_test=True)
    assert reopened.condition_proof(f"{17:064x}") == "f" * 64


def test_existing_ledger_reopens_near_disk_limit_without_second_slot_allowance(
        ledger, monkeypatch):
    from sakurapool.storage import budget

    # Disk is deliberately accounted at DEFAULT_WORK_ROOT, not ledger.root.
    # An unrelated test/worker can add a cluster anywhere under that root
    # between status calls. Freeze just this test's physical-disk scan; never
    # change the production calculation or admit a one-cluster epsilon.
    measured = ledger.status()["disk"]
    original_usage = budget._disk_usage
    physical = ledger._physical_root
    monkeypatch.setattr(budget, "_disk_usage", lambda root: (
        measured if root == physical else original_usage(root)))
    before = tuple((path.stat().st_size, path.stat().st_ino) for path in ledger.slots)
    reopened = BudgetLedger(ledger.root, _offline_test=True,
                            _test_limits={"disk": measured + 1})
    assert reopened.status()["disk"] == measured
    assert tuple((path.stat().st_size, path.stat().st_ino)
                 for path in reopened.slots) == before
    with pytest.raises(BudgetExceeded, match="disk"):
        reopened.reserve(Reservation(disk=2))
    assert reopened.status()["attempts"] == 0
    assert tuple((path.stat().st_size, path.stat().st_ino)
                 for path in reopened.slots) == before


@pytest.mark.parametrize("suffix", ["", "-journal", "-wal", "-shm"])
def test_legacy_sqlite_or_orphan_sidecar_blocks_first_bootstrap(suffix):
    with tempfile.TemporaryDirectory(prefix="offline-budget-", dir=DEFAULT_WORK_ROOT) as temp:
        root = Path(temp)
        legacy = root / ("p4-budget.sqlite" + suffix)
        legacy.write_bytes(b"old-unknown-ledger")
        with pytest.raises(BudgetCorrupt, match="legacy"):
            BudgetLedger(root, _offline_test=True)
        assert legacy.read_bytes() == b"old-unknown-ledger"
        assert not (root / "p4-budget.lock").exists()


@pytest.mark.parametrize("issue", ["pending-disk", "used-metadata", "pending-metadata"])
def test_resigned_semantically_invalid_slot_is_rejected(ledger, issue):
    with ledger._locked():
        index, (generation, used, leases, proofs) = ledger._read_pair()
        if issue == "pending-disk":
            leases["01" * 16] = {name: 0 for name in (
                "body", "metadata", "disk", "records", "saved_samples",
                "saved_bytes", "inflight", "consumed_body", "consumed_metadata")}
            leases["01" * 16]["disk"] = LIMITS["disk"] + 1
        elif issue == "used-metadata":
            used["body"] = 1
            used["metadata"] = 2
        else:
            leases["01" * 16] = {name: 0 for name in (
                "body", "metadata", "disk", "records", "saved_samples",
                "saved_bytes", "inflight", "consumed_body", "consumed_metadata")}
            leases["01" * 16]["body"] = 1
            leases["01" * 16]["metadata"] = 2
        ledger._write_slot(ledger.slots[1 - index], generation + 1, used, leases, proofs)
    with pytest.raises(BudgetCorrupt):
        BudgetLedger(ledger.root, _offline_test=True)


@pytest.mark.parametrize("partial", ["before-slots", "after-first-slot"])
def test_partial_first_creator_never_auto_initializes_or_replaces(partial):
    with tempfile.TemporaryDirectory(prefix="offline-budget-", dir=DEFAULT_WORK_ROOT) as temp:
        root = Path(temp)
        code = """import os,sys
from pathlib import Path
from contextlib import contextmanager
from sakurapool.storage.budget import BudgetLedger
if sys.argv[2]=='before-slots':
 @contextmanager
 def stop_lock(self):os._exit(87);yield
 BudgetLedger._locked=stop_lock
else:
 original=BudgetLedger._write_slot
 def stop_after_one(self,path,*args):
  original(self,path,*args)
  if path==self.slots[0]:os._exit(88)
 BudgetLedger._write_slot=stop_after_one
BudgetLedger(Path(sys.argv[1]),_offline_test=True)
"""
        result = subprocess.run([sys.executable, "-c", code, str(root), partial],
                                capture_output=True, text=True, timeout=20, check=False)
        assert result.returncode == (87 if partial == "before-slots" else 88), result.stderr
        with pytest.raises(BudgetCorrupt):
            BudgetLedger(root, _offline_test=True)
        assert (root / "p4-budget.lock").read_bytes() == b"L"


def test_follower_fails_closed_during_creator_bootstrap_then_can_reopen():
    with tempfile.TemporaryDirectory(prefix="offline-budget-", dir=DEFAULT_WORK_ROOT) as temp:
        root = Path(temp)
        ready, resume = root / "ready", root / "resume"
        code = """import sys,time
from pathlib import Path
from contextlib import contextmanager
from sakurapool.storage.budget import BudgetLedger,Reservation
root,ready,resume=map(Path,sys.argv[1:])
original=BudgetLedger._locked
@contextmanager
def paused(self):
 ready.write_bytes(b'waiting')
 for i in range(500):
  if resume.exists():break
  time.sleep(.01)
 else:raise RuntimeError('bootstrap coordination timed out')
 with original(self) as lock:yield lock
BudgetLedger._locked=paused
b=BudgetLedger(root,_offline_test=True)
b.reserve(Reservation(body=3,attempt=True))
"""
        creator = subprocess.Popen([sys.executable, "-c", code, str(root),
                                    str(ready), str(resume)],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True)
        try:
            for _ in range(500):
                if ready.exists():
                    break
                if creator.poll() is not None:
                    raise AssertionError(f"creator exited before lock: {creator.stderr.read()}")
                time.sleep(.01)
            assert ready.read_bytes() == b"waiting"
            assert (root / "p4-budget.lock").read_bytes() == b"L"
            with pytest.raises(BudgetCorrupt, match="slot"):
                BudgetLedger(root, _offline_test=True)
        finally:
            resume.write_bytes(b"continue")
            out, err = creator.communicate(timeout=25)
            assert creator.returncode == 0, (out, err)
        reopened = BudgetLedger(root, _offline_test=True)
        assert reopened.status()["attempts"] == 1
        assert reopened.status()["body"] == 3


def test_no_alternate_production_root_or_unknown_file(ledger):
    with pytest.raises(ValueError, match="fixed"):
        BudgetLedger(ledger.root)
    unknown = ledger.root / "unknown"
    unknown.mkdir()
    (unknown / "p4-budget.sqlite").write_text(json.dumps({"not": "ours"}))
    with pytest.raises(BudgetCorrupt):
        BudgetLedger(unknown, _offline_test=True)
    assert (unknown / "p4-budget.sqlite").read_text() == '{"not": "ours"}'
