"""Resource contracts and real-written synthetic streaming bodies, never live HTTP."""

import ctypes
import hashlib
import io
import json
import os
import tarfile
import threading
from dataclasses import replace

import pytest
from test_r2_production import twohop as _twohop

from sakurapool.registry import DatasetAdapter
from sakurapool.storage.budget import Reservation, _disk_usage
from sakurapool.storage.production_resources import (
    METADATA_CAP,
    RECORD_CAP,
    STAGE_DISK_CAP,
    STREAM_MEMORY,
    ProductionFootprint,
)
from sakurapool.storage.production_stage import _json_bounded
from sakurapool.storage.rust_index import RustScanAuditError

twohop = _twohop


@pytest.mark.parametrize("requested", [1, 64 << 10, 1 << 20, 8 << 20])
def test_range_requested_bytes_only(requested):
    footprint = ProductionFootprint.admit("range", requested)
    assert footprint.memory == (32 << 20) + 2 * requested
    assert footprint.artifacts == requested
    assert footprint.memory < 256 << 20


@pytest.mark.parametrize("mode", ["download-then-scan", "remote-stream-scan"])
def test_stream_memory_independent_tar_size(mode):
    small = ProductionFootprint.admit(mode, 64 << 20)
    large = ProductionFootprint.admit(mode, 128 << 20)
    assert small.memory == large.memory == STREAM_MEMORY == 128 << 20
    assert small.artifacts == METADATA_CAP + RECORD_CAP + 4096 + (
        small.object_bytes if mode == "download-then-scan" else 0
    )
    phases = large.admin_phases(1152 << 20)
    assert large.admin_peak(1152 << 20) == max(phases.values())
    assert phases["stage"] == large.transfer_disk + STAGE_DISK_CAP
    if mode == "download-then-scan":
        assert phases["durable"] >= large.object_bytes + (1152 << 20)


def test_limits_reject_before_unbounded_json_decode():
    for payload in (b"[" * 33 + b"0" + b"]" * 33, b"[" + b"0," * 32768 + b"0]"):
        with pytest.raises(RustScanAuditError, match="structural"):
            _json_bounded(payload)
    with pytest.raises(ValueError):
        ProductionFootprint.admit("range", (8 << 20) + 1)


class RepeatedBytes:
    """Actual writes from a fixed 64KiB buffer, not truncate/sparse allocation."""

    def read(self, size):
        return b"image-bytes-" * (size // 12) + b"!" * (size % 12)


def worker_peak_rss(worker):
    return process_rss(int(worker._proc._handle), peak=True)


def process_rss(handle=None, *, peak=False):
    if os.name != "nt":
        return 0  # Explicit accounting remains portable; Windows acceptance observes RSS.
    from ctypes import wintypes

    class Counters(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            *[
                (name, ctypes.c_size_t)
                for name in (
                    "PeakWorkingSetSize",
                    "WorkingSetSize",
                    "QuotaPeakPagedPoolUsage",
                    "QuotaPagedPoolUsage",
                    "QuotaPeakNonPagedPoolUsage",
                    "QuotaNonPagedPoolUsage",
                    "PagefileUsage",
                    "PeakPagefileUsage",
                )
            ],
        ]

    counters = Counters()
    counters.cb = ctypes.sizeof(counters)
    get = ctypes.windll.psapi.GetProcessMemoryInfo
    get.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
    if handle is None:
        ctypes.windll.kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        handle = ctypes.windll.kernel32.GetCurrentProcess()
    assert get(wintypes.HANDLE(handle), ctypes.byref(counters), counters.cb)
    return counters.PeakWorkingSetSize if peak else counters.WorkingSetSize


@pytest.mark.parametrize("mode", ["remote-stream-scan", "download-then-scan"])
@pytest.mark.parametrize("size_mib", [64, 128])
def test_large_true_stream_no_tar_disk_and_bounded_rss(
    twohop, tmp_path, monkeypatch, size_mib, mode
):
    state, ledger, transport, original = twohop
    path = tmp_path / "actual-written.tar"
    with tarfile.open(path, "w", format=tarfile.USTAR_FORMAT) as tar:
        image = tarfile.TarInfo("large.png")
        image.size = size_mib << 20
        tar.addfile(image, RepeatedBytes())
        payload = b'{"tags":["bounded"],"width":12,"height":34}'
        meta = tarfile.TarInfo("large.json")
        meta.size = len(payload)
        tar.addfile(meta, io.BytesIO(payload))
    state["file"] = path
    obj = replace(original, object_size=path.stat().st_size)
    bound = transport.verify_conditions(obj)
    from sakurapool.storage import production

    worker_type = production.RustWorker
    peaks = []

    class ObservedWorker(worker_type):
        def read_raw(self):
            value = super().read_raw()
            peaks.append(worker_peak_rss(self))
            return value

    monkeypatch.setattr(production, "RustWorker", ObservedWorker)
    baseline_disk = _disk_usage(ledger.root)
    peak_disk = [baseline_disk]
    stop = threading.Event()
    observation_errors = []
    measurement_lock = threading.Lock()
    delete_owned = transport._delete_owned

    def synchronized_delete(root, snapshot):
        # Only this fixture's directory removal shares the measurement lock.
        # No network/read/scan/stage work is held under it. Stage files are added,
        # not deleted; fixture teardown happens after observer stop+join below.
        with measurement_lock:
            return delete_owned(root, snapshot)

    monkeypatch.setattr(transport, "_delete_owned", synchronized_delete)

    def observe():
        try:
            while not stop.wait(0.005):
                with measurement_lock:
                    peak_disk[0] = max(peak_disk[0], _disk_usage(ledger.root))
        except BaseException as exc:
            observation_errors.append(exc)  # Main thread must reject damaged evidence.

    thread = threading.Thread(target=observe, daemon=True)
    thread.start()
    try:
        stage = transport.build_stage(
            bound,
            DatasetAdapter("resource", "synthetic"),
            ledger.root / "stage",
            mode=mode,
        )
    finally:
        stop.set()
        thread.join()
    assert not thread.is_alive()
    assert observation_errors == [], "disk sampler failed; observations are invalid"
    assert stage.potential_records == 1 and stage.members == 2
    retained = list(ledger.root.glob("rust-transfer-*"))
    if mode == "remote-stream-scan":
        assert not retained
        assert peak_disk[0] - baseline_disk < 2 << 20  # sampled metadata/report/stage growth
    else:
        assert len(retained) == 1
        assert (retained[0] / "body").stat().st_size == path.stat().st_size
        assert peak_disk[0] - baseline_disk >= path.stat().st_size
    if os.name == "nt":
        assert max(peaks) < STREAM_MEMORY
    with path.open("rb") as handle:
        digest = hashlib.file_digest(handle, "sha256").hexdigest()
    assert stage.content_sha256 == digest
    assert ledger.status()["inflight"] == 0
    print(
        json.dumps(
            {
                "synthetic_tar_bytes": path.stat().st_size,
                "peak_job_disk_bytes": peak_disk[0] - baseline_disk,
                "peak_root_disk_bytes": peak_disk[0],
                "worker_peak_rss": max(peaks),
                "reserved_memory": STREAM_MEMORY,
                "whole_tar_spool": mode == "download-then-scan",
                "mode": mode,
            }
        )
    )


@pytest.mark.parametrize("resource", ["inflight", "disk"])
def test_capacity_rejection_precedes_network(twohop, resource):
    state, ledger, transport, obj = twohop
    bound = transport.verify_conditions(obj)
    before = len(state["calls"])
    reservation = (
        Reservation(inflight=(256 << 20) - 1)
        if resource == "inflight"
        else Reservation(disk=(4 << 30) - ledger.status()["disk"] - (1 << 20))
    )
    lease = ledger.reserve(reservation)
    from sakurapool.storage.budget import BudgetExceeded

    with pytest.raises(BudgetExceeded):
        transport.build_stage(
            bound,
            DatasetAdapter("resource", "synthetic"),
            ledger.root / "no-stage",
            mode="remote-stream-scan",
        )
    assert len(state["calls"]) == before
    assert not (ledger.root / "no-stage").exists()
    ledger.settle(lease)


def test_dense_metadata_python_native_pipeline_headroom(twohop, tmp_path):
    """32KiB metadata/record, near32MiB aggregate; sampled Python/native increments.

    Not total process RSS admission: loaded interpreter/Arrow baseline disclosed.
    Worker is closed before stage/P2 native conversions, so maxima do not overlap.
    """
    state, ledger, transport, original = twohop
    path = tmp_path / "dense-written.tar"
    payload = json.dumps(
        {"tags": ["bounded"], "caption": "\U0001f338" * 8000}, ensure_ascii=False
    ).encode()
    with tarfile.open(path, "w", format=tarfile.USTAR_FORMAT) as tar:
        for number in range(900):
            for suffix, body in ((".png", b"image"), (".json", payload)):
                member = tarfile.TarInfo(f"{number}{suffix}")
                member.size = len(body)
                tar.addfile(member, io.BytesIO(body))
    state["file"] = path
    obj = replace(original, object_size=path.stat().st_size)
    bound = transport.verify_conditions(obj)
    import gc

    import pyarrow

    from sakurapool.storage.modelscope import ModelScopeDataset
    from sakurapool.storage.remote_index import write_staged_v4
    from sakurapool.storage.transport import BoundObject

    pyarrow.total_allocated_bytes()
    gc.collect()
    baseline = process_rss()
    peak = [baseline]
    stop = threading.Event()

    def sample():
        while not stop.wait(0.005):
            peak[0] = max(peak[0], process_rss())

    thread = threading.Thread(target=sample, daemon=True)
    thread.start()
    try:
        adapter = DatasetAdapter("dense", "synthetic")
        stage = transport.build_stage(
            bound, adapter, ledger.root / "dense-stage", mode="remote-stream-scan"
        )
        provider = ModelScopeDataset(transport, obj.origin, obj.repo_id)
        resolved = BoundObject(
            provider.download_url(obj.revision, obj.object_path),
            obj.object_size,
            obj.revision,
            bound.validator,
            repository=obj.repo_id,
        )
        summary = write_staged_v4(
            ledger,
            [(obj.object_path, resolved, stage)],
            ledger.root / "dense-p2",
            adapter,
            production_transport=transport,
        )
        transport.release_committed_downloads(ledger.root / "dense-p2")
        assert summary["samples"] == 900 and summary["errors"] == 0
    finally:
        stop.set()
        thread.join()
    if os.name == "nt":
        assert peak[0] - baseline < STREAM_MEMORY
    print(
        json.dumps(
            {
                "dense_metadata_bytes": len(payload) * 900,
                "python_native_baseline_rss": baseline,
                "python_native_sampled_rss": peak[0],
                "sampled_job_rss_delta": peak[0] - baseline,
                "sampling_ms": 5,
                "reserved_memory": STREAM_MEMORY,
            }
        )
    )
