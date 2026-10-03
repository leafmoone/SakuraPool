"""Small synthetic ledger persistence cost, not production throughput."""

import os
import tempfile
import time
from pathlib import Path

from sakurapool.storage.budget import DEFAULT_WORK_ROOT, SLOT_BYTES, BudgetLedger, Reservation


def test_small_isolated_ledger_cost(monkeypatch, record_property):
    with tempfile.TemporaryDirectory(dir=DEFAULT_WORK_ROOT, prefix="offline-task-cost-") as raw:
        root = Path(raw)
        ledger = BudgetLedger(root, _offline_test=True)
        entries_before = sum(1 for _ in root.rglob("*"))
        bytes_written = []
        syncs = []
        original_write = BudgetLedger._write_exact
        original_sync = os.fsync

        def write(stream, payload):
            bytes_written.append(len(payload))
            return original_write(stream, payload)

        def sync(fd):
            started = time.perf_counter()
            original_sync(fd)
            syncs.append(time.perf_counter() - started)

        monkeypatch.setattr(BudgetLedger, "_write_exact", staticmethod(write))
        monkeypatch.setattr(os, "fsync", sync)
        started = time.perf_counter()
        lease = ledger.reserve(Reservation(body=5, attempt=True))
        reserved = time.perf_counter()
        ledger.consume_body(lease, 5)
        consumed = time.perf_counter()
        ledger.settle(lease)
        settled = time.perf_counter()
        assert sum(bytes_written) == 3 * SLOT_BYTES
        assert len(syncs) == 6
        assert sum(1 for _ in root.rglob("*")) == entries_before
        assert ledger.status()["body"] == 5 and ledger.status()["attempts"] == 1
        values = {"synthetic_reserve_seconds": reserved - started,
                  "synthetic_consume_seconds": consumed - reserved,
                  "synthetic_settle_seconds": settled - consumed,
                  "slot_write_bytes": sum(bytes_written), "fsync_calls": len(syncs),
                  "fsync_seconds": sum(syncs), "physical_entries_before": entries_before,
                  "physical_entries_after": entries_before}
        for name, value in values.items():
            record_property(name, value)
        print("SYNTHETIC_LEDGER_COST", values)
