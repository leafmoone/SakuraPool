"""Gate 4: the shared Rust-scan-driven stage builder rejects bad reports.

Pure-Python audit tests: every bound in ``rust_index._audit_scan`` /
``_member_rows`` is exercised with a synthesized scan dict; nothing here
needs the worker binary, a budget ledger root, or the network.
"""

import hashlib
import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

from sakurapool.registry import DatasetAdapter
from sakurapool.storage.budget import DEFAULT_WORK_ROOT, MIB, BudgetLedger
from sakurapool.storage.rust_index import (
    MAX_JSON_PAYLOAD_BYTES,
    RustScanAuditError,
    build_stage_from_scan,
)
from sakurapool.storage.transport import BoundObject

PNG = b"\x89PNG-fake"
GOLD = b"\x01\x02\x03\x04"
JSON = b'{"text": "x", "tags": ["a"]}'

# Realistic tar layout: every member's data sits on a 512 boundary (header
# + data padded to the next boundary), exactly what the Rust scanner reports.
RAW = (
    b"\x00" * 512  # header 1
    + PNG
    + b"\x00" * (512 - len(PNG))  # data 1, padded
    + b"\x00" * 512  # header 2
    + JSON
    + b"\x00" * (512 - len(JSON))
)  # data 2, padded

PNG_OFF = 512
JSON_OFF = 1536


def _member(path, kind, data, offset):
    return {
        "path": path,
        "kind": kind,
        "offset": offset,
        "size": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }


def _valid_scan():
    return {
        "size": len(RAW),
        "whole_sha256": hashlib.sha256(RAW).hexdigest(),
        "trailing_bytes": 0,
        "members": [
            {"path": "d", "kind": "dir", "offset": 0, "size": 0, "sha256": "0" * 64},
            _member("a.png", "file", PNG, PNG_OFF),
            _member("a.json", "file", JSON, JSON_OFF),
        ],
    }


def _bound():
    return BoundObject("http://127.0.0.1:0/x.tar", len(RAW), strong_etag='"gate4"')


def _adapter():
    return DatasetAdapter("gate4", "synthetic")


def _ledger_root():
    tmp = tempfile.TemporaryDirectory(prefix="r1-gate4-", dir=DEFAULT_WORK_ROOT)
    root = Path(tmp.name)
    (root / "ledger").mkdir()
    return tmp, BudgetLedger(
        root / "ledger",
        _offline_test=True,
        _test_limits={"body": 4 * MIB, "inflight": 8 * MIB, "attempts": 5},
    )


def _expect_audit_error(mutate):
    tmp, ledger = _ledger_root()
    try:
        scan = _valid_scan()
        mutate(scan)
        with pytest.raises(RustScanAuditError):
            build_stage_from_scan(scan, RAW, _bound(), _adapter(), Path(tmp.name) / "stage", ledger)
    finally:
        tmp.cleanup()


def test_size_mismatch_rejected():
    _expect_audit_error(lambda s: s.update(size=len(RAW) + 1))


def test_whole_sha_mismatch_rejected():
    _expect_audit_error(lambda s: s.update(whole_sha256="f" * 64))


def test_duplicate_member_path_rejected():
    def mutate(s):
        s["members"].append(_member("a.png", "file", PNG, 512))

    _expect_audit_error(mutate)


def test_unaligned_extent_rejected():
    def mutate(s):
        s["members"][1]["offset"] = PNG_OFF + 1
        s["members"][1]["sha256"] = hashlib.sha256(
            RAW[PNG_OFF + 1 : PNG_OFF + 1 + len(PNG)]
        ).hexdigest()

    _expect_audit_error(mutate)


def test_extent_out_of_bounds_rejected():
    def mutate(s):
        s["members"][1]["size"] = len(RAW) - 512 + 1

    _expect_audit_error(mutate)


def test_member_sha_mismatch_rejected():
    _expect_audit_error(lambda s: s["members"][1].update(sha256="e" * 64))


def test_empty_member_payload_rejected():
    def mutate(s):
        m = s["members"][1]
        m.update(size=0, sha256=hashlib.sha256(b"").hexdigest())

    _expect_audit_error(mutate)


def test_unparsable_json_rejected():
    bad = b"{not json"
    raw = RAW[:JSON_OFF] + bad + RAW[JSON_OFF + len(JSON) :]

    def mutate(s):
        s["size"] = len(raw)
        s["whole_sha256"] = hashlib.sha256(raw).hexdigest()
        m = s["members"][2]
        m["sha256"] = hashlib.sha256(bad).hexdigest()

    tmp, ledger = _ledger_root()
    try:
        scan = _valid_scan()
        mutate(scan)
        with pytest.raises(RustScanAuditError):
            build_stage_from_scan(scan, raw, _bound(), _adapter(), Path(tmp.name) / "stage", ledger)
    finally:
        tmp.cleanup()


def test_oversized_json_rejected():
    big = b'{"text": "' + b"y" * (MAX_JSON_PAYLOAD_BYTES) + b'"}'
    raw = RAW[:JSON_OFF] + big + RAW[JSON_OFF + len(JSON) :]

    tmp, ledger = _ledger_root()
    try:
        scan = _valid_scan()
        scan["size"] = len(raw)
        scan["whole_sha256"] = hashlib.sha256(raw).hexdigest()
        m = scan["members"][2]
        m.update(size=len(big), sha256=hashlib.sha256(big).hexdigest())
        with pytest.raises(RustScanAuditError, match="json payload"):
            build_stage_from_scan(scan, raw, _bound(), _adapter(), Path(tmp.name) / "stage", ledger)
    finally:
        tmp.cleanup()


def test_valid_report_stages_and_marker_is_exact():
    tmp, ledger = _ledger_root()
    try:
        stage_dir = Path(tmp.name) / "stage"
        stage = build_stage_from_scan(_valid_scan(), RAW, _bound(), _adapter(), stage_dir, ledger)
        assert stage.members == 2
        assert stage.potential_records == 1
        # Schema: the durable table exists with the audited rows.
        from contextlib import closing

        with closing(sqlite3.connect(stage_dir / "members.sqlite")) as db:
            rows = db.execute(
                "SELECT name, kind, offset_data, size, sha256 FROM members ORDER BY name"
            ).fetchall()
        assert [r[0] for r in rows] == ["a.json", "a.png"]
        assert all(r[1] in {"json", "image"} for r in rows)
        # The marker is the exact canonical stamp.
        stamp = json.loads((stage_dir / "stage.complete").read_bytes())
        assert stamp["sha256"] == hashlib.sha256(RAW).hexdigest()
        assert stamp["members"] == 2
        assert stamp["potential_records"] == 1
    finally:
        tmp.cleanup()


def test_failed_audit_leaves_no_marker_and_settles_lease():
    tmp, ledger = _ledger_root()
    try:
        stage_dir = Path(tmp.name) / "stage"
        scan = _valid_scan()
        scan.update(size=len(RAW) + 1)
        before = ledger.status()["disk"]
        with pytest.raises(RustScanAuditError):
            build_stage_from_scan(scan, RAW, _bound(), _adapter(), stage_dir, ledger)
        assert not stage_dir.exists()
        assert ledger.status()["disk"] == before
    finally:
        tmp.cleanup()
