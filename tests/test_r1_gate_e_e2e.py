"""Gate E offline end-to-end fixture: raw -> Rust scan -> audit -> Python
adapter/P2 durable v4 -> P3 compile/query -> Rust range fetch.

The Rust worker's sequential tar scan is the source of truth for every
offset/size/sha that P2 records below.  Python never re-parses the archive:
it (a) audits the Rust report by hashing the raw byte slices at the
Rust-provided extents, and (b) stages the audited JSON payloads into the P2
durable v4 writer through the completed-stage contract.

Everything runs on synthetic data under the fixed P4 work root and loopback
HTTP only. No external network, no real repository access.
"""

import base64
import hashlib
import json
import os
import sqlite3
import tarfile
import tempfile
from contextlib import closing
from io import BytesIO
from pathlib import Path
from posixpath import splitext

import pytest
from test_r1_loopback_loop import _WORKER_BINARY, WORKER_JOB_BUDGET, _serve, requires_worker

from sakurapool.registry import DatasetAdapter
from sakurapool.runtime.compiler import compile_runtime
from sakurapool.runtime.inventory import load_p2_inventory
from sakurapool.runtime.query import RuntimeQuerySpec
from sakurapool.runtime.snapshot import RuntimeSnapshot
from sakurapool.storage.budget import DEFAULT_WORK_ROOT, MIB, BudgetLedger, Reservation
from sakurapool.storage.remote_index import (
    MAX_STAGE_PAGES,
    OFFLINE_STAGE_ALLOWANCE,
    StagedObject,
    open_completed_stage,
    write_staged_v4,
)
from sakurapool.storage.rust_bridge import RustWorker
from sakurapool.storage.transport import BoundObject

# 1x1 transparent PNG (68 bytes).
PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJ"
    "AAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==")

FIRST_JSON = json.dumps({"text": "first image", "tags": ["alpha"],
                         "width": 1, "height": 1, "has_alpha": False}).encode()
SECOND_JSON = json.dumps({"text": "second image", "tags": ["beta"]}).encode()

ETAG = '"gate-e-etag"'


def _tar_bytes() -> bytes:
    buf = BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.GNU_FORMAT) as tf:
        for name, data in [("dir/1.png", PNG_1X1), ("dir/1.json", FIRST_JSON),
                           ("dir/2.png", bytes(range(64))),
                           ("dir/2.json", SECOND_JSON)]:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, BytesIO(data))
    return buf.getvalue()


@pytest.fixture
def work_root():
    # Offline budget roots and fixtures must live under the fixed P4 work root.
    with tempfile.TemporaryDirectory(prefix="r1-gate-e-", dir=DEFAULT_WORK_ROOT) as temp:
        yield Path(temp)


def _build_stage(scan: dict, raw: bytes, bound: BoundObject,
                 adapter: DatasetAdapter, stage_dir: Path,
                 ledger: BudgetLedger) -> StagedObject:
    """Populate a completed stage from the *Rust scan report*.

    Mirrors ``stage_tar``'s finished-stage contract (schema, stamp, marker
    ordering) but takes member extents/hashes from the audited Rust result
    instead of re-streaming the TAR.  JSON payloads are taken from the raw
    slices at the Rust-provided offsets.
    """
    rows = []
    for member in scan["members"]:
        if member["kind"] != "file":
            continue  # dir entries are never staged
        suffix = splitext(member["path"])[1].lower()
        if suffix not in adapter.image_extensions and suffix != ".json":
            continue
        is_json = suffix == ".json"
        payload = raw[member["offset"]: member["offset"] + member["size"]]
        assert payload, "empty member payload"
        if is_json:
            json.loads(payload)  # audit: staged JSON must parse
        rows.append((member["path"], "json" if is_json else "image",
                     member["offset"], member["size"], member["sha256"],
                     payload if is_json else None))
    images = sum(1 for row in rows if row[1] == "image")
    lease = ledger.reserve(Reservation(disk=OFFLINE_STAGE_ALLOWANCE))
    try:
        stage_dir.mkdir(exist_ok=False)
        db_path = stage_dir / "members.sqlite"
        with closing(sqlite3.connect(db_path)) as db:
            db.execute("PRAGMA journal_mode=DELETE")
            db.execute("PRAGMA page_size=4096")
            if db.execute(f"PRAGMA max_page_count={MAX_STAGE_PAGES}").fetchone()[0] \
                    > MAX_STAGE_PAGES:
                raise AssertionError("staging SQLite page cap not enforceable")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("CREATE TABLE members (name TEXT NOT NULL PRIMARY KEY,"
                       "kind TEXT NOT NULL, offset_data INTEGER NOT NULL,"
                       "size INTEGER NOT NULL, sha256 TEXT NOT NULL,"
                       "json_payload BLOB)")
            db.executemany("INSERT INTO members VALUES (?,?,?,?,?,?)", rows)
            db.commit()
        database_hash = hashlib.sha256(db_path.read_bytes()).hexdigest()
        adapter_hash = hashlib.sha256(json.dumps(
            adapter.to_dict(), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        stamp = json.dumps({"sha256": scan["whole_sha256"], "size": bound.size,
                            "members": len(rows), "potential_records": images,
                            "database_sha256": database_hash,
                            "adapter_sha256": adapter_hash,
                            "strong_etag_sha256": hashlib.sha256(
                                bound.strong_etag.encode("ascii")).hexdigest(),
                            "revision": bound.immutable_revision},
                           sort_keys=True).encode("ascii")
        marker = stage_dir / "stage.complete"
        with marker.open("xb") as handle:
            handle.write(stamp)
            handle.flush()
            os.fsync(handle.fileno())
        ledger.settle(lease)
        return StagedObject(db_path, scan["whole_sha256"], bound.size,
                            len(rows), images)
    except BaseException:
        ledger.settle(lease)
        raise



@requires_worker
def test_rust_scan_drives_p2_p3_rust_fetch(work_root: Path) -> None:
    raw = _tar_bytes()
    src_dir = work_root / "src"
    src_dir.mkdir()
    tar_path = src_dir / "example.tar"
    tar_path.write_bytes(raw)

    adapter = DatasetAdapter("r1gatee", "synthetic")
    (work_root / "ledger").mkdir()
    ledger = BudgetLedger(
        work_root / "ledger",
        _offline_test=True,
        _test_limits={"body": 4 * MIB, "inflight": 8 * MIB, "attempts": 5},
    )
    server, url, _thread = _serve(raw)
    try:
        with RustWorker(_WORKER_BINARY, job_budget=WORKER_JOB_BUDGET) as worker:
            # 1. Rust sequential scan of the raw object.
            scan = worker.request(
                "scan_tar",
                budget={"body": len(raw), "disk": 0,
                        "inflight": len(raw), "attempts": 1},
                payload={"path": str(tar_path)},
            )

            # 2. Audit the Rust report against the raw bytes without
            #    re-parsing the archive: whole sha, trailing zeros, and one
            #    independent hash per member slice at the Rust extent.
            assert scan["size"] == len(raw)
            assert scan["whole_sha256"] == hashlib.sha256(raw).hexdigest()
            assert 0 < scan["trailing_bytes"] < len(raw)
            assert raw[len(raw) - scan["trailing_bytes"]:]\
                == b"\x00" * scan["trailing_bytes"]
            scanned_files = 0
            for member in scan["members"]:
                start, size = member["offset"], member["size"]
                assert start % 512 == 0
                assert 0 <= start + size <= len(raw)
                if member["kind"] != "file":
                    continue
                digest = hashlib.sha256(raw[start:start + size]).hexdigest()
                assert digest == member["sha256"]
                scanned_files += 1
            assert scanned_files == 4

            # 3. Python adapter: stage the audited scan into the completed
            #    stage contract, then build the P2 durable v4 (no TAR read).
            bound = BoundObject(url, len(raw), strong_etag=ETAG)
            stage = _build_stage(scan, raw, bound, adapter,
                                 work_root / "stage", ledger)
            assert open_completed_stage(work_root / "stage", bound, adapter) == stage
            durable = ledger.root / "durable"
            summary = write_staged_v4(
                ledger, [("gate-e/example.tar", bound, stage)], durable,
                adapter, audit_output=ledger.root / "scan-audit.json",
            )
            assert summary["samples"] == 2
            assert summary["errors"] == 0

            # 4. P3 compile + query + location resolution.
            runtime_root = work_root / "runtime"
            compiled = compile_runtime(load_p2_inventory(durable), runtime_root)
            assert compiled.rid_count == 2
            with RuntimeSnapshot.open(runtime_root) as rt:
                rids = rt.query(
                    RuntimeQuerySpec(all_tags=[("tags", "alpha")])).limit(10)
                assert len(rids) == 1
                loc = rt.location(rids[0])
                obj = rt.object_ref(loc["object_idx"])
                assert obj["object_size"] == len(raw)

            # 5. Rust budget-gated range fetch of exactly the range P3
            #    resolved, over loopback.
            expected_sha = next(m["sha256"] for m in scan["members"]
                                if m["path"] == "dir/1.png")
            reply = worker.fetch_range_gated(
                url, loc["image_offset"],
                loc["image_offset"] + loc["image_size"] - 1, obj["object_size"],
                ledger=ledger, disk_reserve=loc["image_size"],
            )
            assert reply == {"sha256": expected_sha, "bytes": loc["image_size"]}
            # Byte-level agreement: the fetched extent is the original
            # fixture image bytes.
            assert raw[loc["image_offset"]:
                       loc["image_offset"] + loc["image_size"]] == PNG_1X1
            status = ledger.status()
            assert status["attempts"] == 1
            assert status["body"] == loc["image_size"]
    finally:
        server.shutdown()
        server.server_close()
