"""Shared completed-stage builder driven by a Rust scan report (Gate 4).

Both index-build pipelines - download-then-scan and remote stream scan -
stage through this one module so the completed-stage contract (schema,
bounds, audit, stamp, marker ordering) cannot drift between them.

The Rust scan report is the source of truth for member extents and hashes.
Python never re-parses the archive; it audits the report against the raw
bytes (whole hash, per-member extent hash, JSON parseability and size
bound) and then writes the stage.  Every check is fail-closed: any
mismatch raises before the durable marker exists.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path
from posixpath import splitext
from typing import Any, Iterable, Mapping

from sakurapool.registry import DatasetAdapter
from sakurapool.storage.budget import BudgetLedger, Reservation, is_reparse
from sakurapool.storage.remote_index import (
    JSON_OFFSET_INDEX_SQL,
    MAX_STAGE_PAGES,
    OFFLINE_STAGE_ALLOWANCE,
    StagedObject,
)
from sakurapool.storage.transport import BoundObject

# Remote audited payloads retain the bounded transport JSON limit.
MAX_JSON_PAYLOAD_BYTES = 1 << 20


class RustScanAuditError(ValueError):
    """The Rust scan report failed audit against the raw bytes."""


class FileArchive:
    """Bounded file-backed audit source, not a whole-TAR memory buffer."""

    def __init__(self, path: Path):
        self.path = Path(path)
        if not self.path.is_file() or self.path.is_symlink() or is_reparse(self.path):
            raise RustScanAuditError("owned archive file required")
        self.size = self.path.stat().st_size

    def __len__(self):
        return self.size

    def __getitem__(self, span):
        start, end, _ = span.indices(self.size)
        if end-start > MAX_JSON_PAYLOAD_BYTES:
            raise RustScanAuditError("audit extent exceeds bounded JSON read")
        with self.path.open("rb") as f:
            f.seek(start)
            data = f.read(end-start)
            if len(data) != end-start:
                raise RustScanAuditError("archive truncated during audit")
            return data


def _digest(raw, start, size):
    digest = hashlib.sha256()
    while size:
        n = min(size, 65536)
        digest.update(raw[start:start+n])
        start += n
        size -= n
    return digest.hexdigest()


def _audit_scan(scan: Mapping[str, Any], raw: bytes | FileArchive) -> None:
    """Fail-closed audit of the Rust report against the raw object bytes."""
    if int(scan["size"]) != len(raw):
        raise RustScanAuditError("scan size disagrees with raw object size")
    if scan["whole_sha256"] != _digest(raw, 0, len(raw)):
        raise RustScanAuditError("scan whole_sha256 disagrees with raw object")
    seen: set[str] = set()
    for member in scan["members"]:
        name = member["path"]
        if name in seen:
            raise RustScanAuditError(f"duplicate member path: {name!r}")
        seen.add(name)
        if member["kind"] != "file":
            continue
        start = int(member["offset"])
        size = int(member["size"])
        if start % 512 != 0:
            raise RustScanAuditError(f"member extent not 512-aligned: {name!r}")
        if size <= 0 or start + size > len(raw):
            raise RustScanAuditError(f"member extent out of bounds: {name!r}")
        # Independent per-member hash at the Rust-provided extent.
        digest = _digest(raw, start, size)
        if digest != member["sha256"]:
            raise RustScanAuditError(f"member sha256 mismatch: {name!r}")


def _member_rows(
    scan: Mapping[str, Any],
    raw: bytes | FileArchive,
    adapter: DatasetAdapter,
) -> list[tuple[str, str, int, int, str, bytes | None]]:
    rows: list[tuple[str, str, int, int, str, bytes | None]] = []
    for member in scan["members"]:
        if member["kind"] != "file":
            continue  # directory entries are never staged
        suffix = splitext(member["path"])[1].lower()
        if suffix not in adapter.image_extensions and suffix != ".json":
            continue
        is_json = suffix == ".json"
        start = int(member["offset"])
        size = int(member["size"])
        payload = None
        if size <= 0:
            raise RustScanAuditError(f"empty member payload: {member['path']!r}")
        if is_json:
            if size > min(MAX_JSON_PAYLOAD_BYTES, adapter.max_json_bytes):
                raise RustScanAuditError(
                    f"json payload exceeds {adapter.max_json_bytes} bytes: {member['path']!r}"
                )
            payload = raw[start : start + size]
            json.loads(payload)  # staged JSON must parse
        rows.append(
            (
                member["path"],
                "json" if is_json else "image",
                start,
                size,
                member["sha256"],
                payload if is_json else None,
            )
        )
    return rows


def build_stage_from_scan(
    scan: Mapping[str, Any],
    raw: bytes | FileArchive,
    bound: BoundObject,
    adapter: DatasetAdapter,
    stage_dir: Path,
    ledger: BudgetLedger,
    *, _stage_lease: str | None = None,
) -> StagedObject:
    """Populate a completed stage from an audited *Rust scan report*.

    Mirrors ``stage_tar``'s finished-stage contract (schema, stamp, marker
    ordering) but takes member extents/hashes from the Rust result instead
    of re-streaming the TAR.  JSON payloads are taken from the raw slices
    at the Rust-provided offsets.
    """
    try:
        _audit_scan(scan, raw)
        rows = _member_rows(scan, raw, adapter)
    except BaseException:
        if _stage_lease is not None:
            ledger.settle(_stage_lease)
        raise
    return _write_stage(scan, rows, bound, adapter, stage_dir, ledger,
                        _stage_lease=_stage_lease)


def _write_stage(
    scan: Mapping[str, Any],
    rows: Iterable[tuple[str, str, int, int, str, bytes | None]],
    bound: BoundObject, adapter: DatasetAdapter,
    stage_dir: Path, ledger: BudgetLedger | None,
    *, _stage_lease: str | None = None,
) -> StagedObject:
    images = 0
    member_count = 0
    lease = _stage_lease or (
        ledger.reserve(Reservation(disk=OFFLINE_STAGE_ALLOWANCE)) if ledger else None)

    try:
        stage_dir.mkdir(exist_ok=False)
        db_path = stage_dir / "members.sqlite"
        with closing(sqlite3.connect(db_path)) as db:
            db.execute("PRAGMA journal_mode=DELETE")
            db.execute("PRAGMA page_size=4096")
            if (
                db.execute(f"PRAGMA max_page_count={MAX_STAGE_PAGES}").fetchone()[0]
                > MAX_STAGE_PAGES
            ):
                raise AssertionError("staging SQLite page cap not enforceable")
            db.execute("PRAGMA synchronous=FULL")
            db.execute(
                "CREATE TABLE members (name TEXT NOT NULL PRIMARY KEY,"
                "kind TEXT NOT NULL, offset_data INTEGER NOT NULL,"
                "size INTEGER NOT NULL, sha256 TEXT NOT NULL,"
                "json_payload BLOB)"
            )
            db.execute(JSON_OFFSET_INDEX_SQL)
            for row in rows:
                db.execute("INSERT INTO members VALUES (?,?,?,?,?,?)", row)
                member_count += 1
                images += row[1] == "image"
            db.commit()
        database_hash = _file_sha256(db_path)
        adapter_hash = hashlib.sha256(
            json.dumps(adapter.to_dict(), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        stamp = json.dumps(
            {
                "sha256": scan["whole_sha256"],
                "size": bound.size,
                "members": member_count,
                "potential_records": images,
                "database_sha256": database_hash,
                "adapter_sha256": adapter_hash,
                "strong_etag_sha256": hashlib.sha256(bound.strong_etag.encode("ascii")).hexdigest(),
                "revision": bound.immutable_revision,
            },
            sort_keys=True,
        ).encode("ascii")
        marker = stage_dir / "stage.complete"
        with marker.open("xb") as handle:
            handle.write(stamp)
            handle.flush()
            os.fsync(handle.fileno())
        if ledger is not None and lease is not None:
            ledger.settle(lease)
        return StagedObject(db_path, scan["whole_sha256"], bound.size, member_count, images)
    except BaseException:
        if ledger is not None and lease is not None:
            ledger.settle(lease)
        raise


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_stage_from_file_scan(
    scan: Mapping[str, Any], tar_path: Path, bound: BoundObject,
    adapter: DatasetAdapter, stage_dir: Path, ledger: BudgetLedger | None = None,
) -> StagedObject:
    """Stage a trusted local-worker report, reading only JSON payload extents.

    The caller must obtain the report from its own single-pass Rust worker and
    compare its whole hash against the provider validator before invoking this.
    This API does not authenticate arbitrary externally supplied scan reports.
    """
    before = tar_path.stat()
    if before.st_size != scan["size"] or bound.size != scan["size"]:
        raise RustScanAuditError("scan/file/binding size mismatch")
    seen = set()
    previous_end = 0
    for member in scan["members"]:
        name = member["path"]
        if name in seen:
            raise RustScanAuditError(f"duplicate member path: {name!r}")
        seen.add(name)
        if member["kind"] != "file":
            continue
        start, size = member["offset"], member["size"]
        if (type(start) is not int or type(size) is not int or start % 512
                or start < previous_end or size <= 0 or start + size > before.st_size):
            raise RustScanAuditError(f"invalid member extent: {name!r}")
        digest = member["sha256"]
        if (not isinstance(digest, str) or len(digest) != 64
                or any(c not in "0123456789abcdef" for c in digest)):
            raise RustScanAuditError(f"invalid member SHA256: {name!r}")
        previous_end = start + size

    def rows():
        with tar_path.open("rb") as handle:
            for member in scan["members"]:
                if member["kind"] != "file":
                    continue
                suffix = splitext(member["path"])[1].lower()
                if suffix not in adapter.image_extensions and suffix != ".json":
                    continue
                payload = None
                if suffix == ".json":
                    if member["size"] > adapter.max_json_bytes:
                        raise RustScanAuditError("JSON exceeds adapter.max_json_bytes")
                    handle.seek(member["offset"])
                    payload = handle.read(member["size"])
                    if (len(payload) != member["size"] or
                            hashlib.sha256(payload).hexdigest() != member["sha256"]):
                        raise RustScanAuditError("JSON extent hash mismatch")
                    json.loads(payload)
                yield (member["path"], "json" if suffix == ".json" else "image",
                       member["offset"], member["size"], member["sha256"], payload)
            after = tar_path.stat()
            if (before.st_size, before.st_mtime_ns, before.st_ino) != (
                    after.st_size, after.st_mtime_ns, after.st_ino):
                raise RustScanAuditError("local TAR changed during staging")

    return _write_stage(scan, rows(), bound, adapter, stage_dir, ledger)
