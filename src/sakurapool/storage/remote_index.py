"""P4 synthetic-fixture sequential TAR staging and P2 v4 seam.

Production scan/build is BLOCKED until the 4 GiB application data
working-set budget is proven, including SQLite rollback journals, spools,
compiler and temporary copies.
Offline fixtures may exercise the parser, P2 writer and P3 compiler without
claiming that a synthetic run proves the target-service or disk safety gate.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import tarfile
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

from .. import indexer
from ..records import canonical_object_id
from ..registry import DatasetAdapter
from .budget import DEFAULT_WORK_ROOT, BudgetExceeded, BudgetLedger, Reservation
from .transport import READ_CHUNK, BoundObject, GuardedTransport, RemoteIOError

# Synthetic-fixture allowances only; NOT a proven working-set budget bound.
# Production paths reject before creating files or making HTTP requests.
OFFLINE_BUILD_ALLOWANCE = 1152 * (1 << 20)
OFFLINE_FRAGMENT_CAP = 192 * (1 << 20)
MAX_STAGED_MEMBERS = 200_000
MAX_PATH_BYTES = 4096
MAX_JSON_BYTES = 16 << 20
# Page cap bounds the *main database*, NOT journal/temp/whole work root.
MAX_STAGE_PAGES = 131_072
MAX_STAGE_DB_BYTES = MAX_STAGE_PAGES * 4096
OFFLINE_STAGE_ALLOWANCE = 1152 * (1 << 20)


def _offline_only(ledger: BudgetLedger) -> None:
    if not isinstance(ledger, BudgetLedger) or not ledger.offline_mode:
        raise BudgetExceeded("P4 remote scan/compile BLOCKED: 4 GiB working-set budget unproven")


@dataclass(frozen=True)
class StagedObject:
    database: Path
    content_sha256: str
    object_size: int
    members: int
    potential_records: int


class StagedArchive:
    """Only the tarfile-facing P2 pairing/JSON seam, never remote seek/download.

    Stage was already completed and rehashed. The SQLite cursor supplies
    members once; JSON seeks read stored audited bytes, image SHA is read from
    the completed audit rather than trying to seek the remote stream.
    """

    def __init__(self, staged: StagedObject):
        self.db = sqlite3.connect(f"file:{staged.database.as_posix()}?mode=ro", uri=True)
        self.cursor = self.db.execute("SELECT name,offset_data,size FROM members "
                                      "ORDER BY offset_data,name")
        self.fileobj = self
        self.members: list[tarfile.TarInfo] = []
        self._json = b""
        self._cursor = 0

    def __enter__(self) -> StagedArchive:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.db.close()

    def next(self) -> tarfile.TarInfo | None:
        row = self.cursor.fetchone()
        if row is None:
            return None
        name, offset, length = row
        member = tarfile.TarInfo(name)
        member.offset_data, member.size = offset, length
        return member

    def member_sha256(self, name: str) -> str:
        row = self.db.execute("SELECT sha256 FROM members WHERE name=? AND kind='image'",
                              (name,)).fetchone()
        if row is None:
            raise RemoteIOError("missing staged image SHA")
        return row[0]

    def seek(self, offset: int) -> None:
        row = self.db.execute("SELECT json_payload FROM members "
                              "WHERE offset_data=? AND kind='json'", (offset,)).fetchone()
        if row is None:
            raise RemoteIOError("no audited staged JSON at requested extent")
        self._json = row[0]
        self._cursor = 0

    def read(self, size: int) -> bytes:
        chunk = self._json[self._cursor:self._cursor + size]
        self._cursor += len(chunk)
        return chunk


def _hash_file(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(READ_CHUNK), b""):
            result.update(chunk)
    return result.hexdigest()


def open_completed_stage(output: Path, bound: BoundObject,
                         adapter: DatasetAdapter) -> StagedObject:
    """Reuse only a completed matching stage, hashing SQLite, never TAR.

    The caller independently verifies the remote conditional object binding
    (via guarded read/negative If-Match) before claiming same remote version.
    """
    output = Path(output).absolute()
    if (not output.is_relative_to(DEFAULT_WORK_ROOT) or output.is_symlink()
            or not output.is_dir() or any(p.is_symlink() for p in output.parents
                                       if p.is_relative_to(DEFAULT_WORK_ROOT))):
        raise RemoteIOError("unsafe completed staging directory")
    if {p.name for p in output.iterdir()} != {"members.sqlite", "stage.complete"}:
        raise RemoteIOError("unrecognized or incomplete staging file set")
    db_path = output / "members.sqlite"
    marker = output / "stage.complete"
    if (not db_path.is_file() or db_path.is_symlink() or not marker.is_file()
            or marker.is_symlink() or marker.stat().st_size > 4096
            or db_path.stat().st_size > MAX_STAGE_DB_BYTES):
        raise RemoteIOError("unsafe stage marker or database")
    try:
        stamp = json.loads(marker.read_bytes())
    except (UnicodeError, ValueError):
        stamp = None
    adapter_hash = hashlib.sha256(json.dumps(adapter.to_dict(), sort_keys=True,
                                              separators=(",", ":")).encode()).hexdigest()
    if (not isinstance(stamp, dict) or set(stamp) != {
            "sha256", "size", "members", "potential_records", "database_sha256",
            "adapter_sha256", "strong_etag_sha256", "revision"}
            or stamp["size"] != bound.size or stamp["adapter_sha256"] != adapter_hash
            or stamp["strong_etag_sha256"] != hashlib.sha256(
                bound.strong_etag.encode("ascii")).hexdigest()
            or stamp["revision"] != bound.immutable_revision
            or not isinstance(stamp["sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", stamp["sha256"])
            or not isinstance(stamp["database_sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", stamp["database_sha256"])
            or type(stamp["members"]) is not int
            or not 0 <= stamp["members"] <= MAX_STAGED_MEMBERS
            or type(stamp["potential_records"]) is not int
            or not 0 <= stamp["potential_records"] <= 100_000
            or _hash_file(db_path) != stamp["database_sha256"]):
        raise RemoteIOError("completed stage validator/content mismatch")
    return StagedObject(db_path, stamp["sha256"], bound.size,
                        stamp["members"], stamp["potential_records"])


def write_staged_v4(ledger: BudgetLedger,
                    frozen: list[tuple[str, BoundObject, StagedObject]],
                    output: Path, adapter: DatasetAdapter,
                    *, audit_output: Path | None = None) -> dict[str, int]:
    """Build real P2 durable v4 from *all completed* frozen objects.

    No TAR read or network request. P2 scanner pairing/metadata/record writer
    and COMMIT contract are reused. Every object is verified *before* INPUT;
    the only release path is the local P3 compiler operating on this v4 root.
    """
    _offline_only(ledger)
    if not 1 <= len(frozen) <= 3:
        raise ValueError("canary v4 build requires 1..3 completed objects")
    output = Path(output).absolute()
    if not output.is_relative_to(ledger.root) or output.exists() or output.is_symlink():
        raise ValueError("durable output must be fresh under work root")
    if not output.parent.is_dir() or output.parent.is_symlink():
        raise ValueError("durable parent must exist without symlink")
    if audit_output is not None:
        audit_output = Path(audit_output).absolute()
        if (not audit_output.is_relative_to(ledger.root)
                or not audit_output.parent.is_dir() or audit_output.parent.is_symlink()
                or audit_output.exists() or audit_output.is_symlink()
                or audit_output.is_relative_to(output)):
            raise ValueError("audit output must be fresh, separate from P2 durable root")
    seen = set()
    validators = {}
    for rel, bound, stage in frozen:
        canonical_object_id(rel)
        if rel in seen or not rel.lower().endswith(".tar"):
            raise ValueError("duplicate or non-TAR frozen path")
        seen.add(rel)
        verified = open_completed_stage(stage.database.parent, bound, adapter)
        if verified != stage:
            raise RemoteIOError("completed stage changed since frozen plan")
        validators[rel] = {"size": stage.object_size, "mtime_ns": 0,
                           "sha256": stage.content_sha256, "strength": "strong:sha256",
                           "version": 1}
    if sum(stage.potential_records for _, _, stage in frozen) > 100_000:
        raise RemoteIOError("frozen candidate record upper bound exceeds 100k")
    frozen = sorted(frozen, key=lambda item: item[0])
    contract = {"format_version": indexer.FORMAT_VERSION, "builder": indexer.BUILDER,
                "adapter": adapter.to_dict(), "hash_images": True, "inputs": validators}
    # Offline allowance tests cross-process accounting but does not certify
    # the maximum of SQLite journals, sort scratch or runtime files inside
    # the working-set budget.
    total = {"objects": 0, "samples": 0, "annotations": 0, "errors": 0}
    lease = ledger.reserve(Reservation(disk=OFFLINE_BUILD_ALLOWANCE,
                                       records=sum(s.potential_records for _, _, s in frozen)))
    audit_stream = None
    audit_bytes = 0
    audit_first = True
    try:
        output.mkdir(exist_ok=False)
        indexer._atomic(output / "INPUT.json", indexer._json(contract))
        if audit_output is not None:
            audit_stream = audit_output.open("xb")
            audit_stream.write(b"{")
            audit_bytes = 1
        for rel, bound, stage in frozen:
            shard_id = hashlib.sha256(indexer._json([adapter.dataset, rel])).hexdigest()
            files = {name: output / f"{shard_id}.{name}.parquet"
                     for name in indexer.SCHEMAS}
            scope = indexer._SpoolScope()
            try:
                timings = {"header_seconds": 0., "json_seconds": 0.}
                rows = indexer._scan_shard_impl(
                    None, rel, adapter, True, timings, validators[rel],
                    scope=scope, staged_archive=StagedArchive(stage),
                    spool_directory=ledger.root)
                try:
                    indexer._validate_references(rows)
                    if rows.counts["samples"] > 100_000 - total["samples"]:
                        raise RemoteIOError("real indexed record count cap reached")
                    info = indexer._write_fragments(files, rows, lambda _: None,
                                                    byte_limit=OFFLINE_FRAGMENT_CAP)
                    if audit_output is not None:
                        with closing(sqlite3.connect(f"file:{stage.database.as_posix()}?mode=ro",
                                                     uri=True)) as db:
                            for sample in rows.iter_rows("samples"):
                                img = db.execute("SELECT sha256 FROM members WHERE name=?",
                                                 (sample["image_path"],)).fetchone()
                                meta = (db.execute("SELECT sha256 FROM members WHERE name=?",
                                                   (sample["json_path"],)).fetchone()
                                        if sample["json_path"] else None)
                                if img is None or (sample["json_path"] and meta is None):
                                    raise RemoteIOError("staged member missing during audit")
                                entry = {
                                    "identity": [adapter.dataset, adapter.storage_id,
                                                 f"{rel}@sha256-{stage.content_sha256}"],
                                    "image_path": sample["image_path"],
                                    "json_path": sample["json_path"],
                                    "image": {"offset": sample["offset_data"],
                                              "size": sample["size"], "sha256": img[0]},
                                    "json": ({"offset": sample["json_offset_data"],
                                              "size": sample["json_size"],
                                              "sha256": meta[0]} if meta else None),
                                    "suffix": "." + sample["image_format"]}
                                encoded = json.dumps(sample["record_id"], ensure_ascii=False)
                                encoded += ":" + json.dumps(entry, ensure_ascii=False,
                                                             separators=(",", ":"))
                                data = (("" if audit_first else ",") + encoded).encode("utf-8")
                                if audit_bytes + len(data) + 1 > 64 * (1 << 20):
                                    raise RemoteIOError("scan audit exceeds reserved size cap")
                                audit_stream.write(data)
                                audit_bytes += len(data)
                                audit_first = False
                    for name in total:
                        total[name] += rows.counts[name]
                finally:
                    rows.close()
            except BaseException:
                scope.cleanup()
                raise
            marker = output / f"{shard_id}.COMMIT"
            commit = {"schema": indexer.FORMAT_VERSION, "builder": indexer.BUILDER,
                      "dataset_id": adapter.dataset,
                      "object_id": f"{rel}@sha256-{stage.content_sha256}",
                      "created_at": datetime.now(timezone.utc).isoformat(),
                      "input": validators[rel], "files": info,
                      "contract_sha256": hashlib.sha256(indexer._json(contract)).hexdigest()}
            indexer._atomic(marker, indexer._json(commit))
        if audit_stream is not None:
            audit_stream.write(b"}")
            audit_stream.flush()
            os.fsync(audit_stream.fileno())
            audit_stream.close()
            audit_stream = None
        ledger.settle(lease, records=total["samples"])
        return total
    except BaseException:
        if audit_stream is not None:
            audit_stream.close()
        # INPUT can exist with partial fragments but no full COMMIT set;
        # never claim success or automatically delete unfamiliar artifacts.
        # Preserve the disk lease until explicit inspected recovery.
        raise


def stage_tar(transport: GuardedTransport, ledger: BudgetLedger,
              bound: BoundObject, output: Path, adapter: DatasetAdapter, *,
              max_records: int = 100_000,
              max_json_bytes: int = MAX_JSON_BYTES) -> StagedObject:
    """Single explicit full-object stream; caller must first verify binding.

    Production rejects before any HTTP request: the working-set budget is
    not yet proven. Synthetic fixtures retain an accounting allowance,
    which is not an independently proven maximum for journal/temp growth.
    Caller chooses a fresh child under the offline test root. Existing output
    (including a completed stage) is never overwritten or silently reused.
    """
    _offline_only(ledger)
    if (not isinstance(transport, GuardedTransport)
            or transport.ledger is not ledger):
        raise ValueError("staging transport and offline ledger must be identical")
    transport._host(bound.url)  # reject external/redirect-origin before stage artifacts
    if (not 0 < max_records <= 100_000 or not 0 < max_json_bytes <= MAX_JSON_BYTES
            or max_records * max_json_bytes > 2**64):
        raise ValueError("invalid staging limits")
    output = Path(output).absolute()
    if not output.is_relative_to(ledger.root) or output == ledger.root:
        raise ValueError("stage output must be inside the fixed budget root")
    for ancestor in (output, *output.parents):
        if ancestor == ledger.root.parent:
            break
        if ancestor.is_symlink() or (hasattr(ancestor, "is_junction")
                                     and ancestor.is_junction()):
            raise ValueError("stage path contains a symlink or junction")
    if not output.parent.is_dir() or not output.parent.resolve().is_relative_to(
            ledger.root.resolve()):
        raise ValueError("stage parent must be an existing real work-root child")
    if output.exists() or output.is_symlink():
        raise FileExistsError("stage output already exists; never overwrite")
    if bound.size > 2 * (1 << 30):
        raise ValueError("remote TAR exceeds 2 GiB single-object limit")
    # Offline accounting allowance, not a proven SQLite/journal bound.
    disk_lease = ledger.reserve(Reservation(disk=OFFLINE_STAGE_ALLOWANCE))
    try:
        output.mkdir(exist_ok=False)
        db_path = output / "members.sqlite"
        with closing(sqlite3.connect(db_path)) as db:
            db.execute("PRAGMA journal_mode=DELETE")
            db.execute("PRAGMA page_size=4096")
            if db.execute(f"PRAGMA max_page_count={MAX_STAGE_PAGES}").fetchone()[0] \
                    > MAX_STAGE_PAGES:
                raise RemoteIOError("staging SQLite page cap not enforceable")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("CREATE TABLE members (name TEXT NOT NULL PRIMARY KEY,"
                       "kind TEXT NOT NULL, offset_data INTEGER NOT NULL,"
                       "size INTEGER NOT NULL, sha256 TEXT NOT NULL,"
                       "json_payload BLOB)")
            count = images = 0
            with transport.stream_object(bound) as stream:
                try:
                    with tarfile.open(fileobj=stream, mode="r|") as archive:
                        for item in archive:
                            if item.isdir():
                                continue
                            if not item.isfile() or item.issparse():
                                raise RemoteIOError("unsupported TAR member type")
                            try:
                                canonical_object_id(item.name)
                            except (TypeError, ValueError):
                                raise RemoteIOError("unsafe TAR member path") from None
                            if len(item.name.encode("utf-8")) > MAX_PATH_BYTES:
                                raise RemoteIOError("TAR member path exceeds staging limit")
                            name = PurePosixPath(item.name)
                            if (any(p.startswith(".") for p in name.parts)
                                    or name.name in adapter.ignored_names):
                                continue
                            suffix = name.suffix.lower()
                            if suffix != ".json" and suffix not in adapter.image_extensions:
                                continue
                            count += 1
                            if count > MAX_STAGED_MEMBERS:
                                raise RemoteIOError("TAR member count exceeds staging cap")
                            if item.offset_data < 0 or item.size < 0 or (
                                    item.offset_data + item.size > bound.size):
                                raise RemoteIOError("invalid TAR member raw extent")
                            is_json = suffix == ".json"
                            if is_json and item.size > min(max_json_bytes,
                                                           adapter.max_json_bytes):
                                raise RemoteIOError("TAR JSON exceeds staging cap")
                            if not is_json:
                                images += 1
                                if images > max_records:
                                    raise RemoteIOError("record cap hit: no publish or tail scan")
                            payload = archive.extractfile(item)
                            if payload is None:
                                raise RemoteIOError("missing regular TAR member payload")
                            digest = hashlib.sha256()
                            json_parts: list[bytes] = []
                            remaining = item.size
                            while remaining:
                                chunk = payload.read(min(remaining, READ_CHUNK))
                                if not chunk:
                                    raise RemoteIOError("short TAR member payload")
                                digest.update(chunk)
                                remaining -= len(chunk)
                                if is_json:
                                    json_parts.append(chunk)
                            value = b"".join(json_parts) if is_json else None
                            db.execute("INSERT INTO members VALUES (?,?,?,?,?,?)",
                                       (item.name, "json" if is_json else "image",
                                        item.offset_data, item.size, digest.hexdigest(), value))
                            if count % 256 == 0:
                                db.commit()
                except (tarfile.TarError, sqlite3.Error):
                    # Raw unsupported archives or duplicate names must not
                    # publish a truncated member index.
                    raise RemoteIOError("invalid or ambiguous remote TAR staging") from None
                content_hash = stream.drain_and_verify()
                if stream.count != bound.size:
                    raise RemoteIOError("incomplete raw TAR stream")
            db.commit()
        # The complete marker is deliberately *last*, AFTER the SQLite fd is
        # closed, and binds its content SHA and the exact adapter configuration.
        database_hash = _hash_file(db_path)
        adapter_hash = hashlib.sha256(json.dumps(adapter.to_dict(), sort_keys=True,
                                                  separators=(",", ":")).encode()).hexdigest()
        stamp = json.dumps({"sha256": content_hash, "size": bound.size,
                            "members": count, "potential_records": images,
                            "database_sha256": database_hash, "adapter_sha256": adapter_hash,
                            "strong_etag_sha256": hashlib.sha256(
                                bound.strong_etag.encode("ascii")).hexdigest(),
                            "revision": bound.immutable_revision},
                           sort_keys=True).encode("ascii")
        marker = output / "stage.complete"
        with marker.open("xb") as handle:
            handle.write(stamp)
            handle.flush()
            os.fsync(handle.fileno())
        result = StagedObject(db_path, content_hash, bound.size, count, images)
        ledger.settle(disk_lease)
        return result
    except BaseException:
        # Pending disk quota is conservatively retained until owned stage
        # files are inspected and removed by explicit recovery, never blindly
        # deleting an unknown file in a user-supplied directory.
        raise
