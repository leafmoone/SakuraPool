"""P3 streaming compiler: committed P2 v4 fragments -> immutable runtime snapshot.

Design (P3-Execution-Plan 0-75):
- rid: dense uint32, canonical order (dataset_id, object_id, sample_path, record_id).
- tag first-seen: canonical input order only (objects by (dataset_id, object_id),
  per-object annotations sorted (rid, namespace, origin), tags sorted
  (value, category)). Never Parquet physical row order, never directory
  enumeration order.
- No long-lived membership table: stage2 streams canonical annotations directly
  into chunked bitmap_parts (§22); a per-object temp table only bounds the
  canonical sort working set to one shard.
- locations.npy: structured mmap array, index == rid, uint64 sizes.
- publication: staging -> SNAPSHOT.json -> READY -> atomic rename
  snapshots/<id> -> atomic current.json. Nothing openable before READY.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import stat
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import numpy as np
import pyarrow
import pyarrow.parquet as pq
import pyroaring
from pyroaring import BitMap

from . import RUNTIME_COMPILER, RUNTIME_FORMAT_VERSION
from .errors import (
    CorruptInputError,
    SnapshotCorruptError,
    SnapshotMixError,
)
from .identity import check_rid_capacity, snapshot_id
from .inventory import P2Inventory

HAS_METADATA = 1
NO_FORMAT = 0
DEFAULT_CHUNK = 500_000
LOCATION_IDENTITY_MARKER = b"SAP3LOC1"
LOCATION_IDENTITY_SIZE = 82  # marker(8) + '\n'(1) + snapshot_id(64) + '\n'(1) + <Q rid_count>(8)


def _u64(value: int | None) -> bytes:
    """Lossless uint64 SQLite encoding: fixed-length big-endian BLOB."""
    if value is None:
        value = 0
    if not isinstance(value, int) or value < 0 or value >= 2**64:
        _fail(f"uint64 value out of range: {value!r}")
    return value.to_bytes(8, "big")


def _from_u64(blob: bytes | None) -> int:
    if not blob:
        return 0
    return int.from_bytes(bytes(blob), "big")


def _sha256_file(path: Path) -> str:
    """Streaming SHA-256 so full-file verification never holds the file in RAM."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _location_identity(snap_id: str, rid_count: int) -> bytes:
    return (LOCATION_IDENTITY_MARKER + b"\n"
            + snap_id.encode("ascii") + b"\n"
            + rid_count.to_bytes(8, "little"))


def _write_location_identity(path: Path, snap_id: str, rid_count: int) -> None:
    with path.open("r+b") as handle:
        handle.seek(0, os.SEEK_END)
        handle.write(_location_identity(snap_id, rid_count))


def _check_location_identity(path: Path, snap_id: str,
                             rid_count: int) -> None:
    """Fast whole-file snapshot binding for locations.npy.

    Detects any foreign or truncated file (same-shape swap from another
    snapshot included). In-place payload edits of the same length are NOT
    detectable here; those are covered only by the full SHA-256 verification.
    """
    # Only the 6-byte npy magic and the 85-byte identity trailer are read:
    # the fast open must not pull a 185 MiB locations.npy into Python RAM.
    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        size = handle.tell()
        if size < LOCATION_IDENTITY_SIZE:
            raise SnapshotCorruptError("locations.npy identity trailer missing")
        handle.seek(0)
        magic = handle.read(6)
        handle.seek(-LOCATION_IDENTITY_SIZE, os.SEEK_END)
        trailer = handle.read(LOCATION_IDENTITY_SIZE)
    if magic != b"\x93NUMPY":
        raise SnapshotCorruptError("locations.npy identity trailer missing")
    if (trailer[:8] != LOCATION_IDENTITY_MARKER
            or trailer[9:73].decode("ascii") != snap_id
            or int.from_bytes(trailer[74:82], "little") != rid_count):
        raise SnapshotMixError(
            "locations.npy snapshot identity mismatch (mixed snapshot?)")

LOCATION_DTYPE = np.dtype([
    ("object_idx", "u4"),
    ("image_offset", "u8"),
    ("image_size", "u8"),
    ("metadata_offset", "u8"),
    ("metadata_size", "u8"),
    ("format_id", "u2"),
    ("flags", "u1"),
])

_STAGES = ("stage1", "stage2", "catalog", "bitmaps", "locations", "snapshot",
           "ready")


def _fail(message: str) -> Any:
    raise CorruptInputError(message)


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def _read_batches(
    path: Path, *, columns: list[str] | None = None
) -> Iterator[list[dict[str, Any]]]:
    with pq.ParquetFile(path) as handle:
        # Footer-only empty fragments are valid P2 output, not missing input.
        if handle.num_row_groups == 0:
            return
        for batch in handle.iter_batches(batch_size=10_000, columns=columns):
            yield batch.to_pylist()


def _ensure_no_wal(path: Path) -> None:
    for suffix in ("-wal", "-shm", "-journal"):
        side = Path(str(path) + suffix)
        if side.exists():
            _fail(f"leftover SQLite sidecar after close: {side.name}")


class _Stage:
    def __init__(self, staging: Path) -> None:
        self.path = staging / "STAGE.txt"
        self.current = -1
        if self.path.exists():
            try:
                self.current = _STAGES.index(self.path.read_text(encoding="utf-8").strip())
            except (ValueError, OSError):
                self.current = -1

    def done(self, stage: str) -> bool:
        return _STAGES.index(stage) <= self.current

    def complete(self, stage: str) -> None:
        self.path.write_text(stage, encoding="utf-8")


@dataclass(frozen=True)
class CompiledSnapshot:
    snapshot_id: str
    path: Path
    rid_count: int
    object_count: int
    source_count: int
    dataset_count: int
    tag_count: int
    tag_memberships: int


def _fragment_path(obj: Any, name: str) -> Path:
    for fragment in obj.fragments:
        if fragment.name == name:
            return fragment.path
    _fail(f"missing fragment {name}: {obj.object_id}")


def _stage1(inventory: P2Inventory, staging: Path) -> None:
    """Stream samples into compiler-staging.sqlite and assign canonical rids."""
    db_path = staging / "compiler-staging.sqlite"
    if db_path.exists():
        db_path.unlink()
    db = sqlite3.connect(db_path)
    try:
        db.execute("PRAGMA journal_mode=OFF")
        db.execute("PRAGMA synchronous=OFF")
        db.execute(
            """
            CREATE TABLE samples (
                seq INTEGER PRIMARY KEY,
                dataset_id TEXT NOT NULL,
                object_id TEXT NOT NULL,
                sample_path TEXT NOT NULL,
                record_id BLOB(16) NOT NULL,
                source TEXT NOT NULL,
                post_id TEXT NOT NULL,
                image_offset BLOB(8) NOT NULL,
                image_size BLOB(8) NOT NULL,
                metadata_offset BLOB(8) NOT NULL DEFAULT x'0000000000000000',
                metadata_size BLOB(8) NOT NULL DEFAULT x'0000000000000000',
                has_metadata INTEGER NOT NULL DEFAULT 0,
                image_format TEXT
            )
            """)
        db.execute("CREATE UNIQUE INDEX samples_record_unique ON samples(record_id)")
        objects = sorted(inventory.objects, key=lambda o: (o.dataset_id, o.object_id))
        insert_sql = (
            "INSERT INTO samples (dataset_id, object_id, sample_path, record_id, source,"
            " post_id, image_offset, image_size, metadata_offset, metadata_size,"
            " has_metadata, image_format) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")
        sample_count = 0
        for obj in objects:
            # Inventory validation still checks the full schema and file hash.
            # Avoid materializing unused text, tags and hashes as Python objects.
            for batch in _read_batches(_fragment_path(obj, "samples"), columns=[
                "dataset_id", "object_id", "sample_path", "record_id", "source",
                "post_id", "offset_data", "size", "json_offset_data", "json_size",
                "json_path", "image_format",
            ]):
                db.executemany(insert_sql, [
                    (s["dataset_id"], s["object_id"], s["sample_path"],
                     bytes.fromhex(s["record_id"]), s["source"], s["post_id"],
                     _u64(s["offset_data"]), _u64(s["size"]),
                     _u64(s["json_offset_data"]), _u64(s["json_size"]),
                     1 if s["json_path"] is not None else 0, s["image_format"])
                    for s in batch
                ])
                sample_count += len(batch)
        check_rid_capacity(sample_count)
        db.execute(
            "CREATE TABLE rids AS SELECT seq, row_number() OVER ("
            "ORDER BY dataset_id, object_id, sample_path, record_id) - 1 AS rid "
            "FROM samples")
        db.execute("CREATE INDEX rids_seq ON rids(seq)")
        db.commit()
        staging.joinpath("STAGE1-COUNTS.json").write_text(
            json.dumps({"samples": sample_count}), encoding="utf-8")
        db.close()
        _ensure_no_wal(db_path)
    except BaseException:
        db.close()
        raise


def _stage2(inventory: P2Inventory, staging: Path, chunk_size: int) -> None:
    """Stream annotations in canonical order straight into chunked bitmap parts.

    Canonical order: objects by (dataset_id, object_id); within an object the
    (rid, namespace, origin) rows are sorted in a per-object temp table (so
    Parquet physical row order never leaks into first-seen assignment); tags
    within a row are sorted (value, category).
    """
    db = sqlite3.connect(staging / "compiler-staging.sqlite")
    parts_path = staging / "bitmap_parts.sqlite"
    if parts_path.exists():
        parts_path.unlink()
    parts = sqlite3.connect(parts_path)
    try:
        parts.execute(
            "CREATE TABLE tag_parts (tag_id INTEGER NOT NULL, part_no INTEGER NOT NULL,"
            " cardinality INTEGER NOT NULL, blob BLOB NOT NULL,"
            " PRIMARY KEY (tag_id, part_no))")
        namespaces: list[str] = []
        ns_ids: dict[str, int] = {}
        tag_ids: dict[tuple[int, str], int] = {}
        categories: set[tuple[int, str]] = set()
        namespace_known: dict[int, BitMap] = {}
        chunk: dict[int, BitMap] = {}
        part_no = 0
        flush_rows = 0
        processed = 0

        def flush() -> None:
            nonlocal part_no, flush_rows
            for tid, bitmap in chunk.items():
                parts.execute(
                    "INSERT INTO tag_parts VALUES (?,?,?,?)",
                    (tid, part_no, len(bitmap), bytes(bitmap.serialize())))
            part_no += 1
            flush_rows = 0
            chunk.clear()
            parts.commit()

        objects = sorted(inventory.objects, key=lambda o: (o.dataset_id, o.object_id))
        for obj in objects:
            db.execute("DROP TABLE IF EXISTS ann_obj")
            db.execute(
                "CREATE TEMP TABLE ann_obj (rid INTEGER, ns TEXT NOT NULL,"
                " origin TEXT NOT NULL, known INTEGER NOT NULL, tags TEXT,"
                " record_id BLOB(16) NOT NULL)")
            for batch in _read_batches(_fragment_path(obj, "annotations")):
                db.executemany(
                    "INSERT INTO ann_obj VALUES (NULL,?,?,?,?,?)", [
                        (ann["namespace"], ann["origin"],
                         1 if ann["tags_state"] in ("known", "empty") else 0,
                         json.dumps(sorted(
                             ((tag["value"], tag["category"])
                              for tag in ann["tags"]) if ann["tags_state"] == "known"
                             else [],
                             key=lambda pair: (pair[0], pair[1] or ""))),
                         bytes.fromhex(ann["record_id"]))
                        for ann in batch
                    ])
            db.execute(
                "UPDATE ann_obj SET rid = (SELECT r.rid FROM rids r JOIN samples s "
                "ON r.seq = s.seq WHERE s.record_id = ann_obj.record_id)")
            if db.execute("SELECT COUNT(*) FROM ann_obj WHERE rid IS NULL").fetchone()[0]:
                _fail(f"annotation references unknown record in {obj.object_id}")
            for rid, ns, origin, known, tags_json in db.execute(
                    "SELECT rid, ns, origin, known, tags FROM ann_obj "
                    "ORDER BY rid, ns, origin"):
                ns_id = ns_ids.get(ns)
                if ns_id is None:
                    ns_id = len(namespaces)
                    namespaces.append(ns)
                    ns_ids[ns] = ns_id
                if known:
                    known_bitmap = namespace_known.get(ns_id)
                    if known_bitmap is None:
                        known_bitmap = namespace_known[ns_id] = BitMap()
                    known_bitmap.add(rid)
                for value, category in json.loads(tags_json):
                    key = (ns_id, value)
                    tag_id = tag_ids.get(key)
                    if tag_id is None:
                        tag_id = len(tag_ids)
                        tag_ids[key] = tag_id
                    if category is not None:
                        categories.add((tag_id, category))
                    current = chunk.get(tag_id)
                    if current is None:
                        chunk[tag_id] = BitMap((rid,))
                    else:
                        current.add(rid)
                    processed += 1
                    flush_rows += 1
                    if flush_rows >= chunk_size:
                        flush()
        db.execute("DROP TABLE IF EXISTS ann_obj")
        if chunk:
            flush()
        staging.joinpath("TAG-IDS.json").write_text(
            json.dumps([[tag_id, ns_id, value]
                        for (ns_id, value), tag_id in sorted(
                            tag_ids.items(), key=lambda kv: kv[1])]),
            encoding="utf-8")
        staging.joinpath("CATEGORIES.json").write_text(
            json.dumps(sorted(categories)),
            encoding="utf-8")
        ns_known_path = staging / "ns-known.sqlite"
        if ns_known_path.exists():
            ns_known_path.unlink()
        ns_db = sqlite3.connect(ns_known_path)
        ns_db.execute("CREATE TABLE ns_known (ns_id INTEGER PRIMARY KEY, blob BLOB)")
        for ns_id in sorted(namespace_known):
            ns_db.execute(
                "INSERT INTO ns_known VALUES (?,?)",
                (ns_id, bytes(namespace_known[ns_id].serialize())))
        ns_db.commit()
        ns_db.close()
        staging.joinpath("STAGE2-COUNTS.json").write_text(
            json.dumps({"tag_occurrences": processed, "namespaces": namespaces,
                        "tags": len(tag_ids)}), encoding="utf-8")
        db.close()
        parts.close()
        _ensure_no_wal(staging / "compiler-staging.sqlite")
        _ensure_no_wal(parts_path)
        _ensure_no_wal(ns_known_path)
    except BaseException:
        db.close()
        parts.close()
        raise


def _catalog(inventory: P2Inventory, staging: Path, snap_id: str) -> int:
    """Build catalog.sqlite from compiler-staging.sqlite; returns rid_count."""
    db = sqlite3.connect(staging / "compiler-staging.sqlite")
    catalog = staging / "catalog.sqlite"
    if catalog.exists():
        catalog.unlink()
    cat = sqlite3.connect(catalog)
    try:
        cat.executescript(
            """
            CREATE TABLE meta (
                snapshot_id TEXT PRIMARY KEY,
                runtime_format_version INTEGER NOT NULL,
                compiler TEXT NOT NULL,
                source_fingerprint TEXT NOT NULL,
                rid_count INTEGER NOT NULL
            );
            CREATE TABLE sources (
                source_id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);
            CREATE TABLE datasets (
                dataset_id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE,
                source_id INTEGER NOT NULL REFERENCES sources(source_id));
            CREATE TABLE objects (
                object_idx INTEGER PRIMARY KEY,
                storage_id TEXT NOT NULL,
                object_id TEXT NOT NULL,
                object_path TEXT NOT NULL,
                object_size BLOB(8) NOT NULL,
                object_version TEXT NOT NULL,
                validator TEXT NOT NULL,
                backend TEXT NOT NULL,
                repo_type TEXT,
                archive_format TEXT NOT NULL,
                validator_kind TEXT NOT NULL,
                validator_strength TEXT NOT NULL,
                dataset_id TEXT NOT NULL,
                UNIQUE (dataset_id, object_id)
            );
            CREATE TABLE formats (
                format_id INTEGER PRIMARY KEY, format TEXT NOT NULL UNIQUE);
            CREATE TABLE records (
                rid INTEGER PRIMARY KEY,
                record_id BLOB(16) NOT NULL UNIQUE,
                source_id INTEGER NOT NULL,
                dataset_id INTEGER NOT NULL,
                post_id TEXT NOT NULL
            );
            CREATE INDEX records_source_post ON records(source_id, post_id);
            CREATE INDEX records_dataset_post ON records(dataset_id, post_id);
            CREATE TABLE tags (
                tag_id INTEGER PRIMARY KEY,
                namespace_id INTEGER NOT NULL,
                value TEXT NOT NULL,
                cardinality INTEGER NOT NULL DEFAULT 0,
                UNIQUE (namespace_id, value)
            );
            CREATE TABLE tag_categories (
                tag_id INTEGER NOT NULL REFERENCES tags(tag_id),
                category TEXT NOT NULL,
                PRIMARY KEY (tag_id, category)
            );
            CREATE INDEX tag_categories_category_tag
                ON tag_categories(category, tag_id);
            CREATE TABLE namespaces (
                namespace_id INTEGER PRIMARY KEY, namespace TEXT NOT NULL UNIQUE);
            """
        )
        rid_count = db.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
        cat.execute(
            "INSERT INTO meta VALUES (?,?,?,?,?)",
            (snap_id, RUNTIME_FORMAT_VERSION, RUNTIME_COMPILER,
             inventory.source_fingerprint, rid_count))

        sources = [r[0] for r in db.execute(
            "SELECT DISTINCT source FROM samples ORDER BY source")]
        source_ids = {name: idx for idx, name in enumerate(sources)}
        for name, source_id in source_ids.items():
            cat.execute("INSERT INTO sources VALUES (?,?)", (source_id, name))

        datasets = [r for r in db.execute(
            "SELECT DISTINCT dataset_id, source FROM samples ORDER BY dataset_id")]
        dataset_ids = {name: idx for idx, (name, _) in enumerate(datasets)}
        for dataset_id, (name, source) in enumerate(datasets):
            cat.execute("INSERT INTO datasets VALUES (?,?,?)",
                        (dataset_id, name, source_ids[source]))

        objects = sorted(inventory.objects, key=lambda o: (o.dataset_id, o.object_id))
        for object_idx, obj in enumerate(objects):
            rows = [r for batch in _read_batches(_fragment_path(obj, "objects"))
                    for r in batch
                    if r["dataset_id"] == obj.dataset_id
                    and r["object_id"] == obj.object_id]
            if not rows:
                _fail(f"objects fragment missing row: {obj.object_id}")
            first = rows[0]
            for extra in rows[1:]:
                if extra != first:
                    _fail(f"object metadata conflict: {obj.object_id}")
            cat.execute(
                "INSERT INTO objects VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (object_idx, first["storage_id"], first["object_id"],
                 first["object_path"], _u64(first["object_size"]),
                 first["object_version"],
                 first["validator"], first["backend"], first["repo_type"],
                 first["archive_format"], first["validator_kind"],
                 first["validator_strength"], obj.dataset_id))

        formats = [r[0] for r in db.execute(
            "SELECT DISTINCT image_format FROM samples "
            "WHERE image_format IS NOT NULL ORDER BY image_format")]
        format_ids = {name: idx for idx, name in enumerate(formats, start=1)}
        for name, format_id in format_ids.items():
            cat.execute("INSERT INTO formats VALUES (?,?)", (format_id, name))
        staging.joinpath("FORMAT-IDS.json").write_text(
            json.dumps(format_ids, sort_keys=True), encoding="utf-8")

        record_sql = (
            "INSERT INTO records (rid, record_id, source_id, dataset_id, post_id) "
            "VALUES (?,?,?,?,?)")
        record_batch: list[tuple] = []
        for rid, record_id, source, dataset, post_id in db.execute(
                "SELECT r.rid, s.record_id, s.source, s.dataset_id, s.post_id "
                "FROM rids r JOIN samples s ON s.seq = r.seq ORDER BY r.rid"):
            record_batch.append(
                (rid, record_id, source_ids[source], dataset_ids[dataset], post_id))
            if len(record_batch) >= 10_000:
                cat.executemany(record_sql, record_batch)
                record_batch = []
        if record_batch:
            cat.executemany(record_sql, record_batch)

        stage2 = json.loads(
            staging.joinpath("STAGE2-COUNTS.json").read_text(encoding="utf-8"))
        for ns_id, name in enumerate(stage2["namespaces"]):
            cat.execute("INSERT INTO namespaces VALUES (?,?)", (ns_id, name))

        for tag_id, ns_id, value in json.loads(
                staging.joinpath("TAG-IDS.json").read_text(encoding="utf-8")):
            cat.execute(
                "INSERT INTO tags (tag_id, namespace_id, value) VALUES (?,?,?)",
                (tag_id, ns_id, value))
        for tag_id, category in json.loads(
                staging.joinpath("CATEGORIES.json").read_text(encoding="utf-8")):
            if cat.execute("SELECT 1 FROM tags WHERE tag_id = ?",
                           (tag_id,)).fetchone() is None:
                _fail(f"category references unknown tag_id: {tag_id}")
            cat.execute("INSERT INTO tag_categories VALUES (?,?)", (tag_id, category))
        cat.commit()
        db.close()
        cat.close()
        _ensure_no_wal(catalog)
        _ensure_no_wal(staging / "compiler-staging.sqlite")
        return rid_count
    except BaseException:
        cat.close()
        db.close()
        raise


def _bitmaps(staging: Path, snap_id: str) -> None:
    """OR-merge tag parts per tag; write final bitmaps.sqlite; drop parts."""
    catalog_path = staging / "catalog.sqlite"
    bitmaps_path = staging / "bitmaps.sqlite"
    parts_path = staging / "bitmap_parts.sqlite"
    if bitmaps_path.exists():
        bitmaps_path.unlink()
    bits = sqlite3.connect(bitmaps_path)
    parts = sqlite3.connect(parts_path)
    cat = sqlite3.connect(catalog_path)
    try:
        bits.execute(
            "CREATE TABLE bitmaps (kind TEXT NOT NULL, id INTEGER NOT NULL,"
            " cardinality INTEGER NOT NULL, serialized_bytes INTEGER NOT NULL,"
            " blob_sha256 TEXT NOT NULL, blob BLOB NOT NULL,"
            " PRIMARY KEY (kind, id))")
        bits.execute(
            "CREATE TABLE bitmaps_meta (snapshot_id TEXT PRIMARY KEY,"
            " rid_count INTEGER NOT NULL)")

        def store(kind: str, bitmap_id: int, bitmap: BitMap) -> None:
            blob = bytes(bitmap.serialize())
            bits.execute(
                "INSERT INTO bitmaps VALUES (?,?,?,?,?,?)",
                (kind, bitmap_id, len(bitmap), len(blob),
                 hashlib.sha256(blob).hexdigest(), blob))

        for tag_id, in cat.execute("SELECT tag_id FROM tags ORDER BY tag_id"):
            merged = BitMap()
            for blob, in parts.execute(
                    "SELECT blob FROM tag_parts WHERE tag_id = ? ORDER BY part_no",
                    (tag_id,)):
                merged |= BitMap.deserialize(bytes(blob))
            store("tag", tag_id, merged)
            cat.execute("UPDATE tags SET cardinality = ? WHERE tag_id = ?",
                        (len(merged), tag_id))
        parts.commit()
        cat.commit()

        source_bitmaps: dict[int, BitMap] = {}
        dataset_bitmaps: dict[int, BitMap] = {}
        for rid, source_id, dataset_id in cat.execute(
                "SELECT rid, source_id, dataset_id FROM records ORDER BY rid"):
            source_bitmap = source_bitmaps.get(source_id)
            if source_bitmap is None:
                source_bitmap = source_bitmaps[source_id] = BitMap()
            source_bitmap.add(rid)
            dataset_bitmap = dataset_bitmaps.get(dataset_id)
            if dataset_bitmap is None:
                dataset_bitmap = dataset_bitmaps[dataset_id] = BitMap()
            dataset_bitmap.add(rid)
        for source_id, bitmap in sorted(source_bitmaps.items()):
            store("source", source_id, bitmap)
        for dataset_id, bitmap in sorted(dataset_bitmaps.items()):
            store("dataset", dataset_id, bitmap)

        ns_db = sqlite3.connect(staging / "ns-known.sqlite")
        for ns_id, blob in ns_db.execute("SELECT ns_id, blob FROM ns_known"):
            store("namespace", ns_id, BitMap.deserialize(bytes(blob)))
        ns_db.close()

        rid_count = cat.execute("SELECT rid_count FROM meta").fetchone()[0]
        bits.execute("INSERT INTO bitmaps_meta VALUES (?,?)", (snap_id, rid_count))
        bits.commit()
        bits.close()
        parts.close()
        cat.close()
        _ensure_no_wal(bitmaps_path)
        _ensure_no_wal(catalog_path)
        parts_path.unlink()
    except BaseException:
        bits.close()
        parts.close()
        cat.close()
        raise


def _locations(staging: Path, rid_count: int, snap_id: str) -> None:
    """Write locations.npy in a single rid-ordered pass plus identity trailer."""
    db = sqlite3.connect(staging / "compiler-staging.sqlite")
    catalog = sqlite3.connect(staging / "catalog.sqlite")
    locations_path = staging / "locations.npy"
    if locations_path.exists():
        locations_path.unlink()
    format_ids = json.loads(
        staging.joinpath("FORMAT-IDS.json").read_text(encoding="utf-8"))
    array = np.lib.format.open_memmap(
        locations_path, mode="w+", dtype=LOCATION_DTYPE, shape=(rid_count,))
    try:
        try:
            cursor = db.execute(
                "SELECT r.rid, s.image_offset, s.image_size, s.metadata_offset,"
                " s.metadata_size, s.has_metadata, s.image_format, s.object_id,"
                " s.dataset_id"
                " FROM rids r JOIN samples s ON s.seq = r.seq ORDER BY r.rid")
            object_idx = {
                (dataset, object_id): idx for dataset, object_id, idx in
                catalog.execute(
                    "SELECT dataset_id, object_id, object_idx FROM objects")
            }
            for row in cursor:
                (rid, image_offset, image_size, metadata_offset, metadata_size,
                 has_metadata, image_format, object_id, dataset_id) = row
                array[rid] = (
                    object_idx[(dataset_id, object_id)],
                    _from_u64(image_offset), _from_u64(image_size),
                    _from_u64(metadata_offset), _from_u64(metadata_size),
                    format_ids.get(image_format, NO_FORMAT)
                    if image_format else NO_FORMAT,
                    HAS_METADATA if has_metadata else 0,
                )
            array.flush()
        finally:
            del array
        _write_location_identity(locations_path, snap_id, rid_count)
    finally:
        db.close()
        catalog.close()
        _ensure_no_wal(staging / "compiler-staging.sqlite")
        _ensure_no_wal(staging / "catalog.sqlite")


def _snapshot(staging: Path, snap_id: str, rid_count: int,
              counts: dict[str, Any]) -> None:
    files = {}
    for name in ("catalog.sqlite", "bitmaps.sqlite", "locations.npy"):
        files[name] = {"path": name, "bytes": (staging / name).stat().st_size,
                       "sha256": _sha256_file(staging / name)}
    manifest = {
        "snapshot_id": snap_id,
        "runtime_format_version": RUNTIME_FORMAT_VERSION,
        "compiler": RUNTIME_COMPILER,
        "source_fingerprint": counts["fingerprint"],
        "rid_count": rid_count,
        "object_count": counts["objects"],
        "source_count": counts["sources"],
        "dataset_count": counts["datasets"],
        "tag_count": counts["tags"],
        "tag_memberships": counts["memberships"],
        "python": sys.version.split()[0],
        "pyarrow": pyarrow.__version__,
        "numpy": np.__version__,
        "pyroaring": getattr(pyroaring, "__version__", "unknown"),
        "locations": {"dtype": LOCATION_DTYPE.descr, "shape": [rid_count]},
        "files": files,
        "created_at": counts["created_at"],
    }
    _atomic_write(staging / "SNAPSHOT.json",
                  json.dumps(manifest, indent=2, sort_keys=True).encode())


def _current_matches(output_root: Path, snap_id: str) -> bool:
    current = output_root / "current.json"
    if not current.exists():
        return False
    try:
        return (json.loads(current.read_text(encoding="utf-8"))
                .get("snapshot_id") == snap_id)
    except (OSError, json.JSONDecodeError):
        return False


STAGING_OWNER_PROTOCOL = 1
KNOWN_STAGING_FILES = frozenset({
    "OWNER.json", "STAGE.txt", "compiler-staging.sqlite",
    "bitmap_parts.sqlite", "ns-known.sqlite", "TAG-IDS.json",
    "CATEGORIES.json", "STAGE1-COUNTS.json", "STAGE2-COUNTS.json",
    "catalog.sqlite", "FORMAT-IDS.json", "bitmaps.sqlite",
    "locations.npy", "SNAPSHOT.json", "READY",
})


def _is_link(path: Path) -> bool:
    """is_symlink() plus, on Windows, the reparse-point attribute:
    CPython does NOT report junctions (mklink /J) as symlinks, and a
    junction is exactly the escape vector we must treat as a link."""
    if path.is_symlink():
        return True
    if sys.platform == "win32":
        try:
            return bool(os.lstat(path).st_file_attributes
                        & stat.FILE_ATTRIBUTE_REPARSE_POINT)
        except OSError:
            return False
    return False


def _marker_valid(path: Path, snap_id: str, role: str) -> bool:
    """Ownership is proven by a marker, never by the directory name: a
    pre-created `.staging-<snap_id>` (or `snapshots/<snap_id>`) with foreign
    files is someone else's data and must fail closed, not be deleted.

    role is "staging" while the directory is a build workspace and is
    rewritten to "published" at publish time, so a published directory can
    never be mistaken for an interrupted publish (and thus never deleted).
    """
    marker_path = path / "OWNER.json"
    try:
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return (marker.get("snapshot_id") == snap_id
            and marker.get("owner") == RUNTIME_COMPILER
            and marker.get("protocol") == STAGING_OWNER_PROTOCOL
            and marker.get("role") == role)


def _staging_type_audit(staging: Path) -> bool:
    """Known-file + entry-TYPE audit of a directory claimed to be ours.
    A directory, symlink, junction or reparse point named like a known
    file (e.g. a directory called catalog.sqlite hiding user data) makes
    the directory foreign, so nothing nested inside it can ever be
    rmtree'd. Never proves ownership by itself - the marker does."""
    try:
        entries = list(staging.iterdir())
    except OSError:
        return False
    for entry in entries:
        if _is_link(entry) or not entry.is_file():
            return False
    return {entry.name for entry in entries} <= KNOWN_STAGING_FILES


def _staging_owned(staging: Path, snap_id: str) -> bool:
    if not _marker_valid(staging, snap_id, "staging"):
        return False
    # Known-file audit: even with a forged marker, any file outside the
    # compiler's own file list makes the directory foreign.
    return _staging_type_audit(staging)


def _refuse_link_escape(output_root: Path, child: Path) -> None:
    """The runtime root and everything we create inside it must resolve
    within the declared root: a symlink/junction pointing outside would
    let the compiler read, overwrite or delete files elsewhere."""
    root = output_root.resolve()
    try:
        resolved = child.resolve()
    except OSError:
        raise SnapshotCorruptError(f"cannot resolve path: {child}")
    if not resolved.is_relative_to(root):
        raise SnapshotCorruptError(
            f"link escape refused: {child} resolves outside {root}")


def _staging_owner_marker(staging: Path, snap_id: str) -> None:
    """Write the ownership marker for a staging directory we are creating;
    keep an existing valid marker untouched so resume stays stable."""
    if (staging / "OWNER.json").exists():
        if _marker_valid(staging, snap_id, "staging"):
            return
        raise SnapshotCorruptError(
            f"staging OWNER.json does not match snapshot: {staging}")
    _write_owner_marker(staging, snap_id, "staging")


def _write_owner_marker(path: Path, snap_id: str, role: str) -> None:
    """Unconditionally (re)write the marker - only legal for a directory
    the caller just created or just renamed itself."""
    marker = {
        "protocol": STAGING_OWNER_PROTOCOL,
        "owner": RUNTIME_COMPILER,
        "snapshot_id": snap_id,
        "role": role,
        "known_files": sorted(KNOWN_STAGING_FILES),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    _atomic_write(path / "OWNER.json", json.dumps(marker, sort_keys=True)
                  .encode())


def _manifest_data_files(manifest: dict[str, Any]) -> dict[str, Any]:
    """Reject noncanonical paths before any manifested I/O."""
    names = {"catalog.sqlite", "bitmaps.sqlite", "locations.npy"}
    files = manifest.get("files")
    if not isinstance(files, dict) or set(files) != names:
        raise SnapshotCorruptError("invalid manifest file set")
    if any(not isinstance(e, dict) or e.get("path") != n
           for n, e in files.items()):
        raise SnapshotCorruptError("noncanonical manifest path")
    return files


def _verify_manifest_files(final: Path, snap_id: str) -> dict[str, Any]:
    """Stream-hash every manifested file against the SNAPSHOT.json
    manifest (bounded RSS). READY and identity are NOT checked here."""
    manifest = json.loads((final / "SNAPSHOT.json").read_text(encoding="utf-8"))
    if manifest.get("snapshot_id") != snap_id:
        raise SnapshotMixError("SNAPSHOT.json snapshot_id mismatch")
    for entry in _manifest_data_files(manifest).values():
        path = final / entry["path"]
        if not path.is_file():
            raise SnapshotCorruptError(f"missing published file: {entry['path']}")
        if path.stat().st_size != entry["bytes"]:
            raise SnapshotCorruptError(f"size mismatch: {entry['path']}")
        if _sha256_file(path) != entry["sha256"]:
            raise SnapshotCorruptError(f"sha256 mismatch: {entry['path']}")
    return manifest


def _verify_ready(final: Path, snap_id: str) -> dict[str, Any]:
    """Reuse validation of a published snapshot (fail closed).

    Reuse is stricter than a fast open: every published file is streamed
    and hashed against the manifest, so a silently corrupted snapshot is
    never reopened. The streaming hash keeps RSS bounded.
    """
    from .snapshot import RuntimeSnapshot
    if _is_link(final) or not _staging_type_audit(final):
        raise SnapshotCorruptError("unsafe snapshot entries")
    ready = final / "READY"
    if not ready.exists():
        raise SnapshotCorruptError("published snapshot has no READY")
    if ready.read_text(encoding="utf-8").strip() != snap_id:
        raise SnapshotCorruptError("READY content does not match snapshot_id")
    manifest = _verify_manifest_files(final, snap_id)
    opened = RuntimeSnapshot._open_snapshot(final, staging_id=snap_id)
    opened.close()
    return manifest


def _resume_interrupted_publish(staging: Path, final: Path,
                                snap_id: str) -> None:
    """§48F resume: our staging survived a publish failure AFTER the owner
    role had already been flipped to "published". Two crash windows:

    (a) READY written inside staging, os.rename failed;
    (b) role flipped, READY not yet written (deterministic content lost).

    Safe resume is fully deterministic and never deletes: verify the READY
    file (or, in (b), STAGE=ready plus every manifested file), complete a
    missing READY with its exact deterministic content, re-verify
    everything (READY, manifest, streamed file hashes, schema/identity via
    a real open), require the entry set to equal exactly the published
    file set (no sidecars, no foreign files), then promote with a plain
    os.rename - which writes no interior byte. Any mismatch fails closed:
    the directory is left in place for manual inspection."""
    if final.exists():
        raise SnapshotCorruptError(
            f"cannot resume publish: staging {staging.name} and final "
            f"{final.name} both exist; refusing to touch either (remove "
            "one manually after inspection)")
    if _is_link(staging) or not _staging_type_audit(staging):
        raise SnapshotCorruptError("unowned staging: unsafe entry")
    ready = staging / "READY"
    missing_ready = not ready.exists()
    if missing_ready:
        if _Stage(staging).current != len(_STAGES) - 1:
            raise SnapshotCorruptError(
                f"staging {staging.name} carries a published owner marker "
                "but is not a completed build (no valid READY): refusing "
                "to touch it (remove it manually after inspection)")
        manifest = _verify_manifest_files(staging, snap_id)
        from .snapshot import RuntimeSnapshot
        with RuntimeSnapshot._open_snapshot(
                staging, staging_id=snap_id, require_ready=False):
            pass
    else:
        manifest = _verify_ready(staging, snap_id)
    expected = ({entry["path"] for entry in manifest["files"].values()}
                | {"OWNER.json", "STAGE.txt", "SNAPSHOT.json"})
    if not missing_ready:
        expected.add("READY")
    entries = set()
    for entry in staging.iterdir():
        if _is_link(entry) or not entry.is_file():
            raise SnapshotCorruptError(
                f"staging {staging.name} contains a non-regular entry "
                f"({entry.name}); refusing to touch it (remove it manually "
                "after inspection)")
        entries.add(entry.name)
    if entries != expected:
        raise SnapshotCorruptError(
            f"staging {staging.name} does not match the published file "
            "set; refusing to touch it (remove it manually after inspection)")
    final.parent.mkdir(parents=True, exist_ok=True)
    if missing_ready:
        with ready.open("x", encoding="utf-8") as handle:
            handle.write(snap_id)
    os.rename(staging, final)


def _finish_publish(output_root: Path, final: Path,
                    snap_id: str) -> "CompiledSnapshot":
    """Post-rename tail: the EXTERNAL current.json update plus the summary.
    The published directory itself is never written after the rename."""
    current = {
        "snapshot_id": snap_id,
        "path": f"snapshots/{snap_id}",
        "runtime_format_version": RUNTIME_FORMAT_VERSION,
        "compiler": RUNTIME_COMPILER,
    }
    _atomic_write(output_root / "current.json",
                  json.dumps(current, indent=2, sort_keys=True).encode())
    manifest = json.loads((final / "SNAPSHOT.json").read_text(encoding="utf-8"))
    return CompiledSnapshot(
        snap_id, final, manifest["rid_count"], manifest["object_count"],
        manifest["source_count"], manifest["dataset_count"],
        manifest["tag_count"], manifest["tag_memberships"])


def _sqlite_one(path: Path, sql: str) -> tuple | None:
    """Single SELECT with explicit close (a sqlite3 with-block does not
    close the connection, and a live handle locks files on Windows)."""
    db = sqlite3.connect(path)
    try:
        return db.execute(sql).fetchone()
    finally:
        db.close()


def _staging_rid_count(staging: Path) -> int | None:
    db_path = staging / "compiler-staging.sqlite"
    if not db_path.exists():
        return None
    try:
        row = _sqlite_one(db_path, "SELECT COUNT(*) FROM samples")
        return row[0] if row else None
    except (sqlite3.Error, OSError):
        return None


def _verify_stage1(staging: Path) -> bool:
    try:
        counts = json.loads(
            (staging / "STAGE1-COUNTS.json").read_text(encoding="utf-8"))
        actual = _staging_rid_count(staging)
        return (counts["samples"] == actual
                and (staging / "compiler-staging.sqlite").exists())
    except (OSError, json.JSONDecodeError, KeyError):
        return False


def _verify_stage2(staging: Path) -> bool:
    try:
        ids = json.loads((staging / "TAG-IDS.json").read_text(encoding="utf-8"))
        json.loads((staging / "CATEGORIES.json").read_text(encoding="utf-8"))
        counts = json.loads(
            (staging / "STAGE2-COUNTS.json").read_text(encoding="utf-8"))
        return (isinstance(ids, list)
                and (staging / "bitmap_parts.sqlite").exists()
                and (staging / "ns-known.sqlite").exists()
                and isinstance(counts["tag_occurrences"], int))
    except (OSError, json.JSONDecodeError, KeyError):
        return False


def _verify_catalog(staging: Path, snap_id: str) -> bool:
    path = staging / "catalog.sqlite"
    if not path.exists():
        return False
    try:
        row = _sqlite_one(path, "SELECT snapshot_id, rid_count FROM meta")
        return (row is not None and row[0] == snap_id
                and row[1] == _staging_rid_count(staging))
    except (sqlite3.Error, OSError):
        return False


def _verify_bitmaps(staging: Path, snap_id: str) -> bool:
    path = staging / "bitmaps.sqlite"
    if not path.exists():
        return False
    try:
        row = _sqlite_one(path, "SELECT snapshot_id, rid_count FROM bitmaps_meta")
        expected = _sqlite_one(staging / "catalog.sqlite",
                               "SELECT rid_count FROM meta")
        expected = expected[0] if expected else None
        return (row is not None and row[0] == snap_id
                and row[1] == expected == _staging_rid_count(staging))
    except (sqlite3.Error, OSError):
        return False


def _verify_locations(staging: Path, snap_id: str) -> bool:
    path = staging / "locations.npy"
    rid_count = _staging_rid_count(staging)
    if not path.exists() or rid_count is None:
        return False
    try:
        _check_location_identity(path, snap_id, rid_count)
        return True
    except SnapshotCorruptError:
        return False


def _verify_snapshot(staging: Path, snap_id: str) -> bool:
    path = staging / "SNAPSHOT.json"
    if not path.exists():
        return False
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
        return (manifest.get("snapshot_id") == snap_id
                and manifest.get("rid_count") == _staging_rid_count(staging)
                and all((staging / entry["path"]).exists()
                        for entry in _manifest_data_files(manifest).values()))
    except (OSError, json.JSONDecodeError, KeyError):
        return False


def compile_runtime(
    inventory: P2Inventory,
    output_root: Path,
    *,
    chunk_size: int = DEFAULT_CHUNK,
) -> CompiledSnapshot:
    """Compile committed P2 inputs into an immutable snapshot under output_root."""
    if chunk_size <= 0:
        _fail("chunk_size must be positive")
    options = {"chunk_size": chunk_size}
    snap_id = snapshot_id(inventory.source_fingerprint, options,
                          RUNTIME_COMPILER, RUNTIME_FORMAT_VERSION)
    output_root = Path(output_root)
    for path in (output_root, *output_root.parents,
                 output_root / "snapshots", output_root / "snapshots" / snap_id):
        if _is_link(path):
            raise SnapshotCorruptError(f"runtime path must not be a symlink or junction: {path}")
    for path in (output_root / "current.json", output_root / "current.json.tmp"):
        if _is_link(path) or (path.exists() and not path.is_file()):
            raise SnapshotCorruptError(f"unsafe runtime pointer: {path}")
    output_root.mkdir(parents=True, exist_ok=True)
    final = output_root / "snapshots" / snap_id
    if (final / "READY").exists():
        # Reuse is allowed only after validating the published snapshot.
        manifest = _verify_ready(final, snap_id)
        if not _current_matches(output_root, snap_id):
            current = {
                "snapshot_id": snap_id,
                "path": f"snapshots/{snap_id}",
                "runtime_format_version": RUNTIME_FORMAT_VERSION,
                "compiler": RUNTIME_COMPILER,
            }
            _atomic_write(output_root / "current.json",
                          json.dumps(current, indent=2, sort_keys=True).encode())
        return CompiledSnapshot(
            snap_id, final, manifest["rid_count"], manifest["object_count"],
            manifest["source_count"], manifest["dataset_count"],
            manifest["tag_count"], manifest["tag_memberships"])
    # The runtime root itself must be a real directory we can reason
    # about: a symlink/junction root would point the whole build (and its
    # deletion branches) somewhere else.
    if _is_link(output_root):
        raise SnapshotCorruptError(
            f"runtime root must not be a symlink or junction: {output_root}")
    staging = output_root / f".staging-{snap_id}"
    if staging.exists():
        _refuse_link_escape(output_root, staging)
        if _is_link(staging) or not _staging_type_audit(staging):
            raise SnapshotCorruptError("unowned staging: unsafe entry")
        if _marker_valid(staging, snap_id, "published"):
            # Interrupted publish after the role flip (§48F): fully verify
            # and promote, or fail closed - this branch never deletes. The
            # resume consumes the staging (rename promotion), so the build
            # steps must not re-run: finish externally and return.
            _refuse_link_escape(output_root, final)
            _resume_interrupted_publish(staging, final, snap_id)
            return _finish_publish(output_root, final, snap_id)
        elif not _staging_owned(staging, snap_id):
            # Fail closed: the directory name alone proves nothing, so a
            # pre-created .staging-<snap_id> (marker missing or foreign
            # files inside) is never deleted or reused.
            raise SnapshotCorruptError(
                f"refusing to touch unowned staging directory: {staging} "
                "(remove it manually if it is stale)")
        else:
            stage = _Stage(staging)
            if stage.current < 0:
                # Ownership proven (marker + known-file audit), so every
                # entry is a compiler artifact and resetting is safe.
                shutil.rmtree(staging)
            else:
                checks = [("stage1", lambda: _verify_stage1(staging)),
                          ("stage2", lambda: _verify_stage2(staging)),
                          ("catalog", lambda: _verify_catalog(staging, snap_id)),
                          ("bitmaps", lambda: _verify_bitmaps(staging, snap_id)),
                          ("locations", lambda: _verify_locations(staging, snap_id)),
                          ("snapshot", lambda: _verify_snapshot(staging, snap_id))]
                for index, (name, check) in enumerate(checks):
                    if index > stage.current:
                        break
                    if not check():
                        if index == 0:
                            (staging / "STAGE.txt").unlink()
                        else:
                            (staging / "STAGE.txt").write_text(checks[index - 1][0],
                                                               encoding="utf-8")
                        stage.current = index - 1
                        break
    staging.mkdir(parents=True, exist_ok=True)
    _refuse_link_escape(output_root, staging)
    _staging_owner_marker(staging, snap_id)
    stage = _Stage(staging)

    def redo(name: str, *paths: Path) -> bool:
        if stage.done(name):
            return False
        for path in paths:
            if path.exists():
                path.unlink()
        return True

    if redo("stage1"):
        _stage1(inventory, staging)
        stage.complete("stage1")
    if redo("stage2", staging / "bitmap_parts.sqlite", staging / "ns-known.sqlite",
            staging / "TAG-IDS.json", staging / "CATEGORIES.json"):
        _stage2(inventory, staging, chunk_size)
        stage.complete("stage2")
    if redo("catalog", staging / "catalog.sqlite", staging / "FORMAT-IDS.json"):
        _catalog(inventory, staging, snap_id)
        stage.complete("catalog")
    if redo("bitmaps", staging / "bitmaps.sqlite"):
        if not (staging / "bitmap_parts.sqlite").exists():
            # parts already consumed but the stage marker was lost: rebuild.
            _stage2(inventory, staging, chunk_size)
            stage.complete("stage2")
        _bitmaps(staging, snap_id)
        stage.complete("bitmaps")
    rid_count = 0
    if redo("locations", staging / "locations.npy"):
        db = sqlite3.connect(staging / "compiler-staging.sqlite")
        try:
            rid_count = db.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
        finally:
            db.close()
        _locations(staging, rid_count, snap_id)
        stage.complete("locations")
    if redo("snapshot", staging / "SNAPSHOT.json"):
        db = sqlite3.connect(staging / "catalog.sqlite")
        try:
            objects = db.execute("SELECT COUNT(*) FROM objects").fetchone()[0]
            sources = db.execute("SELECT COUNT(*) FROM sources").fetchone()[0]
            datasets = db.execute("SELECT COUNT(*) FROM datasets").fetchone()[0]
            tags = db.execute("SELECT COUNT(*) FROM tags").fetchone()[0]
            memberships = db.execute(
                "SELECT COALESCE(SUM(cardinality), 0) FROM tags").fetchone()[0]
        finally:
            db.close()
        if rid_count == 0:
            db = sqlite3.connect(staging / "compiler-staging.sqlite")
            try:
                rid_count = db.execute(
                    "SELECT COUNT(*) FROM samples").fetchone()[0]
            finally:
                db.close()
        _snapshot(staging, snap_id, rid_count, dict(
            fingerprint=inventory.source_fingerprint, objects=objects,
            sources=sources, datasets=datasets, tags=tags, memberships=memberships,
            created_at=datetime.now(timezone.utc).isoformat()))
        stage.complete("snapshot")
    # The ready step is gated on the final READY file, not on STAGE.txt:
    # a crash between `complete("ready")` and the READY write (or after
    # the role flip, handled by _resume_interrupted_publish above) must
    # re-run this tail, and a READY'd final directory can only be reached
    # through the reuse path above.
    if not (final / "READY").exists():
        for name in ("compiler-staging.sqlite", "bitmap_parts.sqlite",
                     "ns-known.sqlite", "FORMAT-IDS.json", "STAGE1-COUNTS.json",
                     "STAGE2-COUNTS.json", "TAG-IDS.json", "CATEGORIES.json"):
            sidecar = staging / name
            if sidecar.exists():
                sidecar.unlink()
        if final.exists():
            # No READY here (a READY'd snapshot returned above). Our own
            # interrupted publish carries the staging owner marker, but the
            # marker alone is not enough: a directory we previously published
            # also carries it, so deletion additionally requires the
            # known-file audit - any file outside the compiler's own list
            # (e.g. user data dropped into snapshots/<snap_id>) fails closed.
            if not _staging_owned(final, snap_id):
                raise SnapshotCorruptError(
                    f"refusing to delete unowned snapshot directory: {final} "
                    "(remove it manually if it is stale)")
            shutil.rmtree(final)
        final.parent.mkdir(parents=True, exist_ok=True)
        # The published directory is IMMUTABLE: every final marker
        # (STAGE=ready, OWNER role=published, READY) is completed inside
        # staging BEFORE the atomic rename. After the rename this process
        # writes nothing inside final (only the external current.json),
        # so a failure after the rename can never leave a half-marked
        # published directory. A crash in the flip->READY->rename window
        # leaves staging with role=published: the NEXT compile resumes it
        # via _resume_interrupted_publish (full verification, then rename
        # promotion with no interior byte written), or fails closed and
        # preserves it - it is never rmtree'd and never half-reused.
        _Stage(staging).complete("ready")
        _write_owner_marker(staging, snap_id, "published")
        (staging / "READY").write_text(snap_id, encoding="utf-8")
        os.rename(staging, final)
    return _finish_publish(output_root, final, snap_id)
