"""Open, verify and read an immutable P3 runtime snapshot (read-only)."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from pyroaring import BitMap

from . import RUNTIME_FORMAT_VERSION
from .compiler import LOCATION_DTYPE, _check_location_identity, _from_u64, _sha256_file
from .errors import (
    AmbiguousRecordError,
    SnapshotClosedError,
    SnapshotCorruptError,
    SnapshotMixError,
    UnknownQueryValueError,
)

DEFAULT_CACHE_BYTES = 256 * 1024 * 1024
_DATA_FILES = ("catalog.sqlite", "bitmaps.sqlite", "locations.npy")


class ByteLRU:
    """Byte-budget LRU for serialized bitmaps."""

    def __init__(self, byte_limit: int) -> None:
        if byte_limit <= 0:
            raise ValueError("cache byte_limit must be positive")
        self.byte_limit = byte_limit
        self._entries: OrderedDict[tuple[str, int], bytes] = OrderedDict()
        self._resident = 0
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def get(self, key: tuple[str, int]) -> bytes | None:
        value = self._entries.get(key)
        if value is None:
            self.misses += 1
            return None
        self.hits += 1
        self._entries.move_to_end(key)
        return value

    def put(self, key: tuple[str, int], value: bytes) -> None:
        # A single blob that alone exceeds the budget is never cached: it
        # would permanently evict everything else and breach the budget.
        if len(value) > self.byte_limit:
            return
        if key in self._entries:
            self._resident -= len(self._entries[key])
            del self._entries[key]
        self._entries[key] = value
        self._resident += len(value)
        while self._resident > self.byte_limit and len(self._entries) > 1:
            _, evicted = self._entries.popitem(last=False)
            self._resident -= len(evicted)
            self.evictions += 1

    def resident_bytes(self) -> int:
        return self._resident

    def clear(self) -> None:
        self._entries.clear()
        self._resident = 0


@dataclass(frozen=True)
class ResolvedRecord:
    rid: int
    record_id: str
    source_id: int
    dataset_id: int
    post_id: str


class RuntimeSnapshot:
    """Read-only handle over one published snapshot."""

    def __init__(self, root: Path, snapshot_id: str, snap_dir: Path,
                 manifest: dict, catalog: sqlite3.Connection,
                 bitmaps: sqlite3.Connection, locations: np.memmap,
                 cache: ByteLRU, rid_count: int) -> None:
        self.root = root
        self.path = snap_dir
        self.snapshot_id = snapshot_id
        self.manifest = manifest
        self._catalog = catalog
        self._bitmaps = bitmaps
        self._locations = locations
        self.cache = cache
        self.rid_count = rid_count
        self._closed = False

    # -- lifecycle ---------------------------------------------------------
    @classmethod
    def open(cls, path: Path | str, *, full_verify: bool = False,
             cache_bytes: int = DEFAULT_CACHE_BYTES,
             snapshot_id: str | None = None) -> "RuntimeSnapshot":
        base = Path(path)
        if not base.is_dir():
            raise SnapshotCorruptError(f"snapshot path is not a directory: {base}")
        root = base
        if (base / "current.json").exists():
            current = json.loads((base / "current.json").read_text(encoding="utf-8"))
            if current.get("runtime_format_version") != RUNTIME_FORMAT_VERSION:
                raise SnapshotCorruptError(
                    f"unsupported runtime_format_version: {current.get('runtime_format_version')}")
            snap_dir = base / current["path"]
            snapshot_id = current["snapshot_id"]
        elif (base / "SNAPSHOT.json").exists():
            root = base.parent.parent
            snap_dir = base
            # Explicit snapshot directory: the directory name is the id
            # (or the caller-provided id for a staging directory verified
            # before its publish rename), and it must agree with both
            # READY and the SNAPSHOT.json manifest.
            snapshot_id = snapshot_id or base.name
        else:
            raise SnapshotCorruptError(
                f"not a runtime root or snapshot: {base}")
        if not (snap_dir / "READY").exists():
            raise SnapshotCorruptError(
                f"snapshot not ready (READY missing): {snap_dir}")
        manifest = json.loads(
            (snap_dir / "SNAPSHOT.json").read_text(encoding="utf-8"))
        if manifest["snapshot_id"] != (snapshot_id or manifest["snapshot_id"]):
            raise SnapshotCorruptError("snapshot_id mismatch with current.json")
        if manifest["runtime_format_version"] != RUNTIME_FORMAT_VERSION:
            raise SnapshotCorruptError(
                f"unsupported runtime_format_version: "
                f"{manifest['runtime_format_version']}")
        for name in _DATA_FILES:
            entry = manifest["files"].get(name)
            if entry is None:
                raise SnapshotCorruptError(f"SNAPSHOT.json missing {name}")
            file_path = snap_dir / entry["path"]
            if not file_path.is_file():
                raise SnapshotCorruptError(f"missing data file: {entry['path']}")
            if file_path.stat().st_size != entry["bytes"]:
                raise SnapshotCorruptError(f"size mismatch: {entry['path']}")
            if full_verify:
                # Streaming hash: full verification must not hold the whole
                # file in Python memory (a 185 MiB locations.npy would).
                digest = _sha256_file(file_path)
                if digest != entry["sha256"]:
                    raise SnapshotCorruptError(f"sha256 mismatch: {entry['path']}")

        for sidecar in ("-wal", "-shm"):
            for name in _DATA_FILES:
                if Path(str(snap_dir / name) + sidecar).exists():
                    raise SnapshotCorruptError(
                        f"leftover sidecar for {name}: {sidecar}")

        catalog = sqlite3.connect(
            f"file:{(snap_dir / 'catalog.sqlite').as_posix()}?mode=ro", uri=True)
        bitmaps = sqlite3.connect(
            f"file:{(snap_dir / 'bitmaps.sqlite').as_posix()}?mode=ro", uri=True)
        try:
            meta = catalog.execute(
                "SELECT snapshot_id, runtime_format_version, compiler, "
                "source_fingerprint, rid_count FROM meta").fetchone()
            if meta is None or meta[0] != snapshot_id or \
                    meta[1] != RUNTIME_FORMAT_VERSION:
                raise SnapshotCorruptError("catalog meta mismatch")
            rid_count = meta[4]
            if rid_count != manifest["rid_count"]:
                raise SnapshotCorruptError("rid_count mismatch")
            bitmap_meta = bitmaps.execute(
                "SELECT snapshot_id, rid_count FROM bitmaps_meta").fetchone()
            if bitmap_meta != (snapshot_id, rid_count):
                raise SnapshotMixError(
                    "bitmaps.sqlite snapshot identity mismatch (mixed snapshot?)")
        except BaseException:
            catalog.close()
            bitmaps.close()
            raise
        try:
            locations = np.load(str(snap_dir / "locations.npy"), mmap_mode="r")
            if locations.dtype != LOCATION_DTYPE:
                raise SnapshotCorruptError("locations dtype mismatch")
            if locations.shape != (rid_count,):
                raise SnapshotCorruptError("locations shape mismatch")
            # Whole-file snapshot binding: an entire locations.npy swapped in
            # from another snapshot of identical shape is rejected here. Same
            # length in-place payload edits stay detectable only by the
            # streaming full SHA-256 verification, by design.
            _check_location_identity(snap_dir / "locations.npy", snapshot_id,
                                     rid_count)
        except BaseException:
            locations = None
            catalog.close()
            bitmaps.close()
            raise
        return cls(root, snapshot_id, snap_dir, manifest, catalog, bitmaps,
                   locations, ByteLRU(cache_bytes), rid_count)

    def close(self) -> None:
        if self._closed:
            return
        self.cache.clear()
        self._catalog.close()
        self._bitmaps.close()
        del self._locations
        self._locations = None
        self._closed = True

    def __enter__(self) -> "RuntimeSnapshot":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- internals ---------------------------------------------------------
    def _check_open(self) -> None:
        if self._closed or self._locations is None:
            raise SnapshotClosedError("snapshot is closed")

    def _get_bitmap(self, kind: str, bitmap_id: int) -> BitMap:
        self._check_open()
        key = (kind, bitmap_id)
        blob = self.cache.get(key)
        if blob is None:
            row = self._bitmaps.execute(
                "SELECT blob, blob_sha256, cardinality FROM bitmaps "
                "WHERE kind = ? AND id = ?", (kind, bitmap_id)).fetchone()
            if row is None:
                raise UnknownQueryValueError(f"unknown {kind} id: {bitmap_id}")
            blob, stored_sha, _ = row
            blob = bytes(blob)
            if hashlib.sha256(blob).hexdigest() != stored_sha:
                raise SnapshotCorruptError(f"blob sha256 mismatch: {kind} {bitmap_id}")
            self.cache.put(key, blob)
        try:
            return BitMap.deserialize(blob)
        except Exception as exc:  # noqa: BLE001 - deserialize error types vary
            raise SnapshotCorruptError(f"bitmap deserialization failed: {exc}") from exc

    def _namespace_id(self, namespace: str) -> int:
        row = self._catalog.execute(
            "SELECT namespace_id FROM namespaces WHERE namespace = ?",
            (namespace,)).fetchone()
        if row is None:
            raise UnknownQueryValueError(f"unknown namespace: {namespace}")
        return row[0]

    def _tag_id(self, namespace: str, value: str) -> int:
        row = self._catalog.execute(
            "SELECT t.tag_id FROM tags t JOIN namespaces n "
            "ON n.namespace_id = t.namespace_id WHERE n.namespace = ? "
            "AND t.value = ?", (namespace, value)).fetchone()
        if row is None:
            raise UnknownQueryValueError(f"unknown tag: {namespace}/{value}")
        return row[0]

    def _source_id(self, source: str) -> int:
        row = self._catalog.execute(
            "SELECT source_id FROM sources WHERE name = ?", (source,)
        ).fetchone()
        if row is None:
            raise UnknownQueryValueError(f"unknown source: {source}")
        return row[0]

    def _dataset_id(self, dataset: str) -> int:
        row = self._catalog.execute(
            "SELECT dataset_id FROM datasets WHERE name = ?", (dataset,)
        ).fetchone()
        if row is None:
            raise UnknownQueryValueError(f"unknown dataset: {dataset}")
        return row[0]

    # -- lookups -----------------------------------------------------------
    def lookup_rids(self, source: str, post_id: str,
                    dataset: str | None = None) -> list[int]:
        self._check_open()
        source_id = self._source_id(source)
        sql = ("SELECT rid FROM records WHERE source_id = ? AND post_id = ?")
        params: list[object] = [source_id, post_id]
        if dataset is not None:
            sql += " AND dataset_id = ?"
            params.append(self._dataset_id(dataset))
        sql += " ORDER BY rid"
        return [row[0] for row in self._catalog.execute(sql, params)]

    def resolve_one(self, source: str, post_id: str,
                    dataset: str | None = None) -> ResolvedRecord:
        rids = self.lookup_rids(source, post_id, dataset)
        if not rids:
            raise UnknownQueryValueError(
                f"no record for source={source} post_id={post_id} "
                f"dataset={dataset}")
        if len(rids) > 1:
            raise AmbiguousRecordError(
                f"ambiguous record: {len(rids)} candidates for "
                f"source={source} post_id={post_id} dataset={dataset}")
        rid = rids[0]
        row = self._catalog.execute(
            "SELECT record_id, source_id, dataset_id, post_id FROM records "
            "WHERE rid = ?", (rid,)).fetchone()
        record_id, source_id, dataset_id, post = row
        return ResolvedRecord(rid, record_id.hex(), source_id, dataset_id, post)

    def query(self, spec=None, *, sources=(), datasets=(), namespace=None,
              all_tags=(), any_tags=(), none_tags=(), any_of=()):
        """Run a query. Accepts a RuntimeQuerySpec or keyword terms.

        Keyword form (the P3 keyword entry): any combination of
        sources=(), datasets=(), namespace=, all_tags=(), any_tags=(),
        none_tags=(), any_of=() builds the spec. An explicitly named
        namespace is validated even without tags.
        """
        from .query import RuntimeQuerySpec, evaluate_spec
        self._check_open()
        if spec is None:
            spec = RuntimeQuerySpec(
                sources=tuple(sources), datasets=tuple(datasets),
                namespace=namespace, all_tags=tuple(all_tags),
                any_tags=tuple(any_tags), none_tags=tuple(none_tags),
                any_of=tuple(any_of))
        self._validate_namespaces(spec)
        return evaluate_spec(self, spec)

    def _validate_namespaces(self, spec) -> None:  # noqa: ANN401
        if spec.namespace is not None:
            self._namespace_id(spec.namespace)
        if spec.any_of:
            for branch in spec.any_of:
                self._validate_namespaces(branch)

    def location(self, rid: int) -> dict:
        """Plain-dict view of one locations row (for CLI/tooling)."""
        self._check_open()
        if not 0 <= rid < self.rid_count:
            raise UnknownQueryValueError(f"rid out of range: {rid}")
        row = self._locations[rid]
        return {name: int(row[name]) for name in row.dtype.names}

    def object_ref(self, object_idx: int) -> dict:
        """Full ObjectRef for one catalog object (object_idx from a location)."""
        self._check_open()
        row = self._catalog.execute(
            "SELECT storage_id, object_id, object_path, object_size,"
            " object_version, validator, backend, repo_type,"
            " archive_format, validator_kind, validator_strength"
            " FROM objects WHERE object_idx = ?", (object_idx,)).fetchone()
        if row is None:
            raise UnknownQueryValueError(f"unknown object_idx: {object_idx}")
        return {
            "storage_id": row[0], "object_id": row[1], "object_path": row[2],
            "object_size": _from_u64(row[3]), "object_version": row[4],
            "validator": row[5], "backend": row[6], "repo_type": row[7],
            "archive_format": row[8], "validator_kind": row[9],
            "validator_strength": row[10],
        }
