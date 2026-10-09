"""P3 runtime query domain: spec, plan semantics, lazy result iteration."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from itertools import islice
from typing import Iterator, Union

import numpy as np
from pyroaring import BitMap

from .errors import UnknownQueryValueError
from .snapshot import RuntimeSnapshot

TagKey = tuple[str, str]  # (namespace, tag value)
TagRef = Union[str, TagKey]


@dataclass(frozen=True)
class RuntimeQuerySpec:
    """Independent P3 query domain (does not touch P1 QuerySpec).

    sources OR, datasets OR, then AND with all_tags AND, any_tags OR,
    none_tags excluded within their known namespace. any_of is a union of
    branch specs evaluated with the same single-branch semantics. width_gt
    and height_gt are strict pixel thresholds, ANDed with the other terms.
    """

    sources: tuple[str, ...] = ()
    datasets: tuple[str, ...] = ()
    namespace: str | None = None
    all_tags: tuple[TagRef, ...] = ()
    any_tags: tuple[TagRef, ...] = ()
    none_tags: tuple[TagRef, ...] = ()
    any_of: tuple["RuntimeQuerySpec", ...] = ()
    width_gt: int | None = None
    height_gt: int | None = None

    def __post_init__(self) -> None:
        for name in ("width_gt", "height_gt"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or not 0 <= value < 2**32):
                raise ValueError(f"{name} must be an integer in [0, 2**32)")
        if self.any_of and (self.sources or self.datasets or self.all_tags
                            or self.any_tags or self.none_tags
                            or self.width_gt is not None or self.height_gt is not None):
            raise ValueError(
                "any_of branches carry the terms; top level takes only any_of")


def _resolve_tags(snapshot: RuntimeSnapshot, namespace: str | None,
                  refs: tuple[TagRef, ...], kind: str) -> list[tuple[int, int]]:
    resolved = []
    for ref in refs:
        if isinstance(ref, str):
            if namespace is None:
                raise UnknownQueryValueError(
                    f"{kind} tag {ref!r} needs a namespace")
            namespace_id = snapshot._namespace_id(namespace)
            tag_id = snapshot._tag_id(namespace, ref)
        else:
            tag_namespace, value = ref
            namespace_id = snapshot._namespace_id(tag_namespace)
            tag_id = snapshot._tag_id(tag_namespace, value)
        resolved.append((tag_id, namespace_id))
    return resolved


@dataclass(frozen=True)
class LocationBatch:
    snapshot_id: str
    rid: np.ndarray
    object_idx: np.ndarray
    image_offset: np.ndarray
    image_size: np.ndarray
    metadata_offset: np.ndarray
    metadata_size: np.ndarray
    format_id: np.ndarray
    flags: np.ndarray


@dataclass(frozen=True)
class RecordBatch:
    snapshot_id: str
    rid: list[int]
    record_id: list[str]
    source_id: list[int]
    dataset_id: list[int]
    source_name: list[str]
    dataset_name: list[str]
    post_id: list[str]


@dataclass
class QueryResult:
    """Lazy bitmap-backed result bound to one snapshot."""

    _snapshot: RuntimeSnapshot = field(repr=False)
    _bitmap: BitMap = field(repr=False)

    @property
    def snapshot_id(self) -> str:
        return self._snapshot.snapshot_id

    def count(self) -> int:
        return len(self._bitmap)

    def __len__(self) -> int:
        return len(self._bitmap)

    def iter_rids(self) -> Iterator[int]:
        yield from self._bitmap

    def limit(self, n: int) -> list[int]:
        if n < 0:
            raise ValueError("limit must be >= 0")
        return list(islice(self._bitmap, n))

    def iter_location_batches(self,
                              batch_size: int = 8192) -> Iterator[LocationBatch]:
        self._snapshot._check_open()
        if type(batch_size) is not int or batch_size <= 0:
            raise ValueError("batch_size must be positive")
        locations = self._snapshot._locations
        for chunk in _chunked(self._bitmap, batch_size):
            indexed = np.asarray(chunk, dtype=np.int64)
            rows = locations[indexed]
            yield LocationBatch(
                self._snapshot.snapshot_id,
                indexed,
                np.asarray(rows["object_idx"], dtype=np.uint32),
                np.asarray(rows["image_offset"], dtype=np.uint64),
                np.asarray(rows["image_size"], dtype=np.uint64),
                np.asarray(rows["metadata_offset"], dtype=np.uint64),
                np.asarray(rows["metadata_size"], dtype=np.uint64),
                np.asarray(rows["format_id"], dtype=np.uint16),
                np.asarray(rows["flags"], dtype=np.uint8),
            )

    def iter_record_batches(
            self, batch_size: int = 8192) -> Iterator[RecordBatch]:
        self._snapshot._check_open()
        if type(batch_size) is not int or batch_size <= 0:
            raise ValueError("batch_size must be positive")
        catalog = self._snapshot._catalog
        getlimit = getattr(catalog, "getlimit", None)
        if getlimit is not None:
            limit = getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER)
            if limit <= 0:
                raise ValueError("SQLite variable limit must be positive")
            size = min(batch_size, limit)
        else:
            size = min(batch_size, 999)
        for pending in _chunked(self._bitmap, size):
            offset = 0
            while offset < len(pending):
                chunk = pending[offset:offset + size]
                placeholders = ",".join("?" for _ in chunk)
                try:
                    rows = catalog.execute(
                        "SELECT r.rid, r.record_id, r.source_id, r.dataset_id,"
                        " s.name, d.name, r.post_id"
                        " FROM records r JOIN sources s ON s.source_id = r.source_id"
                        " JOIN datasets d ON d.dataset_id = r.dataset_id"
                        f" WHERE r.rid IN ({placeholders}) ORDER BY r.rid", chunk).fetchall()
                except sqlite3.OperationalError as exc:
                    if (getlimit is not None or str(exc) != "too many SQL variables"
                            or len(chunk) == 1):
                        raise
                    size = max(1, len(chunk) // 2)
                    continue
                offset += len(chunk)
                yield RecordBatch(
                    self._snapshot.snapshot_id,
                    [row[0] for row in rows],
                    [row[1].hex() for row in rows],
                    [row[2] for row in rows],
                    [row[3] for row in rows],
                    [row[4] for row in rows],
                    [row[5] for row in rows],
                    [row[6] for row in rows],
                )


def _chunked(bitmap: BitMap, size: int) -> Iterator[list[int]]:
    iterator = iter(bitmap)
    while True:
        chunk = list(islice(iterator, size))
        if not chunk:
            return
        yield chunk


class _Term:
    """One AND term planned from stored cardinality before any blob loads."""

    def __init__(self, stored: int, kind: str,
                 ids: list[int], namespace_id: int | None) -> None:
        self.stored = stored
        self.kind = kind
        self.ids = ids
        self.namespace_id = namespace_id

    def materialize(self, snapshot: RuntimeSnapshot) -> BitMap:
        if self.kind == "tag":
            bitmap = snapshot._get_bitmap("tag", self.ids[0])
            for tag_id in self.ids[1:]:
                bitmap |= snapshot._get_bitmap("tag", tag_id)
            return bitmap
        if self.kind == "namespace_minus":
            excluded = BitMap()
            for tag_id in self.ids:
                excluded |= snapshot._get_bitmap("tag", tag_id)
            known = snapshot._get_bitmap("namespace", self.namespace_id)
            return known - excluded
        # source/dataset terms: OR over every selected id.
        union = snapshot._get_bitmap(self.kind, self.ids[0])
        for bitmap_id in self.ids[1:]:
            union |= snapshot._get_bitmap(self.kind, bitmap_id)
        return union


def _stored_cardinality(snapshot: RuntimeSnapshot, kind: str,
                        ids: list[int]) -> int:
    """Planner input from the catalog, read before any blob loads.

    Single id: the exact stored cardinality. OR group: the pre-materialization
    upper bound (sum of stored cardinalities) for tag/source/dataset kinds.
    """
    if len(ids) == 1:
        row = snapshot._bitmaps.execute(
            "SELECT cardinality FROM bitmaps WHERE kind = ? AND id = ?",
            (kind, ids[0])).fetchone()
        return row[0] if row else 0
    if kind in ("tag", "source", "dataset"):
        placeholders = ",".join("?" for _ in ids)
        row = snapshot._bitmaps.execute(
            f"SELECT COALESCE(SUM(cardinality), 0) FROM bitmaps"
            f" WHERE kind = ? AND id IN ({placeholders})",
            [kind, *ids]).fetchone()
        return row[0]
    raise ValueError(f"unsupported union kind: {kind}")


def _branch_result(snapshot: RuntimeSnapshot, spec: RuntimeQuerySpec) -> BitMap:
    filters = [(name, value) for name, value in
               (("width", spec.width_gt), ("height", spec.height_gt))
               if value is not None]
    if filters:
        columns = {row[1] for row in snapshot._catalog.execute("PRAGMA table_info(records)")}
        if not {name for name, _ in filters} <= columns:
            raise ValueError("dimension filters require recompiling this runtime from its P2 index")

    def dimensions() -> BitMap:
        # Indexed catalog range scan; no image reads or per-record queries.
        where = " AND ".join(f"{name} > ?" for name, _ in filters)
        return BitMap(row[0] for row in snapshot._catalog.execute(
            f"SELECT rid FROM records WHERE {where}", [value for _, value in filters]))

    terms: list[_Term] = []
    if spec.sources:
        source_ids = [snapshot._source_id(source)
                      for source in spec.sources]
        terms.append(_Term(
            _stored_cardinality(snapshot, "source", source_ids), "source",
            source_ids, None))
    if spec.datasets:
        dataset_ids = [snapshot._dataset_id(dataset)
                       for dataset in spec.datasets]
        terms.append(_Term(
            _stored_cardinality(snapshot, "dataset", dataset_ids), "dataset",
            dataset_ids, None))
    if spec.all_tags:
        for tag_id, _ in _resolve_tags(snapshot, spec.namespace, spec.all_tags,
                                       "all_tags"):
            terms.append(_Term(
                _stored_cardinality(snapshot, "tag", [tag_id]), "tag",
                [tag_id], None))
    if spec.any_tags:
        ids = [tag_id for tag_id, _ in _resolve_tags(
            snapshot, spec.namespace, spec.any_tags, "any_tags")]
        terms.append(_Term(
            _stored_cardinality(snapshot, "tag", ids), "tag", ids, None))
    if spec.none_tags:
        by_namespace: dict[int, list[int]] = {}
        for tag_id, namespace_id in _resolve_tags(
                snapshot, spec.namespace, spec.none_tags, "none_tags"):
            by_namespace.setdefault(namespace_id, []).append(tag_id)
        for namespace_id, tag_ids in sorted(by_namespace.items()):
            stored_known = _stored_cardinality(
                snapshot, "namespace", [namespace_id])
            terms.append(_Term(stored_known, "namespace_minus", tag_ids,
                               namespace_id))
    if not terms:
        return dimensions() if filters else BitMap(range(snapshot.rid_count))
    # Plan by stored cardinality: OR terms use their pre-materialization
    # bound, so the smallest constraint is applied first and the AND can
    # short-circuit without loading the remaining blobs at all.
    terms.sort(key=lambda term: term.stored)
    result: BitMap | None = None
    for term in terms:
        bitmap = term.materialize(snapshot)
        result = bitmap if result is None else (result & bitmap)
        if result is not None and not result:
            return result
    return result & dimensions() if filters else result


def evaluate_spec(snapshot: RuntimeSnapshot,
                  spec: RuntimeQuerySpec) -> QueryResult:
    if spec.any_of:
        if not spec.any_of:
            raise ValueError("any_of must not be empty")
        result = BitMap()
        for branch in spec.any_of:
            if branch.any_of:
                raise ValueError("nested any_of is not supported")
            result |= _branch_result(snapshot, branch)
        return QueryResult(snapshot, result)
    return QueryResult(snapshot, _branch_result(snapshot, spec))
