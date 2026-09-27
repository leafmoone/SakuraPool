"""P3 runtime query domain: spec, plan semantics, lazy result iteration."""

from __future__ import annotations

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
    branch specs evaluated with the same single-branch semantics.
    """

    sources: tuple[str, ...] = ()
    datasets: tuple[str, ...] = ()
    namespace: str | None = None
    all_tags: tuple[TagRef, ...] = ()
    any_tags: tuple[TagRef, ...] = ()
    none_tags: tuple[TagRef, ...] = ()
    any_of: tuple["RuntimeQuerySpec", ...] = ()

    def __post_init__(self) -> None:
        if self.any_of and (self.sources or self.datasets or self.all_tags
                            or self.any_tags or self.none_tags):
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

    def iter_rids(self) -> Iterator[int]:
        yield from self._bitmap

    def limit(self, n: int) -> list[int]:
        if n < 0:
            raise ValueError("limit must be >= 0")
        return list(islice(self._bitmap, n))

    def iter_location_batches(self,
                              batch_size: int = 8192) -> Iterator[LocationBatch]:
        self._snapshot._check_open()
        if batch_size <= 0:
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
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        catalog = self._snapshot._catalog
        for chunk in _chunked(self._bitmap, batch_size):
            placeholders = ",".join("?" for _ in chunk)
            rows = catalog.execute(
                "SELECT rid, record_id, source_id, dataset_id, post_id FROM records"
                f" WHERE rid IN ({placeholders}) ORDER BY rid", chunk).fetchall()
            rows.sort(key=lambda row: row[0])
            yield RecordBatch(
                self._snapshot.snapshot_id,
                [row[0] for row in rows],
                [row[1].hex() for row in rows],
                [row[2] for row in rows],
                [row[3] for row in rows],
                [row[4] for row in rows],
            )


def _chunked(bitmap: BitMap, size: int) -> Iterator[list[int]]:
    iterator = iter(bitmap)
    while True:
        chunk = list(islice(iterator, size))
        if not chunk:
            return
        yield chunk


def _branch_result(snapshot: RuntimeSnapshot, spec: RuntimeQuerySpec) -> BitMap:
    terms: list[tuple[int, BitMap]] = []
    if spec.sources:
        union = BitMap()
        for source in spec.sources:
            union |= snapshot._get_bitmap(
                "source", snapshot._source_id(source))
        terms.append((len(union), union))
    if spec.datasets:
        union = BitMap()
        for dataset in spec.datasets:
            union |= snapshot._get_bitmap(
                "dataset", snapshot._dataset_id(dataset))
        terms.append((len(union), union))
    if spec.all_tags:
        for tag_id, _ in _resolve_tags(snapshot, spec.namespace, spec.all_tags,
                                       "all_tags"):
            bitmap = snapshot._get_bitmap("tag", tag_id)
            terms.append((len(bitmap), bitmap))
    if spec.any_tags:
        union = BitMap()
        for tag_id, _ in _resolve_tags(snapshot, spec.namespace, spec.any_tags,
                                       "any_tags"):
            union |= snapshot._get_bitmap("tag", tag_id)
        terms.append((len(union), union))
    if spec.none_tags:
        by_namespace: dict[int, list[int]] = {}
        for tag_id, namespace_id in _resolve_tags(
                snapshot, spec.namespace, spec.none_tags, "none_tags"):
            by_namespace.setdefault(namespace_id, []).append(tag_id)
        for namespace_id, tag_ids in sorted(by_namespace.items()):
            excluded = BitMap()
            for tag_id in tag_ids:
                excluded |= snapshot._get_bitmap("tag", tag_id)
            known = snapshot._get_bitmap("namespace", namespace_id)
            terms.append((len(known - excluded), known - excluded))
    if not terms:
        return BitMap(range(snapshot.rid_count))
    result: BitMap | None = None
    for _, bitmap in sorted(terms, key=lambda pair: pair[0]):
        result = bitmap if result is None else (result & bitmap)
    return result


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
