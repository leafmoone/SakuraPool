"""Frozen deterministic selection; selection is local and never fetches providers."""

from __future__ import annotations

import hashlib
import heapq
import json
import re
import sys
from dataclasses import dataclass
from itertools import islice

from ..capacity import CapacityConfig
from ..runtime import RuntimeQuerySpec

FORMAT = "sakurapool-task-v1"
ALGORITHM = "sha256-seed-record-topk-v1"
MAX_SELECTION = 100_000
MAX_SAMPLE = 10_000
BATCH_SIZE = 512


def canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


def normalize_query(spec):
    """Canonicalize commutative terms without changing namespace semantics."""
    if not isinstance(spec, RuntimeQuerySpec):
        raise ValueError("typed P3 query required")

    def names(values):
        if any(not isinstance(v, str) or not v or len(v) > 4096 for v in values):
            raise ValueError("query names invalid")
        return sorted(set(values))

    def tags(values):
        normalized = []
        for value in values:
            if isinstance(value, str):
                if not value or len(value) > 4096:
                    raise ValueError("query tag invalid")
                normalized.append(value)
            elif isinstance(value, (tuple, list)) and len(value) == 2:
                if any(not isinstance(v, str) or not v or len(v) > 4096 for v in value):
                    raise ValueError("qualified query tag invalid")
                normalized.append(list(value))
            else:
                raise ValueError("query tag shape invalid")
        unique = {canonical(value): value for value in normalized}
        return [unique[key] for key in sorted(unique)]

    if spec.namespace is not None and (
        not isinstance(spec.namespace, str) or not spec.namespace or len(spec.namespace) > 4096
    ):
        raise ValueError("query namespace invalid")
    branches = {}
    for branch in spec.any_of:
        normalized = normalize_query(branch)
        branches[canonical(normalized)] = normalized
    return {
        "sources": names(spec.sources),
        "datasets": names(spec.datasets),
        "namespace": spec.namespace,
        "all_tags": tags(spec.all_tags),
        "any_tags": tags(spec.any_tags),
        "none_tags": tags(spec.none_tags),
        "any_of": [branches[key] for key in sorted(branches)],
    }


@dataclass(frozen=True)
class Selection:
    mode: str = "all"
    limit: int | None = None
    seed: str | None = None
    records: tuple[str, ...] = ()
    algorithm: str = ALGORITHM

    def validate(self, capacity=None):
        capacity = CapacityConfig() if capacity is None else capacity
        if self.mode not in ("all", "first", "sample", "records"):
            raise ValueError("selection mode invalid")
        if self.algorithm != ALGORITHM:
            raise ValueError("selection algorithm unsupported")
        if self.mode in ("first", "sample"):
            maximum = capacity.sample_heap_count if self.mode == "sample" else capacity.freeze_count
            if type(self.limit) is not int or not 0 <= self.limit <= maximum:
                raise ValueError("selection limit invalid")
        elif self.limit is not None:
            raise ValueError("selection limit not applicable")
        if self.mode == "sample":
            if not isinstance(self.seed, str) or not 1 <= len(self.seed) <= 1024:
                raise ValueError("sample requires explicit bounded seed")
        elif self.seed is not None:
            raise ValueError("seed only applies to sample")
        if self.mode != "records" and self.records:
            raise ValueError("explicit records require records selection")
        if len(self.records) > capacity.freeze_count:
            raise ValueError("explicit selection cap")
        if len(set(self.records)) != len(self.records) or any(
            not isinstance(v, str) or not re.fullmatch(r"[0-9a-f]{32}", v) for v in self.records
        ):
            raise ValueError("explicit records invalid or duplicated")
        if len(canonical(self.records)) > capacity.explicit_records_bytes:
            raise ValueError("explicit records byte cap")

    def header(self, capacity=None):
        self.validate(capacity)
        return {
            "mode": self.mode,
            "limit": self.limit,
            "seed": self.seed,
            "algorithm": self.algorithm if self.mode == "sample" else "rid-order-v1",
        }


@dataclass(frozen=True)
class SelectedRecord:
    rid: int
    record_id: str
    source: str
    dataset: str
    post_id: str


def _entry_bytes(entry):
    record = entry[2]
    return (
        sys.getsizeof(entry)
        + sys.getsizeof(entry[0])
        + sys.getsizeof(entry[1])
        + sys.getsizeof(record)
        + sys.getsizeof(record.__dict__)
        + sum(sys.getsizeof(value) for value in record.__dict__.values())
    )


def selected_records(runtime, query, selection, capacity=None):
    """Stream all candidates; sample ranks and ordering remain v1-compatible."""
    capacity = CapacityConfig() if capacity is None else capacity
    selection.validate(capacity)
    normalize_query(query)
    result = runtime.query(query)

    def candidates():
        for batch in result.iter_record_batches(capacity.record_batch):
            for values in zip(
                batch.rid, batch.record_id, batch.source_name, batch.dataset_name, batch.post_id
            ):
                yield SelectedRecord(*values)

    if selection.mode == "records":
        for record_id in selection.records:
            record = runtime.resolve_record(record_id)
            if record.rid not in result._bitmap:
                raise ValueError("explicit record outside query")
            row = runtime._catalog.execute(
                "SELECT s.name,d.name FROM sources s,datasets d "
                "WHERE s.source_id=? AND d.dataset_id=?",
                (record.source_id, record.dataset_id),
            ).fetchone()
            yield SelectedRecord(record.rid, record_id, *row, record.post_id)
    elif selection.mode == "first":
        yield from islice(candidates(), selection.limit)
    elif selection.mode == "sample":
        heap = []
        live_bytes = 0
        if selection.limit == 0:
            return
        # Catalog record IDs are lowercase ASCII hex. Reuse the canonical seed
        # prefix while hashing exactly the same JSON bytes as the v1 algorithm.
        prefix = hashlib.sha256(
            b"sakurapool-task-sample-v1\x00[" + canonical(selection.seed) + b',"'
        )
        for record in candidates():
            digest = prefix.copy()
            digest.update(record.record_id.encode("ascii") + b'"]')
            rank = int.from_bytes(digest.digest(), "big")
            entry = (-rank, -int(record.record_id, 16), record)
            if len(heap) < selection.limit or entry > heap[0]:
                amount = _entry_bytes(entry)
                old = _entry_bytes(heap[0]) if len(heap) == selection.limit else 0
                # Include the replacement candidate, list allocation and final sort scratch.
                if (
                    live_bytes + amount + sys.getsizeof(heap) + 32 * (len(heap) + 1)
                    > capacity.heap_memory_bytes
                ):
                    raise ValueError("sample heap memory cap exceeded")
                if len(heap) < selection.limit:
                    heapq.heappush(heap, entry)
                else:
                    heapq.heapreplace(heap, entry)
                live_bytes += amount - old
        for _, _, record in sorted(heap, key=lambda entry: (-entry[0], -entry[1])):
            yield record
    else:
        if result.count() > capacity.freeze_count:
            raise ValueError("selection entry cap exceeded")
        yield from candidates()


def selection_digest(rows):
    digest = hashlib.sha256(b"sakurapool-task-selection-v1\x00")
    count = 0
    for values in rows:
        digest.update(canonical([count, *values]) + b"\n")
        count += 1
    return count, digest.hexdigest()


def plan_digest(header):
    return hashlib.sha256(b"sakurapool-task-plan-v1\x00" + canonical(header)).hexdigest()
