"""Versioned implementation capacity and explicit user resource policy."""

from __future__ import annotations

import sys
from dataclasses import asdict, dataclass

CAPACITY_VERSION = 1
UINT64_MAX = (1 << 64) - 1
# Protocol bootstrap is read before negotiated configuration; bound it independently.
PROTOCOL_SAFETY_BYTES = 16 << 20
# Locked reqwest 0.12.28 does not forward hyper's public max_buf_size/max_headers.
# hyper 1.11.1 src/proto/h1/io.rs:15-23,182-207: incomplete-head threshold,
# NOT a decoded-field cap or strict allocation ceiling (BytesMut can overallocate).
HTTP_PARSER_BYTES = 8192 + 4096 * 100
# Reserve the canonical Range status line and terminating CRLF. Decoded fields
# are charged as name + value + 4; arbitrary wire whitespace/reason phrases can
# hit the parser threshold earlier. Independently, role.rs:31,1033-1052 permits
# at most 100 wire fields, including duplicates. Neither bound is user-adjustable.
HTTP_SUPPORTED_HEADER_BYTES = HTTP_PARSER_BYTES - len(b"HTTP/1.1 206 Partial Content\r\n\r\n")
HTTP_MAX_HEADER_FIELDS = 100
IMPLEMENTATION_BOUNDARIES = {
    "integer": UINT64_MAX,
    "buffer": min(sys.maxsize, UINT64_MAX),
    "sqlite_bytes": min(sys.maxsize, 4294967294 * 4096),
    "sqlite_records": (1 << 63) - 1,
    "ledger_count": (1 << 16) - 1,
    "ledger_payload_bytes": (1 << 32) - 1,
    "protocol_safety_bytes": PROTOCOL_SAFETY_BYTES,
    "http_header_bytes": HTTP_SUPPORTED_HEADER_BYTES,
    "http_header_fields": HTTP_MAX_HEADER_FIELDS,
}


def _configuration(cls, value):
    """Partial user configuration; persisted documents use strict from_dict."""
    if value is None:
        return cls()
    if not isinstance(value, dict) or set(value) - set(cls.__dataclass_fields__):
        raise ValueError("unknown configuration fields")
    return cls(**value)


@dataclass(frozen=True)
class CapacityConfig:
    freeze_count: int = 100_000
    sample_heap_count: int = 10_000
    heap_memory_bytes: int = 256 << 20
    record_batch: int = 512
    explicit_records_bytes: int = 4 << 20
    task_db_bytes: int = 32 << 20
    task_journal_bytes: int = 33 << 20
    task_header_bytes: int = 65536
    export_manifest_bytes: int = 32 << 20
    image_max_bytes: int = 8 << 20
    metadata_max_bytes: int = 8 << 20
    range_chunk_bytes: int = 8 << 20
    ledger_slot_bytes: int = 1 << 20
    pending_leases: int = 2048
    condition_proofs: int = 2048
    http_header_bytes: int = 65536
    rpc_line_bytes: int = 65536

    def __post_init__(self):
        for name, value in asdict(self).items():
            maximum = IMPLEMENTATION_BOUNDARIES["buffer"]
            if name in ("freeze_count", "sample_heap_count", "record_batch"):
                maximum = min(maximum, IMPLEMENTATION_BOUNDARIES["sqlite_records"])
            elif name == "task_db_bytes":
                maximum = IMPLEMENTATION_BOUNDARIES["sqlite_bytes"]
            elif name in ("pending_leases", "condition_proofs"):
                maximum = IMPLEMENTATION_BOUNDARIES["ledger_count"]
            elif name == "ledger_slot_bytes":
                maximum = min(maximum, 128 + IMPLEMENTATION_BOUNDARIES["ledger_payload_bytes"])
            elif name == "http_header_bytes":
                maximum = IMPLEMENTATION_BOUNDARIES["http_header_bytes"]
            elif name == "rpc_line_bytes":
                maximum = PROTOCOL_SAFETY_BYTES
            if type(value) is not int or not 0 < value <= maximum:
                raise ValueError(
                    f"{name} must be positive and within implementation boundary {maximum}"
                )
        if self.sample_heap_count > self.freeze_count:
            raise ValueError("sample_heap_count exceeds freeze_count")
        if self.task_db_bytes < 4096:
            raise ValueError("task_db_bytes must allow at least one SQLite page")
        if self.task_journal_bytes < self.task_db_bytes:
            raise ValueError("task_journal_bytes must cover task_db_bytes")
        # v3: header + policy prefix + used counters + maximum lease/proof rows.
        if self.ledger_slot_bytes < self.ledger_layout_bytes:
            raise ValueError("ledger_slot_bytes cannot contain configured maximum rows")

    @property
    def ledger_layout_bytes(self):
        return 128 + 73 + 48 + self.pending_leases * 88 + self.condition_proofs * 64

    @property
    def ledger_memory_model(self):
        """Conservative live Python-object admission, not a process RSS ceiling.

        Charge both decoded states, dict resizing/sorting, and all overlapping
        read/hash/padded encode/write copies. Per-row table allowance deliberately
        exceeds a sparse dict entry; object sizes come from this interpreter.
        Static headroom is shared only while the interprocess domain lock is held
        and operation frames are cleared before unlock. Allocator caches, native
        libraries, unrelated user references and other processes' baselines are
        outside this working-set contract.
        """
        size = sys.getsizeof
        integer = size(UINT64_MAX)
        lease = size("f" * 32) + size(dict.fromkeys(range(9))) + 9 * integer + 256
        proof = 2 * size("f" * 64) + 256
        state = 2 * size({}) + size(dict.fromkeys(range(6))) + 6 * integer
        rows = state + self.pending_leases * lease + self.condition_proofs * proof
        model = {
            "two_slot_reads": 2 * (self.ledger_slot_bytes + size(b"")),
            "two_decoded_states": 2 * rows,
            "decode_hash_and_padding_copies": 2 * (self.ledger_slot_bytes + size(b"")),
            "encode_payload": 2 * self.ledger_layout_bytes + size(bytearray()),
            "encode_padded_hash_and_write_copies": 4 * (self.ledger_slot_bytes + size(b"")),
            "sort_rows": (self.pending_leases + self.condition_proofs)
            * (size((None, None)) + 4 * size(None)),
            "fixed_control_and_bootstrap": 2 * (1 << 20),
        }
        return model

    @property
    def ledger_effective_headroom(self):
        return sum(self.ledger_memory_model.values())

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, value):
        if not isinstance(value, dict) or set(value) != set(cls.__dataclass_fields__):
            raise ValueError("capacity fields invalid")
        return cls(**value)

    @classmethod
    def from_configuration(cls, value=None):
        return _configuration(cls, value)


@dataclass(frozen=True)
class ResourcePolicy:
    """None means no cumulative user quota; uint64 remains a format boundary.

    Disk and inflight must remain bounded. Legacy P4 uses its unchanged LIMITS.
    """

    body: int | None = None
    metadata: int | None = None
    attempts: int | None = None
    records: int | None = None
    saved_samples: int | None = None
    saved_bytes: int | None = None
    disk: int = 4 << 30
    inflight: int = 256 << 20

    def __post_init__(self):
        for name, value in asdict(self).items():
            if value is None and name not in ("disk", "inflight"):
                continue
            if type(value) is not int or not 0 <= value <= UINT64_MAX:
                raise ValueError(f"{name} must be an unsigned 64-bit bound")
            if name in ("disk", "inflight") and value == 0:
                raise ValueError(f"{name} must be positive and bounded")

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, value):
        if not isinstance(value, dict) or set(value) != set(cls.__dataclass_fields__):
            raise ValueError("resource policy fields invalid")
        return cls(**value)

    @classmethod
    def from_configuration(cls, value=None):
        return _configuration(cls, value)

    def admit(self, totals):
        for name, maximum in self.to_dict().items():
            value = totals[name]
            if type(value) is not int or not 0 <= value <= UINT64_MAX:
                raise ValueError(f"{name} integer implementation boundary exceeded")
            if maximum is not None and value > maximum:
                raise ValueError(f"{name} quota exceeded")
