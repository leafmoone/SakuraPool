"""Deterministic P3 source fingerprints and dense record ids."""

from __future__ import annotations

import hashlib
import json
from typing import Any

MAX_UINT32 = 2**32


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()


def snapshot_id(
    source_fingerprint: str, options: dict[str, Any], compiler: str, version: int
) -> str:
    payload = [version, compiler, source_fingerprint, options]
    return hashlib.sha256(canonical_json(payload)).hexdigest()


def record_sort_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return tuple(row[name] for name in ("dataset_id", "object_id", "sample_path", "record_id"))


def check_rid_capacity(count: int) -> None:
    """Reject count > 2**32 before any allocation or uint32 cast."""
    if not isinstance(count, int) or isinstance(count, bool):
        raise ValueError("sample count must be an int")
    if count < 0 or count > MAX_UINT32:
        raise ValueError("sample count exceeds uint32 rid capacity")


def assign_rids(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    check_rid_capacity(len(rows))
    ordered = sorted(rows, key=record_sort_key)
    return [dict(row, rid=rid) for rid, row in enumerate(ordered)]
