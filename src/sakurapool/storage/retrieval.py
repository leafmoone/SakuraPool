"""Offline-testable single-sample byte retrieval; not a package loader.

The caller MUST supply a binding independently verified against one fixed
snapshot and a complete scan-audit manifest. This routine cannot infer remote
paths, member paths or expected hashes from record_id. It never scans TARs.
"""

from __future__ import annotations

import hashlib
import os
import re
import secrets
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from ..fs_durability import publish_noreplace as _publish_directory
from .budget import BudgetExceeded, BudgetLedger, Reservation
from .transport import MAX_MEMBER, BoundObject, GuardedTransport, RemoteIOError

HEX64 = re.compile(r"[0-9a-f]{64}\Z")
RECORD_ID = re.compile(r"[0-9a-f]{32}\Z")
EXTENSIONS = frozenset({".jpg", ".jpeg", ".png", ".webp", ".avif"})
MAX_MERGE = 16 * (1 << 20)
MAX_GAP = 64 * (1 << 10)


@dataclass(frozen=True)
class Extent:
    offset: int
    size: int
    sha256: str

    def validate(self, object_size: int, *, allow_empty: bool = False) -> None:
        if (
            type(self.offset) is not int
            or type(self.size) is not int
            or self.offset < 0
            or not 0 <= self.size <= MAX_MEMBER
            or (self.size == 0 and not allow_empty)
            or self.offset + self.size > object_size
            or not isinstance(self.sha256, str)
            or not HEX64.fullmatch(self.sha256)
        ):
            raise ValueError("invalid audited member extent or SHA")


@dataclass(frozen=True)
class AuditedSample:
    record_id: str
    image: Extent
    json_member: Extent | None
    image_suffix: str

    def validate(self, size: int) -> None:
        if not isinstance(self.record_id, str) or not RECORD_ID.fullmatch(self.record_id):
            raise ValueError("invalid record identity")
        if self.image_suffix not in EXTENSIONS:
            raise ValueError("invalid audited image suffix")
        self.image.validate(size)
        if self.json_member is not None:
            self.json_member.validate(size, allow_empty=True)


def _checked(payload: bytes, expected: Extent) -> bytes:
    if len(payload) != expected.size or hashlib.sha256(payload).hexdigest() != expected.sha256:
        raise RemoteIOError("member content SHA/size changed; no delivery")
    return payload


def _real_output_root(root: Path, ledger=None, *, physical_root=None) -> Path:
    root = Path(root).absolute()
    if ledger is None:
        from ..fs_safety import plain_entry

        if ".." in root.parts:
            raise ValueError("noncanonical output path")
        plain_entry(root, directory=True)
        if physical_root is not None:
            physical_root = plain_entry(Path(physical_root).absolute(), directory=True)
            if root != physical_root and not root.is_relative_to(physical_root):
                raise ValueError("output escapes execution root")
        for component in root.parents:
            plain_entry(component, directory=True)
        return root
    workspace = getattr(ledger, "workspace", None)
    if workspace is not None:
        workspace.check()
        if Path(ledger.root).absolute() != workspace.state:
            raise ValueError("workspace ledger state root mismatch")
        physical_root = workspace.root
    else:
        physical_root = ledger.root
    if ".." in root.parts:
        raise ValueError("noncanonical output path")
    if (root != physical_root and not root.is_relative_to(physical_root)) or not root.is_dir():
        raise ValueError("fetch output must be existing child of budget work root")
    for component in (root, *root.parents):
        if component == physical_root.parent:
            break
        if component.is_symlink() or (
            hasattr(component, "is_junction") and component.is_junction()
        ):
            raise ValueError("output contains symlink or junction")
    if not root.resolve().is_relative_to(physical_root.resolve()):
        raise ValueError("output escaped fixed budget root")
    return root


def fetch_bounded_samples(
    transport: GuardedTransport,
    ledger: BudgetLedger,
    bound: BoundObject,
    samples: Iterable[AuditedSample],
    output_root: Path,
    *,
    workers: int = 4,
    merged: bool = True,
) -> list[Path]:
    """Bounded worker futures contain *paths*, never pending payload bytes.

    Caller resolves and validates snapshot/package in the owner thread first.
    Each worker has its own requests Session; all share the SQLite ledger and
    no more than `workers` work items exist at once. Cancellation is awaited.
    """
    if transport.ledger is not ledger:
        raise ValueError("retrieval transport and disk budget must be identical")
    from .production import RustProductionTransport

    if not isinstance(ledger, BudgetLedger) or (
        not ledger.offline_mode
        and not (isinstance(transport, RustProductionTransport) and transport.production_profile)
    ):
        raise BudgetExceeded("production fetch BLOCKED: explicit Rust profile and budget required")
    if type(workers) is not int or not 1 <= workers <= 8:
        raise ValueError("worker count must be between 1 and 8")
    root = _real_output_root(output_root, ledger)
    candidate = (
        transport.verified_bound_object(bound)
        if isinstance(transport, RustProductionTransport)
        else None
    )
    pending: list = []
    results: list[Path] = []

    def run(sample: AuditedSample) -> Path:
        with transport.clone() as client:
            if candidate is not None:
                verified = client.verify_conditions(candidate)
                if verified != candidate or client.verified_bound_object(bound) != candidate:
                    raise RemoteIOError("worker fresh binding differs from parent scope")
            return fetch_bound_sample(client, ledger, bound, sample, root, merged=merged)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        try:
            for sample in samples:
                sample.validate(bound.size)
                pending.append(pool.submit(run, sample))
                if len(pending) == workers:
                    results.append(pending.pop(0).result())
            for future in pending:
                results.append(future.result())
        except BaseException:
            for future in pending:
                future.cancel()
            raise
    return results


def fetch_bound_sample(
    transport: GuardedTransport,
    ledger: BudgetLedger,
    bound: BoundObject,
    sample: AuditedSample,
    output_root: Path,
    *,
    merged: bool = True,
) -> Path:
    """Verify raw bytes and deliver image+optional JSON as one atomic dir.

    Single worker consumes and acknowledges each read before any future/queue
    receives the result. Inflight reservation includes raw+slice/copy peak.
    A bounded worker pool must restrict completed-but-unconsumed futures by
    handing back only *paths*, never body bytes. No image decode/transform.
    """
    if transport.ledger is not ledger:
        raise ValueError("retrieval transport and disk budget must be identical")
    from .production import RustProductionTransport

    if not isinstance(ledger, BudgetLedger) or (
        not ledger.offline_mode
        and not (isinstance(transport, RustProductionTransport) and transport.production_profile)
    ):
        raise BudgetExceeded("production fetch BLOCKED: explicit Rust profile and budget required")
    sample.validate(bound.size)
    output_root = _real_output_root(output_root, ledger)
    final = output_root / sample.record_id
    if final.exists() or final.is_symlink():
        raise FileExistsError("sample already exists; never overwrite user files")
    image, meta = sample.image, sample.json_member
    selected = [image] + ([meta] if meta else [])
    start = min(item.offset for item in selected)
    end = max(item.offset + item.size for item in selected)
    merge_limit = min(MAX_MERGE, getattr(transport, "max_range_bytes", MAX_MERGE))
    merge_possible = (
        merged
        and meta is not None
        and meta.size > 0
        and end - start <= merge_limit
        and end - start - image.size - meta.size <= MAX_GAP
        and (image.offset + image.size <= meta.offset or meta.offset + meta.size <= image.offset)
    )
    total_saved = image.size + (meta.size if meta else 0)
    # Stage and final occupy ONE directory on the same filesystem: atomic
    # rename does not duplicate image+JSON bytes. Include marker + filesystem
    # overhead and leave ledger headroom in the root-wide disk check.
    disk_lease = ledger.reserve(
        Reservation(disk=total_saved + 8192, saved_samples=1, saved_bytes=total_saved)
    )
    stage = output_root / (".sakurapool-" + secrets.token_hex(16))
    created: set[str] = set()
    try:
        stage.mkdir(exist_ok=False)
        if merge_possible:
            with transport.read_range_owned(bound, start, end - start) as payload:
                # Both copies live inside the payload context's inflight lease.
                image_bytes = _checked(
                    payload[image.offset - start : image.offset - start + image.size], image
                )
                json_bytes = _checked(
                    payload[meta.offset - start : meta.offset - start + meta.size], meta
                )
                for name, data in (
                    ("image" + sample.image_suffix, image_bytes),
                    ("metadata.json", json_bytes),
                ):
                    with (stage / name).open("xb") as handle:
                        created.add(name)
                        handle.write(data)
                        handle.flush()
                        os.fsync(handle.fileno())
        else:
            for name, item in (("image" + sample.image_suffix, image), ("metadata.json", meta)):
                if item is None:
                    continue
                # Presence is independent of size. A real empty metadata
                # member is published, but incurs no Range or HTTP attempt.
                if item.size == 0:
                    payload = _checked(b"", item)
                    with (stage / name).open("xb") as handle:
                        created.add(name)
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    continue
                with transport.read_range_owned(bound, item.offset, item.size) as payload:
                    _checked(payload, item)
                    with (stage / name).open("xb") as handle:
                        created.add(name)
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
        if final.exists() or final.is_symlink():
            raise FileExistsError("sample appeared while fetching; no overwrite")
        _publish_directory(stage, final)
        ledger.settle(disk_lease, saved_samples=1, saved_bytes=total_saved)
        return final
    except BaseException:
        # Only remove files exclusively created by this call. If an unknown
        # entry appeared, retain the stage and pending quota for manual audit.
        if (
            stage.is_dir()
            and not stage.is_symlink()
            and {p.name for p in stage.iterdir()} == created
        ):
            for name in created:
                (stage / name).unlink()
            stage.rmdir()
            ledger.settle(disk_lease)
        raise
