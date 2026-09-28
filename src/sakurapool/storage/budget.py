"""One persistent, root-wide P4 budget across guarded metadata and object reads.

Two fixed-size, checksummed slots replace SQLite to make this ledger's own
physical expansion finite. Host flush is not a power-loss durability guarantee.
Neither offline fixtures nor a new process may select a second production root.
"""

from __future__ import annotations

import hashlib
import os
import stat
import struct
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

DEFAULT_WORK_ROOT = Path("D:/SakuraTool/SakuraPool-P4-work")
MIB = 1 << 20
GIB = 1 << 30
SLOT_BYTES = MIB
SLOT_COUNT = 2
MAX_PENDING_LEASES = 2048
MAX_PROOFS = 2048
# Actual lock/plan allocations are calculated at runtime from the work volume.
LEDGER_HEADROOM = 2 * SLOT_BYTES
LIMITS = {
    "body": 8 * GIB, "metadata": 64 * MIB, "attempts": 2000,
    "disk": 4 * GIB, "records": 100_000, "saved_samples": 1000,
    "saved_bytes": 512 * MIB, "inflight": 256 * MIB,
}
RESERVED = ("body", "metadata", "disk", "records", "saved_samples", "saved_bytes", "inflight")
COUNTERS = ("body", "metadata", "records", "saved_samples", "saved_bytes")
_USED = (*COUNTERS, "attempts")
_LEASE_FIELDS = (*RESERVED, "consumed_body", "consumed_metadata")
_HEADER = struct.Struct("<8sHBBQHH32sI32s36s")
_USED_STRUCT = struct.Struct("<6Q")
_LEASE = struct.Struct("<16s9Q")
_PROOF = struct.Struct("<32s32s")
_MAGIC = b"SP4SLOT1"
_VERSION = 1
_HEADER_SHA_OFFSET = 60
assert _HEADER.size == 128 and _LEASE.size == 88 and _PROOF.size == 64
assert _HEADER.size + _USED_STRUCT.size + MAX_PENDING_LEASES * _LEASE.size \
    + MAX_PROOFS * _PROOF.size == 311472 < SLOT_BYTES


class BudgetExceeded(RuntimeError):
    """A bound was hit before further I/O; no alternative work root."""


class BudgetCorrupt(RuntimeError):
    """Unknown legacy, missing, short, or inconsistent ledger: never reset."""


def _cluster_bytes(root: Path) -> int:
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        sectors = wintypes.DWORD()
        sector_bytes = wintypes.DWORD()
        free = wintypes.DWORD()
        total = wintypes.DWORD()
        func = kernel.GetDiskFreeSpaceW
        func.argtypes = [wintypes.LPCWSTR, ctypes.POINTER(wintypes.DWORD),
                         ctypes.POINTER(wintypes.DWORD), ctypes.POINTER(wintypes.DWORD),
                         ctypes.POINTER(wintypes.DWORD)]
        func.restype = wintypes.BOOL
        if not func(str(root.anchor), sectors, sector_bytes, free, total):
            raise BudgetCorrupt("cannot query work-volume allocation cluster")
        result = sectors.value * sector_bytes.value
    else:
        result = os.statvfs(root).f_frsize
    if not 512 <= result <= MIB or result & (result - 1):
        raise BudgetCorrupt("unrecognized work-volume cluster size")
    return result


def _allocated_size(path: Path, length: int, cluster: int) -> int:
    rounded = (max(1, length) + cluster - 1) // cluster * cluster
    if os.name != "nt":
        return max(rounded, path.stat(follow_symlinks=False).st_blocks * 512)
    import ctypes
    from ctypes import wintypes
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    upper = wintypes.DWORD()
    func = kernel.GetCompressedFileSizeW
    func.argtypes = [wintypes.LPCWSTR, ctypes.POINTER(wintypes.DWORD)]
    func.restype = wintypes.DWORD
    ctypes.set_last_error(0)
    lower = func(str(path), upper)
    if lower == 0xFFFFFFFF and ctypes.get_last_error():
        raise BudgetCorrupt("cannot inspect allocated file size")
    return max(rounded, (upper.value << 32) | lower)


def _disk_usage(root: Path) -> int:
    """Conservatively count allocated clusters for *all* root files.

    Each hardlink is counted again. Reparse, unsupported, or unreadable entries
    fail closed. Directory metadata/device amplification is not claimed here.
    """
    cluster = _cluster_bytes(root)
    total = 0
    pending = [root]
    while pending:
        directory = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                status = entry.stat(follow_symlinks=False)
                if entry.is_symlink() or (getattr(status, "st_file_attributes", 0)
                                           & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)):
                    raise BudgetCorrupt("work root contains a reparse point")
                if stat.S_ISDIR(status.st_mode):
                    pending.append(Path(entry.path))
                elif stat.S_ISREG(status.st_mode):
                    total += _allocated_size(Path(entry.path), status.st_size, cluster)
                else:
                    raise BudgetCorrupt("work root contains unsupported file type")
    return total


@dataclass(frozen=True)
class Reservation:
    """Maximum outstanding resources, installed before a remote attempt."""

    body: int = 0
    metadata: int = 0
    disk: int = 0
    records: int = 0
    saved_samples: int = 0
    saved_bytes: int = 0
    inflight: int = 0
    attempt: bool = False

    def __post_init__(self) -> None:
        for name in RESERVED:
            value = getattr(self, name)
            if type(value) is not int or not 0 <= value < 2**64:
                raise ValueError(f"{name} reservation must be an unsigned 64-bit integer")
        if type(self.attempt) is not bool:
            raise ValueError("attempt must be boolean")
        if self.metadata > self.body:
            raise ValueError("metadata body must also reserve raw body bytes")
        if not self.attempt and not any(getattr(self, name) for name in RESERVED):
            raise ValueError("empty reservations cannot create unbounded ledger rows")


class BudgetLedger:
    """Single accounting domain for production; nested roots are offline-only."""

    def __init__(self, root: Path = DEFAULT_WORK_ROOT, *, _offline_test: bool = False,
                 _test_limits: dict[str, int] | None = None):
        root = Path(root).absolute()
        fixed = DEFAULT_WORK_ROOT.absolute()
        if root == fixed and _offline_test:
            raise ValueError("offline test ledger must use a nested synthetic root")
        if root != fixed and (not _offline_test or not root.is_relative_to(fixed)):
            raise ValueError("P4 budget root is fixed; alternate workdir forbidden")
        for part in (root, *root.parents):
            if part.is_symlink() or (hasattr(part, "is_junction") and part.is_junction()):
                raise BudgetCorrupt("budget root or parent is a symlink/junction")
        if not root.is_dir() or not fixed.is_dir():
            raise BudgetCorrupt("budget root must already be a real directory")
        if os.path.normcase(str(root.resolve())) != os.path.normcase(str(root)):
            raise BudgetCorrupt("budget root resolves to another location")
        if _test_limits is not None and not _offline_test:
            raise ValueError("only offline tests may lower budget limits")
        self.limits = dict(LIMITS)
        if _test_limits:
            if any(name not in LIMITS or type(value) is not int
                   or not 0 < value <= LIMITS[name]
                   for name, value in _test_limits.items()):
                raise ValueError("offline budget limits may only tighten caps")
            self.limits.update(_test_limits)
        self.root = root
        self._physical_root = fixed
        self._offline_mode = bool(_offline_test)
        self._root_hash = hashlib.sha256(os.path.normcase(str(root.resolve())).encode()).digest()
        self._mutex = threading.RLock()
        self.lock_path = root / "p4-budget.lock"
        self.slots = (root / "p4-budget.slot0", root / "p4-budget.slot1")
        self.path = self.slots[0]  # public diagnostic path, not another ledger
        for suffix in ("", "-journal", "-wal", "-shm"):
            legacy = root / ("p4-budget.sqlite" + suffix)
            if os.path.lexists(legacy):
                raise BudgetCorrupt("legacy SQLite ledger preserved; migration required")
        # Existing ledgers have already paid for their two slots and lock. An
        # ordinary reopen/status/settle must not demand another 2 MiB. Only
        # the first creator checks all new cluster-rounded allocations before
        # CREATE_NEW. Unknown orphan slots/locks are never cleared or reused.
        created = False
        if not os.path.lexists(self.lock_path):
            if any(os.path.lexists(path) for path in self.slots):
                raise BudgetCorrupt("orphan slot without budget lock")
            cluster = _cluster_bytes(fixed)
            if _disk_usage(fixed) + 2 * SLOT_BYTES + cluster > self.limits["disk"]:
                raise BudgetExceeded("P4 disk limit leaves no slot bootstrap room")
            try:
                with self.lock_path.open("xb") as lock:
                    if lock.write(b"L") != 1:
                        raise BudgetCorrupt("short budget lock write")
                    lock.flush()
                    os.fsync(lock.fileno())
                created = True
            except FileExistsError:
                # Another creator won CREATE_NEW; _locked validates its
                # marker/slots. Partial bootstrap is deliberately blocked.
                pass
        with self._locked() as _lock:
            if created:
                if any(path.exists() or path.is_symlink() for path in self.slots):
                    raise BudgetCorrupt("unknown slot during ledger bootstrap")
                if _disk_usage(fixed) + 2 * SLOT_BYTES > self.limits["disk"]:
                    raise BudgetExceeded("P4 disk limit leaves no slot allocation room")
                zero = ({name: 0 for name in _USED}, {}, {})
                for path in self.slots:
                    with path.open("xb") as slot:
                        self._write_exact(slot, b"\x00" * SLOT_BYTES)
                        slot.flush()
                        os.fsync(slot.fileno())
                    self._write_slot(path, 0, *zero)
            self._read_pair()

    @property
    def offline_mode(self) -> bool:
        return self._offline_mode

    @staticmethod
    def _write_exact(stream, data: bytes) -> None:
        view = memoryview(data)
        while view:
            size = stream.write(view)
            if size is None or size <= 0:
                raise BudgetCorrupt("short budget slot write")
            view = view[size:]

    @contextmanager
    def _locked(self):
        with self._mutex:
            with self.lock_path.open("r+b") as lock:
                if lock.seek(0, os.SEEK_END) != 1:
                    raise BudgetCorrupt("unknown budget lock file")
                lock.seek(0)
                if os.name == "nt":
                    import msvcrt
                    deadline = time.monotonic() + 30
                    while True:
                        try:
                            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
                            break
                        except OSError:
                            if time.monotonic() >= deadline:
                                raise BudgetCorrupt("budget interprocess lock timeout") from None
                            time.sleep(0.05)
                    try:
                        lock.seek(0)
                        if lock.read(1) != b"L":
                            raise BudgetCorrupt("unknown budget lock marker")
                        yield lock
                    finally:
                        lock.seek(0)
                        msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
                    try:
                        lock.seek(0)
                        if lock.read(1) != b"L":
                            raise BudgetCorrupt("unknown budget lock marker")
                        yield lock
                    finally:
                        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def _encode(self, generation: int, used: dict, leases: dict, proofs: dict) -> bytes:
        if not 0 <= generation < 2**64 or len(leases) > MAX_PENDING_LEASES \
                or len(proofs) > MAX_PROOFS:
            raise BudgetCorrupt("slot record count or generation exceeds format")
        payload = bytearray(_USED_STRUCT.pack(*(used[name] for name in _USED)))
        for key, fields in sorted(leases.items()):
            values = (fields[name] for name in _LEASE_FIELDS)
            payload.extend(_LEASE.pack(bytes.fromhex(key), *values))
        for key, value in sorted(proofs.items()):
            payload.extend(_PROOF.pack(bytes.fromhex(key), bytes.fromhex(value)))
        if _HEADER.size + len(payload) > SLOT_BYTES:
            raise BudgetExceeded("budget slot full")
        padded = bytes(payload) + bytes(SLOT_BYTES - _HEADER.size - len(payload))
        header = _HEADER.pack(_MAGIC, _VERSION, int(self._offline_mode), 0,
                              generation, len(leases), len(proofs), self._root_hash,
                              len(payload), bytes(32), bytes(36))
        digest = hashlib.sha256(header + padded).digest()
        header = header[:_HEADER_SHA_OFFSET] + digest + header[_HEADER_SHA_OFFSET + 32:]
        assert len(header) == _HEADER.size and len(header + padded) == SLOT_BYTES
        return header + padded

    def _write_slot(self, path: Path, generation: int, used: dict,
                    leases: dict, proofs: dict) -> None:
        content = self._encode(generation, used, leases, proofs)
        if path.is_symlink() or not path.is_file() or path.stat().st_size != SLOT_BYTES:
            raise BudgetCorrupt("budget slot missing, linked or incorrectly sized")
        with path.open("r+b") as slot:
            # Overwriting the old standby makes it invalid until BOTH fsyncs
            # complete. Its damaged state blocks reopening; never fall back to
            # the older slot and accidentally refund a sent request.
            slot.seek(_HEADER.size)
            self._write_exact(slot, content[_HEADER.size:])
            slot.flush()
            os.fsync(slot.fileno())
            slot.seek(0)
            self._write_exact(slot, content[:_HEADER.size])
            slot.flush()
            os.fsync(slot.fileno())

    def _decode(self, raw: bytes):
        if len(raw) != SLOT_BYTES:
            raise BudgetCorrupt("short or oversized budget slot")
        (magic, version, mode, flags, generation, lease_count, proof_count,
         root_hash, length, digest, spare) = _HEADER.unpack_from(raw)
        if (magic != _MAGIC or version != _VERSION or flags or spare.strip(b"\x00")
                or mode != int(self._offline_mode) or root_hash != self._root_hash
                or lease_count > MAX_PENDING_LEASES or proof_count > MAX_PROOFS
                or length != _USED_STRUCT.size + lease_count * _LEASE.size
                + proof_count * _PROOF.size):
            raise BudgetCorrupt("budget slot header/identity invalid")
        head = raw[:_HEADER_SHA_OFFSET] + bytes(32) + raw[_HEADER_SHA_OFFSET + 32:_HEADER.size]
        if hashlib.sha256(head + raw[_HEADER.size:]).digest() != digest \
                or any(raw[_HEADER.size + length:]):
            raise BudgetCorrupt("budget slot checksum or padding invalid")
        offset = _HEADER.size
        used = dict(zip(_USED, _USED_STRUCT.unpack_from(raw, offset), strict=True))
        offset += _USED_STRUCT.size
        leases = {}
        last = b""
        for _ in range(lease_count):
            key, *values = _LEASE.unpack_from(raw, offset)
            offset += _LEASE.size
            if key <= last or key == bytes(16):
                raise BudgetCorrupt("duplicate or invalid budget reservation ID")
            last = key
            fields = dict(zip(_LEASE_FIELDS, values, strict=True))
            if fields["metadata"] > fields["body"] or fields["consumed_body"] > fields["body"] \
                    or fields["consumed_metadata"] > fields["metadata"] \
                    or fields["consumed_metadata"] > fields["consumed_body"]:
                raise BudgetCorrupt("invalid charged/pending budget reservation")
            leases[key.hex()] = fields
        proofs = {}
        last = b""
        for _ in range(proof_count):
            key, value = _PROOF.unpack_from(raw, offset)
            offset += _PROOF.size
            if key <= last:
                raise BudgetCorrupt("duplicate conditional proof")
            last = key
            proofs[key.hex()] = value.hex()
        assert offset == _HEADER.size + length
        totals = self._totals(used, leases, physical=False)
        if (any(totals[name] > LIMITS[name] for name in LIMITS)
                or used["metadata"] > used["body"]
                or totals["metadata"] > totals["body"]):
            raise BudgetCorrupt("budget slot counters exceed physical contract")
        return generation, used, leases, proofs

    def _read_pair(self):
        decoded = []
        for path in self.slots:
            if path.is_symlink() or not path.is_file() or path.stat().st_size != SLOT_BYTES:
                raise BudgetCorrupt("missing/short budget slot; do not reset")
            decoded.append(self._decode(path.read_bytes()))
        a, b = decoded
        if a[0] == b[0]:
            if a[0] != 0 or a != b:
                raise BudgetCorrupt("same-generation budget slots disagree")
            return 0, a
        if abs(a[0] - b[0]) != 1:
            raise BudgetCorrupt("nonadjacent budget generations")
        return (0, a) if a[0] > b[0] else (1, b)

    def _commit(self, current_index: int, generation: int,
                used: dict, leases: dict, proofs: dict) -> None:
        if generation == 2**64 - 1:
            raise BudgetExceeded("budget generation exhausted")
        self._write_slot(self.slots[1 - current_index], generation + 1,
                         used, leases, proofs)

    def _totals(self, used: dict, leases: dict, *, physical: bool = True) -> dict[str, int]:
        totals = {name: used.get(name, 0) + sum(lease[name] for lease in leases.values())
                  for name in RESERVED}
        totals["attempts"] = used["attempts"]
        if physical:
            totals["disk"] += _disk_usage(self._physical_root)
        return totals

    def reserve(self, limits: Reservation) -> str:
        lease_id = uuid.uuid4().hex
        with self._locked():
            index, (generation, used, leases, proofs) = self._read_pair()
            if len(leases) >= MAX_PENDING_LEASES:
                raise BudgetExceeded("P4 pending lease cap reached")
            totals = self._totals(used, leases)
            for name in RESERVED:
                totals[name] += getattr(limits, name)
            totals["attempts"] += int(limits.attempt)
            for name, maximum in self.limits.items():
                if totals[name] > maximum:
                    raise BudgetExceeded(f"P4 {name} limit reached")
            # Attempt is charged in the new durable generation BEFORE a socket
            # may be opened. Failed/unknown attempt cannot be refunded.
            used["attempts"] += int(limits.attempt)
            leases[lease_id] = {**{name: getattr(limits, name) for name in RESERVED},
                                "consumed_body": 0, "consumed_metadata": 0}
            self._commit(index, generation, used, leases, proofs)
            return lease_id

    def consume_body(self, lease_id: str, raw_bytes: int, *, metadata: bool = False) -> None:
        if type(raw_bytes) is not int or raw_bytes < 0:
            raise ValueError("raw_bytes must be a nonnegative integer")
        with self._locked():
            index, (generation, used, leases, proofs) = self._read_pair()
            row = leases.get(lease_id)
            if row is None:
                raise BudgetCorrupt("missing or already settled reservation")
            if bool(row["metadata"]) != metadata and raw_bytes:
                raise ValueError("metadata calls must charge raw body and metadata")
            if row["consumed_body"] + raw_bytes > row["body"] or (metadata and
                    row["consumed_metadata"] + raw_bytes > row["metadata"]):
                raise BudgetExceeded("HTTP entity body exceeds reserved quota")
            row["consumed_body"] += raw_bytes
            row["consumed_metadata"] += raw_bytes if metadata else 0
            self._commit(index, generation, used, leases, proofs)

    def settle(self, lease_id: str, *, records: int = 0, saved_samples: int = 0,
               saved_bytes: int = 0) -> None:
        values = {"records": records, "saved_samples": saved_samples,
                  "saved_bytes": saved_bytes}
        if any(type(value) is not int or value < 0 for value in values.values()):
            raise ValueError("settled counts must be nonnegative integers")
        with self._locked():
            index, (generation, used, leases, proofs) = self._read_pair()
            row = leases.get(lease_id)
            if row is None:
                raise BudgetCorrupt("missing or already settled reservation")
            if any(values[name] > row[name] for name in values):
                raise BudgetExceeded("settlement exceeds reservation")
            if self._totals(used, leases)["disk"] > self.limits["disk"]:
                raise BudgetExceeded("physical P4 work root exceeds disk limit")
            for name, value in {**values, "body": row["consumed_body"],
                                "metadata": row["consumed_metadata"]}.items():
                used[name] += value
            del leases[lease_id]
            self._commit(index, generation, used, leases, proofs)

    def condition_proof(self, binding_hash: str) -> str | None:
        if not isinstance(binding_hash, str) or len(binding_hash) != 64:
            raise ValueError("invalid binding proof key")
        with self._locked():
            _index, (_generation, _used, _leases, proofs) = self._read_pair()
            return proofs.get(binding_hash)

    def record_condition_proof(self, binding_hash: str, first_byte_sha: str) -> None:
        if any(not isinstance(value, str) or len(value) != 64
               or any(c not in "0123456789abcdef" for c in value)
               for value in (binding_hash, first_byte_sha)):
            raise ValueError("invalid conditional proof hash")
        with self._locked():
            index, (generation, used, leases, proofs) = self._read_pair()
            if len(proofs) >= MAX_PROOFS and binding_hash not in proofs:
                raise BudgetExceeded("conditional proof count cap")
            if binding_hash in proofs and proofs[binding_hash] != first_byte_sha:
                raise BudgetCorrupt("conditional proof conflicts with stored object")
            proofs[binding_hash] = first_byte_sha
            self._commit(index, generation, used, leases, proofs)

    def status(self) -> dict[str, int]:
        with self._locked():
            _index, (_generation, used, leases, _proofs) = self._read_pair()
            return self._totals(used, leases)
