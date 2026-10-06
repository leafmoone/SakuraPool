"""Crash-conservative finite-slot budget accounting.

Explicit workspaces use versioned identity/policy/layout. Host flush is not a
power-loss promise; unknown or damaged accounting is never reset.
"""

from __future__ import annotations

import hashlib
import os
import stat
import struct
import threading
import time
import traceback
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
from pathlib import Path

from ..capacity import UINT64_MAX, ResourcePolicy


def is_reparse(path: Path) -> bool:
    """No-follow link/junction check, including Python 3.10/3.11."""
    try:
        info = Path(path).lstat()
    except FileNotFoundError:
        return False
    return stat.S_ISLNK(info.st_mode) or bool(
        getattr(info, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    )


DEFAULT_WORK_ROOT = Path("D:/SakuraTool/SakuraPool-P4-work")
MIB = 1 << 20
GIB = 1 << 30
SLOT_BYTES = MIB
SLOT_COUNT = 2
MAX_PENDING_LEASES = 2048
MAX_PROOFS = 2048
LEDGER_HEADROOM = 2 * SLOT_BYTES
LIMITS = {
    "body": 8 * GIB,
    "metadata": 64 * MIB,
    "attempts": 2000,
    "disk": 4 * GIB,
    "records": 100_000,
    "saved_samples": 1000,
    "saved_bytes": 512 * MIB,
    "inflight": 256 * MIB,
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
# None is a mask bit + zero payload, never a substituted enormous quota.
_POLICY_NAMES = tuple(LIMITS)
_POLICY_STRUCT = struct.Struct("<QB8Q")
_WORKSPACE_MAGIC = b"SP6SLOT1"
_WORKSPACE_VERSION = 3
IMPLEMENTATION_BOUNDARIES = {
    "slot_bytes": min(os.sys.maxsize, 128 + (1 << 32) - 1),
    "pending_leases": (1 << 16) - 1,
    "condition_proofs": (1 << 16) - 1,
    "integer": UINT64_MAX,
}
assert _HEADER.size == 128 and _LEASE.size == 88 and _PROOF.size == 64
assert (
    _HEADER.size
    + _POLICY_STRUCT.size
    + _USED_STRUCT.size
    + MAX_PENDING_LEASES * _LEASE.size
    + MAX_PROOFS * _PROOF.size
    < SLOT_BYTES
)


class BudgetExceeded(RuntimeError):
    """A bound was hit before further I/O."""


class BudgetCorrupt(RuntimeError):
    """Unknown, missing, short or inconsistent ledger: never reset."""


def _workspace_operation(method):
    """Keep the domain lock until large operation frames have actually died.

    A contextmanager alone unlocks while its caller's exception frame is live.
    Here the caller is a completed function before unlock; clear every completed
    frame in exception chains (including encode/decode buffers), not just the
    outer traceback. Preserve exception type/message and crash-UNKNOWN state.
    """

    @wraps(method)
    def operation(self, *args, **kwargs):
        if self.workspace is None:
            return method(self, *args, **kwargs)
        with self._locked():
            try:
                return method(self, *args, **kwargs)
            except BaseException as error:
                pending = [error]
                seen = set()
                while pending:
                    item = pending.pop()
                    if id(item) in seen:
                        continue
                    seen.add(id(item))
                    traceback.clear_frames(item.__traceback__)
                    if item.__cause__ is not None:
                        pending.append(item.__cause__)
                    if item.__context__ is not None:
                        pending.append(item.__context__)
                raise

    return operation


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
        func.argtypes = [
            wintypes.LPCWSTR,
            ctypes.POINTER(wintypes.DWORD),
            ctypes.POINTER(wintypes.DWORD),
            ctypes.POINTER(wintypes.DWORD),
            ctypes.POINTER(wintypes.DWORD),
        ]
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
    """Disappearance without persisted ownership evidence blocks admission.

    Names are not ownership proof. Never reuse a partial scan or omit entries.
    This conservative path does not claim concurrent cleanup is fully supported.
    """
    try:
        return _disk_usage_once(root)
    except FileNotFoundError as error:
        raise BudgetCorrupt("entry disappeared during physical admission") from error


def _disk_usage_once(root: Path) -> int:
    """Cluster-rounded files, including unrelated files and repeated hardlinks.

    Links/unsupported entries fail closed. Directory/device amplification is
    not claimed. An unrelated writer can exceed policy; subsequent I/O blocks.
    """
    cluster = _cluster_bytes(root)
    total = 0
    pending = [root]
    while pending:
        directory = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                status = entry.stat(follow_symlinks=False)
                if entry.is_symlink() or (
                    getattr(status, "st_file_attributes", 0)
                    & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
                ):
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
    """Maximum outstanding resources installed before a remote attempt."""

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
    """Legacy P4 constructor, or BudgetLedger(workspace=owned_workspace).

    Workspace policy is read under the domain lock before every operation.
    Direct edits to limits are not authoritative for workspace domains.
    """

    def __init__(
        self,
        root: Path = DEFAULT_WORK_ROOT,
        *,
        _offline_test: bool = False,
        _test_limits: dict[str, int] | None = None,
        workspace=None,
    ):
        self.workspace = workspace
        self.slot_bytes = SLOT_BYTES
        self.max_pending_leases = MAX_PENDING_LEASES
        self.max_proofs = MAX_PROOFS
        if workspace is not None:
            from ..workspace import Workspace

            if not isinstance(workspace, Workspace):
                raise ValueError("typed workspace required")
            if Path(root) != DEFAULT_WORK_ROOT or _offline_test or _test_limits is not None:
                raise ValueError("workspace conflicts with legacy budget arguments")
            workspace.check()
            root = workspace.state
            fixed = workspace.root
            self._policy = workspace.initial_policy
            self._policy_version = 1
            self.limits = self._policy.to_dict()
            self.slot_bytes = workspace.capacity.ledger_slot_bytes
            self.max_pending_leases = workspace.capacity.pending_leases
            self.max_proofs = workspace.capacity.condition_proofs
            required = (
                _HEADER.size
                + _POLICY_STRUCT.size
                + _USED_STRUCT.size
                + self.max_pending_leases * _LEASE.size
                + self.max_proofs * _PROOF.size
            )
            if required > self.slot_bytes:
                raise ValueError("configured ledger layout exceeds slot capacity")
        else:
            root = Path(root).absolute()
            fixed = DEFAULT_WORK_ROOT.absolute()
            if root == fixed and _offline_test:
                raise ValueError("offline test ledger must use a nested synthetic root")
            if root != fixed and (not _offline_test or not root.is_relative_to(fixed)):
                raise ValueError("P4 budget root is fixed; alternate workdir forbidden")
            if _test_limits is not None and not _offline_test:
                raise ValueError("only offline tests may lower budget limits")
            self.limits = dict(LIMITS)
            if _test_limits:
                if any(
                    name not in LIMITS or type(value) is not int or not 0 < value <= LIMITS[name]
                    for name, value in _test_limits.items()
                ):
                    raise ValueError("offline budget limits may only tighten caps")
                self.limits.update(_test_limits)
        for part in (root, *root.parents):
            if is_reparse(part):
                raise BudgetCorrupt("budget root or parent is a symlink/junction")
        if not root.is_dir() or not fixed.is_dir():
            raise BudgetCorrupt("budget root must already be a real directory")
        if os.path.normcase(str(root.resolve())) != os.path.normcase(str(root)):
            raise BudgetCorrupt("budget root resolves to another location")
        self.root = root
        self._physical_root = fixed
        self._offline_mode = bool(_offline_test)
        self._root_hash = (
            workspace.binding_hash
            if workspace is not None
            else hashlib.sha256(os.path.normcase(str(root.resolve())).encode()).digest()
        )
        self.effective_headroom = (
            workspace.capacity.ledger_effective_headroom if workspace is not None else 0
        )
        self.ledger_working_bytes = self.effective_headroom
        self._mutex = threading.RLock()
        self._lock_depth = 0
        prefix = "workspace-budget" if workspace is not None else "p4-budget"
        self.lock_path = root / (prefix + ".lock")
        self.slots = (root / (prefix + ".slot0"), root / (prefix + ".slot1"))
        self.path = self.slots[0]
        for suffix in ("", "-journal", "-wal", "-shm"):
            legacy = root / ("p4-budget.sqlite" + suffix)
            if os.path.lexists(legacy):
                raise BudgetCorrupt("legacy SQLite ledger preserved; migration required")
        created = False
        if not os.path.lexists(self.lock_path):
            if any(os.path.lexists(path) for path in self.slots):
                raise BudgetCorrupt("orphan slot without budget lock")
            if workspace is not None and self.effective_headroom > self.limits["inflight"]:
                raise BudgetExceeded("ledger working memory exceeds inflight policy")
            cluster = _cluster_bytes(fixed)
            allocation = ((self.slot_bytes + cluster - 1) // cluster) * cluster
            if _disk_usage(fixed) + 2 * allocation + cluster > self.limits["disk"]:
                raise BudgetExceeded("P4 disk limit leaves no slot bootstrap room")
            try:
                with self.lock_path.open("xb") as lock:
                    if lock.write(b"L") != 1:
                        raise BudgetCorrupt("short budget lock write")
                    lock.flush()
                    os.fsync(lock.fileno())
                created = True
            except FileExistsError:
                pass  # Follower validates marker/slots; partial bootstrap blocks.
        self._initialize(created, fixed)

    @_workspace_operation
    def _initialize(self, created, fixed):
        with self._locked() as _lock:
            if created:
                if any(os.path.lexists(path) for path in self.slots):
                    raise BudgetCorrupt("unknown slot during ledger bootstrap")
                cluster = _cluster_bytes(fixed)
                allocation = ((self.slot_bytes + cluster - 1) // cluster) * cluster
                if _disk_usage(fixed) + 2 * allocation > self.limits["disk"]:
                    raise BudgetExceeded("P4 disk limit leaves no slot allocation room")
                zero = ({name: 0 for name in _USED}, {}, {})
                for path in self.slots:
                    with path.open("xb") as slot:
                        remaining = self.slot_bytes
                        block = bytes(min(MIB, remaining))
                        while remaining:
                            chunk = block[: min(len(block), remaining)]
                            self._write_exact(slot, chunk)
                            remaining -= len(chunk)
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
            if self.workspace is not None and self._lock_depth:
                yield None
                return
            if self.workspace is not None:
                self.workspace.check()
            if is_reparse(self.lock_path) or not self.lock_path.is_file():
                raise BudgetCorrupt("budget lock linked or missing")
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
                        self._lock_depth += 1
                        yield lock
                    finally:
                        self._lock_depth = 0
                        lock.seek(0)
                        msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
                    try:
                        lock.seek(0)
                        if lock.read(1) != b"L":
                            raise BudgetCorrupt("unknown budget lock marker")
                        self._lock_depth += 1
                        yield lock
                    finally:
                        self._lock_depth = 0
                        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def _encode(self, generation: int, used: dict, leases: dict, proofs: dict) -> bytes:
        if (
            not 0 <= generation < 2**64
            or len(leases) > self.max_pending_leases
            or len(proofs) > self.max_proofs
        ):
            raise BudgetCorrupt("slot record count or generation exceeds format")
        payload = bytearray()
        if self.workspace is not None:
            values = [self.limits[name] for name in _POLICY_NAMES]
            mask = sum(1 << index for index, value in enumerate(values) if value is None)
            payload.extend(
                _POLICY_STRUCT.pack(
                    self._policy_version, mask, *(0 if value is None else value for value in values)
                )
            )
        payload.extend(_USED_STRUCT.pack(*(used[name] for name in _USED)))
        for key, fields in sorted(leases.items()):
            payload.extend(
                _LEASE.pack(bytes.fromhex(key), *(fields[name] for name in _LEASE_FIELDS))
            )
        for key, value in sorted(proofs.items()):
            payload.extend(_PROOF.pack(bytes.fromhex(key), bytes.fromhex(value)))
        if _HEADER.size + len(payload) > self.slot_bytes:
            raise BudgetExceeded("budget slot full")
        try:
            padded = bytes(payload) + bytes(self.slot_bytes - _HEADER.size - len(payload))
            magic = _WORKSPACE_MAGIC if self.workspace is not None else _MAGIC
            version = _WORKSPACE_VERSION if self.workspace is not None else _VERSION
            header = _HEADER.pack(
                magic,
                version,
                int(self._offline_mode),
                0,
                generation,
                len(leases),
                len(proofs),
                self._root_hash,
                len(payload),
                bytes(32),
                bytes(36),
            )
            digest = hashlib.sha256(header + padded).digest()
            header = header[:_HEADER_SHA_OFFSET] + digest + header[_HEADER_SHA_OFFSET + 32 :]
            return header + padded
        except MemoryError as error:
            raise BudgetExceeded("insufficient memory for configured ledger slot") from error

    def _write_slot(
        self, path: Path, generation: int, used: dict, leases: dict, proofs: dict
    ) -> None:
        content = self._encode(generation, used, leases, proofs)
        if is_reparse(path) or not path.is_file() or path.stat().st_size != self.slot_bytes:
            raise BudgetCorrupt("budget slot missing, linked or incorrectly sized")
        try:
            with path.open("r+b") as slot:
                # Invalid standby blocks reopening; never fall back and refund.
                slot.seek(_HEADER.size)
                self._write_exact(slot, content[_HEADER.size :])
                slot.flush()
                os.fsync(slot.fileno())
                slot.seek(0)
                self._write_exact(slot, content[: _HEADER.size])
                slot.flush()
                os.fsync(slot.fileno())
        except OSError as error:
            raise BudgetCorrupt("ledger write failed; partial accounting must not reset") from error

    def _decode(self, raw: bytes):
        if len(raw) != self.slot_bytes:
            raise BudgetCorrupt("short or oversized budget slot")
        (
            magic,
            version,
            mode,
            flags,
            generation,
            lease_count,
            proof_count,
            root_hash,
            length,
            digest,
            spare,
        ) = _HEADER.unpack_from(raw)
        prefix_size = _POLICY_STRUCT.size if self.workspace is not None else 0
        expected_magic = _WORKSPACE_MAGIC if self.workspace is not None else _MAGIC
        expected_version = _WORKSPACE_VERSION if self.workspace is not None else _VERSION
        if (
            magic != expected_magic
            or version != expected_version
            or flags
            or spare.strip(b"\x00")
            or mode != int(self._offline_mode)
            or root_hash != self._root_hash
            or lease_count > self.max_pending_leases
            or proof_count > self.max_proofs
            or length
            != prefix_size
            + _USED_STRUCT.size
            + lease_count * _LEASE.size
            + proof_count * _PROOF.size
            or _HEADER.size + length > len(raw)
        ):
            raise BudgetCorrupt("budget slot header/identity invalid")
        head = raw[:_HEADER_SHA_OFFSET] + bytes(32) + raw[_HEADER_SHA_OFFSET + 32 : _HEADER.size]
        if hashlib.sha256(head + raw[_HEADER.size :]).digest() != digest or any(
            raw[_HEADER.size + length :]
        ):
            raise BudgetCorrupt("budget slot checksum or padding invalid")
        offset = _HEADER.size
        policy_state = None
        if self.workspace is not None:
            policy_version, mask, *values = _POLICY_STRUCT.unpack_from(raw, offset)
            offset += prefix_size
            if policy_version == 0 or any(
                mask & (1 << i) and value != 0 for i, value in enumerate(values)
            ):
                raise BudgetCorrupt("invalid policy encoding")
            try:
                policy = ResourcePolicy.from_dict(
                    {
                        name: None if mask & (1 << i) else values[i]
                        for i, name in enumerate(_POLICY_NAMES)
                    }
                )
            except ValueError as error:
                raise BudgetCorrupt("invalid persisted resource policy") from error
            policy_state = (policy_version, policy)
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
            if (
                fields["metadata"] > fields["body"]
                or fields["consumed_body"] > fields["body"]
                or fields["consumed_metadata"] > fields["metadata"]
                or fields["consumed_metadata"] > fields["consumed_body"]
            ):
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
        if self.workspace is None:
            invalid = any(totals[name] > LIMITS[name] for name in LIMITS)
        else:
            invalid = any(value > UINT64_MAX for value in totals.values())
            try:
                policy_state[1].admit(totals)
            except ValueError:
                invalid = True
        if invalid or used["metadata"] > used["body"] or totals["metadata"] > totals["body"]:
            raise BudgetCorrupt("budget slot counters exceed physical contract")
        result = (generation, used, leases, proofs)
        return (*result, policy_state) if self.workspace is not None else result

    def _preflight_memory(self):
        """Authenticate both slots in constant scratch space before whole reads.

        The current slot alone determines active pending and policy; charging the
        standby too would double-spend leases. Never select a valid fallback.
        Hash every byte, including padding, so corruption cannot grant headroom.
        """
        states = []
        for path in self.slots:
            if is_reparse(path) or not path.is_file() or path.stat().st_size != self.slot_bytes:
                raise BudgetCorrupt("missing/short budget slot; do not reset")
            with path.open("rb") as stream:
                header = stream.read(_HEADER.size)
                (
                    magic,
                    version,
                    mode,
                    flags,
                    generation,
                    count,
                    proofs,
                    root_hash,
                    length,
                    digest,
                    spare,
                ) = _HEADER.unpack(header)
                if (
                    magic != _WORKSPACE_MAGIC
                    or version != _WORKSPACE_VERSION
                    or mode != int(self._offline_mode)
                    or flags
                    or spare.strip(b"\x00")
                    or root_hash != self._root_hash
                    or count > self.max_pending_leases
                    or proofs > self.max_proofs
                    or length
                    != _POLICY_STRUCT.size
                    + _USED_STRUCT.size
                    + count * _LEASE.size
                    + proofs * _PROOF.size
                    or _HEADER.size + length > self.slot_bytes
                ):
                    raise BudgetCorrupt("budget slot header/identity invalid")
                hasher = hashlib.sha256(
                    header[:_HEADER_SHA_OFFSET] + bytes(32) + header[_HEADER_SHA_OFFSET + 32 :]
                )
                prefix = stream.read(_POLICY_STRUCT.size + _USED_STRUCT.size)
                hasher.update(prefix)
                policy_version, mask, *values = _POLICY_STRUCT.unpack_from(prefix)
                if (
                    policy_version == 0
                    or mask >> len(_POLICY_NAMES)
                    or any(mask & (1 << i) and value for i, value in enumerate(values))
                ):
                    raise BudgetCorrupt("invalid policy encoding")
                try:
                    policy = ResourcePolicy.from_dict(
                        {
                            name: None if mask & (1 << i) else values[i]
                            for i, name in enumerate(_POLICY_NAMES)
                        }
                    )
                except ValueError as error:
                    raise BudgetCorrupt("invalid persisted resource policy") from error
                active = 0
                for _ in range(count):
                    row = stream.read(_LEASE.size)
                    if len(row) != _LEASE.size:
                        raise BudgetCorrupt("short budget slot")
                    hasher.update(row)
                    active += _LEASE.unpack(row)[7]
                remaining = self.slot_bytes - stream.tell()
                while remaining:
                    block = stream.read(min(65536, remaining))
                    if not block:
                        raise BudgetCorrupt("short budget slot")
                    hasher.update(block)
                    remaining -= len(block)
                if hasher.digest() != digest:
                    raise BudgetCorrupt("budget slot checksum or padding invalid")
                states.append((generation, policy, active, digest))
        a, b = states
        if a[0] == b[0]:
            if a[0] != 0 or a != b:
                raise BudgetCorrupt("same-generation budget slots disagree")
            current = a
        else:
            if abs(a[0] - b[0]) != 1:
                raise BudgetCorrupt("nonadjacent budget generations")
            current = max(states, key=lambda state: state[0])
        if current[2] + self.effective_headroom > current[1].inflight:
            raise BudgetExceeded(
                "ledger working memory plus active pending exceeds inflight policy"
            )

    def _read_pair(self):
        if self.workspace is not None:
            self._preflight_memory()
        decoded = []
        for path in self.slots:
            if is_reparse(path) or not path.is_file() or path.stat().st_size != self.slot_bytes:
                raise BudgetCorrupt("missing/short budget slot; do not reset")
            try:
                decoded.append(self._decode(path.read_bytes()))
            except MemoryError as error:
                raise BudgetExceeded("insufficient memory for configured ledger slot") from error
        a, b = decoded
        if a[0] == b[0]:
            if a[0] != 0 or a != b:
                raise BudgetCorrupt("same-generation budget slots disagree")
            index, current = 0, a
        else:
            if abs(a[0] - b[0]) != 1:
                raise BudgetCorrupt("nonadjacent budget generations")
            index, current = (0, a) if a[0] > b[0] else (1, b)
        if self.workspace is not None:
            self._policy_version, self._policy = current[4]
            self.limits = self._policy.to_dict()
            current = current[:4]
        return index, current

    def _commit(
        self, current_index: int, generation: int, used: dict, leases: dict, proofs: dict
    ) -> None:
        if generation == 2**64 - 1:
            raise BudgetExceeded("budget generation exhausted")
        self._write_slot(self.slots[1 - current_index], generation + 1, used, leases, proofs)

    def _totals(self, used: dict, leases: dict, *, physical: bool = True) -> dict[str, int]:
        totals = {
            name: used.get(name, 0) + sum(lease[name] for lease in leases.values())
            for name in RESERVED
        }
        totals["attempts"] = used["attempts"]
        if physical:
            totals["disk"] += _disk_usage(self._physical_root)
        return totals

    @_workspace_operation
    def _admit_quiescent_disk(self, disk_bytes):
        index, (generation, used, leases, proofs) = self._read_pair()
        if leases or self._totals(used, leases)["inflight"]:
            raise BudgetExceeded("ACCOUNTING_BUSY")
        if self._totals(used, leases)["disk"] + disk_bytes > self.limits["disk"]:
            raise BudgetExceeded("disk limit reached")
        lease = uuid.uuid4().hex
        leases[lease] = {**{name: disk_bytes if name == "disk" else 0 for name in RESERVED},
                         "consumed_body": 0, "consumed_metadata": 0}
        self._commit(index, generation, used, leases, proofs)
        return lease

    @_workspace_operation
    def _finish_quiescent_disk(self, lease, disk_bytes):
        index, (generation, used, leases, proofs) = self._read_pair()
        row = leases.get(lease)
        if row is None or row["disk"] != disk_bytes or len(leases) != 1:
            raise BudgetCorrupt("owned metadata reservation changed")
        if self._totals(used, leases)["disk"] > self.limits["disk"]:
            raise BudgetExceeded("metadata settlement disk limit reached")
        del leases[lease]
        self._commit(index, generation, used, leases, proofs)

    @contextmanager
    def quiescent_disk_operation(self, disk_bytes):
        """Hold ledger exclusion and admit one owned disk-only metadata operation."""
        if type(disk_bytes) is not int or disk_bytes <= 0:
            raise ValueError("positive bounded disk admission required")
        with self._locked():
            lease = self._admit_quiescent_disk(disk_bytes)
            try:
                yield lease
            finally:
                import sys

                primary = sys.exc_info()[1]
                try:
                    self._finish_quiescent_disk(lease, disk_bytes)
                except BaseException:
                    if primary is None:
                        raise BudgetCorrupt("metadata reservation settlement unknown") from None
                    primary.task_secondary = (
                        *getattr(primary, "task_secondary", ()),
                        "TASK_RESOURCE_SETTLEMENT_UNKNOWN",
                    )

    @_workspace_operation
    def reserve(self, limits: Reservation) -> str:
        lease_id = uuid.uuid4().hex
        with self._locked():
            index, (generation, used, leases, proofs) = self._read_pair()
            if len(leases) >= self.max_pending_leases:
                message = (
                    "pending lease capacity reached"
                    if self.workspace is not None
                    else "P4 pending lease cap reached (implementation boundary)"
                )
                raise BudgetExceeded(message)
            totals = self._totals(used, leases)
            totals["inflight"] += self.ledger_working_bytes
            for name in RESERVED:
                totals[name] += getattr(limits, name)
            totals["attempts"] += int(limits.attempt)
            for name, maximum in self.limits.items():
                if self.workspace is not None and totals[name] > UINT64_MAX:
                    raise BudgetExceeded(f"{name} integer implementation boundary exceeded")
                if maximum is not None and totals[name] > maximum:
                    raise BudgetExceeded(f"P4 {name} limit reached")
            used["attempts"] += int(limits.attempt)  # Durable before any socket.
            leases[lease_id] = {
                **{name: getattr(limits, name) for name in RESERVED},
                "consumed_body": 0,
                "consumed_metadata": 0,
            }
            self._commit(index, generation, used, leases, proofs)
            return lease_id

    @_workspace_operation
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
            if row["consumed_body"] + raw_bytes > row["body"] or (
                metadata and row["consumed_metadata"] + raw_bytes > row["metadata"]
            ):
                raise BudgetExceeded("HTTP entity body exceeds reserved quota")
            row["consumed_body"] += raw_bytes
            row["consumed_metadata"] += raw_bytes if metadata else 0
            self._commit(index, generation, used, leases, proofs)

    @_workspace_operation
    def settle(
        self, lease_id: str, *, records: int = 0, saved_samples: int = 0, saved_bytes: int = 0
    ) -> None:
        values = {"records": records, "saved_samples": saved_samples, "saved_bytes": saved_bytes}
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
            for name, value in {
                **values,
                "body": row["consumed_body"],
                "metadata": row["consumed_metadata"],
            }.items():
                used[name] += value
            del leases[lease_id]
            self._commit(index, generation, used, leases, proofs)

    @_workspace_operation
    def condition_proof(self, binding_hash: str) -> str | None:
        if not isinstance(binding_hash, str) or len(binding_hash) != 64:
            raise ValueError("invalid binding proof key")
        with self._locked():
            _index, (_generation, _used, _leases, proofs) = self._read_pair()
            return proofs.get(binding_hash)

    @_workspace_operation
    def record_condition_proof(self, binding_hash: str, first_byte_sha: str) -> None:
        if any(
            not isinstance(value, str)
            or len(value) != 64
            or any(c not in "0123456789abcdef" for c in value)
            for value in (binding_hash, first_byte_sha)
        ):
            raise ValueError("invalid conditional proof hash")
        with self._locked():
            index, (generation, used, leases, proofs) = self._read_pair()
            if len(proofs) >= self.max_proofs and binding_hash not in proofs:
                message = (
                    "conditional proof capacity reached"
                    if self.workspace is not None
                    else "conditional proof count cap (implementation boundary)"
                )
                raise BudgetExceeded(message)
            if binding_hash in proofs and proofs[binding_hash] != first_byte_sha:
                raise BudgetCorrupt("conditional proof conflicts with stored object")
            proofs[binding_hash] = first_byte_sha
            self._commit(index, generation, used, leases, proofs)

    @_workspace_operation
    def status(self) -> dict[str, int]:
        with self._locked():
            _index, (_generation, used, leases, _proofs) = self._read_pair()
            return self._totals(used, leases)

    @property
    @_workspace_operation
    def policy(self):
        if self.workspace is None:
            return ResourcePolicy(**self.limits)
        with self._locked():
            self._read_pair()
            return self._policy

    @property
    @_workspace_operation
    def policy_version(self):
        if self.workspace is None:
            return 0
        with self._locked():
            self._read_pair()
            return self._policy_version

    @_workspace_operation
    def inspect_policy(self):
        with self._locked():
            _index, (_generation, used, leases, _proofs) = self._read_pair()
            usage = self._totals(used, leases)
            return {
                "policy": dict(self.limits),
                "policy_version": self._policy_version,
                "usage": usage,
                "pending_count": len(leases),
                "ledger_version": _WORKSPACE_VERSION,
                "effective_headroom": self.effective_headroom,
                "worker_available_inflight": self.limits["inflight"]
                - self.effective_headroom
                - usage["inflight"],
                "ledger_memory_model": self.workspace.capacity.ledger_memory_model,
                "ledger_capacity": {
                    "slot_bytes": self.slot_bytes,
                    "pending_leases": self.max_pending_leases,
                    "condition_proofs": self.max_proofs,
                },
                "implementation_boundaries": dict(IMPLEMENTATION_BOUNDARIES),
            }

    @_workspace_operation
    def update_policy(self, policy, *, expected_version=None):
        if self.workspace is None:
            raise ValueError("P4 legacy policy is immutable; no migration or refund")
        if not isinstance(policy, ResourcePolicy):
            raise ValueError("typed resource policy required")
        if expected_version is not None and (
            type(expected_version) is not int or not 1 <= expected_version <= UINT64_MAX
        ):
            raise ValueError("invalid expected policy version")
        with self._locked():
            index, (generation, used, leases, proofs) = self._read_pair()
            if policy.inflight < self.ledger_working_bytes:
                raise BudgetExceeded("ledger working memory exceeds inflight policy")
            if expected_version is not None and expected_version != self._policy_version:
                raise ValueError("resource policy version conflict")
            if self._policy_version == UINT64_MAX:
                raise BudgetExceeded("policy version implementation boundary exhausted")
            try:
                totals = self._totals(used, leases)
                totals["inflight"] += self.ledger_working_bytes
                policy.admit(totals)
            except ValueError as error:
                raise BudgetExceeded(str(error)) from error
            self._policy = policy
            self.limits = policy.to_dict()
            self._policy_version += 1
            self._commit(index, generation, used, leases, proofs)
            return self._policy_version
