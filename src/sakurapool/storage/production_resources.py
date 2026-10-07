"""Simultaneous-live production admission ceilings, not throughput estimates.

Production private SQLite uses journal_mode=OFF; failures never publish success.
"""

from dataclasses import dataclass

from ..capacity import UINT64_MAX, CapacityConfig

CONTROL_RESPONSE_BODY_CAP = 65_536  # Explicit worker protocol value; Rust parity is tested.
NEGATIVE_CONDITION_BODY_CAP = CONTROL_RESPONSE_BODY_CAP
ORIGIN_STATUS_MAX_ATTEMPTS = 3
HTTP_ATTEMPTS_MAX = ORIGIN_STATUS_MAX_ATTEMPTS + 1
LEDGER_ATTEMPTS_PER_TRANSFER = HTTP_ATTEMPTS_MAX
# Worker dispatch commits once for request and once for operation, not per HTTP send.
SESSION_ATTEMPTS_PER_TRANSFER = 2


def checked_body_add(*values):
    total = 0
    for value in values:
        if type(value) is not int or not 0 <= value <= UINT64_MAX:
            raise ValueError("production body budget integer invalid")
        total += value
        if total > UINT64_MAX:
            raise ValueError("production body budget overflow")
    return total


def checked_body_mul(value, count):
    checked_body_add(value)
    checked_body_add(count)
    result = value * count
    if result > UINT64_MAX:
        raise ValueError("production body budget overflow")
    return result


def chunked_body_budget(image_size, metadata_size, chunk):
    checked_body_add(chunk)
    if chunk == 0:
        raise ValueError("chunk must be positive")
    total = 0
    for size in (image_size, metadata_size):
        checked_body_add(size)
        full, remainder = divmod(size, chunk)
        if full:
            total = checked_body_add(total, checked_body_mul(network_body_budget(chunk), full))
        if remainder:
            total = checked_body_add(total, network_body_budget(remainder))
    return total


def network_body_budget(size, *, condition="match"):
    origin = checked_body_add(CONTROL_RESPONSE_BODY_CAP, 1)
    business = origin if condition == "wrong" else checked_body_add(size, 1)
    return checked_body_add(
        checked_body_mul(origin, ORIGIN_STATUS_MAX_ATTEMPTS), max(business, origin)
    )


MAX_RANGE = 8 << 20
RECORD_CAP = 32 << 20
METADATA_CAP = 32 << 20
FOOTER_CAP = 4096
LINE_CAP = 32 << 10
PATH_CAP = 4096
JSON_CAP = 1 << 20
MAX_MEMBERS = 100_000
FORMAT = "sakurapool-production-scan-v1"
STAGE_MAIN_CAP = 128 << 20
STAGE_DISK_CAP = STAGE_MAIN_CAP + (1 << 20)
STAGE_PAGES = STAGE_MAIN_CAP // 4096
STREAM_MEMORY = 128 << 20
ALLOCATION_OVERHEAD = 16 << 10
PROTOCOL_BOOTSTRAP_BYTES = 64 << 10
PROTOCOL_MAX_DEPTH = 64
PROTOCOL_MAX_NODES = 65_536
PROTOCOL_NODE_BYTES = 256
METADATA_BODY_CAP = 1 << 20
METADATA_RESPONSE_LINE_BYTES = 2 << 20
METADATA_RESPONSE_NODES = 128
# Pinned hyper HTTP/1 parser default: 8192 + 4096 * 100. reqwest has no knob.
HTTP_PARSER_BYTES = 417_792
DURABLE_SPOOL_LIMITS = {
    "spool_pages": (60 << 20) // 4096,
    "row_bytes": 512 << 10,
    "batch_bytes": 256 << 10,
    "row_groups": 512,
}


def protocolmemory(capacity=None):
    """Shared Python/Rust live protocol/header allocation contract (not RSS).

    Five raw line extents cover producer pending, queue(1), consumer and
    bytearray growth/copy slack. UTF-8 decode and decoded strings each charge
    four bytes/source byte. Keys, values and container slots charge 256 bytes
    per node, including sparse Python dict/list capacity and Rust typed copies.
    Two hops charge parser growth, header values and 128 HeaderMap slots each.
    Actual hyper parser allocation is charged even for tiny semantic limits.
    Fixed pipe chunks, stderr and control charge 1 MiB. Legacy scan manifests
    have their separate response contract, not this production-sidecar model.
    """
    capacity = CapacityConfig() if capacity is None else capacity
    if not isinstance(capacity, CapacityConfig):
        raise ValueError("typed capacity required")
    line = max(PROTOCOL_BOOTSTRAP_BYTES, capacity.rpc_line_bytes)
    memory = (1 << 20) + 13 * line + PROTOCOL_NODE_BYTES * min(PROTOCOL_MAX_NODES, line)
    memory += 2 * (4 * max(HTTP_PARSER_BYTES, capacity.http_header_bytes) + 128 * 256)
    if memory > UINT64_MAX:
        raise ValueError("production memory integer boundary")
    return memory


def metadata_resident_memory(capacity=None):
    """Dedicated worker/client residency, mirrored by Rust metadata.rs."""
    return protocolmemory(capacity) + (32 << 20)


def metadata_response_memory(body, capacity=None):
    """Transport copies only; excludes later provider JSON graphs/caller retention."""
    capacity = CapacityConfig() if capacity is None else capacity
    if type(body) is not int or not 0 <= body <= METADATA_BODY_CAP:
        raise ValueError("metadata response bound invalid")
    if not isinstance(capacity, CapacityConfig):
        raise ValueError("typed capacity required")
    return (13 * METADATA_RESPONSE_LINE_BYTES + 6 * (body + 1)
            + 16 * capacity.http_header_bytes + 256 * METADATA_RESPONSE_NODES + 65_536)


@dataclass(frozen=True)
class ProductionFootprint:
    mode: str
    object_bytes: int
    memory: int
    artifacts: int

    @classmethod
    def admit(cls, mode: str, object_bytes: int, capacity=None):
        if type(object_bytes) is not int or not 0 < object_bytes <= UINT64_MAX:
            raise ValueError("production extent invalid")
        capacity = capacity if capacity is not None else CapacityConfig()
        protocol = protocolmemory(capacity)
        if mode == "range":
            if object_bytes > capacity.range_chunk_bytes:
                raise ValueError("production Range exceeds configured chunk capacity")
            memory = (32 << 20) + 2 * object_bytes + protocol
            if memory > UINT64_MAX:
                raise ValueError("production memory integer boundary")
            return cls(mode, object_bytes, memory, object_bytes)
        if mode not in ("download-then-scan", "remote-stream-scan"):
            raise ValueError("production mode invalid")
        artifacts = RECORD_CAP + FOOTER_CAP + METADATA_CAP
        if mode == "download-then-scan":
            artifacts += object_bytes
        if artifacts + ALLOCATION_OVERHEAD > UINT64_MAX:
            raise ValueError("production artifact integer boundary")
        memory = STREAM_MEMORY + protocol
        if memory > UINT64_MAX:
            raise ValueError("production memory integer boundary")
        return cls(mode, object_bytes, memory, artifacts)

    @property
    def transfer_disk(self):
        return self.artifacts + ALLOCATION_OVERHEAD

    def admin_phases(self, durable_allowance: int):
        if self.mode == "range":
            raise ValueError("Range is not a builder phase")
        transfer = self.transfer_disk
        stage = transfer + STAGE_DISK_CAP
        retained = self.transfer_disk if self.mode == "download-then-scan" else 0
        durable = retained + STAGE_MAIN_CAP + durable_allowance + ALLOCATION_OVERHEAD
        return {"transfer": transfer, "stage": stage, "durable": durable}

    def admin_peak(self, durable_allowance: int):
        return max(self.admin_phases(durable_allowance).values())
