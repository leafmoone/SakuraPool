"""Hard production limits and lifecycle footprints; no TAR-size RAM multiplier.

These are simultaneous-live admission ceilings, not a throughput guarantee.
Production uses fresh disposable pre-publication SQLite files with journal_mode=OFF:
no rollback/WAL/subjournal, no estimated VFS-sector bound. SQL/IO/crash failures can
leave corrupt private databases, never a successful completion marker/COMMIT.
Existing completed artifacts are never updated by this mode; local keeps DELETE.
The all-member uniqueness table shares this capped database, then is dropped.
"""

from dataclasses import dataclass

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
# Each P2 private spool<=60MiB, no journal. Shared fragment cap is the existing192MiB,
# audit64MiB: 2*60+192+64=376MiB before allocation overhead within1152MiB allowance.
# Stage and all Download transfer artifacts remain separately live.
DURABLE_SPOOL_LIMITS = {
    "spool_pages": (60 << 20) // 4096,
    "row_bytes": 512 << 10,
    "batch_bytes": 256 << 10,
    "row_groups": 512,
}


@dataclass(frozen=True)
class ProductionFootprint:
    mode: str
    object_bytes: int
    memory: int
    artifacts: int

    @classmethod
    def admit(cls, mode: str, object_bytes: int):
        if type(object_bytes) is not int or not 0 < object_bytes < 2**64:
            raise ValueError("production extent invalid")
        if mode == "range":
            if object_bytes > MAX_RANGE:
                raise ValueError("production Range exceeds 8MiB")
            # One returned payload and one bounded consumer copy; Rust uses only 64KiB.
            return cls(mode, object_bytes, (32 << 20) + 2 * object_bytes, object_bytes)
        if mode not in ("download-then-scan", "remote-stream-scan"):
            raise ValueError("production mode invalid")
        artifacts = RECORD_CAP + FOOTER_CAP + METADATA_CAP
        if mode == "download-then-scan":
            artifacts += object_bytes
        return cls(mode, object_bytes, STREAM_MEMORY, artifacts)

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
