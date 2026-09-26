"""Versioned physical-record identities and byte references (no image decoding)."""

import hashlib
import json
from dataclasses import dataclass
from pathlib import PurePosixPath


def canonical_object_id(value: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ValueError("object_id must be a relative POSIX path")
    if value.startswith("/") or any(p in ("", ".", "..") or ":" in p for p in value.split("/")):
        raise ValueError("object_id must be a canonical relative POSIX path")
    return str(PurePosixPath(value))


@dataclass(frozen=True)
class RecordKey:
    dataset_id: str
    object_id: str
    sample_path: str

    def __post_init__(self) -> None:
        if not isinstance(self.dataset_id, str) or not self.dataset_id:
            raise ValueError("dataset_id is required")
        canonical_object_id(self.object_id)
        canonical_object_id(self.sample_path)

    @property
    def record_id(self) -> str:
        payload = json.dumps(
            ["sakurapool-record-v1", self.dataset_id, self.object_id, self.sample_path],
            ensure_ascii=False, separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.blake2b(payload, digest_size=16).hexdigest()


@dataclass(frozen=True)
class ObjectRef:
    dataset_id: str
    object_id: str
    path: str
    validator_sha256: str
    object_size: int
    backend: str = "local"
    repo_type: str = "tar"
    object_version: str = ""
    validator_kind: str = "sha256"
    validator_strength: str = "strong:sha256"

    def __post_init__(self) -> None:
        canonical_object_id(self.object_id)
        canonical_object_id(self.path)
        if len(self.validator_sha256) != 64:
            raise ValueError("strong SHA256 validator required")
        if type(self.object_size) is not int or not 0 <= self.object_size < 2**64:
            raise ValueError("object_size must be uint64")
        if not self.backend or not self.repo_type or not self.validator_kind:
            raise ValueError("ObjectRef storage metadata is required")
        if not self.object_version or not self.validator_strength.startswith("strong:"):
            raise ValueError("strong ObjectRef version and validator are required")
        if self.object_version != self.validator_sha256:
            raise ValueError("ObjectRef version must match validator")


@dataclass(frozen=True)
class MemberRef:
    object: ObjectRef
    path: str
    offset_data: int
    size: int

    def __post_init__(self) -> None:
        canonical_object_id(self.path)
        if type(self.offset_data) is not int or type(self.size) is not int:
            raise ValueError("member extents must be uint64")
        if not 0 <= self.offset_data < 2**64 or not 0 <= self.size < 2**64:
            raise ValueError("member extents must be uint64")
        if self.offset_data + self.size > self.object.object_size:
            raise ValueError("member extent exceeds object_size")


class IdentityConflictError(ValueError):
    """A digest must never overwrite a different retained physical key."""


def register_identity(seen: dict[str, RecordKey], key: RecordKey) -> str:
    record_id = key.record_id
    if record_id in seen:
        raise IdentityConflictError(
            f"record_identity_conflict: {record_id}: {seen[record_id]!r} versus {key!r}"
        )
    seen[record_id] = key
    return record_id
