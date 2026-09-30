"""Stable local build manifests; runtime paths are not durable identity."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .records import canonical_object_id

FORMAT = "sakurapool-build-partition-v1"
OBJECTS_PER_PARTITION = 32


@dataclass(frozen=True)
class PartitionObject:
    repository_path: str
    local_path: Path
    declared_size: int
    provider_sha256: str | None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> PartitionObject:
        if not isinstance(value, dict) or set(value) != {
                "repository_path", "local_path", "declared_size", "provider_sha256"}:
            raise ValueError("invalid partition object keys")
        rel = value["repository_path"]
        canonical_object_id(rel)
        if not rel.endswith(".tar"):
            raise ValueError("partition object must be a TAR")
        local = value["local_path"]
        if not isinstance(local, str) or not Path(local).is_absolute():
            raise ValueError("local_path must be absolute")
        size = value["declared_size"]
        if type(size) is not int or size <= 0:
            raise ValueError("declared_size must be a positive integer")
        digest = value["provider_sha256"]
        if digest is not None and (not isinstance(digest, str) or
                                   re.fullmatch(r"[0-9a-f]{64}", digest) is None):
            raise ValueError("provider_sha256 must be lowercase SHA256 or null/UNKNOWN")
        return cls(rel, Path(local), size, digest)

    def to_dict(self) -> dict[str, Any]:
        return {"repository_path": self.repository_path,
                "local_path": str(self.local_path), "declared_size": self.declared_size,
                "provider_sha256": self.provider_sha256}


@dataclass(frozen=True)
class PartitionManifest:
    repository: str
    dataset: str
    source: str
    partition: str
    objects: tuple[PartitionObject, ...]

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> PartitionManifest:
        if not isinstance(value, dict) or set(value) != {
                "format", "repository", "dataset", "source", "partition", "objects"}:
            raise ValueError("invalid manifest keys")
        if value["format"] != FORMAT:
            raise ValueError("unsupported partition format")
        for name in ("repository", "dataset", "source"):
            canonical_object_id(value[name])
        if not isinstance(value["partition"], str) or not re.fullmatch(
                r"part-[0-9]{6}", value["partition"]):
            raise ValueError("invalid partition name")
        if not isinstance(value["objects"], list) or not 1 <= len(value["objects"]) <= 32:
            raise ValueError("partition must contain 1..32 objects")
        objects = tuple(PartitionObject.from_dict(obj) for obj in value["objects"])
        paths = [obj.repository_path for obj in objects]
        if paths != sorted(paths) or len(paths) != len(set(paths)):
            raise ValueError("objects must have unique sorted repository paths")
        return cls(value["repository"], value["dataset"], value["source"],
                   value["partition"], objects)

    def to_dict(self) -> dict[str, Any]:
        return {"format": FORMAT, "repository": self.repository, "dataset": self.dataset,
                "source": self.source, "partition": self.partition,
                "objects": [obj.to_dict() for obj in self.objects]}

    def durable_plan(self) -> dict[str, Any]:
        """Fixed recovery plan, intentionally excluding every local_path."""
        result = self.to_dict()
        for obj in result["objects"]:
            del obj["local_path"]
        return result


def partition_objects(repository: str, dataset: str, source: str,
                      objects: list[dict[str, Any]]) -> list[PartitionManifest]:
    """Stable sorted per-dataset 32-object chunks; never deduplicate inputs."""
    parsed = [PartitionObject.from_dict(obj) for obj in objects]
    parsed.sort(key=lambda obj: obj.repository_path)
    paths = [obj.repository_path for obj in parsed]
    if len(paths) != len(set(paths)):
        raise ValueError("one TAR must belong to exactly one partition")
    return [PartitionManifest.from_dict({
        "format": FORMAT, "repository": repository, "dataset": dataset, "source": source,
        "partition": f"part-{i // OBJECTS_PER_PARTITION:06d}",
        "objects": [obj.to_dict() for obj in parsed[i:i + OBJECTS_PER_PARTITION]],
    }) for i in range(0, len(parsed), OBJECTS_PER_PARTITION)]
