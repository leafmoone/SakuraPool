"""Synthetic committed P2 v4 directories for P3 tests and benchmarks.

Generates P2-contract-correct fragment Parquet + COMMIT markers +
INPUT.json without any real images, so tag distribution, namespaces,
origins and row order are fully controllable.

Contract fidelity (review requirement): the synthetic directory must be a
model the real P2 contract accepts, not a P3-only fixture:
- record_id is the real RecordKey BLAKE2b-16 derivation
  ("sakurapool-record-v1" payload, digest_size=16).
- object_id is exactly `rel@sha256-<input sha256>` with the input sha256
  declared in INPUT.json; object_version and validator equal that sha256.
- objects rows carry the real field semantics (storage_id/backend
  "local", repo_type "local", archive_format "tar", path == rel).
- ObjectRef constructed from a synthetic object row must pass validation.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from sakurapool.indexer import BUILDER, FORMAT_VERSION, SCHEMAS, _json
from sakurapool.records import ObjectRef, RecordKey
from sakurapool.registry import DatasetAdapter

_UINT64_STRESS_BASE = 2**63


@dataclass(frozen=True)
class SampleSpec:
    """One logical image+metadata pair inside a synthetic object."""
    path: str
    post_id: str
    tags: list[tuple[str, str | None]] = field(default_factory=list)
    tags_state: str = "known"
    # Metadata (json) presence is independent of its size: json=False means
    # no json_path at all; json_size=0 keeps a zero-length metadata entry.
    has_json: bool = True
    json_size: int = 64


@dataclass(frozen=True)
class ObjectSpec:
    """One synthetic archive (P2 object) with its samples."""
    rel: str
    samples: list[SampleSpec] = field(default_factory=list)
    namespace: str = "tags"
    origin: str = "default"
    size: int = 1000
    # Set true to emit offsets/sizes beyond 2**63 (uint64 stress): no real
    # file is allocated, only the fragment columns carry the values.
    uint64_offsets: bool = False


def _record_id(dataset: str, object_id: str, sample_path: str) -> str:
    """Real P2 record identity: BLAKE2b-16 over the canonical payload."""
    return RecordKey(dataset, object_id, sample_path).record_id


def _input_digest(rel: str, seed: str) -> str:
    """Declared input sha256; it drives object_id/version/validator."""
    return hashlib.sha256(_json(["synthetic", seed, rel])).hexdigest()


def _object_id(rel: str, digest: str) -> str:
    return f"{rel}@sha256-{digest}"


def _table(name: str, columns: dict[str, list[Any]]) -> pa.Table:
    schema = SCHEMAS[name]
    arrays = [pa.array(columns[field.name], type=field.type) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def _object_row(dataset: str, source: str, obj: ObjectSpec, digest: str) -> dict:
    object_id = _object_id(obj.rel, digest)
    return dict(
        storage_id="local", backend="local", repo_type="local",
        archive_format="tar", dataset=dataset,
        object_path=obj.rel, object_size=obj.size, object_version=digest,
        validator_kind="sha256", validator=digest,
        scan_status="committed", dataset_id=dataset,
        object_id=object_id, source=source, path=obj.rel,
        size=obj.size, validator_strength="strong:sha256",
        sha256=digest)


def build_p2_directory(
        root: Path, *, dataset: str, source: str, objects: list[ObjectSpec],
        created_at: str | None = None,
        tag_category: str = "general") -> dict[str, Any]:
    """Write a committed P2 directory for the given synthetic objects."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    created_at = created_at or datetime.now(timezone.utc).isoformat()
    adapter = DatasetAdapter(dataset=dataset, source=source,
                             tag_category=tag_category)
    contract = dict(format_version=FORMAT_VERSION, builder=BUILDER,
                    adapter=adapter.to_dict(), hash_images=False, inputs={})
    contract["inputs"] = {obj.rel: dict(
        size=obj.size, mtime_ns=0,
        sha256=_input_digest(obj.rel, dataset),
        strength="strong:sha256", version=1) for obj in objects}
    (root / "INPUT.json").write_bytes(_json(contract))
    contract_hash = hashlib.sha256(_json(contract)).hexdigest()

    counts = dict(objects=0, samples=0, annotations=0, errors=0)
    for obj in objects:
        digest = _input_digest(obj.rel, dataset)
        object_id = _object_id(obj.rel, digest)
        shard_id = hashlib.sha256(_json([dataset, obj.rel])).hexdigest()

        objects_rows = {name: [value] for name, value
                        in _object_row(dataset, source, obj, digest).items()}

        samples_rows = {name: [] for name in (
            "record_id", "dataset_id", "object_id", "sample_path", "source",
            "post_id", "image_path", "offset_data", "size", "json_path",
            "json_offset_data", "json_size", "text", "tags_state", "tags",
            "hash_source", "sha256", "image_format", "width", "height",
            "has_alpha", "hash_kind", "status")}
        annotations_rows = {name: [] for name in (
            "record_id", "dataset_id", "object_id", "sample_path",
            "namespace", "origin", "tags_state", "tags")}

        for index, sample in enumerate(obj.samples):
            record_id = _record_id(dataset, object_id, sample.path)
            image_size = 32
            tag_structs = [
                dict(value=value, category=category)
                for value, category in sorted(sample.tags,
                                              key=lambda pair: (pair[0],
                                                                pair[1] or ""))]
            base = _UINT64_STRESS_BASE + index if obj.uint64_offsets else 0
            samples_rows["record_id"].append(record_id)
            samples_rows["dataset_id"].append(dataset)
            samples_rows["object_id"].append(object_id)
            samples_rows["sample_path"].append(sample.path)
            samples_rows["source"].append(source)
            samples_rows["post_id"].append(sample.post_id)
            samples_rows["image_path"].append(f"{obj.rel}/{sample.path}")
            samples_rows["offset_data"].append(base)
            samples_rows["size"].append(image_size)
            samples_rows["json_path"].append(
                f"{obj.rel}/{sample.path}.json" if sample.has_json else None)
            samples_rows["json_offset_data"].append(
                None if not sample.has_json else base + obj.size)
            samples_rows["json_size"].append(
                None if not sample.has_json else sample.json_size)
            samples_rows["text"].append(None)
            samples_rows["tags_state"].append(sample.tags_state)
            samples_rows["tags"].append(tag_structs)
            samples_rows["hash_source"].append("none")
            samples_rows["sha256"].append(None)
            samples_rows["image_format"].append("jpg")
            samples_rows["width"].append(10)
            samples_rows["height"].append(10)
            samples_rows["has_alpha"].append(False)
            samples_rows["hash_kind"].append(None)
            samples_rows["status"].append("ok")

            annotations_rows["record_id"].append(record_id)
            annotations_rows["dataset_id"].append(dataset)
            annotations_rows["object_id"].append(object_id)
            annotations_rows["sample_path"].append(sample.path)
            annotations_rows["namespace"].append(obj.namespace)
            annotations_rows["origin"].append(obj.origin)
            annotations_rows["tags_state"].append(sample.tags_state)
            annotations_rows["tags"].append(tag_structs)

        errors_rows = {name: [] for name in (
            "dataset_id", "object_id", "source", "object_path", "member",
            "post_id", "record_id", "path", "code", "error_detail", "detail")}

        files = {}
        for name, rows in (("objects", objects_rows),
                           ("samples", samples_rows),
                           ("annotations", annotations_rows),
                           ("errors", errors_rows)):
            path = root / f"{shard_id}.{name}.parquet"
            table = _table(name, rows)
            pq.write_table(table, path)
            files[name] = dict(path=path.name,
                               sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                               bytes=path.stat().st_size,
                               rows=table.num_rows)
        commit = dict(
            schema=FORMAT_VERSION, builder=BUILDER, dataset_id=dataset,
            object_id=object_id,
            created_at=created_at,
            input=contract["inputs"][obj.rel], files=files,
            contract_sha256=contract_hash)
        (root / f"{shard_id}.COMMIT").write_bytes(_json(commit))
        counts["objects"] += 1
        counts["samples"] += len(obj.samples)
        counts["annotations"] += len(obj.samples)
    return counts


def assert_object_ref_valid(dataset: str, source: str, obj: ObjectSpec) -> None:
    """A synthetic object must construct a valid real-Contract ObjectRef."""
    digest = _input_digest(obj.rel, dataset)
    ObjectRef(
        storage_id="local",
        object_id=_object_id(obj.rel, digest),
        object_path=obj.rel,
        object_size=obj.size,
        object_version=digest,
        validator=digest,
        backend="local",
        repo_type="local",
        validator_kind="sha256",
        validator_strength="strong:sha256",
        archive_format="tar")


def make_tag_specs(hot: list[str], medium: list[str], rare: list[str],
                   index: int, rng) -> SampleSpec:
    """§52-style distribution: hot ~70%, medium ~10%, rare ~0.1%."""
    tags: list[tuple[str, str | None]] = []
    roll = rng.random()
    if roll < 0.70:
        tags.append((hot[rng.randrange(len(hot))], "general"))
    elif roll < 0.80:
        tags.append((medium[rng.randrange(len(medium))], "general"))
    elif roll < 0.801:
        tags.append((rare[rng.randrange(len(rare))], "general"))
    if rng.random() < 0.2:
        extra = hot[rng.randrange(len(hot))]
        if extra not in [value for value, _ in tags]:
            tags.append((extra, "general"))
    return SampleSpec(path=f"{index}.jpg", post_id=str(index),
                      tags=sorted(tags), tags_state="known")
