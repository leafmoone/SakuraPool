"""Transactional, single-writer indexing of local uncompressed TAR objects."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tarfile
import time
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable

import pyarrow as pa
import pyarrow.parquet as pq

from .records import RecordKey, register_identity
from .registry import AdapterRegistry, DatasetAdapter

TAG_TYPE = pa.list_(pa.struct([
    pa.field("value", pa.string(), nullable=False),
    pa.field("namespace", pa.string(), nullable=False),
    pa.field("origin", pa.string(), nullable=False),
    pa.field("category", pa.string(), nullable=False),
]))
KEY_FIELDS = [pa.field(k, pa.string(), nullable=False)
              for k in ("record_id", "dataset_id", "object_id", "sample_path")]
OBJECTS_SCHEMA = pa.schema([
    pa.field("dataset_id", pa.string(), nullable=False),
    pa.field("object_id", pa.string(), nullable=False),
    pa.field("source", pa.string(), nullable=False),
    pa.field("path", pa.string(), nullable=False),
    pa.field("size", pa.uint64(), nullable=False),
    pa.field("validator_strength", pa.string(), nullable=False),
    pa.field("sha256", pa.string(), nullable=False),
])
SAMPLES_SCHEMA = pa.schema(KEY_FIELDS + [
    pa.field("source", pa.string(), nullable=False),
    pa.field("post_id", pa.string(), nullable=False),
    pa.field("image_path", pa.string(), nullable=False),
    pa.field("offset_data", pa.uint64(), nullable=False),
    pa.field("size", pa.uint64(), nullable=False),
    pa.field("json_path", pa.string()),
    pa.field("json_offset_data", pa.uint64()),
    pa.field("json_size", pa.uint64()),
    pa.field("metadata", pa.string()),
    pa.field("text", pa.string()),
    pa.field("tags_state", pa.string(), nullable=False),
    pa.field("tags", TAG_TYPE),
    pa.field("hash_source", pa.string(), nullable=False),
    pa.field("sha256", pa.string()),
])
ANNOTATIONS_SCHEMA = pa.schema(KEY_FIELDS + [
    pa.field("namespace", pa.string(), nullable=False),
    pa.field("origin", pa.string(), nullable=False),
    pa.field("category", pa.string(), nullable=False),
    pa.field("value", pa.string(), nullable=False),
])
ERROR_CODES = frozenset({
    "invalid_post_id", "missing_image", "missing_metadata", "metadata_invalid", "source_mismatch",
    "ambiguous_image_member", "ambiguous_metadata_member", "unsupported_member",
    "unsupported_archive", "record_identity_conflict",
})
ERRORS_SCHEMA = pa.schema([
    pa.field("dataset_id", pa.string(), nullable=False),
    pa.field("object_id", pa.string(), nullable=False),
    pa.field("source", pa.string(), nullable=False),
    pa.field("path", pa.string()),
    pa.field("code", pa.string(), nullable=False),
    pa.field("detail", pa.string()),
])
SCHEMAS = dict(objects=OBJECTS_SCHEMA, samples=SAMPLES_SCHEMA,
               annotations=ANNOTATIONS_SCHEMA, errors=ERRORS_SCHEMA)
FORMAT_VERSION = 2
BUILDER = "sakurapool-p2-v2"
BATCH_SIZE = 1024


class UnsupportedArchiveError(ValueError):
    """unsupported_archive: compressed or malformed TAR input."""


def _json(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                       separators=(",", ":")) + "\n").encode("utf-8")


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def discover_archives(root: Path) -> list[Path]:
    if root.is_symlink():
        raise ValueError("archive input must not be a symlink")
    candidates = [root] if root.is_file() else sorted(root.rglob("*")) if root.is_dir() else []
    result = []
    for path in candidates:
        name = path.name.lower()
        if name.endswith((".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz", ".tar.zst")):
            raise UnsupportedArchiveError(f"unsupported_archive: compressed TAR: {path}")
        if name.endswith(".tar") or root.is_file():
            if path.is_symlink() or not path.is_file():
                raise ValueError("archive must be a regular local file")
            with path.open("rb") as stream:
                magic = stream.read(6)
            if magic.startswith((b"\x1f\x8b", b"BZh", b"\xfd7zXZ", b"\x28\xb5\x2f\xfd")):
                raise UnsupportedArchiveError(f"unsupported_archive: compressed TAR: {path}")
            result.append(path)
    if not result:
        raise ValueError("no TAR archives found")
    return result


def _validator(path: Path) -> dict[str, Any]:
    before = path.stat()
    digest = _sha(path)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError("input validator mismatch: changed while hashing")
    return dict(size=after.st_size, mtime_ns=after.st_mtime_ns, sha256=digest,
                strength="strong:sha256", version=1)


def _sync_directory(path: Path) -> None:
    # Windows CRT has no directory fsync. Process recovery is tested, not power-loss durability.
    if os.name != "nt":
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _atomic(path: Path, data: bytes) -> None:
    partial = path.with_name(path.name + ".partial")
    with partial.open("wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(partial, path)
    _sync_directory(path.parent)


def _read_member(archive: tarfile.TarFile, member: tarfile.TarInfo) -> bytes:
    archive.fileobj.seek(member.offset_data)
    data = archive.fileobj.read(member.size)
    if len(data) != member.size:
        raise ValueError(f"short read for {member.name}")
    return data


def _safe(name: str) -> bool:
    return bool(name) and "\\" not in name and ":" not in name and all(
        p not in ("", ".", "..") for p in name.split("/")
    )


def _scan_shard(path: Path, rel: str, adapter: DatasetAdapter, hash_images: bool,
                timings: dict[str, float], validator: dict[str, Any]) -> dict[str, list[dict]]:
    rows: dict[str, list[dict]] = {name: [] for name in SCHEMAS}
    rows["objects"].append(dict(
        dataset_id=adapter.dataset, object_id=rel, source=adapter.source,
        path=rel, size=validator["size"], validator_strength=validator["strength"],
        sha256=validator["sha256"],
    ))

    def error(member: str, code: str, detail: str = "") -> None:
        assert code in ERROR_CODES
        rows["errors"].append(dict(dataset_id=adapter.dataset, object_id=rel,
                                   source=adapter.source, path=member, code=code, detail=detail))

    started = time.perf_counter()
    try:
        archive = tarfile.open(path, "r:")
    except tarfile.ReadError as exc:
        raise UnsupportedArchiveError(f"unsupported_archive: {path}") from exc
    with archive:
        members = archive.getmembers()
        timings["header_seconds"] += time.perf_counter() - started
        images: dict[str, list[tarfile.TarInfo]] = {}
        metadata_members: dict[str, list[tarfile.TarInfo]] = {}
        for member in members:
            if member.isdir():
                continue
            if not _safe(member.name):
                error(member.name, "unsupported_member", "unsafe member path")
                continue
            parts = PurePosixPath(member.name).parts
            if any(p.startswith(".") for p in parts) or parts[-1] in adapter.ignored_names:
                continue
            suffix = PurePosixPath(member.name).suffix.lower()
            if suffix != ".json" and suffix not in adapter.image_extensions:
                continue
            if not member.isfile() or member.issparse():
                error(member.name, "unsupported_member")
                continue
            if not 0 <= member.offset_data <= member.offset_data + member.size <= validator["size"]:
                raise ValueError(f"invalid TAR extent: {member.name}")
            key = adapter.pair_key(member.name)
            group = metadata_members if suffix == ".json" else images
            group.setdefault(key, []).append(member)
        seen: dict[str, RecordKey] = {}
        for key in sorted(images.keys() | metadata_members.keys()):
            group, jsons = images.get(key, []), metadata_members.get(key, [])
            if len(group) > 1:
                error(key, "ambiguous_image_member")
            if len(jsons) > 1:
                error(key, "ambiguous_metadata_member")
            if len(group) > 1 or len(jsons) > 1:
                continue
            if not group:
                error(key, "missing_image")
                continue
            if not jsons and adapter.metadata_required:
                error(key, "missing_metadata")
                continue
            post_id = PurePosixPath(key).name
            if not post_id or (adapter.numeric_post_id and not re.fullmatch(r"[0-9]+", post_id)):
                error(key, "invalid_post_id")
                continue
            image, meta = group[0], jsons[0] if jsons else None
            metadata = None
            started = time.perf_counter()
            try:
                if meta is not None:
                    if meta.size > adapter.max_json_bytes:
                        raise ValueError("metadata exceeds configured size limit")
                    metadata = json.loads(_read_member(archive, meta), parse_constant=lambda x: (
                        _ for _ in ()).throw(ValueError(f"non-finite JSON: {x}")))
                    if not isinstance(metadata, dict):
                        raise ValueError("metadata must be a JSON object")
                    _json(metadata)
                    if "source" in metadata and metadata["source"] != adapter.source:
                        error(meta.name, "source_mismatch")
                        continue
            except (ValueError, UnicodeError) as exc:
                error(meta.name, "metadata_invalid", str(exc))
                continue
            finally:
                timings["json_seconds"] += time.perf_counter() - started
            values = metadata or {}
            tags = values.get(adapter.tags_field)
            state = ("missing" if adapter.tags_field not in values else "invalid"
                     if not isinstance(tags, list) or any(not isinstance(t, str) for t in tags)
                     else "empty" if not tags else "known")
            tag_rows = [dict(value=t, namespace=adapter.tag_namespace,
                             origin=f"declared:json.{adapter.tags_field}",
                             category=adapter.tag_category) for t in tags] if state in (
                                 "known", "empty") else None
            digest = values.get("sha256")
            if digest is not None and (not isinstance(digest, str)
                                       or not re.fullmatch(r"[0-9a-fA-F]{64}", digest)):
                error(meta.name, "metadata_invalid", "invalid declared sha256")
                digest = None
            digest = digest.lower() if digest else None
            origin = "declared:json.sha256" if digest else "missing"
            if hash_images:
                digest = hashlib.sha256(_read_member(archive, image)).hexdigest()
                origin = "computed:sha256"
            text = values.get(adapter.text_field)
            if text is not None and not isinstance(text, str):
                error(meta.name, "metadata_invalid", "invalid text")
                text = None
            identity = RecordKey(adapter.dataset, rel, key)
            record_id = register_identity(seen, identity)
            identity_fields = dict(record_id=record_id, dataset_id=adapter.dataset,
                                   object_id=rel, sample_path=key)
            rows["samples"].append(dict(
                **identity_fields, source=adapter.source, post_id=post_id, image_path=image.name,
                offset_data=image.offset_data, size=image.size,
                json_path=meta.name if meta else None,
                json_offset_data=meta.offset_data if meta else None,
                json_size=meta.size if meta else None,
                metadata=_json(metadata).decode().rstrip("\n") if metadata is not None else None,
                text=text, tags_state=state, tags=tag_rows, hash_source=origin, sha256=digest,
            ))
            if metadata:
                rows["annotations"].append(dict(
                    **identity_fields, namespace="metadata", origin="declared:json",
                    category="document", value=_json(metadata).decode().rstrip("\n"),
                ))
    return rows


def _write_fragments(files: dict[str, Path], rows: dict[str, list[dict]],
                     checkpoint: Callable[[str], None]) -> dict[str, dict]:
    info = {}
    for name, final in files.items():
        partial = final.with_name(final.name + ".partial")
        with partial.open("wb") as stream:
            with pq.ParquetWriter(stream, SCHEMAS[name]) as writer:
                if name == "samples":
                    halfway = max(1, len(rows[name]) // 2)
                    writer.write_table(pa.Table.from_pylist(rows[name][:halfway], SCHEMAS[name]))
                    stream.flush()
                    os.fsync(stream.fileno())
                    checkpoint("A")  # samples partial lacks footer and remaining rows
                    start = halfway
                else:
                    start = 0
                for offset in range(start, len(rows[name]), BATCH_SIZE):
                    writer.write_table(pa.Table.from_pylist(
                        rows[name][offset:offset + BATCH_SIZE], SCHEMAS[name]))
            stream.flush()
            os.fsync(stream.fileno())
        info[name] = dict(path=final.name, sha256=_sha(partial),
                          bytes=partial.stat().st_size, rows=len(rows[name]))
    checkpoint("B")  # all parquet closed and fsynced, no rename
    for index, final in enumerate(files.values()):
        os.replace(final.with_name(final.name + ".partial"), final)
        _sync_directory(final.parent)
        if index == 0:
            checkpoint("C")
    checkpoint("D")  # all final files, no marker
    return info


def scan(root: Path, output: Path, *, hash_images: bool = False, dataset: str = "local",
         registry: AdapterRegistry | None = None,
         checkpoint: Callable[[str], None] | None = None) -> dict[str, Any]:
    """A-D restart builds safely; E verifies the commit before skipping the TAR object."""
    started = time.perf_counter()
    timings = dict(header_seconds=0.0, json_seconds=0.0, parquet_seconds=0.0)
    checkpoint = checkpoint or (lambda stage: None)
    adapter = (registry or AdapterRegistry.local()).get(dataset)
    root, output = Path(root).absolute(), Path(output).absolute()
    archives = discover_archives(root)
    root, output = root.resolve(), output.resolve()
    base = root.parent if root.is_file() else root
    if output == root or output.is_relative_to(root) or root.is_relative_to(output):
        raise ValueError("input and output directories must not overlap")
    validators = {p.relative_to(base).as_posix(): _validator(p) for p in archives}
    contract = dict(format_version=FORMAT_VERSION, builder=BUILDER, adapter=adapter.to_dict(),
                    hash_images=hash_images, inputs=validators)
    output.mkdir(parents=True, exist_ok=True)
    input_path = output / "INPUT.json"
    if input_path.exists():
        if json.loads(input_path.read_bytes()) != contract:
            raise ValueError("input validator mismatch: inputs or configuration changed")
    else:
        # Only this exact reserved partial belongs to an interrupted INPUT publication.
        unknown = [p for p in output.iterdir() if p.name != "INPUT.json.partial"]
        if unknown:
            raise ValueError("output contains artifacts without INPUT.json")
        _atomic(input_path, _json(contract))
    counts = dict(archives=len(archives), objects_seen=len(archives), objects_committed=0,
                  objects_skipped=0, objects=0, samples=0, errors=0, skipped=0)
    expected_markers = set()
    for path in archives:
        rel = path.relative_to(base).as_posix()
        shard_id = hashlib.sha256(_json([dataset, rel])).hexdigest()
        marker = output / f"{shard_id}.COMMIT"
        expected_markers.add(marker.name)
        files = {name: output / f"{shard_id}.{name}.parquet" for name in SCHEMAS}
        if marker.exists():
            try:
                commit = json.loads(marker.read_bytes())
                if commit["contract_sha256"] != hashlib.sha256(_json(contract)).hexdigest():
                    raise ValueError("contract hash mismatch")
                if (commit["input"] != validators[rel] or commit["object_id"] != rel
                        or commit["dataset_id"] != dataset or commit["builder"] != BUILDER
                        or commit["schema"] != FORMAT_VERSION):
                    raise ValueError("commit identity/validator mismatch")
                if set(commit["files"]) != set(SCHEMAS):
                    raise ValueError("fragment set mismatch")
                for name, fragment in files.items():
                    info = commit["files"][name]
                    if (info["path"] != fragment.name or info["sha256"] != _sha(fragment)
                            or info["bytes"] != fragment.stat().st_size):
                        raise ValueError(f"output hash mismatch: {name}")
                    table = pq.read_table(fragment)
                    if table.schema != SCHEMAS[name] or table.num_rows != info["rows"]:
                        raise ValueError(f"schema/row mismatch: {name}")
                counts["objects_skipped"] += 1
                counts["skipped"] += 1
            except (OSError, ValueError, KeyError, TypeError, pa.ArrowException) as exc:
                raise ValueError(f"CORRUPT_COMMIT: corrupt committed shard {rel}: {exc}") from exc
        else:
            # INPUT proves ownership of the exact reserved shard files; unknown files are untouched.
            for final in [*files.values(), marker]:
                partial = final.with_name(final.name + ".partial")
                if partial.exists():
                    partial.unlink()
            rows = _scan_shard(path, rel, adapter, hash_images, timings, validators[rel])
            started_parquet = time.perf_counter()
            info = _write_fragments(files, rows, checkpoint)
            timings["parquet_seconds"] += time.perf_counter() - started_parquet
            if _validator(path) != validators[rel]:
                raise ValueError("input validator mismatch: input changed during scan")
            commit = dict(schema=FORMAT_VERSION, builder=BUILDER, dataset_id=dataset, object_id=rel,
                          created_at=datetime.now(timezone.utc).isoformat(), input=validators[rel],
                          files=info, contract_sha256=hashlib.sha256(_json(contract)).hexdigest())
            _atomic(marker, _json(commit))
            counts["objects_committed"] += 1
            checkpoint("E")
        counts["objects"] += commit["files"]["objects"]["rows"]
        counts["samples"] += commit["files"]["samples"]["rows"]
        counts["errors"] += commit["files"]["errors"]["rows"]
    if {p.name for p in output.glob("*.COMMIT")} != expected_markers:
        raise ValueError("unexpected committed shard")
    current = {p.relative_to(base).as_posix(): _validator(p) for p in discover_archives(root)}
    if current != validators:
        raise ValueError("input validator mismatch: inputs changed during scan")
    return {**counts, **timings, "total_seconds": time.perf_counter() - started}
