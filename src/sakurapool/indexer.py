"""Local, header-only TAR indexing; versioned shard transactions, no image decoding."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tarfile
import time
from collections import Counter
from pathlib import Path, PurePosixPath
from typing import Any, Callable

import pyarrow as pa
import pyarrow.parquet as pq

from .registry import AdapterRegistry, DatasetAdapter

OBJECTS_SCHEMA = pa.schema(
    [
        pa.field("object_id", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("dataset", pa.string(), nullable=False),
        pa.field("shard", pa.string(), nullable=False),
        pa.field("image_path", pa.string(), nullable=False),
        pa.field("json_path", pa.string()),
        pa.field("offset_data", pa.uint64(), nullable=False),
        pa.field("size", pa.uint64(), nullable=False),
        pa.field("tags_state", pa.string(), nullable=False),
        pa.field("tags", pa.list_(pa.string())),
        pa.field("hash_source", pa.string(), nullable=False),
        pa.field("sha256", pa.string()),
    ]
)
SAMPLES_SCHEMA = pa.schema(
    [
        pa.field("sample_id", pa.string(), nullable=False),
        pa.field("object_id", pa.string(), nullable=False),
        pa.field("text", pa.string()),
    ]
)
ANNOTATIONS_SCHEMA = pa.schema(
    [
        pa.field("object_id", pa.string(), nullable=False),
        pa.field("key", pa.string(), nullable=False),
        pa.field("value", pa.string(), nullable=False),
    ]
)
ERRORS_SCHEMA = pa.schema(
    [
        pa.field("source", pa.string(), nullable=False),
        pa.field("path", pa.string()),
        pa.field("code", pa.string(), nullable=False),
        pa.field("detail", pa.string()),
    ]
)
SCHEMAS = dict(
    objects=OBJECTS_SCHEMA,
    samples=SAMPLES_SCHEMA,
    annotations=ANNOTATIONS_SCHEMA,
    errors=ERRORS_SCHEMA,
)
FORMAT_VERSION = 1


class UnsupportedArchiveError(ValueError):
    """Compressed, sparse, or otherwise unsupported TAR input."""


def _json(value: Any) -> bytes:
    return (
        json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False, separators=(",", ":"))
        + "\n"
    ).encode()


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def discover_archives(root: Path) -> list[Path]:
    if not root.is_dir():
        raise ValueError(f"input must be an existing directory: {root}")
    paths = []
    for path in sorted(root.rglob("*")):
        name = path.name.lower()
        if name.endswith((".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz", ".tar.zst")):
            raise UnsupportedArchiveError(f"compressed archive unsupported: {path}")
        if name.endswith(".tar"):
            if path.is_symlink() or not path.is_file():
                raise ValueError(f"archive must be a regular local file: {path}")
            if not path.resolve().is_relative_to(root.resolve()):
                raise ValueError("archive escapes input directory")
            paths.append(path)
    if not paths:
        raise ValueError("no TAR archives found")
    return paths


def _validator(path: Path) -> dict[str, Any]:
    before = path.stat()
    digest = _sha(path)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError("input validator mismatch: input changed while hashing")
    return dict(size=after.st_size, mtime_ns=after.st_mtime_ns, sha256=digest)


def _sync_directory(path: Path) -> None:
    # Windows CRT cannot fsync directories (nor read-only file descriptors).
    if os.name != "nt":
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _atomic(path: Path, data: bytes, checkpoint: Callable[[str], None]) -> None:
    partial = path.with_name(path.name + ".partial")
    with partial.open("wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    checkpoint("A")
    os.replace(partial, path)
    _sync_directory(path.parent)


def _atomic_parquet(
    path: Path, rows: list[dict[str, Any]], schema: pa.Schema, checkpoint: Callable[[str], None]
) -> None:
    partial = path.with_name(path.name + ".partial")
    with partial.open("wb") as stream:
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), stream)
        stream.flush()
        os.fsync(stream.fileno())
    checkpoint("A")
    os.replace(partial, path)
    _sync_directory(path.parent)


def _read_member(archive: tarfile.TarFile, member: tarfile.TarInfo) -> bytes:
    archive.fileobj.seek(member.offset_data)
    data = archive.fileobj.read(member.size)
    if len(data) != member.size:
        raise ValueError(f"short read for {member.name}")
    return data


def _safe(name: str) -> bool:
    parts = name.split("/")
    return (
        bool(name)
        and "\\" not in name
        and ":" not in name
        and all(part not in ("", ".", "..") for part in parts)
    )


def _scan_shard(
    path: Path, rel: str, adapter: DatasetAdapter, hash_images: bool, timings: dict[str, float]
) -> dict[str, list[dict[str, Any]]]:
    rows: dict[str, list[dict[str, Any]]] = {name: [] for name in SCHEMAS}

    def error(member: str, code: str, detail: str = "") -> None:
        rows["errors"].append(dict(source=rel, path=member, code=code, detail=detail))

    started = time.perf_counter()
    try:
        archive = tarfile.open(path, "r:")
    except tarfile.ReadError as exc:
        raise UnsupportedArchiveError(f"unsupported uncompressed TAR: {path}") from exc
    with archive:
        members = archive.getmembers()
        timings["header_seconds"] += time.perf_counter() - started
        duplicates = Counter(m.name for m in members)
        candidates: dict[str, list[tarfile.TarInfo]] = {}
        images: dict[str, list[tarfile.TarInfo]] = {}
        size = path.stat().st_size
        for member in members:
            if member.isdir():
                continue
            if not _safe(member.name):
                error(member.name, "unsafe_member")
                continue
            if not member.isfile() or member.issparse():
                error(member.name, "unsupported_member")
                continue
            if member.offset_data < 0 or member.size < 0 or member.offset_data + member.size > size:
                raise ValueError(f"invalid TAR extent: {member.name}")
            if duplicates[member.name] != 1:
                error(member.name, "duplicate_member")
                continue
            key = adapter.pair_key(member.name)
            suffix = PurePosixPath(member.name).suffix.lower()
            if suffix == ".json":
                candidates.setdefault(key, []).append(member)
            elif suffix in adapter.image_extensions:
                images.setdefault(key, []).append(member)
        for key, group in sorted(images.items()):
            jsons = candidates.get(key, [])
            if len(group) != 1 or len(jsons) > 1:
                for member in group:
                    error(member.name, "ambiguous_pair")
                continue
            image = group[0]
            if not jsons:
                error(image.name, "missing_json")
                continue
            meta = jsons[0]
            started = time.perf_counter()
            try:
                if meta.size > adapter.max_json_bytes:
                    raise ValueError("metadata exceeds configured size limit")
                metadata = json.loads(
                    _read_member(archive, meta),
                    parse_constant=lambda x: (_ for _ in ()).throw(
                        ValueError(f"non-finite JSON: {x}")
                    ),
                )
                if not isinstance(metadata, dict):
                    raise ValueError("metadata must be a JSON object")
                if "source" in metadata and metadata["source"] != adapter.source:
                    error(image.name, "source_mismatch")
                    continue
                _json(metadata)
            except (ValueError, UnicodeError) as exc:
                error(meta.name, "invalid_metadata", str(exc))
                continue
            finally:
                timings["json_seconds"] += time.perf_counter() - started
            tags = metadata.get(adapter.tags_field)
            state = (
                "missing"
                if adapter.tags_field not in metadata
                else "invalid"
                if not isinstance(tags, list) or any(not isinstance(t, str) for t in tags)
                else "empty"
                if not tags
                else "known"
            )
            declared = metadata.get("sha256")
            if declared is not None and (
                not isinstance(declared, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", declared)
            ):
                error(meta.name, "invalid_declared_hash")
                declared = None
            digest = declared.lower() if declared else None
            origin = "declared:json.sha256" if digest else "missing"
            if hash_images:
                digest = hashlib.sha256(_read_member(archive, image)).hexdigest()
                origin = "computed:sha256"
            identity = [
                adapter.dataset,
                adapter.source,
                rel,
                image.name,
                image.offset_data,
                image.size,
            ]
            object_id = hashlib.sha256(_json(identity)).hexdigest()
            rows["objects"].append(
                dict(
                    object_id=object_id,
                    source=adapter.source,
                    dataset=adapter.dataset,
                    shard=rel,
                    image_path=image.name,
                    json_path=meta.name,
                    offset_data=image.offset_data,
                    size=image.size,
                    tags_state=state,
                    tags=tags if state in ("empty", "known") else None,
                    hash_source=origin,
                    sha256=digest,
                )
            )
            text = metadata.get(adapter.text_field)
            if text is not None and not isinstance(text, str):
                error(meta.name, "invalid_text")
                text = None
            rows["samples"].append(dict(sample_id=object_id, object_id=object_id, text=text))
            rows["annotations"].extend(
                dict(object_id=object_id, key=k, value=_json(v).decode().rstrip("\n"))
                for k, v in sorted(metadata.items())
            )
        for key, group in sorted(candidates.items()):
            if key not in images:
                for member in group:
                    error(member.name, "orphan_json")
    return rows


def scan(
    root: Path,
    output: Path,
    *,
    hash_images: bool = False,
    dataset: str = "local",
    registry: AdapterRegistry | None = None,
    checkpoint: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Build four fragments per shard. Single writer; resume never trusts mere existence.

    Checkpoints A-E are test hooks: partial fsync, first rename, all fragments,
    commit rename, and publication complete. Exceptions deliberately propagate.
    """
    started = time.perf_counter()
    timings = dict(header_seconds=0.0, json_seconds=0.0, parquet_seconds=0.0)
    checkpoint = checkpoint or (lambda stage: None)
    adapter = (registry or AdapterRegistry.local()).get(dataset)
    root, output = Path(root).resolve(), Path(output).resolve()
    if output == root or output.is_relative_to(root) or root.is_relative_to(output):
        raise ValueError("input and output directories must not overlap")
    archives = discover_archives(root)
    validators = {p.relative_to(root).as_posix(): _validator(p) for p in archives}
    contract = dict(
        format_version=FORMAT_VERSION,
        adapter=adapter.to_dict(),
        hash_images=hash_images,
        inputs=validators,
    )
    output.mkdir(parents=True, exist_ok=True)
    for stale in output.glob("*.partial"):
        stale.unlink()
    input_path = output / "INPUT.json"
    if input_path.exists():
        if json.loads(input_path.read_bytes()) != contract:
            raise ValueError("input validator mismatch: inputs or configuration changed")
    else:
        if any(output.iterdir()):
            raise ValueError("output contains artifacts without INPUT.json")
        _atomic(input_path, _json(contract), lambda _: None)
    counts = dict(archives=len(archives), objects=0, errors=0, skipped=0)
    expected_markers = set()
    for path in archives:
        rel = path.relative_to(root).as_posix()
        shard_id = hashlib.sha256(_json([dataset, adapter.source, rel])).hexdigest()
        marker = output / f"{shard_id}.COMMIT"
        expected_markers.add(marker.name)
        files = {name: output / f"{shard_id}.{name}.parquet" for name in SCHEMAS}
        if marker.exists():
            try:
                commit = json.loads(marker.read_bytes())
                if commit["contract_sha256"] != hashlib.sha256(_json(contract)).hexdigest():
                    raise ValueError("contract hash mismatch")
                if commit["input"] != validators[rel] or commit["shard"] != rel:
                    raise ValueError("input validator mismatch")
                if set(commit["files"]) != set(SCHEMAS):
                    raise ValueError("fragment set mismatch")
                for name, fragment in files.items():
                    info = commit["files"][name]
                    if info["sha256"] != _sha(fragment) or info["bytes"] != fragment.stat().st_size:
                        raise ValueError(f"output hash mismatch: {name}")
                    table = pq.read_table(fragment)
                    if table.schema != SCHEMAS[name] or table.num_rows != info["rows"]:
                        raise ValueError(f"schema/row mismatch: {name}")
                counts["objects"] += commit["files"]["objects"]["rows"]
                counts["errors"] += commit["files"]["errors"]["rows"]
                counts["skipped"] += 1
                continue
            except (OSError, ValueError, KeyError, TypeError, pa.ArrowException) as exc:
                raise ValueError(f"corrupt committed shard {rel}: {exc}") from exc
        rows = _scan_shard(path, rel, adapter, hash_images, timings)
        started_parquet = time.perf_counter()
        info = {}
        for index, (name, fragment) in enumerate(files.items()):
            _atomic_parquet(fragment, rows[name], SCHEMAS[name], checkpoint)
            info[name] = dict(
                sha256=_sha(fragment), bytes=fragment.stat().st_size, rows=len(rows[name])
            )
            if index == 0:
                checkpoint("B")
        timings["parquet_seconds"] += time.perf_counter() - started_parquet
        checkpoint("C")
        if _validator(path) != validators[rel]:
            raise ValueError("input validator mismatch: input changed during scan")
        commit = dict(
            format_version=FORMAT_VERSION,
            shard=rel,
            input=validators[rel],
            files=info,
            contract_sha256=hashlib.sha256(_json(contract)).hexdigest(),
        )
        _atomic(marker, _json(commit), lambda _: None)
        checkpoint("D")
        counts["objects"] += len(rows["objects"])
        counts["errors"] += len(rows["errors"])
        checkpoint("E")
    if {p.name for p in output.glob("*.COMMIT")} != expected_markers:
        raise ValueError("unexpected committed shard")
    current = {p.relative_to(root).as_posix(): _validator(p) for p in discover_archives(root)}
    if current != validators:
        raise ValueError("input validator mismatch: inputs changed during scan")
    return {**counts, **timings, "total_seconds": time.perf_counter() - started}
