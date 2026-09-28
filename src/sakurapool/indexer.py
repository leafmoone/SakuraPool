"""Transactional, single-writer indexing of local uncompressed TAR objects."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable

import pyarrow as pa
import pyarrow.parquet as pq

from .records import IdentityConflictError, RecordKey
from .registry import AdapterRegistry, DatasetAdapter

TAG_TYPE = pa.list_(pa.struct([
    pa.field("value", pa.string(), nullable=False),
    pa.field("category", pa.string()),
]))
KEY_FIELDS = [pa.field(k, pa.string(), nullable=False)
              for k in ("record_id", "dataset_id", "object_id", "sample_path")]
OBJECTS_SCHEMA = pa.schema([
    pa.field("storage_id", pa.string(), nullable=False),
    pa.field("backend", pa.string(), nullable=False),
    pa.field("repo_type", pa.string()),
    pa.field("archive_format", pa.string(), nullable=False),
    pa.field("dataset", pa.string(), nullable=False),
    pa.field("object_path", pa.string(), nullable=False),
    pa.field("object_size", pa.uint64(), nullable=False),
    pa.field("object_version", pa.string(), nullable=False),
    pa.field("validator_kind", pa.string(), nullable=False),
    pa.field("validator", pa.string(), nullable=False),
    pa.field("scan_status", pa.string(), nullable=False),
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
    pa.field("text", pa.string()),
    pa.field("tags_state", pa.string(), nullable=False),
    pa.field("tags", TAG_TYPE),
    pa.field("hash_source", pa.string(), nullable=False),
    pa.field("sha256", pa.string()),
    pa.field("image_format", pa.string()),
    pa.field("width", pa.uint32()),
    pa.field("height", pa.uint32()),
    pa.field("has_alpha", pa.bool_()),
    pa.field("hash_kind", pa.string()),
    pa.field("status", pa.string(), nullable=False),
])
ANNOTATIONS_SCHEMA = KEY_FIELDS + [
    pa.field("namespace", pa.string(), nullable=False),
    pa.field("origin", pa.string(), nullable=False),
    pa.field("tags_state", pa.string(), nullable=False),
    pa.field("tags", TAG_TYPE),
]
ANNOTATIONS_SCHEMA = pa.schema(ANNOTATIONS_SCHEMA)
ERROR_CODES = frozenset({
    "invalid_post_id", "missing_image", "missing_metadata", "metadata_invalid", "source_mismatch",
    "ambiguous_image_member", "ambiguous_metadata_member", "unsupported_member",
    "unsupported_archive", "record_identity_conflict",
})
ERRORS_SCHEMA = pa.schema([
    pa.field("dataset_id", pa.string(), nullable=False),
    pa.field("object_id", pa.string(), nullable=False),
    pa.field("source", pa.string(), nullable=False),
    pa.field("object_path", pa.string(), nullable=False),
    pa.field("member", pa.string()),
    pa.field("post_id", pa.string()),
    pa.field("record_id", pa.string()),
    pa.field("path", pa.string()),
    pa.field("code", pa.string(), nullable=False),
    pa.field("error_detail", pa.string()),
    pa.field("detail", pa.string()),
])
SCHEMAS = dict(objects=OBJECTS_SCHEMA, samples=SAMPLES_SCHEMA,
               annotations=ANNOTATIONS_SCHEMA, errors=ERRORS_SCHEMA)


class _TableProxy:
    def __init__(self, owner: "_SpoolRows", name: str) -> None:
        self.owner, self.name = owner, name

    def append(self, row: dict[str, Any]) -> None:
        self.owner.append(self.name, row)


class _SpoolState:
    def __init__(self, path: Path, handle) -> None:
        self.path, self.handle, self.db = path, handle, None
        self.errors: list[BaseException] = []


class CleanupHandle:
    """Controlled retry handle for owned spool resources after a scan failure."""
    def __init__(self, scope: "_SpoolScope") -> None:
        self._scope = scope
        self.errors: list[BaseException] = []

    @property
    def pending_resources(self) -> list[dict[str, Any]]:
        return [{"path": resource.path, "errors": list(resource.errors)}
                for resource in self._scope.resources]

    def retry(self) -> list[dict[str, Any]]:
        self.errors = self._scope.cleanup()
        return self.pending_resources


class _SpoolScope:
    def __init__(self) -> None:
        self.resources: set[_SpoolState] = set()
        self.handle = CleanupHandle(self)

    def add(self, resource: _SpoolState) -> None:
        self.resources.add(resource)

    def discard(self, resource: _SpoolState) -> None:
        self.resources.discard(resource)

    def cleanup(self) -> list[BaseException]:
        errors = []
        for resource in list(self.resources):
            resource_errors = _release_spool(resource)
            errors.extend(resource_errors)
            if not resource_errors:
                self.discard(resource)
        self.handle.errors = errors
        return errors


def _remove_spool_files(path: Path) -> list[BaseException]:
    errors = []
    for candidate in (path, Path(str(path) + "-journal"), Path(str(path) + "-wal"),
                      Path(str(path) + "-shm")):
        try:
            candidate.unlink(missing_ok=True)
        except BaseException as exc:
            errors.append(exc)
    return errors


def _release_spool(resource: _SpoolState) -> list[BaseException]:
    errors = []
    if resource.db is not None:
        try:
            resource.db.close()
        except BaseException as exc:
            errors.append(exc)
    if resource.handle is not None:
        try:
            resource.handle.close()
        except BaseException as exc:
            errors.append(exc)
    errors.extend(_remove_spool_files(resource.path))
    resource.errors = errors
    if not errors:
        resource.db = None
        resource.handle = None
    return errors


class _MemberSpool:
    def __init__(self, scope: _SpoolScope | None = None,
                 directory: Path | None = None) -> None:
        handle = tempfile.NamedTemporaryFile(
            prefix="sakurapool-members-", suffix=".sqlite", delete=False, dir=directory
        )
        self.scope = scope or _SpoolScope()
        self.resource = _SpoolState(Path(handle.name), handle)
        self.scope.add(self.resource)
        self.path = self.resource.path
        self.db: sqlite3.Connection | None = None
        try:
            handle.close()
            self.resource.handle = None
            self.db = sqlite3.connect(self.path)
            self.resource.db = self.db
            if getattr(self, "_ddl_injection", False):
                raise RuntimeError("ddl injection")
            self.db.execute(
                "CREATE TABLE members (key TEXT, name TEXT, suffix TEXT, "
                "offset INTEGER, size INTEGER, is_image INTEGER)"
            )
            self.db.execute("CREATE INDEX members_lookup ON members(key, is_image, name)")
            self.db.commit()
        except BaseException as primary:
            cleanup_errors = _release_spool(self.resource)
            for cleanup_error in cleanup_errors:
                primary.add_note(f"spool cleanup failed: {cleanup_error!r}")
            if cleanup_errors:
                primary.cleanup_handle = self.scope.handle
            else:
                self.scope.discard(self.resource)
            raise

    def add(self, key: str, member: tarfile.TarInfo, suffix: str, is_image: bool) -> None:
        self.db.execute("INSERT INTO members VALUES (?, ?, ?, ?, ?, ?)",
                        (key, member.name, suffix, member.offset_data, member.size, int(is_image)))

    def iter_keys(self):
        cursor = self.db.execute("SELECT DISTINCT key FROM members ORDER BY key")
        for (key,) in cursor:
            yield key

    def get(self, key: str, is_image: bool) -> list[tarfile.TarInfo]:
        result = []
        for name, suffix, offset, size, flag in self.db.execute(
            "SELECT name, suffix, offset, size, is_image FROM members "
            "WHERE key=? AND is_image=? ORDER BY name LIMIT 2", (key, int(is_image)),
        ):
            if bool(flag) == is_image:
                member = tarfile.TarInfo(name)
                member.offset_data, member.size = offset, size
                result.append(member)
        return result

    def close(self) -> None:
        errors = _release_spool(self.resource)
        if not errors:
            self.scope.discard(self.resource)
            self.db = None
        if errors:
            error = SpoolCleanupError("spool cleanup failed: " + repr(errors[0]))
            error.cleanup_handle = self.scope.handle
            raise error


class _SpoolRows:
    """Disk-backed row spool; only one bounded batch is materialized at a time."""
    def __init__(self, scope: _SpoolScope | None = None,
                 directory: Path | None = None) -> None:
        handle = tempfile.NamedTemporaryFile(
            prefix="sakurapool-spool-", suffix=".sqlite", delete=False, dir=directory
        )
        self.scope = scope or _SpoolScope()
        self.resource = _SpoolState(Path(handle.name), handle)
        self.scope.add(self.resource)
        self.path = self.resource.path
        self.db: sqlite3.Connection | None = None
        try:
            handle.close()
            self.resource.handle = None
            self.db = sqlite3.connect(self.path)
            self.resource.db = self.db
            if getattr(self, "_ddl_injection", False):
                raise RuntimeError("ddl injection")
            self.db.execute(
                "CREATE TABLE rows (kind TEXT, ordinal INTEGER, record_id TEXT, payload TEXT, "
                "PRIMARY KEY (kind, ordinal))"
            )
            self.db.execute("CREATE TABLE identities (record_id TEXT PRIMARY KEY, identity TEXT)")
            self.db.execute("CREATE INDEX rows_record_lookup ON rows(kind, record_id)")
            self.db.commit()
            self.counts = {name: 0 for name in SCHEMAS}
        except BaseException as primary:
            cleanup_errors = _release_spool(self.resource)
            for cleanup_error in cleanup_errors:
                primary.add_note(f"spool cleanup failed: {cleanup_error!r}")
            if cleanup_errors:
                primary.cleanup_handle = self.scope.handle
            else:
                self.scope.discard(self.resource)
            raise

    def __getitem__(self, name: str) -> _TableProxy:
        return _TableProxy(self, name)

    def register_identity(self, record_id: str, identity: RecordKey) -> None:
        try:
            self.db.execute(
                "INSERT INTO identities VALUES (?, ?)",
                (record_id, json.dumps(identity.__dict__, ensure_ascii=False)),
            )
        except sqlite3.IntegrityError as exc:
            raise IdentityConflictError(f"record_identity_conflict: {record_id}") from exc

    def append(self, name: str, row: dict[str, Any]) -> None:
        ordinal = self.counts[name]
        self.db.execute(
            "INSERT INTO rows VALUES (?, ?, ?, ?)",
            (name, ordinal, row.get("record_id"), json.dumps(row, ensure_ascii=False)),
        )
        self.counts[name] += 1
        if ordinal % BATCH_SIZE == BATCH_SIZE - 1:
            self.db.commit()

    def iter_batches(self, name: str):
        cursor = self.db.execute("SELECT payload FROM rows WHERE kind=? ORDER BY ordinal", (name,))
        while True:
            payloads = cursor.fetchmany(BATCH_SIZE)
            if not payloads:
                break
            yield [json.loads(payload) for (payload,) in payloads]

    def iter_rows(self, name: str):
        for batch in self.iter_batches(name):
            yield from batch

    def close(self) -> None:
        errors = _release_spool(self.resource)
        if not errors:
            self.scope.discard(self.resource)
            self.db = None
        if errors:
            error = SpoolCleanupError("spool cleanup failed: " + repr(errors[0]))
            error.cleanup_handle = self.scope.handle
            raise error


FORMAT_VERSION = 4
BUILDER = "sakurapool-p2-v4"
BATCH_SIZE = 1024


class SpoolCleanupError(OSError):
    """One or more owned spool resources could not be fully removed."""


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


def _read_member(archive: tarfile.TarFile, member: tarfile.TarInfo,
                 verify_offsets: bool = False) -> bytes:
    archive.fileobj.seek(member.offset_data)
    data = archive.fileobj.read(member.size)
    if len(data) != member.size:
        raise ValueError(f"short read for {member.name}")
    if verify_offsets:
        archive.fileobj.seek(member.offset_data)
        verified = archive.fileobj.read(member.size)
        if verified != data:
            raise ValueError(f"offset verification failed for {member.name}")
    return data


def _hash_member(archive: tarfile.TarFile, member: tarfile.TarInfo) -> str:
    digest = hashlib.sha256()
    archive.fileobj.seek(member.offset_data)
    remaining = member.size
    while remaining:
        block = archive.fileobj.read(min(1024 * 1024, remaining))
        if not block:
            raise ValueError(f"short read for {member.name}")
        digest.update(block)
        remaining -= len(block)
    return digest.hexdigest()


def _truncate_utf8(value: str, limit: int = 4096) -> str:
    data = value.encode("utf-8")[:limit]
    return data.decode("utf-8", errors="ignore")


def _next_member(archive: tarfile.TarFile) -> tarfile.TarInfo | None:
    member = archive.next()
    archive.members.clear()
    return member


def _safe(name: str) -> bool:
    return bool(name) and "\\" not in name and ":" not in name and all(
        p not in ("", ".", "..") for p in name.split("/")
    )


def _object_id(rel: str, validator_sha256: str) -> str:
    """Bind the logical shard path to its content version."""
    return f"{rel}@sha256-{validator_sha256}"


def _scan_shard_impl(path: Path | None, rel: str, adapter: DatasetAdapter, hash_images: bool,
                timings: dict[str, float], validator: dict[str, Any],
                verify_offsets: bool = False, scope: _SpoolScope | None = None,
                *, staged_archive: Any = None,
                spool_directory: Path | None = None) -> _SpoolRows:
    scope = scope or _SpoolScope()
    rows = _SpoolRows(scope, directory=spool_directory)

    object_id = _object_id(rel, validator["sha256"])
    rows["objects"].append(dict(
        storage_id=adapter.storage_id,
        backend="modelscope" if staged_archive is not None else "local",
        repo_type="dataset" if staged_archive is not None else "local",
        archive_format="tar", dataset=adapter.dataset, object_path=rel,
        object_size=validator["size"], object_version=validator["sha256"],
        validator=validator["sha256"], validator_kind="sha256",
        scan_status="committed", dataset_id=adapter.dataset, object_id=object_id,
        source=adapter.source, path=rel, size=validator["size"],
        validator_strength=validator["strength"], sha256=validator["sha256"],
    ))

    def error(member: str, code: str, detail: str = "", post_id: str | None = None,
              record_id: str | None = None) -> None:
        assert code in ERROR_CODES
        detail = _truncate_utf8(detail)
        rows["errors"].append(dict(
            dataset_id=adapter.dataset, object_id=object_id, source=adapter.source,
            object_path=rel, member=member, post_id=post_id, record_id=record_id, path=member,
            code=code, error_detail=detail, detail=detail,
        ))

    started = time.perf_counter()
    try:
        archive = staged_archive if staged_archive is not None else tarfile.open(path, "r:")
    except tarfile.ReadError as exc:
        raise UnsupportedArchiveError(f"unsupported_archive: {path}") from exc
    with archive:
        member_spool = _MemberSpool(scope, directory=spool_directory)
        member = _next_member(archive)
        while member is not None:
            if member.isdir():
                member = _next_member(archive)
                continue
            if not _safe(member.name):
                error(member.name, "unsupported_member", "unsafe member path")
                member = _next_member(archive)
                continue
            parts = PurePosixPath(member.name).parts
            if any(p.startswith(".") for p in parts) or parts[-1] in adapter.ignored_names:
                member = _next_member(archive)
                continue
            suffix = PurePosixPath(member.name).suffix.lower()
            if suffix != ".json" and suffix not in adapter.image_extensions:
                member = _next_member(archive)
                continue
            if not member.isfile() or member.issparse():
                error(member.name, "unsupported_member")
                member = _next_member(archive)
                continue
            if not 0 <= member.offset_data <= member.offset_data + member.size <= validator["size"]:
                raise ValueError(f"invalid TAR extent: {member.name}")
            key = adapter.pair_key(member.name)
            member_spool.add(key, member, suffix, suffix != ".json")
            member = _next_member(archive)
        member_spool.db.commit()
        timings["header_seconds"] += time.perf_counter() - started
        for key in member_spool.iter_keys():
            group = member_spool.get(key, True)
            jsons = member_spool.get(key, False)
            if len(group) > 1:
                error(key, "ambiguous_image_member")
            if len(jsons) > 1:
                error(key, "ambiguous_metadata_member")
            if len(group) > 1 or len(jsons) > 1:
                continue
            if not group:
                error(key, "missing_image")
                continue
            missing_required_metadata = not jsons and adapter.metadata_required
            post_id = PurePosixPath(key).name
            if not post_id or (adapter.numeric_post_id and not re.fullmatch(r"[0-9]+", post_id)):
                error(key, "invalid_post_id", post_id=post_id)
                continue
            image, meta = group[0], jsons[0] if jsons else None
            identity = RecordKey(adapter.dataset, object_id, key)
            record_id = identity.record_id
            rows.register_identity(record_id, identity)
            if missing_required_metadata:
                error(key, "missing_metadata", post_id=post_id, record_id=record_id)
            metadata = None
            started = time.perf_counter()
            try:
                if meta is not None:
                    if meta.size > adapter.max_json_bytes:
                        raise ValueError("metadata exceeds configured size limit")
                    metadata_payload = (
                        _read_member(archive, meta, verify_offsets)
                        if verify_offsets else _read_member(archive, meta)
                    )
                    metadata = json.loads(
                        metadata_payload,
                        parse_constant=lambda x: (_ for _ in ()).throw(
                            ValueError(f"non-finite JSON: {x}")
                        ),
                    )
                    if not isinstance(metadata, dict):
                        raise ValueError("metadata must be a JSON object")
                    _json(metadata)
                    if "source" in metadata and metadata["source"] != adapter.source:
                        error(meta.name, "source_mismatch", post_id=post_id, record_id=record_id)
                        continue
            except (ValueError, UnicodeError) as exc:
                error(meta.name, "metadata_invalid", str(exc), post_id=post_id,
                      record_id=record_id)
                continue
            finally:
                timings["json_seconds"] += time.perf_counter() - started
            values = metadata or {}
            width = values.get("width")
            height = values.get("height")
            has_alpha = values.get("has_alpha")
            if not isinstance(width, int) or width < 0 or width >= 2**32:
                width = None
            if not isinstance(height, int) or height < 0 or height >= 2**32:
                height = None
            if not isinstance(has_alpha, bool):
                has_alpha = None
            tags = values.get(adapter.tags_field)
            state = ("missing" if adapter.tags_field not in values else "invalid"
                     if not isinstance(tags, list) or any(not isinstance(t, str) for t in tags)
                     else "empty" if not tags else "known")
            tag_rows = [dict(value=t, category=adapter.tag_category) for t in tags] if state in (
                "known", "empty") else None
            digest = values.get("sha256")
            if digest is not None and (not isinstance(digest, str)
                                       or not re.fullmatch(r"[0-9a-fA-F]{64}", digest)):
                error(meta.name, "metadata_invalid", "invalid declared sha256",
                      post_id=post_id, record_id=record_id)
                digest = None
            digest = digest.lower() if digest else None
            origin = "declared:json.sha256" if digest else "missing"
            if hash_images:
                digest = (staged_archive.member_sha256(image.name)
                          if staged_archive is not None else _hash_member(archive, image))
                origin = "computed:sha256"
            text = values.get(adapter.text_field)
            if text is not None and not isinstance(text, str):
                error(meta.name, "metadata_invalid", "invalid text", post_id=post_id,
                      record_id=record_id)
                text = None
            identity_fields = dict(record_id=record_id, dataset_id=adapter.dataset,
                                   object_id=object_id, sample_path=key)
            rows["samples"].append(dict(
                **identity_fields, source=adapter.source, post_id=post_id, image_path=image.name,
                offset_data=image.offset_data, size=image.size,
                json_path=meta.name if meta else None,
                json_offset_data=meta.offset_data if meta else None,
                json_size=meta.size if meta else None,
                text=text, tags_state=state, tags=tag_rows, hash_source=origin, sha256=digest,
                image_format=PurePosixPath(image.name).suffix.lower().lstrip("."),
                width=width, height=height, has_alpha=has_alpha,
                hash_kind="sha256" if digest else None,
                status="indexed",
            ))
            rows["annotations"].append(dict(
                **identity_fields, namespace=adapter.tag_namespace,
                origin=f"declared:json.{adapter.tags_field}", tags_state=state, tags=tag_rows,
            ))
        member_spool.close()
    return rows


def _cleanup_spools(scope: _SpoolScope) -> list[BaseException]:
    return scope.cleanup()


def _scan_shard(path: Path, rel: str, adapter: DatasetAdapter, hash_images: bool,
                timings: dict[str, float], validator: dict[str, Any],
                verify_offsets: bool = False) -> _SpoolRows:
    scope = _SpoolScope()
    try:
        rows = _scan_shard_impl(path, rel, adapter, hash_images, timings, validator,
                                verify_offsets, scope)
    except BaseException as primary:
        cleanup_errors = _cleanup_spools(scope)
        if cleanup_errors:
            primary.cleanup_handle = scope.handle
        for cleanup_error in cleanup_errors:
            primary.add_note(f"spool cleanup failed: {cleanup_error!r}")
        raise
    return rows


def _table_from_rows(rows: list[dict], schema: pa.Schema) -> pa.Table:
    """Materialize one bounded Parquet batch, keeping the conversion seam testable."""
    return pa.Table.from_pylist(rows, schema)


def _validate_references(rows: _SpoolRows) -> None:
    object_row = next(rows.iter_rows("objects"), None)
    if object_row is None:
        raise ValueError("objects table must contain one ObjectRef")
    object_ref = (object_row["dataset_id"], object_row["object_id"])
    for name in ("samples", "annotations", "errors"):
        if any((row["dataset_id"], row["object_id"]) != object_ref
               for row in rows.iter_rows(name)):
            raise ValueError(f"{name} contains an ObjectRef outside objects")
    missing = rows.db.execute(
        "SELECT a.record_id FROM rows AS a "
        "LEFT JOIN rows AS s ON s.kind='samples' AND s.record_id=a.record_id "
        "WHERE a.kind='annotations' AND s.record_id IS NULL LIMIT 1"
    ).fetchone()
    if missing is not None:
        raise ValueError("annotations contains a RecordKey outside samples")


def _verify_fragment(path: Path, schema: pa.Schema) -> int:
    parquet = pq.ParquetFile(path)
    if parquet.schema_arrow != schema:
        raise ValueError(f"schema mismatch: {path.name}")
    metadata_rows = parquet.metadata.num_rows
    if metadata_rows == 0:
        return 0
    iterated_rows = sum(
        batch.num_rows for batch in parquet.iter_batches(batch_size=BATCH_SIZE)
    )
    if iterated_rows != metadata_rows:
        raise ValueError(f"row metadata mismatch: {path.name}")
    return metadata_rows


def _write_fragments(files: dict[str, Path], rows: _SpoolRows,
                     checkpoint: Callable[[str], None],
                     byte_limit: int | None = None) -> dict[str, dict]:
    info = {}
    written = 0
    class CappedStream:
        def __init__(self, target):
            self.target = target
        def write(self, data):
            nonlocal written
            if byte_limit is not None and written + len(data) > byte_limit:
                raise ValueError("durable fragment output byte cap reached")
            consumed = self.target.write(data)
            written += consumed
            return consumed
        def __getattr__(self, name):
            return getattr(self.target, name)
    for name, final in files.items():
        partial = final.with_name(final.name + ".partial")
        # Local P2 retains its original overwrite/recovery semantics. P4's
        # explicit byte-limit path requires a fresh partial and fails closed.
        with partial.open("xb" if byte_limit is not None else "wb") as raw:
            stream = CappedStream(raw) if byte_limit is not None else raw
            with pq.ParquetWriter(stream, SCHEMAS[name]) as writer:
                batches = rows.iter_batches(name)
                first = next(batches, None)
                if name == "samples":
                    if first:
                        writer.write_table(_table_from_rows(first, SCHEMAS[name]))
                    stream.flush()
                    os.fsync(stream.fileno())
                    checkpoint("A")  # samples partial lacks footer and remaining rows
                elif first:
                    writer.write_table(_table_from_rows(first, SCHEMAS[name]))
                for batch in batches:
                    writer.write_table(_table_from_rows(batch, SCHEMAS[name]))
            stream.flush()
            os.fsync(stream.fileno())
        actual_rows = _verify_fragment(partial, SCHEMAS[name])
        if actual_rows != rows.counts[name]:
            raise ValueError(f"schema/row mismatch before publication: {name}")
        info[name] = dict(path=final.name, sha256=_sha(partial),
                          bytes=partial.stat().st_size, rows=actual_rows)
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
         checkpoint: Callable[[str], None] | None = None,
         verify_offsets: bool = False) -> dict[str, Any]:
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
        object_id = _object_id(rel, validators[rel]["sha256"])
        marker = output / f"{shard_id}.COMMIT"
        expected_markers.add(marker.name)
        files = {name: output / f"{shard_id}.{name}.parquet" for name in SCHEMAS}
        if marker.exists():
            try:
                commit = json.loads(marker.read_bytes())
                if commit["contract_sha256"] != hashlib.sha256(_json(contract)).hexdigest():
                    raise ValueError("contract hash mismatch")
                if (commit["input"] != validators[rel] or commit["object_id"] != object_id
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
                    if _verify_fragment(fragment, SCHEMAS[name]) != info["rows"]:
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
            rows = _scan_shard(
                path, rel, adapter, hash_images, timings, validators[rel], verify_offsets
            )
            primary = None
            try:
                _validate_references(rows)
                started_parquet = time.perf_counter()
                info = _write_fragments(files, rows, checkpoint)
                timings["parquet_seconds"] += time.perf_counter() - started_parquet
            except BaseException as exc:
                primary = exc
                raise
            finally:
                try:
                    rows.close()
                except BaseException as cleanup_error:
                    if primary is not None:
                        primary.add_note(f"row spool cleanup failed: {cleanup_error!r}")
                        primary.cleanup_handle = rows.scope.handle
                    else:
                        cleanup_error.cleanup_handle = rows.scope.handle
                        raise
            if _validator(path) != validators[rel]:
                raise ValueError("input validator mismatch: input changed during scan")
            commit = dict(
                schema=FORMAT_VERSION, builder=BUILDER, dataset_id=dataset, object_id=object_id,
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
