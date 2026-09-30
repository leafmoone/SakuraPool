"""Incremental trusted-worker scan sidecars -> existing stage schema.

Remote image hashes/whole SHA originate in the same Rust scanner read. Discarded
image bytes cannot be independently rehashed in Python; metadata is independently
hashed here and sidecars/footer/counts/extents are strictly cross-validated.
Download additionally audits every retained TAR extent and whole digest.
"""

import hashlib
import json
import re
import sqlite3
from posixpath import splitext

from ..indexer import _safe
from .production_resources import (
    FOOTER_CAP,
    FORMAT,
    JSON_CAP,
    LINE_CAP,
    MAX_MEMBERS,
    METADATA_CAP,
    PATH_CAP,
    RECORD_CAP,
    STAGE_PAGES,
)
from .rust_index import FileArchive, RustScanAuditError, _digest, _file_sha256, _write_stage

_SHA = re.compile(r"[0-9a-f]{64}\Z")


def _json_bounded(payload):
    # Lexical admission BEFORE decoder allocations. At most 32 nested containers
    # and 32k punctuation/string/scalar starts, plus <=1MiB total string bytes.
    # Invalid syntax is still rejected by the standard decoder afterward.
    depth = tokens = 0
    quoted = escaped = False
    previous = 32
    for byte in payload:
        if quoted:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                quoted = False
            continue
        if byte == 34:
            quoted = True
            tokens += 1
        elif byte in (123, 91):
            depth += 1
            tokens += 1
        elif byte in (125, 93):
            depth -= 1
        elif byte in (44, 58):
            tokens += 1
        elif previous in (32, 9, 10, 13, 91, 123, 44, 58) and byte not in (32, 9, 10, 13):
            tokens += 1
        if depth > 32 or tokens > 32768 or depth < 0:
            raise RustScanAuditError("metadata JSON structural bound")
        previous = byte
    if quoted or depth:
        raise RustScanAuditError("metadata JSON truncated")
    return json.loads(payload)


def build_stage_from_sidecars(root, result, bound, adapter, stage_dir, ledger, *, stage_lease):
    report = root / "scan.json"
    metadata = root / "metadata.bin"
    archive = FileArchive(root / "body") if (root / "body").exists() else None
    if (
        result.get("format") != FORMAT
        or result.get("bytes") != bound.size
        or not isinstance(result.get("sha256"), str)
        or not _SHA.fullmatch(result["sha256"])
    ):
        raise RustScanAuditError("production scan summary identity")
    for path, size_key, hash_key, cap in (
        (report, "report_bytes", "report_sha256", RECORD_CAP + FOOTER_CAP),
        (metadata, "metadata_bytes", "metadata_sha256", METADATA_CAP),
    ):
        source = FileArchive(path)
        if (
            type(result.get(size_key)) is not int
            or len(source) != result[size_key]
            or len(source) > cap
            or _file_sha256(path) != result.get(hash_key)
        ):
            raise RustScanAuditError("production sidecar digest/size")
    if archive is not None and (
        len(archive) != bound.size or _digest(archive, 0, len(archive)) != result["sha256"]
    ):
        raise RustScanAuditError("download spool digest/size")
    scan = {"whole_sha256": result["sha256"]}

    def rows(db):
        # One capped database, no O(member-count) Python set or second scratch file.
        db.execute("CREATE TABLE audited_names(name TEXT PRIMARY KEY) WITHOUT ROWID")
        count = metadata_end = record_bytes = 0
        previous_end = 512
        footer = None
        with report.open("rb") as records, metadata.open("rb") as payloads:
            while True:
                line = records.readline(LINE_CAP + 1)
                if not line:
                    break
                if len(line) > LINE_CAP or not line.endswith(b"\n"):
                    raise RustScanAuditError("production record line bound")
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise RustScanAuditError("production record schema")
                if "complete" in value:
                    footer = value
                    if (
                        len(line) > FOOTER_CAP
                        or type(value["complete"]) is not bool
                        or records.read(1)
                    ):
                        raise RustScanAuditError("production terminal footer")
                    break
                record_bytes += len(line)
                count += 1
                if record_bytes > RECORD_CAP or count > MAX_MEMBERS:
                    raise RustScanAuditError("production record total bound")
                kind = value.get("kind")
                keys = {"path", "kind", "offset", "size", "metadata_offset"}
                if kind == "file":
                    keys.add("sha256")
                if set(value) != keys or kind not in ("file", "dir"):
                    raise RustScanAuditError("production member schema")
                name, offset, size = value["path"], value["offset"], value["size"]
                if (
                    not isinstance(name, str)
                    or not _safe(name)
                    or len(name.encode()) > PATH_CAP
                    or name.startswith("/")
                    or ".." in name.split("/")
                    or type(offset) is not int
                    or type(size) is not int
                    or offset % 512
                    or offset < previous_end
                    or size < 0
                    or offset + size > bound.size
                ):
                    raise RustScanAuditError("production member geometry/path")
                previous_end = ((offset + size + 511) // 512) * 512 + 512
                try:
                    db.execute("INSERT INTO audited_names VALUES (?)", (name,))
                except sqlite3.IntegrityError:
                    raise RustScanAuditError("duplicate production member") from None
                suffix = splitext(name)[1].lower()
                position = value["metadata_offset"]
                if kind == "dir":
                    if size != 0 or position is not None:
                        raise RustScanAuditError("production directory geometry")
                    continue
                digest = value["sha256"]
                if not isinstance(digest, str) or not _SHA.fullmatch(digest):
                    raise RustScanAuditError("production member digest")
                if archive is not None and _digest(archive, offset, size) != digest:
                    raise RustScanAuditError("download member digest")
                payload = None
                if suffix == ".json":
                    if (
                        type(position) is not int
                        or position != metadata_end
                        or not 0 < size <= min(JSON_CAP, adapter.max_json_bytes)
                        or position + size > result["metadata_bytes"]
                    ):
                        raise RustScanAuditError("production metadata extent")
                    payload = payloads.read(size)
                    if len(payload) != size or hashlib.sha256(payload).hexdigest() != digest:
                        raise RustScanAuditError("production metadata digest")
                    _json_bounded(payload)
                    metadata_end += size
                elif position is not None:
                    raise RustScanAuditError("image retained in metadata sidecar")
                if suffix in adapter.image_extensions or suffix == ".json":
                    if size == 0:
                        raise RustScanAuditError("empty selected member")
                    yield (
                        name,
                        "json" if suffix == ".json" else "image",
                        offset,
                        size,
                        digest,
                        payload,
                    )
        expected = {
            "complete": True,
            "format": FORMAT,
            "whole_sha256": result["sha256"],
            "size": bound.size,
            "member_count": count,
            "trailing_bytes": result.get("trailing_bytes"),
            "metadata_bytes": metadata_end,
        }
        if (
            footer != expected
            or type(result.get("member_count")) is not int
            or result["member_count"] != count
            or metadata_end != result["metadata_bytes"]
            or type(result.get("trailing_bytes")) is not int
            or not 512 <= result["trailing_bytes"] <= 65536
        ):
            raise RustScanAuditError("production completion/count binding")
        db.execute("DROP TABLE audited_names")

    return _write_stage(
        scan,
        rows,
        bound,
        adapter,
        stage_dir,
        ledger,
        _stage_lease=stage_lease,
        _max_pages=STAGE_PAGES,
    )
