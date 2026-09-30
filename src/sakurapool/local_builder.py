"""Single-pass local Rust partition builder, independent of P4 remote budgets.

TARs are never deleted. Only builder-owned, uncommitted fragment files recorded
in a recovery receipt can be replaced on resume. Unknown files fail closed.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from . import indexer
from .partition import PartitionManifest
from .registry import DatasetAdapter
from .runtime import RUNTIME_FORMAT_VERSION
from .runtime.inventory import (
    _check_row_ownership,
    _validate_fragment,
    load_p2_inventory,
)
from .storage.remote_index import StagedArchive
from .storage.rust_bridge import RustWorker
from .storage.rust_index import build_stage_from_file_scan
from .storage.transport import BoundObject

PROVENANCE_KEYS = {"sakurapool_code_sha", "rust_worker_version", "rust_worker_sha256",
                   "runtime_format_version", "durable_format_version", "adapter_contract_version"}


def validate_build_metadata(value: dict[str, Any]) -> None:
    if not isinstance(value, dict) or set(value) != PROVENANCE_KEYS:
        raise ValueError("invalid build metadata keys")
    for key, length in (("sakurapool_code_sha", 40), ("rust_worker_sha256", 64)):
        if not isinstance(value[key], str) or not re.fullmatch(f"[0-9a-f]{{{length}}}", value[key]):
            raise ValueError(f"invalid {key}")
    if not isinstance(value["rust_worker_version"], str) or not value["rust_worker_version"]:
        raise ValueError("missing worker version")
    # P2 provenance records the build-time runtime, not the current reader format.
    if (value["runtime_format_version"] not in (1, RUNTIME_FORMAT_VERSION) or
            value["durable_format_version"] != indexer.FORMAT_VERSION or
            value["adapter_contract_version"] != "nested_json_v1"):
        raise ValueError("unsupported build format provenance")


def _safe_directory(path: Path) -> None:
    for parent in (path, *path.parents):
        if parent.is_symlink() or (hasattr(parent, "is_junction") and parent.is_junction()):
            raise ValueError("builder path contains a symlink/junction")


def _file_identity(path: Path) -> tuple[int, ...] | None:
    if path.is_symlink() or not path.is_file():
        return None
    stat = path.stat()
    return (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns, stat.st_dev, stat.st_ino)


def _clean_stage(path: Path) -> None:
    """Delete only exact receipt-owned stage artifacts, including SQLite crash journal."""
    _safe_directory(path)
    if not path.exists():
        return
    known = {"members.sqlite", "members.sqlite-journal", "stage.complete"}
    if not path.is_dir() or any(
            p.name not in known or not p.is_file() or p.is_symlink()
            for p in path.iterdir()):
        raise ValueError("unknown/unsafe temporary stage files; retained")
    for name in known:
        file = path / name
        if file.exists():
            file.unlink()
    path.rmdir()


def _verify_object(output: Path, rel: str, adapter: DatasetAdapter,
                   contract: dict[str, Any]) -> dict[str, Any]:
    shard = hashlib.sha256(indexer._json([adapter.dataset, rel])).hexdigest()
    marker = output / f"{shard}.COMMIT"
    if not marker.is_file() or marker.is_symlink():
        raise ValueError("missing or unsafe object COMMIT")
    commit = json.loads(marker.read_bytes())
    actual = commit["input"]
    planned = contract["inputs"][rel]
    if (set(commit) != {"schema", "builder", "dataset_id", "object_id", "input", "files",
                       "contract_sha256", "created_at"} or
            commit["schema"] != indexer.FORMAT_VERSION or commit["builder"] != indexer.BUILDER or
            commit["dataset_id"] != adapter.dataset or
            commit["contract_sha256"] != hashlib.sha256(indexer._json(contract)).hexdigest() or
            dict(actual, sha256=planned["sha256"]) != planned or
            (planned["sha256"] is not None and actual["sha256"] != planned["sha256"]) or
            not re.fullmatch(r"[0-9a-f]{64}", actual["sha256"]) or
            commit["object_id"] != f"{rel}@sha256-{actual['sha256']}" or
            set(commit["files"]) != set(indexer.SCHEMAS)):
        raise ValueError("object COMMIT contract mismatch")
    for name, info in commit["files"].items():
        path = output / f"{shard}.{name}.parquet"
        if path.is_symlink() or not path.is_file() or info["path"] != path.name:
            raise ValueError("unsafe object fragment")
        _validate_fragment(path, name, info)
        if name != "errors":
            _check_row_ownership(path, adapter.dataset, commit["object_id"])
    return commit


def build_partition(manifest: PartitionManifest, adapter: DatasetAdapter,
                    worker_path: Path, work_dir: Path, output: Path, *, code_sha: str,
                    completion: Callable[[dict[str, Any]], None] | None = None,
                    checkpoint: Callable[[str], None] | None = None) -> dict[str, Any]:
    """Build/resume up to 32 objects, retaining physical duplicate post ids."""
    if (manifest.dataset != adapter.dataset or manifest.source != adapter.source or
            adapter.metadata_mode != "nested_json_v1"):
        raise ValueError("partition and nested adapter identity mismatch")
    if not re.fullmatch(r"[0-9a-f]{40}", code_sha):
        raise ValueError("a full code SHA is required")
    worker_path, work_dir, output = map(Path, (worker_path, work_dir, output))
    work_dir, output = work_dir.absolute(), output.absolute()
    _safe_directory(work_dir)
    _safe_directory(output)
    if output == work_dir or output.is_relative_to(work_dir) or work_dir.is_relative_to(output):
        raise ValueError("work and durable output must be disjoint")
    for obj in manifest.objects:
        _safe_directory(obj.local_path)
        if obj.local_path.is_relative_to(output) or obj.local_path.is_relative_to(work_dir):
            raise ValueError("TAR inputs must be outside work/durable roots")
    work_dir.mkdir(parents=True, exist_ok=True)
    output.mkdir(parents=True, exist_ok=True)
    # OS-held locks release on process death; a stale file never blocks resume.
    lock = output.parent / ("." + output.name + ".local-builder.lock")
    _safe_directory(lock)
    with lock.open("a+b") as handle:
        if handle.tell() == 0:
            handle.write(b"L")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            return _build_locked(manifest, adapter, worker_path, work_dir, output,
                                 code_sha, completion, checkpoint or (lambda _: None))
        finally:
            handle.seek(0)
            if os.name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _build_locked(manifest, adapter, worker_path, work_dir, output,
                  code_sha, completion, checkpoint):
    started = time.perf_counter()
    receipts = []
    stats = {name: 0 for name in indexer.SCHEMAS}
    # Durable INPUT deliberately excludes local paths and remote signed URLs.
    validators = {obj.repository_path: {"size": obj.declared_size, "mtime_ns": 0,
                  "sha256": obj.provider_sha256, "strength": "strong:sha256", "version": 1}
                  for obj in manifest.objects}
    worker_budget = {"body": sum(obj.declared_size for obj in manifest.objects),
                     "disk": 0, "inflight": 16 * 1024 * 1024, "attempts": 32}
    with RustWorker(worker_path, job_budget=worker_budget, timeout_s=3600) as worker:
        metadata = {"sakurapool_code_sha": code_sha, "rust_worker_version": worker.worker_version,
                    "rust_worker_sha256": indexer._sha(worker_path),
                    "runtime_format_version": RUNTIME_FORMAT_VERSION,
                    "durable_format_version": indexer.FORMAT_VERSION,
                    "adapter_contract_version": "nested_json_v1"}
        validate_build_metadata(metadata)
        contract = {"format_version": indexer.FORMAT_VERSION, "builder": indexer.BUILDER,
                    "adapter": adapter.to_dict(), "hash_images": True, "inputs": validators,
                    "partition_manifest": manifest.durable_plan(), "build_metadata": metadata}
        input_path = output / "INPUT.json"
        recovery = work_dir / (hashlib.sha256(str(output).encode()).hexdigest() + ".recovery.json")
        expected_recovery = {"format": "sakurapool-local-recovery-v2", "output": str(output),
                             "contract_sha256": hashlib.sha256(indexer._json(contract)).hexdigest(),
                             "stages": {rel: "object-temporary-" +
                                 hashlib.sha256(indexer._json(
                                     [str(output), adapter.dataset, rel])).hexdigest()
                                 for rel in validators},
                             "stage_files": ["members.sqlite", "members.sqlite-journal",
                                             "stage.complete"]}
        if recovery.exists():
            if recovery.is_symlink() or json.loads(recovery.read_bytes()) != expected_recovery:
                raise ValueError("recovery receipt mismatch")
        elif any(output.iterdir()):
            raise ValueError("existing output without builder-owned recovery receipt")
        else:
            for stage_name in expected_recovery["stages"].values():
                stage_path = work_dir / stage_name
                _safe_directory(stage_path)
                if stage_path.exists():
                    raise ValueError("existing temporary stage without ownership receipt")
            indexer._atomic(recovery, indexer._json(expected_recovery))
        if input_path.exists():
            if input_path.is_symlink() or json.loads(input_path.read_bytes()) != contract:
                raise ValueError("fixed INPUT/manifest/code contract changed")
        else:
            indexer._atomic(input_path, indexer._json(contract))
        allowed = {"INPUT.json"}
        for rel in validators:
            shard = hashlib.sha256(indexer._json([adapter.dataset, rel])).hexdigest()
            allowed.update((f"{shard}.COMMIT", f"{shard}.COMMIT.partial"))
            for name in indexer.SCHEMAS:
                allowed.update((f"{shard}.{name}.parquet", f"{shard}.{name}.parquet.partial"))
        if any(p.name not in allowed or not p.is_file() or p.is_symlink()
               for p in output.iterdir()):
            raise ValueError("unknown/unsafe durable files; fail closed")
        for obj in manifest.objects:
            rel = obj.repository_path
            shard = hashlib.sha256(indexer._json([adapter.dataset, rel])).hexdigest()
            marker = output / f"{shard}.COMMIT"
            marker_partial = output / f"{shard}.COMMIT.partial"
            temporary = work_dir / expected_recovery["stages"][rel]
            scan_seconds = 0.0
            if marker.exists():
                commit = _verify_object(output, rel, adapter, contract)
                if marker_partial.exists() or any(
                        (output / f"{shard}.{name}.parquet.partial").exists()
                        for name in indexer.SCHEMAS):
                    raise ValueError("committed object has unexpected partial files")
                _clean_stage(temporary)
                reused = True
            else:
                # Exact known shard outputs are ours (receipt + fixed INPUT).
                # No directory glob deletion and no unknown file deletion.
                _clean_stage(temporary)
                if marker_partial.exists():
                    marker_partial.unlink()
                for name in indexer.SCHEMAS:
                    for suffix in (".parquet", ".parquet.partial"):
                        path = output / f"{shard}.{name}{suffix}"
                        if path.exists():
                            path.unlink()
                if (not obj.local_path.is_file() or
                        obj.local_path.stat().st_size != obj.declared_size):
                    raise ValueError("local TAR missing or declared size mismatch")
                before = _file_identity(obj.local_path)
                scan_started = time.perf_counter()
                scan = worker.request("scan_tar", budget={"body": obj.declared_size,
                                      "disk": 0, "inflight": 16 * 1024 * 1024, "attempts": 1},
                                      payload={"path": str(obj.local_path),
                                               "max_bytes": obj.declared_size,
                                               "max_members": 100_000})
                scan_seconds = time.perf_counter() - scan_started
                if before != _file_identity(obj.local_path):
                    raise ValueError("TAR changed during Rust scan")
                digest = scan["whole_sha256"]
                if scan["size"] != obj.declared_size or (obj.provider_sha256 is not None
                                                         and digest != obj.provider_sha256):
                    raise ValueError("Rust whole SHA/size differs from provider validator")
                actual = dict(validators[rel], sha256=digest)
                # Content binding here is local only; not a remote revision claim.
                bound = BoundObject("file://local", obj.declared_size,
                                    strong_etag='"' + digest + '"')
                stage = build_stage_from_file_scan(scan, obj.local_path, bound, adapter, temporary)
                files = {name: output / f"{shard}.{name}.parquet" for name in indexer.SCHEMAS}
                scope = indexer._SpoolScope()
                try:
                    rows = indexer._scan_shard_impl(None, rel, adapter, True,
                        {"header_seconds": 0., "json_seconds": 0.}, actual,
                        scope=scope, staged_archive=StagedArchive(stage),
                        staged_provenance=("local", "local"), spool_directory=work_dir)
                    try:
                        indexer._validate_references(rows)
                        info = indexer._write_fragments(files, rows, checkpoint, byte_limit=2**32)
                    finally:
                        rows.close()
                finally:
                    scope.cleanup()
                commit = {"schema": indexer.FORMAT_VERSION, "builder": indexer.BUILDER,
                          "dataset_id": adapter.dataset, "object_id": f"{rel}@sha256-{digest}",
                          "created_at": datetime.now(timezone.utc).isoformat(), "input": actual,
                          "files": info, "contract_sha256": expected_recovery["contract_sha256"]}
                indexer._atomic(marker, indexer._json(commit))
                commit = _verify_object(output, rel, adapter, contract)
                checkpoint("COMMIT")
                _clean_stage(temporary)
                reused = False
            for name in stats:
                stats[name] += commit["files"][name]["rows"]
            # The receipt digest identifies COMMIT, not an unscanned current local TAR.
            local_identity = _file_identity(obj.local_path)
            verified_local = not reused and local_identity is not None and local_identity == before
            receipt = {"SAFE_TO_RELEASE_LOCAL_OBJECT": verified_local, "repository_path": rel,
                       "local_path": str(obj.local_path),
                       "content_sha256": commit["input"]["sha256"],
                       "content_sha256_scope": "committed_object",
                       "local_content_verification": ("VERIFIED" if verified_local else
                           "NOT_AVAILABLE" if local_identity is None else "UNKNOWN"),
                       "provider_validator": ("UNKNOWN" if obj.provider_sha256 is None
                                              else "VERIFIED"),
                       "reused_commit": reused, "scan_wall_seconds": scan_seconds}
            receipts.append(receipt)
            if completion:
                completion(receipt)
    load_p2_inventory(output)
    return {"dataset": adapter.dataset, "source": adapter.source, "partition": manifest.partition,
            "counts": stats, "objects": receipts, "wall_seconds": time.perf_counter() - started,
            "build_metadata": metadata}
