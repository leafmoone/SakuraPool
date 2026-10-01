"""Read-only P4 local index-package format 1 audit and snapshot binding.

No network calls or mutable URL is serialized here. A package is only valid
when *all* dependent files have canonical paths, plain regular-file types,
exact sizes and SHA-256 hashes. It never rescans a TAR or infers paths from a
record ID; published local/v4 legacy objects are not silently reinterpreted.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.parquet as pq

from .. import __version__ as _CODE_VERSION
from .. import indexer
from ..runtime.compiler import HAS_METADATA
from ..runtime.inventory import load_p2_inventory
from ..runtime.snapshot import RuntimeSnapshot
from .budget import DEFAULT_WORK_ROOT, BudgetExceeded, BudgetLedger, Reservation, is_reparse
from .location_gate import RepositoryConfigError, parse_repository
from .retrieval import AuditedSample, Extent, fetch_bound_sample
from .transport import BoundObject, GuardedTransport, condition_binding_key

_SHA = re.compile(r"[0-9a-f]{64}\Z")
_RECORD = re.compile(r"[0-9a-f]{32}\Z")
_REPO = "leafmoone/game_cg_5M"
_MAX_MANIFEST = 64 * (1 << 20)
_MAX_CANARY_JSON = 1 << 20
_BINDING_MODE = "strong_etag_if_match"
_SCAN_CONFIG = "audit/scan-config.json"
_SCAN_EVIDENCE = "audit/members.json"


class PackageCorrupt(ValueError):
    pass


def _relative(raw: object) -> str:
    if (not isinstance(raw, str) or not raw or raw.startswith("/") or "\\" in raw
            or ":" in raw or "\x00" in raw or len(raw.encode("utf-8")) > 4096
            or any(p in ("", ".", "..") for p in raw.split("/"))
            or PurePosixPath(raw).as_posix() != raw):
        raise PackageCorrupt("unsafe package relative path")
    return raw


def _file(root: Path, relative: str) -> Path:
    path = root
    for component in relative.split("/"):
        path = path / component
        if path.is_symlink() or (hasattr(path, "is_junction") and path.is_junction()):
            raise PackageCorrupt("symlink or junction inside package")
    if not stat.S_ISREG(path.stat(follow_symlinks=False).st_mode):
        raise PackageCorrupt("package entry must be a regular file")
    return path


def _bounded_json(path: Path, limit: int) -> object:
    """Bound JSON allocations on package inputs without changing P2/P3 readers."""
    with path.open("rb") as stream:
        before = stream.seek(0, 2)
        if before > limit:
            raise PackageCorrupt("package JSON exceeds canary verification limit")
        stream.seek(0)
        raw = stream.read(limit + 1)
        after = stream.seek(0, 2)
    if len(raw) != before or before != after or len(raw) > limit:
        raise PackageCorrupt("package JSON changed or exceeds canary limit")
    try:
        return json.loads(raw)
    except (ValueError, UnicodeError):
        raise PackageCorrupt("invalid package JSON") from None


def _preflight_durable_json(root: Path) -> None:
    """Limit all P2 JSON reads on a trusted, static canary package.

    P2's existing loader reads both INPUT and extensionless COMMIT markers
    wholly; it has no size option. Run this check immediately before *each*
    package-owned loader call. This is not a concurrent-writer guarantee.
    """
    directory = root / "durable"
    if (not directory.is_dir() or directory.is_symlink()
            or (hasattr(directory, "is_junction") and directory.is_junction())):
        raise PackageCorrupt("missing or unsafe durable directory")
    input_file = _file(root, "durable/INPUT.json")
    if input_file.stat().st_size > _MAX_CANARY_JSON:
        raise PackageCorrupt("durable INPUT exceeds bounded canary JSON size")
    for entry in directory.iterdir():
        if entry.name.endswith(".COMMIT"):
            marker = _file(root, "durable/" + entry.name)
            if marker.stat().st_size > _MAX_CANARY_JSON:
                raise PackageCorrupt("durable COMMIT exceeds bounded canary JSON size")


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class Binding:
    dataset_id: str
    storage_id: str
    object_id: str
    path: str
    size: int
    content_sha256: str
    strong_etag: str
    conditional_verified: bool
    condition_probe_sha256: str
    legacy_local_binding_verified: bool


@dataclass
class PackageManifest:
    root: Path
    snapshot_id: str
    repo_id: str
    data_revision: str
    endpoint: str
    runtime: str
    durable: str
    bindings: dict[tuple[str, str, str], Binding]
    audit: dict[str, dict]

    def sample(self, snapshot: RuntimeSnapshot, record_id: str) -> tuple[Binding, AuditedSample]:
        """Resolve RID only from this exact snapshot, then cross-check audit."""
        if snapshot.snapshot_id != self.snapshot_id:
            raise PackageCorrupt("runtime snapshot differs from package")
        if not isinstance(record_id, str) or not _RECORD.fullmatch(record_id):
            raise PackageCorrupt("invalid record ID")
        row = snapshot._catalog.execute(
            "SELECT r.rid, d.name FROM records r JOIN datasets d "
            "ON r.dataset_id=d.dataset_id WHERE r.record_id=?",
            (bytes.fromhex(record_id),)).fetchone()
        if row is None:
            raise PackageCorrupt("record not present in pinned runtime snapshot")
        rid, dataset_id = row
        location = snapshot.location(rid)
        obj = snapshot.object_ref(location["object_idx"])
        identity = (dataset_id, obj["storage_id"], obj["object_id"])
        binding = self.bindings.get(identity)
        if binding is None or (obj["object_path"], obj["object_size"],
                               obj["object_version"], obj["validator"]) != (
                                   binding.path, binding.size,
                                   binding.content_sha256, binding.content_sha256):
            raise PackageCorrupt("runtime object identity/content differs from binding")
        remote_ref = obj["backend"] == "modelscope" and obj["repo_type"] == "dataset"
        explicit_legacy = (obj["backend"] == "local" and obj["repo_type"] == "local"
                           and binding.legacy_local_binding_verified)
        if obj["archive_format"] != "tar" or not (remote_ref or explicit_legacy):
            raise PackageCorrupt("legacy/local object lacks explicit verified binding")
        audit = self.audit.get(record_id)
        if not isinstance(audit, dict) or audit.get("identity") != list(identity):
            raise PackageCorrupt("missing or mismatched scan audit record")
        try:
            if (set(audit) != {"identity", "image_path", "json_path",
                               "image", "json", "suffix"}
                    or not isinstance(audit["image_path"], str)
                    or not audit["image_path"]
                    or (audit["json_path"] is None) != (audit["json"] is None)):
                raise ValueError("member path/presence mismatch")
            image = Extent(**audit["image"])
            meta = Extent(**audit["json"]) if audit["json"] is not None else None
            sample = AuditedSample(record_id, image, meta, audit["suffix"])
            sample.validate(binding.size)
        except (KeyError, TypeError, ValueError):
            raise PackageCorrupt("invalid member audit") from None
        if (image.offset, image.size) != (location["image_offset"],
                                          location["image_size"]):
            raise PackageCorrupt("image extent differs from runtime snapshot")
        present = bool(location["flags"] & HAS_METADATA)
        if present != (meta is not None):
            raise PackageCorrupt("runtime metadata presence differs from member audit")
        if meta is not None and (meta.offset, meta.size) != (
                location["metadata_offset"], location["metadata_size"]):
            raise PackageCorrupt("metadata extent differs from runtime snapshot")
        return binding, sample


def _index_config(contract: dict, endpoint: str, revision: str) -> dict:
    """Canonical evidence of the *actual P2 index INPUT*, not provider proof."""
    return {"kind": "p4-index-input-config-v1",
            "producer": "sakurapool-p4-remote-stream",
            "adapter": contract["adapter"], "hash_images": contract["hash_images"],
            "inputs": contract["inputs"], "endpoint": endpoint,
            "data_revision": revision, "binding_mode": _BINDING_MODE}


def _validate_package_chain(root: Path, manifest: dict, names: set[str],
                            bindings: dict[tuple[str, str, str], Binding],
                            audit: dict) -> None:
    """Recheck durable-v4, runtime source and each bounded canary audit row.

    Parquet row batches are consumed one by one, never a whole sample table.
    This is an O(package files + sample rows) package open, not O(1).
    """
    try:
        _preflight_durable_json(root)
        inventory = load_p2_inventory(root / manifest["durable"])
        objects = {(obj.dataset_id, obj.object_id): obj for obj in inventory.objects}
        expected = {(key[0], key[2]) for key in bindings}
        if len(objects) != len(bindings) or set(objects) != expected:
            raise PackageCorrupt("durable objects differ from exact package bindings")
        config_path = _file(root, manifest["scan_config"])
        if (_hash(config_path) != manifest["scan_config_sha256"]
                or config_path.stat().st_size > _MAX_MANIFEST):
            raise PackageCorrupt("index input configuration fingerprint mismatch")
        if _bounded_json(config_path, _MAX_CANARY_JSON) != _index_config(
                inventory.contract, manifest["endpoint"], manifest["data_revision"]):
            raise PackageCorrupt("index input configuration differs from durable INPUT")
        if (inventory.contract["hash_images"] is not True
                or inventory.contract["adapter"].get("storage_id")
                != next(iter(bindings.values())).storage_id):
            raise PackageCorrupt("durable adapter is not the remote staged source")
        declared = {"durable/INPUT.json"}
        for (dataset, object_id), obj in objects.items():
            binding = next(value for (ds, _store, oid), value in bindings.items()
                           if (ds, oid) == (dataset, object_id))
            validator = obj.input
            if (validator.get("sha256"), validator.get("size"),
                    validator.get("mtime_ns")) != (
                    binding.content_sha256, binding.size, 0):
                raise PackageCorrupt("durable object does not match content binding")
            for fragment in obj.fragments:
                declared.add("durable/" + fragment.path.name)
            shard = hashlib.sha256(indexer._json([dataset, binding.path])).hexdigest()
            declared.add("durable/" + shard + ".COMMIT")
        if {name for name in names if name.startswith("durable/")} != declared:
            raise PackageCorrupt("durable inventory differs from committed v4 files")
        with RuntimeSnapshot.open(root / manifest["runtime"], full_verify=True) as snapshot:
            if (snapshot.snapshot_id != manifest["snapshot_id"]
                    or snapshot.manifest.get("source_fingerprint")
                    != inventory.source_fingerprint
                    or snapshot.manifest.get("object_count") != len(objects)):
                raise PackageCorrupt("runtime snapshot does not match durable inputs")
            runtime_objects = set()
            index_keys: dict[int, tuple[str, str, str]] = {}
            catalog_objects = snapshot._catalog.execute(
                "SELECT object_idx, dataset_id FROM objects ORDER BY object_idx").fetchall()
            if len(catalog_objects) != len(objects):
                raise PackageCorrupt("runtime object count differs from durable")
            for idx, dataset_name in catalog_objects:
                ref = snapshot.object_ref(idx)
                key = (dataset_name, ref["storage_id"], ref["object_id"])
                binding = bindings.get(key)
                if (binding is None or ref["object_path"] != binding.path
                        or ref["object_size"] != binding.size
                        or ref["object_version"] != binding.content_sha256
                        or ref["validator"] != binding.content_sha256
                        or ref["backend"] != "modelscope"
                        or ref["repo_type"] != "dataset"):
                    raise PackageCorrupt("runtime object differs from durable binding")
                runtime_objects.add(key)
                if idx in index_keys:
                    raise PackageCorrupt("duplicate runtime object index")
                index_keys[idx] = key
            if runtime_objects != set(bindings):
                raise PackageCorrupt("runtime has missing or duplicate bound objects")
            observed: set[str] = set()
            for obj in inventory.objects:
                sample_fragment = next(f for f in obj.fragments if f.name == "samples")
                parquet = pq.ParquetFile(sample_fragment.path)
                if parquet.num_row_groups == 0:
                    parquet.close()
                    continue
                for batch in parquet.iter_batches(batch_size=256, columns=(
                        "record_id", "dataset_id", "object_id", "image_path",
                        "json_path", "offset_data", "size", "json_offset_data",
                        "json_size", "image_format", "hash_source", "sha256",
                        "hash_kind")):
                    for row in batch.to_pylist():
                        rid = row["record_id"]
                        item = audit.get(rid)
                        if (rid in observed or not isinstance(item, dict)
                                or set(item) != {"identity", "image_path", "json_path",
                                                  "image", "json", "suffix"}):
                            raise PackageCorrupt("missing or duplicate audited P2 sample")
                        key = (row["dataset_id"], row["object_id"])
                        binding = bindings.get((key[0],
                                                inventory.contract["adapter"]["storage_id"],
                                                key[1]))
                        if (binding is None or item["identity"] != list((key[0],
                                binding.storage_id, key[1]))
                                or item["image_path"] != row["image_path"]
                                or item["json_path"] != row["json_path"]
                                or item["suffix"] != "." + row["image_format"]
                                or item["image"].get("offset") != row["offset_data"]
                                or item["image"].get("size") != row["size"]
                                or row["hash_source"] != "computed:sha256"
                                or row["hash_kind"] != "sha256"
                                or not isinstance(row["sha256"], str)
                                or not _SHA.fullmatch(row["sha256"])
                                or item["image"].get("sha256") != row["sha256"]
                                or (item["json"] is None) != (row["json_path"] is None)):
                            raise PackageCorrupt("member audit differs from durable P2 sample")
                        if item["json"] is not None and (
                                item["json"].get("offset") != row["json_offset_data"]
                                or item["json"].get("size") != row["json_size"]):
                            raise PackageCorrupt("metadata audit differs from durable P2 sample")
                        Extent(**item["image"]).validate(binding.size)
                        if item["json"] is not None:
                            Extent(**item["json"]).validate(binding.size, allow_empty=True)
                        record = snapshot._catalog.execute(
                            "SELECT r.rid, d.name FROM records r JOIN datasets d "
                            "ON r.dataset_id=d.dataset_id WHERE r.record_id=?",
                            (bytes.fromhex(rid),)).fetchone()
                        if record is None:
                            raise PackageCorrupt("audited record absent from runtime snapshot")
                        loc = snapshot.location(record[0])
                        if (record[1] != key[0]
                                or index_keys.get(loc["object_idx"]) != (
                                    key[0], binding.storage_id, key[1])):
                            raise PackageCorrupt("runtime record points to another P2 object")
                        if (bool(loc["flags"] & HAS_METADATA)
                                != (item["json"] is not None)
                                or (loc["image_offset"], loc["image_size"]) != (
                                    row["offset_data"], row["size"])
                                or (item["json"] is not None and (
                                    loc["metadata_offset"], loc["metadata_size"]) != (
                                    row["json_offset_data"], row["json_size"]))):
                            raise PackageCorrupt("member extent/presence differs from runtime")
                        observed.add(rid)
            if observed != set(audit):
                raise PackageCorrupt("audit record set differs from durable samples")
    except PackageCorrupt:
        raise
    except (OSError, ValueError, TypeError, KeyError, StopIteration, AttributeError,
            IndexError, pa.ArrowException):
        raise PackageCorrupt("unrecognized durable/runtime/audit chain") from None


def load_package(root: Path, *, allow_offline_loopback: bool = False) -> PackageManifest:
    root = Path(root).absolute()
    if not root.is_relative_to(DEFAULT_WORK_ROOT) or not root.is_dir():
        raise PackageCorrupt("package outside fixed work root")
    for part in (root, *root.parents):
        if part == DEFAULT_WORK_ROOT.parent:
            break
        if part.is_symlink() or (hasattr(part, "is_junction") and part.is_junction()):
            raise PackageCorrupt("package root or parent is symlink/junction")
    if not root.resolve().is_relative_to(DEFAULT_WORK_ROOT.resolve()):
        raise PackageCorrupt("package resolves outside fixed root")
    index = _file(root, "index-package.json")
    if index.stat().st_size > _MAX_MANIFEST:
        raise PackageCorrupt("package manifest exceeds bounded size")
    metadata = _bounded_json(index, _MAX_MANIFEST)
    required = {"package_format_version", "producer", "producer_code_version",
                "repo_id", "repo_type", "endpoint", "data_revision", "snapshot_id",
                "runtime", "durable", "audit", "scan_config", "scan_config_sha256",
                "scan_evidence", "binding_mode", "mtime_provenance", "files",
                "bindings", "coverage", "publication_status"}
    if (not isinstance(metadata, dict) or set(metadata) != required
            or metadata.get("package_format_version") != 1
            or metadata.get("producer") != "sakurapool-p4-remote-stream"
            or metadata.get("producer_code_version") != _CODE_VERSION
            or metadata.get("binding_mode") != _BINDING_MODE
            or metadata.get("mtime_provenance") != "unavailable; P2 mtime_ns=0"
            or metadata.get("coverage") != {
                "mode": "canary", "object_count": len(metadata.get("bindings", []))
                if isinstance(metadata.get("bindings"), list) else -1,
                "full_repository": False}
            or metadata.get("publication_status") != "local_only"):
        raise PackageCorrupt("unrecognized or incorrectly scoped package manifest")
    if metadata.get("repo_id") != _REPO or metadata.get("repo_type") != "dataset":
        raise PackageCorrupt("package targets an unapproved repository")
    revision = metadata.get("data_revision")
    snapshot_id = metadata.get("snapshot_id")
    if (not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", revision)
            or not isinstance(snapshot_id, str) or not _SHA.fullmatch(snapshot_id)):
        raise PackageCorrupt("invalid revision or runtime identity")
    endpoint = metadata.get("endpoint")
    try:
        parsed = urlsplit(endpoint)
        port = parsed.port
    except (TypeError, ValueError):
        parsed = None
        port = None
    offline_origin = (allow_offline_loopback and parsed is not None
                      and parsed.scheme == "http" and parsed.hostname == "127.0.0.1"
                      and port is not None)
    if (parsed is None or (parsed.scheme != "https" and not offline_origin)
            or not parsed.hostname or parsed.path not in ("", "/")
            or parsed.query or parsed.fragment or parsed.username or parsed.password):
        raise PackageCorrupt("invalid provider endpoint")
    inventory = metadata.get("files")
    if not isinstance(inventory, list) or not 1 <= len(inventory) <= 256:
        raise PackageCorrupt("invalid dependent-file inventory")
    names = set()
    for item in inventory:
        if not isinstance(item, dict) or set(item) != {"path", "bytes", "sha256"}:
            raise PackageCorrupt("invalid inventory row")
        name = _relative(item["path"])
        if name in names or name == "index-package.json":
            raise PackageCorrupt("duplicate or circular package inventory")
        names.add(name)
        size, digest = item["bytes"], item["sha256"]
        if type(size) is not int or size < 0 or not isinstance(digest, str) \
                or not _SHA.fullmatch(digest):
            raise PackageCorrupt("invalid inventory file size or SHA")
        json_cap = _MAX_MANIFEST if name == _SCAN_EVIDENCE else _MAX_CANARY_JSON
        bounded_json = (name.lower().endswith(".json")
                        or (name.startswith("durable/") and name.endswith(".COMMIT")))
        if bounded_json and size > json_cap:
            raise PackageCorrupt("canary package JSON input exceeds bounded size")
        path = _file(root, name)
        actual_size = path.stat().st_size
        if bounded_json and actual_size > json_cap:
            raise PackageCorrupt("canary JSON changed beyond bounded size")
        if actual_size != size or _hash(path) != digest:
            raise PackageCorrupt("package dependent-file mismatch")
    runtime = _relative(metadata.get("runtime"))
    durable = _relative(metadata.get("durable"))
    audit_file = _relative(metadata.get("audit"))
    config_file = _relative(metadata.get("scan_config"))
    evidence_file = _relative(metadata.get("scan_evidence"))
    if (runtime != "runtime" or durable != "durable"
            or audit_file != _SCAN_EVIDENCE or evidence_file != audit_file
            or config_file != _SCAN_CONFIG):
        raise PackageCorrupt("package has unexpected durable/runtime/audit layout")
    actual = set()
    for path in root.rglob("*"):
        relative = _relative(path.relative_to(root).as_posix())
        if path.is_symlink() or (hasattr(path, "is_junction") and path.is_junction()):
            raise PackageCorrupt("untrusted symlink or junction in package")
        if path.is_file():
            actual.add(relative)
        elif not path.is_dir():
            raise PackageCorrupt("unrecognized package filesystem entry")
    if actual != names | {"index-package.json"}:
        raise PackageCorrupt("package contains unlisted or missing file")
    if (audit_file not in names or config_file not in names
            or durable + "/INPUT.json" not in names
            or not any(x.startswith(runtime + "/") for x in names)):
        raise PackageCorrupt("package durable, runtime, config or audit not inventoried")
    bindings = metadata.get("bindings")
    if not isinstance(bindings, list) or not 1 <= len(bindings) <= 3:
        raise PackageCorrupt("canary binding count invalid")
    indexed = {}
    for entry in bindings:
        if not isinstance(entry, dict) or set(entry) != set(Binding.__dataclass_fields__):
            raise PackageCorrupt("invalid binding fields")
        try:
            binding = Binding(**entry)
            _relative(binding.path)
        except (TypeError, ValueError):
            raise PackageCorrupt("invalid binding path") from None
        if (not all(isinstance(v, str) and v for v in (
                binding.dataset_id, binding.storage_id, binding.object_id))
                or type(binding.size) is not int or not 0 < binding.size <= 2 * (1 << 30)
                or not isinstance(binding.content_sha256, str)
                or not _SHA.fullmatch(binding.content_sha256)
                or not isinstance(binding.strong_etag, str)
                or not re.fullmatch(r'"[\x21\x23-\x7e]+"', binding.strong_etag)
                or type(binding.conditional_verified) is not bool
                or not binding.conditional_verified
                or not isinstance(binding.condition_probe_sha256, str)
                or not _SHA.fullmatch(binding.condition_probe_sha256)
                or type(binding.legacy_local_binding_verified) is not bool
                or binding.object_id != (
                    f"{binding.path}@sha256-{binding.content_sha256}")):
            raise PackageCorrupt("invalid remote content binding")
        key = (binding.dataset_id, binding.storage_id, binding.object_id)
        if key in indexed:
            raise PackageCorrupt("duplicate binding")
        indexed[key] = binding
    audit_path = _file(root, audit_file)
    if audit_path.stat().st_size > _MAX_MANIFEST:
        raise PackageCorrupt("audit exceeds bounded size")
    audit = _bounded_json(audit_path, _MAX_MANIFEST)
    if not isinstance(audit, dict) or len(audit) > 100_000:
        raise PackageCorrupt("invalid sample audit")
    _validate_package_chain(root, metadata, names, indexed, audit)
    return PackageManifest(root, snapshot_id, _REPO, revision, endpoint,
                           runtime, durable, indexed, audit)


def publish_local_package(root: Path, ledger: BudgetLedger, *, endpoint: str,
                          data_revision: str, bindings: list[Binding],
                          production_transport=None) -> Path:
    """Publish one local-only canary manifest after existing v4/P3/audit verify.

    Caller owns proof that each binding's conditional capability was tested
    once against the corresponding remote object. This function is strictly
    offline; it does not resolve or fetch a target repo. No publication.
    """
    from .modelscope import ModelScopeDataset
    from .production import RustProductionTransport

    if (not isinstance(ledger, BudgetLedger) or (not ledger.offline_mode and not (
            isinstance(production_transport, RustProductionTransport)
            and production_transport.production_profile
            and production_transport.ledger is ledger))):
        raise BudgetExceeded("production package BLOCKED: Rust profile and budget required")
    root = Path(root).absolute()
    if (".." in root.parts or not root.is_relative_to(ledger.root) or not root.is_dir()
            or any(p.is_symlink() or is_reparse(p) for p in (root, *root.parents))):
        raise ValueError("offline package output must exist under test work root")
    if (root / "index-package.json").exists():
        raise FileExistsError("index-package.json already exists")
    if not 1 <= len(bindings) <= 3:
        raise PackageCorrupt("canary bindings missing or too many")
    try:
        parts = urlsplit(endpoint)
        port = parts.port
    except (TypeError, ValueError):
        raise PackageCorrupt("invalid package origin") from None
    if (not isinstance(data_revision, str)
            or not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", data_revision)
            or not parts.hostname or parts.path not in ("", "/")
            or parts.query or parts.fragment or parts.username or parts.password
            or not ((parts.scheme == "https" and parts.hostname == "modelscope.cn"
                     and port in (None, 443))
                    or (parts.scheme == "http" and parts.hostname == "127.0.0.1"
                        and port is not None))):
        raise PackageCorrupt("package revision or origin is unsafe")
    for binding in bindings:
        key = condition_binding_key(endpoint=endpoint, repository=_REPO,
                                    revision=data_revision, path=binding.path,
                                    size=binding.size, strong_etag=binding.strong_etag)
        if production_transport is not None:
            provider = ModelScopeDataset(production_transport, endpoint, _REPO)
            bound = BoundObject(provider.download_url(data_revision, binding.path), binding.size,
                                data_revision, binding.strong_etag, repository=_REPO)
            key = production_transport.condition_key(bound)
        if (not binding.conditional_verified
                or ledger.condition_proof(key) != binding.condition_probe_sha256):
            raise PackageCorrupt("missing app-owned exact conditional proof")
    # This is verified local-index configuration evidence, not a provider
    # discovery/immutability certificate. Derive it from the actual committed
    # P2 INPUT (adapter, hash_images and object validators), not a caller's
    # editable string or a floating remote profile.
    _preflight_durable_json(root)
    inventory = load_p2_inventory(root / "durable")
    config = _index_config(inventory.contract, endpoint, data_revision)
    config_raw = json.dumps(config, sort_keys=True, ensure_ascii=False,
                            separators=(",", ":")).encode("utf-8")
    if len(config_raw) > _MAX_CANARY_JSON:
        raise PackageCorrupt("index input configuration exceeds bounded canary size")
    if (root / _SCAN_CONFIG).exists() or (root / _SCAN_CONFIG).is_symlink():
        raise FileExistsError("scan config already exists; never overwrite")
    # Additional package-side bytes use only the offline allowance. Production
    # publication/fetch remains BLOCKED until complete physical bounds exist.
    config_lease = ledger.reserve(Reservation(disk=_MAX_CANARY_JSON + 4096))
    try:
        with (root / _SCAN_CONFIG).open("xb") as stream:
            stream.write(config_raw)
            stream.flush()
            import os
            os.fsync(stream.fileno())
        ledger.settle(config_lease)
    except BaseException:
        raise
    paths = []
    for directory in ("durable", "runtime", "audit"):
        folder = root / directory
        if not folder.is_dir() or folder.is_symlink():
            raise PackageCorrupt("missing or unsafe package directory")
        for entry in folder.rglob("*"):
            if entry.is_symlink() or (hasattr(entry, "is_junction") and entry.is_junction()):
                raise PackageCorrupt("untrusted package link")
            if entry.is_file():
                paths.append(_relative(entry.relative_to(root).as_posix()))
            elif not entry.is_dir():
                raise PackageCorrupt("unsafe package entry")
    if "audit/members.json" not in paths or len(paths) > 256:
        raise PackageCorrupt("missing scan audit or too many package files")
    with RuntimeSnapshot.open(root / "runtime", full_verify=True) as snapshot:
        snap_id = snapshot.snapshot_id
    files = [{"path": name, "bytes": _file(root, name).stat().st_size,
              "sha256": _hash(_file(root, name))} for name in sorted(paths)]
    from dataclasses import asdict
    manifest = {"package_format_version": 1, "producer": "sakurapool-p4-remote-stream",
                "producer_code_version": _CODE_VERSION,
                "repo_id": _REPO, "repo_type": "dataset", "endpoint": endpoint,
                "data_revision": data_revision, "snapshot_id": snap_id,
                "runtime": "runtime", "durable": "durable", "audit": _SCAN_EVIDENCE,
                "scan_config": _SCAN_CONFIG,
                "scan_config_sha256": hashlib.sha256(config_raw).hexdigest(),
                "scan_evidence": _SCAN_EVIDENCE,
                "binding_mode": _BINDING_MODE,
                "mtime_provenance": "unavailable; P2 mtime_ns=0", "files": files,
                "bindings": [asdict(item) for item in bindings],
                "coverage": {"mode": "canary", "object_count": len(bindings),
                             "full_repository": False},
                "publication_status": "local_only"}
    payload = json.dumps(manifest, sort_keys=True, ensure_ascii=False,
                         separators=(",", ":")).encode("utf-8")
    if len(payload) > _MAX_MANIFEST:
        raise PackageCorrupt("package manifest exceeds bounded size")
    reserve = ledger.reserve(Reservation(disk=_MAX_MANIFEST + 4096))
    try:
        with (root / "index-package.json").open("xb") as stream:
            stream.write(payload)
            stream.flush()
            import os
            os.fsync(stream.fileno())
        # Refuse invalid/incomplete packages; keep manifest+lease for recovery.
        load_package(root, allow_offline_loopback=ledger.offline_mode)
        ledger.settle(reserve)
    except BaseException:
        raise
    return root / "index-package.json"


@contextmanager
def admitted_package(root: Path, ledger: BudgetLedger):
    """Conservative small-package production admission, retained across fetch.

    Walk lstat-only before parsing; bound package bytes plus JSON/Parquet expansion.
    This does not change the frozen package schema or certify arbitrary large indexes.
    """
    root = Path(root).absolute()
    if (".." in root.parts or not root.is_relative_to(ledger.root) or not root.is_dir()
            or any(is_reparse(p) for p in (root, *root.parents))):
        raise PackageCorrupt("package budget path invalid")
    paths, pending, total, visited = [], [root], 0, 0
    while pending:
        directory = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                visited += 1
                path = Path(entry.path)
                info = path.lstat()
                if is_reparse(path):
                    raise PackageCorrupt("package links forbidden")
                if stat.S_ISDIR(info.st_mode):
                    pending.append(path)
                elif stat.S_ISREG(info.st_mode):
                    paths.append(path)
                    total += info.st_size
                else:
                    raise PackageCorrupt("package special files forbidden")
                if visited > 4096 or total > (1 << 20):
                    raise BudgetExceeded("production package BLOCKED: small canary only")
    memory = (64 << 20) + 128*total
    lease = ledger.reserve(Reservation(inflight=memory))
    try:
        # Compressed fragments must not expand beyond the reserved decoded budget.
        expanded = 0
        for path in paths:
            if path.suffix != ".parquet":
                continue
            with path.open("rb") as stream:
                if path.stat().st_size < 12:
                    raise PackageCorrupt("short package Parquet")
                stream.seek(-8, 2)
                footer = stream.read(8)
                if footer[4:] != b"PAR1" or int.from_bytes(footer[:4], "little") > (1 << 20):
                    raise PackageCorrupt("package Parquet footer exceeds bounded size")
            metadata = pq.ParquetFile(path).metadata
            if metadata.num_rows > 100_000:
                raise BudgetExceeded("production package row bound exceeds canary")
            for row in range(metadata.num_row_groups):
                group = metadata.row_group(row)
                for col in range(group.num_columns):
                    size = group.column(col).total_uncompressed_size
                    if size < 0:
                        raise PackageCorrupt("unrecognized package expansion")
                    expanded += size
                    if 4*expanded+64*total+(64 << 20) > memory:
                        raise BudgetExceeded("package decoded working set exceeds admission")
        package = load_package(root, allow_offline_loopback=ledger.offline_mode)
        yield package
    finally:
        ledger.settle(lease)


def fetch_from_package(root: Path, record_id: str, output: Path,
                       transport: GuardedTransport, *, merged: bool = True) -> Path:
    """Fresh-process package + complete runtime verification, never TAR cache.

    No network happens before local package, dependent-file hashes and runtime
    snapshot/record/object/audit bindings all succeed. Provider resolution and
    conditional semantics then use this exact frozen package revision; URL is
    ephemeral, never stored. The guarded transport owns every remote attempt.

    The P2 manifest schema is FROZEN to _REPO: load_package refuses any other
    repository, and that boundary does NOT imply migration support for new
    repositories. The parse_repository gate below is defensive parsing of the
    already-frozen value so the same sealed identity flows into BoundObject,
    not a repository configuration surface.
    """
    from .production import RustProductionTransport

    if (not isinstance(transport.ledger, BudgetLedger)
            or (not transport.ledger.offline_mode and not (
                isinstance(transport, RustProductionTransport) and transport.production_profile))):
        raise BudgetExceeded("production fetch BLOCKED: explicit Rust profile and budget required")
    if not Path(root).absolute().is_relative_to(transport.ledger.root):
        raise PackageCorrupt("package and transport budget roots differ")
    if isinstance(transport, RustProductionTransport):
        with admitted_package(root, transport.ledger) as package:
            return _fetch_loaded_package(package, record_id, output, transport, merged=merged)
    package = load_package(root, allow_offline_loopback=transport.ledger.offline_mode)
    return _fetch_loaded_package(package, record_id, output, transport, merged=merged)


def _fetch_loaded_package(package, record_id, output, transport, *, merged=True):
    """Internal call while an admitted package's inflight lease is still held."""
    from .modelscope import ModelScopeDataset

    # The package schema is frozen to its producer contract; parse again here
    # before provider/bound-object construction so malformed runtime identity
    # cannot cross this boundary. This is defensive validation, not migration.
    try:
        parse_repository(package.repo_id)
    except (RepositoryConfigError, TypeError):
        raise PackageCorrupt("package repository is not a valid owner/name") from None
    with RuntimeSnapshot.open(package.root / package.runtime, full_verify=True) as snapshot:
        binding, sample = package.sample(snapshot, record_id)
    # Defensive re-parse of the frozen manifest value: the sealed parsed
    # identity (BoundObject.repository_id) must equal the runtime
    # configuration; a malformed value can never flow into the transport.
    try:
        parse_repository(package.repo_id)
    except (RepositoryConfigError, TypeError):
        raise PackageCorrupt("package repository is not a valid owner/name") from None
    provider = ModelScopeDataset(transport, package.endpoint, package.repo_id)
    url = provider.download_url(package.data_revision, binding.path)
    bound = BoundObject(url, binding.size, package.data_revision,
                        binding.strong_etag, repository=package.repo_id)
    # Package booleans are UNTRUSTED. Only the guarded persistent budget
    # ledger's exact identity-bound proof can suppress redundant 2-call
    # positive/negative probes. Fresh workspaces must probe before reading.
    transport.ensure_verified_condition(
        bound, endpoint=package.endpoint, repository=package.repo_id,
        path=binding.path, expected_probe_sha256=binding.condition_probe_sha256)
    return fetch_bound_sample(transport, transport.ledger, bound, sample,
                              output, merged=merged)
