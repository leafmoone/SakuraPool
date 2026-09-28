"""Read-only P4 local index-package format 1 audit and snapshot binding.

No network calls or mutable URL is serialized here. A package is only valid
when *all* dependent files have canonical paths, plain regular-file types,
exact sizes and SHA-256 hashes. It never rescans a TAR or infers paths from a
record ID; published local/v4 legacy objects are not silently reinterpreted.
"""

from __future__ import annotations

import hashlib
import json
import re
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

from ..runtime.snapshot import RuntimeSnapshot
from .budget import DEFAULT_WORK_ROOT, BudgetExceeded, BudgetLedger, Reservation
from .retrieval import AuditedSample, Extent, fetch_bound_sample
from .transport import BoundObject, GuardedTransport, condition_binding_key

_SHA = re.compile(r"[0-9a-f]{64}\Z")
_RECORD = re.compile(r"[0-9a-f]{32}\Z")
_REPO = "leafmoone/game_cg_5M"
_MAX_MANIFEST = 64 * (1 << 20)


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
            image = Extent(**audit["image"])
            meta = Extent(**audit["json"]) if audit.get("json") is not None else None
            sample = AuditedSample(record_id, image, meta, audit["suffix"])
            sample.validate(binding.size)
        except (KeyError, TypeError, ValueError):
            raise PackageCorrupt("invalid member audit") from None
        if (image.offset, image.size) != (location["image_offset"],
                                          location["image_size"]):
            raise PackageCorrupt("image extent differs from runtime snapshot")
        if meta is not None and (meta.offset, meta.size) != (
                location["metadata_offset"], location["metadata_size"]):
            raise PackageCorrupt("metadata extent differs from runtime snapshot")
        if meta is None and location["metadata_size"] != 0:
            raise PackageCorrupt("runtime metadata lacks an audit hash")
        return binding, sample


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
    try:
        metadata = json.loads(index.read_bytes())
    except (ValueError, UnicodeError):
        metadata = None
    required = {"package_format_version", "producer", "repo_id", "repo_type",
                "endpoint", "data_revision", "snapshot_id", "runtime", "audit",
                "files", "bindings", "coverage", "publication_status"}
    if (not isinstance(metadata, dict) or set(metadata) != required
            or metadata.get("package_format_version") != 1
            or metadata.get("producer") != "sakurapool-p4-remote-stream"
            or metadata.get("coverage") != "canary"
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
        path = _file(root, name)
        if path.stat().st_size != size or _hash(path) != digest:
            raise PackageCorrupt("package dependent-file mismatch")
    runtime = _relative(metadata.get("runtime"))
    audit_file = _relative(metadata.get("audit"))
    if runtime != "runtime" or audit_file != "audit/members.json":
        raise PackageCorrupt("package has unexpected runtime/audit layout")
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
    if audit_file not in names or not any(x.startswith(runtime + "/") for x in names):
        raise PackageCorrupt("package runtime or audit not inventoried")
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
    try:
        audit = json.loads(audit_path.read_bytes())
    except (UnicodeError, ValueError):
        audit = None
    if not isinstance(audit, dict) or len(audit) > 100_000:
        raise PackageCorrupt("invalid sample audit")
    return PackageManifest(root, snapshot_id, _REPO, revision, endpoint,
                           runtime, indexed, audit)


def publish_local_package(root: Path, ledger: BudgetLedger, *, endpoint: str,
                          data_revision: str, bindings: list[Binding]) -> Path:
    """Publish one local-only canary manifest after existing v4/P3/audit verify.

    Caller owns proof that each binding's conditional capability was tested
    once against the corresponding remote object. This function is strictly
    offline; it does not resolve or fetch a target repo. No publication.
    """
    if not isinstance(ledger, BudgetLedger) or not ledger.offline_mode:
        raise BudgetExceeded("P4 package production build BLOCKED: unproven full-chain disk cap")
    root = Path(root).absolute()
    if not root.is_relative_to(ledger.root) or not root.is_dir():
        raise ValueError("offline package output must exist under test work root")
    if (root / "index-package.json").exists():
        raise FileExistsError("index-package.json already exists")
    if not 1 <= len(bindings) <= 3:
        raise PackageCorrupt("canary bindings missing or too many")
    for binding in bindings:
        key = condition_binding_key(endpoint=endpoint, repository=_REPO,
                                    revision=data_revision, path=binding.path,
                                    size=binding.size, strong_etag=binding.strong_etag)
        if (not binding.conditional_verified
                or ledger.condition_proof(key) != binding.condition_probe_sha256):
            raise PackageCorrupt("missing app-owned exact conditional proof")
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
                "repo_id": _REPO, "repo_type": "dataset", "endpoint": endpoint,
                "data_revision": data_revision, "snapshot_id": snap_id,
                "runtime": "runtime", "audit": "audit/members.json", "files": files,
                "bindings": [asdict(item) for item in bindings], "coverage": "canary",
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


def fetch_from_package(root: Path, record_id: str, output: Path,
                       transport: GuardedTransport, *, merged: bool = True) -> Path:
    """Fresh-process package + complete runtime verification, never TAR cache.

    No network happens before local package, dependent-file hashes and runtime
    snapshot/record/object/audit bindings all succeed. Provider resolution and
    conditional semantics then use this exact frozen package revision; URL is
    ephemeral, never stored. The guarded transport owns every remote attempt.
    """
    from .modelscope import ModelScopeDataset

    if not Path(root).absolute().is_relative_to(transport.ledger.root):
        raise PackageCorrupt("package and transport budget roots differ")
    package = load_package(root, allow_offline_loopback=transport.ledger.offline_mode)
    with RuntimeSnapshot.open(package.root / package.runtime, full_verify=True) as snapshot:
        binding, sample = package.sample(snapshot, record_id)
    provider = ModelScopeDataset(transport, package.endpoint, package.repo_id)
    url = provider.download_url(package.data_revision, binding.path)
    bound = BoundObject(url, binding.size, package.data_revision,
                        binding.strong_etag)
    # Package booleans are UNTRUSTED. Only the guarded persistent budget
    # ledger's exact identity-bound proof can suppress redundant 2-call
    # positive/negative probes. Fresh workspaces must probe before reading.
    transport.ensure_verified_condition(
        bound, endpoint=package.endpoint, repository=package.repo_id,
        path=binding.path, expected_probe_sha256=binding.condition_probe_sha256)
    return fetch_bound_sample(transport, transport.ledger, bound, sample,
                              output, merged=merged)
