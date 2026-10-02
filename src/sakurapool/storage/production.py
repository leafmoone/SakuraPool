"""Explicit Rust ModelScope byte plane; Python remains the discovery control plane.

No signed URL or secret is serialized outside the anonymous worker stdin pipe.
Conditional capability is scoped to object+validator+host, never inferred from 206.
"""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import stat
import sys
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from pathlib import Path

from .budget import BudgetLedger, Reservation, _cluster_bytes, _disk_usage, is_reparse
from .location_gate import normalize_endpoint, two_hop_proof_key
from .modelscope import ListedFile, ModelScopeDataset
from .production_resources import (
    NEGATIVE_CONDITION_BODY_CAP,
    STAGE_DISK_CAP,
    STREAM_MEMORY,
    ProductionFootprint,
)
from .rust_bridge import RustWorker, RustWorkerError
from .transport import RemoteIOError

MAX_RANGE = 8 * (1 << 20)
_DIAGNOSTIC_FLAGS = (
    "content_length_present",
    "content_range_present",
    "etag_present",
    "etag_is_strong",
    "content_encoding_present",
)
_PUBLIC_ERROR_CODES = frozenset(
    {
        "location_invalid",
        "location_encoding",
        "validator_mismatch",
        "cdn_status",
        "origin_status",
        "conditional_unsupported",
        "body_framing",
        "content_range",
        "body_length",
        "body_io",
        "scan_failed",
        "metadata_limit",  # Fixed Rust observer capacity enum; no raw scanner error.
        "origin_transport",
        "cdn_transport",
    }
)


def _safe_diagnostic(value, accounting):
    keys = {
        "phase",
        "http_status",
        "attempts",
        "body_bytes_observed",
        "accounting_complete",
        *_DIAGNOSTIC_FLAGS,
    }
    if not isinstance(value, dict) or set(value) != keys:
        raise RemoteIOError("Rust production request rejected; diagnostic invalid")
    if (
        value["phase"] not in ("origin", "cdn", "body", "scan")
        or not (
            value["http_status"] is None
            or type(value["http_status"]) is int
            and 100 <= value["http_status"] <= 599
        )
        or any(type(value[k]) is not bool for k in (*_DIAGNOSTIC_FLAGS, "accounting_complete"))
        or type(value["attempts"]) is not int
        or type(value["body_bytes_observed"]) is not int
        or (value["attempts"], value["body_bytes_observed"], value["accounting_complete"])
        != (accounting["attempts"], accounting["body"], accounting["complete"])
    ):
        raise RemoteIOError("Rust production request rejected; diagnostic invalid")
    return dict(value)


@dataclass(frozen=True)
class ProviderObject:
    repo_id: str
    repo_type: str
    origin: str
    revision: str
    object_path: str
    object_size: int
    validator: str | None = None
    cdn_host: str | None = None

    def validate(self, *, test=False):
        from urllib.parse import urlsplit

        p = urlsplit(self.origin)
        if (
            not test and self.origin not in ("https://www.modelscope.cn", "https://modelscope.cn")
        ) or (test and (p.scheme != "http" or p.hostname != "127.0.0.1" or not p.port)):
            raise ValueError("explicit ModelScope origin required")
        if (
            not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", self.repo_id)
            or self.repo_type != "modelscope_dataset_legacy"
            or not re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", self.revision)
            or not self.object_path
            or any(
                not re.fullmatch(r"[A-Za-z0-9_.-]+", s) or s in (".", "..")
                for s in self.object_path.split("/")
            )
            or type(self.object_size) is not int
            or not 0 < self.object_size < 2**64
            or (
                self.validator is not None
                and not re.fullmatch(r'"[A-Za-z0-9_.-]{1,254}"', self.validator)
            )
        ):
            raise ValueError("provider object identity invalid")

    @classmethod
    def from_tree(cls, dataset: ModelScopeDataset, entry: ListedFile):
        """Only an actual control-plane tree selection; revision is still a candidate."""
        if not isinstance(entry, ListedFile):
            raise ValueError("tree-listed object required")
        result = cls(
            dataset.repo_id,
            "modelscope_dataset_legacy",
            dataset.endpoint,
            entry.revision_candidate,
            entry.path,
            entry.size,
        )
        result.validate(test=dataset.transport.ledger.offline_mode)
        return result


def proof_key(obj: ProviderObject, *, test=False):
    """Domain-separated conditional v3 proof; old plain-206 proof cannot authorize."""
    obj.validate(test=test)
    if not obj.validator or not obj.cdn_host:
        raise RemoteIOError("production conditional binding missing")
    # The old safe key validator forbids test endpoints; test has its own domain.
    scope = (
        asdict(obj)
        if test
        else two_hop_proof_key(
            origin_endpoint=obj.origin,
            repository=obj.repo_id,
            repository_type=obj.repo_type,
            revision=obj.revision,
            path=obj.object_path,
            size=obj.object_size,
            etag=obj.validator,
            cdn_host=obj.cdn_host,
        )
    )
    return hashlib.sha256(
        json.dumps(
            ["rust-conditional-v3-test" if test else "rust-conditional-v3", scope], sort_keys=True
        ).encode()
    ).hexdigest()


class RustProductionTransport:
    """No Python data-plane fallback; one bounded two-hop logical worker request."""

    def __init__(
        self,
        ledger: BudgetLedger,
        worker: Path,
        *,
        origin: str,
        token: str | None = None,
        same_origin_cookie: str | None = None,
        _test=False,
    ):
        if ledger.offline_mode != _test:
            raise ValueError("production/test ledger profile mismatch")
        if not _test and origin not in ("https://www.modelscope.cn", "https://modelscope.cn"):
            raise ValueError("explicit configured ModelScope origin required")
        self.ledger, self.worker, self.origin = ledger, Path(worker), origin
        if same_origin_cookie is not None:
            from .transport import validate_same_origin_cookie

            validate_same_origin_cookie(same_origin_cookie)
        self._token, self._test = token, _test
        self._cookie = same_origin_cookie
        self.offline_only = _test
        self.production_profile = not _test
        self.allowed_hosts = {normalize_endpoint(origin)} if not _test else {"127.0.0.1"}
        self._objects: dict[str, ProviderObject] = {}

    max_range_bytes = MAX_RANGE

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def clone(self):
        copy = RustProductionTransport(
            self.ledger,
            self.worker,
            origin=self.origin,
            token=self._token,
            same_origin_cookie=self._cookie,
            _test=self._test,
        )
        copy._objects = dict(self._objects)
        return copy

    def _host(self, url):
        from urllib.parse import urlsplit

        p, origin = urlsplit(url), urlsplit(self.origin)
        if (p.scheme, p.hostname, p.port) != (origin.scheme, origin.hostname, origin.port):
            raise RemoteIOError("production configured origin mismatch")
        if p.username or p.password or p.fragment:
            raise RemoteIOError("production origin rejected")
        return p.hostname

    def ensure_verified_condition(
        self, bound, *, endpoint, repository, path, expected_probe_sha256
    ):
        obj = self._objects.get(path)
        if (
            obj is None
            or obj.origin != endpoint
            or obj.repo_id != repository
            or obj.revision != bound.immutable_revision
            or obj.object_size != bound.size
            or obj.validator != bound.strong_etag
            or self.ledger.condition_proof(proof_key(obj, test=self._test)) != expected_probe_sha256
        ):
            raise RemoteIOError("package requires independently verified Rust conditional binding")

    def register(self, obj: ProviderObject):
        obj.validate(test=self._test)
        if (
            obj.origin != self.origin
            or self.ledger.condition_proof(proof_key(obj, test=self._test)) is None
        ):
            raise RemoteIOError("production object lacks verified conditional binding")
        self._objects[obj.object_path] = obj

    def _owned_dir(self):
        root = self.ledger.root / ("rust-transfer-" + secrets.token_hex(16))
        root.mkdir(exist_ok=False)
        return root

    def _call(
        self, obj, root, *, start=0, length=1, condition="match", mode="range", json_limit=1 << 20
    ):
        try:
            return self._call_accounted(
                obj,
                root,
                start=start,
                length=length,
                condition=condition,
                mode=mode,
                json_limit=json_limit,
            )
        except RustWorkerError:
            # Leave the handler before raising: no raw cause OR retained __context__.
            pass
        raise RemoteIOError("Rust production request rejected; accounting uncertain")

    def _call_accounted(
        self, obj, root, *, start=0, length=1, condition="match", mode="range", json_limit=1 << 20
    ):
        obj.validate(test=self._test)
        if obj.origin != self.origin:
            raise RemoteIOError("origin binding mismatch")
        secrets_in_memory = [self._token] if self._token else []
        if self._cookie:
            secrets_in_memory += [
                part.split("=", 1)[1]
                for part in self._cookie.split(";")
                if "=" in part and part.split("=", 1)[1]
            ]
        if any(value in json.dumps(asdict(obj)) for value in secrets_in_memory):
            raise RemoteIOError("credential echo in object description rejected")
        size = length if mode == "range" else obj.object_size
        footprint = ProductionFootprint.admit(mode, size)
        memory, disk = footprint.memory, footprint.artifacts
        body_budget = (
            NEGATIVE_CONDITION_BODY_CAP + 1
            if mode == "range" and condition == "wrong"
            else size + 1
        )
        budget = {"body": body_budget, "attempts": 2, "disk": disk, "inflight": memory}
        lease1 = self.ledger.reserve(Reservation(body=body_budget, attempt=True))
        try:
            lease2 = self.ledger.reserve(Reservation(attempt=True))
        except BaseException:
            self.ledger.settle(lease1)  # No network started; admitted attempts stay charged.
            raise
        payload = dict(
            profile="twohop_test" if self._test else "modelscope_https_v1",
            object=asdict(obj),
            token=self._token,
            cookie=self._cookie,
            start=start,
            length=length,
            condition=condition,
            output_root=str(root),
            output_name="body",
            report_name=None if mode == "range" else "scan.json",
            mode=mode,
            json_limit=json_limit,
        )
        # All leases exist before hello; malformed/crash/timeout keeps unknown body pending.
        worker_type = RustWorker
        if getattr(self, "_track_correct_worker", False):
            transport = self

            class CorrectWorker(RustWorker):
                def __init__(self, *args, **kwargs):
                    self._proc = None  # Distinguish constructor-before-spawn from live unknown.
                    transport._correct_worker = self
                    super().__init__(*args, **kwargs)

            worker_type = CorrectWorker
        with worker_type(self.worker, job_budget=budget, timeout_s=40) as worker:
            request_id = uuid.uuid4().hex
            worker.send_raw(
                (
                    json.dumps(
                        dict(
                            type="request",
                            request_id=request_id,
                            operation="fetch_range" if mode == "range" else "scan_http_tar",
                            budget=budget,
                            payload={"production": payload},
                        )
                    )
                    + "\n"
                ).encode()
            )
            msg = worker.read_raw()
            result = msg.get("result")
            if (
                msg.get("type") != "response"
                or msg.get("request_id") != request_id
                or not isinstance(result, dict)
            ):
                raise RustWorkerError("production accounting unavailable; leases pending")
            accounting = result.get("accounting")
            if (
                not isinstance(accounting, dict)
                or set(accounting) != {"body", "attempts", "complete"}
                or type(accounting["body"]) is not int
                or not 0 <= accounting["body"] <= body_budget
                or type(accounting["attempts"]) is not int
                or not 0 <= accounting["attempts"] <= 2
                or type(accounting["complete"]) is not bool
            ):
                raise RustWorkerError("production accounting invalid; leases pending")
            self.ledger.consume_body(lease1, accounting["body"])
            if accounting["complete"]:
                self.ledger.settle(lease1)
                self.ledger.settle(lease2)
            self.last_result = {
                "accounting": dict(accounting),
                "diagnostic": _safe_diagnostic(result.get("diagnostic"), accounting),
            }
            observation = result.get("observation")
            if isinstance(observation, dict):
                self.last_result["observation"] = {
                    key: value if type(value) is int and 0 <= value <= maximum else None
                    for key, maximum in (
                        ("origin_http_status", 599),
                        ("cdn_http_status", 599),
                        ("content_length", 2**63 - 1),
                    )
                    for value in (observation.get(key),)
                }
            if "production_error" in result:
                code = result["production_error"]
                self.last_result["production_error"] = (
                    code if isinstance(code, str) and code in _PUBLIC_ERROR_CODES else "rejected"
                )
            if not msg.get("ok") or "production_error" in result:
                # Account first even when ok=true: the envelope is not business success.
                raise RemoteIOError("Rust production request rejected")
            if not accounting["complete"]:
                raise RustWorkerError("production body uncertain; leases pending")
            return result

    @contextmanager
    def transfer(
        self,
        obj,
        *,
        start=0,
        length=1,
        condition="match",
        mode="range",
        json_limit=1 << 20,
        retain=False,
    ):
        """Keep disk/inflight reserved through consumer audit; never publish partial files."""
        size = length if mode == "range" else obj.object_size
        footprint = ProductionFootprint.admit(mode, size)
        memory, disk = footprint.memory, footprint.transfer_disk
        # Check profile/binding and working set before filesystem creation or network.
        if (
            condition == "match"
            and self.ledger.condition_proof(proof_key(obj, test=self._test)) is None
        ):
            raise RemoteIOError("verified production binding required")
        lease = self.ledger.reserve(Reservation(disk=disk))
        try:
            memory_lease = self.ledger.reserve(Reservation(inflight=memory))
        except BaseException:
            self.ledger.settle(lease)
            raise
        root = None
        completed = False
        try:
            root = self._owned_dir()
            result = self._call(
                obj,
                root,
                start=start,
                length=length,
                condition=condition,
                mode=mode,
                json_limit=json_limit,
            )
            body = root / "body"
            if mode == "range":
                if condition == "wrong":
                    if body.stat().st_size != 0 or result.get("status") != 412:
                        raise RemoteIOError("negative condition verification failed")
                else:
                    raw = body.read_bytes()
                    if len(raw) != size or hashlib.sha256(raw).hexdigest() != result.get("sha256"):
                        raise RemoteIOError("Rust Range artifact verification failed")
            else:
                if (
                    mode == "remote-stream-scan"
                    and body.exists()
                    or mode == "download-then-scan"
                    and body.stat().st_size != size
                    or _disk_usage(root) > disk
                ):
                    raise RemoteIOError("Rust stream artifact budget mismatch")
            yield root, result
            completed = True
        finally:
            primary = sys.exc_info()[1]
            failed = False
            try:
                try:
                    self.ledger.settle(memory_lease)
                finally:
                    # Always attempt ownership registration/cleanup, even if settlement rejects.
                    if root is None:
                        self.ledger.settle(lease)
                    else:
                        snapshot = self._owned_snapshot(root)
                        if snapshot is not None and retain:
                            self._retained_download = getattr(self, "_retained_download", {})
                            self._retained_download[str(root)] = {
                                "lease": lease,
                                "object": obj,
                                "snapshot": snapshot,
                                "digest": result.get("sha256") if completed else None,
                            }
                        elif snapshot is not None:
                            self._delete_owned(root, snapshot)
                            self.ledger.settle(lease)
                        # Unknown/reparse/failed ownership stays on disk with quota.
            except Exception:
                failed = True
            if failed and primary is None:
                raise RemoteIOError("production resource finalization incomplete; quota retained")

    def _owned_snapshot(self, root):
        try:
            if not root.is_relative_to(self.ledger.root) or any(
                p.is_symlink() or is_reparse(p) for p in (root, *root.parents)
            ):
                return None
            directory = root.lstat()
            if not stat.S_ISDIR(directory.st_mode):
                return None
            files = {}
            for path in root.iterdir():
                info = path.lstat()
                if (
                    path.name not in {"body", "scan.json", "metadata.bin"}
                    or not stat.S_ISREG(info.st_mode)
                    or is_reparse(path)
                    or info.st_nlink != 1
                ):
                    return None
                files[path.name] = (info.st_dev, info.st_ino, info.st_size)
            after = root.lstat()
            if (directory.st_dev, directory.st_ino) != (after.st_dev, after.st_ino):
                return None
            return (directory.st_dev, directory.st_ino), files
        except OSError:
            return None

    def _delete_owned(self, root, snapshot):
        if self._owned_snapshot(root) != snapshot:
            raise RemoteIOError("owned production artifacts changed; retained")
        for name, identity in snapshot[1].items():
            path = root / name
            info = path.lstat()
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or is_reparse(path)
                or (info.st_dev, info.st_ino, info.st_size) != identity
            ):
                raise RemoteIOError("owned production identity changed; retained")
            path.unlink()
        root.rmdir()

    def _expect_download_commit(self, p2_root, contract, frozen):
        """Called only by validated write_staged_v4, binding intended output and full INPUT."""
        for job in getattr(self, "_retained_download", {}).values():
            for rel, bound, stage in frozen:
                if (
                    rel == job["object"].object_path
                    and stage.content_sha256 == job["digest"]
                    and contract.get("adapter") == job.get("adapter")
                    and self._bound_object(bound) == job["object"]
                ):
                    job["expected_root"] = str(Path(p2_root).absolute())
                    job["expected_contract"] = json.dumps(contract, sort_keys=True)

    def release_committed_downloads(self, p2_root):
        """Release only unchanged owned spools matched by validated durable P2 COMMIT.

        A crash loses the in-memory ownership map: retained roots/quota are NOT
        automatically reclaimed. Unknown files and failed jobs remain conservative.
        """
        lease = self.ledger.reserve(Reservation(inflight=STREAM_MEMORY))
        try:
            return self._release_verified_downloads(p2_root)
        finally:
            self.ledger.settle(lease)

    def _release_verified_downloads(self, p2_root):
        from ..runtime.inventory import load_p2_inventory

        inventory = load_p2_inventory(p2_root, _row_batch_size=128)
        committed = {item.object_id for item in inventory.objects}
        owned = getattr(self, "_retained_download", {})
        for text, job in list(owned.items()):
            if (
                job["digest"] is None
                or job.get("expected_root") != str(Path(p2_root).absolute())
                or job.get("expected_contract") != json.dumps(inventory.contract, sort_keys=True)
                or f"{job['object'].object_path}@sha256-{job['digest']}" not in committed
            ):
                continue
            self._delete_owned(Path(text), job["snapshot"])
            self.ledger.settle(job["lease"])
            del owned[text]

    def verify_conditions(self, candidate: ProviderObject):
        """Observe then positive+negative at the actual byte endpoint. Never infer support."""
        with self.transfer(candidate, condition="observe") as (_, observed):
            bound = replace(candidate, validator=observed["etag"], cdn_host=observed["cdn_host"])
            digest = observed["sha256"]
        # Capability test is not a production transfer: no proof exists yet.
        with self._capability_match(bound) as positive:
            if positive.get("status") != 206 or positive.get("sha256") != digest:
                raise RemoteIOError("conditional positive changed object")
        with self.transfer(bound, condition="wrong") as (_, negative):
            if negative.get("status") != 412 or negative.get("cdn_host") != bound.cdn_host:
                raise RemoteIOError("conditional negative unsupported")
        key = proof_key(bound, test=self._test)
        self.ledger.record_condition_proof(key, digest)
        self.register(bound)
        return bound

    @contextmanager
    def _capability_match(self, obj):
        # Same bytes/header/size path; only the proof prerequisite differs.
        footprint = ProductionFootprint.admit("range", 1)
        lease = self.ledger.reserve(Reservation(disk=footprint.transfer_disk))
        try:
            memory_lease = self.ledger.reserve(Reservation(inflight=footprint.memory))
        except BaseException:
            self.ledger.settle(lease)
            raise
        root = None
        self._correct_worker = None
        self._track_correct_worker = True
        try:
            root = self._owned_dir()
            result = self._call(obj, root, condition="match")
            raw = (root / "body").read_bytes()
            if len(raw) != 1 or hashlib.sha256(raw).hexdigest() != result.get("sha256"):
                raise RemoteIOError("conditional capability bytes invalid")
            yield result
        finally:
            primary = sys.exc_info()[1]
            self._track_correct_worker = False
            failed = False
            try:
                worker = self._correct_worker
                proc = getattr(worker, "_proc", None) if worker is not None else None
                # Base shutdown clears _proc only after wait. Otherwise poll the actual
                # retained Popen. No handle means no successful spawn or already reaped.
                stopped = proc is None or proc.poll() is not None
                if stopped:
                    try:
                        self.ledger.settle(memory_lease)
                    finally:
                        if root is None:
                            self.ledger.settle(lease)
                        else:
                            snapshot = self._owned_snapshot(root)
                            if snapshot is not None and set(snapshot[1]) <= {"body"}:
                                self._delete_owned(root, snapshot)
                                self.ledger.settle(lease)
                            # Unknown artifacts retain disk quota, never dead worker RAM.
                else:
                    failed = True  # Live/unknown worker retains both quota and artifacts.
            except Exception:
                failed = True
            if failed and primary is None:
                raise RemoteIOError("conditional resource finalization incomplete; quota retained")

    def _bound_object(self, bound):
        from urllib.parse import parse_qs, urlsplit

        paths = parse_qs(urlsplit(bound.url).query).get("FilePath", [])
        obj = self._objects.get(paths[0]) if len(paths) == 1 else None
        if (
            obj is None
            or obj.object_size != bound.size
            or obj.validator != bound.strong_etag
            or obj.revision != bound.immutable_revision
            or bound.repository_id is None
            or obj.repo_id != bound.repository_id.id
        ):
            raise RemoteIOError("package object and production binding differ")
        expected = ModelScopeDataset(self, obj.origin, obj.repo_id).download_url(
            obj.revision, obj.object_path
        )
        if bound.url != expected:
            raise RemoteIOError("package provider URL differs from exact object scope")
        return obj

    @contextmanager
    def read_range_owned(self, bound, start, length):
        obj = self._bound_object(bound)
        if length == 0:
            yield b""
            return
        if not 0 < length <= MAX_RANGE:
            raise RemoteIOError("production Range exceeds bound")
        with self.transfer(obj, start=start, length=length) as (root, _):
            yield (root / "body").read_bytes()

    def build_stage(self, obj, adapter, stage_dir: Path, *, mode: str):
        """Existing stage contract via capped scanner sidecars; only Download keeps TAR."""
        from .production_stage import build_stage_from_sidecars
        from .transport import BoundObject

        stage_dir = Path(stage_dir).absolute()
        if (
            mode not in ("remote-stream-scan", "download-then-scan")
            or ".." in stage_dir.parts
            or not stage_dir.is_relative_to(self.ledger.root)
            or stage_dir.exists()
            or stage_dir.is_symlink()
        ):
            raise ValueError("fresh stage path inside budget root required")
        for part in (stage_dir.parent, *stage_dir.parent.parents):
            if part.is_symlink() or is_reparse(part):
                raise ValueError("stage parent reparse rejected")
        self.register(obj)
        if _cluster_bytes(self.ledger.root) > 4096:
            raise RemoteIOError("production artifact allocation bound unavailable on work volume")
        stage_lease = self.ledger.reserve(Reservation(disk=STAGE_DISK_CAP))
        transferred = False
        try:
            with self.transfer(
                obj,
                mode=mode,
                json_limit=min(adapter.max_json_bytes, 1 << 20),
                retain=mode == "download-then-scan",
            ) as (root, result):
                provider = ModelScopeDataset(self, obj.origin, obj.repo_id)
                bound = BoundObject(
                    provider.download_url(obj.revision, obj.object_path),
                    obj.object_size,
                    obj.revision,
                    obj.validator,
                    repository=obj.repo_id,
                )
                transferred = True
                stage = build_stage_from_sidecars(
                    root,
                    result,
                    bound,
                    adapter,
                    stage_dir,
                    self.ledger,
                    stage_lease=stage_lease,
                )
            if mode == "download-then-scan":
                job = getattr(self, "_retained_download", {}).get(str(root))
                if job is not None:
                    job["adapter"] = adapter.to_dict()
            return stage
        finally:
            if not transferred:
                self.ledger.settle(stage_lease)

    def condition_key(self, bound):
        return proof_key(self._bound_object(bound), test=self._test)
