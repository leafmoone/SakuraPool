"""Explicit Rust ModelScope byte plane; Python remains the discovery control plane.

No signed URL or secret is serialized outside the anonymous worker stdin pipe.
Conditional capability is scoped to object+validator+host, never inferred from 206.
"""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from pathlib import Path

from .budget import BudgetLedger, Reservation, _disk_usage, is_reparse
from .location_gate import normalize_endpoint, two_hop_proof_key
from .modelscope import ListedFile, ModelScopeDataset
from .rust_bridge import RustWorker, RustWorkerError
from .transport import RemoteIOError

REPORT_CAP = 16 * (1 << 20)
MAX_RANGE = 8 * (1 << 20)
_DIAGNOSTIC_FLAGS = ("content_length_present", "content_range_present", "etag_present",
                     "etag_is_strong", "content_encoding_present")
_PUBLIC_ERROR_CODES = frozenset({"location_invalid", "location_encoding", "validator_mismatch",
                                "cdn_status", "origin_status", "conditional_unsupported",
                                "body_framing", "content_range", "body_length", "body_io",
                                "scan_failed", "origin_transport", "cdn_transport"})


def _safe_diagnostic(value, accounting):
    keys = {"phase", "http_status", "attempts", "body_bytes_observed", "accounting_complete",
            *_DIAGNOSTIC_FLAGS}
    if not isinstance(value, dict) or set(value) != keys:
        raise RemoteIOError("Rust production request rejected; diagnostic invalid")
    if (value["phase"] not in ("origin", "cdn", "body", "scan") or
            not (value["http_status"] is None or type(value["http_status"]) is int and
                 100 <= value["http_status"] <= 599) or
            any(type(value[k]) is not bool for k in (*_DIAGNOSTIC_FLAGS, "accounting_complete")) or
            type(value["attempts"]) is not int or type(value["body_bytes_observed"]) is not int or
            (value["attempts"], value["body_bytes_observed"], value["accounting_complete"]) !=
            (accounting["attempts"], accounting["body"], accounting["complete"])):
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

    def _call(self, obj, root, *, start=0, length=1, condition="match", mode="range"):
        try:
            return self._call_accounted(obj, root, start=start, length=length,
                                        condition=condition, mode=mode)
        except RustWorkerError:
            # Leave the handler before raising: no raw cause OR retained __context__.
            pass
        raise RemoteIOError("Rust production request rejected; accounting uncertain")

    def _call_accounted(self, obj, root, *, start=0, length=1, condition="match", mode="range"):
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
        memory = 4 * size + 65536 if mode == "range" else 64 * size + (4 << 20)
        disk = size if mode == "range" else size + REPORT_CAP
        budget = {"body": size + 1, "attempts": 2, "disk": disk, "inflight": memory}
        lease1 = self.ledger.reserve(Reservation(body=size + 1, attempt=True))
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
        )
        # All leases exist before hello; malformed/crash/timeout keeps unknown body pending.
        with RustWorker(self.worker, job_budget=budget, timeout_s=40) as worker:
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
                or not 0 <= accounting["body"] <= size + 1
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
            if "production_error" in result:
                code = result["production_error"]
                self.last_result["production_error"] = (
                    code if isinstance(code, str) and code in _PUBLIC_ERROR_CODES else "rejected")
            if not msg.get("ok") or "production_error" in result:
                # Account first even when ok=true: the envelope is not business success.
                raise RemoteIOError("Rust production request rejected")
            if not accounting["complete"]:
                raise RustWorkerError("production body uncertain; leases pending")
            return result

    @contextmanager
    def transfer(self, obj, *, start=0, length=1, condition="match", mode="range"):
        """Keep disk/inflight reserved through consumer audit; never publish partial files."""
        size = length if mode == "range" else obj.object_size
        memory = 4 * size + 65536 if mode == "range" else 64 * size + (4 << 20)
        disk = size + 8192 if mode == "range" else size + REPORT_CAP + 8192
        # Check profile/binding and working set before filesystem creation or network.
        if (
            condition == "match"
            and self.ledger.condition_proof(proof_key(obj, test=self._test)) is None
        ):
            raise RemoteIOError("verified production binding required")
        lease = self.ledger.reserve(Reservation(disk=disk, inflight=memory))
        root = None
        try:
            root = self._owned_dir()
            result = self._call(
                obj, root, start=start, length=length, condition=condition, mode=mode
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
                if body.stat().st_size != size or _disk_usage(root) > disk:
                    raise RemoteIOError("Rust fullstream artifact budget mismatch")
            yield root, result
        finally:
            # Only this call's exact owned file set. Unknown or linked files retain quota.
            if root is None:
                self.ledger.settle(lease)
            elif (
                root.is_dir()
                and not root.is_symlink()
                and not is_reparse(root)
                and {p.name for p in root.iterdir()} <= {"body", "scan.json"}
                and all(
                    p.is_file() and not p.is_symlink() and not is_reparse(p) for p in root.iterdir()
                )
            ):
                for path in root.iterdir():
                    path.unlink()
                root.rmdir()
                self.ledger.settle(lease)

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
        lease = self.ledger.reserve(Reservation(disk=8192, inflight=65540))
        root = None
        try:
            root = self._owned_dir()
            result = self._call(obj, root, condition="match")
            raw = (root / "body").read_bytes()
            if len(raw) != 1 or hashlib.sha256(raw).hexdigest() != result.get("sha256"):
                raise RemoteIOError("conditional capability bytes invalid")
            yield result
        finally:
            if root is None:
                self.ledger.settle(lease)
            elif (
                root.is_dir()
                and not root.is_symlink()
                and not is_reparse(root)
                and {p.name for p in root.iterdir()} == {"body"}
                and (root / "body").is_file()
                and not (root / "body").is_symlink()
                and not is_reparse(root / "body")
            ):
                (root / "body").unlink()
                root.rmdir()
                self.ledger.settle(lease)

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
        """Admin-only small admitted job; both pipelines share the existing stage contract.

        Both modes retain a whole-TAR temporary disk spool. The remote mode still
        feeds live response Read directly into the unchanged Rust scanner.
        """
        from .remote_index import OFFLINE_STAGE_ALLOWANCE
        from .rust_index import FileArchive, build_stage_from_scan
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
        stage_lease = self.ledger.reserve(Reservation(disk=OFFLINE_STAGE_ALLOWANCE))
        transferred = False
        try:
            with self.transfer(obj, mode=mode) as (root, result):
                report_path = root / "scan.json"
                if not 0 < report_path.stat().st_size <= REPORT_CAP:
                    raise RemoteIOError("scan report exceeds production bound")
                report = json.loads(report_path.read_bytes())
                if report.get("whole_sha256") != result.get("sha256"):
                    raise RemoteIOError("scan report identity mismatch")
                provider = ModelScopeDataset(self, obj.origin, obj.repo_id)
                bound = BoundObject(
                    provider.download_url(obj.revision, obj.object_path),
                    obj.object_size,
                    obj.revision,
                    obj.validator,
                    repository=obj.repo_id,
                )
                transferred = True
                return build_stage_from_scan(
                    report,
                    FileArchive(root / "body"),
                    bound,
                    adapter,
                    stage_dir,
                    self.ledger,
                    _stage_lease=stage_lease,
                )
        finally:
            if not transferred:
                self.ledger.settle(stage_lease)

    def condition_key(self, bound):
        return proof_key(self._bound_object(bound), test=self._test)
