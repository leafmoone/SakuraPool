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
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from pathlib import Path

from ..capacity import CapacityConfig
from .budget import BudgetLedger, Reservation, _cluster_bytes, _disk_usage, is_reparse
from .location_gate import normalize_endpoint, two_hop_proof_key
from .modelscope import ListedFile, ModelScopeDataset
from .production_resources import (
    HTTP_ATTEMPTS_MAX,
    LEDGER_ATTEMPTS_PER_TRANSFER,
    SESSION_ATTEMPTS_PER_TRANSFER,
    STAGE_DISK_CAP,
    STREAM_MEMORY,
    ProductionFootprint,
    checked_body_add,
    checked_body_mul,
    network_body_budget,
    protocolmemory,
)
from .rust_bridge import PRODUCTION_RPC_TIMEOUT_S, RustWorker, RustWorkerError
from .transport import RemoteIOError

MAX_RANGE = 8 * (1 << 20)
# In-process evidence minted only after the request resource and ledger gates.
_CONSERVATIVE_FINALIZED = object()
_HTTP_STATUS_FINALIZED = object()
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
        "origin_timeout",
        "origin_connect",
        "origin_request",
        "cdn_timeout",
        "cdn_connect",
        "cdn_request",
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
        result.validate(
            test=getattr(
                dataset.transport,
                "offline_mode",
                getattr(dataset.transport.ledger, "offline_mode", False),
            )
        )
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
        ledger: BudgetLedger | None = None,
        worker: Path | None = None,
        *,
        origin: str,
        token: str | None = None,
        same_origin_cookie: str | None = None,
        capacity=None,
        _test=False,
        root: Path | None = None,
        offline_mode: bool = False,
        _worker_pool=None,
    ):
        self.lightweight = ledger is None
        if self.lightweight and root is None:
            raise ValueError("owned temporary root required")
        _test = _test or offline_mode
        if ledger is not None and ledger.offline_mode != _test:
            raise ValueError("production/test ledger profile mismatch")
        if not _test and origin not in ("https://www.modelscope.cn", "https://modelscope.cn"):
            raise ValueError("explicit configured ModelScope origin required")
        if worker is None:
            raise ValueError("worker binary required")
        self.ledger, self.worker, self.origin = ledger, Path(worker), origin
        self.root = Path(root if root is not None else ledger.root).absolute()
        self.offline_mode = _test
        if self.lightweight and (
            not self.root.is_dir()
            or ".." in self.root.parts
            or any(p.is_symlink() or is_reparse(p) for p in (self.root, *self.root.parents))
        ):
            raise ValueError("owned temporary root rejected")
        self._root_identity = (self.root.stat().st_dev, self.root.stat().st_ino)
        self._condition_proofs = {}
        workspace = getattr(ledger, "workspace", None)
        self.capacity = (
            capacity
            if capacity is not None
            else (workspace.capacity if workspace is not None else CapacityConfig())
        )
        if not isinstance(self.capacity, CapacityConfig):
            raise ValueError("typed capacity required")
        if workspace is not None and workspace.capacity != self.capacity:
            raise ValueError("workspace/transport capacity mismatch")
        self.max_range_bytes = self.capacity.range_chunk_bytes
        self._capacity_v2 = self.capacity != CapacityConfig()
        if same_origin_cookie is not None:
            from .transport import validate_same_origin_cookie

            validate_same_origin_cookie(same_origin_cookie)
        self._token, self._test = token, _test
        self._cookie = same_origin_cookie
        self.offline_only = _test
        self.production_profile = not _test
        self.allowed_hosts = {normalize_endpoint(origin)} if not _test else {"127.0.0.1"}
        from collections import OrderedDict

        self._objects = OrderedDict()
        import threading

        self._lane_lock = threading.RLock()
        self._operation_active = False
        self._generation = 0
        self._rotating = False
        self._proof_group = False
        self._live_proofs = set()
        self._closed = False
        self._close_deadline = None
        self._worker_pool = _worker_pool
        self._clone_pool = None
        self._retained_pool = None
        self._retained_limit = None
        if _worker_pool is not None:
            from .worker_pool import WorkerPool

            if type(_worker_pool) is not WorkerPool or not self.lightweight:
                raise ValueError("owned lightweight worker pool required")
            _worker_pool.validate(self.worker, self.root, self.origin, self.capacity,
                                  self._token, self._cookie, self._test)
        if self.lightweight:
            self._persistent = True
            self._lane_worker = None
            self._lane_failed = False
            self._lane_requests = 0
            try:
                self._lane_worker = self._new_lightweight_worker()
            except RustWorkerError:
                raise RemoteIOError(
                    "Lightweight worker capability unavailable",
                    code="WORKER_CAPABILITY_UNAVAILABLE",
                    phase="worker",
                    lightweight=True,
                ) from None

    max_range_bytes = MAX_RANGE

    def _new_lightweight_worker(self):
        if self._worker_pool is None:
            worker = RustWorker(self.worker, capacity=self.capacity, lightweight=True)
        else:
            from .worker_pool import PooledWorker

            worker = PooledWorker(self._worker_pool)
        try:
            worker.require_metadata()
        except BaseException:
            worker.cancel(deadline=time.monotonic() + 5.0)
            raise
        return worker

    def _begin_task_pool(self, lanes):
        from .worker_pool import CAPABILITY, WorkerPool

        if self._clone_pool is not None:
            raise RemoteIOError("Transport already owns a task pool", lightweight=True)
        if not self.lightweight or CAPABILITY not in self._lane_worker.capabilities:
            return None
        if self._retained_limit is not None and lanes > self._retained_limit:
            raise RemoteIOError("Session worker limit exceeded", lightweight=True)
        pool = self._retained_pool
        if pool is None:
            pool = WorkerPool(self, 2 * (self._retained_limit or lanes))
            if self._retained_limit is not None:
                self._retained_pool = pool
        else:
            pool.resume()
        self._clone_pool = pool
        return pool

    def _end_task_pool(self, pool, *, deadline=None):
        try:
            if pool is self._retained_pool:
                pool.park()
            else:
                pool.close(deadline=deadline)
        finally:
            if self._clone_pool is pool:
                self._clone_pool = None

    def preflight_metadata(self):
        """Check the additive capability before any task row/NETWORK_START mutation."""
        if self._closed:
            raise RemoteIOError("Closed production transport")
        if self.lightweight:
            try:
                self._lane_worker.require_metadata()
            except (RustWorkerError, AttributeError):
                raise RemoteIOError("Native metadata capability unavailable",
                                    code="WORKER_CAPABILITY_UNAVAILABLE", phase="worker",
                                    lightweight=True) from None
        else:
            self.metadata_control().preflight_metadata()

    def _condition_proof(self, key):
        return (
            self._condition_proofs.get(key)
            if self.lightweight
            else self.ledger.condition_proof(key)
        )

    def _record_condition_proof(self, key, digest):
        if self.lightweight:
            if len(self._condition_proofs) >= 64 and key not in self._condition_proofs:
                self._condition_proofs.pop(next(iter(self._condition_proofs)))
            self._condition_proofs[key] = digest
        else:
            self.ledger.record_condition_proof(key, digest)

    def _admit_lightweight_generation(self, requests):
        if self._closed or self._operation_active or self._lane_failed or self._rotating:
            raise RemoteIOError("Execution generation unavailable", lightweight=True)
        if type(requests) is not int or not 0 <= requests <= 253:
            raise ValueError("operation exceeds execution generation")
        # Leave three slots for fresh observe/positive/negative proof requests.
        if self._lane_requests + requests + 3 > 256:
            self._rotating = True
            try:
                self._lane_worker.close()
                self._lane_worker = None
                self._generation += 1
                self._objects.clear()
                self._live_proofs.clear()
                self._condition_proofs.clear()
                self._lane_requests = 0
                self._lane_worker = self._new_lightweight_worker()
            except RustWorkerError:
                self._lane_failed = True
                raise RemoteIOError(
                    "Lightweight worker capability unavailable",
                    code="WORKER_CAPABILITY_UNAVAILABLE",
                    phase="worker",
                    lightweight=True,
                ) from None
            finally:
                self._rotating = False

    def _call_lightweight(
        self, obj, root, *, start=0, length=1, condition="match", mode="range", json_limit=1 << 20
    ):
        obj.validate(test=self._test)
        if obj.origin != self.origin:
            raise RemoteIOError("Origin binding mismatch", lightweight=True)
        sensitive = [self._token] if self._token else []
        if self._cookie:
            sensitive.extend(p.split("=", 1)[1] for p in self._cookie.split(";") if "=" in p)
        if any(v and v in json.dumps(asdict(obj)) for v in sensitive):
            raise RemoteIOError("Credential echo rejected", lightweight=True)
        ProductionFootprint.admit(
            mode, length if mode == "range" else obj.object_size, capacity=self.capacity
        )
        if self._lane_failed or self._lane_worker is None or self._lane_requests >= 256:
            raise RustWorkerError("execution channel unavailable")
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
            payload_revision=2,
            range_chunk_bytes=self.max_range_bytes,
            http_header_bytes=self.capacity.http_header_bytes,
        )
        worker = self._request_worker = self._lane_worker
        request_id = uuid.uuid4().hex
        try:
            # Each logical transfer has its own finite budget; handshake keeps
            # the bridge default. The lane lock prevents overlapping requests.
            worker.timeout_s = PRODUCTION_RPC_TIMEOUT_S
            worker.send_raw(
                (
                    json.dumps(
                        dict(
                            type="request",
                            request_id=request_id,
                            operation="fetch_range" if mode == "range" else "scan_http_tar",
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
                raise RustWorkerError("worker response invalid")
            diagnostic = result.get("diagnostic")
            if (
                not isinstance(diagnostic, dict)
                or set(diagnostic)
                != {"code", "phase", "recoverable", "delivery_safe", "http_status"}
                or diagnostic["code"]
                not in _PUBLIC_ERROR_CODES
                | {"ok", "rejected", "network_ambiguous", "validator_missing"}
                or diagnostic["phase"] not in {"worker", "origin", "cdn", "body", "scan"}
                or any(type(diagnostic[k]) is not bool for k in ("recoverable", "delivery_safe"))
                or not (
                    diagnostic["http_status"] is None
                    or type(diagnostic["http_status"]) is int
                    and 100 <= diagnostic["http_status"] <= 599
                )
            ):
                raise RustWorkerError("worker diagnostic invalid")
            self._request_terminal = True
            self._lane_requests += 1
            self.last_result = {"diagnostic": dict(diagnostic)}
            if msg.get("ok") is not True or "production_error" in result:
                raise RemoteIOError(
                    "Rust production request rejected",
                    lightweight=True,
                    code=diagnostic["code"] if diagnostic["code"] != "ok" else "rejected",
                    phase=diagnostic["phase"],
                    http_status=diagnostic["http_status"],
                    recoverable=diagnostic["recoverable"],
                    delivery_safe=False,
                )
            if (
                diagnostic["code"] != "ok"
                or not diagnostic["delivery_safe"]
                or result.get("technical") != {"eof_observed": True}
            ):
                raise RustWorkerError("worker delivery unconfirmed")
            return {k: v for k, v in result.items() if k != "technical"}
        except RustWorkerError:
            self._lane_failed = True
            primary = sys.exc_info()[1]
            try:
                worker.cancel()
            except BaseException:
                primary.finalization_secondary = (
                    *getattr(primary, "finalization_secondary", ()),
                    "worker_cancel",
                )
            raise
        except RemoteIOError as error:
            if not self._request_terminal or error.code in {
                "network_ambiguous",
                "body_io",
                "body_framing",
                "body_length",
            }:
                self._lane_failed = True
                try:
                    worker.cancel()
                except BaseException:
                    error.finalization_secondary = (
                        *getattr(error, "finalization_secondary", ()),
                        "worker_cancel",
                    )
            raise

    @contextmanager
    def _transfer_lightweight(
        self,
        obj,
        *,
        start=0,
        length=1,
        condition="match",
        mode="range",
        json_limit=1 << 20,
        retain=False,
        _proof=False,
    ):
        footprint = ProductionFootprint.admit(
            mode, length if mode == "range" else obj.object_size, capacity=self.capacity
        )
        if (
            condition == "match"
            and not _proof
            and self._condition_proof(proof_key(obj, test=self._test)) is None
        ):
            raise RemoteIOError("Verified production binding required", lightweight=True)
        if retain:
            raise ValueError("retained admin downloads require legacy transport")
        root = self._owned_dir()
        created = root.lstat()
        root_identity = (created.st_dev, created.st_ino)
        snapshot = None
        try:
            result = self._call(
                obj,
                root,
                start=start,
                length=length,
                condition=condition,
                mode=mode,
                json_limit=json_limit,
            )
            snapshot = self._owned_snapshot(root)
            if (
                snapshot is None
                or snapshot[0] != root_identity
                or sum(v[2] for v in snapshot[1].values()) > footprint.artifacts
            ):
                raise RemoteIOError("Production artifact ownership unconfirmed", lightweight=True)
            raw = b""
            if mode == "range":
                raw = (root / "body").read_bytes()
                if condition == "wrong":
                    if raw or result.get("status") != 412:
                        raise RemoteIOError(
                            "Negative condition verification failed", lightweight=True
                        )
                elif (
                    len(raw) != length
                    or hashlib.sha256(raw).hexdigest() != result.get("sha256")
                    or result.get("status") != 206
                    or condition == "match"
                    and (
                        result.get("etag") != obj.validator
                        or result.get("cdn_host") != obj.cdn_host
                    )
                ):
                    raise RemoteIOError("Range artifact verification failed", lightweight=True)
            elif mode == "remote-stream-scan" and "body" in snapshot[1]:
                raise RemoteIOError("Unexpected stream artifact", lightweight=True)
            elif (
                mode == "download-then-scan"
                and snapshot[1].get("body", (0, 0, -1))[2] != obj.object_size
            ):
                raise RemoteIOError("Stream artifact length invalid", lightweight=True)
            yield root, result, raw
        finally:
            primary = sys.exc_info()[1]
            try:
                current = self._owned_snapshot(root)
                if (
                    current is None
                    or current[0] != root_identity
                    or snapshot is not None
                    and current != snapshot
                ):
                    raise RemoteIOError("Owned artifacts changed; retained", lightweight=True)
                self._delete_owned(root, current)
            except BaseException:
                if primary is None:
                    raise
                primary.finalization_secondary = (
                    *getattr(primary, "finalization_secondary", ()),
                    "production_finalization",
                )

    def _worker_capacity(self):
        return {"capacity": self.capacity} if self._capacity_v2 else {}

    @contextmanager
    def _execution_worker(self, worker_type, budget):
        with self._lane_lock:
            if self._closed:
                raise RustWorkerError("execution channel closed")
        if not getattr(self, "_persistent", False):
            with worker_type(
                self.worker, job_budget=budget, **self._worker_capacity()
            ) as worker:
                yield worker
            return
        with self._persistent_worker(worker_type, budget) as worker:
            if getattr(self, "_track_correct_worker", False):
                self._correct_worker = worker
            yield worker

    @contextmanager
    def _persistent_worker(self, worker_type, budget):
        try:
            with self._persistent_worker_body(worker_type, budget) as worker:
                yield worker
        except BaseException as primary:
            candidate = getattr(self, "_conservative_candidate", None)
            if (
                isinstance(primary, RemoteIOError)
                and (
                    getattr(self, "_transfer_finalizer_owner", False)
                    or (
                        getattr(self, "_capability_finalizer_owner", False)
                        and getattr(self, "_http_status_candidate", None) is not None
                    )
                )
                and getattr(self, "_request_terminal", False)
                and (
                    (
                        candidate is not None
                        and not candidate["used"]
                        and primary.code == candidate["code"]
                    )
                    or (
                        getattr(self, "_http_status_candidate", None) is not None
                        and primary.code == self._http_status_candidate["code"]
                    )
                )
                and not getattr(primary, "finalization_secondary", ())
            ):
                # Defer, never confirm: outer resource/ledger gates own the verdict.
                self._deferred_network_error = primary
                raise
            self._lane_failed = True
            if self._lane_worker is not None:
                try:
                    self._lane_worker.cancel()
                except BaseException:
                    primary.finalization_secondary = (
                        *getattr(primary, "finalization_secondary", ()),
                        "worker_cancel",
                    )
            raise

    def _generation_budget(self, *, refresh=True):
        # Session credit bounds protocol work, not a cumulative user quota.
        # Real ledger reservations remain authoritative for every operation.
        if refresh:
            self.ledger.status()  # Refresh policy only from an authorized lane operation.
        maximum = max(self.capacity.range_chunk_bytes, self.capacity.metadata_max_bytes)
        body = checked_body_mul(network_body_budget(maximum), 256)
        if body >= 1 << 64:
            raise RustWorkerError("generation body integer capacity exceeded")
        return {
            "body": body,
            "attempts": SESSION_ATTEMPTS_PER_TRANSFER * 256,
            "disk": self.ledger.limits["disk"],
            "inflight": max(
                0, self.ledger.limits["inflight"] - getattr(self.ledger, "effective_headroom", 0)
            ),
        }

    @contextmanager
    def _persistent_worker_body(self, worker_type, budget):
        if self._lane_failed:
            raise RustWorkerError("execution channel unavailable")
        credit = self._generation_budget()
        if self._lane_worker is not None and (
            self._lane_requests + 1 > 256
            or self._lane_body + budget["body"] > credit["body"]
            or self._lane_attempts + budget["attempts"] > credit["attempts"]
        ):
            raise RustWorkerError("execution generation budget exhausted")
        if self._lane_worker is None:
            worker = worker_type(
                self.worker, job_budget=credit, **self._worker_capacity()
            )
            self._lane_worker = worker
            if "bounded_session_v1" not in worker.capabilities:
                self._lane_failed = True
                worker.close()
                raise RustWorkerError("execution channel capability unavailable")
            self._lane_requests = self._lane_body = self._lane_attempts = 0
        try:
            yield self._lane_worker
        finally:
            if getattr(self, "_request_terminal", False):
                self._lane_requests += 1
                self._lane_body += budget["body"]
                self._lane_attempts += budget["attempts"]

    def _admit_generation(self, body, attempts, requests):
        if self.lightweight:
            self._admit_lightweight_generation(requests)
            return
        # Session credit only: every request still reserves its real ledger quota.
        if not getattr(self, "_persistent", False):
            return
        if self._closed or self._operation_active or self._lane_failed or self._rotating:
            raise RemoteIOError("execution generation unavailable")
        credit = self._generation_budget()
        if self._lane_worker is not None and (
            self._lane_requests + requests > 256
            or self._lane_body + body > credit["body"]
            or self._lane_attempts + attempts > credit["attempts"]
        ):
            worker = self._lane_worker
            self._rotating = True
            self._lane_lock.release()
            try:
                worker.close()
            finally:
                self._lane_lock.acquire()
                self._rotating = False
            if self._closed:
                self._close_channel()
                raise RemoteIOError("execution generation closed during rotation")
            if self._lane_worker._proc is not None:
                self._lane_failed = True
                raise RemoteIOError("execution generation exit unconfirmed")
            self._lane_worker = None
            self._generation += 1
            self._objects.clear()
            self._live_proofs.clear()
            self._lane_requests = self._lane_body = self._lane_attempts = 0
            start_generation = getattr(self.ledger, "generation_start", None)
            if start_generation is not None:
                self._rotating = True
                self._lane_lock.release()
                try:
                    start_generation()
                except BaseException:
                    self._lane_failed = True
                    raise
                finally:
                    self._lane_lock.acquire()
                    self._rotating = False
                if self._closed:
                    raise RemoteIOError("execution generation closed during admission")

    def enable_persistent(self):
        with self._lane_lock:
            return self._enable_persistent()

    def _enable_persistent(self):
        if self.lightweight and self._persistent and not self._closed:
            return self
        if self._closed or self._operation_active or getattr(self, "_persistent", False):
            raise RemoteIOError("execution channel lifecycle invalid")
        self._lane_lease = self.ledger.reserve(
            Reservation(inflight=(32 << 20) + protocolmemory(self.capacity))
        )
        self._persistent = True
        self._lane_worker = None
        self._lane_failed = False
        self._lane_requests = self._lane_body = self._lane_attempts = 0
        return self

    def metadata_control(self):
        with self._lane_lock:
            return self._metadata_control()

    def _metadata_control(self):
        from urllib.parse import urlsplit

        from .transport import GuardedTransport

        if self._closed:
            raise RemoteIOError("closed production transport")
        if getattr(self, "_control", None) is None:
            self._control = GuardedTransport(
                self.ledger,
                offline_mode=self.offline_mode,
                allow_loopback_http=self.offline_mode,
                trusted_hosts=frozenset({urlsplit(self.origin).hostname}),
                token=self._token,
                credential_origin=None if self.offline_mode else self.origin,
                same_origin_cookie=self._cookie,
                capacity=self.capacity,
                worker=self.worker,
                metadata_origin=self.origin,
                metadata_mode="native",
                _worker_pool=self._worker_pool,
            )
        return self._control

    def close(self):
        if self._close_deadline is None:
            self._close_deadline = time.monotonic() + 5.0
        deadline = self._close_deadline
        pool = self._clone_pool or self._retained_pool
        if pool is not None:
            pool.signal_cancel(deadline=deadline)
        self._closed = True
        # Signal both children before waiting for a data-lane lock or any join.
        control = getattr(self, "_control", None)
        if control is not None:
            control.signal_cancel(deadline=deadline)
        worker = getattr(self, "_request_worker", None) or getattr(self, "_lane_worker", None)
        if worker is not None and hasattr(worker, "_stop"):
            worker.signal_cancel(deadline=deadline)
        if not self._lane_lock.acquire(timeout=max(0.0, deadline - time.monotonic())):
            raise RustWorkerError("execution channel close timed out")
        try:
            active = self._operation_active or self._rotating
            worker = getattr(self, "_request_worker", None) or getattr(self, "_lane_worker", None)
            if active:
                if control is not None:
                    control.close(deadline=deadline)
                if worker is not None and getattr(worker, "_construction_ready", True):
                    worker.cancel(deadline=deadline)
                return  # Active owner finalizes payload before releasing resident quota.
            primary = None
            try:
                self._close_channel()
            except BaseException as error:
                primary = error
            if pool is not None:
                try:
                    pool.close(deadline=deadline)
                except BaseException as error:
                    if primary is None:
                        primary = error
                    else:
                        primary.finalization_secondary = (
                            *getattr(primary, "finalization_secondary", ()), "worker_pool_close")
            if primary is not None:
                raise primary
        finally:
            self._lane_lock.release()

    def cancel(self):
        self.close()

    def _close_channel(self):
        self._objects.clear()
        self._closed = True
        primary = None
        control = getattr(self, "_control", None)
        if control is not None:
            try:
                control.close(deadline=self._close_deadline)
                self._control = None
            except BaseException as error:
                primary = error
        if getattr(self, "_persistent", False):
            worker = self._lane_worker
            try:
                if worker is not None:
                    worker.close(deadline=self._close_deadline)
            except BaseException as error:
                if primary is None:
                    primary = error
                else:
                    primary.finalization_secondary = (
                        *getattr(primary, "finalization_secondary", ()),
                        "worker_close",
                    )
            if worker is None or worker._proc is None:
                try:
                    if self.lightweight:
                        self._persistent = False
                        if primary is not None:
                            raise primary
                        return
                    resident_settle = getattr(self.ledger, "settle_resident", self.ledger.settle)
                    resident_settle(self._lane_lease)
                    self._persistent = False
                except BaseException as error:
                    if primary is None:
                        primary = error
                    else:
                        primary.finalization_secondary = (
                            *getattr(primary, "finalization_secondary", ()),
                            "resident_settlement",
                        )
        if primary is not None:
            raise primary

    def __enter__(self):
        if self._closed:
            raise RemoteIOError("closed production transport")
        return self

    def __exit__(self, exc_type, primary, traceback):
        try:
            self.close()
        except BaseException:
            if primary is None:
                raise
            primary.finalization_secondary = (
                *getattr(primary, "finalization_secondary", ()),
                "transport_close",
            )
        return False

    def clone(self, *, root=None):
        if self._closed:
            raise RemoteIOError("closed production transport")
        copy = RustProductionTransport(
            self.ledger,
            self.worker,
            origin=self.origin,
            token=self._token,
            same_origin_cookie=self._cookie,
            _test=self._test,
            root=self.root if root is None else root,
            capacity=self.capacity,
            offline_mode=self.offline_mode,
            _worker_pool=self._clone_pool or self._worker_pool,
        )
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
        matches = [
            o
            for o in self._objects.values()
            if (
                o.origin,
                o.repo_id,
                o.repo_type,
                o.object_path,
                o.revision,
                o.object_size,
                o.validator,
            )
            == (
                endpoint,
                repository,
                "modelscope_dataset_legacy",
                path,
                bound.immutable_revision,
                bound.size,
                bound.strong_etag,
            )
        ]
        obj = matches[0] if len(matches) == 1 else None
        if (
            obj is None
            or obj.origin != endpoint
            or obj.repo_id != repository
            or obj.revision != bound.immutable_revision
            or obj.object_size != bound.size
            or obj.validator != bound.strong_etag
            or self._condition_proof(proof_key(obj, test=self._test)) != expected_probe_sha256
        ):
            raise RemoteIOError("package requires independently verified Rust conditional binding")

    def predict_warm(self, identity, lengths):
        """Scheduling hint only; never authorizes IO or replaces ledger admission."""
        with self._lane_lock:
            if (
                self._closed
                or self._lane_failed
                or self._rotating
                or not getattr(self, "_persistent", False)
                or self._lane_worker is None
            ):
                return False
            matches = [obj for key, obj in self._objects.items() if key[:6] == identity]
            if len(matches) != 1 or proof_key(matches[0], test=self._test) not in self._live_proofs:
                return False
            from .prepared_fetch import StreamPlan

            if isinstance(lengths, StreamPlan):
                requests = lengths.chunk_count
                body = lengths.generation_body
                attempts = lengths.generation_attempts
            else:
                requests = len(lengths)
                body = checked_body_add(*(network_body_budget(n) for n in lengths))
                attempts = SESSION_ATTEMPTS_PER_TRANSFER * requests
            # Coordinator prediction cannot invoke the lane's owner-ledger RPC.
            # Actual admission refreshes policy and reserves; this is credit only.
            if self.lightweight:
                return self._lane_requests + requests + 3 <= 256
            credit = self._generation_budget(refresh=False)
            return (
                self._lane_requests + requests <= 256
                and self._lane_body + body <= credit["body"]
                and self._lane_attempts + attempts <= credit["attempts"]
            )

    def verified_object(self, candidate: ProviderObject, *, lengths=None):
        with self._lane_lock:
            if lengths is None:
                self._admit_generation(
                    checked_body_add(
                        network_body_budget(1),
                        network_body_budget(1),
                        network_body_budget(1, condition="wrong"),
                    ),
                    3 * SESSION_ATTEMPTS_PER_TRANSFER,
                    3,
                )
            else:
                from .prepared_fetch import StreamPlan

                if isinstance(lengths, StreamPlan):
                    self._admit_generation(
                        lengths.generation_body, lengths.generation_attempts, lengths.chunk_count
                    )
                else:
                    self._admit_generation(
                        checked_body_add(*(network_body_budget(n) for n in lengths)),
                        SESSION_ATTEMPTS_PER_TRANSFER * len(lengths),
                        len(lengths),
                    )
        try:
            return self._verified_object_body(candidate)
        except RemoteIOError:
            if not self.lightweight:
                raise
            return self.verify_conditions(candidate)

    def _verified_object_body(self, candidate: ProviderObject):
        """Resolve only this transport's live proof, matching full candidate identity."""
        if self._closed:
            raise RemoteIOError("closed production transport")
        candidate.validate(test=self._test)
        identity = (
            candidate.origin,
            candidate.repo_id,
            candidate.repo_type,
            candidate.revision,
            candidate.object_path,
            candidate.object_size,
        )
        matches = [
            (key, obj)
            for key, obj in self._objects.items()
            if key[:6] == identity
            and (candidate.validator is None or candidate.validator == obj.validator)
        ]
        if len(matches) != 1:
            raise RemoteIOError("fresh transport verified object absent or ambiguous")
        key, obj = matches[0]
        if self._condition_proof(proof_key(obj, test=self._test)) is None:
            raise RemoteIOError("verified object proof unavailable")
        if (
            getattr(self, "_persistent", False)
            and proof_key(obj, test=self._test) not in self._live_proofs
        ):
            raise RemoteIOError("fresh generation conditional proof required")
        self._objects.move_to_end(key)
        return obj

    def verified_bound_object(self, bound):
        """Resolve an exact provider URL and bound scope using this live transport."""
        return self.verified_object(self._bound_object(bound))

    def register(self, obj: ProviderObject):
        if self._closed:
            raise RemoteIOError("closed production transport")
        obj.validate(test=self._test)
        if (
            obj.origin != self.origin
            or self._condition_proof(proof_key(obj, test=self._test)) is None
        ):
            raise RemoteIOError("production object lacks verified conditional binding")
        key = (
            obj.origin,
            obj.repo_id,
            obj.repo_type,
            obj.revision,
            obj.object_path,
            obj.object_size,
            obj.validator,
        )
        self._objects[key] = obj
        self._objects.move_to_end(key)
        while len(self._objects) > 64:
            self._objects.popitem(last=False)

    def _owned_dir(self):
        if (
            any(p.is_symlink() or is_reparse(p) for p in (self.root, *self.root.parents))
            or (self.root.stat().st_dev, self.root.stat().st_ino) != self._root_identity
        ):
            raise RemoteIOError("Owned temporary root changed", lightweight=self.lightweight)
        root = self.root / ("rust-transfer-" + secrets.token_hex(16))
        root.mkdir(exist_ok=False)
        return root

    def _call(
        self, obj, root, *, start=0, length=1, condition="match", mode="range", json_limit=1 << 20
    ):
        if self._closed:
            raise RemoteIOError("closed production transport")
        secondary = ()
        worker_code = "worker_protocol"
        try:
            if self.lightweight:
                return self._call_lightweight(
                    obj,
                    root,
                    start=start,
                    length=length,
                    condition=condition,
                    mode=mode,
                    json_limit=json_limit,
                )
            return self._call_accounted(
                obj,
                root,
                start=start,
                length=length,
                condition=condition,
                mode=mode,
                json_limit=json_limit,
            )
        except RustWorkerError as error:
            worker_code = error.diagnostic_code
            # Preserve fixed cleanup evidence, never raw worker diagnostics.
            secondary = tuple(
                value
                for value in getattr(error, "finalization_secondary", ())
                if type(value) is str
                and value in {"worker_close", "worker_cancel", "worker_constructor_shutdown"}
            )[:16]
        converted = RemoteIOError(
            "Rust production request rejected"
            if self.lightweight
            else "Rust production request rejected; accounting uncertain",
            lightweight=self.lightweight,
            code=worker_code,
            phase="worker",
            accounting="UNKNOWN",
        )
        converted.finalization_secondary = secondary
        raise converted

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
        footprint = ProductionFootprint.admit(mode, size, capacity=self.capacity)
        memory, disk = footprint.memory, footprint.artifacts
        body_budget = network_body_budget(size, condition=condition)
        budget = {
            "body": body_budget,
            "attempts": SESSION_ATTEMPTS_PER_TRANSFER,
            "disk": disk,
            "inflight": memory,
        }
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
        if self._capacity_v2:
            payload.update(
                payload_revision=2,
                range_chunk_bytes=self.max_range_bytes,
                http_header_bytes=self.capacity.http_header_bytes,
            )
        # Register the actual child owner for both execution modes.
        transport = self

        class CorrectWorker(RustWorker):
            def __init__(self, *args, **kwargs):
                self._proc = None
                self._construction_ready = False
                transport._correct_worker = self
                if getattr(transport, "_persistent", False):
                    transport._lane_worker = self
                transport._request_worker = self
                try:
                    super().__init__(*args, **kwargs)
                finally:
                    self._construction_ready = hasattr(self, "_lifecycle_lock")
                if transport._closed:
                    self.cancel()
                    raise RustWorkerError("execution channel closed during construction")

        worker_type = CorrectWorker
        with self._execution_worker(worker_type, budget) as worker:
            self._request_worker = worker
            if not {"production_transfer_v2", "production_http_status_v1"}.issubset(
                worker.capabilities
            ):
                error = RustWorkerError("production capability unavailable")
                error.diagnostic_code = "WORKER_CAPABILITY_UNAVAILABLE"
                raise error
            leases = []
            try:
                for index in range(LEDGER_ATTEMPTS_PER_TRANSFER):
                    leases.append(
                        self.ledger.reserve(
                            Reservation(body=body_budget if index == 0 else 0, attempt=True)
                        )
                    )
            except BaseException as primary:
                for lease in leases:
                    try:
                        self.ledger.settle(lease)  # No network; admitted attempts stay charged.
                    except BaseException:
                        primary.finalization_secondary = (
                            *getattr(primary, "finalization_secondary", ()),
                            "reservation_rollback",
                        )
                raise
            lease1 = leases[0]
            request_id = uuid.uuid4().hex
            # Each logical transfer has its own finite budget; handshake keeps
            # the bridge default. The lane lock prevents overlapping requests.
            worker.timeout_s = PRODUCTION_RPC_TIMEOUT_S
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
                or not 0 <= accounting["attempts"] <= HTTP_ATTEMPTS_MAX
                or type(accounting["complete"]) is not bool
            ):
                raise RustWorkerError("production accounting invalid; leases pending")
            self._request_terminal = True
            try:
                self.ledger.consume_body(lease1, accounting["body"])
                if accounting["complete"]:
                    for lease in leases:
                        self.ledger.settle(lease)
            except BaseException as error:
                self._unresolved_network = True
                raise RemoteIOError(
                    "production network settlement uncertain",
                    code="network_ambiguous",
                    phase="worker",
                    accounting="UNKNOWN",
                ) from error
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
                        ("origin_status_retried", 2),
                        ("origin_status_exhausted", 1),
                    )
                    for value in (observation.get(key),)
                }
            for name in ("origin_status_retried", "origin_status_exhausted"):
                value = self.last_result.get("observation", {}).get(name)
                if type(value) is int:
                    setattr(self, "_" + name, getattr(self, "_" + name, 0) + value)
            if "production_error" in result:
                code = result["production_error"]
                self.last_result["production_error"] = (
                    code if isinstance(code, str) and code in _PUBLIC_ERROR_CODES else "rejected"
                )
            if not msg.get("ok") or "production_error" in result:
                self._http_status_candidate = None
                diagnostic = self.last_result["diagnostic"]
                status_code = self.last_result.get("production_error")
                if (
                    accounting["complete"]
                    and (status_code, diagnostic["phase"])
                    in {("origin_status", "origin"), ("cdn_status", "cdn")}
                    and type(diagnostic["http_status"]) is int
                    and diagnostic["http_status"]
                    != (
                        302
                        if diagnostic["phase"] == "origin"
                        else 412
                        if condition == "wrong"
                        else 206
                        if mode == "range"
                        else 200
                    )
                ):
                    self._http_status_candidate = {"code": status_code}
                # A valid terminal response is necessary, not sufficient, for reconciliation.
                diagnostic = self.last_result["diagnostic"]
                safe_code = self.last_result.get("production_error", "rejected")
                if (
                    not accounting["complete"]
                    and safe_code
                    in {
                        "origin_timeout",
                        "origin_connect",
                        "origin_request",
                        "origin_transport",
                        "cdn_timeout",
                        "cdn_connect",
                        "cdn_request",
                        "cdn_transport",
                    }
                    and safe_code.startswith(diagnostic["phase"] + "_")
                ):
                    self._conservative_candidate = {
                        "leases": tuple(leases),
                        "maximum": body_budget,
                        "observed": accounting["body"],
                        "used": False,
                        "code": safe_code,
                    }
                raise RemoteIOError(
                    "Rust production request rejected",
                    code=self.last_result.get("production_error", "rejected"),
                    phase=diagnostic["phase"],
                    http_status=diagnostic["http_status"],
                )
            if not accounting["complete"]:
                raise RustWorkerError("production body uncertain; leases pending")
            return result

    @contextmanager
    def transfer(self, obj, **kwargs):
        """Public two-field artifact interface retained for administrative callers."""
        with self._transfer_owned(obj, **kwargs) as (root, result, _):
            yield root, result

    @contextmanager
    def _operation(self):
        with self._lane_lock:
            if (
                self._closed
                or self._operation_active
                or (self._proof_group and self._proof_owner != threading.get_ident())
            ):
                raise RemoteIOError("execution channel lifecycle invalid")
            self._operation_active = True
            self._request_worker = None
            self._request_terminal = False
        try:
            yield
        except RemoteIOError as error:
            if self.lightweight:
                error.lightweight = True
            raise
        finally:
            primary = sys.exc_info()[1]
            with self._lane_lock:
                self._operation_active = False
                if self._closed:
                    try:
                        self._close_channel()
                    except BaseException:
                        if primary is None:
                            raise
                        primary.finalization_secondary = (
                            *getattr(primary, "finalization_secondary", ()),
                            "channel_close",
                        )

    @contextmanager
    def _transfer_owned(self, obj, **kwargs):
        with self._lane_lock:
            if not self._proof_group:
                size = (
                    kwargs.get("length", 1)
                    if kwargs.get("mode", "range") == "range"
                    else obj.object_size
                )
                self._admit_generation(
                    network_body_budget(size, condition=kwargs.get("condition", "match")),
                    SESSION_ATTEMPTS_PER_TRANSFER,
                    1,
                )
                if (
                    getattr(self, "_persistent", False)
                    and kwargs.get("condition", "match") == "match"
                    and proof_key(obj, test=self._test) not in self._live_proofs
                ):
                    if not self.lightweight:
                        raise RemoteIOError("fresh generation conditional proof required")
                    refreshed = self.verify_conditions(obj)
                    if refreshed != obj:
                        raise RemoteIOError("Generation validator changed", lightweight=True)
        with self._operation():
            with self._transfer_owned_body(obj, **kwargs) as result:
                yield result

    @contextmanager
    def _transfer_owned_body(
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
        if self.lightweight:
            with self._transfer_lightweight(
                obj,
                start=start,
                length=length,
                condition=condition,
                mode=mode,
                json_limit=json_limit,
                retain=retain,
            ) as result:
                yield result
            return
        size = length if mode == "range" else obj.object_size
        footprint = ProductionFootprint.admit(mode, size, capacity=self.capacity)
        memory, disk = footprint.memory, footprint.transfer_disk
        if getattr(self, "_persistent", False):
            memory -= (32 << 20) + protocolmemory(self.capacity)
        # Check profile/binding and working set before filesystem creation or network.
        if (
            condition == "match"
            and self.ledger.condition_proof(proof_key(obj, test=self._test)) is None
        ):
            raise RemoteIOError("verified production binding required")
        lease = self.ledger.reserve(Reservation(disk=disk))
        worker_lease = None
        worker_memory = (
            0
            if getattr(self, "_persistent", False) or mode != "range"
            else (32 << 20) + protocolmemory(self.capacity)
        )
        try:
            if worker_memory:
                worker_lease = self.ledger.reserve(Reservation(inflight=worker_memory))
            memory_lease = self.ledger.reserve(Reservation(inflight=memory - worker_memory))
        except BaseException as primary:
            for rollback in (worker_lease, lease):
                if rollback is None:
                    continue
                try:
                    self.ledger.settle(rollback)
                except BaseException:
                    primary.finalization_secondary = (
                        *getattr(primary, "finalization_secondary", ()),
                        "reservation_rollback",
                    )
            raise
        root = None
        raw = b""
        completed = False
        delivered_snapshot = None
        try:
            root = self._owned_dir()
            self._request_worker = None
            self._request_terminal = False
            self._conservative_candidate = None
            self._http_status_candidate = None
            self._transfer_finalizer_owner = True
            try:
                result = self._call(
                    obj,
                    root,
                    start=start,
                    length=length,
                    condition=condition,
                    mode=mode,
                    json_limit=json_limit,
                )
            finally:
                self._transfer_finalizer_owner = False
            body = root / "body"
            raw = b""
            if mode == "range":
                if condition == "wrong":
                    if body.stat().st_size != 0 or result.get("status") != 412:
                        raise RemoteIOError("negative condition verification failed")
                else:
                    raw = body.read_bytes()
                    if len(raw) != size or hashlib.sha256(raw).hexdigest() != result.get("sha256"):
                        raise RemoteIOError("Rust Range artifact verification failed")
                    if (
                        result.get("status") != 206
                        or result.get("etag") != obj.validator
                        or result.get("cdn_host") != obj.cdn_host
                    ) and condition == "match":
                        raise RemoteIOError("Rust Range conditional binding changed")
            else:
                if (
                    mode == "remote-stream-scan"
                    and body.exists()
                    or mode == "download-then-scan"
                    and body.stat().st_size != size
                    or _disk_usage(root) > disk
                ):
                    raise RemoteIOError("Rust stream artifact budget mismatch")
            delivered_snapshot = self._owned_snapshot(root)
            if delivered_snapshot is None:
                raise RemoteIOError("production artifact ownership unconfirmed")
            yield root, result, raw
            completed = True
        finally:
            primary = sys.exc_info()[1]
            first_secondary = None
            failed = False
            worker = getattr(self, "_request_worker", None)
            request_safe = (
                getattr(self, "_request_terminal", False) or worker is None or worker._proc is None
            )
            try:
                if not request_safe:
                    raise RemoteIOError("live request resources retained")
                try:
                    if worker_lease is not None:
                        if worker is None or worker._proc is None:
                            try:
                                self.ledger.settle(worker_lease)
                            except BaseException as worker_settle_error:
                                failed = True
                                first_secondary = worker_settle_error
                                if primary is not None:
                                    primary.finalization_secondary = (
                                        *getattr(primary, "finalization_secondary", ()),
                                        "worker_memory_settle",
                                    )
                        else:
                            failed = True
                            if primary is not None:
                                primary.finalization_secondary = (
                                    *getattr(primary, "finalization_secondary", ()),
                                    "worker_memory",
                                )
                    if primary is not None and raw:
                        # Escaping exceptions can retain payload through arbitrary
                        # frames/containers. Do not guess its object graph or refund
                        # the durable memory lease; network accounting is unchanged.
                        primary.production_payload_lease = memory_lease
                        primary.production_payload_resources = "PRESERVED"
                    else:
                        self.ledger.settle(memory_lease)
                except BaseException as payload_settle_error:
                    if first_secondary is None:
                        first_secondary = payload_settle_error
                    failed = True
                    if primary is not None:
                        primary.finalization_secondary = (
                            *getattr(primary, "finalization_secondary", ()),
                            "payload_memory_settle",
                        )
                finally:
                    # Always attempt ownership registration/cleanup, even if settlement rejects.
                    if root is None:
                        self.ledger.settle(lease)
                    else:
                        snapshot = self._owned_snapshot(root)
                        if delivered_snapshot is not None and snapshot != delivered_snapshot:
                            snapshot = None
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
                        else:
                            # A successful body is not successful finalization:
                            # unowned artifacts retain quota and fail explicitly.
                            raise RemoteIOError("production artifact ownership unconfirmed")
                        # Unknown/reparse/failed ownership stays on disk with quota.
            except BaseException as secondary:
                if first_secondary is None:
                    first_secondary = secondary
                failed = True
            candidate = getattr(self, "_conservative_candidate", None)
            if (
                primary is not None
                and isinstance(primary, RemoteIOError)
                and candidate is not None
                and not candidate["used"]
                and self._request_terminal
                and not failed
                and not raw
                and not retain
                and not getattr(primary, "finalization_secondary", ())
                and not getattr(primary, "production_payload_lease", None)
            ):
                # Sequential terminal envelope proves this request has unwound;
                # the persistent resident lease remains independently admitted.
                candidate["used"] = True
                try:
                    self.ledger.consume_body(
                        candidate["leases"][0], candidate["maximum"] - candidate["observed"]
                    )
                    for network_lease in candidate["leases"]:
                        self.ledger.settle(network_lease)
                except BaseException:
                    failed = True
                    primary.finalization_secondary = (
                        *getattr(primary, "finalization_secondary", ()),
                        "conservative_network_settle",
                    )
                else:
                    primary.accounting_state = "CONFIRMED"
                    primary.accounting_basis = "CONSERVATIVE_MAX_CHARGE"
                    primary.actual_consumption = "UNKNOWN"
                    primary.accounted = "CONSERVATIVE_MAX"
                    if not getattr(self, "_unresolved_network", False):
                        primary._conservative_finalized = _CONSERVATIVE_FINALIZED
            status_candidate = getattr(self, "_http_status_candidate", None)
            if (
                isinstance(primary, RemoteIOError)
                and status_candidate is not None
                and primary.code == status_candidate["code"]
                and not failed
                and not raw
                and not retain
                and not getattr(primary, "finalization_secondary", ())
                and not getattr(self, "_unresolved_network", False)
            ):
                primary.accounting_state = "CONFIRMED"
                primary._http_status_finalized = _HTTP_STATUS_FINALIZED
            if primary is not None and (
                not getattr(self, "_request_terminal", False)
                or failed
                or isinstance(primary, RemoteIOError)
                and primary.accounting_state == "UNKNOWN"
            ):
                # Earlier unresolved requests on this channel cannot be washed by
                # a later confirmed chunk. Historical ledger leases are separate.
                self._unresolved_network = True
            deferred = getattr(self, "_deferred_network_error", None)
            if deferred is not None:
                self._deferred_network_error = None
                if (
                    deferred is not primary
                    or failed
                    or (
                        getattr(primary, "_conservative_finalized", None)
                        is not _CONSERVATIVE_FINALIZED
                        and getattr(primary, "_http_status_finalized", None)
                        is not _HTTP_STATUS_FINALIZED
                    )
                ):
                    self._lane_failed = True
                    self._unresolved_network = True
                    try:
                        if self._lane_worker is not None:
                            self._lane_worker.cancel()
                    except BaseException:
                        if primary is not None:
                            primary.finalization_secondary = (
                                *getattr(primary, "finalization_secondary", ()),
                                "worker_cancel",
                            )
                        else:
                            raise
            if failed:
                if primary is None:
                    if first_secondary is not None and not isinstance(first_secondary, Exception):
                        raise first_secondary
                    raise RemoteIOError(
                        "production resource finalization incomplete; quota retained"
                    )
                primary.finalization_secondary = (
                    *getattr(primary, "finalization_secondary", ()),
                    "production_finalization",
                )

    def _owned_snapshot(self, root):
        try:
            if self.lightweight and (
                self.root.is_symlink()
                or is_reparse(self.root)
                or (self.root.stat().st_dev, self.root.stat().st_ino) != self._root_identity
            ):
                return None
            if not root.is_relative_to(self.root) or any(
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
            primary = sys.exc_info()[1]
            try:
                self.ledger.settle(lease)
            except BaseException:
                if primary is None:
                    raise RemoteIOError("download release finalization incomplete") from None
                primary.finalization_secondary = (
                    *getattr(primary, "finalization_secondary", ()),
                    "download_release_finalization",
                )

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
        return self._verify_conditions(candidate)

    def _verify_conditions_overlapped(self, candidate, metadata):
        """Frozen-scope observe only; actual metadata must pass before any binding."""
        if not self.lightweight or not self._persistent:
            raise RemoteIOError("Metadata overlap requires an owned lightweight lane")
        return self._verify_conditions(candidate, metadata=metadata)

    def _verify_conditions(self, candidate, *, metadata=None):
        with self._lane_lock:
            if self._operation_active or self._proof_group:
                raise RemoteIOError("execution generation already outstanding")
            self._admit_generation(
                checked_body_add(
                    network_body_budget(1),
                    network_body_budget(1),
                    network_body_budget(1, condition="wrong"),
                ),
                6,
                3,
            )
            self._proof_group = True
            self._proof_owner = threading.get_ident()
        try:
            result = (self._verify_conditions_body(candidate) if metadata is None else
                      self._verify_conditions_body(candidate, metadata=metadata))
            self._live_proofs.add(proof_key(result, test=self._test))
            return result
        except BaseException as error:
            if self.lightweight and isinstance(error, RemoteIOError):
                error.lightweight = True
            self._live_proofs.clear()
            self._objects.clear()
            confirmed = (
                isinstance(error, RemoteIOError)
                and error.accounting_state == "CONFIRMED"
                and (
                    getattr(error, "_conservative_finalized", None) is _CONSERVATIVE_FINALIZED
                    or getattr(error, "_http_status_finalized", None) is _HTTP_STATUS_FINALIZED
                )
                and not getattr(error, "finalization_secondary", ())
                and not getattr(self, "_unresolved_network", False)
            )
            if getattr(self, "_persistent", False) and not confirmed and not self.lightweight:
                self._lane_failed = True
            raise
        finally:
            with self._lane_lock:
                self._proof_group = False

    def _verify_conditions_body(self, candidate, *, metadata=None):
        with self.transfer(candidate, condition="observe") as (_, observed):
            validator, host, digest = observed["etag"], observed["cdn_host"], observed["sha256"]
        # The one-byte observation has no authority until the actual exact lookup
        # agrees with the frozen identity. It cannot replace provider metadata.
        if metadata is not None and metadata() != candidate:
            raise RemoteIOError("Frozen metadata identity mismatch",
                                code="provider_listing_incomplete", phase="provider_exact_lookup")
        bound = replace(candidate, validator=validator, cdn_host=host)
        # Capability test is not a production transfer: no proof exists yet.
        with self._capability_match(bound) as positive:
            if positive.get("status") != 206 or positive.get("sha256") != digest:
                raise RemoteIOError("conditional positive changed object")
        with self.transfer(bound, condition="wrong") as (_, negative):
            if negative.get("status") != 412 or negative.get("cdn_host") != bound.cdn_host:
                raise RemoteIOError("conditional negative unsupported")
        key = proof_key(bound, test=self._test)
        self._record_condition_proof(key, digest)
        self.register(bound)
        return bound

    @contextmanager
    def _capability_match(self, obj):
        with self._operation():
            with self._capability_match_body(obj) as result:
                yield result

    @contextmanager
    def _capability_match_body(self, obj):
        # Same bytes/header/size path; only the proof prerequisite differs.
        if self.lightweight:
            with self._transfer_lightweight(obj, _proof=True) as (_, result, _):
                yield result
            return
        footprint = ProductionFootprint.admit("range", 1, capacity=self.capacity)
        lease = self.ledger.reserve(Reservation(disk=footprint.transfer_disk))
        try:
            memory = footprint.memory - (
                ((32 << 20) + protocolmemory(self.capacity))
                if getattr(self, "_persistent", False)
                else 0
            )
            memory_lease = self.ledger.reserve(Reservation(inflight=memory))
        except BaseException as primary:
            try:
                self.ledger.settle(lease)
            except BaseException:
                primary.finalization_secondary = (
                    *getattr(primary, "finalization_secondary", ()),
                    "reservation_rollback",
                )
            raise
        root = None
        delivered_snapshot = None
        self._correct_worker = None
        self._track_correct_worker = True
        try:
            root = self._owned_dir()
            self._http_status_candidate = None
            self._request_terminal = False
            self._conservative_candidate = None
            self._capability_finalizer_owner = True
            try:
                result = self._call(obj, root, condition="match")
            finally:
                self._capability_finalizer_owner = False
            raw = (root / "body").read_bytes()
            if len(raw) != 1 or hashlib.sha256(raw).hexdigest() != result.get("sha256"):
                raise RemoteIOError("conditional capability bytes invalid")
            delivered_snapshot = self._owned_snapshot(root)
            if delivered_snapshot is None or set(delivered_snapshot[1]) != {"body"}:
                raise RemoteIOError("conditional artifact ownership unconfirmed")
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
                artifact_finished = delivered_snapshot is not None and primary is None
                status_terminal = getattr(
                    self, "_http_status_candidate", None
                ) is not None and getattr(self, "_request_terminal", False)
                if stopped or (
                    getattr(self, "_persistent", False) and (artifact_finished or status_terminal)
                ):
                    try:
                        self.ledger.settle(memory_lease)
                    finally:
                        if root is None:
                            self.ledger.settle(lease)
                        else:
                            snapshot = self._owned_snapshot(root)
                            if (
                                snapshot is not None
                                and set(snapshot[1]) <= {"body"}
                                and (delivered_snapshot is None or snapshot == delivered_snapshot)
                            ):
                                self._delete_owned(root, snapshot)
                                self.ledger.settle(lease)
                            else:
                                failed = True
                            # Unknown artifacts retain disk quota, never dead worker RAM.
                else:
                    failed = True  # Live/unknown worker retains both quota and artifacts.
            except BaseException as secondary:
                self._unresolved_network = True
                if primary is None and not isinstance(secondary, Exception):
                    raise
                failed = True
            status = getattr(self, "_http_status_candidate", None)
            confirmed_status = (
                isinstance(primary, RemoteIOError)
                and status is not None
                and primary.code == status["code"]
                and not failed
                and not getattr(primary, "finalization_secondary", ())
                and not getattr(self, "_unresolved_network", False)
            )
            if confirmed_status:
                primary.accounting_state = "CONFIRMED"
                primary._http_status_finalized = _HTTP_STATUS_FINALIZED
            if getattr(self, "_deferred_network_error", None) is not None:
                self._deferred_network_error = None
                if not confirmed_status:
                    self._lane_failed = True
                    try:
                        if self._lane_worker is not None:
                            self._lane_worker.cancel()
                    except BaseException:
                        if primary is not None:
                            primary.finalization_secondary = (
                                *getattr(primary, "finalization_secondary", ()),
                                "worker_cancel",
                            )
                        else:
                            raise
            if (primary is not None and not confirmed_status) or failed:
                # Capability/proof requests share this channel's uncertainty latch.
                # Clearing bindings or later reproof cannot erase unresolved quota.
                self._unresolved_network = True
            if failed:
                if primary is None:
                    raise RemoteIOError(
                        "conditional resource finalization incomplete; quota retained"
                    )
                primary.finalization_secondary = (
                    *getattr(primary, "finalization_secondary", ()),
                    "conditional_finalization",
                )

    def _bound_object(self, bound):
        from urllib.parse import parse_qs, urlsplit

        paths = parse_qs(urlsplit(bound.url).query).get("FilePath", [])
        matches = [
            o
            for o in self._objects.values()
            if len(paths) == 1
            and o.object_path == paths[0]
            and o.origin == self.origin
            and o.revision == bound.immutable_revision
            and o.object_size == bound.size
            and o.validator == bound.strong_etag
            and bound.repository_id is not None
            and o.repo_id == bound.repository_id.id
            and o.repo_type == "modelscope_dataset_legacy"
        ]
        obj = matches[0] if len(matches) == 1 else None
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
    def _shared_range_guard(self, bound, length):
        """Pin this lane's own live generation while borrowing immutable bytes."""
        if not self.lightweight or not 0 < length <= self.max_range_bytes:
            raise RemoteIOError("Shared Range guard unavailable", lightweight=True)
        with self._lane_lock:
            obj = self._bound_object(bound)
            self._admit_lightweight_generation(1)
            if proof_key(obj, test=self._test) not in self._live_proofs:
                if self.verify_conditions(obj) != obj:
                    raise RemoteIOError("Shared Range validator changed", lightweight=True)
            generation, owner = self._generation, threading.get_ident()

            def check():
                worker = self._lane_worker
                if (self._closed or self._lane_failed or self._operation_active
                        or self._generation != generation or threading.get_ident() != owner
                        or worker is None or not worker._alive or worker._proc is None
                        or worker._proc.poll() is not None
                        or self._verified_object_body(obj) != obj):
                    raise RemoteIOError("Shared Range lane proof unavailable", lightweight=True)

            check()
            yield obj, check
            check()

    @contextmanager
    def read_range_owned(self, bound, start, length):
        obj = self._bound_object(bound)
        if length == 0:
            yield b""
            return
        if not 0 < length <= self.max_range_bytes:
            raise RemoteIOError("production Range exceeds bound")
        with self._lane_lock:
            self._admit_generation(network_body_budget(length), SESSION_ATTEMPTS_PER_TRANSFER, 1)
            needs_proof = (
                getattr(self, "_persistent", False)
                and proof_key(obj, test=self._test) not in self._live_proofs
            )
        if needs_proof:
            refreshed = self.verify_conditions(obj)
            if refreshed != obj:
                raise RemoteIOError("chunk generation validator changed")
        with self._transfer_owned(obj, start=start, length=length) as (_, _, raw):
            yield raw

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
