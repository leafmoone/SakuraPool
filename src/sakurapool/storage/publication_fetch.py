from __future__ import annotations

import hashlib
import os
import secrets
import stat
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit

from ..capacity import CapacityConfig
from .bounded_json import VALIDATION_INFLIGHT_BYTES, validate_file
from .budget import BudgetExceeded, BudgetLedger, Reservation
from .modelscope import ModelScopeDataset, _io_error
from .prepared_fetch import stream_plan
from .production import (
    _CONSERVATIVE_FINALIZED,
    _HTTP_STATUS_FINALIZED,
    ProviderObject,
    RustProductionTransport,
)
from .publication import PublicationCorrupt
from .retrieval import _publish_directory, _real_output_root
from .transport import BoundObject, GuardedTransport, RemoteIOError


class PublicationFetchError(RemoteIOError):
    """Safe operation failure; no raw underlying exception or secret text."""

    _MESSAGES = {
        "publication_image_sha": "image SHA mismatch; no delivery",
        "publication_write": "publication write failed",
        "publication_publish": "publication publish failed",
        "publication_accounting": "publication settlement unconfirmed",
        "publication_range": "publication range failed",
        "publication_metadata": "metadata JSON validation failed",
    }

    def __init__(
        self,
        code,
        *,
        delivered,
        cleanup_safe,
        output_lease,
        secondary,
        underlying=None,
        member_kind=None,
        chunk_index=None,
    ):
        super().__init__(self._MESSAGES[code], code=code, phase="publication_fetch")
        self.underlying = {}
        if isinstance(underlying, RemoteIOError):
            from .transport import _SAFE_CODES, _SAFE_PHASES

            if underlying.code in _SAFE_CODES and underlying.phase in _SAFE_PHASES:
                self.underlying = {
                    "cause_code": underlying.code,
                    "cause_phase": underlying.phase,
                    "cause_accounting": (
                        underlying.accounting_state
                        if underlying.accounting_state in ("CONFIRMED", "UNKNOWN")
                        else "UNKNOWN"
                    ),
                }
                if type(underlying.http_status) is int and 100 <= underlying.http_status <= 599:
                    self.underlying["cause_http_status"] = underlying.http_status
        if member_kind in ("image", "metadata"):
            self.underlying["member_kind"] = member_kind
        if type(chunk_index) is int and 0 <= chunk_index < 1 << 64:
            self.underlying["chunk_index"] = chunk_index
        self.validation_failed = code == "publication_metadata"
        self.delivery_published = delivered
        self.cleanup_safe = cleanup_safe
        conservative = (
            not delivered
            and cleanup_safe
            and output_lease == "CONFIRMED"
            and not secondary
            and isinstance(underlying, RemoteIOError)
            and underlying.accounting_state == "CONFIRMED"
            and getattr(underlying, "_conservative_finalized", None) is _CONSERVATIVE_FINALIZED
            and getattr(underlying, "accounting_basis", None) == "CONSERVATIVE_MAX_CHARGE"
            and not getattr(underlying, "finalization_secondary", ())
            and not getattr(underlying, "production_payload_lease", None)
        )
        known_status = (
            not delivered
            and cleanup_safe
            and output_lease == "CONFIRMED"
            and not secondary
            and isinstance(underlying, RemoteIOError)
            and underlying.accounting_state == "CONFIRMED"
            and getattr(underlying, "_http_status_finalized", None) is _HTTP_STATUS_FINALIZED
            and underlying.code in {"origin_status", "cdn_status"}
            and not getattr(underlying, "finalization_secondary", ())
            and not getattr(underlying, "production_payload_lease", None)
        )
        self.accounting_state = "CONFIRMED" if conservative or known_status else "UNKNOWN"
        if conservative:
            self.accounting_basis = "CONSERVATIVE_MAX_CHARGE"
            self.actual_consumption = "UNKNOWN"
            self.accounted = "CONSERVATIVE_MAX"
        self.output_lease_state = output_lease
        self.finalization_errors = tuple(secondary)

    def public_diagnostic(self):
        return {
            **super().public_diagnostic(),
            **self.underlying,
            "delivery": "PUBLISHED" if self.delivery_published else "NOT_PUBLISHED",
            "accounting": self.accounting_state,
            "output_lease": self.output_lease_state,
            "accounting_scope": "OPERATION",
            "cleanup": "SAFE" if self.cleanup_safe else "PRESERVED",
            "secondary": list(self.finalization_errors),
            "recoverable": self.accounting_state == "CONFIRMED",
        }


@contextmanager
def _owned_range(transport, bound, offset, size, state):
    """Keep body-primary classification even if transport exit also fails."""
    state["code"] = "publication_range"
    body_primary = None
    try:
        with transport.read_range_owned(bound, offset, size) as payload:
            try:
                yield payload
            except BaseException as error:
                body_primary = error
                state["body_error_code"] = state["code"]
                raise
            finally:
                state["code"] = "publication_range"
    except BaseException as error:
        if body_primary is not None and error is not body_primary:
            state["range_secondary"] = True
            raise body_primary from None
        raise


def exact_provider_lookup(control, endpoint, repo_id, revision, path, size, digest):
    provider = ModelScopeDataset(control, endpoint, repo_id)
    hub = provider.legacy_hub_id()
    root = path.rpartition("/")[0] or "/"
    found = provider.find_legacy_file(hub, revision, root=root, path=path)
    if (
        found.size != size
        or found.revision_candidate != revision
        or found.provider_sha256 != digest
    ):
        raise _io_error(
            provider,
            "provider exact identity/digest mismatch",
            code="provider_shape",
            phase="provider_exact_lookup",
        )
    return ProviderObject.from_tree(provider, found)


def fetch_publication_sample(
    pub,
    record_id,
    transport,
    output,
    metadata=False,
    control=None,
    scope=None,
    attempt_hook=None,
    capacity=None,
    image_extensions=None,
):
    from .publication import Publication

    if not isinstance(pub, Publication) or pub._closed or not pub.full_verified:
        raise PublicationCorrupt("fetch requires full verified publication")
    return _fetch_publication_sample(
        pub,
        record_id,
        transport,
        output,
        metadata=metadata,
        control=control,
        scope=scope,
        attempt_hook=attempt_hook,
        capacity=capacity,
        image_extensions=image_extensions,
    )


def _fetch_publication_sample(
    pub,
    record_id,
    transport,
    output,
    metadata=False,
    control=None,
    scope=None,
    attempt_hook=None,
    capacity=None,
    image_extensions=None,
):
    if pub._closed or not pub.full_verified:
        raise PublicationCorrupt("fetch requires full verified publication")
    capacity = (
        capacity if capacity is not None else getattr(transport, "capacity", CapacityConfig())
    )
    if not isinstance(capacity, CapacityConfig):
        raise ValueError("typed capacity required")
    if getattr(transport, "capacity", capacity) != capacity:
        raise ValueError("fetch/transport capacity mismatch")
    if control is not None and getattr(control, "capacity", capacity) != capacity:
        raise ValueError("fetch/control capacity mismatch")
    if not isinstance(record_id, str) or len(record_id) != 32:
        raise PublicationCorrupt("record identity")
    try:
        record = bytes.fromhex(record_id)
    except ValueError as e:
        raise PublicationCorrupt("record identity") from e
    rt = pub.runtime
    rid = rt.resolve_record(record.hex()).rid
    loc = rt.location(rid)
    idx = loc["object_idx"]
    r = pub.catalog.execute(
        "SELECT r.endpoint,r.repo_id,r.repo_type,o.revision_candidate,o.object_path,"
        "o.object_size,o.content_sha256,o.provider_sha256,o.fetchable "
        "FROM objects o JOIN repositories r USING(repo_idx) WHERE object_idx=?",
        (idx,),
    ).fetchone()
    if r is None or r[8] != 1 or r[7] != r[6] or not isinstance(r[7], bytes) or len(r[7]) != 32:
        raise PublicationCorrupt("BLOCKED_PROVIDER_DIGEST_UNAVAILABLE")
    endpoint, repo, repo_type, revision, path, size_blob, content, digest, _ = r
    size = int.from_bytes(size_blob, "big")
    plan = stream_plan(loc, size, metadata=metadata, capacity=capacity)
    if plan.chunk_bytes > getattr(transport, "max_range_bytes", 8 << 20):
        raise PublicationCorrupt("transport range capacity unavailable")
    from ..image_formats import image_filename

    try:
        filename = image_filename(rt.image_format(loc["format_id"]), image_extensions)
    except ValueError:
        raise PublicationCorrupt("image format") from None
    suffix = filename.removeprefix("image")
    if scope is not None and (
        scope.origin,
        scope.repo_id,
        scope.repo_type,
        scope.revision,
        scope.object_path,
        scope.object_size,
    ) != (endpoint, repo, repo_type, revision, path, size):
        raise PublicationCorrupt("production profile scope mismatch")
    ro = rt.object_ref(idx)
    if (
        ro["object_path"] != path
        or ro["object_size"] != size
        or ro["object_version"] != content.hex()
    ):
        raise PublicationCorrupt("runtime locator mismatch")
    ledger = transport.ledger
    from ..tasks.pipeline import LedgerRPC

    if not isinstance(ledger, (BudgetLedger, LedgerRPC)) or (
        not ledger.offline_mode
        and (not isinstance(transport, RustProductionTransport) or not transport.production_profile)
    ):
        raise BudgetExceeded("explicit Rust production profile required")
    workspace = getattr(ledger, "workspace", None)
    if workspace is not None and workspace.capacity != capacity:
        raise ValueError("workspace/fetch capacity mismatch")
    if not ledger.offline_mode:
        if not isinstance(transport, RustProductionTransport) or transport.origin != endpoint:
            raise PublicationCorrupt("credential origin mismatch")
        if control is not None:
            raise PublicationCorrupt("external production control rejected")
    output = _real_output_root(Path(output), ledger)
    key = (pub.content_digest, rt.snapshot_id, idx, transport, ledger)
    obj = pub._verified.get(key)
    if getattr(transport, "_closed", False):
        raise RemoteIOError("closed production transport")
    if obj is not None and isinstance(transport, RustProductionTransport):
        try:
            # Only the next chunk is admitted: a complete image need not fit a generation.
            lengths = [min(plan.image_bytes, plan.chunk_bytes)]
            if transport.verified_object(obj, lengths=lengths) != obj:
                raise RemoteIOError("publication verified object changed")
        except RemoteIOError:
            pub._verified.pop(key, None)
            obj = None
    if attempt_hook is not None:
        attempt_hook("NETWORK_START", {})
    if obj is None:
        owned = control is None
        reused = owned and getattr(transport, "_persistent", False)
        if reused:
            control = transport.metadata_control()
        elif owned:
            control = GuardedTransport(
                ledger,
                trusted_hosts=frozenset({urlsplit(endpoint).hostname}),
                token=transport._token,
                credential_origin=endpoint,
                same_origin_cookie=transport._cookie,
                capacity=capacity,
            )
        try:
            obj = exact_provider_lookup(control, endpoint, repo, revision, path, size, digest.hex())
        finally:
            if owned and not reused:
                control.close()
        if obj.repo_type != repo_type:
            raise PublicationCorrupt("repository type mismatch")
        obj = transport.verify_conditions(obj)
        pub._verified[key] = obj
        while len(pub._verified) > 64:
            pub._verified.popitem(last=False)
    else:
        pub._verified.move_to_end(key)
    bound = BoundObject(
        ModelScopeDataset(transport, obj.origin, obj.repo_id).download_url(
            obj.revision, obj.object_path
        ),
        size,
        obj.revision,
        obj.validator,
        repository=obj.repo_id,
    )
    image_size, meta_size = plan.image_bytes, plan.metadata_bytes
    final = output / record_id
    if final.exists() or final.is_symlink():
        raise FileExistsError("never overwrite output")
    lease = ledger.reserve(
        Reservation(disk=plan.saved_bytes + 8192, saved_samples=1, saved_bytes=plan.saved_bytes)
    )
    stage = output / (".publication-fetch-" + secrets.token_hex(16))
    created, receipts = {}, {}
    stage_identity = None
    delivered = False
    state = {"code": "publication_write", "body_error_code": None}
    try:
        if attempt_hook is not None:
            attempt_hook("OUTPUT_RESERVED", {"lease": lease})
        stage.mkdir()
        stage_stat = stage.lstat()
        stage_identity = (stage_stat.st_dev, stage_stat.st_ino)
        image_digest = hashlib.sha256()
        # A single exclusive writer; hash is accumulated inside each payload lease.
        with (stage / ("image" + suffix)).open("xb") as f:
            created["image" + suffix] = _file_identity(f)
            for chunk_index, (offset, length) in enumerate(plan.chunks()):
                state.update(member_kind="image", chunk_index=chunk_index)
                with _owned_range(transport, bound, offset, length, state) as payload:
                    if len(payload) != length:
                        raise RemoteIOError("image exact extent")
                    image_digest.update(payload)
                    if offset + length == plan.image_offset + image_size:
                        if image_digest.digest() != pub.expected_image_sha(rid):
                            state["code"] = "publication_image_sha"
                            raise RemoteIOError("image SHA mismatch; no delivery")
                    state["code"] = "publication_write"
                    f.write(payload)
                del payload
            state["code"] = "publication_write"
            f.flush()
            os.fsync(f.fileno())
        receipts["image" + suffix] = {
            "sha256": image_digest.hexdigest(),
            "bytes": image_size,
            "identity": list(created["image" + suffix]),
        }
        if metadata and loc["flags"] & 1:
            meta_digest = hashlib.sha256()
            state["code"] = "publication_write"
            with (stage / "metadata.json").open("xb") as f:
                created["metadata.json"] = _file_identity(f)
                for chunk_index, (offset, length) in enumerate(plan.chunks(metadata=True)):
                    state.update(member_kind="metadata", chunk_index=chunk_index)
                    with _owned_range(transport, bound, offset, length, state) as payload:
                        if len(payload) != length:
                            raise RemoteIOError("metadata exact extent")
                        meta_digest.update(payload)
                        state["code"] = "publication_write"
                        f.write(payload)
                    del payload
                state["code"] = "publication_write"
                f.flush()
                os.fsync(f.fileno())
            state["code"] = "publication_metadata"
            parse_lease = ledger.reserve(Reservation(inflight=VALIDATION_INFLIGHT_BYTES))
            try:
                validate_file(
                    stage / "metadata.json", meta_size, max_bytes=capacity.metadata_max_bytes
                )
            except BaseException as error:
                # Preserve UNKNOWN admission on any escaping validator failure.
                # In particular, allocation/IO failures can retain scratch in traceback.
                error.production_payload_lease = parse_lease
                error.production_payload_resources = "PRESERVED"
                raise
            else:
                ledger.settle(parse_lease)
            receipts["metadata.json"] = {
                "sha256": meta_digest.hexdigest(),
                "bytes": meta_size,
                "identity": list(created["metadata.json"]),
                "verification": "BOUNDED_JSON_NO_PUBLICATION_SHA",
            }
        state["code"] = "publication_publish"
        if attempt_hook is not None:
            attempt_hook(
                "PREPARED", {"receipt": receipts, "directory_identity": list(stage_identity)}
            )
        _publish_directory(stage, final)
        delivered = True
        if attempt_hook is not None:
            attempt_hook("PUBLISHED", {})
        state["code"] = "publication_accounting"
        ledger.settle(lease, saved_samples=1, saved_bytes=plan.saved_bytes)
        if attempt_hook is not None:
            attempt_hook("SETTLED", {"output_lease": "CONFIRMED"})
        return final
    except BaseException as primary:
        finalization_errors = ["RANGE_FINALIZATION_FAILED"] if state.get("range_secondary") else []
        safe = False
        if not delivered:
            try:
                safe = _cleanup_owned_stage(stage, stage_identity, created)
            except BaseException:
                finalization_errors.append("CLEANUP_FAILED")
            if safe:
                try:
                    ledger.settle(lease)
                except BaseException:
                    finalization_errors.append("ACCOUNTING_UNKNOWN")
        output_lease = (
            "UNKNOWN"
            if delivered or not safe or "ACCOUNTING_UNKNOWN" in finalization_errors
            else "CONFIRMED"
        )
        if isinstance(primary, Exception):
            raise PublicationFetchError(
                state["body_error_code"] or state["code"],
                delivered=delivered,
                cleanup_safe=safe,
                output_lease=output_lease,
                secondary=finalization_errors,
                underlying=primary,
                member_kind=state.get("member_kind"),
                chunk_index=state.get("chunk_index"),
            ) from None
        primary.publication_state = PublicationFetchError(
            state["body_error_code"] or state["code"],
            delivered=delivered,
            cleanup_safe=safe,
            output_lease=output_lease,
            secondary=finalization_errors,
            underlying=primary,
            member_kind=state.get("member_kind"),
            chunk_index=state.get("chunk_index"),
        ).public_diagnostic()
        raise


def _content_sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_identity(stream):
    value = os.fstat(stream.fileno())
    return value.st_dev, value.st_ino


def _cleanup_owned_stage(stage, identity, created):
    """Remove only still-owned plain entries, preserving unknown/replaced files."""
    if identity is None:
        return False
    from ..fs_safety import plain_entry

    try:
        plain_entry(stage, directory=True)
    except ValueError:
        return False
    value = stage.lstat()
    if not stat.S_ISDIR(value.st_mode) or (value.st_dev, value.st_ino) != identity:
        return False
    entries = {entry.name: entry for entry in stage.iterdir()}
    if set(entries) != set(created):
        return False
    for name, entry in entries.items():
        value = entry.lstat()
        if (
            not stat.S_ISREG(value.st_mode)
            or getattr(value, "st_file_attributes", 0) & 0x400
            or (value.st_dev, value.st_ino) != created[name]
        ):
            return False
    for entry in entries.values():
        entry.unlink()
    stage.rmdir()
    return True
