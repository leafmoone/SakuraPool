"""Fresh object digest/proof then random Range; no persisted ETag or signed URL."""

from __future__ import annotations

import hashlib
import os
import secrets
from pathlib import Path

from .budget import BudgetExceeded, BudgetLedger, Reservation
from .modelscope import MAX_PAGES, ModelScopeDataset
from .production import ProviderObject, RustProductionTransport
from .publication import PublicationCorrupt
from .retrieval import EXTENSIONS, _publish_directory, _real_output_root
from .transport import BoundObject, GuardedTransport, RemoteIOError


def exact_provider_lookup(control, endpoint, repo_id, revision, path, size, digest):
    provider = ModelScopeDataset(control, endpoint, repo_id)
    hub = provider.legacy_hub_id()
    root = path.rpartition("/")[0] or "/"
    for page in range(1, MAX_PAGES + 1):
        files, complete = provider.legacy_tree_page(
            hub, revision, root=root, page=page, page_size=200
        )
        found = [f for f in files if f.path == path]
        if found:
            if (
                len(found) != 1
                or found[0].size != size
                or found[0].revision_candidate != revision
                or found[0].provider_sha256 != digest
            ):
                raise RemoteIOError("provider exact identity/digest mismatch")
            return ProviderObject.from_tree(provider, found[0])
        if complete or not files:
            break
    raise RemoteIOError("provider exact object unavailable")


def fetch_publication_sample(
    pub, record_id, transport, output, metadata=False, control=None, scope=None
):
    if pub._closed or not pub.full_verified:
        raise PublicationCorrupt("fetch requires full verified publication")
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
    if not isinstance(ledger, BudgetLedger) or (
        not ledger.offline_mode
        and (not isinstance(transport, RustProductionTransport) or not transport.production_profile)
    ):
        raise BudgetExceeded("explicit Rust production profile required")
    if not ledger.offline_mode:
        if not isinstance(transport, RustProductionTransport) or transport.origin != endpoint:
            raise PublicationCorrupt("credential origin mismatch")
        if control is not None:
            raise PublicationCorrupt("external production control rejected")
    output = _real_output_root(Path(output), ledger)
    key = (pub.content_digest, rt.snapshot_id, idx, transport, ledger)
    obj = pub._verified.get(key)
    if getattr(transport, '_closed', False):
        raise RemoteIOError("closed production transport")
    if obj is not None and isinstance(transport, RustProductionTransport):
        try:
            if transport.verified_object(obj) != obj:
                raise RemoteIOError("publication verified object changed")
        except RemoteIOError:
            pub._verified.pop(key, None)
            obj = None
    if obj is None:
        owned = control is None
        if owned:
            control = GuardedTransport(
                ledger,
                trusted_hosts=frozenset({"modelscope.cn"}),
                token=transport._token,
                credential_origin=endpoint,
                same_origin_cookie=transport._cookie,
            )
        try:
            obj = exact_provider_lookup(control, endpoint, repo, revision, path, size, digest.hex())
        finally:
            if owned:
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
    offset, image_size = loc["image_offset"], loc["image_size"]
    limit = getattr(transport, "max_range_bytes", 8 << 20)
    if image_size <= 0 or image_size > limit or offset + image_size > size:
        raise PublicationCorrupt("image extent")
    suffix = "." + rt.image_format(loc["format_id"])
    if suffix not in EXTENSIONS:
        raise PublicationCorrupt("image format")
    meta_size = loc["metadata_size"] if metadata and loc["flags"] & 1 else 0
    if meta_size > limit or loc["metadata_offset"] + meta_size > size:
        raise PublicationCorrupt("metadata extent")
    final = output / record_id
    if final.exists() or final.is_symlink():
        raise FileExistsError("never overwrite output")
    lease = ledger.reserve(
        Reservation(
            disk=image_size + meta_size + 8192, saved_samples=1, saved_bytes=image_size + meta_size
        )
    )
    stage = output / (".publication-fetch-" + secrets.token_hex(16))
    created = set()
    try:
        stage.mkdir()
        with transport.read_range_owned(bound, offset, image_size) as payload:
            if (
                len(payload) != image_size
                or hashlib.sha256(payload).digest() != pub.expected_image_sha(rid)
            ):
                raise RemoteIOError("image SHA mismatch; no delivery")
            with (stage / ("image" + suffix)).open("xb") as f:
                created.add("image" + suffix)
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
        if metadata and loc["flags"] & 1:
            if meta_size:
                with transport.read_range_owned(
                    bound, loc["metadata_offset"], meta_size
                ) as payload:
                    if len(payload) != meta_size:
                        raise RemoteIOError("metadata exact extent")
                    with (stage / "metadata.json").open("xb") as f:
                        created.add("metadata.json")
                        f.write(payload)
                        f.flush()
                        os.fsync(f.fileno())
            else:
                with (stage / "metadata.json").open("xb") as f:
                    created.add("metadata.json")
                    f.flush()
                    os.fsync(f.fileno())
        _publish_directory(stage, final)
        ledger.settle(lease, saved_samples=1, saved_bytes=image_size + meta_size)
        return final
    except BaseException:
        if (
            stage.is_dir()
            and not stage.is_symlink()
            and {p.name for p in stage.iterdir()} == created
        ):
            for name in created:
                (stage / name).unlink()
            stage.rmdir()
            ledger.settle(lease)
        raise
