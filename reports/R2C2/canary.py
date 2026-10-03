"""C2 small-canary orchestration: formal control/binding/stage/P2/runtime.

No standalone real entrypoint, no provider response/validator/URL serialization.
An actual canary candidate must come from the guarded provider tree, and must
get its OWN formal proof. Discovery avoids a whole-repository traversal.
"""

import json
import math
import uuid
from dataclasses import replace
from itertools import zip_longest
from pathlib import Path

CONFIGURED_REPOSITORY = "leafmoone/webdataset_danbooru_v3"
# Repository API prefix intentionally differs from logical adapter source "gamecg".
CONFIGURED_ROOTS = ("gc5m",)
CONFIGURED_DATASET = "gamecg_v3"


def configured_adapter(config_path=None):
    """Load the checked-in formal registry; override only the remote storage binding."""
    from sakurapool.registry import AdapterRegistry

    config = (
        Path(config_path)
        if config_path is not None
        else Path(__file__).resolve().parents[2] / "examples/webdataset-danbooru-v3.json"
    )
    registry = AdapterRegistry.from_dict(json.loads(config.read_text(encoding="utf-8")))
    return replace(registry.get(CONFIGURED_DATASET), storage_id="modelscope-c2")


def builder_admission(ledger, object_size, mode, *, binding_needed=False):
    """Side-effect-free preflight of actual product phase reservations, not a lease.

    ledger.status includes physical allocations AND existing future reservations.
    Product builders must still perform their own live reservations. Remote never
    adds object_size to disk; Download does. No convenient TAR-size cutoff.
    """
    from sakurapool.storage.production_resources import (
        NEGATIVE_CONDITION_BODY_CAP,
        ProductionFootprint,
    )
    from sakurapool.storage.remote_index import OFFLINE_BUILD_ALLOWANCE

    footprint = ProductionFootprint.admit(mode, object_size)
    used = ledger.status()
    additions = {
        "disk": max(footprint.admin_peak(OFFLINE_BUILD_ALLOWANCE), OFFLINE_BUILD_ALLOWANCE),
        "inflight": footprint.memory,
        "body": object_size
        + 1
        + (2 + 2 + NEGATIVE_CONDITION_BODY_CAP + 1 if binding_needed else 0),
        "attempts": 2 + (6 if binding_needed else 0),
    }
    blocked = [key for key, amount in additions.items() if used[key] + amount > ledger.limits[key]]
    return {
        "admitted": not blocked,
        "blocked_resources": blocked,
        "additional_reservations": additions,
        "used_including_physical_and_pending": {key: used[key] for key in additions},
        "hard_limits": {key: ledger.limits[key] for key in additions},
        "is_live_reservation": False,
        "product_rechecks_required": True,
    }


def discovery(ledger, token, scheduler, identity, *, repository, roots=("/", "pre")):
    from sakurapool.storage.modelscope import MAX_PAGES, ModelScopeDataset
    from sakurapool.storage.production import ProviderObject
    from sakurapool.storage.transport import GuardedTransport

    class Control(GuardedTransport):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.logical = self.attempts = self.accepted = 0
            self.tree_total = None
            self.directory_paths = []

        def _once(self, *args, **kwargs):
            self.attempts += 1
            return super()._once(*args, **kwargs)

        def read_metadata(self, *args, **kwargs):
            self.logical += 1
            raw = super().read_metadata(*args, **kwargs)
            self.accepted += len(raw)
            data = json.loads(raw).get("Data")
            self.tree_total = None
            self.directory_paths = []
            if isinstance(data, dict):
                from sakurapool.storage.modelscope import _is_canonical_path

                files = data.get("Files")
                if isinstance(files, list) and len(files) <= 200:
                    self.directory_paths = [
                        item["Path"]
                        for item in files
                        if isinstance(item, dict)
                        and item.get("Type") in ("tree", "directory")
                        and isinstance(item.get("Path"), str)
                        and len(item["Path"]) <= 512
                        and _is_canonical_path(item["Path"])
                    ]
                value = data.get("TotalCount", data.get("Total"))
                if type(value) is int and value >= 0:
                    self.tree_total = value
            return raw

    selected = None
    observed_tars = 0
    with Control(
        ledger,
        trusted_hosts=frozenset({"modelscope.cn"}),
        token=token,
        credential_origin=scheduler.core.ORIGIN,
        same_origin_cookie="m_session_id=" + token,
    ) as control:
        provider = ModelScopeDataset(control, scheduler.core.ORIGIN, repository)

        def hub_action():
            hub = provider.legacy_hub_id()
            return hub, {
                "status": "PASS",
                "hub_id": hub,
                "control_logical_reads": control.logical,
                "control_attempts": control.attempts,
                "metadata_accepted_bytes": control.accepted,
            }

        hub, report = scheduler.execute_evidenced(
            ledger, "canary_discovery_hub", identity, hub_action, public={"repo_id": repository}
        )
        if report["status"] != "PASS":
            return None, report
        # First and tail page of known roots only, not a giant full listing.
        for root in roots:
            page = 1
            while True:
                before = (control.logical, control.attempts, control.accepted)

                def page_action():
                    files, complete = provider.legacy_tree_page(
                        hub, "master", root=root, page=page, page_size=200
                    )
                    choices = sorted(
                        (f for f in files if f.path.endswith(".tar") and f.size >= 1024),
                        key=lambda f: f.size,
                    )
                    # Evaluate ALL candidates in this bounded page, not just the min-five
                    # displayed in evidence. Keep only typed canonical identity summaries.
                    evaluated = [
                        (
                            f,
                            builder_admission(
                                ledger, f.size, "remote-stream-scan", binding_needed=True
                            ),
                        )
                        for f in choices
                    ]
                    admitted = [f for f, admission in evaluated if admission["admitted"]]
                    candidate = (
                        ProviderObject.from_tree(provider, admitted[0]) if admitted else None
                    )
                    details = {
                        "status": "PASS",
                        "files_examined": len(files),
                        "listing_complete": complete,
                        "tar_candidates": len(choices),
                        "remote_admitted_candidates": len(admitted),
                        "minimum_tar_bytes": choices[0].size if choices else None,
                        "maximum_tar_bytes": choices[-1].size if choices else None,
                        "minimum_candidates": [
                            {
                                "object_path": f.path,
                                "object_size": f.size,
                                "revision_candidate": f.revision_candidate,
                            }
                            for f in choices[:5]
                        ],
                        "candidate_evaluation_complete_for_page": True,
                        "candidate_size_cutoff": None,
                        "directory_paths_observed": control.directory_paths,
                        "directory_coverage_limited": True,
                        "recursive_listing_requested": True,
                        "listing_total_declared": control.tree_total,
                        "control_logical_reads": control.logical - before[0],
                        "control_attempts": control.attempts - before[1],
                        "metadata_accepted_bytes": control.accepted - before[2],
                    }
                    if candidate:
                        details.update(
                            object_path=candidate.object_path,
                            object_size=candidate.object_size,
                            revision_candidate=candidate.revision,
                            remote_admission=builder_admission(
                                ledger,
                                candidate.object_size,
                                "remote-stream-scan",
                                binding_needed=True,
                            ),
                            download_admission=builder_admission(
                                ledger,
                                candidate.object_size,
                                "download-then-scan",
                                binding_needed=True,
                            ),
                        )
                    elif choices:
                        details["minimum_remote_admission"] = evaluated[0][1]
                    return candidate, details

                selected, report = scheduler.execute_evidenced(
                    ledger,
                    "canary_discovery_page",
                    identity,
                    page_action,
                    public={"repo_id": repository, "root": root, "page": page},
                )
                if report["status"] != "PASS":
                    return None, report
                observed_tars += report.get("tar_candidates", 0)
                if selected:
                    return selected, report
                tail = math.ceil((control.tree_total or 0) / 200)
                if page != 1 or not 1 < tail <= MAX_PAGES:
                    break
                page = tail
    return None, {
        **report,
        "origin_status": report["status"],
        "status": "CAPACITY_OR_BUDGET_BLOCKED" if observed_tars else "DISCOVERY_NOT_IDENTIFIED",
        "scope_limited": True,
        "tar_candidates_examined": observed_tars,
    }
    # Not proof that no small object exists anywhere in the repository.


def audit_small_sample(stage, p2_root, adapter):
    import hashlib
    import sqlite3
    from contextlib import closing
    from pathlib import PurePosixPath

    import pyarrow.parquet as pq

    from sakurapool.metadata import normalize_nested
    from sakurapool.storage.production_stage import _json_bounded
    from sakurapool.storage.remote_index import _configure_stage_reader

    checked = 0
    selected = []
    flags = {
        key: False
        for key in (
            "source_post_id_present",
            "caption_present",
            "tags_present",
            "tag_categories_present",
            "format_present",
            "dimensions_present",
        )
    }
    with closing(sqlite3.connect(f"file:{stage.database.as_posix()}?mode=ro", uri=True)) as db:
        _configure_stage_reader(db)
        for path in sorted(p2_root.rglob("*.samples.parquet")):
            parquet = pq.ParquetFile(path)
            if parquet.num_row_groups == 0:
                continue  # Legitimate footer-only input; strict inventory is checked separately.
            for batch in parquet.iter_batches(batch_size=3):
                for row in batch.to_pylist():
                    if not row["json_path"]:
                        raise ValueError("canary selected record lacks JSON metadata")
                    member = db.execute(
                        "SELECT offset_data,size,sha256 FROM members WHERE name=?",
                        (row["image_path"],),
                    ).fetchone()
                    if member != (row["offset_data"], row["size"], row["sha256"]):
                        raise ValueError("P2 image extent/hash differs from audited stage")
                    if row["json_path"]:
                        if not 0 < row["json_size"] <= adapter.max_json_bytes:
                            raise ValueError("P2 JSON exceeds configured adapter cap")
                        metadata = db.execute(
                            "SELECT offset_data,size,sha256,json_payload FROM members WHERE name=?",
                            (row["json_path"],),
                        ).fetchone()
                        if (
                            metadata is None
                            or metadata[:2] != (row["json_offset_data"], row["json_size"])
                            or len(metadata[3]) != metadata[1]
                            or metadata[1] > adapter.max_json_bytes
                            or hashlib.sha256(metadata[3]).hexdigest() != metadata[2]
                        ):
                            raise ValueError("P2 JSON extent/hash audit missing")
                        values = _json_bounded(metadata[3])
                        if adapter.metadata_mode == "nested_json_v1":
                            normalized = normalize_nested(
                                values,
                                adapter,
                                row["post_id"],
                                PurePosixPath(row["image_path"]).suffix,
                            )
                            expected = (
                                normalized.text,
                                normalized.tags,
                                normalized.width,
                                normalized.height,
                                normalized.image_format,
                            )
                            actual = (
                                row["text"],
                                row["tags"],
                                row["width"],
                                row["height"],
                                row["image_format"],
                            )
                            if expected != actual:
                                raise ValueError("P2 metadata mapping differs from actual JSON")
                        elif values.get(adapter.text_field) != row["text"]:
                            raise ValueError("P2 flat text mapping differs from actual JSON")
                    if row["source"] != adapter.source:
                        raise ValueError("P2 configured source differs")
                    selected.append(row)
                    flags["source_post_id_present"] |= bool(row["source"] and row["post_id"])
                    flags["caption_present"] |= bool(row["text"])
                    flags["tags_present"] |= bool(row["tags"])
                    flags["tag_categories_present"] |= any(
                        tag.get("category") for tag in (row["tags"] or [])
                    )
                    flags["format_present"] |= bool(row["image_format"])
                    flags["dimensions_present"] |= bool(row["width"] and row["height"])
                    checked += 1
                    if checked >= 3:
                        return {
                            "records_audited": checked,
                            "audit_batch_rows": 3,
                            "audit_json_cap_bytes": adapter.max_json_bytes,
                            "json_content_sha_independently_verified": True,
                            "image_sha_matches_same_rust_stage": True,
                            "image_pixel_sha_independently_verified": False,
                            **flags,
                        }, selected
    return {
        "records_audited": checked,
        "audit_batch_rows": 3,
        "audit_json_cap_bytes": adapter.max_json_bytes,
        "json_content_sha_independently_verified": checked > 0,
        "image_sha_matches_same_rust_stage": checked > 0,
        "image_pixel_sha_independently_verified": False,
        **flags,
    }, selected


def identify_adapter(transport, candidate, scheduler, identity):
    """Diagnostic-only, nonauthoritative and not required by configured canaries.

    Read only bounded TAR headers and one JSON via registered-proof Range.

    Recognize the formal nested_json_v1 source.dataset/id/image schema only
    after observing a matching actual image header. This limited standard-header
    discovery is NOT a TAR validity certificate; the formal Rust scan and P2
    audit remain authoritative. PAX/GNU extensions/unsupported layouts and local
    limits remain NOT_IDENTIFIED, not a claim of invalid WebDataset.
    """
    import tarfile
    from pathlib import PurePosixPath

    from sakurapool.metadata import MetadataInvalid, SourceMismatch, normalize_nested
    from sakurapool.registry import DatasetAdapter
    from sakurapool.storage.budget import BudgetExceeded, Reservation
    from sakurapool.storage.modelscope import ModelScopeDataset
    from sakurapool.storage.production_resources import STREAM_MEMORY
    from sakurapool.storage.production_stage import _json_bounded
    from sakurapool.storage.transport import BoundObject, RemoteIOError

    try:
        obj = transport.verified_object(candidate)
    except RemoteIOError as exc:
        raise ValueError("own verified proof scope required") from exc
    bound = BoundObject(
        ModelScopeDataset(transport, obj.origin, obj.repo_id).download_url(
            obj.revision, obj.object_path
        ),
        obj.object_size,
        obj.revision,
        obj.validator,
        repository=obj.repo_id,
    )

    def action():
        offset = examined = 0
        lease = None
        completed = [None, {"status": "ADAPTER_NOT_IDENTIFIED"}]
        images = {}
        pending = None  # At most one decoded JSON <=1MiB, not a whole-member table.
        image_extensions = (".jpg", ".jpeg", ".png", ".webp", ".avif")

        def complete(value, details):
            completed[:] = value, details
            return value, details

        def unidentified(reason):
            return complete(
                None,
                {
                    "status": "ADAPTER_NOT_IDENTIFIED",
                    "failure_kind": reason,
                    "scope_limited": True,
                    "headers_examined": examined,
                },
            )

        try:
            lease = transport.ledger.reserve(Reservation(inflight=STREAM_MEMORY))
            while offset + 512 <= obj.object_size and examined < 64:
                # Formal registered-proof transport accounts and admits EVERY extra Range.
                with transport.read_range_owned(bound, offset, 512) as raw:
                    header = tarfile.TarInfo.frombuf(raw, "utf-8", "strict")
                examined += 1
                name = PurePosixPath(header.name)
                padded_end = offset + 512 + ((header.size + 511) // 512) * 512
                if (
                    header.type not in (tarfile.REGTYPE, tarfile.AREGTYPE)
                    or len(header.name.encode("utf-8")) > 4096
                    or header.size < 0
                    or padded_end > obj.object_size
                    or name.is_absolute()
                    or ".." in name.parts
                    or str(name) != header.name
                    or "\\" in header.name
                ):
                    return unidentified("unsupported_header_or_extent")
                stem, extension = str(name.with_suffix("")), name.suffix.lower()
                if extension in image_extensions:
                    if stem in images:
                        return unidentified("ambiguous_image_pair")
                    images[stem] = extension
                elif extension == ".json":
                    if not 0 < header.size <= 1 << 20 or not name.stem.isdigit():
                        return unidentified("metadata_identification_scope")
                    if pending is None:
                        with transport.read_range_owned(
                            bound, offset + 512, header.size
                        ) as payload:
                            pending = (stem, name.stem, header.size, _json_bounded(payload))
                if pending is not None and pending[0] in images:
                    _, post_id, json_size, meta = pending
                    provenance = (
                        meta.get("source", {}).get("dataset")
                        if (isinstance(meta, dict) and isinstance(meta.get("source"), dict))
                        else None
                    )
                    if not isinstance(provenance, str) or not provenance:
                        return unidentified("metadata_schema_unrecognized")
                    adapter = DatasetAdapter(
                        candidate.repo_id,
                        provenance,
                        storage_id="modelscope-c2",
                        metadata_mode="nested_json_v1",
                        numeric_post_id=True,
                        allowed_provenance=(provenance,),
                    )
                    normalize_nested(meta, adapter, post_id, images[pending[0]])
                    return complete(
                        adapter,
                        {
                            "status": "PASS",
                            "schema": "nested_json_v1",
                            "basis": "actual_json_source_dataset_and_matching_image_header",
                            "headers_examined": examined,
                            "json_size": json_size,
                            "source_field_observed": True,
                            "schema_consistent": True,
                            "source_policy_independently_verified": False,
                            "image_header_observed": True,
                            "tar_validity_certified": False,
                        },
                    )
                offset = padded_end
            return unidentified("identification_limit_or_missing_pair")
        except BudgetExceeded:
            return complete(None, {"status": "CAPACITY_BLOCKED", "failure_kind": "capacity"})
        except RemoteIOError:
            return complete(
                None,
                {
                    "status": "BLOCKED",
                    "failure_kind": "provider_or_network",
                    "observation": scheduler.core.public_result(transport.last_result),
                },
            )
        except (tarfile.HeaderError, UnicodeError, MetadataInvalid, SourceMismatch, ValueError):
            return unidentified("header_or_metadata_not_identified")
        finally:
            if lease is not None:
                settle_preserving(transport.ledger, lease, completed[1])

    return scheduler.execute_evidenced(
        transport.ledger,
        "canary_schema_identification",
        identity,
        action,
        public={"repo_id": candidate.repo_id, "object_path": candidate.object_path},
    )


def validate_canary_rows(root, summary, *, offline_fixture=True):
    """Stream count validation; this is not a native-memory admission theorem.

    A one-row batch does not bound native Arrow row-group decoder memory.
    Configured readiness performs durable reopen and audit without P3 compilation.
    """
    import pyarrow.parquet as pq

    total = rows = 0
    counts = {}
    for name in ("objects", "samples", "annotations", "errors"):
        count = summary.get(name)
        if type(count) is not int or count < 0:
            raise ValueError("canary decoded record count scope")
        table_rows = 0
        for path in sorted(root.rglob("*." + name + ".parquet")):
            parquet = pq.ParquetFile(path)
            if parquet.num_row_groups == 0:
                continue  # PyArrow 18 iter_batches cannot read footer-only zero-group input.
            for batch in parquet.iter_batches(batch_size=1):
                for row in batch.to_pylist():
                    size = len(json.dumps(row, ensure_ascii=True).encode("ascii"))
                    total += size
                    rows += 1
                    table_rows += 1
        if table_rows != count:
            raise ValueError("P2 full-table count differs from builder summary")
        counts[name] = table_rows
    return {
        "rows": rows,
        "serialized_bytes": total,
        "full_table_counts_verified": counts,
        "all_rows_streamed": True,
        "arrow_batch_rows": 1,
        "compiler_general_memory_theorem": False,
        "offline_functional_fixture_only": offline_fixture,
    }


def settle_preserving(ledger, lease, details):
    """Settlement failure is additive; never replace the typed primary cause."""
    try:
        ledger.settle(lease)
    except Exception:
        details["primary_status"] = details["status"]
        details["origin_status"] = details["status"]
        details["settlement_failed"] = True
        details["status"] = "SETTLEMENT_BLOCKED"


def public_summary(summary):
    """Keep every nonnegative protocol count without a convenient sample cutoff."""
    return {
        key: value
        for key, value in summary.items()
        if key in ("objects", "samples", "annotations", "errors")
        and type(value) is int
        and value >= 0
    }


def build_one(transport, candidate, adapter, mode, scheduler, identity, *, network_readiness=False):
    from sakurapool.runtime.compiler import compile_runtime
    from sakurapool.runtime.inventory import load_p2_inventory
    from sakurapool.runtime.query import RuntimeQuerySpec
    from sakurapool.runtime.snapshot import RuntimeSnapshot
    from sakurapool.storage.budget import BudgetExceeded, Reservation
    from sakurapool.storage.modelscope import ModelScopeDataset
    from sakurapool.storage.production_resources import ProductionFootprint
    from sakurapool.storage.remote_index import OFFLINE_BUILD_ALLOWANCE, write_staged_v4
    from sakurapool.storage.rust_index import RustScanAuditError
    from sakurapool.storage.transport import BoundObject, RemoteIOError

    ledger = transport.ledger
    try:
        obj = transport.verified_object(candidate)
    except RemoteIOError as exc:
        raise ValueError("own verified proof scope required") from exc
    if (
        replace(candidate, validator=obj.validator, cdn_host=obj.cdn_host) != obj
        or candidate.validator is not None
        and candidate.validator != obj.validator
        or candidate.cdn_host is not None
        and candidate.cdn_host != obj.cdn_host
    ):
        raise ValueError("canary candidate differs from its own verified proof scope")
    bound = BoundObject(
        ModelScopeDataset(transport, obj.origin, obj.repo_id).download_url(
            obj.revision, obj.object_path
        ),
        obj.object_size,
        obj.revision,
        obj.validator,
        repository=obj.repo_id,
    )
    job = ledger.root / ("r2c2-canary-" + uuid.uuid4().hex)
    footprint = ProductionFootprint.admit(mode, obj.object_size)

    def action():
        admission = builder_admission(ledger, obj.object_size, mode)
        if not admission["admitted"]:
            return None, {
                "status": "CAPACITY_BLOCKED",
                "failure_kind": "capacity",
                "admission": admission,
                "network_requests": 0,
            }
        # build_stage/write_staged_v4 each maintain their OWN product admission.
        # Do not claim a reserve-immediately-settle probe covers either lifetime.
        job.mkdir(exist_ok=False)
        transport.last_result = {}
        validation_lease = None
        completed = [None, {"status": "BLOCKED", "failure_kind": "unknown_unclassified"}]

        def complete(value, details):
            completed[:] = (value, details)
            return value, details

        try:
            stage = transport.build_stage(obj, adapter, job / "stage", mode=mode)
            summary = write_staged_v4(
                ledger,
                [(obj.object_path, bound, stage)],
                job / "p2",
                adapter,
                production_transport=transport,
            )
            transport.release_committed_downloads(job / "p2")
            if not ledger.offline_mode and not network_readiness:
                # Formal production scan and write_staged_v4 have finished under
                # their own bounded gates. Do not run an unproven extra Arrow
                # row-group decoder, inventory reload, sampled audit or compiler
                # merely to estimate if those operations might fit 128MiB.
                good = summary["objects"] == 1 and summary["samples"] > 0 and summary["errors"] == 0
                return complete(
                    job / "p2",
                    {
                        "status": "COMPILER_INPUT_CAPACITY_BLOCKED" if good else "BLOCKED",
                        "failure_kind": "runtime_native_workspace_unproven"
                        if good
                        else "scan_or_p2_errors",
                        "summary": {
                            key: summary[key]
                            for key in ("objects", "samples", "annotations", "errors")
                        },
                        "formal_scan_complete": True,
                        "formal_p2_write_complete": True,
                        "scan_p2_verified": good,
                        "scan_mode": mode,
                        "p2_format": 4,
                        "whole_sha256": stage.content_sha256,
                        "members": stage.members,
                        "whole_tar_spool": mode == "download-then-scan",
                        "durable_path": str(job / "p2"),
                        "runtime_verified": False,
                        "extra_arrow_decode_performed": False,
                        "compiler_executed": False,
                        "inventory_reopened": False,
                        "sample_metadata_audit_performed": False,
                        "runtime_bounded_general_proof": False,
                    },
                )
            validation_lease = ledger.reserve(
                Reservation(disk=OFFLINE_BUILD_ALLOWANCE, inflight=footprint.memory)
            )
            small_rows = validate_canary_rows(
                job / "p2", summary, offline_fixture=ledger.offline_mode
            )
            inventory = load_p2_inventory(job / "p2", _row_batch_size=1)
            details = {
                "status": "PASS",
                "summary": public_summary(summary),
                "small_canary_input_bounds": small_rows,
                "inventory_verified": True,
                "whole_sha256": stage.content_sha256,
                "members": stage.members,
                "whole_tar_spool": mode == "download-then-scan",
                "durable_path": str(job / "p2"),
                "runtime_verified": False,
            }
            flags, selected = audit_small_sample(stage, job / "p2", adapter)
            details.update(flags)
            if summary["samples"] == 0 or summary["errors"] != 0:
                details.update(status="BLOCKED", failure_kind="canary_record_scope")
                return complete(job / "p2", details)
            details.update(
                inventory_reopened=True,
                formal_scan_complete=True,
                formal_p2_write_complete=True,
                scan_p2_verified=True,
                p2_format=4,
            )
            if network_readiness:
                details.update(
                    runtime="NOT_REQUIRED_FOR_NETWORK_READINESS",
                    compiler_executed=False,
                    inventory_batch_rows=1,
                    json_container_memory_bound_proven=False,
                    validation_memory_reservation_bytes=footprint.memory,
                    inventory_rows_materialized=False,
                    inventory_object_fragment_descriptors_materialized=True,
                    runtime_bounded_general_proof=False,
                )
                return complete(job / "p2", details)
            details["offline_functional_fixture_only"] = True
            compile_runtime(inventory, job / "runtime")
            with RuntimeSnapshot.open(job / "runtime", full_verify=True) as rt:
                count = rt.query(RuntimeQuerySpec()).count()
                ref = rt.object_ref(0)
                if count != summary["samples"] or ref["object_size"] != obj.object_size:
                    raise ValueError("runtime count/object extent differs from P2")
                for row in selected:
                    resolved = rt.resolve_one(row["source"], row["post_id"], row["dataset_id"])
                    location = rt.location(resolved.rid)
                    if resolved.record_id != row["record_id"] or any(
                        location[key] != row[value]
                        for key, value in (
                            ("image_offset", "offset_data"),
                            ("image_size", "size"),
                            ("metadata_offset", "json_offset_data"),
                            ("metadata_size", "json_size"),
                        )
                    ):
                        raise ValueError("runtime rid/record/image/JSON binding differs from P2")
                details.update(
                    runtime_verified=True,
                    runtime_format=2,
                    runtime_query_samples=count,
                    runtime_object_size=ref["object_size"],
                    rid_record_locations_verified=len(selected),
                    runtime_bounded_general_proof=False,
                )
            return complete(job / "p2", details)
        except MemoryError:
            return complete(
                None,
                {
                    "status": "OOM_BLOCKED",
                    "failure_kind": "memory_error",
                    "operation_failed": True,
                },
            )
        except BudgetExceeded:
            return complete(None, {"status": "CAPACITY_BLOCKED", "failure_kind": "capacity"})
        except RustScanAuditError:
            return complete(None, {"status": "BLOCKED", "failure_kind": "scan_or_webdataset_audit"})
        except RemoteIOError:
            if transport.last_result.get("production_error") == "metadata_limit":
                return complete(
                    None,
                    {
                        "status": "PRODUCTION_METADATA_CAPACITY_BLOCKED",
                        "failure_kind": "production_metadata_limit",
                        "production_json_cap_bytes": 1 << 20,
                        "adapter_json_cap_bytes": adapter.max_json_bytes,
                    },
                )
            public = scheduler.core.public_result(transport.last_result)
            kind = (
                "provider_or_network"
                if public.get("http_status") or public.get("cdn_http_status")
                else "transport_unverified"
            )
            return complete(
                None, {"status": "BLOCKED", "failure_kind": kind, "observation": public}
            )
        except Exception:
            return complete(
                None,
                {
                    "status": "BLOCKED",
                    "failure_kind": "unknown_unclassified",
                    "observation": scheduler.core.public_result(transport.last_result),
                },
            )
        finally:
            if validation_lease is not None:
                settle_preserving(ledger, validation_lease, completed[1])

    return scheduler.execute_evidenced(
        ledger,
        "real_network_builder",
        identity,
        action,
        public={
            "mode": mode,
            "repo_id": obj.repo_id,
            "object_path": obj.object_path,
            "object_size": obj.object_size,
            "revision_candidate": obj.revision,
        },
    )


def closure_result(
    scheduler, report, phase, *, remote="NOT_RUN", download="NOT_RUN", equivalence="NOT_AVAILABLE"
):
    return {
        **scheduler.outcome(report["status"], report),
        "origin_phase": phase,
        "remote": remote,
        "download": download,
        "equivalence": equivalence,
    }


def closure(
    initial_transport,
    initial_candidate,
    token,
    scheduler,
    identity,
    *,
    resume_discovery=False,
    repository=None,
    roots=("/", "pre"),
    adapter=None,
):
    """Ordered closure; terminal control reasons must never become business failure."""
    if not resume_discovery:
        report = scheduler.proof_use(initial_transport, initial_candidate, identity)
        if report["status"] != "PASS":
            return closure_result(scheduler, report, "initial_registered_range")
    # Resume after prior successful binding+proof-use evidence: never replay the
    # large initial object's requests; it does NOT authorize the new candidate.
    candidate, report = discovery(
        initial_transport.ledger,
        token,
        scheduler,
        identity,
        repository=repository if repository is not None else initial_candidate.repo_id,
        roots=roots,
    )
    if report["status"] != "PASS" or candidate is None:
        return closure_result(scheduler, report, "discovery")
    transport, report = scheduler.schedule(initial_transport.ledger, token, candidate, identity)
    if report["status"] != "PASS":
        return closure_result(scheduler, report, "own_binding")
    report = scheduler.proof_use(transport, candidate, identity)
    if report["status"] != "PASS":
        return closure_result(scheduler, report, "own_registered_range")
    adapter_explicit = adapter is not None
    if adapter is None:
        adapter, report = identify_adapter(transport, candidate, scheduler, identity)
        if report["status"] != "PASS":
            return closure_result(scheduler, report, "schema_identification")
    remote, report = build_one(
        transport,
        candidate,
        adapter,
        "remote-stream-scan",
        scheduler,
        identity,
        **({"network_readiness": True} if adapter_explicit else {}),
    )
    if (
        report["status"] == "COMPILER_INPUT_CAPACITY_BLOCKED"
        and report.get("scan_p2_verified") is True
    ):
        # Full Remote scan/P2 permits independent Download admission, NOT runtime
        # execution or a full semantic equivalence certificate.
        remote_report = report
        download, download_report = build_one(
            transport, candidate, adapter, "download-then-scan", scheduler, identity
        )
        result = {
            **download_report,
            **closure_result(
                scheduler,
                download_report,
                "download_builder",
                remote=remote_report["status"],
                download=download_report["status"],
            ),
        }
        result.update(
            remote_scan_p2_verified=True,
            download_scan_p2_verified=download_report.get("scan_p2_verified") is True,
            remote_runtime="NOT_RUN",
            download_runtime="NOT_RUN",
            inventory_reopened=False,
            full_semantic_equivalence_performed=False,
            scalar_summaries_equal=(
                remote_report["summary"] == download_report.get("summary")
                if download_report.get("scan_p2_verified") is True
                else None
            ),
            scalar_summary_equality_is_not_semantic_equivalence=True,
            runtime_resource_gate_blocked=True,
        )
        # Preserve every unexpected/terminal Download cause and its diagnostics.
        # Only the two deliberately supported nonfatal outcomes are summarized as
        # runtime-resource BLOCKED. Sticky source/ledger/settlement/operation flags
        # take precedence even if a draft report incorrectly labels itself capacity.
        terminal = any(
            download_report.get(key)
            for key in (
                "identity_mismatch",
                "ledger_snapshot_failed",
                "settlement_failed",
                "operation_failed",
            )
        )
        legitimate = download_report["status"] == "CAPACITY_BLOCKED" or (
            download_report["status"] == "COMPILER_INPUT_CAPACITY_BLOCKED"
            and download_report.get("scan_p2_verified") is True
        )
        if legitimate and not terminal:
            result.update(status="BLOCKED", origin_phase="runtime_resource_gate")
        return result
    if report["status"] != "PASS":
        return closure_result(scheduler, report, "remote_builder", remote=report["status"])
    download, report = build_one(
        transport,
        candidate,
        adapter,
        "download-then-scan",
        scheduler,
        identity,
        **({"network_readiness": True} if adapter_explicit else {}),
    )
    if report["status"] != "PASS":
        result = closure_result(
            scheduler, report, "download_builder", remote="PASS", download=report["status"]
        )
        if report["status"] == "CAPACITY_BLOCKED" and not any(
            report.get(key)
            for key in (
                "identity_mismatch",
                "ledger_snapshot_failed",
                "settlement_failed",
                "operation_failed",
            )
        ):
            result["status"] = "REMOTE_READY_DOWNLOAD_CAPACITY_BLOCKED"
        return result
    _, report = scheduler.execute_evidenced(
        transport.ledger,
        "canary_equivalence",
        identity,
        lambda: (None, equivalent(transport.ledger, remote, download)),
    )
    return closure_result(
        scheduler,
        report,
        "equivalence",
        remote="PASS",
        download="PASS",
        equivalence=report["status"],
    )


def equivalent(ledger, remote, download):
    from sakurapool.storage.budget import BudgetExceeded, Reservation
    from sakurapool.storage.production_resources import STREAM_MEMORY
    from sakurapool.storage.remote_index import OFFLINE_BUILD_ALLOWANCE

    lease = None
    details = {"status": "BLOCKED", "failure_kind": "unknown_unclassified"}
    try:
        lease = ledger.reserve(Reservation(disk=OFFLINE_BUILD_ALLOWANCE, inflight=STREAM_MEMORY))
        # Both inputs must already satisfy validate_canary_rows from build_one.
        matched = _equivalent_rows(remote, download)
        details = {"status": "PASS" if matched else "FAIL", "equivalent": matched}
    except MemoryError:
        details = {
            "status": "OOM_BLOCKED",
            "failure_kind": "memory_error",
            "operation_failed": True,
        }
    except BudgetExceeded:
        details = {"status": "CAPACITY_BLOCKED", "failure_kind": "capacity"}
    except Exception:
        pass  # Preserve only typed unknown cause, never raw exception/chain.
    finally:
        if lease is not None:
            settle_preserving(ledger, lease, details)
    return details


def _equivalent_rows(remote, download):
    """Compare ALL typed rows including extent/hash/content with one-row batches."""
    import pyarrow.parquet as pq

    names = ("samples", "annotations", "errors", "objects")
    for name in names:
        left = sorted(remote.rglob("*." + name + ".parquet"))
        right = sorted(download.rglob("*." + name + ".parquet"))
        if not left or not right:
            return False

        def rows(paths):
            for path in paths:
                parquet = pq.ParquetFile(path)
                if parquet.num_row_groups == 0:
                    continue
                for batch in parquet.iter_batches(batch_size=1):
                    for row in batch.to_pylist():
                        yield row

        absent = object()
        if any(a != b for a, b in zip_longest(rows(left), rows(right), fillvalue=absent)):
            return False
    return True
