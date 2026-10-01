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


def discovery(ledger, token, scheduler, identity, *, repository, roots=("/", "pre")):
    from sakurapool.storage.modelscope import MAX_PAGES, ModelScopeDataset
    from sakurapool.storage.production import ProviderObject
    from sakurapool.storage.transport import GuardedTransport

    class Control(GuardedTransport):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.logical = self.attempts = self.accepted = 0
            self.tree_total = None

        def _once(self, *args, **kwargs):
            self.attempts += 1
            return super()._once(*args, **kwargs)

        def read_metadata(self, *args, **kwargs):
            self.logical += 1
            raw = super().read_metadata(*args, **kwargs)
            self.accepted += len(raw)
            data = json.loads(raw).get("Data")
            self.tree_total = None
            if isinstance(data, dict):
                value = data.get("TotalCount", data.get("Total"))
                if type(value) is int and value >= 0:
                    self.tree_total = value
            return raw

    selected = None
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
                        (
                            f
                            for f in files
                            if f.path.endswith(".tar") and 1024 <= f.size <= 64 * (1 << 20)
                        ),
                        key=lambda f: f.size,
                    )
                    candidate = ProviderObject.from_tree(provider, choices[0]) if choices else None
                    details = {
                        "status": "PASS",
                        "files_examined": len(files),
                        "listing_complete": complete,
                        "small_candidates": len(choices),
                        "control_logical_reads": control.logical - before[0],
                        "control_attempts": control.attempts - before[1],
                        "metadata_accepted_bytes": control.accepted - before[2],
                    }
                    if candidate:
                        details.update(
                            object_path=candidate.object_path,
                            object_size=candidate.object_size,
                            revision_candidate=candidate.revision,
                        )
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
                if selected:
                    return selected, report
                tail = math.ceil((control.tree_total or 0) / 200)
                if page != 1 or not 1 < tail <= MAX_PAGES:
                    break
                page = tail
    return None, {
        **report,
        "origin_status": report["status"],
        "status": "DISCOVERY_NOT_IDENTIFIED",
        "scope_limited": True,
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
                        metadata = db.execute(
                            "SELECT offset_data,size,sha256,json_payload FROM members WHERE name=?",
                            (row["json_path"],),
                        ).fetchone()
                        if (
                            metadata is None
                            or metadata[:2] != (row["json_offset_data"], row["json_size"])
                            or len(metadata[3]) != metadata[1]
                            or metadata[1] > 1 << 20
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
                            "json_content_sha_independently_verified": True,
                            "image_sha_matches_same_rust_stage": True,
                            "image_pixel_sha_independently_verified": False,
                            **flags,
                        }, selected
    return {
        "records_audited": checked,
        "json_content_sha_independently_verified": checked > 0,
        "image_sha_matches_same_rust_stage": checked > 0,
        "image_pixel_sha_independently_verified": False,
        **flags,
    }, selected


def identify_adapter(transport, candidate, scheduler, identity):
    """Read only bounded TAR headers and one JSON via registered-proof Range.

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

    obj = transport._objects[candidate.object_path]
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


def validate_canary_rows(root, summary):
    """Stricter small-canary admission, not a change to frozen product limits."""
    import pyarrow.parquet as pq

    from sakurapool.storage.production_resources import LINE_CAP

    total = rows = 0
    for name in ("objects", "samples", "annotations", "errors"):
        count = summary.get(name)
        if type(count) is not int or not 0 <= count <= 1000:
            raise ValueError("canary decoded record count scope")
        for path in sorted(root.rglob("*." + name + ".parquet")):
            parquet = pq.ParquetFile(path)
            if parquet.num_row_groups == 0:
                continue  # PyArrow 18 iter_batches cannot read footer-only zero-group input.
            for batch in parquet.iter_batches(batch_size=1):
                for row in batch.to_pylist():
                    size = len(json.dumps(row, ensure_ascii=True).encode("ascii"))
                    if size > LINE_CAP:
                        raise ValueError("canary decoded line scope")
                    total += size
                    rows += 1
                    if total > 1 << 20 or rows > 4000:
                        raise ValueError("canary aggregate decoded input scope")
    return {
        "rows": rows,
        "serialized_bytes": total,
        "row_limit": LINE_CAP,
        "aggregate_limit": 1 << 20,
        "compiler_general_memory_theorem": False,
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


def build_one(transport, candidate, adapter, mode, scheduler, identity):
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
    obj = transport._objects[candidate.object_path]
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
            validation_lease = ledger.reserve(
                Reservation(disk=OFFLINE_BUILD_ALLOWANCE, inflight=footprint.memory)
            )
            small_rows = validate_canary_rows(job / "p2", summary)
            inventory = load_p2_inventory(job / "p2", _row_batch_size=1)
            details = {
                "status": "PASS",
                "summary": {
                    k: v
                    for k, v in summary.items()
                    if k in ("objects", "samples", "annotations", "errors")
                    and type(v) is int
                    and 0 <= v <= 100000
                },
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
            if summary["samples"] == 0 or summary["samples"] > 1000:
                details.update(status="BLOCKED", failure_kind="canary_record_scope")
                return complete(job / "p2", details)
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
        except BudgetExceeded:
            return complete(None, {"status": "CAPACITY_BLOCKED", "failure_kind": "capacity"})
        except RustScanAuditError:
            return complete(None, {"status": "BLOCKED", "failure_kind": "scan_or_webdataset_audit"})
        except RemoteIOError:
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


def closure(initial_transport, initial_candidate, token, scheduler, identity):
    """Ordered closure; terminal control reasons must never become business failure."""
    report = scheduler.proof_use(initial_transport, initial_candidate, identity)
    if report["status"] != "PASS":
        return closure_result(scheduler, report, "initial_registered_range")
    candidate, report = discovery(
        initial_transport.ledger, token, scheduler, identity, repository=initial_candidate.repo_id
    )
    if report["status"] != "PASS" or candidate is None:
        return closure_result(scheduler, report, "discovery")
    transport, report = scheduler.schedule(initial_transport.ledger, token, candidate, identity)
    if report["status"] != "PASS":
        return closure_result(scheduler, report, "own_binding")
    report = scheduler.proof_use(transport, candidate, identity)
    if report["status"] != "PASS":
        return closure_result(scheduler, report, "own_registered_range")
    adapter, report = identify_adapter(transport, candidate, scheduler, identity)
    if report["status"] != "PASS":
        return closure_result(scheduler, report, "schema_identification")
    remote, report = build_one(
        transport, candidate, adapter, "remote-stream-scan", scheduler, identity
    )
    if report["status"] != "PASS":
        return closure_result(scheduler, report, "remote_builder", remote=report["status"])
    download, report = build_one(
        transport, candidate, adapter, "download-then-scan", scheduler, identity
    )
    if report["status"] != "PASS":
        result = closure_result(
            scheduler, report, "download_builder", remote="PASS", download=report["status"]
        )
        if report["status"] == "CAPACITY_BLOCKED":
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
    except BudgetExceeded:
        details = {"status": "CAPACITY_BLOCKED", "failure_kind": "capacity"}
    except Exception:
        pass  # Preserve only typed unknown cause, never raw exception/chain.
    finally:
        if lease is not None:
            settle_preserving(ledger, lease, details)
    return details


def _equivalent_rows(remote, download):
    """Small canary only; compare full typed tables including extent/hash/content."""
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
