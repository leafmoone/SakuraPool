"""Task freeze, current-policy admission and coordinator execution."""

import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path

from ..fs_safety import plain_entry
from ..storage.budget import BudgetExceeded, Reservation
from ..storage.publication import load_publication
from ..storage.publication_fetch import PublicationFetchError, _content_sha
from ..storage.publication_session import PublicationSession
from ..storage.retrieval import _real_output_root
from ..storage.transport import _SAFE_CODES, _SAFE_PHASES, RemoteIOError
from .context import (
    LEGACY_CAPACITY,
    chunk_plan,
    effective_capacity,
    remaining_limits,
    validate_ledger,
)
from .plan import Selection, normalize_query, selected_records
from .store import TaskDB, TaskError


def create_task(
    publication,
    directory,
    ledger,
    query,
    selection=None,
    *,
    metadata=False,
    max_output_bytes=512 << 20,
):
    workspace = getattr(ledger, "workspace", None)
    capacity = workspace.capacity if workspace is not None else LEGACY_CAPACITY
    if workspace is not None:
        workspace.task_path(directory)
    selection = selection if selection is not None else Selection()
    selection.validate(capacity)
    normalized = normalize_query(query)
    with load_publication(publication, full_verify=True) as pub:
        header = {
            "publication_digest": pub.content_digest,
            "snapshot_id": pub.runtime.snapshot_id,
            "query": normalized,
            "selection": selection.header(capacity),
            "metadata": metadata,
        }
        return TaskDB.create(
            directory,
            ledger,
            header,
            selected_records(pub.runtime, query, selection, capacity),
            publication_path=publication,
            max_bytes=max_output_bytes,
        )


def check_publication_identity(task, publication):
    header = task.validate_plan()
    if (
        publication.content_digest != header["publication_digest"]
        or publication.runtime.snapshot_id != header["snapshot_id"]
    ):
        raise TaskError("PUBLICATION_IDENTITY_MISMATCH", "plan")
    return header


def verify_delivery(task, item):
    if item["receipt"] is None:
        raise TaskError("OUTPUT_CONFLICT", "recovery")
    receipt = json.loads(item["receipt"])
    attempt = task.db.execute(
        "SELECT seq,operation_id,receipt FROM attempts WHERE attempt_id=?", (item["attempt_id"],)
    ).fetchone()
    if (
        receipt["task_id"] != task.meta("task_id")
        or receipt["plan_digest"] != task.meta("plan_digest")
        or attempt is None
        or attempt["seq"] != item["seq"]
        or attempt["operation_id"] != receipt["operation_id"]
        or attempt["receipt"] != item["receipt"]
    ):
        raise TaskError("OUTPUT_CONFLICT", "recovery")
    final = plain_entry(task.directory / "output" / item["record_id"], directory=True)
    info = final.stat()
    if [info.st_dev, info.st_ino] != receipt["directory_identity"]:
        raise TaskError("OUTPUT_REPLACED", "recovery")
    if {path.name for path in final.iterdir()} != set(receipt["receipt"]):
        raise TaskError("OUTPUT_CONFLICT", "recovery")
    for name, proof in receipt["receipt"].items():
        if name not in (
            "image.jpg",
            "image.jpeg",
            "image.png",
            "image.webp",
            "image.avif",
            "metadata.json",
        ):
            raise TaskError("OUTPUT_CONFLICT", "recovery")
        path = plain_entry(final / name)
        info = path.stat()
        if (
            [info.st_dev, info.st_ino] != proof["identity"]
            or info.st_size != proof["bytes"]
            or _content_sha(path) != proof["sha256"]
        ):
            raise TaskError("OUTPUT_CORRUPT", "recovery")
    return receipt


def reconcile(task):
    """UNKNOWN is never permission to retry or reset accounting."""
    blocker = None
    for row in task.db.execute("SELECT * FROM items WHERE state IN ('IN_PROGRESS','DONE')"):
        final = task.directory / "output" / row["record_id"]
        attempt = task.db.execute(
            "SELECT * FROM attempts WHERE attempt_id=?", (row["attempt_id"],)
        ).fetchone()
        if os.path.lexists(final):
            try:
                verify_delivery(task, row)
            except TaskError as error:
                blocker = blocker or error
                continue
            if row["accounting"] == "CONFIRMED" or (
                attempt is not None
                and attempt["accounting"] == "CONFIRMED"
                and attempt["phase"] == "SETTLED"
            ):
                task.finish_item(row["seq"], state="DONE", accounting="CONFIRMED")
                continue
            task.finish_item(
                row["seq"], state="BLOCKED", code="BLOCKED_ACCOUNTING", accounting="UNKNOWN"
            )
            blocker = blocker or TaskError("BLOCKED_ACCOUNTING", "recovery")
            continue
        if row["state"] == "DONE":
            blocker = blocker or TaskError("OUTPUT_MISSING", "recovery")
            continue
        if attempt is None or attempt["network_state"] != "NOT_STARTED":
            task.finish_item(
                row["seq"], state="BLOCKED", code="NETWORK_ACCOUNTING_UNKNOWN", accounting="UNKNOWN"
            )
            blocker = blocker or TaskError("NETWORK_ACCOUNTING_UNKNOWN", "recovery")
            continue
        task.finish_item(row["seq"], state="READY")
    if blocker is not None:
        raise blocker


def preflight(task, pub, item, transport, *, prepared=None, proof_warm=None, ledger=None):
    ledger = transport.ledger if ledger is None else ledger
    validate_ledger(task, ledger)
    capacity = effective_capacity(transport, task.capacity)
    if capacity != task.capacity:
        raise TaskError("TASK_CAPACITY_CONFLICT", "preflight")
    runtime = pub.runtime
    if prepared is None:
        record = runtime.resolve_record(item["record_id"])
        rid = record.rid
        loc = runtime.location(rid)
        obj = pub.catalog.execute(
            "SELECT object_size,fetchable FROM objects WHERE object_idx=?", (loc["object_idx"],)
        ).fetchone()
    else:
        if (
            prepared.record_id != item["record_id"]
            or prepared.content_digest != pub.content_digest
            or prepared.snapshot_id != runtime.snapshot_id
        ):
            raise TaskError("RECORD_IDENTITY_MISMATCH", "preflight")
        rid, loc = prepared.rid, prepared.location
        obj = (prepared.catalog_row[5], prepared.catalog_row[8])
    if rid != item["rid"]:
        raise TaskError("RECORD_IDENTITY_MISMATCH", "preflight")
    if obj is None or obj[1] != 1:
        raise TaskError("PROVIDER_DIGEST_UNAVAILABLE", "preflight")
    size = int.from_bytes(obj[0], "big")
    metadata = task.meta("header")["metadata"]
    meta_size = loc["metadata_size"] if metadata and loc["flags"] & 1 else 0
    if loc["image_size"] > capacity.image_max_bytes or meta_size > capacity.metadata_max_bytes:
        raise TaskError("RANGE_TOO_LARGE", "preflight")
    if (
        loc["image_size"] <= 0
        or loc["image_offset"] + loc["image_size"] > size
        or loc["metadata_offset"] + meta_size > size
    ):
        raise TaskError("RECORD_EXTENT_INVALID", "preflight")
    output = _real_output_root(task.directory / "output", ledger)
    if os.path.lexists(output / item["record_id"]):
        raise TaskError("OUTPUT_CONFLICT", "preflight")
    if proof_warm is None:
        predict = getattr(transport, "predict_warm", None)
        if callable(predict):
            from ..storage.prepared_fetch import PreparedFetch, stream_plan

            descriptor = (
                prepared
                if prepared is not None
                else PreparedFetch._prepare(pub, item["record_id"], capacity=capacity)
            )
            proof_cached = predict(
                descriptor.transport_identity,
                stream_plan(loc, size, metadata=metadata, capacity=capacity),
            )
        else:
            proof_cached = (
                pub.content_digest,
                runtime.snapshot_id,
                loc["object_idx"],
                transport,
                transport.ledger,
            ) in pub._verified
    else:
        proof_cached = proof_warm
    range_count, max_chunk = chunk_plan(loc["image_size"], meta_size, capacity)
    from ..storage.prepared_fetch import stream_plan
    from ..storage.production_resources import checked_body_add, network_body_budget

    required = {
        "saved_samples": 1,
        "saved_bytes": loc["image_size"] + meta_size,
        "body": stream_plan(loc, size, metadata=metadata, capacity=capacity).generation_body,
        "attempts": 2 * range_count,
    }
    if not proof_cached:
        from ..storage.production_resources import checked_body_add, network_body_budget

        listing_cap = (1 << 20) + 1
        required["body"] = checked_body_add(
            required["body"],
            listing_cap,
            listing_cap,
            network_body_budget(1),
            network_body_budget(1),
            network_body_budget(1, condition="wrong"),
        )
        required["metadata"] = 2 * listing_cap
        required["attempts"] += 8
    if not ledger.offline_mode:
        from ..storage.production_resources import ProductionFootprint

        footprint = ProductionFootprint.admit("range", max_chunk, capacity=capacity)
        required.update(
            inflight=footprint.memory,
            disk=footprint.transfer_disk + loc["image_size"] + meta_size + 12288,
        )
    remaining = remaining_limits(ledger, required)
    delivered_bytes = task.meta("confirmed_output_bytes")
    if any(required[key] > remaining[key] for key in required) or delivered_bytes + required[
        "saved_bytes"
    ] > task.meta("max_output_bytes"):
        error = TaskError("RESOURCE_BLOCKED", "preflight", recoverable=True)
        error.resources = {
            "profile": "WORKSPACE" if task.workspace is not None else "P4_LEGACY",
            "required": required,
            "network_requirement": "known_cold_steps_plus_per_request_admission",
            "remaining": remaining,
            "effective_limit": ledger.limits,
            "task_output_remaining": task.meta("max_output_bytes") - delivered_bytes,
        }
        raise error
    return output


@contextmanager
def admit_task_growth(directory, ledger):
    directory = plain_entry(Path(directory).absolute(), directory=True)
    workspace = getattr(ledger, "workspace", None)
    if workspace is None:
        _real_output_root(directory, ledger)
    else:
        workspace.task_path(directory, must_exist=True)
    probe = TaskDB(directory, readonly=True)
    try:
        validate_ledger(probe, ledger)
        capacity, path = probe.capacity, probe.path
    finally:
        # This read-only ownership probe owns no task execution lifecycle.
        probe.db.close()
    required = (
        capacity.task_journal_bytes + max(0, capacity.task_db_bytes - path.stat().st_size) + 16384
    )
    try:
        lease = ledger.reserve(Reservation(disk=required))
    except BudgetExceeded:
        error = TaskError("RESOURCE_BLOCKED", "taskdb", recoverable=True)
        remaining = remaining_limits(ledger, {"disk": required})["disk"]
        error.resources = {
            "required": {"disk": required},
            "effective_limit": ledger.limits,
            "remaining": {"disk": remaining},
        }
        raise error from None
    try:
        yield
    finally:
        primary = sys.exc_info()[1]
        try:
            ledger.settle(lease)
        except BaseException as secondary:
            if primary is None:
                if not isinstance(secondary, Exception):
                    raise
                raise TaskError("TASK_RESOURCE_SETTLEMENT_UNKNOWN", "taskdb") from None
            primary.task_secondary = (
                *getattr(primary, "task_secondary", ()),
                "TASK_RESOURCE_SETTLEMENT_UNKNOWN",
            )


def run_task(
    directory,
    transport,
    *,
    control=None,
    resume=False,
    fault_hook=None,
    connection_profile=None,
    workers=1,
):
    if type(workers) is not int or workers not in (1, 2, 4):
        raise TaskError("WORKERS_INVALID", "preflight")
    with (
        admit_task_growth(directory, transport.ledger),
        TaskDB(directory) as task,
        task.runner_lock(),
    ):
        validate_ledger(task, transport.ledger)
        if effective_capacity(transport, task.capacity) != task.capacity:
            raise TaskError("TASK_CAPACITY_CONFLICT", "preflight")
        _real_output_root(task.directory, transport.ledger)
        if task.meta("state") in ("PAUSED", "CANCELLED", "BLOCKED") and not resume:
            raise TaskError("EXPLICIT_RESUME_REQUIRED")
        with task.transaction() as db:
            if resume:
                task.set_meta(db, "request", None)
            task.set_meta(db, "state", "RUNNING")
        try:
            with PublicationSession(
                task.meta("publication_path"), transport, control=control
            ) as session:
                header = check_publication_identity(task, session.publication)
                if connection_profile is not None:
                    from .profile import validate_allowlist

                    validate_allowlist(connection_profile, session.publication)
                reconcile(task)
                if task.db.execute("SELECT 1 FROM items WHERE state='BLOCKED' LIMIT 1").fetchone():
                    raise TaskError("BLOCKED_ACCOUNTING", "recovery")
                if hasattr(transport, "clone"):
                    from .pipeline import run_pipeline

                    run_pipeline(
                        task,
                        session.publication,
                        transport,
                        workers=workers,
                        metadata=header["metadata"],
                        fault_hook=fault_hook,
                        control=control,
                    )
                    return task.inspect()
                if workers != 1:
                    raise TaskError("WORKER_CHANNEL_UNAVAILABLE", "preflight")
                while True:
                    request = task.meta("request")
                    if request:
                        with task.transaction() as db:
                            task.set_meta(
                                db, "state", "PAUSED" if request == "PAUSE" else "CANCELLED"
                            )
                        break
                    item = task.claim()
                    if item is None:
                        if task.meta("request") is not None:
                            continue
                        with task.transaction() as db:
                            task.set_meta(db, "state", "COMPLETED")
                        break
                    if fault_hook:
                        fault_hook("CLAIMED", item)
                    try:
                        output = preflight(task, session.publication, item, transport)
                    except TaskError as error:
                        try:
                            task.finish_item(item["seq"], state="READY", code=error.code)
                        except BaseException:
                            error.task_secondary = (
                                *getattr(error, "task_secondary", ()),
                                "TASK_STATE_PERSIST_FAILED",
                            )
                        raise

                    def hook(event, payload):
                        if fault_hook:
                            fault_hook("BEFORE_" + event, payload)
                        task.event(item["attempt_id"], event, payload)
                        if fault_hook:
                            fault_hook(event, payload)

                    try:
                        session.fetch(
                            item["record_id"],
                            output,
                            metadata=header["metadata"],
                            attempt_hook=hook,
                        )
                    except Exception as primary:
                        error = TaskError("FETCH_UNCONFIRMED", "fetch")
                        if isinstance(primary, (PublicationFetchError, RemoteIOError)):
                            details = primary.public_diagnostic()
                            if (
                                details.get("code") in _SAFE_CODES
                                and details.get("phase") in _SAFE_PHASES
                            ):
                                error = TaskError(details["code"], details["phase"])
                                error.safe_details = {
                                    "code": details["code"],
                                    "phase": details["phase"],
                                }
                                if details.get("accounting") in {"CONFIRMED", "UNKNOWN"}:
                                    error.safe_details["accounting"] = details["accounting"]
                                if isinstance(primary, PublicationFetchError):
                                    for key, allowed in {
                                        "delivery": {"PUBLISHED", "NOT_PUBLISHED"},
                                        "accounting": {"UNKNOWN", "CONFIRMED"},
                                        "output_lease": {"UNKNOWN", "CONFIRMED"},
                                        "accounting_scope": {"OPERATION"},
                                        "cleanup": {"SAFE", "PRESERVED"},
                                    }.items():
                                        if details.get(key) in allowed:
                                            error.safe_details[key] = details[key]
                                    from .pipeline import _safe_diagnostic

                                    error.safe_details.update(_safe_diagnostic(primary))
                                    error.safe_details["secondary"] = [
                                        v
                                        for v in details.get("secondary", ())
                                        if v
                                        in {
                                            "CLEANUP_FAILED",
                                            "ACCOUNTING_UNKNOWN",
                                            "RANGE_FINALIZATION_FAILED",
                                        }
                                    ]
                                status_code = details.get("http_status")
                                if type(status_code) is int and 100 <= status_code <= 599:
                                    error.safe_details["http_status"] = status_code
                        try:
                            status = task.db.execute(
                                "SELECT delivery FROM items WHERE seq=?", (item["seq"],)
                            ).fetchone()[0]
                            known = (
                                status != "PUBLISHED"
                                and getattr(error, "safe_details", {}).get("accounting")
                                == "CONFIRMED"
                            )
                            error.recoverable = known
                            task.finish_item(
                                item["seq"],
                                state="READY" if known else "BLOCKED",
                                code=error.code,
                                accounting="CONFIRMED" if known else "UNKNOWN",
                                accounting_basis=(
                                    error.safe_details.get("accounting_basis") if known else None
                                ),
                            )
                            error.delivery = (
                                "PUBLISHED" if status == "PUBLISHED" else "NOT_PUBLISHED"
                            )
                        except BaseException:
                            error.recoverable = False
                            error.safe_details = getattr(error, "safe_details", {})
                            error.safe_details["accounting"] = "UNKNOWN"
                            for key in ("accounting_basis", "actual_consumption", "accounted"):
                                error.safe_details.pop(key, None)
                            error.task_secondary = ("TASK_STATE_PERSIST_FAILED",)
                        raise error from None
                    task.finish_item(item["seq"], state="DONE")
        except BaseException as primary:
            try:
                with task.transaction() as db:
                    task.set_meta(db, "state", "BLOCKED")
            except BaseException:
                primary.task_secondary = (
                    *getattr(primary, "task_secondary", ()),
                    "TASK_STATE_PERSIST_FAILED",
                )
            raise
        return task.inspect()
