"""Local task admission; frozen selection is never expanded during execution."""

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
from .plan import Selection, normalize_query, selected_records
from .store import MAX_DB_BYTES, MAX_JOURNAL_BYTES, TaskDB, TaskError


def create_task(publication, directory, ledger, query, selection=None, *, metadata=False,
                max_output_bytes=512 << 20):
    """Freeze actual content identity and streamed selection without provider IO."""
    selection = selection if selection is not None else Selection()
    selection.validate()
    normalized = normalize_query(query)
    with load_publication(publication, full_verify=True) as pub:
        header = {"publication_digest": pub.content_digest,
                  "snapshot_id": pub.runtime.snapshot_id,
                  "query": normalized, "selection": selection.header(), "metadata": metadata}
        return TaskDB.create(directory, ledger, header,
                             selected_records(pub.runtime, query, selection),
                             publication_path=publication, max_bytes=max_output_bytes)


def check_publication_identity(task, publication):
    """Local identity checks must precede credential/provider IO."""
    header = task.validate_plan()
    if (publication.content_digest != header["publication_digest"]
            or publication.runtime.snapshot_id != header["snapshot_id"]):
        raise TaskError("PUBLICATION_IDENTITY_MISMATCH", "plan")
    return header


def verify_delivery(task, item):
    """Check ownership, pinned plan, exact entries and streamed content."""
    if item["receipt"] is None:
        raise TaskError("OUTPUT_CONFLICT", "recovery")
    receipt = json.loads(item["receipt"])
    attempt = task.db.execute("SELECT seq,operation_id,receipt FROM attempts WHERE attempt_id=?",
                              (item["attempt_id"],)).fetchone()
    if (receipt["task_id"] != task.meta("task_id")
            or receipt["plan_digest"] != task.meta("plan_digest")
            or attempt is None or attempt["seq"] != item["seq"]
            or attempt["operation_id"] != receipt["operation_id"]
            or attempt["receipt"] != item["receipt"]):
        raise TaskError("OUTPUT_CONFLICT", "recovery")
    final = plain_entry(task.directory / "output" / item["record_id"], directory=True)
    info = final.stat()
    if [info.st_dev, info.st_ino] != receipt["directory_identity"]:
        raise TaskError("OUTPUT_REPLACED", "recovery")
    if {path.name for path in final.iterdir()} != set(receipt["receipt"]):
        raise TaskError("OUTPUT_CONFLICT", "recovery")
    for name, proof in receipt["receipt"].items():
        if name not in ("image.jpg", "image.jpeg", "image.png", "image.webp", "image.avif",
                        "metadata.json"):
            raise TaskError("OUTPUT_CONFLICT", "recovery")
        path = plain_entry(final / name)
        info = path.stat()
        if ([info.st_dev, info.st_ino] != proof["identity"] or info.st_size != proof["bytes"]
                or _content_sha(path) != proof["sha256"]):
            raise TaskError("OUTPUT_CORRUPT", "recovery")
    return receipt


def reconcile(task):
    """Unknown network/settlement is a blocker, never permission to retry/refund."""
    for row in task.db.execute("SELECT * FROM items WHERE state IN ('IN_PROGRESS','DONE')"):
        final = task.directory / "output" / row["record_id"]
        attempt = task.db.execute("SELECT * FROM attempts WHERE attempt_id=?",
                                  (row["attempt_id"],)).fetchone()
        if os.path.lexists(final):
            verify_delivery(task, row)
            if (row["accounting"] == "CONFIRMED"
                    or (attempt is not None and attempt["accounting"] == "CONFIRMED"
                        and attempt["phase"] == "SETTLED")):
                task.finish_item(row["seq"], state="DONE", accounting="CONFIRMED")
                continue
            task.finish_item(row["seq"], state="BLOCKED", code="BLOCKED_ACCOUNTING",
                             accounting="UNKNOWN")
            raise TaskError("BLOCKED_ACCOUNTING", "recovery")
        if row["state"] == "DONE":
            raise TaskError("OUTPUT_MISSING", "recovery")
        if attempt is None or attempt["network_state"] != "NOT_STARTED":
            task.finish_item(row["seq"], state="BLOCKED", code="NETWORK_ACCOUNTING_UNKNOWN",
                             accounting="UNKNOWN")
            raise TaskError("NETWORK_ACCOUNTING_UNKNOWN", "recovery")
        task.finish_item(row["seq"], state="READY")


def preflight(task, pub, item, transport):
    runtime = pub.runtime
    record = runtime.resolve_record(item["record_id"])
    if record.rid != item["rid"]:
        raise TaskError("RECORD_IDENTITY_MISMATCH", "preflight")
    loc = runtime.location(record.rid)
    obj = pub.catalog.execute("SELECT object_size,fetchable FROM objects WHERE object_idx=?",
                              (loc["object_idx"],)).fetchone()
    if obj is None or obj[1] != 1:
        raise TaskError("PROVIDER_DIGEST_UNAVAILABLE", "preflight")
    size = int.from_bytes(obj[0], "big")
    metadata = task.meta("header")["metadata"]
    meta_size = loc["metadata_size"] if metadata and loc["flags"] & 1 else 0
    limit = getattr(transport, "max_range_bytes", 8 << 20)
    if loc["image_size"] > limit or meta_size > limit:
        raise TaskError("RANGE_TOO_LARGE", "preflight")
    if (loc["image_size"] <= 0 or loc["image_offset"] + loc["image_size"] > size
            or loc["metadata_offset"] + meta_size > size):
        raise TaskError("RECORD_EXTENT_INVALID", "preflight")
    output = _real_output_root(task.directory / "output", transport.ledger)
    if os.path.lexists(output / item["record_id"]):
        raise TaskError("OUTPUT_CONFLICT", "preflight")
    status = transport.ledger.status()
    proof_cached = (pub.content_digest, runtime.snapshot_id, loc["object_idx"],
                    transport, transport.ledger) in pub._verified
    range_count = 1 + int(meta_size > 0)
    required = {"saved_samples": 1, "saved_bytes": loc["image_size"] + meta_size,
                "body": loc["image_size"] + meta_size + range_count,
                "attempts": 2 * range_count}
    if not proof_cached:
        from ..storage.production_resources import NEGATIVE_CONDITION_BODY_CAP

        # Cold topology: two mandatory metadata responses (hub + first page),
        # observe + positive + negative probes (each two hops). Later pages
        # remain individually admitted by GuardedTransport before sending.
        listing_cap = (1 << 20) + 1
        required["body"] += 2 * listing_cap + 4 + NEGATIVE_CONDITION_BODY_CAP + 1
        required["metadata"] = 2 * listing_cap
        required["attempts"] += 8
    if not transport.ledger.offline_mode:
        from ..storage.production_resources import ProductionFootprint

        footprint = ProductionFootprint.admit("range", max(loc["image_size"], meta_size))
        required.update(inflight=footprint.memory,
                        disk=footprint.transfer_disk + loc["image_size"] + meta_size + 12288)
    remaining = {key: max(0, transport.ledger.limits[key] - status[key]) for key in required}
    delivered_bytes = task.meta("confirmed_output_bytes")
    if (any(required[key] > remaining[key] for key in required)
            or delivered_bytes + required["saved_bytes"] > task.meta("max_output_bytes")):
        error = TaskError("RESOURCE_BLOCKED", "preflight", recoverable=True)
        error.resources = {"profile": "P4_LEGACY", "required": required,
                           "network_requirement": "known_cold_steps_plus_per_request_admission",
                           "remaining": remaining, "effective_limit": transport.ledger.limits,
                           "task_output_remaining": task.meta("max_output_bytes") - delivered_bytes}
        raise error
    return output


@contextmanager
def admit_task_growth(directory, ledger):
    """Physical DB growth/journal admission, not a parallel network reservation."""
    directory = plain_entry(Path(directory).absolute(), directory=True)
    directory = _real_output_root(directory, ledger)
    path = plain_entry(directory / "task.sqlite")
    required = MAX_JOURNAL_BYTES + max(0, MAX_DB_BYTES - path.stat().st_size) + 16384
    try:
        lease = ledger.reserve(Reservation(disk=required))
    except BudgetExceeded:
        error = TaskError("RESOURCE_BLOCKED", "taskdb", recoverable=True)
        remaining = max(0, ledger.limits["disk"] - ledger.status()["disk"])
        error.resources = {"required": {"disk": required}, "effective_limit": ledger.limits,
                           "remaining": {"disk": remaining}}
        raise error from None
    try:
        yield
    finally:
        # Disk-only reservation: settlement verifies physical usage, no network
        # or saved consumption may be inferred from this lease.
        primary = sys.exc_info()[1]
        try:
            ledger.settle(lease)
        except BaseException as secondary:
            if primary is None:
                if not isinstance(secondary, Exception):
                    raise
                raise TaskError("TASK_RESOURCE_SETTLEMENT_UNKNOWN", "taskdb") from None
            primary.task_secondary = (*getattr(primary, "task_secondary", ()),
                                      "TASK_RESOURCE_SETTLEMENT_UNKNOWN")


def run_task(directory, transport, *, control=None, resume=False, fault_hook=None,
             connection_profile=None):
    """One process, one serial session, short DB transactions outside all IO."""
    with (admit_task_growth(directory, transport.ledger),
          TaskDB(directory) as task, task.runner_lock()):
        _real_output_root(task.directory, transport.ledger)
        if task.meta("state") in ("PAUSED", "CANCELLED") and not resume:
            raise TaskError("EXPLICIT_RESUME_REQUIRED")
        with task.transaction() as db:
            if resume:
                task.set_meta(db, "request", None)
            task.set_meta(db, "state", "RUNNING")
        try:
            with PublicationSession(task.meta("publication_path"), transport,
                                    control=control) as session:
                header = check_publication_identity(task, session.publication)
                if connection_profile is not None:
                    from .profile import validate_allowlist

                    validate_allowlist(connection_profile, session.publication)
                reconcile(task)
                if task.db.execute("SELECT 1 FROM items WHERE state='BLOCKED' LIMIT 1").fetchone():
                    raise TaskError("BLOCKED_ACCOUNTING", "recovery")
                while True:
                    request = task.meta("request")
                    if request:
                        with task.transaction() as db:
                            task.set_meta(db, "state",
                                          "PAUSED" if request == "PAUSE" else "CANCELLED")
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
                            error.task_secondary = (*getattr(error, "task_secondary", ()),
                                                    "TASK_STATE_PERSIST_FAILED")
                        raise

                    def hook(event, payload):
                        if fault_hook:
                            fault_hook("BEFORE_" + event, payload)
                        task.event(item["attempt_id"], event, payload)
                        if fault_hook:
                            fault_hook(event, payload)

                    try:
                        session.fetch(item["record_id"], output, metadata=header["metadata"],
                                      attempt_hook=hook)
                    except Exception as primary:
                        error = TaskError("FETCH_UNCONFIRMED", "fetch")
                        if isinstance(primary, (PublicationFetchError, RemoteIOError)):
                            details = primary.public_diagnostic()
                            if (details.get("code") in _SAFE_CODES
                                    and details.get("phase") in _SAFE_PHASES):
                                error = TaskError(details["code"], details["phase"])
                                error.safe_details = {"code": details["code"],
                                                      "phase": details["phase"]}
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
                                    allowed_secondary = {"CLEANUP_FAILED", "ACCOUNTING_UNKNOWN",
                                                         "RANGE_FINALIZATION_FAILED"}
                                    error.safe_details["secondary"] = [
                                        value for value in details.get("secondary", ())
                                        if value in allowed_secondary
                                    ]
                                status_code = details.get("http_status")
                                if type(status_code) is int and 100 <= status_code <= 599:
                                    error.safe_details["http_status"] = status_code
                        try:
                            task.finish_item(item["seq"], state="BLOCKED", code=error.code,
                                             accounting="UNKNOWN")
                            status = task.db.execute("SELECT delivery FROM items WHERE seq=?",
                                                     (item["seq"],)).fetchone()[0]
                            error.delivery = ("PUBLISHED" if status == "PUBLISHED"
                                              else "NOT_PUBLISHED")
                        except BaseException:
                            error.task_secondary = ("TASK_STATE_PERSIST_FAILED",)
                        raise error from None
                    task.finish_item(item["seq"], state="DONE")
        except BaseException as primary:
            try:
                with task.transaction() as db:
                    task.set_meta(db, "state", "BLOCKED")
            except BaseException:
                primary.task_secondary = (*getattr(primary, "task_secondary", ()),
                                          "TASK_STATE_PERSIST_FAILED")
            raise
        return task.inspect()
