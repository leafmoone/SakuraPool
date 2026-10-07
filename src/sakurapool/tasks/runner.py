"""Frozen lightweight downloads, verified delivery and conservative owned-temp recovery."""

import json
import os
from contextlib import ExitStack

from ..fs_safety import plain_entry
from ..storage.publication import load_publication
from ..storage.publication_fetch import _cleanup_owned_stage, _content_sha
from ..storage.retrieval import _real_output_root
from .context import LEGACY_CAPACITY, effective_capacity
from .plan import Selection, normalize_query, selected_records
from .store import TaskDB, TaskError

RECONCILE_PAGE_ROWS = 64


def create_task(
    publication,
    directory,
    workspace,
    query,
    selection=None,
    *,
    metadata=False,
    image_extensions=None,
    filename_template="{tag}_{index}",
    filename_prefix=None,
):
    if workspace is not None and not getattr(workspace, "lightweight", False):
        raise TaskError("LEGACY_TASK_MIGRATION_REQUIRED", "create")
    capacity = workspace.capacity if workspace is not None else LEGACY_CAPACITY
    if workspace is not None:
        workspace.task_path(directory)
    selection = Selection() if selection is None else selection
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
            workspace,
            header,
            selected_records(pub.runtime, query, selection, capacity),
            publication_path=publication,
            image_extensions=image_extensions,
            filename_template=filename_template,
            filename_prefix=filename_prefix,
        )


def check_publication_identity(task, publication):
    header = task.validate_plan()
    if (
        publication.content_digest != header["publication_digest"]
        or publication.runtime.snapshot_id != header["snapshot_id"]
    ):
        raise TaskError("PUBLICATION_IDENTITY_MISMATCH", "plan")
    return header


def delivery_mapping(task, item, publication):
    from ..download_naming import FilenameConfig
    from ..image_formats import image_filename
    from ..storage.flat_delivery import DeliveryMapping

    header = task.meta("header")
    stem = FilenameConfig.from_dict(header["filename_config"]).stem(item["seq"])
    if item["output_stem"] != stem:
        raise TaskError("OUTPUT_CONFLICT", "recovery")
    loc = publication.runtime.location(item["rid"])
    suffix = image_filename(publication.runtime.image_format(loc["format_id"]),
                            task.image_extensions).removeprefix("image")
    return DeliveryMapping(stem, suffix, bool(header["metadata"] and loc["flags"] & 1))


def _flat_receipt(task, item, publication):
    from ..storage.flat_delivery import validate_receipt

    if type(item["receipt"]) is not str or len(item["receipt"].encode("utf-8")) > 8192:
        raise TaskError("OUTPUT_CONFLICT", "recovery")
    try:
        if publication.runtime.resolve_record(item["record_id"]).rid != item["rid"]:
            raise ValueError()
        mapping = delivery_mapping(task, item, publication)
        receipt = validate_receipt(json.loads(item["receipt"]), mapping)
        operation = item["operation_id"]
        if type(operation) is not str or len(operation) != 32 or any(
            c not in "0123456789abcdef" for c in operation
        ):
            raise ValueError()
        if (receipt.get("task_id") != task.meta("task_id")
                or receipt.get("plan_digest") != task.meta("plan_digest")
                or receipt.get("operation_id") != operation):
            raise ValueError()
        if type(item["stage"]) is not str or len(item["stage"].encode("utf-8")) > 8192:
            raise ValueError()
        stage = json.loads(item["stage"])
        if (type(stage) is not dict or stage.get("name") != receipt["stage_name"]
                or stage.get("identity") != receipt["stage_identity"]):
            raise ValueError()
        if stage.get("created") != {
            proof["staged_name"]: proof["identity"] for proof in receipt["receipt"].values()
        }:
            raise ValueError()
        loc = publication.runtime.location(item["rid"])
        expected = {mapping.names[0]: loc["image_size"]}
        if mapping.metadata:
            expected[mapping.names[1]] = loc["metadata_size"]
        for name, size in expected.items():
            if receipt["receipt"][name]["bytes"] != size:
                raise ValueError()
        if receipt["receipt"][mapping.names[0]]["sha256"] != (
            publication.expected_image_sha(item["rid"]).hex()
        ):
            raise ValueError()
        return receipt, mapping
    except (ValueError, TypeError, KeyError, RecursionError):
        raise TaskError("OUTPUT_CONFLICT", "recovery") from None


def verify_delivery(task, item, publication=None):
    if publication is None:
        with load_publication(task.meta("publication_path"), full_verify=True) as verified:
            check_publication_identity(task, verified)
            return verify_delivery(task, item, verified)
    if publication._closed or not publication.full_verified:
        raise TaskError("PUBLICATION_IDENTITY_MISMATCH", "recovery")
    header = task.meta("header")
    if (
        publication.content_digest != header["publication_digest"]
        or publication.runtime.snapshot_id != header["snapshot_id"]
    ):
        raise TaskError("PUBLICATION_IDENTITY_MISMATCH", "recovery")
    from ..image_formats import image_filename

    record = publication.runtime.resolve_record(item["record_id"])
    if record.rid != item["rid"]:
        raise TaskError("RECORD_IDENTITY_MISMATCH", "recovery")
    location = publication.runtime.location(record.rid)
    if task.version == 4:
        from ..storage.flat_delivery import verify_final

        receipt, mapping = _flat_receipt(task, item, publication)
        try:
            if item["phase"] != "PUBLISHED":
                raise ValueError()
            published = json.loads(item["published_members"])
            if (type(published) is not list or any(type(n) is not str for n in published)
                    or len(published) != len(mapping.names)
                    or set(published) != set(mapping.names)):
                raise ValueError()
            verify_final(task.directory / "output", receipt, mapping)
        except (ValueError, OSError):
            raise TaskError("OUTPUT_CONFLICT", "recovery") from None
        return receipt
    try:
        image_name = image_filename(
            publication.runtime.image_format(location["format_id"]), task.image_extensions
        )
    except ValueError:
        raise TaskError("OUTPUT_CONFLICT", "recovery") from None
    if type(item["receipt"]) is not str or len(item["receipt"].encode("utf-8")) > 8192:
        raise TaskError("OUTPUT_CONFLICT", "recovery")
    try:
        receipt = json.loads(item["receipt"])
        if (
            type(receipt) is not dict
            or type(receipt.get("receipt")) is not dict
            or not _identity(receipt.get("directory_identity"))
        ):
            raise TaskError("OUTPUT_CONFLICT", "recovery")
        if task.version >= 3:
            operation = item["operation_id"]
        else:
            attempt = task.db.execute(
                "SELECT seq,operation_id,receipt FROM attempts WHERE attempt_id=?",
                (item["attempt_id"],),
            ).fetchone()
            if (
                attempt is None
                or attempt["seq"] != item["seq"]
                or attempt["receipt"] != item["receipt"]
            ):
                raise TaskError("OUTPUT_CONFLICT", "recovery")
            operation = attempt["operation_id"]
        if (
            type(operation) is not str
            or len(operation) != 32
            or any(c not in "0123456789abcdef" for c in operation)
        ):
            raise TaskError("OUTPUT_CONFLICT", "recovery")
        if (
            receipt["task_id"] != task.meta("task_id")
            or receipt["plan_digest"] != task.meta("plan_digest")
            or receipt["operation_id"] != operation
        ):
            raise TaskError("OUTPUT_CONFLICT", "recovery")
        final = plain_entry(task.directory / "output" / item["record_id"], directory=True)
        info = final.stat()
        if [info.st_dev, info.st_ino] != receipt["directory_identity"]:
            raise TaskError("OUTPUT_REPLACED", "recovery")
        if {path.name for path in final.iterdir()} != set(receipt["receipt"]):
            raise TaskError("OUTPUT_CONFLICT", "recovery")
        expected = {image_name: location["image_size"]}
        if header["metadata"] and location["flags"] & 1:
            expected["metadata.json"] = location["metadata_size"]
        if set(receipt["receipt"]) != set(expected):
            raise TaskError("OUTPUT_CONFLICT", "recovery")
        for name, proof in receipt["receipt"].items():
            if (
                type(proof) is not dict
                or type(proof.get("bytes")) is not int
                or not _identity(proof.get("identity"))
                or type(proof.get("sha256")) is not str
                or len(proof["sha256"]) != 64
                or any(c not in "0123456789abcdef" for c in proof["sha256"])
            ):
                raise TaskError("OUTPUT_CONFLICT", "recovery")
            if proof["bytes"] != expected[name]:
                raise TaskError("OUTPUT_CONFLICT", "recovery")
            path = plain_entry(final / name)
            info = path.stat()
            if (
                info.st_nlink != 1
                or [info.st_dev, info.st_ino] != proof["identity"]
                or info.st_size != proof["bytes"]
                or _content_sha(path) != proof["sha256"]
            ):
                raise TaskError("OUTPUT_CORRUPT", "recovery")
            if (
                name == image_name
                and proof["sha256"] != publication.expected_image_sha(record.rid).hex()
            ):
                raise TaskError("OUTPUT_CORRUPT", "recovery")
        return receipt
    except (OSError, ValueError, KeyError, TypeError, RecursionError):
        raise TaskError("OUTPUT_CONFLICT", "recovery") from None


def _identity(value):
    return type(value) is list and len(value) == 2 and all(type(v) is int and v >= 0 for v in value)


def _recover_stage(task, row):
    """Only a durable exact owned-stage identity allows cleanup; never name-only adoption."""
    if (
        row["delivery"] != "NONE"
        or row["phase"] in {"PREPARED", "PUBLISH_INTENT", "PARTIAL", "PUBLISHED"}
        or row["receipt"] is not None
    ):
        return False
    if row["stage"] is None:
        return row["phase"] == "CLAIMED"
    if type(row["stage"]) is not str or len(row["stage"].encode("utf-8")) > 8192:
        return False
    try:
        stage = json.loads(row["stage"])
        name = stage["name"]
        if (
            type(name) is not str
            or not name.startswith(".publication-fetch-")
            or len(name) != len(".publication-fetch-") + 32
            or any(c not in "0123456789abcdef" for c in name.removeprefix(".publication-fetch-"))
            or set(stage) != {"name", "identity", "created"}
        ):
            return False
        from ..image_formats import SUPPORTED_IMAGE_EXTENSIONS

        allowed = {"image" + suffix for suffix in SUPPORTED_IMAGE_EXTENSIONS} | {"metadata.json"}
        if (
            not _identity(stage["identity"])
            or type(stage["created"]) is not dict
            or not set(stage["created"]).issubset(allowed)
            or any(not _identity(value) for value in stage["created"].values())
        ):
            return False
        path = task.directory / "output" / name
        if not os.path.lexists(path):
            # Ordinary failed cleanup is durable as SAFE. A crashed stage disappearing
            # without this evidence cannot prove that output was not moved elsewhere.
            diagnostic = task.failure_diagnostic(row["seq"]) or {}
            return (
                diagnostic.get("cleanup") == "SAFE"
                and diagnostic.get("delivery") == "NOT_PUBLISHED"
            )
        created = {name: tuple(identity) for name, identity in stage["created"].items()}
        return _cleanup_owned_stage(path, tuple(stage["identity"]), created)
    except (OSError, ValueError, TypeError, KeyError):
        return False


def _reconcile_items(task):
    """Materialize one bounded page, close its cursor before any state update."""
    last_seq = -1
    while True:
        cursor = task.db.execute(
            "SELECT * FROM items WHERE seq>? "
            "AND state IN ('IN_PROGRESS','DONE','FAILED','BLOCKED') ORDER BY seq LIMIT ?",
            (last_seq, RECONCILE_PAGE_ROWS),
        )
        try:
            rows = cursor.fetchmany(RECONCILE_PAGE_ROWS)
        finally:
            cursor.close()
        if not rows:
            return
        last_seq = rows[-1]["seq"]
        yield from rows


def reconcile(task, publication=None):
    if task.version != 4:
        raise TaskError("LEGACY_TASK_MIGRATION_REQUIRED", "recovery")
    if publication is None:
        with load_publication(task.meta("publication_path"), full_verify=True) as verified:
            check_publication_identity(task, verified)
            return reconcile(task, verified)
    blocker = None
    for row in _reconcile_items(task):
        if row["receipt"] is not None:
            try:
                if row["phase"] not in {"PREPARED", "PUBLISH_INTENT", "PARTIAL", "PUBLISHED"}:
                    raise TaskError("OUTPUT_UNCERTAIN", "recovery")
                receipt, mapping = _flat_receipt(task, row, publication)
                if row["state"] != "DONE":
                    from ..storage.flat_delivery import publish_flat

                    published = json.loads(row["published_members"])
                    publish_flat(task.directory / "output", receipt, mapping,
                                 intent=row["phase"] != "PREPARED", published=published,
                                 hook=lambda event, payload: task.recovery_event(
                                     row["operation_id"], event, payload))
                current = task.db.execute(
                    "SELECT * FROM items WHERE seq=?", (row["seq"],)
                ).fetchone()
                verify_delivery(task, current, publication)
                task.finish_item(row["seq"], state="DONE", operation=row["operation_id"])
            except (TaskError, ValueError, OSError) as error:
                classified = (error if isinstance(error, TaskError)
                              else TaskError("OUTPUT_UNCERTAIN", "recovery"))
                task.finish_item(row["seq"], state="BLOCKED", code=classified.code,
                                 diagnostic={"phase": "publication_fetch", "delivery": "PUBLISHED",
                                             "cleanup": "PRESERVED", "recoverable": False},
                                 operation=row["operation_id"])
                blocker = blocker or classified
            continue
        mapping = delivery_mapping(task, row, publication)
        if any(os.path.lexists(task.directory / "output" / name) for name in mapping.names):
            try:
                verify_delivery(task, row, publication)
                task.finish_item(row["seq"], state="DONE", operation=row["operation_id"])
            except TaskError as error:
                task.finish_item(
                    row["seq"],
                    state="BLOCKED",
                    code=error.code,
                    diagnostic={
                        "phase": "publication_fetch",
                        "delivery": "PUBLISHED",
                        "recoverable": False,
                    },
                    operation=row["operation_id"],
                )
                blocker = blocker or error
            continue
        if row["state"] == "DONE":
            blocker = blocker or TaskError("OUTPUT_MISSING", "recovery")
            continue
        if row["state"] == "IN_PROGRESS" and _recover_stage(task, row):
            task.finish_item(
                row["seq"], state="READY", operation=row["operation_id"], recovery_retry=True
            )
            continue
        # Failed operations never receive an unlimited automatic retry. Only an explicit
        # resume after a classified transient and verified unpublished cleanup may retry.
        details = task.failure_diagnostic(row["seq"]) or {}
        transient = details.get("cause_code") in {
            "origin_timeout",
            "origin_connect",
            "cdn_timeout",
            "cdn_connect",
        }
        if (
            row["state"] == "FAILED"
            and transient
            and _recover_stage(task, row)
            and details.get("cleanup") == "SAFE"
            and details.get("delivery") == "NOT_PUBLISHED"
        ):
            task.finish_item(
                row["seq"], state="READY", operation=row["operation_id"], recovery_retry=True
            )
            continue
        if row["state"] == "IN_PROGRESS":
            task.finish_item(
                row["seq"],
                state="BLOCKED",
                code="OUTPUT_UNCERTAIN",
                diagnostic={"phase": "publication_fetch", "recoverable": False},
                operation=row["operation_id"],
            )
        blocker = blocker or TaskError("OUTPUT_UNCERTAIN", "recovery")
    if blocker is not None:
        raise blocker


def preflight(task, pub, item, transport, *, prepared=None, **_ignored):
    if task.version != 4:
        raise TaskError("LEGACY_TASK_MIGRATION_REQUIRED", "preflight")
    if getattr(transport, "ledger", None) is not None:
        raise TaskError("CONSUMPTION_LEDGER_UNSUPPORTED", "preflight")
    capacity = effective_capacity(transport, task.capacity)
    if capacity != task.capacity:
        raise TaskError("TASK_CAPACITY_CONFLICT", "preflight")
    from ..storage.prepared_fetch import PreparedFetch

    prepared = (
        PreparedFetch._prepare(
            pub, item["record_id"], capacity=capacity, image_extensions=task.image_extensions
        )
        if prepared is None
        else prepared
    )
    if prepared.rid != item["rid"]:
        raise TaskError("RECORD_IDENTITY_MISMATCH", "preflight")
    prepared.plan(metadata=task.meta("header")["metadata"], capacity=capacity)
    output = _real_output_root(
        task.directory / "output", physical_root=getattr(transport, "root", None)
    )
    # Candidate projections have no stem: use the full immutable item after owner lookup.
    full = task.db.execute("SELECT * FROM items WHERE seq=?", (item["seq"],)).fetchone()
    mapping = delivery_mapping(task, full, pub)
    mapping.check_paths(output)
    if any(os.path.lexists(output / name) for name in mapping.names):
        raise TaskError("OUTPUT_CONFLICT", "preflight")
    return output


def _check_task_transport(task, transport):
    if getattr(transport, "ledger", None) is not None:
        raise TaskError("CONSUMPTION_LEDGER_UNSUPPORTED", "preflight")
    if effective_capacity(transport, task.capacity) != task.capacity:
        raise TaskError("TASK_CAPACITY_CONFLICT", "preflight")
    _real_output_root(task.directory, physical_root=getattr(transport, "root", None))


def run_task(
    directory,
    transport=None,
    *,
    control=None,
    resume=False,
    fault_hook=None,
    connection_profile=None,
    workers=1,
):
    if type(workers) is not int or workers < 1:
        raise TaskError("WORKERS_INVALID", "preflight")
    if transport is None and connection_profile is None:
        raise TaskError("PROFILE_REQUIRED", "preflight")
    with TaskDB(directory) as task, task.runner_lock(), ExitStack() as stack:
        if transport is not None:
            # Caller-owned transports retain their preflight order and lifecycle.
            _check_task_transport(task, transport)
        if task.meta("state") in ("PAUSED", "CANCELLED", "FAILED", "BLOCKED") and not resume:
            raise TaskError("EXPLICIT_RESUME_REQUIRED")
        publication = stack.enter_context(
            load_publication(task.meta("publication_path"), full_verify=True)
        )
        header = check_publication_identity(task, publication)
        if connection_profile is not None:
            from .profile import validate_allowlist

            validate_allowlist(connection_profile, publication)
        if transport is None:
            from .profile import connect_profile

            # Credentials and worker creation follow verification under the same lock.
            root = task.workspace.root if task.workspace is not None else task.directory
            _real_output_root(task.directory, physical_root=root)
            transport = stack.enter_context(
                connect_profile(connection_profile, root=root, capacity=task.capacity)
            )
            _check_task_transport(task, transport)
        reconcile(task, publication)
        with task.transaction() as db:
            if resume:
                task.set_meta(db, "request", None)
            task.set_meta(db, "state", "RUNNING")
        try:
            from .pipeline import run_pipeline

            if workers != 1 and not hasattr(transport, "clone"):
                raise TaskError("WORKER_CHANNEL_UNAVAILABLE", "preflight")
            run_pipeline(
                task,
                publication,
                transport,
                workers=workers,
                metadata=header["metadata"],
                control=control,
                fault_hook=fault_hook,
            )
            return task.inspect()
        except BaseException as primary:
            try:
                with task.transaction() as db:
                    task.set_meta(db, "state", "FAILED")
            except BaseException:
                primary.task_secondary = (
                    *getattr(primary, "task_secondary", ()),
                    "TASK_FAILURE_PERSIST_FAILED",
                )
            raise
