"""Bounded download lanes: SQLite is owner-only; no consumption RPC or ledger."""

import queue
import sys
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import get_ident

from ..storage.prepared_fetch import PreparedFetch
from ..storage.publication_fetch import _fetch_publication_sample
from ..storage.transport import RemoteIOError
from .plan import canonical
from .store import TaskError, safe_failure


@dataclass
class EventCall:
    lane: int
    operation: str
    event: str
    payload: dict
    reply: object


def _safe_diagnostic(error):
    return safe_failure(error.public_diagnostic()) if isinstance(error, RemoteIOError) else {}



def _warm_free_lane(free, lanes, prepared, capacity):
    """Return a warm idle lane hint without authorizing a fetch."""
    # Match the data plane's next-chunk admission, not an entire multi-chunk image.
    lengths = [min(prepared.location["image_size"], capacity.range_chunk_bytes)]
    for index in free:
        predict = getattr(lanes[index][0], "predict_warm", None)
        if callable(predict) and predict(prepared.transport_identity, lengths):
            return index
    return None


def _select_free_lane(free, lanes, prepared, capacity):
    index = _warm_free_lane(free, lanes, prepared, capacity)
    return free[0] if index is None else index


def run_pipeline(task, publication, transport, *, workers, metadata, fault_hook=None, control=None):
    """At most W active operations; create lanes only for available independent work."""
    from .runner import delivery_mapping, preflight, verify_delivery

    if type(workers) is not int or workers < 1:
        raise TaskError("WORKERS_INVALID", "preflight")
    ready = task.db.execute("SELECT count(*) FROM items WHERE state='READY'").fetchone()[0]
    # This is available work, not an input maximum or a memory-admission estimate.
    lane_count = min(workers, ready)
    window = 16 * lane_count
    owner = get_ident()
    calls = queue.Queue(maxsize=max(1, 2 * lane_count))
    completions = queue.Queue(maxsize=max(1, lane_count))
    active, lanes, free = {}, [], []
    first_error = None
    stop = False
    blocked = False
    warm_bypass_credit = lane_count
    extensions = task.image_extensions
    output = task.directory / "output"

    def hook(lane, item, event, payload):
        if len(canonical(payload)) > min(8192, task.capacity.rpc_line_bytes):
            raise TaskError("ATTEMPT_EVENT_LIMIT")
        reply = queue.Queue(maxsize=1)
        calls.put(EventCall(lane, item["operation_id"], event, payload, reply))
        error = reply.get()
        if error is not None:
            raise error

    def execute(index, item, prepared, mapping):
        error = None
        try:
            lane, cache = lanes[index]
            _fetch_publication_sample(
                prepared._projection(cache),
                item["record_id"],
                lane,
                output,
                metadata=metadata,
                capacity=task.capacity,
                image_extensions=extensions,
                control=control,
                attempt_hook=lambda name, data: hook(index, item, name, data),
                delivery_mapping=mapping,
            )
        except BaseException as caught:
            error = caught
            # No payload-bearing traceback crosses the lane completion queue.
            caught.__traceback__ = None
            caught.__context__ = None
            caught.__cause__ = None
        finally:
            completions.put((index, item, error))

    def pump(abort=None):
        if get_ident() != owner:
            raise TaskError("TASK_OWNER_MISMATCH")
        try:
            call = calls.get(timeout=0.02)
        except queue.Empty:
            return
        error = None
        try:
            if abort is not None:
                raise abort
            current = active.get(call.lane)
            if current is None or current[0]["operation_id"] != call.operation:
                raise TaskError("ATTEMPT_IDENTITY_MISSING")
            task.event(call.operation, call.event, call.payload)
            if fault_hook:
                fault_hook(call.event, current[0])
        except BaseException as caught:
            error = caught
        call.reply.put(error)

    def complete():
        nonlocal first_error, stop, blocked
        while True:
            try:
                index, item, error = completions.get_nowait()
            except queue.Empty:
                return
            blocked = False
            active.pop(index)
            free.append(index)
            if error is None:
                try:
                    row = task.db.execute(
                        "SELECT * FROM items WHERE seq=?", (item["seq"],)
                    ).fetchone()
                    verify_delivery(task, row, publication)
                    task.finish_item(item["seq"], state="DONE", operation=item["operation_id"])
                    if fault_hook:
                        fault_hook("DONE", item)
                except BaseException as caught:
                    error = caught
            if error is not None:
                details = _safe_diagnostic(error)
                if not details:
                    details = {
                        "code": "publication_write",
                        "phase": "publication_fetch",
                        "recoverable": False,
                    }
                # A published receipt is not permission to retry. Recovery will verify it.
                failed_state = (
                    "BLOCKED"
                    if (
                        details.get("delivery") == "PUBLISHED"
                        or details.get("cleanup") == "PRESERVED"
                    )
                    else "FAILED"
                )
                try:
                    task.finish_item(
                        item["seq"],
                        state=failed_state,
                        code=details.get("code", "FETCH_FAILED"),
                        diagnostic=details,
                        operation=item["operation_id"],
                    )
                    if fault_hook:
                        fault_hook("FAILED", item)
                except BaseException as secondary:
                    error.task_secondary = (
                        *getattr(error, "task_secondary", ()),
                        "TASK_FAILURE_PERSIST_FAILED",
                    )
                    if not isinstance(secondary, Exception) and isinstance(error, Exception):
                        error = secondary
                if first_error is None:
                    first_error = error
                stop = True

    def schedule(executor):
        nonlocal stop, blocked, warm_bypass_credit
        if blocked:
            return
        # Immutable owner-created descriptors are reused only in this scheduling pass.
        prepared_cache = {}

        def prepare(candidate):
            key = (candidate["seq"], candidate["rid"], candidate["record_id"])
            value = prepared_cache.get(key)
            if value is None:
                value = PreparedFetch._prepare(
                    publication,
                    candidate["record_id"],
                    capacity=task.capacity,
                    image_extensions=extensions,
                )
                prepared_cache[key] = value
            return value

        while not stop and (free or len(lanes) < lane_count):
            candidates = task.candidates(limit=window)
            if not candidates:
                stop = True
                break
            # Trim before preparing a shifted window; resident lookahead stays <= 16L.
            keys = {(row["seq"], row["rid"], row["record_id"]) for row in candidates}
            prepared_cache = {key: value for key, value in prepared_cache.items() if key in keys}
            busy = {value[1].transport_identity for value in active.values()}
            selected = None
            for position, candidate in enumerate(candidates):
                prepared = prepare(candidate)
                if prepared.transport_identity not in busy:
                    selected = candidate, prepared, position
                    break
            if selected is None:
                # Only completion can free a busy TAR in this immutable, single-runner task.
                # The outer loop still drains events and polls pause/cancel requests.
                blocked = True
                break
            candidate, prepared, position = selected
            index = _warm_free_lane(free, lanes, prepared, task.capacity)
            bypass = False
            if index is None and free and warm_bypass_credit:
                # A bounded number of warm dispatches may bypass the oldest eligible item.
                for later in candidates[position + 1:]:
                    later_prepared = prepare(later)
                    if later_prepared.transport_identity in busy:
                        continue
                    warm = _warm_free_lane(free, lanes, later_prepared, task.capacity)
                    if warm is not None:
                        candidate, prepared, index = later, later_prepared, warm
                        bypass = True
                        break
            if not free:
                # Never precreate W processes for a small task or busy-TAR backlog.
                lane = transport.clone() if hasattr(transport, "clone") else transport
                lanes.append((lane, OrderedDict()))
                free.append(len(lanes) - 1)
            if index is None:
                index = _select_free_lane(free, lanes, prepared, task.capacity)
            preflight(task, publication, candidate, lanes[index][0], prepared=prepared)
            item = task.claim(expected=candidate, window=window)
            if item is None:
                stop = True
                break
            # Failed/no-op claims never consume fairness credit.
            warm_bypass_credit = warm_bypass_credit - 1 if bypass else lane_count
            if fault_hook:
                fault_hook("CLAIMED", item)
            free.remove(index)
            active[index] = (item, prepared)
            try:
                mapping = delivery_mapping(task, item, publication)
                executor.submit(execute, index, item, prepared, mapping)
            except BaseException:
                active.pop(index)
                free.append(index)
                raise

    try:
        with ThreadPoolExecutor(
            max_workers=max(1, lane_count), thread_name_prefix="sakura-download"
        ) as executor:
            # SQLite/publication handles never cross threads. Lazy clone handshakes
            # before requests; each lane holds its own bounded conditional-proof cache.
            try:
                while True:
                    complete()
                    if task.meta("request") is not None:
                        stop = True
                    schedule(executor)
                    if not active:
                        break
                    pump()
            except BaseException as primary:
                stop = True
                first_error = first_error or primary
                # No SQLite call is needed to reject outstanding event RPCs and reap
                # completion envelopes. This also covers SQL/meta/interrupt failures.
                # Workers must be acknowledged before executor.shutdown waits for them.
                while active:
                    try:
                        while True:
                            index, _, _ = completions.get_nowait()
                            active.pop(index, None)
                    except queue.Empty:
                        pass
                    if active:
                        pump(abort=TaskError("COORDINATOR_STOPPED", "download"))
            if first_error is not None:
                raise first_error
            request = task.meta("request")
            with task.transaction() as db:
                task.set_meta(
                    db,
                    "state",
                    "PAUSED"
                    if request == "PAUSE"
                    else "CANCELLED"
                    if request == "CANCEL"
                    else "COMPLETED",
                )
    finally:
        primary = sys.exc_info()[1]
        close_failed = False
        for lane, _ in lanes:
            if lane is transport:
                continue
            try:
                lane.close()
            except BaseException:
                close_failed = True
        # Every owned lane must be attempted, even if the first close fails.
        if close_failed:
            if primary is not None:
                primary.task_secondary = (
                    *getattr(primary, "task_secondary", ()),
                    "LANE_CLOSE_FAILED",
                )
            else:
                raise TaskError("LANE_CLOSE_FAILED", "download") from None
