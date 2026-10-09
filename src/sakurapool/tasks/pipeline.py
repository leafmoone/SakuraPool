"""Bounded download lanes: SQLite is owner-only; no consumption RPC or ledger."""

import queue
import sys
import time
from collections import Counter, OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import get_ident

from ..storage.prepared_fetch import PreparedFetch
from ..storage.publication_fetch import _fetch_publication_sample
from ..storage.transport import RemoteIOError
from .plan import canonical
from .ready_window import ReadyWindow
from .store import MAX_EVENT_BATCH, TaskError, safe_failure


@dataclass
class EventCall:
    lane: int
    operation: str
    event: str
    payload: dict
    reply: object


def _safe_diagnostic(error):
    return safe_failure(error.public_diagnostic()) if isinstance(error, RemoteIOError) else {}



def _warm_free_lane(free, lanes, prepared, capacity, *, metadata=False):
    """Return a warm idle lane hint without authorizing a fetch."""
    # Match the data plane's next-chunk admission, not an entire multi-chunk image.
    lengths = [prepared.plan(metadata=True, capacity=capacity).first_length] if metadata else [
        min(prepared.location["image_size"], capacity.range_chunk_bytes)]
    for index in free:
        predict = getattr(lanes[index][0], "predict_warm", None)
        if callable(predict) and predict(prepared.transport_identity, lengths):
            return index
    return None


def _select_free_lane(free, lanes, prepared, capacity, *, metadata=False):
    index = _warm_free_lane(free, lanes, prepared, capacity, metadata=metadata)
    return free[0] if index is None else index


def run_pipeline(task, publication, transport, *, workers, metadata, workers_per_tar=6,
                 fault_hook=None, control=None):
    """At most W active operations, with an explicit per-TAR lane bound."""
    from ..image_formats import ImageFormatError
    from ..storage.metadata_candidates import MetadataCandidates
    from ..storage.production import RustProductionTransport
    from .runner import delivery_mapping, format_failure, preflight, verify_delivery

    if type(workers) is not int or workers < 1:
        raise TaskError("WORKERS_INVALID", "preflight")
    if type(workers_per_tar) is not int or workers_per_tar < 1:
        raise TaskError("WORKERS_PER_TAR_INVALID", "preflight")
    ready = task.db.execute("SELECT count(*) FROM items WHERE state='READY'").fetchone()[0]
    # This is available work, not an input maximum or a memory-admission estimate.
    lane_count = min(workers, ready)
    per_tar_limit = min(workers_per_tar, lane_count)
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
    # Immutable descriptors and READY heads belong to this verified owner run.
    # Lane proof caches remain independent and are never scheduling authority.
    tar_active = Counter()

    def prepare(candidate):
        try:
            return PreparedFetch._prepare(
                publication, candidate["record_id"], capacity=task.capacity,
                image_extensions=extensions)
        except ImageFormatError as error:
            raise format_failure(error, candidate) from None

    ready_window = ReadyWindow(window, prepare)
    metadata_candidates = (MetadataCandidates() if control is None
                           and isinstance(transport, RustProductionTransport)
                           and transport.lightweight else None)

    from ..storage.raw_spans import RawSpans

    raw_spans = (RawSpans(lane_count, task.capacity) if control is None
                 and type(transport) is RustProductionTransport
                   and transport.lightweight else None)

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
            projection = prepared._projection(cache)
            if metadata_candidates is not None:
                projection._metadata_candidates = metadata_candidates
            if raw_spans is not None:
                projection._raw_span_claim = (raw_spans, item["operation_id"], prepared)
            _fetch_publication_sample(
                projection,
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
        nonlocal first_error, stop
        if get_ident() != owner:
            raise TaskError("TASK_OWNER_MISMATCH")
        try:
            call = calls.get(timeout=0.02)
        except queue.Empty:
            return
        batch = [call]
        # Never wait to fill a batch: each lane already waits for this durable ack.
        while len(batch) < MAX_EVENT_BATCH:
            try:
                batch.append(calls.get_nowait())
            except queue.Empty:
                break
        error = None
        try:
            if abort is not None:
                raise abort
            for call in batch:
                current = active.get(call.lane)
                if current is None or current[0]["operation_id"] != call.operation:
                    raise TaskError("ATTEMPT_IDENTITY_MISSING")
            task.event_batch([(call.operation, call.event, call.payload) for call in batch])
            # All events are durable before any hook or worker can advance.
            if fault_hook:
                for call in batch:
                    fault_hook(call.event, active[call.lane][0])
        except BaseException as caught:
            error = caught
            first_error = first_error or caught
            stop = True
        finally:
            # Even a hook/commit failure must release every waiter before teardown.
            for call in batch:
                # Keep the original primary on the owner; exceptions raised by
                # several lanes must not share a mutable payload-bearing traceback.
                call.reply.put(None if error is None else RemoteIOError(
                    "Coordinator event rejected", code="publication_write",
                    phase="publication_fetch", lightweight=True))

    def complete():
        nonlocal first_error, stop, blocked
        while True:
            try:
                index, item, error = completions.get_nowait()
            except queue.Empty:
                return
            blocked = False
            _, completed = active.pop(index)
            if raw_spans is not None:
                raw_spans.unregister(item["operation_id"])
            tar_active[completed.transport_identity] -= 1
            if not tar_active[completed.transport_identity]:
                del tar_active[completed.transport_identity]
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
        while not stop and (free or len(lanes) < lane_count):
            if not ready_window.refill(task):
                stop = True
                break
            selected = None
            admission_limit = per_tar_limit
            if per_tar_limit > 1:
                # Extra same-TAR slots follow all visible independent TAR heads.
                selected = ready_window.select(tar_active, 1)
                if selected is not None:
                    admission_limit = 1
            if selected is None:
                selected = ready_window.select(tar_active, per_tar_limit)
            if selected is None:
                # Only completion can free a TAR slot in this immutable, single-runner task.
                # The outer loop still drains events and polls pause/cancel requests.
                blocked = True
                break
            candidate, prepared = selected
            bypass = False
            index = _warm_free_lane(free, lanes, prepared, task.capacity, metadata=metadata)
            if index is None and free and warm_bypass_credit:
                # A bounded number of warm dispatches may bypass the oldest eligible item.
                for later in ready_window.later(candidate["seq"]):
                    later_prepared = ready_window.prepare(later)
                    if tar_active[later_prepared.transport_identity] >= admission_limit:
                        continue
                    warm = _warm_free_lane(free, lanes, later_prepared, task.capacity,
                                           metadata=metadata)
                    if warm is not None:
                        candidate, prepared, index = later, later_prepared, warm
                        bypass = True
                        break
            if not free:
                # Never precreate W processes for a small task or busy-TAR backlog.
                has_clone = hasattr(transport, "clone")
                lane = transport.clone() if has_clone else transport
                if workers_per_tar > 1 and has_clone and (
                    lane is transport or any(lane is existing for existing, _ in lanes)
                ):
                    # A borrowed alias is not a new owned lane and must not be closed here.
                    raise TaskError("WORKER_CHANNEL_UNAVAILABLE", "preflight")
                lanes.append((lane, OrderedDict()))
                free.append(len(lanes) - 1)
            if index is None:
                index = _select_free_lane(free, lanes, prepared, task.capacity, metadata=metadata)
            preflight(task, publication, candidate, lanes[index][0], prepared=prepared)
            item = task.claim(expected=candidate, window=window)
            if item is None:
                stop = True
                break
            ready_window.remove(candidate)
            # Failed/no-op claims never consume fairness credit.
            warm_bypass_credit = warm_bypass_credit - 1 if bypass else lane_count
            if fault_hook:
                fault_hook("CLAIMED", item)
            free.remove(index)
            active[index] = (item, prepared)
            tar_active[prepared.transport_identity] += 1
            try:
                mapping = delivery_mapping(task, item, publication)
                if raw_spans is not None:
                    raw_spans.register(item["operation_id"], prepared, lanes[index][0],
                                       metadata=metadata)
                executor.submit(execute, index, item, prepared, mapping)
            except BaseException:
                if raw_spans is not None:
                    raw_spans.unregister(item["operation_id"])
                active.pop(index)
                tar_active[prepared.transport_identity] -= 1
                if not tar_active[prepared.transport_identity]:
                    del tar_active[prepared.transport_identity]
                free.append(index)
                raise

    worker_pool = (transport._begin_task_pool(lane_count) if control is None
                   and type(transport) is RustProductionTransport
                   and transport.lightweight else None)
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
                if metadata_candidates is not None:
                    metadata_candidates.close()
                if raw_spans is not None:
                    raw_spans.close()
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
        if raw_spans is not None:
            raw_spans.close()
        if metadata_candidates is not None:
            metadata_candidates.close()
        close_failed = False
        close_deadline = None
        if worker_pool is not None:
            close_deadline = time.monotonic() + 5
            try:
                worker_pool.prepare_close(close_deadline)
            except BaseException:
                close_failed = True
            for lane, _ in lanes:
                lane._close_deadline = close_deadline
        for lane, _ in lanes:
            if lane is transport:
                continue
            try:
                lane.close()
            except BaseException:
                close_failed = True
        if worker_pool is not None:
            try:
                transport._end_task_pool(worker_pool, deadline=close_deadline)
            except BaseException:
                close_failed = True
        # Every owned lane/control/group must be attempted despite earlier failures.
        if close_failed:
            if primary is not None:
                primary.task_secondary = (
                    *getattr(primary, "task_secondary", ()),
                    "LANE_CLOSE_FAILED",
                )
            else:
                raise TaskError("LANE_CLOSE_FAILED", "download") from None
