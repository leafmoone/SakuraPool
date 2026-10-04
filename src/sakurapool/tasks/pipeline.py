"""Internal bounded coordinator RPC: no ledger or SQLite handle crosses threads."""

import queue
from dataclasses import dataclass
from threading import get_ident


class OwnerAuthorityError(RuntimeError):
    """Fixed coordinator authority rejection, distinct from ledger IO failure."""


def _lane_item(row):
    item = {key: row[key] for key in
            ("seq", "rid", "record_id", "attempt_id", "operation_id")}
    if (any(type(item[key]) is not int or not 0 <= item[key] < 1 << 64
            for key in ("seq", "rid"))
            or any(type(item[key]) is not str or len(item[key]) != 32
                   for key in ("record_id", "attempt_id", "operation_id"))):
        raise OwnerAuthorityError("owner item bound")
    return item


def _envelope_bytes(value, depth=0):
    """Conservative bounded typed metadata model; never traverses exception frames."""
    from ..storage.budget import Reservation

    if depth > 5:
        raise OwnerAuthorityError("owner RPC nesting exceeds bound")
    if value is None or type(value) is bool:
        return 32
    if type(value) is int:
        if value.bit_length() > 65:
            raise OwnerAuthorityError("owner RPC integer exceeds bound")
        return 64
    if type(value) is str:
        if len(value) > 4096:
            raise OwnerAuthorityError("owner RPC text exceeds bound")
        return len(value) * 4 + 128
    if type(value) in (tuple, list) and len(value) <= 32:
        return 128 + sum(_envelope_bytes(v, depth + 1) for v in value)
    if type(value) is dict and len(value) <= 32:
        return 128 + sum(_envelope_bytes(k, depth + 1) + _envelope_bytes(v, depth + 1)
                         for k, v in value.items())
    if type(value) is Reservation:
        return 512
    raise OwnerAuthorityError("owner RPC value invalid")


def _safe_diagnostic(error):
    from ..storage.transport import _SAFE_CODES, _SAFE_PHASES

    details = error.public_diagnostic()
    allowed = {"code": _SAFE_CODES, "phase": _SAFE_PHASES,
               "delivery": {"PUBLISHED", "NOT_PUBLISHED"},
               "accounting": {"CONFIRMED", "UNKNOWN"},
               "output_lease": {"CONFIRMED", "UNKNOWN"},
               "accounting_scope": {"OPERATION"}, "cleanup": {"SAFE", "PRESERVED"}}
    safe = {key: value for key, value in details.items()
            if key in allowed and type(value) is str and value in allowed[key]}
    status = details.get("http_status")
    if type(status) is int and 100 <= status <= 599:
        safe["http_status"] = status
    secondary = details.get("secondary", ())
    if type(secondary) in (list, tuple) and len(secondary) <= 16:
        safe["secondary"] = [v for v in secondary if type(v) is str and
                             v in {"CLEANUP_FAILED", "ACCOUNTING_UNKNOWN",
                                   "RANGE_FINALIZATION_FAILED"}]
    return safe


def _rpc_error(error):
    """Discard frames/raw attributes, preserve only process-control object identity."""
    from ..storage.budget import BudgetExceeded

    if isinstance(error, (KeyboardInterrupt, SystemExit)):
        error.__traceback__ = None
        error.__context__ = None
        error.__cause__ = None
        # Process-control identity, code and fixed diagnostics are contractual.
        # Keep the original exclusively on the owner; lane receives a fixed signal.
        return RuntimeError("coordinator process-control interruption")
    if isinstance(error, BudgetExceeded):
        return BudgetExceeded("coordinator budget operation failed")
    return RuntimeError("coordinator operation failed")


@dataclass(frozen=True)
class OwnerCall:
    lane_id: int
    generation: int
    method: str
    args: tuple
    kwargs: dict
    reply: queue.Queue


class LedgerRPC:
    """Lane proxy; only explicit operations run on the coordinator thread."""

    METHODS = frozenset({"reserve", "consume_body", "settle", "status",
                         "condition_proof", "record_condition_proof"})

    def __init__(self, owner, calls, ledger, lane_id, channel):
        self._channel = channel
        self._lane_id = lane_id
        self._owner = owner
        self._calls = calls
        self.root = ledger.root
        self.limits = dict(ledger.limits)
        self.offline_mode = ledger.offline_mode

    def _invoke(self, method, *args, **kwargs):
        if get_ident() == self._owner:
            raise RuntimeError("lane RPC invoked by coordinator")
        if method not in self.METHODS | {"event", "settle_resident", "generation_start"}:
            raise OwnerAuthorityError("owner RPC method invalid")

        if _envelope_bytes(args) + _envelope_bytes(kwargs) > 16384:
            raise OwnerAuthorityError("owner RPC envelope exceeds bound")
        reply = queue.Queue(maxsize=1)
        generation = getattr(self._channel, "_generation", 0)
        self._calls.put(OwnerCall(self._lane_id, generation, method, args, kwargs, reply))
        ok, result = reply.get()
        if not ok:
            raise result
        return result

    def generation_start(self):
        return self._invoke("generation_start")

    def settle_resident(self, lease):
        return self._invoke("settle_resident", lease)

    def __getattr__(self, name):
        if name not in self.METHODS:
            raise AttributeError(name)
        return lambda *args, **kwargs: self._invoke(name, *args, **kwargs)


def run_pipeline(task, publication, transport, *, workers, metadata, fault_hook=None, control=None):
    """Bounded lanes; every durable state and budget operation is owner-dispatched."""
    from collections import OrderedDict
    from concurrent.futures import ThreadPoolExecutor

    from ..storage.prepared_fetch import PreparedFetch
    from ..storage.publication_fetch import _fetch_publication_sample
    from ..storage.transport import RemoteIOError
    from .runner import preflight
    from .store import TaskError

    if control is not None and workers != 1 and not transport.ledger.offline_mode:
        raise TaskError("EXTERNAL_CONTROL_UNAVAILABLE", "preflight")
    if not task.db.execute("SELECT 1 FROM items WHERE state='READY' LIMIT 1").fetchone():
        with task.transaction() as db:
            task.set_meta(db, "state", "COMPLETED")
        return
    owner = get_ident()
    calls = queue.Queue(maxsize=2 * workers)
    completions = queue.Queue(maxsize=workers)
    active = {}
    holds = {}
    lanes = []
    free = list(range(workers))
    first_error = None
    pump_error = None
    stop = False
    # Per lane simultaneous metadata envelope (conservative UTF-32): catalog
    # five text fields <=40KiB + fixed descriptor/item <=16KiB; projection/prepare
    # temporary copies <=64KiB; 2W queued RPCs <=32KiB/lane, W replies <=16KiB,
    # W completions <=16KiB and container overhead <=32KiB: <=216KiB/lane.
    # 256KiB/lane covers these; payload and resident leases are separate.
    from ..storage.budget import Reservation

    queue_lease = transport.ledger.reserve(Reservation(inflight=workers * (256 << 10)))
    reservations = {}
    retained_receipts = {}
    completed = set()
    lease_owners = {}
    generations = {}
    interrupts = {}
    descriptors = {}

    def event(proxy, attempt, name, payload):
        return proxy._invoke("event", attempt, name, payload)

    def execute(lane_index, item, prepared, output):
        result = ("ERROR", {})
        try:
            lane, cache = lanes[lane_index]
            _fetch_publication_sample(prepared._projection(cache), item["record_id"], lane,
                                     output, metadata=metadata, control=control,
                                     attempt_hook=lambda name, data:
                                     event(lane.ledger, item["attempt_id"], name, data))
            result = None
        except BaseException as error:
            try:
                if not isinstance(error, Exception):
                    interrupts[lane_index] = error
                    result = ("INTERRUPT", None)
                else:
                    safe = _safe_diagnostic(error) if isinstance(error, RemoteIOError) else {}
                    result = ("ERROR", safe)
            except BaseException:
                interrupts[lane_index] = error
                result = ("INTERRUPT", None)
            finally:
                error.__traceback__ = None
                error.__context__ = None
                error.__cause__ = None
        finally:
            completed.add(item["attempt_id"])
            completions.put((lane_index, item, result))

    close_errors = {}
    def close_lane(index, lane):
        try:
            lane.close()
            result = None
        except BaseException as error:
            error.__traceback__ = None
            error.__context__ = None
            error.__cause__ = None
            close_errors[index] = error
            result = "FAILED"
        completions.put((None, index, result))

    def pump(timeout=0.02):
        nonlocal pump_error
        try:
            call = calls.get(timeout=timeout)
        except queue.Empty:
            return
        try:
            if call.generation != getattr(lanes[call.lane_id][0], "_generation", 0):
                raise OwnerAuthorityError("owner generation mismatch")
            if call.method == "generation_start":
                previous = generations.get(call.lane_id)
                if previous is not None and call.generation < previous:
                    raise OwnerAuthorityError("owner generation regression")
                if any(lease_owner[0] == call.lane_id and lease_owner[1] is not None
                       for lease_owner in lease_owners.values()):
                    raise OwnerAuthorityError("owner generation has unfinished request leases")
                generations[call.lane_id] = call.generation
                result = None
            elif (call.generation != generations.get(call.lane_id)
                  and call.method != "settle_resident"):
                raise OwnerAuthorityError("owner generation unacknowledged")
            elif call.method == "event":
                attempt, name, payload = call.args
                registered = active.get(call.lane_id)
                if registered is None or registered[0]["attempt_id"] != attempt:
                    raise OwnerAuthorityError("owner event lane mismatch")
                if name == "OUTPUT_RESERVED":
                    lease = payload.get("lease")
                    expected = (call.lane_id, attempt, call.generation)
                    reservation = reservations.get(lease)
                    descriptor = descriptors[call.lane_id]
                    amount = descriptor.location["image_size"]
                    if metadata and descriptor.location["flags"] & 1:
                        amount += descriptor.location["metadata_size"]
                    if (lease_owners.get(lease) != expected or reservation is None
                            or reservation.saved_samples != 1 or reservation.saved_bytes != amount
                            or reservation.disk < amount + 8192):
                        raise OwnerAuthorityError("owner output reservation mismatch")
                if name == "PREPARED":
                    descriptor = descriptors[call.lane_id]
                    names = {"image." + descriptor.image_format: descriptor.location["image_size"]}
                    if metadata and descriptor.location["flags"] & 1:
                        names["metadata.json"] = descriptor.location["metadata_size"]
                    receipt = payload.get("receipt")
                    if not isinstance(receipt, dict) or set(receipt) != set(names):
                        raise OwnerAuthorityError("owner receipt files mismatch")
                    for filename, length in names.items():
                        proof = receipt[filename]
                        if (filename.startswith("image.")
                                and proof.get("sha256") != descriptor.image_sha.hex()):
                            raise OwnerAuthorityError("owner receipt digest mismatch")
                        if (proof.get("bytes") != length or len(proof.get("sha256", "")) != 64
                                or len(proof.get("identity", ())) != 2):
                            raise OwnerAuthorityError("owner receipt extent mismatch")
                if fault_hook:
                    fault_hook("BEFORE_" + name, payload)
                task.event(attempt, name, payload)
                if name == "SETTLED":
                    # Durable confirmation replaces exactly this admission hold.
                    holds.pop(registered[0]["seq"], None)
                if fault_hook:
                    fault_hook(name, payload)
                result = None
            elif call.method == "settle_resident":
                if lease_owners.get(call.args[0]) != (call.lane_id, None):
                    raise OwnerAuthorityError("owner resident ownership mismatch")
                lane = lanes[call.lane_id][0]
                worker = getattr(lane, "_lane_worker", None)
                if (not lane._closed or lane._operation_active
                        or (worker is not None and worker._proc is not None)):
                    raise OwnerAuthorityError("owner resident lifecycle mismatch")
                result = transport.ledger.settle(call.args[0])
                lease_owners.pop(call.args[0], None)
            elif call.method in LedgerRPC.METHODS:
                if call.method in ("consume_body", "settle"):
                    ownership = lease_owners.get(call.args[0])
                    current = active.get(call.lane_id)
                    attempt_id = current[0]["attempt_id"] if current else None
                    if (ownership is None or ownership[0] != call.lane_id
                            or ownership[1] is None or ownership[1] != attempt_id
                            or ownership[2] != call.generation):
                        raise OwnerAuthorityError("owner lease lane mismatch")
                if call.method == "reserve" and call.lane_id not in active:
                    raise OwnerAuthorityError("owner reservation lacks active attempt")
                result = getattr(transport.ledger, call.method)(*call.args, **call.kwargs)
                if call.method == "reserve":
                    current = active.get(call.lane_id)
                    if current is None:
                        raise OwnerAuthorityError("owner reservation lacks active attempt")
                    lease_owners[result] = (call.lane_id, current[0]["attempt_id"], call.generation)
                    reservations[result] = call.args[0]
                elif call.method == "settle":
                    lease_owners.pop(call.args[0], None)
                    reservations.pop(call.args[0], None)
            else:
                raise RuntimeError("unknown owner operation")
            if _envelope_bytes(result) > 16384:
                raise OwnerAuthorityError("owner RPC reply exceeds bound")
            call.reply.put((True, result))
        except BaseException as error:
            if (call.method == "event" or not isinstance(error, Exception)
                    or isinstance(error, OwnerAuthorityError)):
                record_error(error, "OWNER_RPC_FAILED")
            if call.method == "event":
                pump_error = pump_error or error
            call.reply.put((False, _rpc_error(error)))

    def record_error(error, secondary="PIPELINE_SECONDARY"):
        nonlocal first_error
        if error is None:
            return
        if first_error is None:
            first_error = error
        elif error is not first_error:
            first_error.task_secondary = (
                *getattr(first_error, "task_secondary", ()), secondary)

    def finish(item, error):
        primary = error
        try:
            safe = _safe_diagnostic(error) if isinstance(error, RemoteIOError) else {}
        except BaseException:
            return error
        published = False
        try:
            if error is None:
                task.finish_item(item["seq"], state="DONE")
                return None
            published = task.db.execute("SELECT delivery FROM items WHERE seq=?",
                                        (item["seq"],)).fetchone()[0] == "PUBLISHED"
            known = not published and safe.get("accounting") == "CONFIRMED"
            task.finish_item(item["seq"], state="READY" if known else "BLOCKED",
                             code=safe.get("code", "FETCH_UNCONFIRMED"),
                             accounting="CONFIRMED" if known else "UNKNOWN")
        except BaseException as persistence:
            if primary is None:
                return persistence
            primary.task_secondary = (*getattr(primary, "task_secondary", ()),
                                      "TASK_STATE_PERSIST_FAILED")
        if not isinstance(primary, Exception):
            return primary
        converted = TaskError(safe.get("code", "FETCH_UNCONFIRMED"), safe.get("phase", "fetch"))
        converted.safe_details = safe
        converted.delivery = "PUBLISHED" if published else safe.get("delivery", "NOT_PUBLISHED")
        safe["delivery"] = converted.delivery
        converted.task_secondary = getattr(primary, "task_secondary", ())
        return converted

    def decode_error(index, envelope):
        if envelope is None:
            return None
        if envelope[0] == "INTERRUPT":
            return interrupts.pop(index)
        error = RemoteIOError("lane operation failed")
        safe = envelope[1]
        error.public_diagnostic = lambda: safe
        return error

    def consume_completions():
        nonlocal stop
        while not completions.empty():
            index, item, error = completions.get_nowait()
            # Snapshot retained ownership before detaching the active attempt.
            for lease, ownership in lease_owners.items():
                if ownership[0] == index and ownership[1] == item["attempt_id"]:
                    retained_receipts[lease] = (ownership, reservations.get(lease), "PRESERVED")
            completed.discard(item["attempt_id"])
            active.pop(index, None)
            holds.pop(item["seq"], None)
            free.append(index)
            try:
                decoded = decode_error(index, error)
                error = finish(item, decoded)
            except BaseException as failure:
                error = failure
            for lease, ownership in lease_owners.items():
                if ownership[0] == index and ownership[1] == item["attempt_id"]:
                    retained_receipts[lease] = (ownership, reservations.get(lease), "PRESERVED")
            descriptors.pop(index, None)
            if error is not None:
                record_error(error)
                stop = True

    from ..storage.budget import BudgetExceeded

    def observe_bookkeeping():
        if fault_hook:
            fault_hook("BOOKKEEPING", {"active": len(active), "completed": len(completed),
                                       "holds": len(holds)})
    with ThreadPoolExecutor(max_workers=workers) as pool:
        try:
            for _ in range(workers):
                lane = transport.clone()
                # Admission occurs on the coordinator before assigning the proxy.
                try:
                    if hasattr(lane, "enable_persistent"):
                        lane.enable_persistent()
                except BudgetExceeded:
                    lane.close()
                    if not lanes:
                        raise TaskError("RESOURCE_BLOCKED", "preflight", recoverable=True) from None
                    break
                lane_index = len(lanes)
                resident = getattr(lane, "_lane_lease", None)
                if resident is not None:
                    lease_owners[resident] = (lane_index, None)
                generations[lane_index] = getattr(lane, "_generation", 0)
                lane.ledger = LedgerRPC(owner, calls, transport.ledger, lane_index, lane)
                lanes.append((lane, OrderedDict()))
            free = list(range(len(lanes)))
            while True:
                consume_completions()
                observe_bookkeeping()
                done_indices = [index for index, (item, future) in active.items()
                                if future.done()]
                consume_completions()
                if any(index in active for index in done_indices):
                    raise RuntimeError("lane completion publication failed")
                if task.meta("request") is not None or pump_error is not None:
                    stop = True
                    record_error(pump_error)
                while free and not stop:
                    candidates = task._pipeline_candidate(limit=2 * workers)
                    if not candidates:
                        stop = True
                        break
                    # Oldest eligible object wins: hot work never jumps an eligible
                    # older different key. A busy object waits rather than replicating
                    # its binding in every free lane. Window is always <=2W.
                    def object_key(descriptor):
                        return (descriptor.content_digest, descriptor.snapshot_id,
                                *descriptor.catalog_row[:8])
                    busy_keys = {object_key(descriptors[i]) for i in active}
                    selected = None
                    for candidate in candidates:
                        descriptor = PreparedFetch._prepare(publication, candidate["record_id"])
                        if object_key(descriptor) not in busy_keys:
                            selected = (candidate, descriptor)
                            break
                    if selected is None:
                        break
                    item, prepared = selected
                    del descriptor, selected
                    amount = prepared.location["image_size"]
                    if metadata and prepared.location["flags"] & 1:
                        amount += prepared.location["metadata_size"]
                    if (task.meta("confirmed_output_bytes") + sum(holds.values()) + amount
                            > task.meta("max_output_bytes")):
                        record_error(TaskError("RESOURCE_BLOCKED", "preflight", recoverable=True))
                        stop = True
                        break
                    # Pick the real destination before estimating its topology.
                    identity = (*prepared.catalog_row[:5],
                                int.from_bytes(prepared.catalog_row[5], "big"))
                    lengths = [prepared.location["image_size"]]
                    if metadata and prepared.location["flags"] & 1:
                        lengths.append(prepared.location["metadata_size"])
                    # Only free lanes are observed; transport validates its actual
                    # live proof and generation credit, never shared across lanes.
                    warm = [i for i in free if hasattr(lanes[i][0], "predict_warm")
                            and (prepared.content_digest, prepared.snapshot_id,
                                 prepared.location["object_idx"], lanes[i][0],
                                 lanes[i][0].ledger) in lanes[i][1]
                            and lanes[i][0].predict_warm(identity, lengths)]
                    index = warm[0] if warm else free[0]
                    lane, _ = lanes[index]
                    try:
                        output = preflight(task, publication, item, lane, prepared=prepared,
                                           proof_warm=index in warm, ledger=transport.ledger)
                    except BaseException as error:
                        record_error(error)
                        stop = True
                        break
                    item = task._pipeline_claim(item, window=2 * workers)
                    if item is None:
                        stop = True
                        break
                    if fault_hook:
                        fault_hook("CLAIMED", item)
                    if task.meta("request") is not None:
                        task.finish_item(item["seq"], state="READY")
                        stop = True
                        break
                    item = _lane_item(item)
                    free.remove(index)
                    holds[item["seq"]] = amount
                    descriptors[index] = prepared
                    future = pool.submit(execute, index, item, prepared, output)
                    active[index] = (item, future)
                pump()
                consume_completions()
                if not active and stop:
                    break
        except BaseException as error:
            record_error(error)
        finally:
            # Even a coordinator failure must serve outstanding ledger/events
            # while lanes finish; waiting for futures before pumping deadlocks.
            while active:
                pump()
                consume_completions()
                done_indices = [index for index, (item, future) in active.items()
                                if future.done()]
                consume_completions()
                for index in done_indices:
                    if index in active:
                        item, future = active[index]
                        completed.add(item["attempt_id"])
                        completions.put_nowait((index, item, ("ERROR", {})))
                consume_completions()
            closing = []
            for index, (lane, _) in enumerate(lanes):
                try:
                    closing.append(pool.submit(close_lane, index, lane))
                except BaseException as error:
                    record_error(error, "LANE_CLOSE_SUBMIT_FAILED")
            while any(not future.done() for future in closing):
                pump()
            for future in closing:
                future.result()
            while not completions.empty():
                _, index, signal = completions.get_nowait()
                if signal is not None:
                    record_error(close_errors.pop(index), "LANE_CLOSE_FAILED")
            for lease, receipt in retained_receipts.items():
                # The ledger's pending lease is the durable evidence. This local
                # receipt supplies ownership only; there is deliberately no auto
                # recovery/refund based on exception deletion or process death.
                if receipt[1] is not None and receipt[1].inflight:
                    if first_error is not None:
                        first_error.payload_resources = "PRESERVED"
            try:
                transport.ledger.settle(queue_lease)
            except BaseException as error:
                record_error(error, "QUEUE_SETTLEMENT_FAILED")
    if first_error is not None:
        raise first_error
    request = task.meta("request")
    with task.transaction() as db:
        task.set_meta(db, "state", "PAUSED" if request == "PAUSE" else
                      "CANCELLED" if request == "CANCEL" else "COMPLETED")
