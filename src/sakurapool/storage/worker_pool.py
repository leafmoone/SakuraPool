"""Run-owned groups of eight isolated native channels; no shared proof authority."""

import json
import queue
import threading
import time
import uuid

from .production_resources import METADATA_RESPONSE_LINE_BYTES, METADATA_RESPONSE_NODES
from .rust_bridge import RustWorker, RustWorkerError, _json_peak

CHANNELS = 8
CAPABILITY = "multiplex_channel_v1"


def _error():
    return RustWorkerError("worker channel group unavailable")


class WorkerPool:
    def __init__(self, owner, limit):
        self.owner = owner
        self.limit = limit
        self._lock = threading.Lock()
        self._closed = threading.Event()
        self._failed = threading.Event()
        self._groups = []

    def validate(self, worker, root, origin, capacity, token, cookie, test):
        owner = self.owner
        if (worker != owner.worker or root != owner.root or origin != owner.origin
                or capacity != owner.capacity or token != owner._token
                or cookie != owner._cookie or test != owner._test):
            raise _error()

    def attach(self, channel, deadline):
        with channel._construction_lock, self._lock:
            if (channel._released or channel._stop.is_set()
                    or channel._cancel_event is not None and channel._cancel_event.is_set()):
                raise _error()
            if self._closed.is_set() or self._failed.is_set():
                raise _error()
            occupied = sum(sum(slot is not None for slot in group.slots)
                           for group in self._groups)
            if occupied >= self.limit:
                raise _error()
            selected = None
            for group in self._groups:
                with group.lock:
                    if not group.dead.is_set() and None in group.slots:
                        selected = group
                        break
            if selected is None:
                if len(self._groups) >= (self.limit + CHANNELS - 1) // CHANNELS:
                    raise _error()
                selected = _Group(self)
                self._groups.append(selected)
            with selected.lock:
                index = selected.slots.index(None)
                selected.generations[index] += 1
                if selected.generations[index] >= 1 << 64:
                    raise _error()
                channel._group, channel._slot = selected, index
                channel._generation = selected.generations[index]
                selected.slots[index] = channel
        # No pool/group lock covers a process construction or acknowledgement wait.
        selected.start(deadline)
        if (channel._stop.is_set()
                or channel._cancel_event is not None and channel._cancel_event.is_set()):
            channel.cancel(deadline=deadline)
            raise _error()
        channel._control("open", deadline)

    def prepare_close(self, deadline):
        """Queue every idle slot close before waiting on any one group."""
        self._closed.set()
        with self._lock:
            groups = tuple(self._groups)
        for group in groups:
            with group.lock:
                channels = tuple(c for c in group.slots if c is not None)
            for channel in channels:
                try:
                    channel.signal_cancel(deadline=deadline)
                except BaseException:
                    group.signal(deadline=deadline)

    def resume(self):
        """Only a fixed foreground owner can reuse an entirely idle healthy group set."""
        with self._lock:
            if not self._closed.is_set() or self._failed.is_set():
                raise _error()
            for group in self._groups:
                with group.lock:
                    if group.dead.is_set() or any(c is not None for c in group.slots):
                        raise _error()
            self._closed.clear()

    def park(self):
        with self._lock:
            if not self._closed.is_set() or self._failed.is_set():
                raise _error()
            for group in self._groups:
                with group.lock:
                    if (group.dead.is_set() or any(c is not None for c in group.slots)
                            or not group._outbound.empty()):
                        raise _error()

    def signal_cancel(self, *, deadline=None):
        deadline = min(deadline or float("inf"), time.monotonic() + 5)
        self._closed.set()
        with self._lock:
            groups = tuple(self._groups)
        for group in groups:
            group.signal(shutdown=True, deadline=deadline)
    def close(self, *, deadline=None):
        deadline = min(deadline or float("inf"), time.monotonic() + 5)
        self.signal_cancel(deadline=deadline)
        with self._lock:
            groups = tuple(self._groups)
        failed = self._failed.is_set()
        for group in groups:
            try:
                group.reap(deadline)
            except BaseException:
                failed = True
        if failed:
            raise _error()


class _Group:
    def __init__(self, pool):
        self.pool = pool
        self.lock = threading.Lock()
        self.slots = [None] * CHANNELS
        self.generations = [0] * CHANNELS
        self.dead = threading.Event()
        self.started = threading.Event()
        self.initialized = threading.Event()
        self.worker = RustWorker.__new__(RustWorker)
        self.router = self.writer = None
        self._outbound = queue.Queue(maxsize=CHANNELS)
        self._start_lock = threading.Lock()
        self._reap_lock = threading.Lock()
        self._deadline = None

    def start(self, deadline):
        with self._start_lock:
            if self.dead.is_set():
                self.initialized.set()
                raise _error()
            initialize = not self.started.is_set()
            self.started.set()
        if initialize:
            try:
                RustWorker.__init__(self.worker, self.pool.owner.worker,
                                    capacity=self.pool.owner.capacity, lightweight=True,
                                    metadata=True, _multiplex=True, deadline=deadline,
                                    cancel_event=self.dead)
                self.worker._line_bytes += 512
                self.worker._response_line_bytes = METADATA_RESPONSE_LINE_BYTES + 512
                self.router = threading.Thread(target=self._route, name="sakura-channel-router",
                                               daemon=True)
                self.router.start()
                self.writer = threading.Thread(target=self._write, name="sakura-channel-writer",
                                               daemon=True)
                self.writer.start()
            except BaseException:
                self.signal()
                raise
            finally:
                self.initialized.set()
        elif not self.initialized.wait(max(0, deadline - time.monotonic())):
            self.signal()
            raise _error()
        if self.dead.is_set():
            raise _error()

    def _route(self):
        try:
            while not self.dead.is_set():
                with self.lock:
                    expired = any(channel is not None and channel._pending is not None
                                  and time.monotonic() >= channel._pending[2]
                                  for channel in self.slots)
                if expired:
                    raise _error()
                try:
                    message = self.worker._receive(
                        max_line_bytes=METADATA_RESPONSE_LINE_BYTES + 512,
                        max_nodes=METADATA_RESPONSE_NODES + 32,
                        deadline=time.monotonic() + .05)
                except RustWorkerError as error:
                    if error.diagnostic_code == "worker_timeout" and not self.dead.is_set():
                        continue
                    raise
                if (set(message) != {"type", "channel", "generation", "reply"}
                        or message["type"] != "channel_response"
                        or type(message["channel"]) is not int
                        or not 0 <= message["channel"] < CHANNELS
                        or type(message["generation"]) is not int):
                    raise _error()
                with self.lock:
                    channel = self.slots[message["channel"]]
                    reply = message["reply"]
                    if (channel is None or channel._generation != message["generation"]
                            or channel._pending is None or channel._reply is not None
                            or type(reply) is not dict
                            or set(reply) not in ({"type", "request_id", "ok", "result"},
                                                  {"type", "request_id", "ok", "result", "error"})
                            or reply.get("type") != "response"
                            or reply.get("request_id") != channel._pending[0]
                            or type(reply.get("ok")) is not bool
                            or type(reply.get("result")) is not dict
                            or self.worker._last_received_bytes >
                               (METADATA_RESPONSE_LINE_BYTES if channel.metadata else
                                self.pool.owner.capacity.rpc_line_bytes) + 512):
                        raise _error()
                    channel._reply = reply
                    channel._wake.set()
                message = reply = channel = None
        except BaseException:
            if not self.dead.is_set():
                self.signal()

    def _write(self):
        message = None
        try:
            while not self.dead.is_set():
                try:
                    message = self._outbound.get(timeout=.05)
                except queue.Empty:
                    continue
                self.worker.send_raw((json.dumps(message, separators=(",", ":")) + "\n").encode())
                message = None
        except BaseException:
            if not self.dead.is_set():
                self.signal()
        finally:
            message = None

    def signal(self, *, shutdown=False, deadline=None):
        if not shutdown:
            self.pool._failed.set()
        with self.lock:
            self._deadline = min(self._deadline or float("inf"), deadline or time.monotonic() + 5)
            self.dead.set()
            for channel in self.slots:
                if channel is not None:
                    channel._reply = None
                    channel._wake.set()
            while True:
                try:
                    self._outbound.get_nowait()
                except queue.Empty:
                    break
        if hasattr(self.worker, "_stop"):
            self.worker.signal_cancel(deadline=self._deadline)

    def reap(self, deadline):
        deadline = min(deadline, self._deadline or float("inf"))
        with self._start_lock:
            if self.dead.is_set() and not self.started.is_set():
                self.initialized.set()
        if not self.initialized.wait(max(0, deadline - time.monotonic())):
            raise _error()
        if not self._reap_lock.acquire(timeout=max(0, deadline - time.monotonic())):
            raise _error()
        try:
            primary = None
            try:
                if hasattr(self.worker, "_proc"):
                    self.worker.cancel(deadline=deadline)
            except BaseException as error:
                primary = error
            failed = False
            for thread in (self.router, self.writer):
                if (thread is not None and thread.ident is not None
                        and thread is not threading.current_thread()):
                    thread.join(timeout=max(0, deadline - time.monotonic()))
                    failed |= thread.is_alive()
            if primary is not None:
                raise primary
            if failed:
                raise _error()
        finally:
            self._reap_lock.release()

    def send(self, message):
        try:
            with self.lock:
                if self.dead.is_set():
                    raise _error()
                self._outbound.put_nowait(message)
        except BaseException:
            self.signal()
            raise


class PooledWorker(RustWorker):
    """RustWorker-compatible channel. A normal idle signal closes only this slot."""

    def __new__(cls, *args, **kwargs):
        self = super().__new__(cls)
        self._construction_lock = threading.Lock()
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._group = None
        self._pending = self._reply = None
        self._closing = self._released = False
        self._construction_ready = False
        self._teardown_deadline = None
        return self

    def __init__(self, pool, *, metadata=False, timeout_s=60.0, deadline=None, cancel_event=None):
        self.metadata, self.lightweight = metadata, True
        self.capacity = pool.owner.capacity
        self.timeout_s = timeout_s
        self._line_bytes = self.capacity.rpc_line_bytes
        self._response_line_bytes = self._line_bytes
        self._cancel_event = cancel_event
        self._seen = set()
        deadline = min(deadline or float("inf"), time.monotonic() + timeout_s)
        try:
            if self._stop.is_set() or cancel_event is not None and cancel_event.is_set():
                raise _error()
            pool.attach(self, deadline)
            worker = self._group.worker
            self.capabilities = worker.capabilities
            self.metadata_limits = worker.metadata_limits
            self.protocol_resident_bytes = worker.protocol_resident_bytes
            self.worker_version = worker.worker_version
            self.protocol_version = 3
            self._construction_ready = True
        except BaseException as primary:
            try:
                self.cancel(deadline=min(deadline, time.monotonic() + 5))
            except BaseException:
                primary.finalization_secondary = ("WORKER_GROUP_FINALIZATION_FAILED",)
            raise

    @property
    def _proc(self):
        return (None if self._released or self._group is None else
                getattr(self._group.worker, "_proc", None))

    @property
    def _alive(self):
        return (not self._stop.is_set() and not self._released and self._group is not None
                and not self._group.dead.is_set())

    def _begin(self, request_id, kind, deadline):
        group = self._group
        with group.lock:
            if group.dead.is_set() or self._released or self._pending is not None:
                raise _error()
            self._pending = request_id, kind, deadline
            self._reply = None
            self._wake.clear()

    def _take(self, deadline):
        group = self._group
        with group.lock:
            if self._pending is None:
                raise _error()
            deadline = min(deadline, self._pending[2])
        while not self._wake.wait(min(.05, max(0, deadline - time.monotonic()))):
            if time.monotonic() >= deadline:
                group.signal()
                raise RustWorkerError("worker timed out")
        if time.monotonic() >= deadline:
            group.signal()
            raise RustWorkerError("worker timed out")
        with group.lock:
            if group.dead.is_set() or self._reply is None:
                raise _error()
            reply = self._reply
            self._reply = self._pending = None
            self._wake.clear()
        if time.monotonic() >= deadline:
            reply = None
            group.signal()
            raise RustWorkerError("worker timed out")
        return reply

    def _control(self, kind, deadline):
        request_id = uuid.uuid4().hex
        self._begin(request_id, kind, deadline)
        message = {"type": "channel_" + kind, "channel": self._slot,
                   "generation": self._generation, "request_id": request_id}
        if kind == "open":
            message["metadata"] = self.metadata
        self._group.send(message)
        reply = self._take(deadline)
        expected = "open" if kind == "open" else "closed"
        if reply.get("ok") is not True or reply.get("result") != {"channel_state": expected}:
            self._group.signal()
            raise _error()

    def _send(self, line):
        self._send_deadline(line, time.monotonic() + self.timeout_s)

    def _send_deadline(self, line, deadline):
        if not self._alive or len(line) > self._line_bytes:
            raise _error()
        _json_peak(line)
        try:
            request = json.loads(line)
        except (ValueError, UnicodeError):
            self._group.signal()
            raise _error() from None
        identity = request.get("request_id") if type(request) is dict else None
        if (type(request) is not dict
                or set(request) != {"type", "request_id", "operation", "payload"}
                or request["type"] != "request" or type(identity) is not str
                or not 0 < len(identity) <= 64 or len(self._seen) >= 256
                or identity in self._seen):
            self._group.signal()
            raise _error()
        self._seen.add(identity)
        self._begin(identity, "request", deadline)
        self._group.send({"type": "channel_request", "channel": self._slot,
                          "generation": self._generation, "request": request})

    def _exchange_deadline(self, line, *, max_line_bytes, deadline, max_nodes=None):
        self._send_deadline(line, deadline)
        result = self._take(deadline)
        if time.monotonic() >= deadline:
            result = None
            self._group.signal()
            raise RustWorkerError("worker timed out")
        return result

    def _receive(self, *, max_line_bytes=None, deadline=None, max_nodes=None):
        return self._take(deadline or time.monotonic() + self.timeout_s)

    def signal_cancel(self, *, deadline=None):
        self._stop.set()
        self._teardown_deadline = min(self._teardown_deadline or float("inf"),
                                      deadline or time.monotonic() + 5)
        with self._construction_lock:
            group = self._group
        if group is None or self._released:
            return
        with group.lock:
            busy = self._pending is not None
            already = self._closing
            self._closing = True
        if already:
            return
        if busy or not self._construction_ready:
            group.signal(deadline=self._teardown_deadline)
            return
        request_id = uuid.uuid4().hex
        try:
            self._begin(request_id, "close", self._teardown_deadline)
            group.send({"type": "channel_close", "channel": self._slot,
                        "generation": self._generation, "request_id": request_id})
        except BaseException:
            group.signal(deadline=self._teardown_deadline)

    def close(self, *, deadline=None):
        if self._released:
            return
        self.signal_cancel(deadline=deadline)
        with self._construction_lock:
            group = self._group
            if group is None:
                self._released = True
                return
        limit = self._teardown_deadline
        if group.dead.is_set():
            group.reap(limit)
            raise _error()
        reply = self._take(limit)
        if reply.get("ok") is not True or reply.get("result") != {"channel_state": "closed"}:
            group.signal(deadline=limit)
            group.reap(limit)
            raise _error()
        with group.lock:
            if group.slots[self._slot] is not self:
                raise _error()
            group.slots[self._slot] = None
            self._released = True

    def cancel(self, *, deadline=None):
        if self._released:
            return
        if self._closing:
            self.close(deadline=deadline)
            return
        self._stop.set()
        with self._construction_lock:
            group = self._group
            if group is None:
                self._released = True
                return
        limit = min(deadline or float("inf"), time.monotonic() + 5)
        group.signal(deadline=limit)
        group.reap(limit)
        self._released = True
