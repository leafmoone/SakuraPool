"""Bounded immutable spans for independently claimed, independently proven lanes."""

import re
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import Event, Lock, get_ident

from .diagnostic_codes import _SAFE_CODES, _SAFE_PHASES
from .prepared_fetch import _FACTORY, PreparedFetch
from .transport import RemoteIOError


def _unavailable():
    return RemoteIOError("Shared Range unavailable", code="publication_range",
                         phase="publication_fetch", lightweight=True)


def _failure(error):
    code, phase, status = "publication_range", "publication_fetch", None
    if isinstance(error, RemoteIOError):
        if type(error.code) is str and error.code in _SAFE_CODES:
            code = error.code
        if type(error.phase) is str and error.phase in _SAFE_PHASES:
            phase = error.phase
        if type(error.http_status) is int and 100 <= error.http_status <= 599:
            status = error.http_status
    return code, phase, status, bool(getattr(error, "finalization_secondary", ()))


def _raise_failure(failure):
    code, phase, status, uncertain = failure
    error = RemoteIOError("Shared Range failed", code=code, phase=phase,
                          http_status=status, lightweight=True)
    if uncertain:
        error.finalization_secondary = ("RANGE_FINALIZATION_FAILED",)
    raise error


@dataclass
class _Member:
    prepared: PreparedFetch
    plan: object
    lane: object
    cancelled: Event = field(default_factory=Event)
    started: bool = False

    @property
    def span(self):
        if self.plan.coalesced_range is not None:
            return self.plan.coalesced_range
        if not self.plan.metadata_bytes and self.plan.image_bytes <= self.plan.chunk_bytes:
            return self.plan.image_offset, self.plan.image_bytes
        return None

    @property
    def identity(self):
        p = self.prepared
        return p.content_digest, p.snapshot_id, p.catalog_row


@dataclass(eq=False)
class _Span:
    start: int
    length: int
    binding: object
    remaining: set
    charge: int
    done: Event = field(default_factory=Event)
    data: bytes | None = None
    failure: tuple | None = None
    pending: bool = True
    leases: int = 0


class RawSpans:
    """One run, at most L active claims and one chunk of aggregate resident bytes.

    Pending reads reserve twice their length for the native buffer plus immutable
    copy. Retired/pinned bodies remain charged until their final owned view exits.
    Saturation only disables sharing; it never rejects an otherwise valid Range.
    """

    def __init__(self, lanes, capacity):
        self._owner = get_ident()
        self._limit = lanes
        self._capacity = capacity
        self._lock = Lock()
        self._members, self._assigned = {}, {}
        self._spans = set()
        self._bytes = 0
        self._closed = False

    def register(self, operation, prepared, lane, *, metadata):
        if (get_ident() != self._owner or type(prepared) is not PreparedFetch
                or prepared._provenance is not _FACTORY or prepared._owner != self._owner
                or type(operation) is not str or re.fullmatch(r"[0-9a-f]{32}", operation) is None):
            raise _unavailable()
        plan = prepared.plan(metadata=metadata, capacity=self._capacity)
        with self._lock:
            if self._closed or operation in self._members or len(self._members) >= self._limit:
                raise _unavailable()
            self._members[operation] = _Member(prepared, plan, lane)

    def unregister(self, operation):
        if get_ident() != self._owner:
            raise _unavailable()
        with self._lock:
            member = self._members.pop(operation, None)
            if member is not None:
                member.cancelled.set()
            span = self._assigned.pop(operation, None)
            if span is not None:
                span.remaining.discard(operation)
                self._retire(span)

    def _retire(self, span):
        if not span.pending and not span.leases and not span.remaining:
            self._bytes -= span.charge
            span.charge = 0
            span.data = None
            self._spans.discard(span)

    def _evict_idle(self):
        for span in tuple(self._spans):
            if not span.pending and not span.leases:
                for operation in span.remaining:
                    self._assigned.pop(operation, None)
                span.remaining.clear()
                self._retire(span)

    def _group(self, operation, member):
        candidates = sorted(
            ((op, m) for op, m in self._members.items()
             if op not in self._assigned and (not m.started or op == operation)
             and m.identity == member.identity
             and m.span is not None),
            key=lambda pair: pair[1].span[0])
        index = next((i for i, pair in enumerate(candidates) if pair[0] == operation), None)
        if index is None:
            return None
        selected = [candidates[index]]
        # Only contiguous active single-span neighbours; never unclaimed READY work.
        for neighbour in (index + 1, index - 1, index + 2):
            if len(selected) == 3 or not 0 <= neighbour < len(candidates):
                continue
            proposed = sorted([*selected, candidates[neighbour]],
                              key=lambda pair: pair[1].span[0])
            spans = [m.span for _, m in proposed]
            if any(a + n > b for (a, n), (b, _) in zip(spans, spans[1:])):
                continue
            start = spans[0][0]
            length = spans[-1][0] + spans[-1][1] - start
            useful = sum(m.plan.saved_bytes for _, m in proposed)
            padding = length - useful
            if (length <= self._capacity.range_chunk_bytes and padding <= 65536
                    and padding * 16 <= useful):
                selected = proposed
        if len(selected) < 2:
            return None
        start = selected[0][1].span[0]
        end, length = selected[-1][1].span
        return start, end + length - start, {op for op, _ in selected}

    def _check(self, operation, member):
        with self._lock:
            if (self._closed or member.cancelled.is_set()
                    or self._members.get(operation) is not member):
                raise _unavailable()

    @contextmanager
    def read(self, operation, prepared, lane, bound, offset, length):
        with self._lock:
            member = self._members.get(operation)
            if (self._closed or member is None or member.prepared is not prepared
                    or member.lane is not lane or member.started
                    or member.span != (offset, length)):
                raise _unavailable()
            member.started = True
        # This guard is the consuming lane's proof, never the producer's authority.
        # Its lock is separate from the broker lock, including throughout waits/IO.
        with lane._shared_range_guard(bound, length) as (binding, check):
            owner = False
            with self._lock:
                span = self._assigned.get(operation)
                if span is not None and span.binding != binding:
                    span = None
                elif span is None:
                    group = self._group(operation, member)
                    if group is not None:
                        start, count, operations = group
                        if self._bytes + 2 * count > self._capacity.range_chunk_bytes:
                            self._evict_idle()
                        if self._bytes + 2 * count <= self._capacity.range_chunk_bytes:
                            span = _Span(start, count, binding, operations, 2 * count)
                            self._spans.add(span)
                            self._bytes += span.charge
                            for op in operations:
                                self._assigned[op] = span
                            owner = True
                if span is not None:
                    span.leases += 1
            if span is None:
                self._check(operation, member)
                check()
                with lane.read_range_owned(bound, offset, length) as payload:
                    yield payload
                return
            with self._read_span(operation, member, lane, bound, offset, length,
                                 span, owner, check) as payload:
                yield payload

    @contextmanager
    def _read_span(self, operation, member, lane, bound, offset, length, span, owner, check):
        view = None
        try:
            if owner:
                data = None
                try:
                    with lane.read_range_owned(bound, span.start, span.length) as payload:
                        if len(payload) != span.length:
                            raise _unavailable()
                        data = bytes(payload)
                        del payload
                    # The producer's result is fully finalized before publication.
                    check()
                    self._check(operation, member)
                except BaseException as error:
                    data = payload = None
                    # Finalizer tracebacks can otherwise retain the native body
                    # after its shared reservation is released. Keep typed error
                    # identity/uncertainty, never payload-bearing traceback state.
                    error.__traceback__ = error.__context__ = error.__cause__ = None
                    with self._lock:
                        span.failure = _failure(error)
                        span.pending = False
                        self._bytes -= span.charge
                        span.charge = 0
                        span.done.set()
                        self._retire(span)
                    raise
                with self._lock:
                    span.pending = False
                    self._bytes -= span.charge - span.length
                    span.charge = span.length
                    if self._closed:
                        span.failure = ("publication_range", "publication_fetch", None, False)
                    else:
                        span.data = data
                    del data
                    span.done.set()
                    self._retire(span)
            else:
                # One producer Range uses the existing <=125 s operation deadline.
                # This is a follower fail-safe, not a new producer deadline.
                deadline = time.monotonic() + 130
                while not span.done.wait(0.05):
                    self._check(operation, member)
                    check()
                    if time.monotonic() >= deadline:
                        raise _unavailable()
            check()  # Recheck own generation/closed state after waiting.
            self._check(operation, member)
            with self._lock:
                if span.failure is not None:
                    _raise_failure(span.failure)
                if span.data is None or operation not in span.remaining:
                    raise _unavailable()
                view = memoryview(span.data)[offset - span.start:offset - span.start + length]
            yield view
        finally:
            if view is not None:
                view.release()
            with self._lock:
                span.leases -= 1
                span.remaining.discard(operation)
                if self._assigned.get(operation) is span:
                    del self._assigned[operation]
                self._retire(span)

    def close(self):
        with self._lock:
            self._closed = True
            for member in self._members.values():
                member.cancelled.set()
            self._members.clear()
            self._assigned.clear()
            for span in tuple(self._spans):
                span.remaining.clear()
                if span.pending:
                    span.failure = ("publication_range", "publication_fetch", None, True)
                    span.done.set()
                self._retire(span)
