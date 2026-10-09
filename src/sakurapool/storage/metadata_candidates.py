"""Run-scoped, bounded reuse of unbound fixed-revision metadata candidates."""

import re
from collections import OrderedDict
from dataclasses import dataclass, field
from threading import Event, Lock

from .diagnostic_codes import _SAFE_CODES, _SAFE_PHASES
from .production import ProviderObject
from .transport import RemoteIOError

MAX_CANDIDATES = 64
MAX_PENDING = 64
# Follower fail-safe, not a total deadline for the leader's paginated lookup.
WAIT_SECONDS = 125.0


@dataclass(frozen=True)
class MetadataKey:
    publication: str
    snapshot: str
    endpoint: str
    origin: str
    repository: str
    repo_type: str
    revision: str
    path: str
    size: int
    digest: str
    test: bool

    def __post_init__(self):
        for value in (self.publication, self.snapshot, self.digest):
            if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
                raise ValueError("metadata cache identity")
        if (any(type(value) is not str or not 0 < len(value) <= 2048 for value in
                (self.endpoint, self.origin, self.repository, self.repo_type,
                 self.revision, self.path))
                or type(self.size) is not int or not 0 < self.size < (1 << 64)
                or type(self.test) is not bool):
            raise ValueError("metadata cache identity")


@dataclass
class _Flight:
    done: Event = field(default_factory=Event)
    value: object = None
    failure: tuple | None = None


def _failure(error):
    """Copy fixed typed diagnostics only, never an exception/traceback or its text."""
    if isinstance(error, RemoteIOError):
        code, phase, status = error.code, error.phase, error.http_status
        if (type(code) is str and code in _SAFE_CODES
                and type(phase) is str and phase in _SAFE_PHASES):
            status = status if type(status) is int and 100 <= status <= 599 else None
            return code, phase, status
    return "provider_listing_incomplete", "provider_exact_lookup", None


def _error(failure):
    code, phase, status = failure
    return RemoteIOError("Shared metadata lookup unavailable", code=code, phase=phase,
                         http_status=status, lightweight=True)


def _checked(key, value):
    if (type(value) is not ProviderObject or value.validator is not None
            or value.cdn_host is not None
            or (value.origin, value.repo_id, value.repo_type, value.revision,
                value.object_path, value.object_size) !=
               (key.origin, key.repository, key.repo_type, key.revision, key.path, key.size)):
        raise ValueError("unbound metadata candidate required")
    value.validate(test=key.test)
    return value


class MetadataCandidates:
    """Positive candidates only; each recipient must create its own conditional proof."""

    def __init__(self):
        self._lock = Lock()
        self._values = OrderedDict()
        self._pending = {}
        self._closed = False

    def lookup(self, key, loader):
        if type(key) is not MetadataKey:
            raise ValueError("typed metadata cache key required")
        owner = False
        with self._lock:
            if self._closed:
                raise _error(("metadata_cache_closed", "provider_exact_lookup", None))
            value = self._values.get(key)
            if value is not None:
                self._values.move_to_end(key)
                return value
            flight = self._pending.get(key)
            if flight is None and len(self._pending) < MAX_PENDING:
                flight = _Flight()
                self._pending[key] = flight
                owner = True
        if flight is None:
            # Saturation must neither grow shared state nor reject valid work.
            value = _checked(key, loader())
            with self._lock:
                if self._closed:
                    raise _error(("metadata_cache_closed", "provider_exact_lookup", None))
            return value
        if not owner:
            if not flight.done.wait(WAIT_SECONDS):
                raise _error(("metadata_cache_wait", "provider_exact_lookup", None))
            if flight.failure is not None:
                raise _error(flight.failure)
            return flight.value
        try:
            value = _checked(key, loader())
        except BaseException as error:
            failure = _failure(error)
            with self._lock:
                self._pending.pop(key, None)
                flight.failure = failure
                flight.done.set()
            raise
        with self._lock:
            self._pending.pop(key, None)
            if self._closed:
                flight.failure = ("metadata_cache_closed", "provider_exact_lookup", None)
            else:
                self._values[key] = value
                self._values.move_to_end(key)
                while len(self._values) > MAX_CANDIDATES:
                    self._values.popitem(last=False)
                flight.value = value
            flight.done.set()
        if flight.failure is not None:
            raise _error(flight.failure)
        return value

    def close(self):
        with self._lock:
            self._closed = True
            self._values.clear()
            for flight in self._pending.values():
                flight.failure = ("metadata_cache_closed", "provider_exact_lookup", None)
                flight.done.set()
            self._pending.clear()
