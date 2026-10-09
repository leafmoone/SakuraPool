"""Owner-only bounded READY lookahead and lazily prepared per-TAR heads."""

from collections import OrderedDict, deque

from .store import TaskError


class ReadyWindow:
    def __init__(self, limit, prepare):
        self.limit = limit
        self._prepare = prepare
        self._rows = OrderedDict()
        self._unknown = deque()
        self._prepared = {}
        self._groups = {}
        self._tail = None
        self._ended = False

    def refill(self, task):
        missing = self.limit - len(self._rows)
        if missing and not self._ended:
            rows = task.candidates(limit=missing, after=self._tail)
            if len(rows) > missing:
                raise TaskError("TASK_IDENTITY_INVALID")
            for row in rows:
                seq = row["seq"]
                if self._tail is not None and seq <= self._tail:
                    raise TaskError("TASK_IDENTITY_INVALID")
                self._rows[seq] = row
                self._unknown.append(seq)
                self._tail = seq
            self._ended = len(rows) < missing
        if not self._rows:
            # Claims/settings cannot legitimately introduce older READY rows in
            # this runner. Never silently complete after an unexpected mutation.
            if task.candidates(limit=1):
                raise TaskError("TASK_IDENTITY_INVALID")
            return False
        return True

    def prepare(self, row):
        seq = row["seq"]
        value = self._prepared.get(seq)
        if value is None:
            # Selection and warm scans both prepare the unknown suffix in order.
            if not self._unknown or self._unknown[0] != seq:
                raise TaskError("TASK_IDENTITY_INVALID")
            value = self._prepare(row)
            self._unknown.popleft()
            self._prepared[seq] = value
            self._groups.setdefault(value.transport_identity, OrderedDict())[seq] = None
        return value

    def select(self, active, limit):
        """Earliest eligible head, including any earlier not-yet-prepared row."""
        while True:
            first = min((next(iter(rows)) for identity, rows in self._groups.items()
                         if active[identity] < limit), default=None)
            if self._unknown and (first is None or self._unknown[0] < first):
                self.prepare(self._rows[self._unknown[0]])
                continue
            if first is None:
                return None
            return self._rows[first], self._prepared[first]

    def later(self, seq):
        for index, row in self._rows.items():
            if index > seq:
                yield row

    def remove(self, row):
        seq = row["seq"]
        prepared = self._prepared.pop(seq)
        del self._rows[seq]
        identity = prepared.transport_identity
        group = self._groups[identity]
        del group[seq]
        if not group:
            del self._groups[identity]
