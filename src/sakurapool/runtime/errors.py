"""Public P3 runtime exception types."""


class RuntimeErrorBase(Exception):
    """Base class for runtime compiler/query errors."""


class CorruptInputError(RuntimeErrorBase, ValueError):
    """A P2 committed input is missing, changed, or malformed."""


class SnapshotCorruptError(RuntimeErrorBase, ValueError):
    """A runtime snapshot is incomplete or fails its integrity contract."""


class SnapshotMixError(SnapshotCorruptError):
    """Files from different runtime snapshots were combined."""


class SnapshotClosedError(RuntimeErrorBase):
    """An operation was attempted after closing a runtime snapshot/result."""


class AmbiguousRecordError(RuntimeErrorBase, LookupError):
    """A point lookup matched more than one runtime record."""


class UnknownQueryValueError(RuntimeErrorBase, ValueError):
    """A query names a source, dataset, namespace, or tag not in the snapshot."""
