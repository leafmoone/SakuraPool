"""P3 runtime snapshot and bitmap query engine."""

from .errors import (
    AmbiguousRecordError,
    CorruptInputError,
    RuntimeErrorBase,
    SnapshotClosedError,
    SnapshotCorruptError,
    SnapshotMixError,
    UnknownQueryValueError,
)
from .inventory import P2Inventory, combine_inventories, load_p2_inventory

RUNTIME_FORMAT_VERSION = 2
RUNTIME_COMPILER = "sakurapool-p3-v2"

__all__ = [
    "AmbiguousRecordError",
    "ByteLRU",
    "CompiledSnapshot",
    "CorruptInputError",
    "LocationBatch",
    "P2Inventory",
    "QueryResult",
    "combine_inventories",
    "RecordBatch",
    "ResolvedRecord",
    "RUNTIME_COMPILER",
    "RUNTIME_FORMAT_VERSION",
    "RuntimeQuerySpec",
    "RuntimeSnapshot",
    "RuntimeErrorBase",
    "SnapshotClosedError",
    "SnapshotCorruptError",
    "SnapshotMixError",
    "UnknownQueryValueError",
    "compile_runtime",
    "load_p2_inventory",
]


def __getattr__(name: str):  # lazy heavy imports (numpy/pyroaring)
    if name in ("ByteLRU", "CompiledSnapshot", "compile_runtime",
                "LocationBatch", "QueryResult", "RecordBatch",
                "ResolvedRecord", "RuntimeQuerySpec", "RuntimeSnapshot"):
        if name == "compile_runtime" or name == "CompiledSnapshot":
            from . import compiler
            return getattr(compiler, name)
        if name in ("ByteLRU", "ResolvedRecord", "RuntimeSnapshot"):
            from . import snapshot
            return getattr(snapshot, name)
        from . import query
        return getattr(query, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
