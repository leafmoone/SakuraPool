"""SakuraPool: typed contracts, local TAR indexing, and reference evaluation."""

from .evaluator import ReferenceEvaluator
from .indexer import (
    ANNOTATIONS_SCHEMA,
    ERRORS_SCHEMA,
    OBJECTS_SCHEMA,
    SAMPLES_SCHEMA,
    UnsupportedArchiveError,
    discover_archives,
    scan,
)
from .models import ModelRegistry, ModelSpec
from .query import FilterSpec, QuerySpec
from .registry import AdapterRegistry, DatasetAdapter, Registry
from .schema import DEFAULT_SCHEMA, arrow_schema, rows_to_table

__all__ = [
    "ANNOTATIONS_SCHEMA",
    "AdapterRegistry",
    "DEFAULT_SCHEMA",
    "DatasetAdapter",
    "ERRORS_SCHEMA",
    "FilterSpec",
    "ModelRegistry",
    "ModelSpec",
    "OBJECTS_SCHEMA",
    "QuerySpec",
    "ReferenceEvaluator",
    "Registry",
    "SAMPLES_SCHEMA",
    "UnsupportedArchiveError",
    "arrow_schema",
    "discover_archives",
    "rows_to_table",
    "scan",
]

__version__ = "0.1.0"
