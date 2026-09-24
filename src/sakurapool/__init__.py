"""SakuraPool P1: typed contracts and a small-data reference evaluator."""

from .evaluator import ReferenceEvaluator
from .models import ModelRegistry, ModelSpec
from .query import FilterSpec, QuerySpec
from .registry import Registry
from .schema import DEFAULT_SCHEMA, arrow_schema, rows_to_table

__all__ = [
    "DEFAULT_SCHEMA",
    "FilterSpec",
    "ModelRegistry",
    "ModelSpec",
    "QuerySpec",
    "ReferenceEvaluator",
    "Registry",
    "arrow_schema",
    "rows_to_table",
]

__version__ = "0.1.0"
