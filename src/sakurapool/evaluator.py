"""Deterministic reference evaluator for small in-memory row datasets."""

from collections.abc import Iterable, Mapping
from typing import Any

import pyarrow as pa

from .query import FilterSpec, QuerySpec
from .schema import DEFAULT_SCHEMA, rows_to_table


class ReferenceEvaluator:
    """Evaluate QuerySpec against materialized rows; intended for correctness tests."""

    def __init__(self, schema: pa.Schema = DEFAULT_SCHEMA) -> None:
        self.schema = schema

    def evaluate(self, rows: Iterable[Mapping[str, Any]], query: QuerySpec) -> pa.Table:
        query.validate(self.schema)
        table = rows_to_table(rows, self.schema)
        selected = [
            row
            for row in table.to_pylist()
            if all(self._matches(row, item) for item in query.filters)
        ]
        if query.limit is not None:
            selected = selected[: query.limit]
        if query.operation == "count":
            return pa.table({"count": [len(selected)]}, schema=pa.schema([("count", pa.int64())]))
        columns = query.select or tuple(self.schema.names)
        return rows_to_table(
            ({name: row.get(name) for name in columns} for row in selected),
            pa.schema([(name, self.schema.field(name).type) for name in columns]),
        )

    @staticmethod
    def _matches(row: dict[str, Any], item: FilterSpec) -> bool:
        actual = row.get(item.column)
        expected = item.value
        if item.operator == "eq":
            return actual == expected
        if item.operator == "ne":
            return actual != expected
        if item.operator == "gt":
            return actual is not None and actual > expected
        if item.operator == "gte":
            return actual is not None and actual >= expected
        if item.operator == "lt":
            return actual is not None and actual < expected
        if item.operator == "lte":
            return actual is not None and actual <= expected
        return actual in expected
