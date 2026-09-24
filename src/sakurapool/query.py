"""Validated, serializable query specifications."""

from dataclasses import dataclass
from typing import Any, Literal

import pyarrow as pa

_ALLOWED_OPERATORS = frozenset({"eq", "ne", "gt", "gte", "lt", "lte", "in"})


@dataclass(frozen=True)
class FilterSpec:
    column: str
    operator: str
    value: Any

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "FilterSpec":
        try:
            return cls(value["column"], value["operator"], value["value"])
        except KeyError as exc:
            raise ValueError(f"filter missing key: {exc.args[0]}") from exc

    def validate(self, schema: pa.Schema) -> None:
        if self.column not in schema.names:
            raise ValueError(f"unknown filter column: {self.column}")
        if self.operator not in _ALLOWED_OPERATORS:
            raise ValueError(f"unsupported filter operator: {self.operator}")
        if self.operator == "in" and (not isinstance(self.value, list) or not self.value):
            raise ValueError("the 'in' filter value must be a non-empty list")


@dataclass(frozen=True)
class QuerySpec:
    dataset: str
    operation: Literal["filter", "select", "count"] = "filter"
    select: tuple[str, ...] = ()
    filters: tuple[FilterSpec, ...] = ()
    limit: int | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "QuerySpec":
        if not isinstance(value, dict):
            raise ValueError("query must be an object")
        try:
            filters = tuple(FilterSpec.from_dict(item) for item in value.get("filters", []))
            select = tuple(value.get("select", []))
            return cls(
                value["dataset"],
                value.get("operation", "filter"),
                select,
                filters,
                value.get("limit"),
            )
        except KeyError as exc:
            raise ValueError(f"query missing key: {exc.args[0]}") from exc

    def validate(self, schema: pa.Schema) -> None:
        if not self.dataset or not isinstance(self.dataset, str):
            raise ValueError("dataset must be a non-empty string")
        if self.operation not in {"filter", "select", "count"}:
            raise ValueError(f"unsupported query operation: {self.operation}")
        if len(set(self.select)) != len(self.select):
            raise ValueError("select columns must be unique")
        unknown = sorted(set(self.select) - set(schema.names))
        if unknown:
            raise ValueError(f"unknown select columns: {unknown}")
        for item in self.filters:
            item.validate(schema)
        if self.limit is not None and (not isinstance(self.limit, int) or self.limit < 0):
            raise ValueError("limit must be a non-negative integer")
        if self.operation == "count" and self.select:
            raise ValueError("count queries cannot select columns")

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "operation": self.operation,
            "select": list(self.select),
            "filters": [
                {"column": f.column, "operator": f.operator, "value": f.value} for f in self.filters
            ],
            "limit": self.limit,
        }
