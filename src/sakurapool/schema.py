"""Arrow schema contracts and conversion helpers."""

from collections.abc import Iterable, Mapping
from typing import Any

import pyarrow as pa

DEFAULT_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("value", pa.float64(), nullable=False),
        pa.field("label", pa.string(), nullable=True),
    ]
)


def arrow_schema(
    fields: Mapping[str, pa.DataType] | Iterable[tuple[str, pa.DataType]],
) -> pa.Schema:
    """Create a nullable Arrow schema from a mapping or ordered field pairs."""
    pairs = fields.items() if isinstance(fields, Mapping) else fields
    return pa.schema([pa.field(name, dtype) for name, dtype in pairs])


def rows_to_table(
    rows: Iterable[Mapping[str, Any]], schema: pa.Schema = DEFAULT_SCHEMA
) -> pa.Table:
    """Convert row mappings to an Arrow table and reject missing/unknown columns."""
    materialized = list(rows)
    names = set(schema.names)
    for index, row in enumerate(materialized):
        missing = [
            name for name in schema.names if name not in row and not schema.field(name).nullable
        ]
        unknown = sorted(set(row) - names)
        if missing or unknown:
            details = []
            if missing:
                details.append(f"missing required columns {missing}")
            if unknown:
                details.append(f"unknown columns {unknown}")
            raise ValueError(f"row {index}: {', '.join(details)}")
    columns = {name: [row.get(name) for row in materialized] for name in schema.names}
    try:
        return pa.table(columns, schema=schema)
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError) as exc:
        raise ValueError(f"rows do not match schema: {exc}") from exc
