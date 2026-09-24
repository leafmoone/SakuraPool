import pyarrow as pa
import pytest

from sakurapool.schema import DEFAULT_SCHEMA, rows_to_table


def test_rows_to_table_preserves_schema_and_values():
    table = rows_to_table([{"id": "a", "value": 1, "label": "x"}], DEFAULT_SCHEMA)
    assert table.schema == DEFAULT_SCHEMA
    assert table.to_pylist() == [{"id": "a", "value": 1.0, "label": "x"}]


def test_rows_to_table_rejects_unknown_and_missing_fields():
    with pytest.raises(ValueError, match="unknown columns"):
        rows_to_table([{"id": "a", "value": 1, "extra": True}])
    with pytest.raises(ValueError, match="missing required"):
        rows_to_table([{"id": "a"}])


def test_custom_schema():
    schema = pa.schema([("name", pa.string())])
    assert rows_to_table([{"name": "Ada"}], schema).num_rows == 1
