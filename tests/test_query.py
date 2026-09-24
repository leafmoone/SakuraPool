import pytest

from sakurapool.query import FilterSpec, QuerySpec
from sakurapool.schema import DEFAULT_SCHEMA


def test_query_round_trip():
    query = QuerySpec("items", "select", ("id",), (FilterSpec("value", "gte", 2),), 4)
    assert QuerySpec.from_dict(query.to_dict()) == query
    query.validate(DEFAULT_SCHEMA)


@pytest.mark.parametrize(
    "bad",
    [
        {"dataset": "x", "filters": [{"column": "nope", "operator": "eq", "value": 1}]},
        {"dataset": "x", "filters": [{"column": "value", "operator": "bogus", "value": 1}]},
        {"dataset": "x", "operation": "count", "select": ["id"]},
    ],
)
def test_query_validation_rejects_invalid_specs(bad):
    with pytest.raises(ValueError):
        QuerySpec.from_dict(bad).validate(DEFAULT_SCHEMA)


def test_in_filter_requires_nonempty_list():
    query = QuerySpec.from_dict(
        {"dataset": "x", "filters": [{"column": "id", "operator": "in", "value": []}]}
    )
    with pytest.raises(ValueError, match="non-empty"):
        query.validate(DEFAULT_SCHEMA)
