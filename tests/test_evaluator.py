from sakurapool.evaluator import ReferenceEvaluator
from sakurapool.query import QuerySpec

ROWS = [
    {"id": "a", "value": 1, "label": "low"},
    {"id": "b", "value": 3, "label": "high"},
    {"id": "c", "value": 4, "label": "high"},
]


def test_filter_select_and_limit_are_deterministic():
    query = QuerySpec.from_dict(
        {
            "dataset": "items",
            "operation": "select",
            "select": ["id"],
            "filters": [{"column": "value", "operator": "gte", "value": 3}],
            "limit": 1,
        }
    )
    assert ReferenceEvaluator().evaluate(ROWS, query).to_pylist() == [{"id": "b"}]


def test_count():
    query = QuerySpec.from_dict(
        {
            "dataset": "items",
            "operation": "count",
            "filters": [{"column": "label", "operator": "eq", "value": "high"}],
        }
    )
    assert ReferenceEvaluator().evaluate(ROWS, query).to_pylist() == [{"count": 2}]
