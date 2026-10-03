"""Freeze selection preserves actual P3 known-namespace and any_of semantics."""

import pytest
from synthetic_p2 import ObjectSpec, SampleSpec
from test_runtime_remediation import _compile_synthetic

from sakurapool.runtime import RuntimeQuerySpec, RuntimeSnapshot
from sakurapool.tasks.plan import Selection, selected_records


@pytest.mark.parametrize("query", [
    RuntimeQuerySpec(namespace="tags", none_tags=("a",)),
    RuntimeQuerySpec(namespace="other", none_tags=("b",)),
    RuntimeQuerySpec(all_tags=(("tags", "a"),)),
    RuntimeQuerySpec(any_of=(RuntimeQuerySpec(namespace="tags", all_tags=("a",)),
                            RuntimeQuerySpec(namespace="other", none_tags=("b",)))),
])
def test_typed_query_domain_is_not_replaced(tmp_path, query):
    _, root = _compile_synthetic(tmp_path, "task-semantics", [
        ObjectSpec("tags.tar", [SampleSpec("a.png", "same", [("a", None)]),
                                SampleSpec("empty.png", "same", []),
                                SampleSpec("unknown.png", "same", [], tags_state="unknown")],
                   namespace="tags", size=100000),
        ObjectSpec("other.tar", [SampleSpec("b.png", "same", [("b", None)]),
                                 SampleSpec("empty.png", "same", [])],
                   namespace="other", size=100000),
    ])
    with RuntimeSnapshot.open(root, full_verify=True) as runtime:
        expected = list(runtime.query(query).iter_rids())
        actual = [row.rid for row in selected_records(runtime, query, Selection())]
        assert actual == expected
        assert len(actual) < 5
