"""Task selection uses real synthetic P3 results, without network."""

import tempfile
import time
from pathlib import Path

import pytest
from synthetic_p2 import ObjectSpec, SampleSpec
from test_runtime_remediation import _compile_synthetic

from sakurapool.runtime import RuntimeQuerySpec, RuntimeSnapshot
from sakurapool.tasks.plan import Selection, normalize_query, selected_records, selection_digest


@pytest.fixture
def runtime(tmp_path):
    _, root = _compile_synthetic(tmp_path, "task-plan", [
        ObjectSpec("a.tar", [SampleSpec(f"{i}.png", str(i), [("a", None)])
                             for i in range(12)], size=100000),
        ObjectSpec("b.tar", [SampleSpec(f"b{i}.png", str(i), [("b", None)])
                             for i in range(12)], size=100000),
    ])
    with RuntimeSnapshot.open(root, full_verify=True) as snapshot:
        yield snapshot


def test_all_first_empty_and_explicit(runtime):
    query = RuntimeQuerySpec()
    all_rows = list(selected_records(runtime, query, Selection()))
    assert len(all_rows) == 24
    assert [row.rid for row in all_rows] == list(range(24))
    first = list(selected_records(runtime, query, Selection("first", 7)))
    assert first == all_rows[:7]
    assert list(selected_records(runtime, query, Selection("first", 0))) == []
    records = tuple(row.record_id for row in reversed(first))
    explicit = list(selected_records(runtime, query, Selection("records", records=records)))
    assert [row.record_id for row in explicit] == list(records)
    assert selection_digest((row.rid, row.record_id) for row in first)[0] == 7


def test_sample_reproducible_bounded_and_seeded(runtime):
    query = RuntimeQuerySpec()
    first = list(selected_records(runtime, query, Selection("sample", 8, "seed")))
    second = list(selected_records(runtime, query, Selection("sample", 8, "seed")))
    other = list(selected_records(runtime, query, Selection("sample", 8, "other")))
    assert first == second and len(first) == 8 and first != other
    assert len({row.record_id for row in first}) == 8
    assert list(selected_records(runtime, query, Selection("sample", 0, "seed"))) == []


def test_normalization_keeps_namespace_none_tags_anyof():
    query = RuntimeQuerySpec(namespace="tags", none_tags=("a", ("other", "b")))
    normalized = normalize_query(query)
    assert normalized["namespace"] == "tags"
    assert normalized["none_tags"] == ["a", ["other", "b"]]
    union = RuntimeQuerySpec(any_of=(RuntimeQuerySpec(sources=("b", "a", "a")), query))
    reverse = RuntimeQuerySpec(any_of=tuple(reversed(union.any_of)))
    assert normalize_query(union) == normalize_query(reverse)


@pytest.mark.parametrize("selection", [
    Selection("sample", 1), Selection("sample", 10001, "seed"),
    Selection("first", -1), Selection("first", True), Selection("all", seed="seed"),
    Selection("records", records=("bad",)), Selection("sample", 1, "seed", algorithm="unknown"),
])
def test_invalid_selection_rejected(selection):
    with pytest.raises(ValueError):
        selection.validate()


def test_10k_multisource_freeze_sql_cost_and_reopen(tmp_path, record_property):
    from synthetic_p2 import build_p2_directory

    from sakurapool.runtime import combine_inventories, compile_runtime, load_p2_inventory
    from sakurapool.storage.budget import DEFAULT_WORK_ROOT, BudgetLedger
    from sakurapool.tasks.store import TaskDB

    roots = []
    for index in range(2):
        root = tmp_path / f"p2-{index}"
        build_p2_directory(root, dataset=f"dataset-{index}", source=f"source-{index}", objects=[
            ObjectSpec(f"{index}.tar", [SampleSpec(f"{i}.png", str(i), [("tag", None)])
                                      for i in range(5000)], size=10000000),
        ])
        roots.append(load_p2_inventory(root))
    runtime_root = tmp_path / "runtime"
    compile_runtime(combine_inventories(roots), runtime_root)
    with RuntimeSnapshot.open(runtime_root, full_verify=True) as runtime:
        queries = []
        runtime._catalog.set_trace_callback(queries.append)
        with tempfile.TemporaryDirectory(dir=DEFAULT_WORK_ROOT, prefix="offline-task-10k-") as raw:
            ledger = BudgetLedger(raw, _offline_test=True)
            started = time.perf_counter()
            with TaskDB.create(Path(raw) / "task", ledger,
                               {"publication_digest": "a" * 64,
                                "snapshot_id": runtime.snapshot_id, "query": normalize_query(
                                    RuntimeQuerySpec())},
                               selected_records(runtime, RuntimeQuerySpec(), Selection()),
                               publication_path=runtime_root) as task:
                elapsed = time.perf_counter() - started
                assert task.validate_plan()["selection_count"] == 10000
                for field, expected in (("source", 2), ("record_id", 10000), ("post_id", 5000)):
                    count = task.db.execute(
                        f"SELECT count(DISTINCT {field}) FROM items"
                    ).fetchone()[0]
                    assert count == expected
                identity = task.meta("plan_digest")
            assert sum("WHERE r.rid IN" in sql for sql in queries) == 20
            queries.clear()
            started = time.perf_counter()
            with TaskDB(Path(raw) / "task", readonly=True) as reopened:
                assert reopened.validate_plan()["selection_count"] == 10000
                assert reopened.meta("plan_digest") == identity
            assert queries == []
            reopen_elapsed = time.perf_counter() - started
            record_property("synthetic_plan_create_seconds", elapsed)
            record_property("synthetic_plan_reopen_seconds", reopen_elapsed)
            record_property("record_batch_sql_queries", 20)
            print("SYNTHETIC_10K_SELECTION_COST", {"create_seconds": elapsed,
                  "reopen_seconds": reopen_elapsed, "record_batch_sql_queries": 20,
                  "reopen_runtime_sql_queries": 0, "selection_count": 10000})
