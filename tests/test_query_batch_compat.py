"""Small actual SQLite record batches; no budget root or provider requests."""
import sqlite3
from types import SimpleNamespace

import pytest
from pyroaring import BitMap

from sakurapool.runtime.query import QueryResult


class WithoutGetlimit:
    """Execute-only wrapper, deliberately with no getlimit API."""
    def __init__(self, connection):
        self.connection = connection
        self.calls = []

    def execute(self, sql, parameters):
        self.calls.append(len(parameters))
        return self.connection.execute(sql, parameters)


@pytest.fixture
def query():
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE sources(source_id INTEGER, name TEXT);
        CREATE TABLE datasets(dataset_id INTEGER, name TEXT);
        CREATE TABLE records(rid INTEGER,record_id BLOB,source_id INTEGER,
                             dataset_id INTEGER,post_id TEXT);
        INSERT INTO sources VALUES(1,'source'); INSERT INTO datasets VALUES(2,'dataset');
    """)
    db.executemany("INSERT INTO records VALUES(?,?,1,2,?)",
                   [(i, bytes([i]) * 16, str(i)) for i in range(5)])
    snapshot = SimpleNamespace(_catalog=db, _check_open=lambda: None, snapshot_id="test")
    result = QueryResult(snapshot, BitMap(range(5)))
    yield db, snapshot, result
    db.close()


def flattened(result, size):
    return [(rid, record, source, dataset, source_name, dataset_name, post)
            for batch in result.iter_record_batches(size)
            for rid, record, source, dataset, source_name, dataset_name, post in zip(
                batch.rid, batch.record_id, batch.source_id, batch.dataset_id,
                batch.source_name, batch.dataset_name, batch.post_id)]


def test_absent_getlimit_batches_and_dynamic_fallback(query):
    db, snapshot, result = query
    expected = flattened(result, 3)
    wrapper = WithoutGetlimit(db)
    assert not hasattr(wrapper, "getlimit")
    snapshot._catalog = wrapper
    assert flattened(result, 3) == expected
    assert wrapper.calls == [3, 2]
    db.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 2)
    wrapper.calls.clear()
    assert flattened(result, 5) == expected
    assert 5 in wrapper.calls and 2 in wrapper.calls


def test_native_python310_getlimit_absent(query):
    db, _, result = query
    if hasattr(db, "getlimit"):
        pytest.skip("native getlimit-absent runtime required")
    rows = flattened(result, 3)
    assert [row[0] for row in rows] == list(range(5))
    assert [row[1] for row in rows] == [(bytes([i]) * 16).hex() for i in range(5)]
    assert [len(batch.rid) for batch in result.iter_record_batches(3)] == [3, 2]


def test_zero_variable_limit_explicit_failure(query):
    db, _, result = query
    assert len(result) >= 3
    db.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 0)
    with pytest.raises(ValueError, match="SQLite variable limit must be positive"):
        list(result.iter_record_batches(3))


@pytest.mark.parametrize("limit", [1, 2, 100])
@pytest.mark.parametrize("size", [1, 3, 8])
def test_limit_order_and_partial_batches(query, limit, size):
    db, _, result = query
    expected = flattened(result, 5)
    db.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, limit)
    assert flattened(result, size) == expected
    assert [row[0] for row in expected] == list(range(5))


@pytest.mark.parametrize("size", [True, False, 0, -1, 1.5, "3"])
def test_invalid_batch_types(query, size):
    _, _, result = query
    with pytest.raises(ValueError, match="batch_size must be positive"):
        list(result.iter_record_batches(size))


def test_empty_result_consistent(query):
    db, snapshot, _ = query
    result = QueryResult(snapshot, BitMap())
    assert list(result.iter_record_batches()) == []
    snapshot._catalog = WithoutGetlimit(db)
    assert list(result.iter_record_batches()) == []
    snapshot._catalog = db
    db.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 0)
    with pytest.raises(ValueError, match="SQLite variable limit must be positive"):
        list(result.iter_record_batches())


def test_absent_getlimit_other_operational_error_not_swallowed(query):
    db, snapshot, result = query
    snapshot._catalog = WithoutGetlimit(db)
    db.execute("DROP TABLE records")
    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        list(result.iter_record_batches())
    assert snapshot._catalog.calls == [5]


def test_absent_getlimit_size_one_failure_propagates(query):
    db, snapshot, result = query
    snapshot._catalog = WithoutGetlimit(db)
    db.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 0)
    with pytest.raises(sqlite3.OperationalError, match="too many SQL variables"):
        list(result.iter_record_batches(3))
    assert snapshot._catalog.calls[-1] == 1
