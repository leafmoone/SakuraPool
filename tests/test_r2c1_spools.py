"""Production opt-in spool bounds leave the local defaults unchanged."""

import json
import sqlite3

import pytest

from sakurapool import indexer
from sakurapool.storage.production_resources import DURABLE_SPOOL_LIMITS


def test_byte_batches_include_bounded_large_singletons_and_unicode(tmp_path):
    scope = indexer._SpoolScope(limits=DURABLE_SPOOL_LIMITS)
    rows = indexer._SpoolRows(scope, tmp_path)
    try:
        for size in (100_000, 100_000, 300_000, 10_000):
            rows.append("annotations", {"value": "x" * size + "\U0001f338"})
        batches = list(rows.iter_batches("annotations"))
        assert [len(batch) for batch in batches] == [2, 1, 1]
        for batch in batches:
            encoded = sum(len(json.dumps(row, ensure_ascii=False).encode()) for row in batch)
            assert encoded <= DURABLE_SPOOL_LIMITS["batch_bytes"] or (
                len(batch) == 1 and encoded <= DURABLE_SPOOL_LIMITS["row_bytes"]
            )
        with pytest.raises(ValueError, match="row byte cap"):
            rows.append("annotations", {"value": "x" * (512 << 10)})
        assert rows.counts["annotations"] == 4
    finally:
        rows.close()


def test_spool_fixed_queries_have_no_temp_sort_and_caps_are_each_db(tmp_path):
    scope = indexer._SpoolScope(limits=DURABLE_SPOOL_LIMITS)
    rows = indexer._SpoolRows(scope, tmp_path)
    members = indexer._MemberSpool(scope, tmp_path)
    try:
        queries = [
            (members.db, "SELECT DISTINCT key FROM members ORDER BY key", ()),
            (
                members.db,
                "SELECT name,suffix,offset,size,is_image FROM members "
                "WHERE key=? AND is_image=? ORDER BY name LIMIT 2",
                ("x", 1),
            ),
            (rows.db, "SELECT payload FROM rows WHERE kind=? ORDER BY ordinal", ("samples",)),
            (
                rows.db,
                "SELECT a.record_id FROM rows AS a LEFT JOIN rows AS s ON "
                "s.kind='samples' AND s.record_id=a.record_id WHERE "
                "a.kind='annotations' AND s.record_id IS NULL LIMIT 1",
                (),
            ),
        ]
        for db, query, args in queries:
            plan = db.execute("EXPLAIN QUERY PLAN " + query, args).fetchall()
            assert not any("TEMP B-TREE" in step[-1] for step in plan)
        for db in (rows.db, members.db):
            assert db.execute("PRAGMA page_size").fetchone()[0] == 4096
            assert (
                db.execute("PRAGMA max_page_count").fetchone()[0]
                == DURABLE_SPOOL_LIMITS["spool_pages"]
            )
            assert db.execute("PRAGMA cache_size").fetchone()[0] == -2048
            assert db.execute("PRAGMA mmap_size").fetchone()[0] == 0
            assert db.execute("PRAGMA journal_mode").fetchone()[0] == "off"
        assert not list(tmp_path.glob("*-journal")) and not list(tmp_path.glob("*-wal"))
    finally:
        rows.close()
        members.close()


def test_sqlite_page_limit_fails_closed_before_more_growth(tmp_path):
    scope = indexer._SpoolScope(limits={**DURABLE_SPOOL_LIMITS, "spool_pages": 16})
    rows = indexer._SpoolRows(scope, tmp_path)
    try:
        with pytest.raises(sqlite3.DatabaseError, match="full"):
            for _ in range(100):
                rows.append("annotations", {"value": "x" * 4096})
        assert rows.path.stat().st_size <= 16 * 4096
        assert not list(tmp_path.glob("*.COMMIT"))
    finally:
        rows.close()


def test_production_fragment_group_cap_retains_partial_without_commit(tmp_path):
    scope = indexer._SpoolScope(limits={**DURABLE_SPOOL_LIMITS, "row_groups": 1})
    rows = indexer._SpoolRows(scope, tmp_path)
    try:
        for _ in range(2):
            rows.append("annotations", {"value": "x" * 300_000})
        files = {name: tmp_path / f"output.{name}.parquet" for name in indexer.SCHEMAS}
        with pytest.raises(ValueError, match="row-group cap"):
            indexer._write_fragments(files, rows, lambda _: None, byte_limit=192 << 20)
        assert not list(tmp_path.glob("*.COMMIT"))
        assert (tmp_path / "output.annotations.parquet.partial").exists()
    finally:
        rows.close()


def test_disposable_spool_process_death_never_creates_commit_or_journal(tmp_path):
    import subprocess
    import sys

    code = """
import os,sys
from pathlib import Path
from sakurapool import indexer
from sakurapool.storage.production_resources import DURABLE_SPOOL_LIMITS
rows=indexer._SpoolRows(indexer._SpoolScope(limits=DURABLE_SPOOL_LIMITS),Path(sys.argv[1]))
for _ in range(500): rows.append("annotations",{"value":"x"*4096})
os._exit(7)
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(tmp_path)], capture_output=True, timeout=30
    )
    assert result.returncode == 7
    assert list(tmp_path.glob("*.sqlite"))
    assert not list(tmp_path.glob("*.COMMIT"))
    assert not list(tmp_path.glob("*-journal")) and not list(tmp_path.glob("*-wal"))


def test_stage_database_fsync_failure_prevents_marker(tmp_path, monkeypatch):
    from sakurapool.registry import DatasetAdapter
    from sakurapool.storage import rust_index
    from sakurapool.storage.production_resources import STAGE_PAGES
    from sakurapool.storage.transport import BoundObject

    def refused(_handle):
        raise OSError("synthetic fsync refusal")

    monkeypatch.setattr(rust_index.os, "fsync", refused)
    revision = "a" * 40
    bound = BoundObject(
        f"http://127.0.0.1/object?Revision={revision}", 10240, revision, '"bounded"'
    )
    stage = tmp_path / "stage"
    with pytest.raises(OSError, match="fsync refusal"):
        rust_index._write_stage(
            {"whole_sha256": "b" * 64},
            lambda _db: iter(()),
            bound,
            DatasetAdapter("test", "synthetic"),
            stage,
            None,
            _max_pages=STAGE_PAGES,
        )
    assert (stage / "members.sqlite").exists()
    assert not (stage / "stage.complete").exists()
