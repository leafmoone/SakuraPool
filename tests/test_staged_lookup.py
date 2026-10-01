"""JSON seek complexity is bounded by SQLite VM work, not wall-clock time."""

import hashlib
import sqlite3

import pytest
from test_p4_transport import ETAG, TAR
from test_p4_transport import http_and_budget as _http_and_budget

from sakurapool.registry import DatasetAdapter
from sakurapool.storage.remote_index import StagedArchive, stage_tar
from sakurapool.storage.rust_index import _write_stage
from sakurapool.storage.transport import BoundObject, RemoteIOError


@pytest.fixture
def http_and_budget():
    yield from _http_and_budget.__wrapped__()


def _assert_indexed_seek(stage):
    with StagedArchive(stage) as archive:
        plan = archive.db.execute(
            "EXPLAIN QUERY PLAN SELECT json_payload FROM members "
            "WHERE offset_data=? AND kind='json'", (1536,)
        ).fetchall()
        detail = " ".join(row[3] for row in plan)
        assert "SEARCH members USING INDEX members_json_offset" in detail
        assert "SCAN members" not in detail


def test_stream_stage_json_offset_index(http_and_budget):
    base, ledger, client = http_and_budget
    bound = BoundObject(base + "/full", len(TAR), strong_etag=ETAG)
    stage = stage_tar(client, ledger, bound, ledger.root / "stage",
                      DatasetAdapter("synthetic", "synthetic"))
    _assert_indexed_seek(stage)


@pytest.mark.parametrize("count", [64, 8192])
def test_rust_stage_seek_bounded_vm_work_and_contract(tmp_path, count):
    payload = b'{"tags":["a"]}'
    digest = hashlib.sha256(payload).hexdigest()
    rows = []
    for i in range(count):
        rows.extend([(f"{i}.jpg", "image", i * 2048 + 512, 4, "a" * 64, None),
                     (f"{i}.json", "json", i * 2048 + 1536,
                      len(payload), digest, payload)])
    size = count * 2048
    bound = BoundObject("http://127.0.0.1/x.tar", size, strong_etag=ETAG)
    stage = _write_stage({"whole_sha256": "b" * 64}, rows, bound,
                         DatasetAdapter("synthetic", "synthetic"),
                         tmp_path / "stage", None)
    _assert_indexed_seek(stage)
    with StagedArchive(stage) as archive:
        calls = 0

        def progress():
            nonlocal calls
            calls += 1
            return 0

        archive.db.set_progress_handler(progress, 1)
        for i in range(count):
            archive.seek(i * 2048 + 1536)
            assert archive.read(5) == payload[:5]
            assert archive.read(100) == payload[5:]
            assert archive.read(1) == b""
        # A full-table scan needs thousands of VM steps per seek at 8192;
        # an indexed lookup uses a small fixed number, independent of size.
        assert calls < count * 64
        archive.db.set_progress_handler(None, 0)
        archive.seek(1536)
        assert archive.read(100) == payload  # reseek resets the read cursor
        assert archive.member_sha256("0.jpg") == "a" * 64
        for offset in (512, -1, size + 1536):
            with pytest.raises(RemoteIOError, match="no audited staged JSON"):
                archive.seek(offset)
        members = []
        while (member := archive.next()) is not None:
            members.append((member.name, member.offset_data, member.size))
        assert members == [(row[0], row[2], row[3]) for row in rows]
    # Completed stages are still read-only and their marker hashes the index.
    with sqlite3.connect(f"file:{stage.database.as_posix()}?mode=ro", uri=True) as db:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            db.execute("DELETE FROM members")
