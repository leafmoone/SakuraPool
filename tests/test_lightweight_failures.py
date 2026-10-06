"""Regression coverage for review findings; local synthetic data only."""

import json
import sqlite3
import threading

import pytest
from test_lightweight_tasks import lightweight as lightweight_fixture
from test_lightweight_transport import loopback as loopback_fixture
from test_lightweight_transport import make_transport

from sakurapool.storage.transport import RemoteIOError
from sakurapool.tasks.plan import SelectedRecord, Selection
from sakurapool.tasks.runner import _recover_stage, create_task, run_task, verify_delivery
from sakurapool.tasks.store import TaskDB, TaskError

lightweight = lightweight_fixture
loopback = loopback_fixture


@pytest.mark.parametrize("phase", ["PREPARED", "PUBLISHED"])
def test_published_directory_moved_to_stage_never_deleted(lightweight, phase):
    env = lightweight
    with create_task(
        env.publication, env.directory, env.workspace, env.query, Selection("first", 1)
    ):
        pass
    run_task(env.directory, env.transport(), control=object())
    with TaskDB(env.directory) as task:
        row = dict(task.db.execute("SELECT * FROM items").fetchone())
        stage = json.loads(row["stage"])
        final = env.directory / "output" / (row["output_stem"] + ".jpg")
        moved = env.directory / "output" / stage["name"]
        moved.mkdir()
        final.rename(moved / "image.jpg")
        delivery = "NONE" if phase == "PREPARED" else "PUBLISHED"
        row.update(state="IN_PROGRESS", phase=phase, delivery=delivery)
        task.db.execute(
            "UPDATE items SET state='IN_PROGRESS',phase=?,delivery=? WHERE seq=0", (phase, delivery)
        )
        assert _recover_stage(task, row) is False
    with pytest.raises(TaskError, match="OUTPUT_UNCERTAIN"):
        run_task(env.directory, env.transport(), control=object(), resume=True)
    assert (moved / "image.jpg").is_file()


@pytest.mark.parametrize("malformed", [[], {"receipt": []}, {"receipt": {"image.jpg": []}}])
def test_malformed_receipt_classified_blocked(lightweight, malformed):
    env = lightweight
    with create_task(
        env.publication, env.directory, env.workspace, env.query, Selection("first", 1)
    ):
        pass
    run_task(env.directory, env.transport(), control=object())
    with TaskDB(env.directory) as task:
        row = dict(task.db.execute("SELECT * FROM items").fetchone())
        row["receipt"] = json.dumps(malformed)
        with pytest.raises(TaskError, match="OUTPUT_CONFLICT"):
            verify_delivery(task, row)
        task.db.execute("UPDATE items SET receipt=? WHERE seq=0", (row["receipt"],))
    with pytest.raises(TaskError, match="OUTPUT_CONFLICT"):
        run_task(env.directory, env.transport(), control=object(), resume=True)
    with TaskDB(env.directory, readonly=True) as task:
        assert task.db.execute("SELECT state FROM items").fetchone()[0] == "BLOCKED"


def test_request_read_failure_rejects_waiting_rpc_not_deadlock(lightweight, monkeypatch):
    env = lightweight
    with create_task(
        env.publication, env.directory, env.workspace, env.query, Selection("first", 4)
    ):
        pass
    original = TaskDB.meta
    armed = threading.Event()
    fired = []
    claim = TaskDB.claim

    def tracked_claim(self, **kwargs):
        result = claim(self, **kwargs)
        if result is not None:
            armed.set()
        return result

    def broken_meta(self, key):
        if key == "request" and armed.is_set() and not fired:
            fired.append(True)
            raise RuntimeError("synthetic coordinator SQL failure")
        return original(self, key)

    monkeypatch.setattr(TaskDB, "claim", tracked_claim)
    monkeypatch.setattr(TaskDB, "meta", broken_meta)
    with pytest.raises(RuntimeError, match="coordinator SQL"):
        run_task(env.directory, env.transport(), workers=4, control=object())
    assert fired


def test_worker_replacement_temp_identity_preserved(tmp_path, loopback, monkeypatch):
    transport, candidate = make_transport(tmp_path, loopback, monkeypatch)
    moved = []

    def changed_call(obj, root, **kwargs):
        original = root.with_name(root.name + "-moved")
        root.rename(original)
        root.mkdir()
        (root / "body").write_bytes(b"foreign")
        moved.append((root, original))
        raise RemoteIOError(
            "fixed primary", code="origin_timeout", phase="origin", lightweight=True
        )

    monkeypatch.setattr(transport, "_call", changed_call)
    with transport, pytest.raises(RemoteIOError) as caught:
        with transport.transfer(candidate, condition="observe"):
            pass
    assert caught.value.code == "origin_timeout"
    assert "production_finalization" in caught.value.finalization_secondary
    assert (moved[0][0] / "body").read_bytes() == b"foreign"


@pytest.mark.parametrize("with_primary", [False, True])
def test_all_lanes_closed_after_first_close_failure(lightweight, monkeypatch, with_primary):
    env = lightweight
    with create_task(env.publication, env.directory, env.workspace, env.query):
        pass
    closed = []
    count = []
    primary = RuntimeError("synthetic original failure")

    class Lane(env.transport):
        def __init__(self, index):
            self.index = index

        def close(self):
            closed.append(self.index)
            if self.index == 0 or with_primary:
                raise RuntimeError("synthetic private close message")

    def clone(self):
        index = len(count)
        count.append(index)
        return Lane(index)

    def hook(event, item):
        if with_primary and event == "CLAIMED":
            raise primary

    monkeypatch.setattr(env.transport, "clone", clone)
    expected = RuntimeError if with_primary else TaskError
    with pytest.raises(expected) as caught:
        run_task(env.directory, env.transport(), workers=2, control=object(), fault_hook=hook)
    # Lanes are lazy: an immediate first-claim fault owns only lane 0.
    assert closed == ([0] if with_primary else [0, 1])
    if with_primary:
        assert caught.value is primary
        assert caught.value.task_secondary == ("LANE_CLOSE_FAILED",)
    else:
        assert caught.value.code == "LANE_CLOSE_FAILED"
        assert caught.value.__cause__ is None
        assert "private" not in json.dumps(caught.value.public_diagnostic())


def test_reconcile_keyset_pages_close_before_updates(tmp_path, monkeypatch):
    from sakurapool.tasks import runner

    page_size = runner.RECONCILE_PAGE_ROWS
    count = 2 * page_size + 5
    rows = [SelectedRecord(i, f"{i:032x}", "synthetic", "small", str(i)) for i in range(count)]
    with TaskDB.create(
        tmp_path / "pages", None, {}, rows, publication_path=tmp_path / "unused"
    ) as task:
        for seq in range(count):
            state = ("IN_PROGRESS", "FAILED", "DONE")[seq % 3]
            details = {
                "cause_code": "origin_timeout",
                "cleanup": "SAFE",
                "delivery": "NOT_PUBLISHED",
            }
            task.db.execute(
                "UPDATE items SET state=?,phase='CLAIMED',operation_id=?,diagnostic=?,receipt=? "
                "WHERE seq=?",
                (
                    state,
                    f"{seq + 1:032x}",
                    json.dumps(details),
                    json.dumps({"receipt": "x" * 6000}) if state == "DONE" else None,
                    seq,
                ),
            )
            if state == "DONE":
                (task.directory / "output" / f"{seq:032x}").mkdir()
        original = task.db
        open_pages = []
        sizes, boundaries, visited = [], [], []

        class Page:
            def __init__(self, cursor):
                self.cursor = cursor
                open_pages.append(self)

            def fetchmany(self, size):
                assert size == page_size
                result = self.cursor.fetchmany(size)
                sizes.append(len(result))
                return result

            def fetchall(self):
                raise AssertionError("unbounded reconcile materialization")

            def close(self):
                self.cursor.close()
                open_pages.remove(self)

        class Connection:
            def execute(self, sql, params=()):
                if sql.startswith("SELECT * FROM items WHERE seq>?"):
                    assert sql.endswith("ORDER BY seq LIMIT ?")
                    assert params[1] == page_size
                    boundaries.append(params[0])
                    return Page(original.execute(sql, params))
                if sql.startswith("UPDATE"):
                    assert not open_pages, "reconcile mutated an active page cursor"
                return original.execute(sql, params)

            def __getattr__(self, name):
                return getattr(original, name)

        finish = task.finish_item

        def tracked_finish(seq, **kwargs):
            assert not open_pages
            visited.append(seq)
            return finish(seq, **kwargs)

        monkeypatch.setattr(task, "finish_item", tracked_finish)
        from sakurapool.storage.flat_delivery import DeliveryMapping

        monkeypatch.setattr(runner, "verify_delivery", lambda *_args: {})
        monkeypatch.setattr(runner, "delivery_mapping", lambda _task, row, _pub:
                            DeliveryMapping(row["output_stem"], ".jpg"))
        monkeypatch.setattr(runner, "_flat_receipt", lambda _task, row, _pub:
                            ({}, DeliveryMapping(row["output_stem"], ".jpg")))
        original.execute("UPDATE items SET phase='PUBLISHED' WHERE state='DONE'")
        task.db = Connection()
        try:
            runner.reconcile(task, object())
        finally:
            task.db = original
        assert sizes == [page_size, page_size, 5, 0]
        assert boundaries == [-1, page_size - 1, 2 * page_size - 1, count - 1]
        assert visited == list(range(count))
        assert not open_pages
        for row in task.db.execute("SELECT seq,state,recovery_retries FROM items ORDER BY seq"):
            assert row["state"] == ("DONE" if row["seq"] % 3 == 2 else "READY")
            assert row["recovery_retries"] == (0 if row["seq"] % 3 == 2 else 1)


def test_failed_state_and_diagnostic_sql_failure_roll_back_together(lightweight):
    env = lightweight
    with create_task(env.publication, env.directory, env.workspace, env.query) as task:
        item = task.claim()
        before = dict(task.db.execute("SELECT * FROM items WHERE seq=?", (item["seq"],)).fetchone())
        state_before = task.meta("state")
        task.db.execute(
            "CREATE TRIGGER reject_failed BEFORE UPDATE ON meta "
            "WHEN NEW.key='state' AND NEW.value='\"FAILED\"' "
            "BEGIN SELECT RAISE(ABORT,'synthetic failed-state SQL fault'); END"
        )
        with pytest.raises(sqlite3.IntegrityError, match="failed-state SQL fault"):
            task.finish_item(
                item["seq"],
                state="FAILED",
                code="publication_range",
                operation=item["operation_id"],
                diagnostic={
                    "code": "publication_range",
                    "phase": "publication_fetch",
                    "cause_code": "origin_timeout",
                    "url": "SECRET_TOKEN",
                },
            )
        assert (
            dict(task.db.execute("SELECT * FROM items WHERE seq=?", (item["seq"],)).fetchone())
            == before
        )
        assert task.meta("state") == state_before
        assert task.failure_diagnostic(item["seq"]) is None
    with TaskDB(env.directory, readonly=True) as task:
        assert (
            dict(task.db.execute("SELECT * FROM items WHERE seq=?", (item["seq"],)).fetchone())
            == before
        )
        assert task.meta("state") == state_before
        assert task.failure_diagnostic(item["seq"]) is None
