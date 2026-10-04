"""Bounded SQLite authority with explicit short durable transactions."""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import uuid
from contextlib import contextmanager
from pathlib import Path

from ..fs_safety import plain_entry
from ..storage.budget import BudgetExceeded, Reservation
from ..storage.retrieval import _real_output_root
from .plan import FORMAT, MAX_SELECTION, canonical, plan_digest, selection_digest

MAX_DB_BYTES = 32 << 20
MAX_JOURNAL_BYTES = MAX_DB_BYTES + (1 << 20)
SCHEMA_VERSION = 1


class TaskError(RuntimeError):
    def __init__(self, code, phase="task", *, recoverable=False):
        super().__init__(code)
        self.code, self.phase, self.recoverable = code, phase, recoverable

    def public_diagnostic(self):
        result = {"code": self.code, "phase": self.phase, "recoverable": self.recoverable,
                  "delivery": getattr(self, "delivery", "NOT_PUBLISHED"),
                  "accounting": getattr(self, "accounting", "UNKNOWN")}
        result.update(getattr(self, "safe_details", {}))
        result["secondary"] = [*result.get("secondary", ()),
                               *getattr(self, "task_secondary", ())]
        if hasattr(self, "resources"):
            result["resources"] = self.resources
        return result


def _connect(path, *, readonly=False):
    db = sqlite3.connect(path.as_uri() + ("?mode=ro" if readonly else "?mode=rw"),
                         uri=True, isolation_level=None, timeout=5)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys=ON")
    if not readonly:
        db.execute("PRAGMA journal_mode=DELETE")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA temp_store=MEMORY")
        page_size = db.execute("PRAGMA page_size").fetchone()[0]
        db.execute(f"PRAGMA max_page_count={MAX_DB_BYTES // page_size}")
    return db


class TaskDB:
    def __init__(self, directory, *, readonly=False):
        self.directory = plain_entry(Path(directory).absolute(), directory=True)
        self.path = plain_entry(self.directory / "task.sqlite")
        if self.path.stat().st_size > MAX_DB_BYTES:
            raise TaskError("RESOURCE_BLOCKED", "taskdb")
        for suffix in ("-wal", "-shm", "-journal"):
            sidecar = Path(str(self.path) + suffix)
            if os.path.lexists(sidecar):
                plain_entry(sidecar)
                if suffix != "-journal" or sidecar.stat().st_size > MAX_JOURNAL_BYTES:
                    raise TaskError("TASKDB_SIDECAR_CONFLICT")
        self.db = _connect(self.path, readonly=readonly)
        self.readonly = readonly
        if self.db.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION:
            self.close()
            raise TaskError("TASKDB_VERSION")

    @classmethod
    def create(cls, directory, ledger, header, rows, *, publication_path, max_bytes=512 << 20):
        directory = Path(directory).absolute()
        parent = plain_entry(directory.parent, directory=True)
        _real_output_root(parent, ledger)
        if os.path.lexists(directory):
            raise TaskError("TASK_DIRECTORY_CONFLICT")
        if type(max_bytes) is not int or not 0 <= max_bytes <= ledger.limits["saved_bytes"]:
            raise TaskError("TASK_OUTPUT_LIMIT")
        required = MAX_DB_BYTES + MAX_JOURNAL_BYTES + 16384
        try:
            lease = ledger.reserve(Reservation(disk=required))
        except BudgetExceeded:
            error = TaskError("RESOURCE_BLOCKED", "create", recoverable=True)
            remaining = max(0, ledger.limits["disk"] - ledger.status()["disk"])
            error.resources = {"required": {"disk": required}, "remaining": {"disk": remaining},
                               "effective_limit": ledger.limits}
            raise error from None
        directory.mkdir()
        path = directory / "task.sqlite"
        with path.open("xb"):
            pass
        db = _connect(path)
        try:
            db.executescript("""
                CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE items(
                    seq INTEGER PRIMARY KEY,rid INTEGER NOT NULL,record_id TEXT UNIQUE NOT NULL,
                    source TEXT NOT NULL,dataset TEXT NOT NULL,post_id TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'READY',delivery TEXT NOT NULL DEFAULT 'NONE',
                    accounting TEXT NOT NULL DEFAULT 'NONE',attempt_id TEXT,receipt TEXT,code TEXT);
                CREATE INDEX items_state_seq ON items(state,seq);
                CREATE TABLE attempts(
                    attempt_id TEXT PRIMARY KEY,seq INTEGER NOT NULL REFERENCES items(seq),
                    operation_id TEXT NOT NULL UNIQUE,phase TEXT NOT NULL,
                    output_lease TEXT,network_state TEXT NOT NULL DEFAULT 'NOT_STARTED',
                    delivery TEXT NOT NULL DEFAULT 'NONE',
                    accounting TEXT NOT NULL DEFAULT 'UNKNOWN',
                    receipt TEXT,code TEXT);
                PRAGMA user_version=1;
            """)
            db.execute("BEGIN IMMEDIATE")
            count = 0
            for row in rows:
                if count >= MAX_SELECTION:
                    raise TaskError("RESOURCE_BLOCKED", "selection")
                db.execute("INSERT INTO items(seq,rid,record_id,source,dataset,post_id) "
                           "VALUES(?,?,?,?,?,?)",
                           (count, row.rid, row.record_id, row.source, row.dataset, row.post_id))
                count += 1
                if count % 512 == 0:
                    db.execute("COMMIT")
                    db.execute("BEGIN IMMEDIATE")
            actual_count, digest = selection_digest(
                db.execute(
                    "SELECT seq,rid,record_id,source,dataset,post_id FROM items ORDER BY seq"
                )
            )
            frozen = {**header, "format": FORMAT, "selection_count": actual_count,
                      "selection_digest": digest, "metadata": bool(header.get("metadata", False)),
                      "output_policy": "task-relative-no-overwrite-v1"}
            if len(canonical(frozen)) > 65536:
                raise TaskError("TASK_HEADER_LIMIT")
            values = {"task_id": uuid.uuid4().hex, "header": frozen,
                      "plan_digest": plan_digest(frozen), "state": "READY", "request": None,
                      "publication_path": str(Path(publication_path).absolute()),
                      "max_output_bytes": max_bytes, "confirmed_output_bytes": 0}
            db.executemany("INSERT INTO meta VALUES(?,?)",
                           [(key, canonical(value).decode()) for key, value in values.items()])
            db.execute("COMMIT")
            (directory / "output").mkdir()
            with (directory / "runner.lock").open("xb") as lock:
                lock.write(b"T")
                lock.flush()
                os.fsync(lock.fileno())
        except BaseException as error:
            try:
                if db.in_transaction:
                    db.execute("ROLLBACK")
            except BaseException:
                error.task_secondary = (*getattr(error, "task_secondary", ()),
                                        "TASK_ROLLBACK_FAILED")
            # Preserve incomplete artifact, never broad cleanup or quota reset.
            if (isinstance(error, sqlite3.OperationalError)
                    and str(error) == "database or disk is full"):
                converted = TaskError("RESOURCE_BLOCKED", "taskdb", recoverable=True)
                converted.task_secondary = getattr(error, "task_secondary", ())
                raise converted from None
            raise
        finally:
            primary = sys.exc_info()[1]
            try:
                db.close()
            except BaseException:
                if primary is None:
                    raise
                primary.task_secondary = (*getattr(primary, "task_secondary", ()),
                                          "TASK_CLOSE_FAILED")
        ledger.settle(lease)
        return cls(directory)

    @contextmanager
    def transaction(self):
        if self.readonly:
            raise TaskError("TASKDB_READONLY")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield self.db
            self.db.execute("COMMIT")
        except BaseException as error:
            try:
                if self.db.in_transaction:
                    self.db.execute("ROLLBACK")
            except BaseException:
                error.task_secondary = (*getattr(error, "task_secondary", ()),
                                        "TASK_ROLLBACK_FAILED")
            if (isinstance(error, sqlite3.OperationalError)
                    and str(error) == "database or disk is full"):
                converted = TaskError("RESOURCE_BLOCKED", "taskdb", recoverable=True)
                converted.task_secondary = getattr(error, "task_secondary", ())
                raise converted from None
            raise

    def meta(self, key):
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        if row is None:
            raise TaskError("TASKDB_META_MISSING")
        return json.loads(row[0])

    def set_meta(self, db, key, value):
        db.execute("UPDATE meta SET value=? WHERE key=?", (canonical(value).decode(), key))

    def validate_plan(self):
        header = self.meta("header")
        count, digest = selection_digest(self.db.execute(
            "SELECT seq,rid,record_id,source,dataset,post_id FROM items ORDER BY seq"
        ))
        if (header.get("format") != FORMAT or count != header.get("selection_count")
                or digest != header.get("selection_digest")
                or plan_digest(header) != self.meta("plan_digest")):
            raise TaskError("PLAN_IDENTITY_MISMATCH", "plan")
        return header

    def request(self, value):
        if value not in ("PAUSE", "CANCEL"):
            raise TaskError("TASK_REQUEST_INVALID")
        with self.transaction() as db:
            self.set_meta(db, "request", value)
        return {"requested": value, "state": self.meta("state")}

    def _pipeline_candidate(self, db=None):
        connection = self.db if db is None else db
        # CASE gates prevent materializing oversized selected values; selecting
        # first READY before validation avoids silently skipping corrupt identity.
        row = connection.execute(
            "SELECT CASE WHEN typeof(seq)='integer' THEN seq ELSE NULL END AS seq,"
            "CASE WHEN typeof(rid)='integer' THEN rid ELSE NULL END AS rid,"
            "CASE WHEN typeof(record_id)='text' AND length(record_id)=32 "
            "AND instr(record_id,char(0))=0 "
            "THEN record_id ELSE NULL END AS record_id FROM items "
            "WHERE state='READY' ORDER BY seq LIMIT 1").fetchone()
        if row is not None and (row["record_id"] is None
                or type(row["seq"]) is not int or not 0 <= row["seq"] < 1 << 64
                or type(row["rid"]) is not int or not 0 <= row["rid"] < 1 << 64):
            raise TaskError("TASK_IDENTITY_INVALID")
        return None if row is None else dict(row)

    def _pipeline_claim(self):
        with self.transaction() as db:
            if self.meta("request") is not None:
                return None
            row = self._pipeline_candidate(db)
            if row is None:
                return None
            attempt, operation = uuid.uuid4().hex, uuid.uuid4().hex
            db.execute("INSERT INTO attempts(attempt_id,seq,operation_id,phase) VALUES(?,?,?,?)",
                       (attempt, row["seq"], operation, "CLAIMED"))
            db.execute("UPDATE items SET state='IN_PROGRESS',attempt_id=?,accounting='NONE' "
                       "WHERE seq=?", (attempt, row["seq"]))
            return row | {"attempt_id": attempt, "operation_id": operation}

    def claim(self):
        """Claim one seq in a short transaction, only under runner ownership."""
        with self.transaction() as db:
            if self.meta("request") is not None:
                return None
            row = db.execute(
                "SELECT * FROM items WHERE state='READY' ORDER BY seq LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            attempt = uuid.uuid4().hex
            operation = uuid.uuid4().hex
            db.execute("INSERT INTO attempts(attempt_id,seq,operation_id,phase) VALUES(?,?,?,?)",
                       (attempt, row["seq"], operation, "CLAIMED"))
            db.execute("UPDATE items SET state='IN_PROGRESS',attempt_id=?,accounting='NONE' "
                       "WHERE seq=?",
                       (attempt, row["seq"]))
            return dict(row) | {"attempt_id": attempt, "operation_id": operation}

    def event(self, attempt, event, payload):
        allowed = {"NETWORK_START", "OUTPUT_RESERVED", "PREPARED", "PUBLISHED", "SETTLED"}
        if event not in allowed:
            raise TaskError("ATTEMPT_EVENT_INVALID")
        with self.transaction() as db:
            # Only current newly-generated attempt is admitted here; payload
            # receipt came through the 16KiB typed RPC gate, not historical items.
            row = db.execute("SELECT * FROM attempts WHERE attempt_id=?", (attempt,)).fetchone()
            if row is None:
                raise TaskError("ATTEMPT_IDENTITY_MISSING")
            previous = {"NETWORK_START": "CLAIMED", "OUTPUT_RESERVED": "NETWORK_START",
                        "PREPARED": "OUTPUT_RESERVED", "PUBLISHED": "PREPARED",
                        "SETTLED": "PUBLISHED"}
            if row["phase"] != previous[event]:
                raise TaskError("ATTEMPT_EVENT_ORDER_INVALID")
            db.execute("UPDATE attempts SET phase=? WHERE attempt_id=?", (event, attempt))
            if event == "NETWORK_START":
                db.execute("UPDATE attempts SET network_state='UNKNOWN' WHERE attempt_id=?",
                           (attempt,))
            elif event == "OUTPUT_RESERVED":
                db.execute("UPDATE attempts SET output_lease=? WHERE attempt_id=?",
                           (payload["lease"], attempt))
            elif event == "PREPARED":
                receipt = canonical({**payload, "task_id": self.meta("task_id"),
                                     "plan_digest": self.meta("plan_digest"),
                                     "operation_id": row["operation_id"]}).decode()
                db.execute("UPDATE attempts SET receipt=? WHERE attempt_id=?", (receipt, attempt))
                db.execute("UPDATE items SET receipt=? WHERE seq=?", (receipt, row["seq"]))
            elif event == "PUBLISHED":
                db.execute("UPDATE attempts SET delivery='PUBLISHED' WHERE attempt_id=?",
                           (attempt,))
                db.execute("UPDATE items SET delivery='PUBLISHED' WHERE seq=?", (row["seq"],))
            elif event == "SETTLED":
                prepared = json.loads(row["receipt"])
                amount = sum(proof["bytes"] for proof in prepared["receipt"].values())
                if row["accounting"] != "CONFIRMED":
                    self.set_meta(db, "confirmed_output_bytes",
                                  self.meta("confirmed_output_bytes") + amount)
                # Evidence is this operation's successful settle return and all
                # successful range context exits, not ledger absence/counter delta.
                db.execute("UPDATE attempts SET accounting='CONFIRMED',network_state='CONFIRMED' "
                           "WHERE attempt_id=?", (attempt,))
                db.execute("UPDATE items SET delivery='PUBLISHED',accounting='CONFIRMED' "
                           "WHERE seq=?", (row["seq"],))

    def finish_item(self, seq, *, state, code=None, accounting=None):
        with self.transaction() as db:
            if state == "READY" and accounting == "CONFIRMED" and code is not None:
                db.execute("UPDATE attempts SET accounting='CONFIRMED',network_state='CONFIRMED' "
                           "WHERE attempt_id=(SELECT attempt_id FROM items WHERE seq=?)", (seq,))
            db.execute("UPDATE items SET state=?,code=?,accounting=COALESCE(?,accounting) "
                       "WHERE seq=?", (state, code, accounting, seq))

    def inspect(self):
        stats = [dict(row) for row in self.db.execute(
            "SELECT state,delivery,accounting,count(*) AS count FROM items "
            "GROUP BY state,delivery,accounting"
        )]
        errors = self.db.execute("SELECT count(*) FROM items WHERE code IS NOT NULL").fetchone()[0]
        confirmed = self.db.execute("SELECT count(*) FROM items WHERE accounting='CONFIRMED' "
                                    "AND delivery='PUBLISHED'").fetchone()[0]
        unknown = self.db.execute(
            "SELECT count(*) FROM items WHERE accounting='UNKNOWN'"
        ).fetchone()[0]
        return {"requested_count": self.meta("header")["selection_count"],
                "delivered_confirmed": confirmed, "error_count": errors,
                "unknown_accounting_count": unknown,
                "task_id": self.meta("task_id"), "plan_digest": self.meta("plan_digest"),
                "state": self.meta("state"), "request": self.meta("request"),
                "header": self.meta("header"), "items": stats}

    @contextmanager
    def runner_lock(self):
        path = plain_entry(self.directory / "runner.lock")
        lock = path.open("r+b")
        try:
            try:
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                raise TaskError("RUNNER_BUSY", recoverable=True) from None
            try:
                lock.seek(0)
                if lock.read() != b"T":
                    raise TaskError("RUNNER_LOCK_INVALID")
                yield
            finally:
                primary = sys.exc_info()[1]
                try:
                    lock.seek(0)
                    if os.name == "nt":
                        msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                except BaseException:
                    if primary is None:
                        raise
                    primary.task_secondary = (*getattr(primary, "task_secondary", ()),
                                              "TASK_UNLOCK_FAILED")
        finally:
            primary = sys.exc_info()[1]
            try:
                lock.close()
            except BaseException:
                if primary is None:
                    raise
                primary.task_secondary = (*getattr(primary, "task_secondary", ()),
                                          "TASK_LOCK_CLOSE_FAILED")

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, primary, traceback):
        try:
            self.close()
        except BaseException:
            if primary is None:
                raise
            primary.task_secondary = (*getattr(primary, "task_secondary", ()),
                                      "TASK_CLOSE_FAILED")
