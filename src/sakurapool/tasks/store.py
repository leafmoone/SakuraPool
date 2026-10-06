"""Durable task state; v2 explicitly freezes workspace ownership and capacity."""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import uuid
from contextlib import contextmanager
from pathlib import Path

from ..capacity import UINT64_MAX, CapacityConfig
from ..fs_safety import plain_entry
from ..storage.budget import BudgetExceeded, Reservation
from ..storage.retrieval import _real_output_root
from .context import (
    LEGACY_CAPACITY,
    WORKSPACE_FORMAT,
    bootstrap_workspace,
    remaining_limits,
    resolve_workspace,
)
from .plan import FORMAT, canonical, plan_digest, selection_digest

MAX_DB_BYTES = 32 << 20
MAX_JOURNAL_BYTES = MAX_DB_BYTES + (1 << 20)
SCHEMA_VERSION = 1


class TaskError(RuntimeError):
    def __init__(self, code, phase="task", *, recoverable=False):
        super().__init__(code)
        self.code, self.phase, self.recoverable = code, phase, recoverable

    def public_diagnostic(self):
        result = {
            "code": self.code,
            "phase": self.phase,
            "recoverable": self.recoverable,
            "delivery": getattr(self, "delivery", "NOT_PUBLISHED"),
            "accounting": getattr(self, "accounting", "UNKNOWN"),
        }
        result.update(getattr(self, "safe_details", {}))
        result["secondary"] = [*result.get("secondary", ()), *getattr(self, "task_secondary", ())]
        if hasattr(self, "resources"):
            result["resources"] = self.resources
        return result


def _connect(path, *, readonly=False, capacity=None):
    capacity = LEGACY_CAPACITY if capacity is None else capacity
    db = sqlite3.connect(
        path.as_uri() + ("?mode=ro" if readonly else "?mode=rw"),
        uri=True,
        isolation_level=None,
        timeout=5,
    )
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys=ON")
    if not readonly:
        db.execute("PRAGMA journal_mode=DELETE")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA temp_store=MEMORY")
        page_size = db.execute("PRAGMA page_size").fetchone()[0]
        db.execute(f"PRAGMA max_page_count={capacity.task_db_bytes // page_size}")
    return db


def _check_files(path, capacity):
    if plain_entry(path).stat().st_size > capacity.task_db_bytes:
        raise TaskError("RESOURCE_BLOCKED", "taskdb")
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(str(path) + suffix)
        if os.path.lexists(sidecar):
            plain_entry(sidecar)
            if suffix != "-journal" or sidecar.stat().st_size > capacity.task_journal_bytes:
                raise TaskError("TASKDB_SIDECAR_CONFLICT")


def _bounded_meta(db, key, limit, *, missing=False):
    # length(TEXT) counts characters and stops at NUL; neither is a byte bound.
    row = db.execute(
        "SELECT CASE WHEN typeof(value)='text' AND length(value)<=? "
        "AND length(CAST(value AS BLOB))<=? AND instr(value,char(0))=0 "
        "THEN value ELSE NULL END FROM meta WHERE key=? LIMIT 2",
        (limit, limit, key),
    ).fetchall()
    if not row:
        if missing:
            return None
        raise TaskError("TASKDB_META_MISSING")
    if len(row) != 1 or row[0][0] is None:
        raise TaskError("TASK_HEADER_LIMIT")
    raw = row[0][0]
    # Bound nesting before the recursive JSON decoder or canonical writer runs.
    depth = 0
    quoted = escaped = False
    for char in raw:
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
        elif char == '"':
            quoted = True
        elif char in "[{":
            depth += 1
            if depth > 64:
                raise TaskError("TASK_HEADER_LIMIT")
        elif char in "]}":
            depth -= 1
    try:
        value = json.loads(raw)
        if len(canonical(value)) > limit:
            raise TaskError("TASK_HEADER_LIMIT")
        return value
    except (ValueError, TypeError, RecursionError):
        raise TaskError("TASK_HEADER_INVALID") from None


class TaskDB:
    def __init__(self, directory, *, readonly=False, workspace=None):
        self.directory = plain_entry(Path(directory).absolute(), directory=True)
        self.path = plain_entry(self.directory / "task.sqlite")
        self.workspace = bootstrap_workspace(self.directory, workspace)
        self.capacity = self.workspace.capacity if self.workspace is not None else LEGACY_CAPACITY
        _check_files(self.path, self.capacity)
        # Probe read-only first: no journal recovery, PRAGMA mutation or capacity
        # defaults may precede durable ownership validation.
        probe = _connect(self.path, readonly=True)
        try:
            version = probe.execute("PRAGMA user_version").fetchone()[0]
            if version not in (1, 2):
                raise TaskError("TASKDB_VERSION")
            if version == 2 and self.workspace is None:
                raise TaskError("TASK_WORKSPACE_CONFLICT", "plan")
            if version == 1 and self.workspace is not None:
                raise TaskError("TASK_WORKSPACE_CONFLICT", "plan")
            # New tasks carry a small independent binding. Older v2 tasks use
            # the same trusted ancestor capacity, then validate the bounded header.
            durable_binding = _bounded_meta(probe, "workspace_binding", 16384, missing=True)
            if durable_binding is not None:
                resolve_workspace(durable_binding, self.directory, self.workspace)
                if version != 2:
                    raise TaskError("TASKDB_VERSION")
            header = _bounded_meta(probe, "header", self.capacity.task_header_bytes)
            if not isinstance(header, dict):
                raise TaskError("TASK_HEADER_INVALID")
            binding = header.get("workspace_binding") if version == 2 else None
            if (version == 2 and (header.get("format") != WORKSPACE_FORMAT or binding is None)) or (
                version == 1
                and (
                    header.get("format") != FORMAT
                    or "workspace_binding" in header
                    or "effective_capacity" in header
                )
            ):
                raise TaskError("TASKDB_VERSION")
            resolve_workspace(binding, self.directory, self.workspace)
            declared_capacity = (
                CapacityConfig.from_dict(header["effective_capacity"])
                if version == 2
                else LEGACY_CAPACITY
            )
            if declared_capacity != self.capacity:
                raise TaskError("TASK_WORKSPACE_CONFLICT", "plan")
            if _bounded_meta(probe, "plan_digest", 128) != plan_digest(header):
                raise TaskError("PLAN_IDENTITY_MISMATCH", "plan")
        except (ValueError, KeyError, TypeError, RecursionError):
            raise TaskError("TASK_HEADER_INVALID") from None
        finally:
            probe.close()
        _check_files(self.path, self.capacity)
        self.db = _connect(self.path, readonly=readonly, capacity=self.capacity)
        self.readonly = readonly
        self.version = version

    @classmethod
    def create(cls, directory, ledger, header, rows, *, publication_path, max_bytes=512 << 20,
               image_extensions=None):
        from ..image_formats import image_extensions as validate_extensions

        extensions = validate_extensions(image_extensions)
        directory = Path(directory).absolute()
        workspace = getattr(ledger, "workspace", None)
        capacity = workspace.capacity if workspace is not None else LEGACY_CAPACITY
        binding = workspace.task_binding(directory) if workspace is not None else None
        if "workspace_binding" in header or "effective_capacity" in header:
            raise TaskError("TASK_HEADER_INVALID")
        parent = plain_entry(directory.parent, directory=True)
        _real_output_root(parent, ledger)
        if os.path.lexists(directory):
            raise TaskError("TASK_DIRECTORY_CONFLICT")
        ledger.status()
        maximum = ledger.limits["saved_bytes"]
        if type(max_bytes) is not int or not 0 <= max_bytes <= (
            UINT64_MAX if maximum is None else maximum
        ):
            raise TaskError("TASK_OUTPUT_LIMIT")
        required = capacity.task_db_bytes + capacity.task_journal_bytes + 16384
        try:
            lease = ledger.reserve(Reservation(disk=required))
        except BudgetExceeded:
            error = TaskError("RESOURCE_BLOCKED", "create", recoverable=True)
            remaining = remaining_limits(ledger, {"disk": required})["disk"]
            error.resources = {
                "required": {"disk": required},
                "remaining": {"disk": remaining},
                "effective_limit": ledger.limits,
            }
            raise error from None
        directory.mkdir()
        path = directory / "task.sqlite"
        with path.open("xb"):
            pass
        db = _connect(path, capacity=capacity)
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
                    delivery TEXT NOT NULL DEFAULT 'NONE',accounting TEXT NOT NULL DEFAULT \
'UNKNOWN',
                    receipt TEXT,code TEXT);
            """)
            db.execute(f"PRAGMA user_version={2 if workspace is not None else 1}")
            db.execute("BEGIN IMMEDIATE")
            count = 0
            for row in rows:
                if count >= capacity.freeze_count:
                    raise TaskError("RESOURCE_BLOCKED", "selection")
                db.execute(
                    "INSERT INTO items(seq,rid,record_id,source,dataset,post_id) "
                    "VALUES(?,?,?,?,?,?)",
                    (count, row.rid, row.record_id, row.source, row.dataset, row.post_id),
                )
                count += 1
                if count % capacity.record_batch == 0:
                    db.execute("COMMIT")
                    db.execute("BEGIN IMMEDIATE")
            actual_count, digest = selection_digest(
                db.execute(
                    "SELECT seq,rid,record_id,source,dataset,post_id FROM items ORDER BY seq"
                )
            )
            frozen = {
                **header,
                "format": WORKSPACE_FORMAT if workspace is not None else FORMAT,
                "selection_count": actual_count,
                "selection_digest": digest,
                "metadata": bool(header.get("metadata", False)),
                "output_policy": "task-relative-no-overwrite-v1",
            }
            if workspace is not None:
                frozen.update(workspace_binding=binding, effective_capacity=capacity.to_dict())
            if len(canonical(frozen)) > capacity.task_header_bytes:
                raise TaskError("TASK_HEADER_LIMIT")
            values = {
                "task_id": uuid.uuid4().hex,
                "header": frozen,
                "plan_digest": plan_digest(frozen),
                "state": "READY",
                "request": None,
                "publication_path": str(Path(publication_path).absolute()),
                "max_output_bytes": max_bytes,
                "image_extensions": list(extensions),
                "settings_version": 0,
                "confirmed_output_bytes": 0,
            }
            if binding is not None:
                values["workspace_binding"] = binding
            db.executemany(
                "INSERT INTO meta VALUES(?,?)",
                [(key, canonical(value).decode()) for key, value in values.items()],
            )
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
                error.task_secondary = (
                    *getattr(error, "task_secondary", ()),
                    "TASK_ROLLBACK_FAILED",
                )
            if (
                isinstance(error, sqlite3.OperationalError)
                and str(error) == "database or disk is full"
            ):
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
                primary.task_secondary = (
                    *getattr(primary, "task_secondary", ()),
                    "TASK_CLOSE_FAILED",
                )
        ledger.settle(lease)
        return cls(directory, workspace=workspace)

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
                error.task_secondary = (
                    *getattr(error, "task_secondary", ()),
                    "TASK_ROLLBACK_FAILED",
                )
            if (
                isinstance(error, sqlite3.OperationalError)
                and str(error) == "database or disk is full"
            ):
                converted = TaskError("RESOURCE_BLOCKED", "taskdb", recoverable=True)
                converted.task_secondary = getattr(error, "task_secondary", ())
                raise converted from None
            raise

    def meta(self, key):
        if key == "header":
            return _bounded_meta(self.db, key, self.capacity.task_header_bytes)
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        if row is None:
            raise TaskError("TASKDB_META_MISSING")
        return json.loads(row[0])

    @property
    def image_extensions(self):
        from ..image_formats import image_extensions

        row = self.db.execute("SELECT value FROM meta WHERE key='image_extensions'").fetchone()
        return image_extensions(json.loads(row[0]) if row else None)

    @property
    def settings_version(self):
        row = self.db.execute("SELECT value FROM meta WHERE key='settings_version'").fetchone()
        version = json.loads(row[0]) if row else 0
        if type(version) is not int or not 0 <= version < (1 << 63):
            raise TaskError("SETTINGS_VERSION_INVALID", "update")
        return version

    def set_meta(self, db, key, value):
        db.execute("UPDATE meta SET value=? WHERE key=?", (canonical(value).decode(), key))

    def validate_plan(self):
        header = self.meta("header")
        count, digest = selection_digest(
            self.db.execute(
                "SELECT seq,rid,record_id,source,dataset,post_id FROM items ORDER BY seq"
            )
        )
        if (
            header.get("format") != (WORKSPACE_FORMAT if self.version == 2 else FORMAT)
            or count != header.get("selection_count")
            or digest != header.get("selection_digest")
            or plan_digest(header) != self.meta("plan_digest")
        ):
            raise TaskError("PLAN_IDENTITY_MISMATCH", "plan")
        return header

    def request(self, value):
        if value not in ("PAUSE", "CANCEL"):
            raise TaskError("TASK_REQUEST_INVALID")
        with self.transaction() as db:
            self.set_meta(db, "request", value)
        return {"requested": value, "state": self.meta("state")}

    def _pipeline_candidate(self, db=None, *, limit=1):
        if type(limit) is not int or not 1 <= limit <= 8:
            raise TaskError("TASK_IDENTITY_INVALID")
        connection = self.db if db is None else db
        rows = connection.execute(
            "SELECT CASE WHEN typeof(seq)='integer' THEN seq ELSE NULL END AS seq,"
            "CASE WHEN typeof(rid)='integer' THEN rid ELSE NULL END AS rid,"
            "CASE WHEN typeof(record_id)='text' AND length(record_id)=32 "
            "AND instr(record_id,char(0))=0 THEN record_id ELSE NULL END AS record_id "
            "FROM items WHERE state='READY' ORDER BY seq LIMIT ?",
            (limit,),
        ).fetchall()
        for row in rows:
            if (
                row["record_id"] is None
                or type(row["seq"]) is not int
                or not 0 <= row["seq"] < 1 << 64
                or type(row["rid"]) is not int
                or not 0 <= row["rid"] < 1 << 64
            ):
                raise TaskError("TASK_IDENTITY_INVALID")
        if limit != 1:
            return [dict(row) for row in rows]
        return None if not rows else dict(rows[0])

    def _pipeline_claim(self, expected=None, *, window=1):
        with self.transaction() as db:
            if self.meta("request") is not None:
                return None
            if expected is None:
                row = self._pipeline_candidate(db)
            else:
                candidates = self._pipeline_candidate(db, limit=window)
                if window == 1:
                    candidates = [] if candidates is None else [candidates]
                row = next((candidate for candidate in candidates if candidate == expected), None)
                if row is None:
                    raise TaskError("TASK_IDENTITY_INVALID")
            return self._claim_row(db, row)

    def _claim_row(self, db, row):
        if row is None:
            return None
        attempt, operation = uuid.uuid4().hex, uuid.uuid4().hex
        db.execute(
            "INSERT INTO attempts(attempt_id,seq,operation_id,phase) VALUES(?,?,?,?)",
            (attempt, row["seq"], operation, "CLAIMED"),
        )
        db.execute(
            "UPDATE items SET state='IN_PROGRESS',attempt_id=?,accounting='NONE' WHERE seq=?",
            (attempt, row["seq"]),
        )
        return dict(row) | {"attempt_id": attempt, "operation_id": operation}

    def claim(self):
        with self.transaction() as db:
            if self.meta("request") is not None:
                return None
            return self._claim_row(
                db,
                db.execute(
                    "SELECT * FROM items WHERE state='READY' ORDER BY seq LIMIT 1"
                ).fetchone(),
            )

    def event(self, attempt, event, payload):
        previous = {
            "NETWORK_START": "CLAIMED",
            "OUTPUT_RESERVED": "NETWORK_START",
            "PREPARED": "OUTPUT_RESERVED",
            "PUBLISHED": "PREPARED",
            "SETTLED": "PUBLISHED",
        }
        if event not in previous:
            raise TaskError("ATTEMPT_EVENT_INVALID")
        with self.transaction() as db:
            row = db.execute("SELECT * FROM attempts WHERE attempt_id=?", (attempt,)).fetchone()
            if row is None:
                raise TaskError("ATTEMPT_IDENTITY_MISSING")
            if row["phase"] != previous[event]:
                raise TaskError("ATTEMPT_EVENT_ORDER_INVALID")
            db.execute("UPDATE attempts SET phase=? WHERE attempt_id=?", (event, attempt))
            if event == "NETWORK_START":
                db.execute(
                    "UPDATE attempts SET network_state='UNKNOWN' WHERE attempt_id=?", (attempt,)
                )
            elif event == "OUTPUT_RESERVED":
                db.execute(
                    "UPDATE attempts SET output_lease=? WHERE attempt_id=?",
                    (payload["lease"], attempt),
                )
            elif event == "PREPARED":
                receipt = canonical(
                    {
                        **payload,
                        "task_id": self.meta("task_id"),
                        "plan_digest": self.meta("plan_digest"),
                        "operation_id": row["operation_id"],
                    }
                ).decode()
                db.execute("UPDATE attempts SET receipt=? WHERE attempt_id=?", (receipt, attempt))
                db.execute("UPDATE items SET receipt=? WHERE seq=?", (receipt, row["seq"]))
            elif event == "PUBLISHED":
                db.execute(
                    "UPDATE attempts SET delivery='PUBLISHED' WHERE attempt_id=?", (attempt,)
                )
                db.execute("UPDATE items SET delivery='PUBLISHED' WHERE seq=?", (row["seq"],))
            elif event == "SETTLED":
                prepared = json.loads(row["receipt"])
                amount = sum(proof["bytes"] for proof in prepared["receipt"].values())
                if row["accounting"] != "CONFIRMED":
                    self.set_meta(
                        db, "confirmed_output_bytes", self.meta("confirmed_output_bytes") + amount
                    )
                db.execute(
                    "UPDATE attempts SET accounting='CONFIRMED',network_state='CONFIRMED' "
                    "WHERE attempt_id=?",
                    (attempt,),
                )
                db.execute(
                    "UPDATE items SET delivery='PUBLISHED',accounting='CONFIRMED' WHERE seq=?",
                    (row["seq"],),
                )

    def finish_item(self, seq, *, state, code=None, accounting=None, accounting_basis=None):
        with self.transaction() as db:
            if state == "READY" and accounting == "CONFIRMED" and code is not None:
                db.execute(
                    "UPDATE attempts SET accounting='CONFIRMED',network_state='CONFIRMED' "
                    "WHERE attempt_id=(SELECT attempt_id FROM items WHERE seq=?)",
                    (seq,),
                )
            if accounting_basis == "CONSERVATIVE_MAX_CHARGE":
                if state != "READY" or accounting != "CONFIRMED" or code is None:
                    raise ValueError("conservative attempt must be confirmed unpublished")
                attempt = db.execute(
                    "SELECT attempt_id FROM items WHERE seq=?", (seq,)
                ).fetchone()[0]
                if attempt is None:
                    raise ValueError("conservative attempt identity absent")
                db.execute(
                    "INSERT INTO meta(key,value) VALUES(?,?)",
                    ("accounting_basis:" + attempt, canonical({
                        "accounting_basis": "CONSERVATIVE_MAX_CHARGE",
                        "actual_consumption": "UNKNOWN", "accounted": "CONSERVATIVE_MAX",
                    }).decode()),
                )
            db.execute(
                "UPDATE items SET state=?,code=?,accounting=COALESCE(?,accounting) WHERE seq=?",
                (state, code, accounting, seq),
            )

    def inspect(self):
        stats = [
            dict(row)
            for row in self.db.execute(
                "SELECT state,delivery,accounting,count(*) AS count FROM items "
                "GROUP BY state,delivery,accounting"
            )
        ]
        errors = self.db.execute("SELECT count(*) FROM items WHERE code IS NOT NULL").fetchone()[0]
        confirmed = self.db.execute(
            "SELECT count(*) FROM items WHERE accounting='CONFIRMED' AND delivery='PUBLISHED'"
        ).fetchone()[0]
        unknown = self.db.execute(
            "SELECT count(*) FROM items WHERE accounting='UNKNOWN'"
        ).fetchone()[0]
        return {
            "settings_version": self.settings_version,
            "image_extensions": list(self.image_extensions),
            "max_output_bytes": self.meta("max_output_bytes"),
            "confirmed_output_bytes": self.meta("confirmed_output_bytes"),
            "requested_count": self.meta("header")["selection_count"],
            "delivered_confirmed": confirmed,
            "error_count": errors,
            "unknown_accounting_count": unknown,
            "task_id": self.meta("task_id"),
            "plan_digest": self.meta("plan_digest"),
            "state": self.meta("state"),
            "request": self.meta("request"),
            "header": self.meta("header"),
            "items": stats,
        }

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
                    primary.task_secondary = (
                        *getattr(primary, "task_secondary", ()),
                        "TASK_UNLOCK_FAILED",
                    )
        finally:
            primary = sys.exc_info()[1]
            try:
                lock.close()
            except BaseException:
                if primary is None:
                    raise
                primary.task_secondary = (
                    *getattr(primary, "task_secondary", ()),
                    "TASK_LOCK_CLOSE_FAILED",
                )

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
            primary.task_secondary = (*getattr(primary, "task_secondary", ()), "TASK_CLOSE_FAILED")
