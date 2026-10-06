"""Lightweight durable task state; legacy quota tasks are read-only archives."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
import uuid
from contextlib import contextmanager
from pathlib import Path

from ..capacity import CapacityConfig
from ..download_naming import FLAT_POLICY, FilenameConfig, safe_output_path, stem_key
from ..fs_safety import plain_entry
from .context import LEGACY_CAPACITY, WORKSPACE_FORMAT, bootstrap_workspace, resolve_workspace
from .plan import FORMAT, canonical, plan_digest, selection_digest

RECORDDIR_FORMAT = "sakurapool-task-v3"
LIGHT_FORMAT = "sakurapool-task-v4"
MAX_DB_BYTES = 32 << 20
MAX_JOURNAL_BYTES = 33 << 20
SCHEMA_VERSION = 4


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
        }
        result.update(getattr(self, "safe_details", {}))
        result["secondary"] = list(getattr(self, "task_secondary", ()))
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
        raise TaskError("TASK_DB_LIMIT", "taskdb")
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(str(path) + suffix)
        if os.path.lexists(sidecar):
            plain_entry(sidecar)
            if suffix != "-journal" or sidecar.stat().st_size > capacity.task_journal_bytes:
                raise TaskError("TASKDB_SIDECAR_CONFLICT")


def _bounded_meta(db, key, limit, *, missing=False):
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


def safe_failure(details):
    """Fixed typed allowlist, no paths, messages, URLs, credentials or consumption."""
    from ..storage.transport import _SAFE_CODES, _SAFE_PHASES

    if type(details) is not dict:
        return {}
    result = {}
    for key in ("code", "cause_code"):
        value = details.get(key)
        if type(value) is str and value in _SAFE_CODES:
            result[key] = value
    for key in ("phase", "cause_phase"):
        value = details.get(key)
        if type(value) is str and value in _SAFE_PHASES:
            result[key] = value
    for key in ("http_status", "cause_http_status"):
        value = details.get(key)
        if type(value) is int and 100 <= value <= 599:
            result[key] = value
    for key in ("chunk_index",):
        value = details.get(key)
        if type(value) is int and 0 <= value < (1 << 64):
            result[key] = value
    if type(details.get("member_kind")) is str and details["member_kind"] in ("image", "metadata"):
        result["member_kind"] = details["member_kind"]
    for key, allowed in (
        ("delivery", {"NOT_PUBLISHED", "PUBLISHED"}),
        ("cleanup", {"SAFE", "PRESERVED"}),
    ):
        if type(details.get(key)) is str and details[key] in allowed:
            result[key] = details[key]
    if type(details.get("recoverable")) is bool:
        result["recoverable"] = details["recoverable"]
    if type(details.get("operation_phase")) is str and details["operation_phase"] in {
        "CLAIMED",
        "NETWORK_START",
        "STAGED",
        "PREPARED",
        "PUBLISH_INTENT",
        "PARTIAL",
        "PUBLISHED",
    }:
        result["operation_phase"] = details["operation_phase"]
    return result


class TaskDB:
    def __init__(self, directory, *, readonly=False, workspace=None):
        self.directory = plain_entry(Path(directory).absolute(), directory=True)
        self.path = plain_entry(self.directory / "task.sqlite")
        self.workspace = bootstrap_workspace(self.directory, workspace)
        self.capacity = self.workspace.capacity if self.workspace is not None else LEGACY_CAPACITY
        _check_files(self.path, self.capacity)
        probe = _connect(self.path, readonly=True)
        try:
            self.version = probe.execute("PRAGMA user_version").fetchone()[0]
            if self.version not in (1, 2, 3, 4):
                raise TaskError("TASKDB_VERSION")
            # This precedes writable connection, SQLite journal recovery and credentials.
            if self.version != 4 and not readonly:
                raise TaskError("LEGACY_TASK_MIGRATION_REQUIRED", "plan")
            header = _bounded_meta(probe, "header", self.capacity.task_header_bytes)
            if not isinstance(header, dict):
                raise TaskError("TASK_HEADER_INVALID")
            if self.version in (3, 4):
                expected_format = LIGHT_FORMAT if self.version == 4 else RECORDDIR_FORMAT
                if header.get("format") != expected_format:
                    raise TaskError("TASKDB_VERSION")
                declared = CapacityConfig.from_dict(header["effective_capacity"])
                if declared != self.capacity:
                    raise TaskError("TASK_CAPACITY_CONFLICT", "plan")
                expected = (
                    self.workspace.download_binding(self.directory) if self.workspace else None
                )
                if header.get("workspace_binding") != expected:
                    raise TaskError("TASK_WORKSPACE_CONFLICT", "plan")
                if self.version == 4:
                    self._validate_filenames(probe, header)
            else:
                if header.get("format") != (WORKSPACE_FORMAT if self.version == 2 else FORMAT):
                    raise TaskError("TASKDB_VERSION")
                binding = header.get("workspace_binding") if self.version == 2 else None
                resolve_workspace(binding, self.directory, self.workspace)
                declared = (
                    CapacityConfig.from_dict(header["effective_capacity"])
                    if self.version == 2
                    else LEGACY_CAPACITY
                )
                if declared != self.capacity:
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

    @classmethod
    def create(cls, directory, workspace, header, rows, *, publication_path, image_extensions=None,
               filename_template="{tag}_{index}", filename_prefix=None):
        from ..image_formats import image_extensions as validate_extensions
        from ..workspace import Workspace

        if workspace is not None and (
            not isinstance(workspace, Workspace) or not workspace.lightweight
        ):
            raise TaskError("LEGACY_TASK_MIGRATION_REQUIRED", "create")
        extensions = validate_extensions(image_extensions)
        try:
            naming = FilenameConfig.resolve(header.get("query", {}), template=filename_template,
                                            prefix=filename_prefix)
        except ValueError as error:
            raise TaskError(str(error), "create") from None
        capacity = workspace.capacity if workspace is not None else LEGACY_CAPACITY
        directory = Path(directory).absolute()
        if workspace is not None:
            workspace.task_path(directory)
        plain_entry(directory.parent, directory=True)
        if ".." in directory.parts or os.path.lexists(directory):
            raise TaskError("TASK_DIRECTORY_CONFLICT")
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
                    operation_id TEXT,phase TEXT,stage TEXT,receipt TEXT,code TEXT,diagnostic TEXT,
                    recovery_retries INTEGER NOT NULL DEFAULT 0,
                    output_stem TEXT NOT NULL,output_key TEXT UNIQUE NOT NULL,
                    published_members TEXT NOT NULL DEFAULT '[]');
                CREATE INDEX items_state_seq ON items(state,seq);
            """)
            db.execute("PRAGMA user_version=4")
            db.execute("BEGIN IMMEDIATE")
            count = 0
            names_digest = hashlib.sha256()
            for row in rows:
                if count >= capacity.freeze_count:
                    raise TaskError("SELECTION_LIMIT", "selection")
                try:
                    stem = naming.stem(count)
                    for suffix in (".jpeg", ".json"):
                        safe_output_path(directory / "output", stem + suffix)
                    key = stem_key(stem)
                    db.execute(
                        "INSERT INTO items(seq,rid,record_id,source,dataset,post_id,"
                        "output_stem,output_key) "
                        "VALUES(?,?,?,?,?,?,?,?)",
                        (count, row.rid, row.record_id, row.source, row.dataset, row.post_id,
                         stem, key),
                    )
                except (ValueError, sqlite3.IntegrityError):
                    raise TaskError("FILENAME_COLLISION_OR_INVALID", "create") from None
                names_digest.update(canonical([count, stem, key]) + b"\n")
                count += 1
            actual_count, digest = selection_digest(
                db.execute(
                    "SELECT seq,rid,record_id,source,dataset,post_id FROM items ORDER BY seq"
                )
            )
            frozen = {
                **header,
                "format": LIGHT_FORMAT,
                "selection_count": actual_count,
                "selection_digest": digest,
                "metadata": bool(header.get("metadata", False)),
                "output_policy": FLAT_POLICY,
                "filename_config": naming.to_dict(),
                "filename_digest": names_digest.hexdigest(),
                "effective_capacity": capacity.to_dict(),
                "workspace_binding": workspace.download_binding(directory) if workspace else None,
            }
            if len(canonical(frozen)) > capacity.task_header_bytes:
                raise TaskError("TASK_HEADER_LIMIT")
            values = {
                "task_id": uuid.uuid4().hex,
                "header": frozen,
                "plan_digest": plan_digest(frozen),
                "state": "READY",
                "request": None,
                "publication_path": str(Path(publication_path).absolute()),
                "image_extensions": list(extensions),
                "settings_version": 0,
            }
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
        finally:
            db.close()
        return cls(directory, workspace=workspace)

    @contextmanager
    def transaction(self):
        if self.readonly or self.version != 4:
            raise TaskError("TASKDB_READONLY")
        _check_files(self.path, self.capacity)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield self.db
            _check_files(self.path, self.capacity)
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
            raise

    def meta(self, key):
        return _bounded_meta(self.db, key, self.capacity.task_header_bytes)

    @property
    def image_extensions(self):
        from ..image_formats import image_extensions

        value = _bounded_meta(self.db, "image_extensions", 1024, missing=True)
        return image_extensions(value)

    @property
    def settings_version(self):
        version = _bounded_meta(self.db, "settings_version", 64, missing=True)
        version = 0 if version is None else version
        if type(version) is not int or not 0 <= version < (1 << 63):
            raise TaskError("SETTINGS_VERSION_INVALID", "update")
        return version

    def set_meta(self, db, key, value):
        db.execute("UPDATE meta SET value=? WHERE key=?", (canonical(value).decode(), key))

    def _validate_filenames(self, db, header):
        if header.get("output_policy") != FLAT_POLICY:
            raise TaskError("OUTPUT_POLICY_INVALID", "plan")
        try:
            naming = FilenameConfig.from_dict(header["filename_config"])
            expected = FilenameConfig.resolve(header.get("query", {}), template=naming.template,
                                              prefix=naming.prefix)
            if expected != naming:
                raise ValueError()
            digest = hashlib.sha256()
            count = 0
            for row in db.execute("SELECT seq,output_stem,output_key FROM items ORDER BY seq"):
                if row["seq"] != count or row["output_stem"] != naming.stem(count):
                    raise ValueError()
                if row["output_key"] != stem_key(row["output_stem"]):
                    raise ValueError()
                for suffix in (".jpeg", ".json"):
                    safe_output_path(self.directory / "output", row["output_stem"] + suffix)
                digest.update(canonical(list(row)) + b"\n")
                count += 1
            if (count != header["selection_count"]
                    or digest.hexdigest() != header["filename_digest"]):
                raise ValueError()
        except (ValueError, KeyError, TypeError):
            raise TaskError("FILENAME_CONFIG_INVALID", "plan") from None

    def validate_plan(self):
        header = self.meta("header")
        count, digest = selection_digest(
            self.db.execute(
                "SELECT seq,rid,record_id,source,dataset,post_id FROM items ORDER BY seq"
            )
        )
        if (
            count != header.get("selection_count")
            or digest != header.get("selection_digest")
            or plan_digest(header) != self.meta("plan_digest")
        ):
            raise TaskError("PLAN_IDENTITY_MISMATCH", "plan")
        if self.version == 4:
            self._validate_filenames(self.db, header)
        return header

    def request(self, value):
        if value not in ("PAUSE", "CANCEL"):
            raise TaskError("TASK_REQUEST_INVALID")
        with self.transaction() as db:
            self.set_meta(db, "request", value)
        return {"requested": value, "state": self.meta("state")}

    def candidates(self, *, limit):
        if type(limit) is not int or not 1 <= limit <= 12:
            raise TaskError("TASK_IDENTITY_INVALID")
        rows = self.db.execute(
            "SELECT seq,rid,record_id FROM items WHERE state='READY' ORDER BY seq LIMIT ?", (limit,)
        ).fetchall()
        for row in rows:
            if (
                type(row["seq"]) is not int
                or not 0 <= row["seq"] < (1 << 63)
                or type(row["rid"]) is not int
                or not 0 <= row["rid"] < (1 << 63)
                or type(row["record_id"]) is not str
                or len(row["record_id"]) != 32
                or any(c not in "0123456789abcdef" for c in row["record_id"])
            ):
                raise TaskError("TASK_IDENTITY_INVALID")
        return [dict(row) for row in rows]

    def claim(self, *, expected=None, window=1):
        with self.transaction() as db:
            if self.meta("request") is not None:
                return None
            if expected is None:
                row = db.execute(
                    "SELECT * FROM items WHERE state='READY' ORDER BY seq LIMIT 1"
                ).fetchone()
            else:
                if expected not in self.candidates(limit=window):
                    raise TaskError("TASK_IDENTITY_INVALID")
                row = db.execute(
                    "SELECT * FROM items WHERE seq=? AND state='READY'", (expected["seq"],)
                ).fetchone()
            if row is None:
                return None
            operation = uuid.uuid4().hex
            db.execute(
                "UPDATE items SET state='IN_PROGRESS',operation_id=?,phase='CLAIMED',"
                "stage=NULL,receipt=NULL,diagnostic=NULL,code=NULL,published_members='[]' "
                "WHERE seq=? AND state='READY'",
                (operation, row["seq"]),
            )
            return dict(row) | {"operation_id": operation, "phase": "CLAIMED"}

    def recovery_event(self, operation, event, payload):
        self.event(operation, event, payload, recovery=True)

    def event(self, operation, event, payload, *, recovery=False):
        transitions = {
            "NETWORK_START": "CLAIMED",
            "STAGED": "NETWORK_START",
            "CREATED": "STAGED",
            "PREPARED": "STAGED",
            "PUBLISH_INTENT": "PREPARED",
            "MEMBER_PUBLISHED": ("PUBLISH_INTENT", "PARTIAL"),
            "PUBLISHED": ("PUBLISH_INTENT", "PARTIAL"),
        }
        if event not in transitions:
            raise TaskError("ATTEMPT_EVENT_INVALID")
        with self.transaction() as db:
            row = db.execute(
                "SELECT * FROM items WHERE operation_id=? AND state IN "
                "('IN_PROGRESS','FAILED','BLOCKED')", (operation,)
            ).fetchone()
            if row is not None and not recovery and row["state"] != "IN_PROGRESS":
                raise TaskError("ATTEMPT_EVENT_ORDER_INVALID")
            if (recovery and row is not None and event == "PUBLISHED"
                    and row["phase"] == "PUBLISHED"):
                return
            allowed = transitions[event]
            allowed = (allowed,) if type(allowed) is str else allowed
            if row is None or row["phase"] not in allowed:
                raise TaskError("ATTEMPT_EVENT_ORDER_INVALID")
            if event == "STAGED":
                stage = {**payload, "created": {}}
                db.execute(
                    "UPDATE items SET stage=? WHERE seq=?", (canonical(stage).decode(), row["seq"])
                )
            elif event == "CREATED":
                stage = json.loads(row["stage"])
                stage["created"][payload["name"]] = payload["identity"]
                db.execute(
                    "UPDATE items SET stage=? WHERE seq=?", (canonical(stage).decode(), row["seq"])
                )
            elif event == "PREPARED":
                from ..storage.flat_delivery import DeliveryMapping, validate_receipt

                try:
                    config = FilenameConfig.from_dict(self.meta("header")["filename_config"])
                    stem = config.stem(row["seq"])
                    if stem != row["output_stem"]:
                        raise ValueError()
                    proofs = payload["receipt"]
                    image_names = [n for n in proofs
                                   if any(n == stem + ext for ext in self.image_extensions)]
                    if len(image_names) != 1:
                        raise ValueError()
                    mapping = DeliveryMapping(stem, image_names[0][len(stem):],
                                              stem + ".json" in proofs)
                    validate_receipt(payload, mapping)
                    stage = json.loads(row["stage"])
                    if (payload["stage_name"] != stage["name"]
                            or payload["stage_identity"] != stage["identity"]
                            or stage["created"] != {
                                p["staged_name"]: p["identity"] for p in proofs.values()}):
                        raise ValueError()
                except (KeyError, TypeError, ValueError):
                    raise TaskError("OUTPUT_CONFLICT", "publication_fetch") from None
                receipt = {
                    **payload,
                    "task_id": self.meta("task_id"),
                    "plan_digest": self.meta("plan_digest"),
                    "operation_id": operation,
                }
                encoded = canonical(receipt)
                if len(encoded) > 8192:
                    raise TaskError("RECEIPT_LIMIT")
                db.execute("UPDATE items SET receipt=? WHERE seq=?", (encoded.decode(), row["seq"]))
            elif event == "MEMBER_PUBLISHED":
                receipt = json.loads(row["receipt"])
                names = json.loads(row["published_members"])
                if set(payload) != {"name"} or payload["name"] not in receipt["receipt"]:
                    raise TaskError("ATTEMPT_EVENT_INVALID")
                if payload["name"] in names:
                    raise TaskError("ATTEMPT_EVENT_ORDER_INVALID")
                names.append(payload["name"])
                db.execute("UPDATE items SET published_members=?,phase='PARTIAL' WHERE seq=?",
                           (canonical(names).decode(), row["seq"]))
            elif event == "PUBLISHED":
                receipt = json.loads(row["receipt"])
                if set(json.loads(row["published_members"])) != set(receipt["receipt"]):
                    raise TaskError("ATTEMPT_EVENT_ORDER_INVALID")
                db.execute("UPDATE items SET delivery='PUBLISHED' WHERE seq=?", (row["seq"],))
            if event not in ("CREATED", "MEMBER_PUBLISHED"):
                db.execute("UPDATE items SET phase=? WHERE seq=?", (event, row["seq"]))

    def finish_item(
        self, seq, *, state, code=None, diagnostic=None, operation=None, recovery_retry=False
    ):
        if state not in ("READY", "DONE", "FAILED", "BLOCKED"):
            raise TaskError("TASK_STATE_INVALID")
        details = safe_failure(diagnostic or {})
        with self.transaction() as db:
            row = db.execute("SELECT * FROM items WHERE seq=?", (seq,)).fetchone()
            if row is None or (operation is not None and row["operation_id"] != operation):
                raise TaskError("ATTEMPT_IDENTITY_MISSING")
            if recovery_retry:
                retries = row["recovery_retries"]
                if state != "READY" or type(retries) is not int or not 0 <= retries < 2:
                    raise TaskError("RECOVERY_RETRY_LIMIT", "recovery")
                db.execute(
                    "UPDATE items SET recovery_retries=recovery_retries+1 WHERE seq=?", (seq,)
                )
            if state in ("FAILED", "BLOCKED"):
                phase = row["phase"]
                details["operation_phase"] = (
                    phase
                    if phase in {"CLAIMED", "NETWORK_START", "STAGED", "PREPARED",
                                 "PUBLISH_INTENT", "PARTIAL", "PUBLISHED"}
                    else "CLAIMED"
                )
            db.execute(
                "UPDATE items SET state=?,code=?,diagnostic=?,delivery=? WHERE seq=?",
                (
                    state,
                    code,
                    canonical(details).decode() if diagnostic is not None else None,
                    "VERIFIED" if state == "DONE" else row["delivery"],
                    seq,
                ),
            )
            if state in ("FAILED", "BLOCKED"):
                self.set_meta(db, "state", "FAILED")

    def failure_diagnostic(self, seq):
        if type(seq) is not int or not 0 <= seq < (1 << 63):
            raise TaskError("ATTEMPT_IDENTITY_INVALID")
        if self.version not in (3, 4):
            return None
        row = self.db.execute(
            "SELECT CASE WHEN typeof(diagnostic)='text' AND "
            "length(CAST(diagnostic AS BLOB))<=8192 THEN diagnostic ELSE NULL END "
            "FROM items WHERE seq=?",
            (seq,),
        ).fetchone()
        if row is None or row[0] is None:
            return None
        try:
            value = json.loads(row[0])
            if value != safe_failure(value):
                raise ValueError()
            return value
        except (ValueError, TypeError, RecursionError):
            raise TaskError("ATTEMPT_DIAGNOSTIC_INVALID") from None

    def inspect(self):
        group = "state,delivery" + (",accounting" if self.version < 3 else "")
        stats = [
            dict(row)
            for row in self.db.execute(
                f"SELECT {group},count(*) AS count FROM items GROUP BY {group}"
            )
        ]
        result = {
            "format": self.meta("header")["format"],
            "settings_version": self.settings_version,
            "image_extensions": list(self.image_extensions),
            "requested_count": self.meta("header")["selection_count"],
            "task_id": self.meta("task_id"),
            "plan_digest": self.meta("plan_digest"),
            "state": self.meta("state"),
            "request": self.meta("request"),
            "header": self.meta("header"),
            "items": stats,
            "error_count": self.db.execute(
                "SELECT count(*) FROM items WHERE code IS NOT NULL"
            ).fetchone()[0],
        }
        if self.version in (3, 4):
            result["read_only_archive"] = self.version == 3
            result["migration_required"] = self.version == 3
            result["delivered_verified"] = self.db.execute(
                "SELECT count(*) FROM items WHERE state='DONE' AND delivery='VERIFIED'"
            ).fetchone()[0]
        else:
            result["read_only_legacy"] = True
            result["migration_required"] = True
            result["delivered_confirmed"] = self.db.execute(
                "SELECT count(*) FROM items WHERE accounting='CONFIRMED' AND delivery='PUBLISHED'"
            ).fetchone()[0]
            result["unknown_accounting_count"] = self.db.execute(
                "SELECT count(*) FROM items WHERE accounting='UNKNOWN'"
            ).fetchone()[0]
        return result

    @contextmanager
    def runner_lock(self):
        if self.version != 4 or self.readonly:
            raise TaskError("TASKDB_READONLY")
        lock = plain_entry(self.directory / "runner.lock").open("r+b")
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
            lock.seek(0)
            if lock.read() != b"T":
                raise TaskError("RUNNER_LOCK_INVALID")
            try:
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
