"""Explicit foreground task owner and inherited-pipe command clients."""

import hashlib
import json
import math
import multiprocessing
import os
import queue
import re
import sys
import threading
import time
from pathlib import Path

from ..fs_safety import plain_entry
from ..workspace import Workspace
from . import runner
from .plan import Selection
from .profile import _validate_profile, connect_profile, validate_allowlist
from .store import TaskDB, TaskError
from .verified_session import _VerifiedPublication
from .windows_pins import _PinUnavailable

FRAME_BYTES = 65536
SUMMARY_BYTES = 4096


def _invalid():
    return TaskError("SESSION_COMMAND_INVALID", "session")


def decode_frame(raw):
    """Bound allocations before JSON decoding, then validate every scalar/container."""
    if type(raw) is not bytes or not raw or len(raw) > FRAME_BYTES:
        raise _invalid()
    depth, quoted, escaped = 0, False, False
    for byte in raw:
        if quoted:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                quoted = False
        elif byte == 34:
            quoted = True
        elif byte in (91, 123):
            depth += 1
            if depth > 16:
                raise _invalid()
        elif byte in (93, 125):
            depth -= 1
            if depth < 0:
                raise _invalid()
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise _invalid()
            result[key] = value
        return result
    def integer(value):
        if len(value) > 20:
            raise _invalid()
        return int(value)
    def constant(_):
        raise _invalid()
    try:
        value = json.loads(raw, object_pairs_hook=pairs, parse_int=integer,
                           parse_constant=constant)
        pending, count = [value], 0
        while pending:
            item = pending.pop()
            count += 1
            if count > 4096:
                raise _invalid()
            if type(item) is dict:
                pending.extend(item.keys())
                pending.extend(item.values())
            elif type(item) is list:
                pending.extend(item)
            elif type(item) is str and len(item) > 4096:
                raise _invalid()
            elif type(item) is float and not math.isfinite(item):
                raise _invalid()
        if type(value) is not dict:
            raise _invalid()
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise _invalid() from None


def _encode(value, limit=FRAME_BYTES):
    try:
        raw = json.dumps(value, separators=(",", ":"), allow_nan=False).encode()
    except (ValueError, TypeError, RecursionError):
        raise _invalid() from None
    if len(raw) > limit:
        raise _invalid()
    return raw


def _manifest_identity(root):
    with plain_entry(root / "PUBLICATION.json").open("rb") as stream:
        raw = stream.read((1 << 20) + 1)
    if len(raw) > 1 << 20:
        raise TaskError("SESSION_IDENTITY_INVALID", "session")
    return hashlib.sha256(raw).digest()


def _signature(path):
    info = plain_entry(path).stat()
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


class TaskSession:
    """One PID/thread owns verification, native initialization and serial task locks."""

    def __init__(self, publication, workspace, connection_profile, *, workers=6,
                 workers_per_tar=6):
        runner._run_options(None, connection_profile, workers, workers_per_tar)
        self.owner = os.getpid(), threading.get_ident()
        self.workspace = Workspace.open(getattr(workspace, "root", workspace))
        if not self.workspace.lightweight:
            raise TaskError("LEGACY_TASK_MIGRATION_REQUIRED", "session")
        self.root = plain_entry(publication, directory=True)
        if self.workspace.root.is_relative_to(self.root):
            raise TaskError("TASK_PUBLICATION_CONTAINMENT", "session")
        self.capacity = self.workspace.capacity
        self._manifest = _manifest_identity(self.root)
        self._profile_bytes = _encode(connection_profile)
        self._profile = decode_frame(self._profile_bytes)
        _validate_profile(self._profile)
        self._worker = plain_entry(self._profile["worker"])
        self._worker_identity = _signature(self._worker)
        self.workers, self.workers_per_tar = workers, workers_per_tar
        self._scope = self.root, self.workspace, self.capacity, workers, workers_per_tar
        self._next = 1
        self._closed = self._busy = False
        self._verified = self._transport = None
        self.mode = "full_each_command"
        try:
            if runner._supports_live_verification():
                try:
                    self._verified = _VerifiedPublication(self.root, self.capacity,
                                                          runner.load_publication)
                except _PinUnavailable:
                    pass
            if self._verified is not None:
                if self._verified._pins is not None:
                    try:
                        self._verified._pins._add(self._worker, False)
                        self._verified._pins.check()
                    except _PinUnavailable:
                        self._verified._close()
                        self._verified = None
                if self._verified is not None:
                    validate_allowlist(
                        self._profile, self._verified.check(self.capacity, self.root))
                    self.mode = self._verified.mode
            self._check()
        except BaseException as primary:
            self._close(primary)
            raise

    def _check(self):
        if (self._closed or self.owner != (os.getpid(), threading.get_ident())
                or (self.root, self.workspace, self.capacity, self.workers, self.workers_per_tar)
                   != self._scope
                or _manifest_identity(self.root) != self._manifest
                or self.workspace != Workspace.open(self.workspace.root)
                or _signature(self._worker) != self._worker_identity
                or _encode(self._profile) != self._profile_bytes):
            raise TaskError("SESSION_IDENTITY_INVALID", "session")
        if self._verified is not None:
            self._verified.check(self.capacity, self.root)
        if self._transport is not None:
            worker = self._transport._lane_worker
            pool = self._transport._retained_pool
            if (self._transport._closed or worker is None or not worker._alive
                    or worker._proc is None or worker._proc.poll() is not None
                    or pool is not None and pool._failed.is_set()):
                raise TaskError("SESSION_WORKER_FAILED", "session")

    def status(self):
        self._check()
        return {"state": "READY", "next_id": self._next, "owner_pid": self.owner[0],
                "verification_reuse": self.mode,
                "native_process_reuse": self._verified is not None}

    def _connection(self, task, profile):
        """Called only after full/session verification and allowlist under runner lock."""
        self._check()
        if (self._verified is None or task.workspace != self.workspace
                or task.capacity != self.capacity or _encode(profile) != self._profile_bytes):
            raise TaskError("SESSION_IDENTITY_INVALID", "preflight")
        if self._transport is None:
            self._transport = connect_profile(profile, root=self.workspace.root,
                                              capacity=self.capacity)
            self._transport._retained_limit = self.workers
        self._check()
        return self._transport

    def execute(self, command):
        """Validate again even for direct API calls; every accepted command consumes one ID."""
        try:
            self._check()
            if self._busy:
                raise _invalid()
            command = decode_frame(_encode(command))
            identity, action = command.get("id"), command.get("action")
            if type(identity) is not int or identity != self._next or identity >= 1 << 63:
                raise _invalid()
            allowed = {"id", "action"}
            if action in ("start", "run", "resume"):
                allowed.add("task")
            if action == "start":
                allowed.update(("query", "selection", "metadata", "image_extensions",
                                "filename_template", "filename_prefix"))
            if action not in ("start", "run", "resume", "exit", "detach") or set(command) - allowed:
                raise _invalid()
            self._next += 1
            if action in ("exit", "detach"):
                if action == "exit":
                    self.close()
                return {"id": identity, "ok": True, "state": action.upper()}
            name = command.get("task")
            if (type(name) is not str
                    or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", name) is None):
                raise _invalid()
            directory = self.workspace.task_path(self.workspace.tasks / name)
            if directory.is_relative_to(self.root):
                raise TaskError("TASK_PUBLICATION_CONTAINMENT", "session")
            self._busy = True
            if action == "start":
                from ..cli import _spec_from_dict

                selection = command.get("selection", {"mode": "all"})
                if (type(selection) is not dict
                        or set(selection) - {"mode", "limit", "seed", "records"}
                        or type(selection.get("records", [])) is not list
                        or type(command.get("metadata", False)) is not bool):
                    raise _invalid()
                selected = Selection(selection.get("mode", "all"), selection.get("limit"),
                                     selection.get("seed"), tuple(selection.get("records", [])))
                query = _spec_from_dict(command.get("query", {}))
                options = runner._create_options(directory, self.workspace, query, selected,
                                                 command.get("image_extensions"))
                output = (command.get("metadata", False),
                          command.get("filename_template", "{tag}_{index}"),
                          command.get("filename_prefix"))
                if self._verified is None:
                    with runner.create_task(self.root, directory, self.workspace, query, selected,
                            metadata=output[0], filename_template=output[1],
                            filename_prefix=output[2],
                            image_extensions=command.get("image_extensions")):
                        pass
                else:
                    pub = self._verified.check(self.capacity, self.root)
                    with runner._create_verified(pub, self.root, directory, self.workspace,
                                                 query, options, *output):
                        pass
            else:
                with TaskDB(directory, readonly=True, workspace=self.workspace) as task:
                    task.validate_plan()
                    if Path(task.meta("publication_path")).absolute() != self.root:
                        raise TaskError("SESSION_IDENTITY_INVALID", "preflight")
            result = runner._run_task(directory, resume=action == "resume",
                connection_profile=self._profile, workers=self.workers,
                workers_per_tar=self.workers_per_tar, _session=self._verified,
                _transport_owner=self if self._verified is not None else None)
            self._check()
            summary = {k: result[k] for k in ("state", "requested_count", "delivered_verified",
                                              "error_count")}
            response = {"id": identity, "ok": True, "result": summary,
                        "verification_reuse": self.mode, "owner_pid": self.owner[0]}
            _encode(response, SUMMARY_BYTES)
            return response
        except BaseException as primary:
            self._close(primary)
            raise
        finally:
            self._busy = False

    def _close(self, primary=None, *, deadline=None):
        if self.owner != (os.getpid(), threading.get_ident()):
            if primary is None:
                raise TaskError("SESSION_IDENTITY_INVALID", "session")
            return
        self._closed = True
        failure = None
        if self._transport is not None:
            self._transport._close_deadline = min(
                self._transport._close_deadline or float("inf"), deadline or time.monotonic() + 5)
        for resource in (self._transport, self._verified):
            if resource is not None:
                try:
                    resource.close() if resource is self._transport else resource._close()
                except BaseException as error:
                    if failure is None:
                        failure = error
        if failure is not None:
            if primary is None:
                raise failure
            primary.task_secondary = (*getattr(primary, "task_secondary", ()),
                                      "SESSION_CLOSE_FAILED")

    def close(self):
        if self.owner != (os.getpid(), threading.get_ident()):
            raise TaskError("SESSION_IDENTITY_INVALID", "session")
        self._close()

    def __enter__(self):
        self._check()
        return self

    def __exit__(self, kind, primary, traceback):
        self._close(primary)

    def serve_child(self, target, args=()):
        """Run explicitly supplied child client code; commands remain bytes-only IPC."""
        self._check()
        context = multiprocessing.get_context("spawn")
        owner, child = context.Pipe(duplex=True)
        process = context.Process(target=_child_main, args=(child, target, args))
        threads = []
        deadline = None
        try:
            process.start()
            child.close()
            ready = _encode(self.status(), SUMMARY_BYTES)
            _pipe_call(lambda: owner.send_bytes(ready), process, threads, 5)
            while True:
                raw = _pipe_call(lambda: owner.recv_bytes(FRAME_BYTES), process, threads, 30)
                command = decode_frame(raw)
                result = self.execute(command)
                _pipe_call(lambda: owner.send_bytes(_encode(result, SUMMARY_BYTES)),
                           process, threads, 5)
                if command["action"] in ("detach", "exit"):
                    break
            deadline = time.monotonic() + 5
            process.join(min(1, max(0, deadline - time.monotonic())))
            if process.is_alive() or process.exitcode != 0:
                raise TaskError("SESSION_CLIENT_FAILED", "session")
        except BaseException as primary:
            deadline = deadline or time.monotonic() + 5
            if process.pid is not None and process.is_alive():
                try:
                    process.terminate()
                except BaseException:
                    primary.task_secondary = (*getattr(primary, "task_secondary", ()),
                                              "SESSION_CLIENT_CLOSE_FAILED")
            self._close(primary, deadline=deadline)
            raise
        finally:
            primary = sys.exc_info()[1]
            deadline = deadline or time.monotonic() + 5
            failed = False
            if process.pid is not None:
                if process.is_alive():
                    try:
                        process.terminate()
                    except BaseException:
                        failed = True
                try:
                    process.join(max(0, deadline - time.monotonic()))
                except BaseException:
                    failed = True
                if process.is_alive():
                    try:
                        process.kill()
                        process.join(max(0, deadline - time.monotonic()))
                    except BaseException:
                        failed = True
                failed |= process.is_alive()
            for connection in (owner, child):
                try:
                    connection.close()
                except BaseException:
                    failed = True
            for thread in threads:
                if thread.ident is not None:
                    try:
                        thread.join(max(0, deadline - time.monotonic()))
                    except BaseException:
                        failed = True
                failed |= thread.is_alive()
            if failed:
                if primary is not None:
                    primary.task_secondary = (*getattr(primary, "task_secondary", ()),
                                              "SESSION_CLIENT_CLOSE_FAILED")
                else:
                    failure = TaskError("SESSION_CLIENT_CLOSE_FAILED", "finalize")
                    self._close(failure, deadline=deadline)
                    raise failure


def _pipe_call(operation, process, threads, seconds):
    result = queue.Queue(maxsize=1)
    def invoke():
        try:
            result.put((True, operation()))
        except BaseException:
            result.put((False, None))
    thread = threading.Thread(target=invoke, daemon=True, name="sakura-session-ipc")
    # Only the current transfer can be outstanding; completed threads are discarded.
    threads[:] = [t for t in threads if t.is_alive()]
    threads.append(thread)
    thread.start()
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            ok, value = result.get(timeout=min(.1, max(0, deadline - time.monotonic())))
            thread.join(max(0, deadline - time.monotonic()))
            if not ok or thread.is_alive():
                break
            return value
        except queue.Empty:
            if process.exitcode is not None:
                break
    raise TaskError("SESSION_CLIENT_FAILED", "session")


class SessionClient:
    """Only an inherited private connection, never a publication verification token."""

    def __init__(self, connection):
        self.connection = connection
        self.owner = os.getpid(), threading.get_ident()
        ready = decode_frame(connection.recv_bytes(SUMMARY_BYTES))
        if (set(ready) != {"state", "next_id", "owner_pid", "verification_reuse",
                           "native_process_reuse"} or ready["state"] != "READY"
                or type(ready["next_id"]) is not int or not 1 <= ready["next_id"] < 1 << 63
                or type(ready["owner_pid"]) is not int or ready["owner_pid"] < 1
                or ready["verification_reuse"] not in
                   ("posix_live", "windows_pinned", "full_each_command")
                or type(ready["native_process_reuse"]) is not bool):
            raise _invalid()
        self.next_id = ready["next_id"]
        self.owner_pid = ready["owner_pid"]
        self.mode = ready["verification_reuse"]
        self.closed = False

    def request(self, action, **fields):
        if self.closed or self.owner != (os.getpid(), threading.get_ident()):
            raise _invalid()
        command = {"id": self.next_id, "action": action, **fields}
        self.connection.send_bytes(_encode(command))
        response = decode_frame(self.connection.recv_bytes(SUMMARY_BYTES))
        if (type(response.get("id")) is not int or response["id"] != self.next_id
                or response.get("ok") is not True):
            raise _invalid()
        if action in ("detach", "exit"):
            if set(response) != {"id", "ok", "state"} or response["state"] != action.upper():
                raise _invalid()
        else:
            if (set(response) != {"id", "ok", "result", "verification_reuse", "owner_pid"}
                    or response["verification_reuse"] != self.mode
                    or type(response["owner_pid"]) is not int
                    or response["owner_pid"] != self.owner_pid
                    or type(response["result"]) is not dict):
                raise _invalid()
            summary = response["result"]
            if (set(summary) != {"state", "requested_count", "delivered_verified", "error_count"}
                    or summary["state"] not in ("COMPLETED", "PAUSED", "CANCELLED")
                    or any(type(summary[key]) is not int or summary[key] < 0
                           for key in ("requested_count", "delivered_verified", "error_count"))):
                raise _invalid()
        self.next_id += 1
        self.closed = action in ("detach", "exit")
        return response


def _child_main(connection, target, args):
    try:
        client = SessionClient(connection)
        target(client, *args)
        if not client.closed:
            client.request("detach")
    except BaseException:
        # Never print arbitrary exception text, frames or request fields to inherited stderr.
        raise SystemExit(2) from None
    finally:
        connection.close()


def command_loop(session, source, destination):
    try:
        destination.write(_encode(session.status(), SUMMARY_BYTES).decode() + "\n")
        destination.flush()
        while not session._closed:
            raw = source.readline(FRAME_BYTES + 1)
            if not raw:
                break
            response = session.execute(decode_frame(raw))
            destination.write(_encode(response, SUMMARY_BYTES).decode() + "\n")
            destination.flush()
        return 0
    finally:
        session._close(sys.exc_info()[1])
