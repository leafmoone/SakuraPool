"""Subprocess bridge for the version-1 NDJSON Rust worker protocol.

Python owns durable budget reservations. Clean worker rejection settles the
lease; crash-class failures leave it pending. Requests and control responses
are limited to 64 KiB; successful TAR manifests may occupy up to 64 MiB and
contain at most 100,000 members. stdout is framed in bounded chunks, stderr
is continuously drained into a bounded tail, and teardown has its own timeout.
"""

from __future__ import annotations

import json
import queue
import subprocess
import threading
import time
import uuid
from pathlib import Path

from .budget import Reservation

PROTOCOL_VERSION = 1
MAX_LINE_BYTES = 64 * 1024
MAX_SCAN_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_SCAN_MEMBERS = 100_000
_STDERR_TAIL_BYTES = 64 * 1024
_STDOUT_QUEUE_LINES = 8
_QUEUE_WAIT_S = 0.05
_TIMEOUT_S = 60.0
_SHUTDOWN_TIMEOUT_S = 2.0
_REJECTED = "worker rejected request"
_ZERO_BUDGET = {"body": 0, "disk": 0, "inflight": 0, "attempts": 0}


class RustWorkerError(RuntimeError):
    """Static failure surface for the Rust worker bridge."""


def _normalize_budget(budget: dict | None) -> dict:
    if budget is None:
        return dict(_ZERO_BUDGET)
    values = {}
    for name in ("body", "disk", "inflight", "attempts"):
        value = budget.get(name, 0)
        if type(value) is not int or value < 0:
            raise RustWorkerError("worker budget must be nonnegative ints")
        values[name] = value
    if set(budget) != set(values):
        raise RustWorkerError("worker budget has unknown fields")
    return values


class RustWorker:
    """One spawned worker speaking the version-1 NDJSON control protocol."""

    def __init__(
        self,
        binary: Path | str,
        *,
        job_budget: dict | None = None,
        timeout_s: float = _TIMEOUT_S,
    ) -> None:
        binary = Path(binary)
        if not binary.is_file():
            raise RustWorkerError("worker binary missing")
        self.binary = binary
        self.timeout_s = timeout_s
        self.capabilities: tuple[str, ...] = ()
        self.worker_version: str = ""
        normalized_budget = _normalize_budget(job_budget)
        self._proc = subprocess.Popen(
            [str(binary)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,  # no buffered pipe lock held by a blocked operation
        )
        self._lines: queue.Queue[bytes] = queue.Queue(maxsize=_STDOUT_QUEUE_LINES)
        self._response_line_bytes = MAX_LINE_BYTES
        self._stop = threading.Event()
        self._reader_done = threading.Event()
        self._stdout_error: str | None = None
        self._lifecycle_lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._writing = threading.Event()
        self._stderr_tail = bytearray()
        self._stderr_lock = threading.Lock()
        self._alive = True
        self._reader = threading.Thread(target=self._drain_stdout, daemon=True)
        self._stderr_reader = threading.Thread(target=self._drain_stderr, daemon=True)
        self._reader.start()
        self._stderr_reader.start()
        try:
            self._handshake(normalized_budget)
        except BaseException:
            self.cancel()  # constructor failures must not leak a child or threads
            raise

    # -- transport ------------------------------------------------------

    def _drain_stdout(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        proc = self._proc
        pipe = proc.stdout
        pending = bytearray()
        try:
            while True:
                chunk = pipe.read(MAX_LINE_BYTES)
                if not chunk:
                    if pending and not self._stop.is_set():
                        self._stdout_error = "worker returned invalid json"
                    break
                if self._stop.is_set():
                    # Drain discarded output in fixed chunks during graceful close.
                    pending.clear()
                    continue
                start = 0
                while start < len(chunk):
                    newline = chunk.find(b"\n", start)
                    end = len(chunk) if newline < 0 else newline + 1
                    if len(pending) + end - start > self._response_line_bytes:
                        self._stdout_error = "worker response line too long"
                        # Never abandon a blocked writer alive on an undrained
                        # pipe, nor wait for its newline (it might never arrive).
                        try:
                            proc.kill()
                        except OSError:
                            pass
                        return
                    pending.extend(chunk[start:end])
                    start = end
                    if newline < 0:
                        break
                    raw = bytes(pending)
                    pending.clear()
                    while not self._stop.is_set():
                        try:
                            self._lines.put(raw, timeout=_QUEUE_WAIT_S)
                            break
                        except queue.Full:
                            continue  # bounded queue provides backpressure
        except (OSError, ValueError):
            if not self._stop.is_set():
                self._stdout_error = "worker pipe failure"
        finally:
            self._reader_done.set()

    def _drain_stderr(self) -> None:
        assert self._proc is not None and self._proc.stderr is not None
        pipe = self._proc.stderr
        try:
            while True:
                chunk = pipe.read(4096)
                if not chunk:
                    break
                self._append_stderr(chunk)
        except (OSError, ValueError):
            pass

    def _append_stderr(self, chunk: bytes) -> None:
        with self._stderr_lock:
            self._stderr_tail.extend(chunk)
            if len(self._stderr_tail) > _STDERR_TAIL_BYTES:
                del self._stderr_tail[: len(self._stderr_tail) - _STDERR_TAIL_BYTES]

    def stderr_tail(self) -> bytes:
        with self._stderr_lock:
            return bytes(self._stderr_tail)

    def _send(self, line: bytes) -> None:
        if len(line) > MAX_LINE_BYTES:
            raise RustWorkerError("worker request line too long")
        with self._send_lock:
            with self._lifecycle_lock:
                if not self._alive or self._proc is None or self._proc.stdin is None:
                    raise RustWorkerError("worker is closed")
                pipe = self._proc.stdin
                self._writing.set()
            try:
                view = memoryview(line)
                while view:
                    count = pipe.write(view)
                    if not count:
                        raise OSError("short pipe write")
                    view = view[count:]
                pipe.flush()
            except (OSError, ValueError) as exc:
                self._alive = False
                raise RustWorkerError("worker pipe failure") from exc
            finally:
                self._writing.clear()

    def _receive(self, *, max_line_bytes: int = MAX_LINE_BYTES) -> dict:
        deadline = time.monotonic() + self.timeout_s
        while True:
            if self._stop.is_set():
                raise RustWorkerError("worker is closed")
            try:
                wait = min(_QUEUE_WAIT_S, max(0, deadline - time.monotonic()))
                raw = self._lines.get(timeout=wait)
                break
            except queue.Empty:
                if self._stop.is_set():
                    raise RustWorkerError("worker is closed")
                if self._reader_done.is_set():
                    try:
                        raw = self._lines.get_nowait()
                        break
                    except queue.Empty:
                        raise RustWorkerError(self._stdout_error or "worker exited") from None
                if time.monotonic() >= deadline:
                    raise RustWorkerError("worker timed out")
        if len(raw) > max_line_bytes:
            raise RustWorkerError("worker response line too long")
        try:
            message = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError, RecursionError) as exc:
            raise RustWorkerError("worker returned invalid json") from exc
        if not isinstance(message, dict):
            raise RustWorkerError("worker returned invalid json")
        # A scan request permits a large successful report, not a large error
        # or other control message. The transport remains bounded either way.
        if len(raw) > MAX_LINE_BYTES and not (
            message.get("type") == "response" and message.get("ok") is True
        ):
            raise RustWorkerError("worker response line too long")
        return message

    def _handshake(self, job_budget: dict) -> None:
        hello = {
            "type": "hello",
            "protocol_version": PROTOCOL_VERSION,
            "budget": job_budget,
        }
        self._send((json.dumps(hello) + "\n").encode("utf-8"))
        message = self._receive()
        if message.get("type") != "ready" or message.get("protocol_version") != PROTOCOL_VERSION:
            self.cancel()
            raise RustWorkerError("worker handshake failed")
        self.worker_version = str(message.get("worker_version", ""))
        capabilities = message.get("capabilities")
        if not isinstance(capabilities, list) or not all(
            isinstance(item, str) for item in capabilities
        ):
            self.cancel()
            raise RustWorkerError("worker handshake failed")
        self.capabilities = tuple(capabilities)

    # -- request API -----------------------------------------------------

    def request(
        self,
        operation: str,
        *,
        budget: dict | None = None,
        payload: dict | None = None,
    ) -> dict:
        """Return a result; only clean rejection is refundable by callers.

        TAR scan manifests have a separate bounded response allowance. This
        synchronous protocol permits one outstanding request per worker.
        """
        scan = operation in ("scan_tar", "scan_http_tar")
        member_limit = MAX_SCAN_MEMBERS
        if scan and payload is not None:
            member_limit = payload.get("max_members", MAX_SCAN_MEMBERS)
            if type(member_limit) is not int or not 0 <= member_limit <= MAX_SCAN_MEMBERS:
                raise RustWorkerError("worker scan member limit invalid")
        request = {
            "type": "request",
            "request_id": uuid.uuid4().hex,
            "operation": operation,
            "budget": _normalize_budget(budget),
            "payload": payload if payload is not None else {},
        }
        response_limit = MAX_SCAN_RESPONSE_BYTES if scan else MAX_LINE_BYTES
        self._response_line_bytes = response_limit
        try:
            self._send((json.dumps(request) + "\n").encode("utf-8"))
            message = self._receive(max_line_bytes=response_limit)
        finally:
            self._response_line_bytes = MAX_LINE_BYTES
        if message.get("type") == "protocol_error":
            raise RustWorkerError("worker protocol error")
        if (
            message.get("type") != "response"
            or message.get("request_id") != request["request_id"]
        ):
            raise RustWorkerError("worker returned invalid json")
        if not bool(message.get("ok")):
            raise RustWorkerError(_REJECTED)
        result = message.get("result")
        if not isinstance(result, dict):
            raise RustWorkerError("worker returned invalid json")
        if scan:
            members = result.get("members")
            if not isinstance(members, list) or len(members) > member_limit:
                raise RustWorkerError("worker returned invalid json")
        return result

    def send_raw(self, line: bytes) -> None:
        """Protocol-level send (audit/tests); bypasses the request builder."""
        self._send(line)

    def read_raw(self) -> dict:
        """Protocol-level receive (audit/tests), with the control response cap."""
        return self._receive()

    # -- budget-gated fetch ----------------------------------------------

    def fetch_range_gated(
        self,
        url: str,
        start: int,
        end: int,
        total: int,
        *,
        ledger: object,
        disk_reserve: int = 0,
        ipc_reserve: int = 4096,
    ) -> dict:
        """Reserve durably before fetching; crash-class failures stay pending."""
        if not 0 <= start <= end < total:
            raise RustWorkerError("range outside resource")
        body = end - start + 1
        lease = ledger.reserve(  # type: ignore[attr-defined]
            Reservation(body=body, disk=disk_reserve, inflight=body + ipc_reserve, attempt=True)
        )
        try:
            result = self.request(
                "fetch_range",
                budget={
                    "body": body,
                    "disk": disk_reserve,
                    "inflight": body + ipc_reserve,
                    "attempts": 1,
                },
                payload={"url": url, "start": start, "end": end, "total": total},
            )
        except RustWorkerError as error:
            if str(error) == _REJECTED:
                ledger.settle(lease)  # type: ignore[attr-defined]
            raise
        actual = result.get("bytes")
        if type(actual) is not int or not 0 < actual <= body:
            raise RustWorkerError("worker returned invalid json")
        ledger.consume_body(lease, actual)  # type: ignore[attr-defined]
        ledger.settle(lease)  # type: ignore[attr-defined]
        return result

    # -- lifecycle ---------------------------------------------------------

    @property
    def pid(self) -> int | None:
        return self._proc.pid if self._proc is not None else None

    def cancel(self) -> None:
        """Kill, reap, join BOTH pipe readers and close all handles."""
        self._shutdown(kill=True)

    def close(self) -> None:
        """Drain pipes and wait briefly, then kill; independent of scan timeout."""
        self._shutdown(kill=False)

    def _shutdown(self, *, kill: bool) -> None:
        with self._lifecycle_lock:
            proc = self._proc
            if proc is None:
                return
            self._alive = False
            self._stop.set()
            # Kill+wait before closing stdin when a Windows WriteFile is
            # blocked; closing that handle first can itself block forever.
            if kill or self._writing.is_set():
                try:
                    proc.kill()
                except OSError:
                    pass
                proc.wait(timeout=_SHUTDOWN_TIMEOUT_S)
            if proc.stdin is not None:
                try:
                    proc.stdin.close()
                except (OSError, ValueError):
                    pass
            try:
                proc.wait(timeout=_SHUTDOWN_TIMEOUT_S)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=_SHUTDOWN_TIMEOUT_S)
            for reader in (self._reader, self._stderr_reader):
                reader.join(timeout=_SHUTDOWN_TIMEOUT_S)
            if self._reader.is_alive() or self._stderr_reader.is_alive():
                raise RustWorkerError("worker pipe reader failed to exit")
            for pipe in (proc.stdin, proc.stdout, proc.stderr):
                if pipe is not None:
                    pipe.close()
            self._proc = None

    def __enter__(self) -> RustWorker:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
