"""Offline bridge from Python to the ``sakurapool-worker`` Rust binary.

Formal NDJSON control protocol (version 1): a ``hello`` handshake carrying
the job budget, then one ``request`` per line with an operation, a
per-request budget and a payload. The worker's budget counters are
in-memory only; the durable reservation always happens here, on the Python
side, against the BudgetLedger BEFORE any worker request. A clean worker
rejection settles the lease (the attempt stays charged); a crash-class
failure (timeout, pipe failure, protocol violation, process death) leaves
the lease pending, i.e. never refunded.

The worker binary path is explicit; there is no search or fallback. stdout
carries protocol messages only; stderr is drained continuously and kept in
a bounded tail.
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
_STDERR_TAIL_BYTES = 64 * 1024
_STDOUT_QUEUE_LINES = 8
_QUEUE_WAIT_S = 0.05
_TIMEOUT_S = 60.0
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
            bufsize=0,  # no BufferedReader/Writer lock held by a blocked pipe operation
        )
        self._lines: queue.Queue[bytes] = queue.Queue(maxsize=_STDOUT_QUEUE_LINES)
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
            self.cancel()  # constructor failures must not leak a child or pipe threads
            raise

    # -- transport ------------------------------------------------------

    def _drain_stdout(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        pipe = self._proc.stdout
        try:
            while True:
                raw = pipe.readline(MAX_LINE_BYTES + 1)
                if not raw:
                    break
                if self._stop.is_set():
                    # Shutdown abandons queued delivery, but drains the pipe so a
                    # finite writer can exit gracefully instead of deadlocking.
                    continue
                if len(raw) > MAX_LINE_BYTES:
                    self._stdout_error = "worker response line too long"
                    break
                while not self._stop.is_set():
                    try:
                        self._lines.put(raw, timeout=_QUEUE_WAIT_S)
                        break
                    except queue.Full:
                        continue  # real backpressure: do not read another line
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
            pass  # a concurrently closing pipe has no more diagnostics

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

    def _receive(self) -> dict:
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
                    # Producer may have enqueued its final line between our
                    # timeout and publishing done. Drain it before reporting EOF.
                    try:
                        raw = self._lines.get_nowait()
                        break
                    except queue.Empty:
                        raise RustWorkerError(self._stdout_error or "worker exited") from None
                if time.monotonic() >= deadline:
                    raise RustWorkerError("worker timed out")
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RustWorkerError("worker returned invalid json") from exc
        try:
            message = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RustWorkerError("worker returned invalid json") from exc
        if not isinstance(message, dict):
            raise RustWorkerError("worker returned invalid json")
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
        """Send one request and return its ``result`` object.

        A clean worker rejection raises :class:`RustWorkerError` with the
        static message ``worker rejected request``. Timeout, pipe failure
        and malformed replies raise with other static messages (crash
        class); callers with a durable reservation must then leave it
        pending.
        """
        request = {
            "type": "request",
            "request_id": uuid.uuid4().hex,
            "operation": operation,
            "budget": _normalize_budget(budget),
            "payload": payload if payload is not None else {},
        }
        self._send((json.dumps(request) + "\n").encode("utf-8"))
        message = self._receive()
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
        return result

    def send_raw(self, line: bytes) -> None:
        """Protocol-level send (audit/tests); bypasses the request builder."""
        self._send(line)

    def read_raw(self) -> dict:
        """Protocol-level receive (audit/tests)."""
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
        """Loopback range fetch with a durable pre-request reservation.

        The Python BudgetLedger is the only durable budget authority: the
        reservation (body + disk + inflight + one attempt) is persisted
        before any worker request. Clean worker rejection settles the lease
        (attempt stays charged, remainder released). Crash-class failures
        (timeout, pipe failure, protocol error, death) leave the lease
        pending: nothing is refunded.
        """
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
        """Stop queue delivery, drain pipes, close stdin, wait; kill on timeout."""
        self._shutdown(kill=False)

    def _shutdown(self, *, kill: bool) -> None:
        with self._lifecycle_lock:
            proc = self._proc
            if proc is None:
                return
            self._alive = False
            self._stop.set()  # wakes a producer blocked on the bounded queue
            # Closing a pipe while another thread is blocked in WriteFile can
            # itself block forever on Windows. Kill+wait FIRST in that case;
            # the pipe failure releases the writer before any handle is closed.
            if kill or self._writing.is_set():
                try:
                    proc.kill()
                except OSError:
                    pass
                proc.wait(timeout=self.timeout_s)
            if proc.stdin is not None:
                try:
                    proc.stdin.close()
                except (OSError, ValueError):
                    pass
            try:
                proc.wait(timeout=self.timeout_s)
            except subprocess.TimeoutExpired:
                proc.kill()
                # Do not silently mark an unreaped process as closed.
                proc.wait(timeout=self.timeout_s)
            for reader in (self._reader, self._stderr_reader):
                reader.join(timeout=self.timeout_s)
            for pipe in (proc.stdin, proc.stdout, proc.stderr):
                if pipe is not None:
                    pipe.close()
            if self._reader.is_alive() or self._stderr_reader.is_alive():
                raise RustWorkerError("worker pipe reader failed to exit")
            self._proc = None

    def __enter__(self) -> RustWorker:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
