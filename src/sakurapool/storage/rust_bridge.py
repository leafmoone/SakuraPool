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
import uuid
from pathlib import Path

from .budget import Reservation

PROTOCOL_VERSION = 1
MAX_LINE_BYTES = 64 * 1024
_STDERR_TAIL_BYTES = 64 * 1024
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
        self._proc = subprocess.Popen(
            [str(binary)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self._lines: queue.Queue[bytes] = queue.Queue()
        self._stderr_tail = bytearray()
        self._stderr_lock = threading.Lock()
        self._alive = True
        self._reader = threading.Thread(target=self._drain_stdout, daemon=True)
        self._stderr_reader = threading.Thread(target=self._drain_stderr, daemon=True)
        self._reader.start()
        self._stderr_reader.start()
        self._handshake(_normalize_budget(job_budget))

    # -- transport ------------------------------------------------------

    def _drain_stdout(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        for raw in self._proc.stdout:
            self._lines.put(raw)

    def _drain_stderr(self) -> None:
        assert self._proc is not None and self._proc.stderr is not None
        while True:
            chunk = self._proc.stderr.read(4096)
            if not chunk:
                break
            self._append_stderr(chunk)

    def _append_stderr(self, chunk: bytes) -> None:
        with self._stderr_lock:
            self._stderr_tail.extend(chunk)
            if len(self._stderr_tail) > _STDERR_TAIL_BYTES:
                del self._stderr_tail[: len(self._stderr_tail) - _STDERR_TAIL_BYTES]

    def stderr_tail(self) -> bytes:
        with self._stderr_lock:
            return bytes(self._stderr_tail)

    def _send(self, line: bytes) -> None:
        if not self._alive or self._proc is None or self._proc.stdin is None:
            raise RustWorkerError("worker is closed")
        if len(line) > MAX_LINE_BYTES:
            raise RustWorkerError("worker request line too long")
        try:
            self._proc.stdin.write(line)
            self._proc.stdin.flush()
        except (OSError, ValueError) as exc:
            self._alive = False
            raise RustWorkerError("worker pipe failure") from exc

    def _receive(self) -> dict:
        try:
            raw = self._lines.get(timeout=self.timeout_s)
        except queue.Empty as exc:
            raise RustWorkerError("worker timed out") from exc
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
        """Kill the worker and wait for it to exit (crash-class close)."""
        if self._proc is None:
            return
        self._alive = False
        try:
            self._proc.kill()
        except OSError:
            pass
        try:
            self._proc.wait(timeout=self.timeout_s)
        except subprocess.TimeoutExpired:
            pass
        self._drain_stderr_join()
        self._proc = None

    def close(self) -> None:
        """Graceful close: close stdin, wait, then kill as a last resort."""
        if self._proc is None:
            return
        self._alive = False
        try:
            if self._proc.stdin is not None:
                self._proc.stdin.close()
        except OSError:
            pass
        try:
            self._proc.wait(timeout=self.timeout_s)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            try:
                self._proc.wait(timeout=self.timeout_s)
            except subprocess.TimeoutExpired:
                pass
        self._drain_stderr_join()
        self._proc = None

    def _drain_stderr_join(self) -> None:
        try:
            self._stderr_reader.join(timeout=self.timeout_s)
        except RuntimeError:
            pass

    def __enter__(self) -> RustWorker:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
