"""Bounded subprocess NDJSON bridge; Python owns durable reservations."""

from __future__ import annotations

import json
import math
import queue
import re
import subprocess
import threading
import time
import uuid
from pathlib import Path

from .budget import Reservation
from .production_resources import (
    METADATA_BODY_CAP,
    METADATA_RESPONSE_LINE_BYTES,
    METADATA_RESPONSE_NODES,
    PROTOCOL_BOOTSTRAP_BYTES,
    PROTOCOL_MAX_DEPTH,
    PROTOCOL_MAX_NODES,
    PROTOCOL_NODE_BYTES,
)

PROTOCOL_VERSION = 1
LIGHTWEIGHT_PROTOCOL_VERSION = 2
LIGHTWEIGHT_CAPABILITY = "production_download_lightweight_v1"
METADATA_CAPABILITY = "metadata_attempt_v1"
METADATA_LIMITS = {
    "payload_revision": 1,
    "max_body_bytes": METADATA_BODY_CAP,
    "response_line_bytes": METADATA_RESPONSE_LINE_BYTES,
}
MAX_LINE_BYTES = 64 * 1024
MAX_SCAN_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_SCAN_MEMBERS = 100_000
_STDERR_TAIL_BYTES = 64 * 1024
_STDOUT_QUEUE_LINES = 1
_QUEUE_WAIT_S = 0.05
_TIMEOUT_S = 60.0
# Mirrored by rust/src/production.rs; includes all hops and bounded backoff.
PRODUCTION_HTTP_TIMEOUT_S = 30
PRODUCTION_OPERATION_TIMEOUT_S = 4 * PRODUCTION_HTTP_TIMEOUT_S + 3 + 2
PRODUCTION_BRIDGE_MARGIN_S = 5
PRODUCTION_RPC_TIMEOUT_S = PRODUCTION_OPERATION_TIMEOUT_S + PRODUCTION_BRIDGE_MARGIN_S
_SHUTDOWN_TIMEOUT_S = 2.0
_REJECTED = "worker rejected request"
_ZERO_BUDGET = {"body": 0, "disk": 0, "inflight": 0, "attempts": 0}
_STRING_SPECIAL = re.compile(rb'["\\\x00-\x1f]')


class RustWorkerError(RuntimeError):
    """Static failure surface for the Rust worker bridge."""

    def __init__(self, message):
        super().__init__(message)
        self.diagnostic_code = {
            "worker timed out": "worker_timeout",
            "worker exited": "worker_eof",
            "worker pipe failure": "worker_eof",
        }.get(message, "worker_protocol")


def _json_peak(raw, *, max_nodes=PROTOCOL_MAX_NODES, _run_skip_strings=False):
    """Lexically bound allocations BEFORE UTF-8 decoding or json.loads.

    No growing tokens or object graph. Syntax is finally checked by loads;
    this pass rejects root/type, depth, node and numeric allocation attacks.
    Counts keys as well as values, including duplicates discarded by loads.
    """
    length = len(raw)
    # Large legacy scans and negotiated metadata envelopes use the same exact
    # lexical walk with C-level ordinary-string skipping. Normal RPCs are unchanged.
    legacy_strings = length > MAX_LINE_BYTES and (
        max_nodes > PROTOCOL_MAX_NODES or _run_skip_strings
    )
    i = nodes = string_bytes = depth = 0
    stack = bytearray(PROTOCOL_MAX_DEPTH)
    first = True
    while i < length:
        c = raw[i]
        if c in b" \t\r\n,:":
            i += 1
            continue
        if first:
            if c != 123:
                raise RustWorkerError("worker returned invalid json")
            first = False
        if c in (123, 91):
            if depth == PROTOCOL_MAX_DEPTH:
                raise RustWorkerError("worker json depth exceeded")
            stack[depth] = c
            depth += 1
            nodes += 1
            i += 1
        elif c in (125, 93):
            if not depth or stack[depth - 1] != (123 if c == 125 else 91):
                raise RustWorkerError("worker returned invalid json")
            depth -= 1
            i += 1
        elif c == 34:
            nodes += 1
            i += 1
            start = i
            if legacy_strings:
                while i < length and raw[i] != 34:
                    if raw[i] < 32:
                        raise RustWorkerError("worker returned invalid json")
                    if raw[i] == 92:
                        # Escapes keep the scalar path, avoiding a regex call for
                        # every escape in a dense or mostly escaped string.
                        i += 1
                        if i >= length or raw[i] not in b'"\\/bfnrtu':
                            raise RustWorkerError("worker returned invalid json")
                        if raw[i] == 117:
                            for j in range(i + 1, i + 5):
                                if j >= length or raw[j] not in b"0123456789abcdefABCDEF":
                                    raise RustWorkerError("worker returned invalid json")
                            i += 4
                    elif i + 1 < length and raw[i + 1] == 34:
                        i += 1  # One ordinary byte before the closing quote.
                        break
                    else:
                        # Skip longer ordinary runs in C, without a substring.
                        match = _STRING_SPECIAL.search(raw, i + 1)
                        if match is None:
                            i = length
                            break
                        i = match.start()
                        continue
                    i += 1
            else:
                while i < length and raw[i] != 34:
                    if raw[i] < 32:
                        raise RustWorkerError("worker returned invalid json")
                    if raw[i] == 92:
                        i += 1
                        if i >= length or raw[i] not in b'"\\/bfnrtu':
                            raise RustWorkerError("worker returned invalid json")
                        if raw[i] == 117:
                            for j in range(i + 1, i + 5):
                                if j >= length or raw[j] not in b"0123456789abcdefABCDEF":
                                    raise RustWorkerError("worker returned invalid json")
                            i += 4
                    i += 1
            if i >= length:
                raise RustWorkerError("worker returned invalid json")
            string_bytes += i - start
            i += 1
        elif c == 45 or 48 <= c <= 57:
            nodes += 1
            start = i
            while i < length and raw[i] in b"-+0123456789.eE":
                i += 1
                if i - start > 32:
                    raise RustWorkerError("worker json number exceeded")
            token = raw[start:i]  # fixed <=32 bytes, never an unbounded integer
            try:
                if b"." in token or b"e" in token or b"E" in token:
                    if not math.isfinite(float(token)):
                        raise ValueError()
                elif not -(1 << 63) <= int(token) <= (1 << 64) - 1:
                    raise ValueError()
            except ValueError:
                raise RustWorkerError("worker json number exceeded") from None
        elif c in (116, 102, 110):
            token = b"true" if c == 116 else b"false" if c == 102 else b"null"
            if raw[i : i + len(token)] != token:
                raise RustWorkerError("worker returned invalid json")
            nodes += 1
            i += len(token)
        else:
            raise RustWorkerError("worker returned invalid json")
        if nodes > min(max_nodes, length):
            raise RustWorkerError("worker json nodes exceeded")
    if first or depth:
        raise RustWorkerError("worker returned invalid json")
    return 4 * length + 4 * string_bytes + PROTOCOL_NODE_BYTES * nodes


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
        capacity=None,
        lightweight: bool = False,
        metadata: bool = False,
        deadline: float | None = None,
        cancel_event=None,
    ) -> None:
        binary = Path(binary)
        if not binary.is_file():
            raise RustWorkerError("worker binary missing")
        self.binary = binary
        if (
            isinstance(timeout_s, bool)
            or not isinstance(timeout_s, (int, float))
            or not math.isfinite(timeout_s)
            or timeout_s <= 0
        ):
            raise ValueError("worker timeout must be positive and finite")
        self.timeout_s = timeout_s
        self.metadata = metadata
        self._teardown_deadline = None
        self._constructor_deadline = deadline
        if metadata and not lightweight:
            raise ValueError("metadata worker requires lightweight wire protocol")
        if deadline is not None and (not math.isfinite(deadline) or deadline <= time.monotonic()):
            raise RustWorkerError("worker timed out")
        from ..capacity import CapacityConfig

        if capacity is not None and not isinstance(capacity, CapacityConfig):
            raise ValueError("typed capacity required")
        self.lightweight = lightweight
        if lightweight and job_budget is not None:
            raise ValueError("lightweight worker does not accept budget")
        self.protocol_version = LIGHTWEIGHT_PROTOCOL_VERSION if lightweight else PROTOCOL_VERSION
        self.capacity = capacity = (
            CapacityConfig() if lightweight and capacity is None else capacity
        )
        self._line_bytes = capacity.rpc_line_bytes if capacity is not None else MAX_LINE_BYTES
        self._bootstrap_bytes = PROTOCOL_BOOTSTRAP_BYTES
        self.capabilities: tuple[str, ...] = ()
        self.metadata_limits = None
        self.worker_version: str = ""
        self.protocol_resident_bytes = 0
        normalized_budget = None if lightweight else _normalize_budget(job_budget)
        self._proc = None
        self._reader = self._stderr_reader = None
        self._lines: queue.Queue[bytes] = queue.Queue(maxsize=_STDOUT_QUEUE_LINES)
        self._response_line_bytes = self._bootstrap_bytes
        self._stop = threading.Event()
        self._reader_done = threading.Event()
        self._stdout_error: str | None = None
        self._lifecycle_lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._writing = threading.Event()
        self._stderr_tail = bytearray()
        self._stderr_lock = threading.Lock()
        self._alive = True
        try:
            self._proc = subprocess.Popen(
                [str(binary)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
            )
            self._reader = threading.Thread(target=self._drain_stdout, daemon=True)
            self._stderr_reader = threading.Thread(target=self._drain_stderr, daemon=True)
            self._reader.start()
            self._stderr_reader.start()
            if cancel_event is not None and cancel_event.is_set():
                self.signal_cancel()
            self._handshake(normalized_budget)
        except BaseException as primary:
            try:
                self.cancel()
            except BaseException:
                primary.finalization_secondary = (
                    *getattr(primary, "finalization_secondary", ()),
                    "worker_constructor_shutdown",
                )
            raise

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
                    pending.clear()
                    continue
                start = 0
                while start < len(chunk):
                    newline = chunk.find(b"\n", start)
                    end = len(chunk) if newline < 0 else newline + 1
                    if len(pending) + end - start > self._response_line_bytes:
                        self._stdout_error = "worker response line too long"
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
                            continue
                    # Queue ownership has transferred. Do not retain a large
                    # metadata line in this idle reader's next blocking read.
                    raw = None
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
        if len(line) > self._line_bytes:
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

    def _receive(self, *, max_line_bytes=None, deadline=None, max_nodes=None) -> dict:
        max_line_bytes = self._line_bytes if max_line_bytes is None else max_line_bytes
        deadline = time.monotonic() + self.timeout_s if deadline is None else deadline
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
        # Legacy scan manifests have their own bounded member/node allowance.
        node_limit = (
            MAX_SCAN_MEMBERS * 32
            if max_line_bytes == MAX_SCAN_RESPONSE_BYTES
            else PROTOCOL_MAX_NODES
        )
        if max_nodes is not None:
            node_limit = max_nodes
        peak = _json_peak(
            raw, max_nodes=node_limit,
            _run_skip_strings=(getattr(self, "metadata", False)
                               and max_line_bytes == METADATA_RESPONSE_LINE_BYTES
                               and max_nodes == METADATA_RESPONSE_NODES),
        )
        if peak > 8 * max_line_bytes + PROTOCOL_NODE_BYTES * node_limit:
            raise RustWorkerError("worker json allocation exceeded")
        try:
            message = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError, RecursionError) as exc:
            raise RustWorkerError("worker returned invalid json") from exc
        if not isinstance(message, dict):
            raise RustWorkerError("worker returned invalid json")
        if len(raw) > self._line_bytes and not (
            message.get("type") == "response" and message.get("ok") is True
        ):
            raise RustWorkerError("worker response line too long")
        return message

    def _exchange_deadline(self, line, *, max_line_bytes, deadline, max_nodes=None):
        """One watchdog spans pipe send, receive and bounded JSON decoding."""
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RustWorkerError("worker timed out")
        expired = threading.Event()

        def expire():
            expired.set()
            self.signal_cancel()

        timer = threading.Timer(remaining, expire)
        timer.daemon = True
        timer.start()
        try:
            self._send(line)
            result = self._receive(
                max_line_bytes=max_line_bytes, deadline=deadline, max_nodes=max_nodes
            )
            if expired.is_set() or time.monotonic() >= deadline:
                raise RustWorkerError("worker timed out")
            return result
        except Exception:
            if expired.is_set() or time.monotonic() >= deadline:
                raise RustWorkerError("worker timed out") from None
            raise
        finally:
            timer.cancel()
            timer.join(timeout=max(0.0, (self._teardown_deadline or (time.monotonic() + 5.0))
                                   - time.monotonic()))
            if timer.is_alive():
                raise RustWorkerError("worker watchdog failed to exit")

    def _handshake(self, job_budget: dict | None) -> None:
        hello = {"type": "hello", "protocol_version": self.protocol_version}
        if not self.lightweight:
            hello["budget"] = job_budget
        if self.capacity is not None:
            hello["execution_limits" if self.lightweight else "stream_capacity"] = {
                "range_chunk_bytes": self.capacity.range_chunk_bytes,
                "http_header_bytes": self.capacity.http_header_bytes,
                "rpc_line_bytes": self.capacity.rpc_line_bytes,
            }
        line = (json.dumps(hello) + "\n").encode("utf-8")
        if len(line) > self._bootstrap_bytes:
            raise RustWorkerError("worker request line too long")
        if self.metadata:
            deadline = min(
                time.monotonic() + self.timeout_s,
                self._constructor_deadline or float("inf"),
            )
            message = self._exchange_deadline(
                line, max_line_bytes=self._bootstrap_bytes, deadline=deadline
            )
        else:
            self._send(line)
            message = self._receive(max_line_bytes=self._bootstrap_bytes)
        if (
            message.get("type") != "ready"
            or message.get("protocol_version") != self.protocol_version
        ):
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
        self.metadata_limits = message.get("metadata_limits")
        if self.metadata:
            self.require_metadata()
        if self.lightweight and (
            LIGHTWEIGHT_CAPABILITY not in self.capabilities
            or message.get("execution_limits") != hello["execution_limits"]
        ):
            raise RustWorkerError("worker lightweight capability negotiation failed")
        if (
            not self.lightweight
            and self.capacity is not None
            and (
                "production_transfer_v2" not in self.capabilities
                or message.get("stream_capacity") != hello["stream_capacity"]
            )
        ):
            self.cancel()
            raise RustWorkerError("worker capacity negotiation failed")
        from .production_resources import protocolmemory

        resident = message.get("protocol_resident_bytes")
        if self.capacity is not None and (
            type(resident) is not int or resident != protocolmemory(self.capacity)
        ):
            self.cancel()
            raise RustWorkerError("worker protocol memory negotiation failed")
        self.protocol_resident_bytes = resident if type(resident) is int else 0
        self._response_line_bytes = self._line_bytes

    def require_metadata(self):
        if (METADATA_CAPABILITY not in self.capabilities
                or type(self.metadata_limits) is not dict
                or self.metadata_limits != METADATA_LIMITS
                or any(type(v) is not int for v in self.metadata_limits.values())):
            raise RustWorkerError("worker metadata capability negotiation failed")

    def metadata_attempt(self, payload, *, deadline):
        if not self.metadata or not self.lightweight:
            raise RustWorkerError("worker metadata channel unavailable")
        self.require_metadata()
        request_id = uuid.uuid4().hex
        request = {
            "type": "request", "request_id": request_id, "operation": "metadata_attempt",
            "payload": {"metadata": payload},
        }
        self._response_line_bytes = METADATA_RESPONSE_LINE_BYTES
        try:
            line = (json.dumps(request) + "\n").encode("utf-8")
            message = self._exchange_deadline(
                line, max_line_bytes=METADATA_RESPONSE_LINE_BYTES, deadline=deadline,
                max_nodes=METADATA_RESPONSE_NODES,
            )
        finally:
            self._response_line_bytes = self._line_bytes
        if (set(message) != {"type", "request_id", "ok", "result"}
                or message.get("type") != "response" or message.get("request_id") != request_id
                or message.get("ok") is not True or type(message.get("result")) is not dict):
            raise RustWorkerError("worker returned invalid json")
        return message["result"]

    def request(
        self, operation: str, *, budget: dict | None = None, payload: dict | None = None
    ) -> dict:
        scan = operation in ("scan_tar", "scan_http_tar")
        # Production scans return bounded sidecar references, not legacy manifests.
        scan = scan and not (payload is not None and "production" in payload)
        member_limit = MAX_SCAN_MEMBERS
        if scan and payload is not None:
            member_limit = payload.get("max_members", MAX_SCAN_MEMBERS)
            if type(member_limit) is not int or not 0 <= member_limit <= MAX_SCAN_MEMBERS:
                raise RustWorkerError("worker scan member limit invalid")
        request = {
            "type": "request",
            "request_id": uuid.uuid4().hex,
            "operation": operation,
            **({} if self.lightweight else {"budget": _normalize_budget(budget)}),
            "payload": payload if payload is not None else {},
        }
        response_limit = MAX_SCAN_RESPONSE_BYTES if scan else self._line_bytes
        if self.lightweight:
            if budget is not None or payload is None or "production" not in payload:
                raise RustWorkerError("lightweight request invalid")
            response_limit = self._line_bytes
        self._response_line_bytes = response_limit
        try:
            self._send((json.dumps(request) + "\n").encode("utf-8"))
            message = self._receive(max_line_bytes=response_limit)
        finally:
            self._response_line_bytes = self._line_bytes
        if message.get("type") == "protocol_error":
            raise RustWorkerError("worker protocol error")
        if message.get("type") != "response" or message.get("request_id") != request["request_id"]:
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
        self._send(line)

    def read_raw(self) -> dict:
        return self._receive()

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

    @property
    def pid(self) -> int | None:
        return self._proc.pid if self._proc is not None else None

    def signal_cancel(self, *, deadline=None) -> None:
        """Signal before joins, including while another owner is in a pipe write."""
        if self.metadata and deadline is None:
            deadline = time.monotonic() + 5.0
        if deadline is not None:
            self._teardown_deadline = min(self._teardown_deadline or float("inf"), deadline)
        self._alive = False
        self._stop.set()
        proc = self._proc
        if proc is not None:
            try:
                proc.kill()
            except OSError:
                pass

    def cancel(self, *, deadline=None) -> None:
        self.signal_cancel(deadline=deadline)
        self._shutdown(kill=True, deadline=deadline)

    def close(self, *, deadline=None) -> None:
        self._shutdown(kill=False, deadline=deadline)

    def _shutdown(self, *, kill: bool, deadline=None) -> None:
        if self.metadata:
            if self._teardown_deadline is None:
                self._teardown_deadline = time.monotonic() + 5.0
            deadline = min(deadline or float("inf"), self._teardown_deadline)
        elif deadline is not None:
            self._teardown_deadline = min(self._teardown_deadline or float("inf"), deadline)
        def wait_limit():
            shared = self._teardown_deadline
            effective = deadline if shared is None else min(deadline or float("inf"), shared)
            return (_SHUTDOWN_TIMEOUT_S if effective is None
                    else max(0.0, effective - time.monotonic()))

        with self._lifecycle_lock:
            proc = self._proc
            if proc is None:
                return
            self._alive = False
            self._stop.set()
            # Kill+wait before closing a blocked Windows WriteFile handle.
            if kill or self._writing.is_set():
                try:
                    proc.kill()
                except OSError:
                    pass
                proc.wait(timeout=wait_limit())
            if proc.stdin is not None:
                try:
                    proc.stdin.close()
                except (OSError, ValueError):
                    pass
            try:
                proc.wait(timeout=wait_limit())
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=wait_limit())
            readers = [
                r
                for r in (self._reader, self._stderr_reader)
                if r is not None and r.ident is not None
            ]
            for reader in readers:
                reader.join(timeout=wait_limit())
            if any(reader.is_alive() for reader in readers):
                raise RustWorkerError("worker pipe reader failed to exit")
            for pipe in (proc.stdin, proc.stdout, proc.stderr):
                if pipe is not None:
                    pipe.close()
            self._proc = None

    def __enter__(self) -> RustWorker:
        return self

    def __exit__(self, exc_type, primary, traceback):
        try:
            self.close()
        except BaseException:
            if primary is None:
                raise
            primary.finalization_secondary = (
                *getattr(primary, "finalization_secondary", ()),
                "worker_close",
            )
