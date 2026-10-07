"""Owned, serial, memory-only native metadata attempts with terminal evidence."""

from __future__ import annotations

import base64
import binascii
import hashlib
import math
import os
import threading
import time
import traceback
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from urllib.parse import unquote, urlsplit

import requests

from .budget import BudgetDeadlineExceeded, Reservation
from .production_resources import (
    metadata_resident_memory,
    metadata_response_memory,
)
from .rust_bridge import RustWorker, RustWorkerError
from .transport import RemoteIOError

OPERATION_TIMEOUT_S = 125.0
ATTEMPT_TIMEOUT_S = 60.0
TEARDOWN_GRACE_S = 5.0
MAX_REQUESTS = 256
_FIELDS = frozenset({
    "format", "status", "code", "phase", "headers", "disposition", "attempt_started",
    "body_read_started", "accounting_complete", "observed_bytes", "body_eof",
    "request_finalized", "retry_safe", "body_base64", "body_bytes", "body_sha256",
})
_CODES = frozenset({
    "ok", "rejected", "origin_timeout", "origin_connect", "network_ambiguous", "body_io",
    "metadata_encoding", "metadata_limit", "body_framing",
})
_FLAGS = ("attempt_started", "body_read_started", "accounting_complete", "body_eof",
          "request_finalized", "retry_safe")


def remaining(deadline):
    value = deadline - time.monotonic()
    if not math.isfinite(value) or value <= 0:
        raise RemoteIOError("Metadata deadline exceeded", code="worker_timeout",
                            phase="metadata_send", accounting="CONFIRMED")
    return value


def _protocol_error():
    return RustWorkerError("worker returned invalid json")


def _clear_error_buffers(error):
    """A sanitized failure must not retain decoded bodies through hidden frames."""
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        following = error.__context__ or error.__cause__
        traceback.clear_frames(error.__traceback__)
        error.__traceback__ = error.__context__ = error.__cause__ = None
        error = following


def _secrets(ticket):
    values = [ticket["token"]] if ticket.get("token") else []
    if ticket.get("cookie"):
        values.extend(part.split("=", 1)[1] for part in ticket["cookie"].split(";")
                      if "=" in part and part.split("=", 1)[1])
    if ticket.get("proxy_url"):
        password = urlsplit(ticket["proxy_url"]).password
        if password:
            values.extend((password, unquote(password)))
    return values


def _credential_echo(header, ticket):
    secrets = _secrets(ticket)
    if not secrets:
        return False
    for _ in range(5):
        if any(secret in header for secret in secrets):
            return True
        decoded = unquote(header)
        if decoded == header:
            return False
        header = decoded
    return True  # Deeper ambiguous encoding is not accepted as safe evidence.


def validate_ticket(ticket):
    """Bound all outgoing scalars before serialization or bridge allocation."""
    fields = {"payload_revision", "profile", "url", "origin", "trusted_hosts", "token",
              "cookie", "proxy_url", "tls_policy", "max_body_bytes", "http_header_bytes",
              "remaining_ms"}
    valid = type(ticket) is dict and set(ticket) == fields
    if valid:
        for key, limit, optional in (("url", 8192, False), ("origin", 256, False),
                                     ("token", 4096, True), ("cookie", 512, True),
                                     ("proxy_url", 8192, True)):
            value = ticket[key]
            if value is None and optional:
                continue
            if (type(value) is not str or not value or len(value) > limit or not value.isascii()
                    or any(ord(c) < (32 if key == "cookie" else 33) or ord(c) == 127
                           for c in value)):
                valid = False
                break
    if valid:
        hosts, tls = ticket["trusted_hosts"], ticket["tls_policy"]
        valid = (type(ticket["payload_revision"]) is int and ticket["payload_revision"] == 1
                 and type(ticket["profile"]) is str
                 and ticket["profile"] in {"loopback_test", "modelscope_https_v1"}
                 and type(hosts) is list and 1 <= len(hosts) <= 8
                 and all(type(h) is str and 0 < len(h) <= 253 and h.isascii() for h in hosts)
                 and type(tls) is dict and set(tls) == {"mode", "pem_file"}
                 and type(tls["mode"]) is str and tls["mode"] in {"native", "pem_bundle"}
                 and (tls["pem_file"] is None or type(tls["pem_file"]) is str
                      and 0 < len(tls["pem_file"]) <= 4096
                      and not any(ord(c) < 32 or ord(c) == 127 for c in tls["pem_file"]))
                 and type(ticket["max_body_bytes"]) is int
                 and 0 <= ticket["max_body_bytes"] <= 1 << 20
                 and type(ticket["http_header_bytes"]) is int
                 and 0 < ticket["http_header_bytes"] <= 417_760
                 and type(ticket["remaining_ms"]) is int
                 and 0 < ticket["remaining_ms"] <= 125_000)
    if not valid:
        raise RemoteIOError("Metadata request bounds rejected", code="rejected",
                            phase="metadata_send", accounting="CONFIRMED")


@dataclass(frozen=True, repr=False)
class Attempt:
    status: int | None
    code: str
    phase: str
    headers: dict
    known: bool
    observed: int
    retry_safe: bool
    body: bytes | None


def validate_result(value, ticket):
    """Reject contradictory evidence before trusting any consumption or payload."""
    cap = ticket["max_body_bytes"]
    if (type(value) is not dict or set(value) != _FIELDS
            or value["format"] != "sakurapool-metadata-attempt-v1"
            or type(value["code"]) is not str or type(value["phase"]) is not str
            or type(value["disposition"]) is not str
            or value["code"] not in _CODES
            or value["phase"] not in {"metadata_send", "metadata_headers", "metadata_body"}
            or any(type(value[key]) is not bool for key in _FLAGS)
            or not value["request_finalized"]
            or (value["status"] is not None and (type(value["status"]) is not int
                                                  or not 100 <= value["status"] <= 599))
            or type(value["observed_bytes"]) is not int
            or not 0 <= value["observed_bytes"] <= cap + 1
            or type(value["headers"]) is not dict
            or set(value["headers"]) != {"location", "retry_after"}):
        raise _protocol_error()
    for key, limit in (("location", 8192), ("retry_after", 256)):
        header = value["headers"][key]
        if header is not None and (type(header) is not str or len(header) > limit
                or not header.isascii() or any(ord(c) < 32 or ord(c) == 127 for c in header)
                or _credential_echo(header, ticket)):
            raise _protocol_error()
    status, code = value["status"], value["code"]
    disposition = value["disposition"]
    known, started = value["accounting_complete"], value["body_read_started"]
    observed, eof, retry = value["observed_bytes"], value["body_eof"], value["retry_safe"]
    if (retry and (disposition != "NOT_READ" or observed or started or not known)
            or (status is not None and not value["attempt_started"])
            or (started and not value["attempt_started"])):
        raise _protocol_error()
    payload = None
    if disposition == "EOF":
        encoded, size, digest = (value["body_base64"], value["body_bytes"],
                                 value["body_sha256"])
        if (status != 200 or code != "ok" or not started or not known or not eof or retry
                or value["phase"] != "metadata_body" or type(size) is not int
                or size != observed or size > cap or type(encoded) is not str
                or len(encoded) != 4 * ((size + 2) // 3)
                or type(digest) is not str or len(digest) != 64
                or any(c not in "0123456789abcdef" for c in digest)):
            raise _protocol_error()
        try:
            payload = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error):
            raise _protocol_error() from None
        if (len(payload) != size or base64.b64encode(payload).decode("ascii") != encoded
                or hashlib.sha256(payload).hexdigest() != digest):
            raise _protocol_error()
    else:
        if any(value[key] is not None for key in ("body_base64", "body_bytes", "body_sha256")):
            raise _protocol_error()
        if eof:
            raise _protocol_error()
        if disposition == "NOT_READ":
            if started or observed:
                raise _protocol_error()
            if code == "ok" and (status is None or status == 200 or not known or not retry):
                raise _protocol_error()
            if code in {"origin_connect", "origin_timeout"} and (
                    status is not None or not known or not retry or not value["attempt_started"]
                    or value["phase"] != "metadata_send"):
                raise _protocol_error()
            if retry and code not in {"ok", "origin_connect", "origin_timeout"}:
                raise _protocol_error()
            if (code == "network_ambiguous" and value["attempt_started"] and known):
                raise _protocol_error()
        elif disposition == "LIMIT":
            if (status != 200 or code != "metadata_limit" or not known or not started
                    or observed != cap + 1 or retry or value["phase"] != "metadata_body"):
                raise _protocol_error()
        elif disposition == "PARTIAL":
            if (status != 200 or code != "body_io" or known or not started or retry
                    or observed > cap or value["phase"] != "metadata_body"):
                raise _protocol_error()
        else:
            raise _protocol_error()
    return Attempt(status, code, value["phase"], value["headers"], known,
                   observed, retry, payload)


def network_policy(url, *, offline):
    if offline:
        return None, {"mode": "native", "pem_file": None}
    proxies = requests.utils.get_environ_proxies(url)
    proxy = requests.utils.select_proxy(url, proxies)
    if proxy:
        if type(proxy) is not str or len(proxy) > 8192:
            raise RemoteIOError("Unsupported metadata proxy policy", code="rejected",
                                phase="metadata_send", accounting="CONFIRMED")
        proxy = requests.utils.prepend_scheme_if_needed(proxy, "http")
        try:
            parsed = urlsplit(proxy)
            invalid = (parsed.scheme not in {"http", "https"} or not parsed.hostname
                       or parsed.path not in {"", "/"} or parsed.query or parsed.fragment
                       or parsed.port is not None and not 0 < parsed.port <= 65535)
        except ValueError:
            invalid = True
        if (invalid or len(proxy) > 8192
                or any(ord(c) <= 32 or ord(c) == 127 or c == "\\" for c in proxy)):
            raise RemoteIOError("Unsupported metadata proxy policy", code="rejected",
                                phase="metadata_send", accounting="CONFIRMED")
    pem = os.environ.get("REQUESTS_CA_BUNDLE") or os.environ.get("CURL_CA_BUNDLE")
    if pem and (len(pem) > 4096 or any(ord(c) < 32 or ord(c) == 127 for c in pem)
                or not os.path.isfile(pem)):
        raise RemoteIOError("Unsupported metadata CA policy", code="rejected",
                            phase="metadata_send", accounting="CONFIRMED")
    return proxy or None, {"mode": "pem_bundle" if pem else "native", "pem_file": pem or None}


class MetadataRPC:
    """One independent metadata process/generation; never owns data-lane proofs."""

    def __init__(self, owner, binary):
        self.owner, self.binary = owner, binary
        self.ledger, self.capacity = owner.ledger, owner.capacity
        self._worker = None
        self._resident = None
        self._requests = 0
        self._closed = threading.Event()
        self._serial = threading.Lock()
        self._state = threading.Lock()
        self._failed = False
        self._teardown_deadline = None
        self._operation_deadline = None

    def _budget_call(self, method, *args, deadline=None, **kwargs):
        deadline = self._operation_deadline if deadline is None else deadline
        bounded = getattr(self.ledger, "lock_deadline", None)
        scope = (bounded(deadline) if bounded is not None and deadline is not None
                 else nullcontext())
        try:
            with scope:
                return getattr(self.ledger, method)(*args, **kwargs)
        except BudgetDeadlineExceeded:
            self._failed = True
            self._grace()
            raise RemoteIOError("Metadata accounting deadline exceeded", code="worker_timeout",
                                phase="metadata_send", accounting="UNKNOWN") from None

    def _settle(self, lease, *, cleanup=False):
        if lease is not None:
            self._budget_call("settle", lease, deadline=self._grace() if cleanup else None)

    def _grace_locked(self, deadline=None):
        limits = [time.monotonic() + TEARDOWN_GRACE_S]
        for value in (deadline, self._teardown_deadline,
                      getattr(self._worker, "_teardown_deadline", None)):
            if value is not None:
                limits.append(value)
        if self._operation_deadline is not None:
            limits.append(self._operation_deadline + TEARDOWN_GRACE_S)
        self._teardown_deadline = min(limits)
        return self._teardown_deadline

    def _grace(self, deadline=None):
        with self._state:
            return self._grace_locked(deadline)

    def _ensure_worker(self, deadline):
        if self._closed.is_set() or self._failed:
            raise RustWorkerError("worker is closed")
        if self._worker is not None and self._requests >= MAX_REQUESTS:
            try:
                self._worker.close(deadline=min(deadline, time.monotonic() + TEARDOWN_GRACE_S))
            except BaseException as error:
                self._failed = True
                _clear_error_buffers(error)
                raise RustWorkerError("worker generation close failed") from None
            self._worker = None
            self._requests = 0
        if self._worker is None:
            remaining(deadline)
            if self.ledger is not None and self._resident is None:
                self._resident = self._budget_call("reserve",
                    Reservation(inflight=metadata_resident_memory(self.capacity))
                )
            if self._closed.is_set():
                self._settle(self._resident, cleanup=True)
                self._resident = None
                raise RustWorkerError("worker is closed")
            # Publish the constructing worker before __init__: cancellation can
            # signal it as soon as Popen has supplied an actual child handle.
            worker = RustWorker.__new__(RustWorker)
            self._worker = worker
            try:
                RustWorker.__init__(worker, self.binary, capacity=self.capacity,
                                    lightweight=True, metadata=True,
                                    timeout_s=min(60.0, remaining(deadline)), deadline=deadline,
                                    cancel_event=self._closed)
                if self._closed.is_set():
                    worker.cancel(deadline=self._grace())
                    raise RustWorkerError("worker is closed")
            except BaseException:
                self._failed = True
                if getattr(worker, "_proc", None) is None:
                    self._settle(self._resident, cleanup=True)
                    self._resident = None
                raise

    def preflight(self):
        if not self._serial.acquire(blocking=False):
            raise RustWorkerError("worker metadata channel busy")
        try:
            self._operation_deadline = time.monotonic() + OPERATION_TIMEOUT_S
            self._ensure_worker(self._operation_deadline)
        finally:
            with self._state:
                self._operation_deadline = None
                if not self._failed and not self._closed.is_set():
                    self._teardown_deadline = None
            self._serial.release()

    @contextmanager
    def operation(self, deadline):
        if not self._serial.acquire(blocking=False):
            raise RemoteIOError("Concurrent metadata request rejected", code="rejected",
                                phase="metadata_send", accounting="CONFIRMED")
        try:
            self._operation_deadline = deadline
            self._ensure_worker(deadline)
            yield
        except RustWorkerError as error:
            code = error.diagnostic_code
            _clear_error_buffers(error)
            self._failed = True
            primary = RemoteIOError("Metadata worker unavailable", code=code,
                                    phase="metadata_send")
            self._abort(primary)
            raise primary from None
        finally:
            with self._state:
                self._operation_deadline = None
                if not self._failed and not self._closed.is_set():
                    self._teardown_deadline = None
            self._serial.release()

    def _abort(self, primary):
        worker = self._worker
        if worker is not None and getattr(worker, "_proc", None) is not None:
            try:
                worker.cancel(deadline=self._grace())
            except BaseException:
                primary.finalization_secondary = ("METADATA_FINALIZATION_FAILED",)
        if worker is None or getattr(worker, "_proc", None) is None:
            try:
                self._settle(self._resident, cleanup=True)
                self._resident = None
            except BaseException:
                primary.finalization_secondary = ("METADATA_FINALIZATION_FAILED",)

    @contextmanager
    def attempt(self, ticket, *, deadline):
        validate_ticket(ticket)
        self._ensure_worker(deadline)
        memory = network = None
        validated = False
        issued = False
        primary = None
        value = result = None
        try:
            if self.ledger is not None:
                memory = self._budget_call("reserve", Reservation(
                    inflight=metadata_response_memory(ticket["max_body_bytes"], self.capacity)))
                network = self._budget_call("reserve", Reservation(
                    body=ticket["max_body_bytes"] + 1, metadata=ticket["max_body_bytes"] + 1,
                    attempt=True))
            available = remaining(deadline)
            ticket["remaining_ms"] = max(1, min(125_000, int(available * 1000)))
            # The attempt's native 60s timer is independent of the logical125s
            # budget. A silent/broken child gets at most5s IPC slack within it.
            watchdog = min(deadline, time.monotonic() + min(ATTEMPT_TIMEOUT_S, available)
                           + TEARDOWN_GRACE_S)
            issued = True
            value = self._worker.metadata_attempt(ticket, deadline=watchdog)
            self._requests += 1
            result = validate_result(value, ticket)
            value = None  # Release encoded body before handing off decoded bytes.
            validated = True
            if network is not None:
                self._budget_call("consume_body", network, result.observed, metadata=True)
                if result.known:
                    self._budget_call("settle", network)
                    network = None
            remaining(deadline)
            yield result
        except RustWorkerError as error:
            self._failed = True
            code = error.diagnostic_code
            _clear_error_buffers(error)
            value = result = None
            primary = RemoteIOError("Metadata worker attempt failed", code=code,
                                    phase="metadata_send")
            self._abort(primary)
            raise primary from None
        except BaseException as error:
            primary = error
            if issued and (not validated or self._failed):
                self._failed = True
                _clear_error_buffers(error)
                value = result = None
                self._abort(primary)
            elif not issued and network is not None:
                try:
                    self._settle(network, cleanup=True)
                    network = None
                except BaseException:
                    primary.finalization_secondary = ("METADATA_FINALIZATION_FAILED",)
            raise
        finally:
            value = result = None
            # Unknown network consumption stays reserved independently of dead
            # process/response memory. Never refund network in this cleanup.
            if memory is not None and (not issued or validated or self._worker is None
                                      or getattr(self._worker, "_proc", None) is None):
                try:
                    self._settle(memory, cleanup=primary is not None)
                except BaseException:
                    if primary is not None:
                        primary.finalization_secondary = ("METADATA_FINALIZATION_FAILED",)
                        if isinstance(primary, RemoteIOError):
                            primary.accounting_state = "UNKNOWN"
                    else:
                        error = RemoteIOError("Metadata memory finalization failed",
                                              code="worker_protocol", phase="metadata_send")
                        error.finalization_secondary = ("METADATA_FINALIZATION_FAILED",)
                        raise error from None

    def signal_cancel(self, *, deadline=None):
        with self._state:
            grace = self._grace_locked(deadline)
            self._closed.set()
        worker = self._worker
        if worker is not None and hasattr(worker, "_stop"):
            worker.signal_cancel(deadline=grace)

    def close(self, *, deadline=None):
        self.signal_cancel(deadline=deadline)
        grace = self._grace()
        if not self._serial.acquire(timeout=max(0.0, grace - time.monotonic())):
            raise RustWorkerError("worker metadata owner failed to exit")
        try:
            worker = self._worker
            if worker is not None and hasattr(worker, "_proc"):
                worker.cancel(deadline=grace)
            if worker is None or getattr(worker, "_proc", None) is None:
                self._settle(self._resident, cleanup=True)
                self._resident = None
        finally:
            self._serial.release()
