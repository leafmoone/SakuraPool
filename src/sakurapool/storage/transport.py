"""The single P4 HTTP reader; no SDK/internal retries or implicit full-TAR fallback.

All GETs, including discovery, must use this transport. Its requests session
handles *no* automatic redirects/retries. The caller freezes trusted hostnames
from reviewed provider configuration; Location never expands that set.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import math
import random
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass as _dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Iterator
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .budget import MIB, BudgetLedger, Reservation
from .location_gate import (
    LocationRejected,
    RepositoryConfigError,
    TwoHopKeyError,
    check_location,
    check_token_not_in_etag,
    normalize_endpoint,
    normalize_host,
    parse_repository,
    two_hop_proof_key,
    two_hop_record_key,
    validate_two_hop_record,
)

MAX_MEMBER = 64 * MIB
READ_CHUNK = MIB
MAX_ATTEMPTS = 3  # redirects + retries + expired-URL resolution share this ceiling
_CONTENT_RANGE = re.compile(r"bytes ([0-9]+)-([0-9]+)/([0-9]+)\Z")


_SAFE_CODES = frozenset({"remote_io", "network_ambiguous", "http_status",
                         "metadata_encoding", "redirect_policy", "invalid_json",
                         "provider_shape", "retry_policy",
                         "provider_page_shape", "provider_entry_shape",
                         "provider_entry_type", "provider_entry_path",
                         "provider_entry_duplicate", "provider_entry_scope",
                         "provider_entry_size", "provider_entry_revision_shape",
                         "provider_entry_digest"})
_SAFE_PHASES = frozenset({"transport", "metadata_send", "metadata_headers",
                          "metadata_body", "provider_revision_shape",
                          "provider_listing_shape", "response_headers",
                          "two_hop_redirect"})


class RemoteIOError(RuntimeError):
    """Only static diagnostics, never provider body, URL, token or exception text."""

    def __init__(self, message: str, *, code: str = "remote_io",
                 phase: str = "transport", http_status: int | None = None):
        if code not in _SAFE_CODES or phase not in _SAFE_PHASES:
            raise ValueError("unrecognized safe remote diagnostic")
        if http_status is not None and (type(http_status) is not int
                                        or not 100 <= http_status <= 599):
            raise ValueError("invalid safe HTTP status")
        super().__init__(message)
        self.code = code
        self.phase = phase
        self.http_status = http_status

    def public_diagnostic(self) -> dict[str, str | int]:
        result: dict[str, str | int] = {"code": self.code, "phase": self.phase}
        if self.http_status is not None:
            result["http_status"] = self.http_status
        return result


class _AmbiguousRead(RemoteIOError):
    """Connection broke while reading: keep full body reservation pending."""

    def __init__(self, message: str, *, phase: str = "transport"):
        super().__init__(message, code="network_ambiguous", phase=phase)


class BoundObject:
    """Ephemeral object identity. Not a dataclass: asdict must not leak a URL."""

    __slots__ = ("url", "size", "immutable_revision", "strong_etag", "repository_id",
                 "_sealed")

    def __init__(self, url: str, size: int, immutable_revision: str | None = None,
                 strong_etag: str | None = None, repository: str | None = None):
        if type(size) is not int or not 0 <= size < 2**64:
            raise ValueError("object size must be uint64")
        repo = None
        if repository is not None:
            # Persistent identity binding: the runtime repository configuration
            # is parsed ONCE (parse_repository, fail-closed) and sealed into the
            # bound object; downstream refresh-origin and condition-proof logic
            # only ever sees this structured value, never a raw string.
            try:
                repo = parse_repository(repository)
            except RepositoryConfigError:
                raise ValueError("bound object repository is not a valid owner/name") from None
            # If this URL is a provider repo route, its owner/name segments
            # must EQUAL the configured (owner, name) exactly, in order. A
            # swapped or otherwise mismatched route is a clear refusal (never
            # accepted, never silently re-bound), so refresh and proofs can
            # not bind a repository-X object under identity Y.
            # Non-provider URLs (loopback fixtures) are not repo routes and
            # stay repository-neutral.
            segments = urlsplit(url).path.split("/")
            if (len(segments) == 7 and segments[1:4] == ["api", "v1", "datasets"]
                    and segments[6] == "repo"
                    and (segments[4], segments[5]) != (repo.owner, repo.name)):
                raise ValueError(
                    "repository route mismatch: bound object URL owner/name "
                    "does not equal the configured repository")
        if strong_etag and (strong_etag.startswith("W/") or not re.fullmatch(
                r'"[\x21\x23-\x7e]+"', strong_etag)):
            raise ValueError("weak or malformed ETag is not a reliable validator")
        if immutable_revision:
            if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", immutable_revision):
                raise ValueError("floating revision is not an immutable object binding")
            try:
                revisions = parse_qs(urlsplit(url).query).get("Revision")
            except ValueError:
                revisions = None
            if revisions != [immutable_revision]:
                raise ValueError("download URL is not bound to the immutable revision")
        # A 40-hex URL parameter alone is not evidence of an immutable object:
        # a trusted provider can redirect it to mutable CDN latest. Until
        # provider semantics are evidenced, require observed strong ETag.
        if not strong_etag:
            raise ValueError("strong validator required until immutable revision is verified")
        object.__setattr__(self, "url", url)
        object.__setattr__(self, "size", size)
        object.__setattr__(self, "immutable_revision", immutable_revision)
        object.__setattr__(self, "strong_etag", strong_etag)
        object.__setattr__(self, "repository_id", repo)
        object.__setattr__(self, "_sealed", True)

    def __setattr__(self, _name: str, _value: object) -> None:
        raise AttributeError("bound object is immutable")

    def __repr__(self) -> str:
        return f"BoundObject(size={self.size}, authenticated_binding=True)"

    def __reduce_ex__(self, _protocol: int):
        raise TypeError("ephemeral signed object URLs cannot be serialized")


@_dataclass(frozen=True)
class TwoHopResult:
    """Sanitized two-hop capability result; never carries URL or signature."""

    hop1_status: int
    hop2_status: int
    approved_host: str
    etag: str
    bytes_read: int
    payload: bytes
    attempts: int
    batch: str

    def as_dict(self) -> dict:
        return {
            "hop1_status": self.hop1_status, "hop2_status": self.hop2_status,
            "approved_host": self.approved_host, "etag": self.etag,
            "bytes_read": self.bytes_read, "attempts": self.attempts,
            "batch": self.batch,
            "payload_sha256": hashlib.sha256(self.payload).hexdigest(),
        }


class RawObjectStream:
    """Nonseekable, bounded, uncompressed raw body for tarfile mode r|.

    Every byte is hashed and charged exactly once on application read. Socket
    and TLS prefetch before the application's read is outside this counter.
    """

    def __init__(self, response: requests.Response, lease: str, ledger: BudgetLedger,
                 expected_size: int):
        self.response = response
        self.lease = lease
        self.ledger = ledger
        self.expected_size = expected_size
        self.count = 0
        self._hash = hashlib.sha256()

    def read(self, size: int = -1) -> bytes:
        if size == 0:
            return b""
        remaining = self.expected_size + 1 - self.count
        if remaining <= 0:
            raise RemoteIOError("remote stream exceeded declared size")
        wanted = min(READ_CHUNK if size < 0 else size, remaining, READ_CHUNK)
        if wanted < 0:
            raise ValueError("negative stream read")
        try:
            chunk = self.response.raw.read(wanted, decode_content=False)
        except Exception:
            chunk = None
        if chunk is None:
            raise _AmbiguousRead("raw stream read failed; reservation retained")
        self.ledger.consume_body(self.lease, len(chunk))
        self.count += len(chunk)
        self._hash.update(chunk)
        if self.count > self.expected_size:
            raise RemoteIOError("remote stream exceeded declared size")
        return chunk

    def drain_and_verify(self) -> str:
        """After tarfile stops at EOF headers, include trailing padding/data."""
        while self.read(READ_CHUNK):
            pass
        if self.count != self.expected_size:
            raise RemoteIOError("short remote stream")
        return self._hash.hexdigest()


def validate_same_origin_cookie(value: str | None) -> None:
    """In-memory-only same-origin cookie value; never persisted or logged."""
    if (not isinstance(value, str) or not value or len(value) > 512
            or any(ord(c) < 0x20 or ord(c) == 0x7F for c in value)):
        raise ValueError("same-origin cookie value is unsafe")


def condition_binding_key(*, endpoint: str, repository: str, revision: str,
                          path: str, size: int, strong_etag: str) -> str:
    """Hash the public frozen identity, never a signed redirect URL or token."""
    if (not isinstance(endpoint, str) or not isinstance(repository, str)
            or not isinstance(revision, str) or not isinstance(path, str)
            or not isinstance(strong_etag, str) or type(size) is not int):
        raise ValueError("incomplete condition identity")
    return hashlib.sha256(json.dumps(
        [endpoint, repository, revision, path, size, strong_etag],
        separators=(",", ":")).encode("utf-8")).hexdigest()


class GuardedTransport:
    def __init__(self, ledger: BudgetLedger, *, trusted_hosts: frozenset[str],
                 allow_loopback_http: bool = False, token: str | None = None,
                 credential_origin: str | None = None,
                 same_origin_cookie: str | None = None,
                 max_retry_wait_s: float = 5.0):
        if not trusted_hosts or any(not h or h != h.lower() or ":" in h or "/" in h
                                    for h in trusted_hosts):
            raise ValueError("trusted hosts must be explicit canonical hostnames")
        if ledger.offline_mode:
            if (token is not None or credential_origin is not None
                        or same_origin_cookie is not None):
                raise ValueError("offline HTTP cannot receive credentials")
            if not trusted_hosts.issubset({"127.0.0.1"}):
                raise ValueError("offline transport requires literal IPv4 loopback only")
        self.ledger = ledger
        self._trusted_hosts = frozenset(trusted_hosts)
        self.allow_loopback_http = allow_loopback_http
        self.token = token  # never persisted, never included in exception
        if token and credential_origin is None:
            raise ValueError("credential origin must be fixed in the storage profile")
        if credential_origin is not None:
            parts = urlsplit(credential_origin)
            if (parts.path not in ("", "/") or parts.query or parts.fragment
                    or parts.username or parts.password):
                raise ValueError("credential origin must contain scheme and authority only")
            self._host(credential_origin)
        self.credential_origin = (credential_origin or "").rstrip("/")
        # Design §1: one object identity (everything except the CDN host)
        # binds to at most one exact CDN host, in-memory only, never
        # persisted and never a static allowlist.
        self._twohop_cdn_by_identity: dict[tuple, str] = {}
        if same_origin_cookie is not None:
            # In-memory only; sent exclusively against the exact credential
            # origin (see _attach_origin_cookie). Never persisted or logged.
            validate_same_origin_cookie(same_origin_cookie)
        self.same_origin_cookie = same_origin_cookie
        if not math.isfinite(max_retry_wait_s) or max_retry_wait_s < 0:
            raise ValueError("retry wait must be a finite nonnegative number")
        self.max_retry_wait_s = max_retry_wait_s
        self.session = requests.Session()
        # No netrc credential injection or environment-driven proxy switch.
        # A required proxy must be reviewed/configured explicitly, not guessed.
        self.session.trust_env = False
        no_retry = HTTPAdapter(max_retries=Retry(total=0, redirect=0))
        self.session.mount("https://", no_retry)
        self.session.mount("http://", no_retry)

    @property
    def trusted_hosts(self) -> frozenset[str]:
        """Only configured before network use; cannot grow via Location."""
        return self._trusted_hosts

    def clone(self) -> GuardedTransport:
        """Independent Session for a bounded worker, same persistent ledger."""
        return GuardedTransport(self.ledger, trusted_hosts=self.trusted_hosts,
                                allow_loopback_http=self.allow_loopback_http,
                                token=self.token, credential_origin=self.credential_origin or None,
                                same_origin_cookie=self.same_origin_cookie,
                                max_retry_wait_s=self.max_retry_wait_s)

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> GuardedTransport:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _host(self, url: str) -> str:
        try:
            parts = urlsplit(url)
            hostname = parts.hostname
        except ValueError:
            parts = None
            hostname = None
        if parts is None:
            raise RemoteIOError("invalid target URL")
        if (parts.username or parts.password or parts.fragment or not hostname
                or hostname.lower() not in self.trusted_hosts):
            raise RemoteIOError("untrusted target or redirect")
        if self.ledger.offline_mode:
            # No localhost DNS rebinding, alternate numeric notation, external
            # HTTPS host, proxy or credential-bearing offline request.
            try:
                parsed_ip = ipaddress.ip_address(hostname)
            except ValueError:
                parsed_ip = None
            if parsed_ip != ipaddress.ip_address("127.0.0.1"):
                raise RemoteIOError("offline transport requires literal 127.0.0.1")
            if parts.scheme != "http" or not self.allow_loopback_http:
                raise RemoteIOError("offline transport requires explicit loopback HTTP")
        loopback = (self.allow_loopback_http and parts.scheme == "http"
                    and hostname == "127.0.0.1")
        if parts.scheme != "https" and not loopback:
            raise RemoteIOError("non-HTTPS remote request refused")
        try:
            port = parts.port
        except ValueError:
            port = -1
        if port == -1:
            raise RemoteIOError("invalid remote port")
        if not loopback and port not in (None, 443):
            raise RemoteIOError("nonstandard remote port refused")
        return hostname.lower()

    def _once(self, url: str, *, max_body: int, metadata: bool, inflight: int,
              headers: dict[str, str]):
        self._host(url)
        if self.ledger.offline_mode and (
                self.token is not None or self.credential_origin or
                self.session.trust_env or self.session.proxies or self.session.auth
                or self.session.cookies or any(
                    key.lower() in {"authorization", "proxy-authorization", "cookie"}
                    for key in (*self.session.headers, *headers))):
            raise RemoteIOError("offline transport configuration cannot use credentials/proxy")
        lease = self.ledger.reserve(Reservation(
            body=max_body, metadata=max_body if metadata else 0,
            inflight=inflight, attempt=True))
        self.session.cookies.clear()  # do not preserve server-set cookies across requests
        request_headers = {**headers, "Accept-Encoding": "identity"}
        # Identity comes from the configured profile, NOT this call's URL.
        # A directly supplied trusted CDN URL never receives API credentials.
        if self.token and urlsplit(url).scheme + "://" + urlsplit(url).netloc == \
                self.credential_origin:
            request_headers["Authorization"] = f"Bearer {self.token}"
            self._attach_origin_cookie(url, request_headers)
        try:
            response = self.session.get(url, headers=request_headers,
                                        stream=True, allow_redirects=False,
                                        timeout=(10, 60))
        except Exception:
            response = None
        if response is None:
            # Raise OUTSIDE the handler: even `from None` retains __context__.
            # Unknown bytes retain their full pending reservation.
            raise _AmbiguousRead("network attempt failed; body reservation retained",
                                 phase="metadata_send" if metadata else "transport")
        return response, lease

    def _attach_origin_cookie(self, url: str, headers: dict[str, str]) -> None:
        """Exact-origin same-domain session cookie; never sent to any CDN."""
        if not self.same_origin_cookie or "Cookie" in headers:
            return
        if urlsplit(url).scheme + "://" + urlsplit(url).netloc == self.credential_origin:
            headers["Cookie"] = self.same_origin_cookie

    def two_hop_range(self, url: str, *, start: int, length: int,
                      expected_size: int, batch: str = "", max_bytes: int = 256):
        """One-shot origin 302 -> per-object exact CDN hop2, no credentials.

        Design r5: hop1 expects 302; the Location is validated in memory only
        (strict component normalisation, bounded layered decode, per-layer
        credential check, exact one-shot host approval) and is never persisted.
        Hop2 is a fresh credential-free session. 206 + exact Content-Range and
        Content-Length + identity + non-multipart + strong non-echo ETag are
        required before any body byte; at most `length` bytes plus one overlong
        probe byte are read.

        Ledger: each hop is one attempt; body charges only bytes actually read;
        header-level rejects settle at 0; unknown failures retain the full
        pending reservation (no refund, mirroring _AmbiguousRead semantics).
        """
        if (type(start) is not int or start < 0 or type(length) is not int
                or length < 1 or length > max_bytes or type(expected_size) is not int
                or expected_size < start + length):
            raise ValueError("two-hop range parameters out of bounds")
        self._host(url)
        # Deliberate single-shot attempt: the legacy _response auto-follows
        # 3xx (retried), which the one-hop protocol must not reuse.
        hop1, lease1 = self._once(
            url, max_body=0, metadata=False, inflight=0,
            headers={"Range": f"bytes={start}-{start + length - 1}"})
        try:
            if hop1.status_code != 302:
                hop1.close()
                self.ledger.settle(lease1)  # non-302 body not read: 0 known
                raise RemoteIOError("two-hop expects 302 at origin",
                                    code="redirect_policy",
                                    phase="two_hop_redirect",
                                    http_status=hop1.status_code)
            raw_headers = getattr(getattr(hop1, "raw", None), "headers", None)
            locations = (raw_headers.getlist("Location")
                         if raw_headers is not None and hasattr(raw_headers, "getlist")
                         else [hop1.headers["Location"]] if "Location" in hop1.headers
                         else [])
            if len(locations) != 1 or not locations[0]:
                raise RemoteIOError("two-hop location missing or duplicated",
                                    code="redirect_policy", phase="two_hop_redirect",
                                    http_status=302)
            raw_location = locations[0]
        except RemoteIOError:
            hop1.close()
            self.ledger.settle(lease1)
            raise
        except Exception:
            hop1.close()
            raise _AmbiguousRead("two-hop hop1 handling failed; reservation retained",
                                 phase="two_hop_redirect") from None
        self.ledger.settle(lease1)  # 302 body 0, known
        try:
            # The one-shot approval is generated FROM this hop-1 observation:
            # the gate validates structure/credential safety on the raw
            # Location and every bounded decode layer, and returns the exact
            # host observed HERE. It is a local variable, used only for this
            # hop2 below; it is never cached, persisted, or reused across
            # requests/objects, and no external/static allowlist is consulted.
            approved_host = check_location(
                raw_location, self.credential_origin, self.token,
                offline=self.ledger.offline_mode)
            # Preserve the signed Location exactly for hop2; validation above
            # authorizes only its host/components and never reconstructs its query.
            target = raw_location
        except LocationRejected:
            raise RemoteIOError("two-hop location rejected by gate",
                                code="redirect_policy", phase="two_hop_redirect",
                                http_status=302) from None
        # Fresh credential-free session: no token, no cookies, no environment
        # proxies, no retries, no redirect following.
        session2 = requests.Session()
        session2.trust_env = False
        session2.mount("https://", HTTPAdapter(
            max_retries=Retry(total=0, redirect=0)))
        lease2 = self.ledger.reserve(Reservation(
            body=length + 1, metadata=0, inflight=READ_CHUNK, attempt=True))
        try:
            try:
                hop2 = session2.get(target, headers={
                    "Range": f"bytes={start}-{start + length - 1}",
                    "Accept-Encoding": "identity"},
                    stream=True, allow_redirects=False, timeout=(10, 60))
            except Exception:
                raise _AmbiguousRead(
                    "two-hop hop2 attempt failed; reservation retained",
                    phase="two_hop_redirect") from None
            status = hop2.status_code
            if status == 206:
                m = _CONTENT_RANGE.fullmatch(hop2.headers.get("Content-Range") or "")
                if (m is None or int(m.group(1)) != start
                        or int(m.group(2)) != start + length - 1
                        or int(m.group(3)) != expected_size):
                    hop2.close()
                    self.ledger.settle(lease2)
                    raise RemoteIOError("two-hop content-range mismatch",
                                        code="http_status", phase="response_headers",
                                        http_status=206)
                if hop2.headers.get("Content-Length") != str(length):
                    hop2.close()
                    self.ledger.settle(lease2)
                    raise RemoteIOError("two-hop content-length mismatch",
                                        code="http_status", phase="response_headers",
                                        http_status=206)
                if (hop2.headers.get("Content-Encoding", "identity").lower()
                        != "identity" or "multipart"
                        in (hop2.headers.get("Content-Type") or "").lower()):
                    hop2.close()
                    self.ledger.settle(lease2)
                    raise RemoteIOError("two-hop body framing rejected",
                                        code="http_status", phase="response_headers",
                                        http_status=206)
                etag = hop2.headers.get("ETag") or ""
                try:
                    check_token_not_in_etag(etag, self.token)
                except LocationRejected:
                    hop2.close()
                    self.ledger.settle(lease2)
                    raise RemoteIOError("two-hop validator rejected",
                                        code="http_status", phase="response_headers",
                                        http_status=206) from None
                try:
                    data = bytearray()
                    got = 0
                    while got < length + 1:
                        chunk = hop2.raw.read(min(READ_CHUNK, length + 1 - got),
                                              decode_content=False)
                        if not chunk:
                            break
                        got += len(chunk)
                        self.ledger.consume_body(lease2, len(chunk))
                        data.extend(chunk)
                except Exception:
                    raise _AmbiguousRead(
                        "two-hop body read failed; reservation retained",
                        phase="two_hop_redirect") from None
                if got != length:
                    # Overlong or short: every arrived byte is known and charged.
                    self.ledger.settle(lease2)
                    raise RemoteIOError("two-hop entity length rejected",
                                        code="http_status", phase="response_headers",
                                        http_status=206)
                hop2.close()
                self.ledger.settle(lease2)
                return TwoHopResult(
                    hop1_status=302, hop2_status=206, approved_host=approved_host,
                    etag=etag, bytes_read=len(data), payload=bytes(data[:length]),
                    attempts=2, batch=batch)
            hop2.close()
            self.ledger.settle(lease2)  # header-level reject: body 0, known
            raise RemoteIOError("two-hop hop2 status rejected",
                                code="http_status", phase="response_headers",
                                http_status=status if type(status) is int
                                and 100 <= status <= 599 else None)
        except _AmbiguousRead:
            raise  # full pending reservation retained; never refunded
        except RemoteIOError:
            raise  # validated rejection already settled its lease
        except Exception:
            raise _AmbiguousRead("two-hop unknown failure; reservation retained",
                                 phase="two_hop_redirect") from None
        finally:
            session2.close()

    def record_two_hop_proof(self, *, origin_endpoint: str, repository: str,
                             repository_type: str, revision: str, path: str,
                             size: int, etag: str, cdn_host: str,
                             payload_sha: str, observed_batch: str = "") -> str:
        """Store a v2 two-hop conditional probe digest; no network is made.

        Key and record are cross-checked against each other; the v2 domain is
        disjoint from the legacy single-hop condition_binding_key domain by
        construction (the domain constant cannot appear in a v1 payload).
        """
        key = two_hop_proof_key(origin_endpoint=origin_endpoint,
                                repository=repository,
                                repository_type=repository_type,
                                revision=revision, path=path, size=size,
                                etag=etag, cdn_host=cdn_host)
        if (not isinstance(payload_sha, str) or len(payload_sha) != 64
                or not all(c in "0123456789abcdef" for c in payload_sha)):
            raise TwoHopKeyError("invalid probe digest")
        cdn_norm = normalize_host(cdn_host)
        identity = (normalize_endpoint(origin_endpoint), repository,
                    repository_type, revision, path, size, etag)
        bound = self._twohop_cdn_by_identity.get(identity)
        if bound is not None and bound != cdn_norm:
            raise TwoHopKeyError("two-hop object identity is already bound "
                                 "to another CDN host")
        record = validate_two_hop_record(dict(
            schema_version="transport_validator_v1", kind="cdn_strong_etag",
            etag_value=etag, repository=repository,
            repository_type=repository_type,
            origin_endpoint=origin_endpoint,
            origin_host=normalize_endpoint(origin_endpoint),
            cdn_host=normalize_host(cdn_host), hop_count=2,
            revision_candidate=revision, path=path, size=size,
            policy_profile_descriptor=repository_type,
            observed_batch=observed_batch or "unbound"),
            expected_repository=repository)
        if two_hop_record_key(record) != key:
            raise RemoteIOError("two-hop proof key/record domain mismatch",
                                code="redirect_policy", phase="two_hop_redirect")
        self._twohop_cdn_by_identity[identity] = cdn_norm
        self.ledger.record_condition_proof(key, payload_sha)
        return key

    def _retry_delay(self, retry_after: str | None, attempt: int) -> float:
        if retry_after is None:
            return random.uniform(0, 0.1 * 2**attempt)
        try:
            seconds = float(retry_after)
        except ValueError:
            seconds = None
        if seconds is None:
            try:
                moment = parsedate_to_datetime(retry_after)
            except (ValueError, TypeError, IndexError):
                moment = None
            if moment is None:
                raise RemoteIOError("invalid Retry-After; stop", code="retry_policy")
            if moment.tzinfo is None:
                raise RemoteIOError("Retry-After missing timezone; stop")
            seconds = (moment - datetime.now(timezone.utc)).total_seconds()
        if not math.isfinite(seconds) or seconds > self.max_retry_wait_s:
            raise RemoteIOError("Retry-After exceeds configured wait; stop",
                                code="retry_policy")
        return max(0.0, seconds)

    @staticmethod
    def _pinned_refresh_origin(bound: BoundObject) -> str | None:
        """Check an exact provider GET for a frozen revision, without retrying.

        This validator does NOT authorize a refresh on 403; expiry recovery is
        unavailable until provider semantics are independently established.

        The provider path segment is rebuilt from the PERSISTENT repository
        identity sealed into the bound object (parse_repository output), never
        from a hardcoded repository name: a bound object without a parsed
        repository configuration refuses the refresh origin entirely.
        """
        if (not bound.immutable_revision or not bound.strong_etag
                or bound.repository_id is None):
            return None
        parsed = urlsplit(bound.url)
        expected_repo_path = ("/api/v1/datasets"
                              f"/{bound.repository_id.owner}"
                              f"/{bound.repository_id.name}/repo")
        if (parsed.path != expected_repo_path
                or parsed.fragment or parsed.username or parsed.password):
            return None
        try:
            params = parse_qs(parsed.query, keep_blank_values=True,
                              strict_parsing=True)
        except ValueError:
            return None
        if set(params) != {"Revision", "FilePath"} or params["Revision"] != [
                bound.immutable_revision]:
            return None
        from ..records import canonical_object_id
        if len(params["FilePath"]) != 1:
            return None
        path = params["FilePath"][0]
        try:
            canonical_object_id(path)
        except (TypeError, ValueError, IndexError):
            return None
        # Exact SDK-audited provider builder order/encoding; no duplicate
        # FilePath, permissive query aliases, extra signature or fragment.
        if parsed.query != urlencode({"Revision": bound.immutable_revision,
                                      "FilePath": path}):
            return None
        return bound.url

    def _response(self, url: str, *, max_body: int, metadata: bool, inflight: int,
                  headers: dict[str, str], refresh_origin: str | None = None):
        """At most 3 attempts; any 403 fails closed, even at a signed redirect.

        A canonical refresh origin can be checked but must NOT be used until an
        externally verified provider-specific expiry signal is established.
        """
        self._host(url)
        if refresh_origin is not None and refresh_origin != url:
            raise RemoteIOError("refresh origin differs from frozen object")
        for attempt in range(MAX_ATTEMPTS):
            try:
                response, lease = self._once(url, max_body=max_body, metadata=metadata,
                                             inflight=inflight, headers=headers)
            except _AmbiguousRead:
                # The failed attempt and full unknown body remain charged.
                if attempt + 1 < MAX_ATTEMPTS:
                    time.sleep(self._retry_delay(None, attempt))
                    continue
                raise
            status = response.status_code
            if status in (301, 302, 303, 307, 308):
                try:
                    target = urljoin(url, response.headers.get("Location", ""))
                    self._host(target)  # checked *before* next request
                except (ValueError, RemoteIOError):
                    target = None
                response.close()
                self.ledger.settle(lease)
                if target is None:
                    raise RemoteIOError("unsafe redirect target", code="redirect_policy",
                                        phase="response_headers")
                url = target
                continue
            if status in (429, 500, 502, 503, 504):
                retry_after = response.headers.get("Retry-After")
                response.close()
                self.ledger.settle(lease)
                if attempt + 1 < MAX_ATTEMPTS:
                    time.sleep(self._retry_delay(retry_after, attempt))
                    continue
                raise RemoteIOError("bounded retry attempts exhausted")
            return response, lease
        raise RemoteIOError("bounded redirect attempts exhausted")

    def _read_bounded(self, response: requests.Response, lease: str, limit: int,
                      *, metadata: bool, exact: bool) -> bytes:
        chunks: list[bytes] = []
        got = 0
        try:
            while got <= limit:
                # One extra byte, already reserved, detects overlong entities.
                chunk = response.raw.read(min(READ_CHUNK, limit + 1 - got),
                                          decode_content=False)
                if not chunk:
                    break
                got += len(chunk)
                self.ledger.consume_body(lease, len(chunk), metadata=metadata)
                chunks.append(chunk)
        except Exception:
            failed = True
        else:
            failed = False
        if failed:
            raise _AmbiguousRead("raw body read failed; reservation retained",
                                 phase="metadata_body" if metadata else "transport")
        if got > limit or (exact and got != limit):
            raise RemoteIOError("short or overlong entity body")
        return b"".join(chunks)

    @contextmanager
    def stream_object(self, bound: BoundObject) -> Iterator[RawObjectStream]:
        """Explicit administrator-only full-object scan; never a Range fallback.

        A successful caller must let the context drain the raw entity through
        EOF, then use the stream SHA to assign the final v4 object identity.
        Parser failure retains a conservative pending lease, not a COMMIT.
        """
        if bound.size > 2 * (1 << 30):
            raise ValueError("single TAR exceeds authorized 2 GiB")
        headers = {"If-Match": bound.strong_etag} if bound.strong_etag else {}
        response, lease = self._response(bound.url, max_body=bound.size + 1,
                                         metadata=False, inflight=2 * READ_CHUNK,
                                         headers=headers,
                                         refresh_origin=self._pinned_refresh_origin(bound))
        try:
            if response.status_code != 200:
                raise RemoteIOError("full stream must be explicitly returned as 200")
            if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                raise RemoteIOError("encoded object stream refused")
            declared = response.headers.get("Content-Length")
            if declared is not None and declared != str(bound.size):
                raise RemoteIOError("stream Content-Length mismatch")
            if bound.strong_etag and response.headers.get("ETag") != bound.strong_etag:
                raise RemoteIOError("stream ETag changed or missing")
            stream = RawObjectStream(response, lease, self.ledger, bound.size)
            yield stream
            stream.drain_and_verify()
        except _AmbiguousRead:
            response.close()
            raise  # raw bytes unknown; pending remains unavailable
        except BaseException:
            response.close()
            # Conservative on parser failure: no valid completed object and
            # no proof that no response bytes arrived; keep pending quota.
            raise
        response.close()
        self.ledger.settle(lease)

    @contextmanager
    def read_range_owned(self, bound: BoundObject, offset: int,
                         length: int) -> Iterator[bytes]:
        """Keep reserved in-flight quota through the consumer's acknowledgement.

        Use this for fetch workers and their bounded completion queue; readers
        MUST exit the context only once data were consumed/written. A worker
        must not return the raw bytes to an unbounded future after closing.
        """
        if (type(offset) is not int or type(length) is not int or offset < 0 or length < 0
                or offset >= 2**64 or length >= 2**64 or offset + length > bound.size
                or length > MAX_MEMBER):
            raise ValueError("range outside bound object, uint64, or 64 MiB member cap")
        if length == 0:
            yield b""  # no HTTP request and no attempt quota
            return
        end = offset + length - 1
        headers = {"Range": f"bytes={offset}-{end}"}
        if bound.strong_etag:
            headers["If-Match"] = bound.strong_etag
        response, lease = self._response(bound.url, max_body=length + 1,
                                         metadata=False,
                                         inflight=3 * (length + 1) + 2 * READ_CHUNK,
                                         headers=headers,
                                         refresh_origin=self._pinned_refresh_origin(bound))
        try:
            if response.status_code != 206:  # 200 is NEVER a full-object fallback
                raise RemoteIOError("range refused: response is not 206")
            encoding = response.headers.get("Content-Encoding", "identity").lower()
            if encoding != "identity" or "multipart/" in response.headers.get(
                "Content-Type", "").lower():
                raise RemoteIOError("encoded or multipart range refused")
            parsed = _CONTENT_RANGE.fullmatch(response.headers.get("Content-Range", ""))
            if parsed is None or tuple(map(int, parsed.groups())) != (
                offset, end, bound.size
            ):
                raise RemoteIOError("Content-Range does not match object and request")
            provided_length = response.headers.get("Content-Length")
            if provided_length is not None and provided_length != str(length):
                raise RemoteIOError("Content-Length does not equal requested length")
            if bound.strong_etag and response.headers.get("ETag") != bound.strong_etag:
                raise RemoteIOError("strong ETag changed or missing")
            body = self._read_bounded(response, lease, length, metadata=False, exact=True)
        except _AmbiguousRead:
            response.close()  # unknown actual bytes: pending reservation is NOT refunded
            raise
        except RemoteIOError:
            response.close()
            self.ledger.settle(lease)  # validated reject, known short/overlong
            raise
        except BaseException:
            response.close()  # unknown failure: NEVER refund pending
            raise
        response.close()
        try:
            yield body
        except BaseException:
            # Bytes may be handed off to another owner after a consumer fault.
            # Preserve pending inflight until verified cleanup/recovery.
            raise
        else:
            self.ledger.settle(lease)

    def read_range(self, bound: BoundObject, offset: int, length: int) -> bytes:
        """One-off synchronous reader; no future/queue may own returned bytes.

        Concurrent fetchers MUST use read_range_owned through consumer ack.
        """
        with self.read_range_owned(bound, offset, length) as body:
            return body

    def verify_if_match(self, bound: BoundObject) -> str:
        """Budgeted positive AND negative conditional test on the same object.

        Required before authorizing real scan/fetch for the strong-ETag mode.
        Distinguishes a server returning ETag while ignoring If-Match; it does
        not mistake a 40-hex revision parameter for proof. Each attempt is
        charged (including any redirects). Returns the positive byte SHA only,
        not a signed URL or token. A provider with absent/ignored conditions
        must block real work instead of silently accepting a mutable object.
        """
        if not bound.size or not bound.strong_etag:
            raise ValueError("nonempty object with strong ETag is required")
        first = self.read_range(bound, 0, 1)
        mismatch = hashlib.sha256(bound.strong_etag.encode("ascii")).hexdigest()
        negative = {"Range": "bytes=0-0", "If-Match": f'"p4-invalid-{mismatch}"'}
        response, lease = self._response(bound.url, max_body=2, metadata=False,
                                         inflight=4, headers=negative,
                                         refresh_origin=self._pinned_refresh_origin(bound))
        try:
            if response.status_code != 412:
                raise RemoteIOError("server did not enforce If-Match; do not scan")
        except BaseException:
            response.close()
            self.ledger.settle(lease)
            raise
        response.close()  # 412 entity is not read; socket/TLS prefetch may occur
        self.ledger.settle(lease)
        return hashlib.sha256(first).hexdigest()

    def ensure_verified_condition(self, bound: BoundObject, *, endpoint: str,
                                  repository: str, path: str,
                                  expected_probe_sha256: str | None = None) -> str:
        """Persistent, guarded conditional proof bound to immutable identity.

        The pre-scan probe cannot know full TAR content SHA. Its proof key
        binds exactly the provider object/profile/revision/path/size/ETag;
        the completed raw scan separately assigns SHA and RecordKey. The
        package consumer checks that SHA against the v4 snapshot and hashes
        each Range member. Fresh ledgers probe; repeat reads of the same
        conditional binding reuse the guarded ledger, NOT a manifest boolean.

        Legacy single-hop domain only: the hashed identity must never contain
        the v2 two-hop domain constant (two-hop keys never resolve here).

        The repository is the RUNTIME configuration value, strictly parsed once
        (parse_repository, fail-closed). The parsed owner/name are additionally
        cross-checked against the bound object URL's provider path: a legacy
        record stored under one repository cannot be re-bound to a different
        configured repository, so reconfiguring can never resurrect an old
        record under a new configuration.
        """
        if (not bound.immutable_revision or (expected_probe_sha256 is not None
                and (not isinstance(expected_probe_sha256, str)
                     or not re.fullmatch(r"[0-9a-f]{64}", expected_probe_sha256)))):
            raise ValueError("conditional binding lacks frozen provider identity")
        try:
            repo = parse_repository(repository)
        except RepositoryConfigError:
            raise ValueError("unapproved proof repository") from None
        from .location_gate import TWOHOP_DOMAIN
        assert TWOHOP_DOMAIN not in json.dumps(
            [endpoint, repository, bound.immutable_revision, path, bound.size,
             bound.strong_etag], separators=(",", ":")), "legacy key domain polluted"
        self._host(endpoint)
        self._host(bound.url)
        if not path or ":" in path or ".." in path:
            raise ValueError("unapproved proof repository/path")
        # Isolation note: the condition key itself hashes the repository, so
        # records stored under one configured repository are never addressable
        # under another; the bound object additionally seals the parsed
        # repository (BoundObject) when it is a provider repo route, and the
        # v2 two-hop domain stays disjoint from this legacy key by
        # construction. Reconfiguring can therefore not resurrect old records.
        del repo
        key = condition_binding_key(
            endpoint=endpoint, repository=repository, revision=bound.immutable_revision,
            path=path, size=bound.size, strong_etag=bound.strong_etag)
        stored = self.ledger.condition_proof(key)
        if stored is not None:
            if expected_probe_sha256 is not None and stored != expected_probe_sha256:
                raise RemoteIOError("manifest/ledger conditional proof mismatch")
            return stored
        observed = self.verify_if_match(bound)
        if expected_probe_sha256 is not None and observed != expected_probe_sha256:
            raise RemoteIOError("manifest conditional probe bytes mismatch")
        self.ledger.record_condition_proof(key, observed)
        return observed

    def read_metadata(self, url: str, *, max_bytes: int = MIB) -> bytes:
        """Guarded, bounded provider-listing response; no SDK bypass."""
        if max_bytes < 0 or max_bytes > 64 * MIB:
            raise ValueError("metadata single-response cap exceeded")
        response, lease = self._response(url, max_body=max_bytes + 1,
                                         metadata=True, inflight=2 * (max_bytes + 1),
                                         headers={})
        try:
            if response.status_code != 200:
                status = response.status_code
                raise RemoteIOError("metadata response rejected", code="http_status",
                                    phase="metadata_headers",
                                    http_status=status if type(status) is int
                                    and 100 <= status <= 599 else None)
            if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                raise RemoteIOError("metadata response rejected", code="metadata_encoding",
                                    phase="metadata_headers", http_status=200)
            body = self._read_bounded(response, lease, max_bytes, metadata=True,
                                      exact=False)
        except _AmbiguousRead:
            response.close()
            raise
        except RemoteIOError:
            response.close()
            self.ledger.settle(lease)  # validated reject/overlong with known raw count
            raise
        except BaseException:
            response.close()
            raise
        response.close()
        self.ledger.settle(lease)
        return body
