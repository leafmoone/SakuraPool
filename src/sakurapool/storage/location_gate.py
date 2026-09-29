"""Two-hop (origin -> per-object exact CDN) location gate, v2 proof key and
transport validator schema.

Pure in-memory logic: no network, no logging. The raw Location and its signed
query never leave the process memory of the request chain and never appear in
exceptions (only fixed diagnostic strings are raised). Implements the reviewed
design v1 r5:

- strict URL component normalisation: form '+', malformed percent escapes,
  userinfo, fragment, non-443 ports, IP literals are all REJECTED;
- bounded layered percent decoding (MAX_DECODE_DEPTH layers) with per-layer
  byte caps, rejecting non-converging/ambiguous encodings;
- per-layer token (and its full percent-encoded form) detection;
- per-object one-shot exact CDN host approval (never a static allowlist);
- v2 two-hop conditional proof key domain, disjoint from the legacy
  single-hop condition_binding_key domain by construction (the v2 payload
  always carries the domain constant, which no v1 payload can).
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
from dataclasses import dataclass

# Frozen implementation constants (reviewed together with the design).
MAX_DECODE_DEPTH = 2
MAX_LAYER_BYTES = 8 * 1024
MAX_LOCATION_BYTES = 4 * 1024

TWOHOP_DOMAIN = "p4-condproof-v2-twohop"
TWOHOP_SCHEMA_VERSION = "transport_validator_v1"
PROFILE_DESCRIPTORS = frozenset({"modelscope_dataset_legacy"})

_HOST_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_HOSTNAME = re.compile(
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+")
_TAG = re.compile(r'"[\x21\x23-\x7e]+"\Z')
# §3bis query encoding allowlist. Signed CDN values legitimately carry only
# these escapes (verified on the approved hop-1 observation: %2F %3D, with
# %2B standard base64url padding); EVERY other percent sequence in the query
# is rejected (fail-closed). In the PATH, ANY escape is rejected: real object
# paths are clean ASCII, so a percent sequence there is a mixed/partial
# encoding and is always suspicious.
_QUERY_ALLOWED_ESCAPES = frozenset(("2F", "3D", "2B"))
_REVISION = re.compile(r"[0-9a-f]{40}\Z|[0-9a-f]{64}\Z")
_SEG = re.compile(r"[A-Za-z0-9_.-]+\Z")


def _is_canonical_path(path: str) -> bool:
    """Canonical relative object path: no traversal, no absolute, no blanks."""
    if not isinstance(path, str) or not path:
        return False
    if path.startswith("/") or path.endswith("/"):
        return False
    if ("\\" in path or " " in path or "\t" in path
            or any(ord(c) < 0x20 or ord(c) == 0x7F for c in path)):
        return False
    for seg in path.split("/"):
        if not seg or seg in (".", "..") or not _SEG.fullmatch(seg):
            return False
    return True


class LocationRejected(ValueError):
    """Fixed-string reject; carries no raw URL, signature or credential."""


class TwoHopKeyError(ValueError):
    """Proof-key domain misuse; never carries raw key material."""


class RepositoryConfigError(ValueError):
    """A runtime repository configuration string failed strict parsing."""


@dataclass(frozen=True)
class RepositoryId:
    """Structured owner/name parsed ONCE from a runtime configuration string.

    Every URL, probe identity and proof key downstream must use these parsed
    components; the raw configuration string is never re-split or re-parsed.
    No fixed repository allowlist exists: any SYNTACTICALLY valid owner/name
    is accepted as a runtime configuration; whether ModelScope actually has
    that repository is the provider's answer (a clear not-found error), not
    this parser's. The fail-closed boundary here is SYNTAX plus full identity
    binding (cross-repository reuse is rejected by construction).
    """

    owner: str
    name: str

    @property
    def id(self) -> str:
        return f"{self.owner}/{self.name}"


_REPO_PART = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}\Z")


def parse_repository(repo_id: str) -> RepositoryId:
    """The single repository parser (fail-closed).

    Exactly one '/' separating a nonempty owner and name; each part is
    1-100 chars of [A-Za-z0-9_.-] starting alphanumerically, never ending
    with a dot/dash/underscore. This is a SYNTAX boundary only: it does not
    enumerate or allowlist provider repositories; an unknown-but-well-formed
    repository is accepted here and surfaces as a clear provider error.
    Consecutive dots ("a..b") are rejected with a dedicated diagnostic, so
    ANY ".." substring is refused. Also rejects: non-str, empty, extra or
    missing slash, percent signs (pre-encoded traversal), backslashes,
    control characters, blanks and overlong parts. Callers must use the
    returned RepositoryId, not re-parse.
    """
    if not isinstance(repo_id, str) or not repo_id or len(repo_id) > 201:
        raise RepositoryConfigError("repository must be a bounded nonempty string")
    if "\\" in repo_id or "%" in repo_id or any(ord(c) < 0x20 or ord(c) == 0x7F
                                                 for c in repo_id):
        raise RepositoryConfigError("repository contains forbidden characters")
    parts = repo_id.split("/")
    if len(parts) != 2:
        raise RepositoryConfigError("repository must be owner/name with exactly one slash")
    owner, name = parts
    for part in (owner, name):
        # Explicit consecutive-dot / ".." rejection (dedicated diagnostic),
        # then the strict charset (dots are not allowed at all), then the
        # trailing punctuation rule.
        if ".." in part:
            raise RepositoryConfigError("repository part carries consecutive dots")
        if not _REPO_PART.fullmatch(part):
            raise RepositoryConfigError("repository part is not owner/name safe")
        if part[-1] in "._-":
            raise RepositoryConfigError(
                "repository part cannot end with dot, dash or underscore")
    return RepositoryId(owner=owner, name=name)


def _reject(reason: str) -> None:
    raise LocationRejected(reason)


def _is_ip_literal(value: str) -> bool:
    try:
        ipaddress.ip_address(value)
        return True
    except ValueError:
        return False


def normalize_host(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise TwoHopKeyError("host must be a nonempty string")
    lowered = value.lower()
    if ":" in lowered or "/" in lowered or " " in lowered:
        raise TwoHopKeyError("host must be bare")
    return lowered


def normalize_endpoint(value: str) -> str:
    """Lowercased host of an https origin; 443 implicit; no userinfo/query."""
    if not isinstance(value, str):
        raise TwoHopKeyError("endpoint must be a string")
    from urllib.parse import urlsplit
    try:
        parts = urlsplit(value)
    except ValueError:
        raise TwoHopKeyError("unparseable endpoint") from None
    if (parts.scheme != "https" or not parts.hostname or parts.port not in
            (None, 443) or parts.path not in ("", "/") or parts.query or
            parts.fragment or parts.username or parts.password):
        raise TwoHopKeyError("endpoint must be a bare https origin")
    return normalize_host(parts.hostname)


def _strict_decode_layer(raw: str) -> str:
    """One strict full percent-decode pass; '+' is NOT decoded (form reject)."""
    out = bytearray()
    i = 0
    n = len(raw)
    while i < n:
        ch = raw[i]
        if ch == "%":
            if i + 2 > n - 1 or not all(c in "0123456789abcdefABCDEF" for c in raw[i + 1:i + 3]):
                _reject("malformed percent escape")
            out.append(int(raw[i + 1:i + 3], 16))
            i += 3
        else:
            out.append(ord(ch))
            i += 1
    return out.decode("latin-1")


def _encoded_token_forms(token: str) -> list[str]:
    if not isinstance(token, str) or not token:
        return []
    full = "".join(f"%{b:02X}" for b in token.encode("utf-8"))
    return [full, full.lower()]


def _check_components(parts, *, layer_host: str, origin_host: str,
                      offline_loopback: bool, form_plus: bool) -> None:
    """Full component safety check for one URL layer (raw or decoded).

    `layer_host` is the host observed in THIS layer; the same check is applied
    to every bounded decode layer so a decode cannot silently introduce
    userinfo, a fragment, a host change, or a disallowed escape. The form-plus
    rule applies only to the RAW layer: a '+' in a decoded layer is the
    decoded form of an approved %2B (literal plus), not form encoding.
    """
    if parts.username is not None or parts.password is not None:
        _reject("location userinfo rejected")
    if parts.fragment:
        _reject("location fragment rejected")
    hostname = parts.hostname
    if not hostname or len(hostname) > 253:
        _reject("location host rejected")
    lowered = hostname.lower()
    if lowered != layer_host:
        _reject("decoded layer host mismatch")
    if origin_host and lowered == origin_host and not offline_loopback:
        _reject("location host equals origin")
    if parts.scheme != "https" and not offline_loopback:
        _reject("location scheme rejected")
    if not offline_loopback and (_is_ip_literal(lowered)
            or not _HOSTNAME.fullmatch(lowered)):
        _reject("location host must be a dns hostname")
    port = parts.port
    if port not in (None, 443) and not offline_loopback:
        _reject("location port rejected")
    if "\t" in parts.path or "\n" in parts.path or "\r" in parts.path:
        _reject("location path contains control character")
    if "%" in parts.path:
        _reject("path percent escape rejected")
    if form_plus and parts.query and "+" in parts.query:
        _reject("location form plus rejected")
    for m in re.finditer(r"%([0-9A-Fa-f]{2})", parts.query):
        if m.group(1).upper() not in _QUERY_ALLOWED_ESCAPES:
            _reject("query percent escape not approved")


def check_location(raw_location: str, origin_endpoint: str, token: str | None,
                   *, offline: bool = False) -> str:
    """Validate a hop-1 Location and return its ONE exact observed host.

    Design r5 §1/§3bis (r2 revision): the approval is generated FROM this
    hop-1 observation, per request/object, in memory, one-shot. The gate
    enforces structure and credential safety on the raw location AND on every
    bounded decode layer (scheme, DNS-shape host, port, userinfo, fragment,
    path escapes, query allowlist, form plus, token echo); the host returned
    is exactly the host observed in this Location (lowercased DNS identity) —
    never a superset, never a static/external allowlist entry. If an origin
    endpoint (credential origin) is given, it must be a bare https origin and
    the Location must not point back to it.

    Returns the exact observed CDN hostname (lowercase). Raises
    LocationRejected with a fixed string on any violation; never the raw URL.
    """
    if not isinstance(raw_location, str) or not raw_location:
        _reject("location missing or empty")
    if len(raw_location.encode("utf-8", "replace")) > MAX_LOCATION_BYTES:
        _reject("location length exceeded")
    if any(ord(c) < 0x20 or ord(c) == 0x7F for c in raw_location):
        _reject("location control character")
    from urllib.parse import urlsplit
    try:
        parts = urlsplit(raw_location)
    except ValueError:
        _reject("location unparseable")
    hostname = parts.hostname
    if not hostname or len(hostname) > 253:
        _reject("location host rejected")
    lowered = hostname.lower()
    if origin_endpoint:
        try:
            origin_host = normalize_endpoint(origin_endpoint)
        except TwoHopKeyError:
            _reject("origin endpoint rejected")
    else:
        origin_host = ""
    offline_loopback = offline and lowered == "127.0.0.1"
    _check_components(parts, layer_host=lowered, origin_host=origin_host,
                      offline_loopback=offline_loopback, form_plus=True)
    layers = [raw_location]
    for _ in range(MAX_DECODE_DEPTH):
        prev = layers[-1]
        if "%" not in prev:
            break
        decoded = _strict_decode_layer(prev)
        if len(decoded.encode("latin-1")) > MAX_LAYER_BYTES:
            _reject("layer byte cap exceeded")
        if any(ord(c) < 0x20 or ord(c) == 0x7F for c in decoded):
            _reject("layer control character")
        if decoded == prev:
            break  # non-converging; stop, do not loop
        try:
            dp = urlsplit(decoded)
        except ValueError:
            _reject("decoded layer unparseable")
        _check_components(dp, layer_host=lowered, origin_host=origin_host,
                          offline_loopback=offline_loopback, form_plus=False)
        layers.append(decoded)
    if token:
        forms = [token, *_encoded_token_forms(token)]
        for layer in layers:
            if any(form in layer for form in forms if form):
                _reject("credential echo detected")
    return lowered


def check_token_not_in_etag(etag: str, token: str | None) -> None:
    if not _TAG.fullmatch(etag):
        _reject("etag malformed")
    if token:
        forms = [token, *_encoded_token_forms(token)]
        if any(form in etag for form in forms if form):
            _reject("credential echo detected")


def two_hop_proof_key(*, origin_endpoint: str, repository: str, repository_type: str,
                      revision: str, path: str, size: int, etag: str,
                      cdn_host: str) -> str:
    """v2 two-hop proof key; domain-disjoint from condition_binding_key.

    The domain constant inside the hashed payload makes collision with the
    legacy single-hop key domain structurally impossible (a v1 payload never
    contains the constant). Returns a 64-hex key, storable by the existing
    ledger proof table.
    """
    if repository_type not in PROFILE_DESCRIPTORS:
        raise TwoHopKeyError("unapproved profile descriptor")
    origin_host = normalize_endpoint(origin_endpoint)
    cdn = normalize_host(cdn_host)
    if (not isinstance(repository, str) or not repository or
            not isinstance(revision, str) or not revision or
            not isinstance(path, str) or not path or
            not isinstance(etag, str) or not etag or type(size) is not int or
            size < 0 or size >= 2**64):
        raise TwoHopKeyError("incomplete two-hop identity")
    try:
        parse_repository(repository)
    except RepositoryConfigError:
        raise TwoHopKeyError("repository not a valid configuration owner/name") from None
    if not _REVISION.fullmatch(revision):
        raise TwoHopKeyError("revision not a valid object candidate")
    if not _is_canonical_path(path):
        raise TwoHopKeyError("path not canonical for two-hop proof")
    payload = json.dumps([TWOHOP_DOMAIN, TWOHOP_SCHEMA_VERSION, origin_host,
                          repository, repository_type, revision, path, size,
                          etag, cdn], separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class TwoHopRecord:
    schema_version: str
    kind: str
    etag_value: str
    repository: str
    repository_type: str
    origin_endpoint: str
    origin_host: str
    cdn_host: str
    hop_count: int
    revision_candidate: str
    path: str
    size: int
    policy_profile_descriptor: str
    observed_batch: str


_RECORD_FIELDS = frozenset(TwoHopRecord.__dataclass_fields__)


class _Missing:
    """Sentinel: expected_repository has NO default; omission is a contract
    refusal, not a lenient fallback."""

    def __repr__(self) -> str:
        return "<missing expected_repository>"


_MISSING = _Missing()


def validate_two_hop_record(
    record: dict, *, expected_repository: "str | _Missing" = _MISSING) -> TwoHopRecord:
    """Fail-closed reader-side validation; unknown/old/foreign profiles refuse.

    expected_repository (the RUNTIME configuration value) is REQUIRED: there
    is no default. Omission is a contract refusal via the explicit sentinel
    (fixed diagnostic, not an accidental fallback); a malformed or
    non-matching configuration refuses the record. There is no format-only
    lenient path; records from any other repo can never be read back under a
    different configuration.
    """
    if expected_repository is _MISSING:
        raise TwoHopKeyError("expected_repository is required")
    try:
        expected = parse_repository(expected_repository)
    except (RepositoryConfigError, TypeError):
        raise TwoHopKeyError("expected_repository is missing or invalid") from None
    if not isinstance(record, dict) or set(record) != _RECORD_FIELDS:
        raise TwoHopKeyError("schema field set mismatch")
    try:
        built = TwoHopRecord(**record)
    except (TypeError, ValueError):
        raise TwoHopKeyError("schema field type mismatch") from None
    if built.schema_version != TWOHOP_SCHEMA_VERSION:
        raise TwoHopKeyError("unknown schema version")
    if built.kind != "cdn_strong_etag":
        raise TwoHopKeyError("unknown validator kind")
    if built.hop_count != 2:
        raise TwoHopKeyError("hop count mismatch")
    if built.repository != expected.id:
        raise TwoHopKeyError("record repository does not match expected_repository")
    if built.repository_type not in PROFILE_DESCRIPTORS:
        raise TwoHopKeyError("unapproved repository type")
    if built.policy_profile_descriptor != built.repository_type:
        raise TwoHopKeyError("profile descriptor must match repository type")
    if not _REVISION.fullmatch(built.revision_candidate):
        raise TwoHopKeyError("revision not a valid object candidate")
    if not _is_canonical_path(built.path):
        raise TwoHopKeyError("path not canonical for two-hop proof")
    if not built.observed_batch:
        raise TwoHopKeyError("missing observation batch")
    if not _TAG.fullmatch(built.etag_value):
        raise TwoHopKeyError("validator tag malformed")
    if (normalize_endpoint(built.origin_endpoint) != built.origin_host
            or normalize_host(built.cdn_host) != built.cdn_host
            or built.origin_host == built.cdn_host):
        raise TwoHopKeyError("host fields not normalized or degenerate")
    if built.size < 0 or built.size >= 2**64:
        raise TwoHopKeyError("size out of range")
    return built


def two_hop_record_key(record: TwoHopRecord) -> str:
    return two_hop_proof_key(
        origin_endpoint=record.origin_endpoint, repository=record.repository,
        repository_type=record.repository_type, revision=record.revision_candidate,
        path=record.path, size=record.size, etag=record.etag_value,
        cdn_host=record.cdn_host)
