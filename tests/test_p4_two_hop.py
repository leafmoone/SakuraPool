"""Offline tests for the two-hop (origin -> per-object exact CDN) capability.

No external network: the gate and v2 proof domain are pure logic, and the
transport two-hop path is exercised against a local loopback pair of HTTP
servers (origin + CDN) under the offline test ledger.
"""

import hashlib
import inspect
import json
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from sakurapool.storage.budget import DEFAULT_WORK_ROOT, MIB, BudgetLedger
from sakurapool.storage.location_gate import (
    LocationRejected,
    RepositoryConfigError,
    TwoHopKeyError,
    check_location,
    check_token_not_in_etag,
    parse_repository,
    two_hop_proof_key,
    two_hop_record_key,
    validate_two_hop_record,
)
from sakurapool.storage.modelscope import (
    ListedFile,
    ModelScopeDataset,
    TwoHopProbe,
)
from sakurapool.storage.transport import (
    GuardedTransport,
    RemoteIOError,
    TwoHopResult,
    condition_binding_key,
    validate_same_origin_cookie,
)

REV = "a" * 40
REPO = "leafmoone/game_cg_5M"
TOKEN = "tk-secret-0123456789abcdef"
ETAG = '"5483AA783C561A81C28B0A5E27E99E4E-134"'
CDN = "cdn-lfs-cn-1.modelscope.cn"


# ---------------------------------------------------------------- location gate

def test_location_gate_accepts_clean_signed_location():
    assert check_location(
        f"https://{CDN}/files/{REV}/pre/x.tar?Expires=99&OSSAccessKeyId=LTAI5tX&Signature=ab019",
        "https://modelscope.cn", TOKEN) == CDN


def test_location_gate_approved_host_is_per_observation():
    # The approval is generated from the SAME hop-1 observation: each
    # structurally-safe observed Location returns its own exact host, per
    # call, with no cache and no static allowlist.
    a = check_location("https://cdn-a.example.net/f?Expires=1",
                       "https://origin.example", TOKEN)
    b = check_location("https://cdn-b.example.net/f?Expires=1",
                       "https://origin.example", TOKEN)
    assert a == "cdn-a.example.net" and b == "cdn-b.example.net"
    # Re-observing either one changes nothing; nothing is remembered.
    assert check_location("https://cdn-a.example.net/f?Expires=1",
                          "https://origin.example", TOKEN) == "cdn-a.example.net"
    # Pointing back at the credential origin is refused (non-offline).
    with pytest.raises(LocationRejected):
        check_location("https://origin.example/f?Expires=1",
                       "https://origin.example", TOKEN)
    # IP literal / non-DNS host are refused (non-offline).
    with pytest.raises(LocationRejected):
        check_location("https://203.0.113.7/f?Expires=1",
                       "https://origin.example", TOKEN)


@pytest.mark.parametrize("loc", [
    "https://a-b-c.modelscope.cn/abc%41bc",       # mixed path encoding
    "https://a-b-c.modelscope.cn/a%2Fb",          # path segment change
    "https://a-b-c.modelscope.cn/f%3Fq=1",        # embedded query start
    "https://a-b-c.modelscope.cn/f%23frag",       # embedded fragment
    "https://a-b-c.modelscope.cn/f?sig=%40x",     # userinfo start
    "https://a-b-c.modelscope.cn/f?sig=%2541",    # double-encode of %41
    "https://a-b-c.modelscope.cn/f?sig=a%20b",    # space
    "https://a-b-c.modelscope.cn/f?sig=a%3Bx",    # ';' is not an allowed query escape
    "https://a-b-c.modelscope.cn/f?sig=a%24x",    # '$' is not an allowed query escape
    "https://a-b-c.modelscope.cn/f?sig=a%22x",    # '"' is not an allowed query escape
])
def test_location_gate_rejects_structure_altering_escapes(loc):
    with pytest.raises(LocationRejected):
        check_location(loc, "https://origin.example", TOKEN)


def test_location_gate_allows_known_signed_query_encodings():
    # %2F %3D %2B are the verified signed-value encodings; they must pass
    # (the loopback fixture and the real hop-1 observation rely on this).
    # Note: a LITERAL '+' in the query is still rejected (form-plus rule);
    # only the escaped %2B is the legitimate signed encoding.
    loc = (f"https://{CDN}/pre/gamecg-v1-pre-p02-003.tar"
           f"?Expires=99&Signature=AB%2F%3D%2Bok")
    assert check_location(loc, "https://modelscope.cn", TOKEN) == CDN


def test_location_gate_query_escapes_cannot_change_structure():
    # Decoding the allowed query escapes never changes authority/host or
    # introduces a fragment: the decoded layer is re-validated against the
    # observed host, so these pass only because structure is unchanged.
    loc = f"https://{CDN}/f?Signature=AB%2F%3D%2Bok%2F&Expires=9"
    assert check_location(loc, "https://modelscope.cn", TOKEN) == CDN
    # A further decode cannot be smuggled in via double encoding (%25 is not
    # an allowed escape), so the decoded layers never gain new structure.
    with pytest.raises(LocationRejected):
        check_location(f"https://{CDN}/f?Signature=AB%252F",
                       "https://modelscope.cn", TOKEN)
    # Raw fragment / userinfo / scheme downgrade remain refused.
    for bad in (f"https://{CDN}/f#frag", f"https://u@{CDN}/f",
                f"http://{CDN}/f"):
        with pytest.raises(LocationRejected):
            check_location(bad, "https://modelscope.cn", TOKEN)


@pytest.mark.parametrize("loc", [
    f"https://{CDN}/f?Signature=a+b",            # form '+' equivalent
    "https://127.0.0.1/f?Signature=ab019",       # IP literal (non-offline)
    "https://user:pw@cdn-x-y.modelscope.cn/f",    # userinfo
    "https://cdn-x-y.modelscope.cn/f#frag",       # fragment
    "https://cdn-x-y.modelscope.cn:8443/f",       # non-443 port
    "https://cdn-x-y.modelscope.cn/%00",          # control character
    "https://cdn-x-y.modelscope.cn/f?Signature=AB%",   # dangling percent
    "https://cdn-x-y.modelscope.cn/f?Signature=AB%ZZ",  # bad hex
    "http://cdn-x-y.modelscope.cn/f",             # non-https
    "https://cdn_x-y.modelscope.cn/f",            # non-DNS label
])
def test_location_gate_rejects_malformed_or_ambiguous(loc):
    with pytest.raises(Exception):
        check_location(loc, "https://modelscope.cn", TOKEN)


def test_location_gate_length_cap():
    with pytest.raises(Exception):
        check_location("https://cdn-x-y.modelscope.cn/" + "a" * 9000,
                       "https://modelscope.cn", TOKEN)


def test_location_gate_rejects_raw_token_echo():
    # The raw (unencoded) token must be caught by per-layer token detection.
    # Encoded/mixed forms are separately covered by the structure-altering
    # escape tests (they are rejected before token detection is reached).
    with pytest.raises(Exception):
        check_location(f"https://{CDN}/f?sig={TOKEN}",
                       "https://modelscope.cn", TOKEN)


def test_etag_echo_check():
    check_token_not_in_etag(ETAG, TOKEN)  # clean strong etag passes
    with pytest.raises(Exception):
        check_token_not_in_etag(f'"{TOKEN}"', TOKEN)
    with pytest.raises(Exception):
        check_token_not_in_etag('W/"abc"', TOKEN)


# ------------------------------------------------- v2 proof key and record

def _v2_kwargs(**over):
    base = dict(origin_endpoint="https://modelscope.cn",
                repository="leafmoone/game_cg_5M",
                repository_type="modelscope_dataset_legacy",
                revision=REV, path="pre/gamecg-v1-pre-p02-003.tar",
                size=1401159680, etag=ETAG, cdn_host=CDN)
    base.update(over)
    return base


def test_v2_key_domain_disjoint_from_legacy():
    k2 = two_hop_proof_key(**_v2_kwargs())
    k1 = condition_binding_key(
        endpoint="https://modelscope.cn", repository="leafmoone/game_cg_5M",
        revision=REV, path="pre/gamecg-v1-pre-p02-003.tar",
        size=1401159680, strong_etag=ETAG)
    assert k1 != k2 and len(k2) == 64
    assert two_hop_proof_key(**_v2_kwargs(cdn_host="other-cdn.modelscope.cn")) != k2
    with pytest.raises(TwoHopKeyError):
        two_hop_proof_key(**_v2_kwargs(repository_type="other"))


def test_record_schema_fail_closed():
    rec = dict(schema_version="transport_validator_v1", kind="cdn_strong_etag",
               etag_value=ETAG, repository="leafmoone/game_cg_5M",
               repository_type="modelscope_dataset_legacy",
               origin_endpoint="https://modelscope.cn",
               origin_host="modelscope.cn", cdn_host=CDN, hop_count=2,
               revision_candidate=REV, path="pre/x.tar", size=10,
               policy_profile_descriptor="modelscope_dataset_legacy",
               observed_batch="b1")
    built = validate_two_hop_record(dict(rec),
                                     expected_repository=REPO)
    assert two_hop_record_key(built) == two_hop_proof_key(
        **_v2_kwargs(revision=REV, path="pre/x.tar", size=10))
    bad = dict(rec)
    bad["schema_version"] = "transport_validator_v0"
    with pytest.raises(TwoHopKeyError):
        validate_two_hop_record(bad, expected_repository=REPO)
    bad = dict(rec)
    bad.pop("repository")
    with pytest.raises(TwoHopKeyError):
        validate_two_hop_record(bad, expected_repository=REPO)
    bad = dict(rec)
    bad["cdn_host"] = bad["origin_host"]  # degenerate one-host chain
    with pytest.raises(TwoHopKeyError):
        validate_two_hop_record(bad, expected_repository=REPO)
    bad = dict(rec)
    bad["cdn_host"] = CDN.upper()  # unnormalized
    with pytest.raises(TwoHopKeyError):
        validate_two_hop_record(bad, expected_repository=REPO)
    bad = dict(rec)
    bad["policy_profile_descriptor"] = "other_profile"
    with pytest.raises(TwoHopKeyError):
        validate_two_hop_record(bad, expected_repository=REPO)


# ------------------------------------------------------- transport behaviours

def _loopback_transport(tmp_path, **over):
    root = Path(tempfile.mkdtemp(prefix="offline-twohop-", dir=DEFAULT_WORK_ROOT))
    ledger = BudgetLedger(root, _offline_test=True, _test_limits={
        "body": 64, "metadata": 16, "attempts": 8, "records": 2,
        "saved_samples": 1, "saved_bytes": 5, "inflight": 2 * MIB})
    kwargs = dict(ledger=ledger, trusted_hosts=frozenset({"127.0.0.1"}),
                  allow_loopback_http=True)
    kwargs.update(over)
    t = GuardedTransport(**kwargs)
    return t, ledger


def test_same_origin_cookie_attachment_rules(tmp_path):
    # Value validation is a standalone, fixed-string contract.
    validate_same_origin_cookie("m_session_id=ok")
    for bad in ("", "a\r\nb", "x" * 513, 7, "\x00x"):
        with pytest.raises(ValueError):
            validate_same_origin_cookie(bad)
    # Attachment is exact-origin only. An offline transport never receives
    # credentials through the constructor (guarded), so the pure method is
    # exercised on a credential-free instance with the two fields set.
    t, _ = _loopback_transport(tmp_path)
    t.credential_origin = "https://127.0.0.1"
    t.same_origin_cookie = f"m_session_id={TOKEN}"
    headers = {}
    t._attach_origin_cookie("https://127.0.0.1/x", headers)
    assert headers.get("Cookie") == f"m_session_id={TOKEN}"
    headers = {}
    t._attach_origin_cookie(f"https://{CDN}/x", headers)  # CDN never gets it
    assert "Cookie" not in headers
    headers = {"Cookie": "pre=1"}
    t._attach_origin_cookie("https://127.0.0.1:443/x", headers)
    assert headers["Cookie"] == "pre=1"  # netloc mismatch: not attached


def _offline_ledger(tmp_path):
    root = Path(tempfile.mkdtemp(prefix="offline-twohop-", dir=DEFAULT_WORK_ROOT))
    return BudgetLedger(root, _offline_test=True)


def test_offline_rejects_credential_bearing_transport(tmp_path):
    with pytest.raises(ValueError):
        _loopback_transport(tmp_path, token=TOKEN,
                            credential_origin="https://127.0.0.1",
                            same_origin_cookie="m_session_id=x")
    with pytest.raises(ValueError):
        GuardedTransport(_offline_ledger(tmp_path),
                         trusted_hosts=frozenset({"127.0.0.1"}),
                         allow_loopback_http=True, token=None,
                         credential_origin=None,
                         same_origin_cookie="m_session_id=x")


# ------------------------------------------------------- provider total map

class FakeTransport:
    def __init__(self, pages, total_field="Total", total=None):
        self.pages = pages
        self.total_field = total_field
        self.total = total
        self.last_two_hop = None
        self.proofs = []

    def _host(self, url):
        return urlsplit(url).hostname

    def read_metadata(self, url):
        if url.endswith("/revisions"):
            from sakurapool.storage.transport import MetadataBytes

            return MetadataBytes(json.dumps({"Data": {"RevisionMap": {
                "Branches": [{"Revision": "master", "CommitId": REV}],
                "Tags": []}}}).encode(), "CONFIRMED")
        data = {"Files": self.pages[0]}
        value = self.total if self.total is not None else sum(len(p) for p in self.pages)
        data[self.total_field] = value
        if "both" == self.total_field:
            data["Total"] = value + 1
            data["TotalCount"] = value
        from sakurapool.storage.transport import MetadataBytes

        return MetadataBytes(json.dumps({"Data": data}).encode(), "CONFIRMED")

    def two_hop_range(self, url, **kw):
        self.last_two_hop = dict(url=url, **kw)
        return TwoHopResult(hop1_status=302, hop2_status=206,
                            approved_host="loopback.test", etag=ETAG,
                            bytes_read=kw["length"], payload=b"z" * kw["length"],
                            attempts=2, batch=kw.get("batch", ""))

    def record_two_hop_proof(self, **kw):
        self.proofs.append(kw)
        return "k" * 64


def test_list_files_maps_totalcount_field():
    page = [{"Path": "dir/1.tar", "Size": 2048, "Type": "blob"}]
    provider = ModelScopeDataset(FakeTransport([page], total_field="TotalCount"),
                                 "http://localhost", REPO)
    files, complete = provider.list_files(REV)
    assert complete and files[0].path == "dir/1.tar"
    # Every tree entry is tagged with the revision candidate it was listed
    # under, so a later probe can bind to the same tree object.
    assert files[0].revision_candidate == REV


def test_list_files_rejects_disagreeing_totals():
    page = [{"Path": "dir/1.tar", "Size": 2048, "Type": "blob"}]
    provider = ModelScopeDataset(FakeTransport([page], total_field="both"),
                                 "http://localhost", REPO)
    with pytest.raises(RemoteIOError):
        provider.list_files(REV)


def test_range_probe_binds_tree_entry_identity():
    provider = ModelScopeDataset(FakeTransport([]), "http://localhost", REPO)
    entry = ListedFile(path="dir/1.tar", size=2048, provider_sha256=None,
                       lfs=False, revision_candidate=REV)
    probe = provider.range_probe(entry, start=0, length=1, batch="verify")
    d = probe.as_dict()
    assert d["hop2_status"] == 206 and d["attempts"] == 2
    assert d["size"] == 2048 and d["path"] == "dir/1.tar"
    assert d["revision_candidate"] == REV
    assert d["repository"] == "leafmoone/game_cg_5M"
    assert d["payload_sha256"] == hashlib.sha256(b"z").hexdigest()
    # The transport received the tree-bound size, not a caller value.
    assert provider.transport.last_two_hop["expected_size"] == 2048
    # No caller-facing size parameter exists: it cannot be substituted.
    sig = inspect.signature(ModelScopeDataset.range_probe)
    assert "expected_size" not in sig.parameters
    assert "size" not in sig.parameters


def test_range_probe_rejects_unbound_or_malformed_entries():
    provider = ModelScopeDataset(FakeTransport([]), "http://localhost", REPO)
    base = dict(path="dir/1.tar", size=2048, provider_sha256=None, lfs=False,
                revision_candidate=REV)
    for bad in (dict(path="pre/../x.tar"), dict(path="/abs.tar"),
                dict(path="a b.tar"),
                dict(revision_candidate="master"),
                dict(revision_candidate="g" * 40),
                dict(size=0), dict(size=-5), dict(size="2048")):
        entry = ListedFile(**{**base, **bad})
        with pytest.raises(ValueError):
            provider.range_probe(entry, start=0, length=1)
    with pytest.raises(ValueError):
        provider.range_probe("not-an-entry", start=0, length=1)


def test_record_probe_proof_uses_only_bound_identity():
    ft = FakeTransport([])
    provider = ModelScopeDataset(ft, "http://localhost", REPO)
    entry = ListedFile(path="dir/1.tar", size=2048, provider_sha256=None,
                       lfs=False, revision_candidate=REV)
    probe = provider.range_probe(entry, start=0, length=1, batch="verify")
    key = provider.record_probe_proof(probe, payload_sha="0" * 64,
                                      observed_batch="b1")
    assert key == "k" * 64
    kw = ft.proofs[0]
    assert kw["size"] == 2048 and kw["path"] == "dir/1.tar"
    assert kw["revision"] == REV
    assert kw["repository"] == "leafmoone/game_cg_5M"
    assert kw["cdn_host"] == "loopback.test"
    # The caller cannot substitute size/path/revision: none are parameters.
    sig = inspect.signature(ModelScopeDataset.record_probe_proof)
    for name in ("size", "path", "revision"):
        assert name not in sig.parameters
    with pytest.raises(ValueError):
        provider.record_probe_proof("not-a-probe", "0" * 64)


def test_proof_key_is_sensitive_to_object_identity():
    # A proof recorded under a different size/path/revision is a different
    # key: a tampered size can never bind to the observed object's key.
    assert two_hop_proof_key(**_v2_kwargs(size=2049)) != \
        two_hop_proof_key(**_v2_kwargs())
    assert two_hop_proof_key(**_v2_kwargs(path="dir/other.tar")) != \
        two_hop_proof_key(**_v2_kwargs())
    assert two_hop_proof_key(**_v2_kwargs(revision="b" * 40)) != \
        two_hop_proof_key(**_v2_kwargs())


# ----------------------------------------------------- ledger + proof storage

def test_two_hop_proof_stores_in_v2_domain(tmp_path):
    t, ledger = _loopback_transport(tmp_path)
    kwargs = _v2_kwargs()
    key = t.record_two_hop_proof(payload_sha="0" * 64, observed_batch="b1", **kwargs)
    assert ledger.condition_proof(key) == "0" * 64
    legacy = condition_binding_key(
        endpoint=kwargs["origin_endpoint"], repository=kwargs["repository"],
        revision=kwargs["revision"], path=kwargs["path"], size=kwargs["size"],
        strong_etag=kwargs["etag"])
    assert ledger.condition_proof(legacy) is None  # v2 never pollutes v1 domain
    with pytest.raises(TwoHopKeyError):
        t.record_two_hop_proof(payload_sha="0" * 63, observed_batch="b1", **kwargs)
    with pytest.raises(TwoHopKeyError):
        t.record_two_hop_proof(payload_sha="0" * 64, observed_batch="b1",
                               **_v2_kwargs(repository_type="other"))


def test_v2_proof_cross_host_and_cross_profile_never_share_key(tmp_path):
    t, ledger = _loopback_transport(tmp_path)
    k_a = t.record_two_hop_proof(payload_sha="1" * 64, **_v2_kwargs())
    with pytest.raises(ValueError):
        t.record_two_hop_proof(payload_sha="2" * 64,
                               **_v2_kwargs(cdn_host="other-cdn.modelscope.cn"))
    assert ledger.condition_proof(k_a) == "1" * 64


def test_v2_proof_requires_full_business_identity():
    # The v2 key domain binds the complete object identity: malformed
    # repository configuration, malformed revision, and non-canonical path all
    # fail closed; a different well-formed repository configuration yields a
    # different key (cross-repository isolation).
    base = _v2_kwargs()
    assert two_hop_proof_key(**base)
    with pytest.raises(TwoHopKeyError):
        two_hop_proof_key(**_v2_kwargs(repository="a..b/user_repo"))
    with pytest.raises(TwoHopKeyError):
        two_hop_proof_key(**_v2_kwargs(repository="other user/repo"))
    assert two_hop_proof_key(**_v2_kwargs(repository="other/user_repo")) != \
        two_hop_proof_key(**base)
    with pytest.raises(TwoHopKeyError):
        two_hop_proof_key(**_v2_kwargs(revision="master"))
    with pytest.raises(TwoHopKeyError):
        two_hop_proof_key(**_v2_kwargs(revision="g" * 40))
    for bad_path in ("pre/../x.tar", "/pre/x.tar", "pre/x.tar/",
                     "pre/a b.tar", "pre\\x.tar"):
        with pytest.raises(TwoHopKeyError):
            two_hop_proof_key(**_v2_kwargs(path=bad_path))


def test_v2_record_validation_binds_identity_and_profile():
    def rec(**over):
        base = dict(schema_version="transport_validator_v1",
                    kind="cdn_strong_etag", etag_value=ETAG,
                    repository="leafmoone/game_cg_5M",
                    repository_type="modelscope_dataset_legacy",
                    origin_endpoint="https://modelscope.cn",
                    origin_host="modelscope.cn", cdn_host=CDN, hop_count=2,
                    revision_candidate=REV, path="pre/x.tar", size=10,
                    policy_profile_descriptor="modelscope_dataset_legacy",
                    observed_batch="b1")
        base.update(over)
        return base
    # A record from another repository than the runtime configuration, or a
    # malformed revision, or a traversal path, all refuse.
    for over in (dict(repository="other/user_repo"),
                 dict(revision_candidate="master"),
                 dict(path="pre/../x.tar"),
                 dict(path="/pre/x.tar")):
        with pytest.raises(TwoHopKeyError):
            validate_two_hop_record(rec(**over), expected_repository=REPO)
    # Cross-profile: descriptor must equal the repository type exactly.
    with pytest.raises(TwoHopKeyError):
        validate_two_hop_record(rec(policy_profile_descriptor="other"),
                                expected_repository=REPO)
    # origin endpoint and host must be the same normalized identity.
    with pytest.raises(TwoHopKeyError):
        validate_two_hop_record(rec(origin_host="other-origin.cn"),
                                expected_repository=REPO)


# ------------------------------------------------ loopback two-hop end-to-end

ORIGIN_BODY = b"K" * 64
LOOPBACK_STATE = {}


class LoopbackHandler(BaseHTTPRequestHandler):
    mode = "origin"

    def log_message(self, *_a):
        pass

    def do_GET(self):
        if self.mode == "origin":
            self.send_response(302)
            cdn_port = LOOPBACK_STATE["cdn_port"]
            self.send_header("Location",
                             f"http://127.0.0.1:{cdn_port}{self.path}?Signature=ok%3D1")
            self.end_headers()
            return
        if self.headers.get("Authorization") is not None:
            self.send_response(403)
            self.end_headers()
            return
        requested = self.headers.get("Range")
        if requested != "bytes=0-0":
            self.send_response(416)
            self.end_headers()
            return
        self.send_response(206)
        self.send_header("Content-Range", f"bytes 0-0/{len(ORIGIN_BODY)}")
        self.send_header("Content-Length", "1")
        self.send_header("ETag", ETAG)
        self.end_headers()
        self.wfile.write(ORIGIN_BODY[:1])


def _start_server(mode):
    cls = type("H", (LoopbackHandler,), {"mode": mode})
    server = ThreadingHTTPServer(("127.0.0.1", 0), cls)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def test_two_hop_success_on_loopback(tmp_path):
    origin = _start_server("origin")
    cdn = _start_server("cdn")
    LOOPBACK_STATE["cdn_port"] = cdn.server_address[1]
    try:
        t, ledger = _loopback_transport(tmp_path)
        result = t.two_hop_range(
            f"http://127.0.0.1:{origin.server_address[1]}/repo?Revision={REV}"
            "&FilePath=pre%2Fx.tar", start=0, length=1,
            expected_size=len(ORIGIN_BODY), batch="loopback")
        assert result.hop1_status == 302 and result.hop2_status == 206
        # The approved host is the one observed in THIS hop-1 Location.
        assert result.approved_host == "127.0.0.1"
        assert result.payload == ORIGIN_BODY[:1] and result.bytes_read == 1
        _i, (gen, used, leases, proofs) = ledger._read_pair()
        assert not leases and used["attempts"] >= 2 and used["body"] == 1
    finally:
        origin.shutdown()
        cdn.shutdown()


def test_two_hop_range_has_no_external_host_injection(tmp_path):
    # No approved_host parameter, no constructor host: the only possible
    # approval source is the same hop-1 Location, so a cross-object or
    # pre-registered host cannot be replayed into hop2.
    sig = inspect.signature(GuardedTransport.two_hop_range)
    assert "approved_host" not in sig.parameters
    ctor = inspect.signature(GuardedTransport.__init__)
    assert "approved_two_hop_host" not in ctor.parameters
    t, _ = _loopback_transport(tmp_path)
    assert not hasattr(t, "approved_two_hop_host")


def test_two_hop_clone_carries_no_host_policy(tmp_path):
    origin = _start_server("origin")
    cdn = _start_server("cdn")
    LOOPBACK_STATE["cdn_port"] = cdn.server_address[1]
    try:
        t, _ = _loopback_transport(tmp_path)
        tc = t.clone()
        result = tc.two_hop_range(
            f"http://127.0.0.1:{origin.server_address[1]}/repo?Revision={REV}"
            "&FilePath=pre%2Fx.tar", start=0, length=1,
            expected_size=len(ORIGIN_BODY), batch="clone")
        # The clone approved the host from its OWN hop-1 observation.
        assert result.hop2_status == 206 and result.approved_host == "127.0.0.1"
    finally:
        origin.shutdown()
        cdn.shutdown()


def test_two_hop_rejects_size_mismatch_and_retains_nothing_unknown(tmp_path):
    origin = _start_server("origin")
    cdn = _start_server("cdn")
    LOOPBACK_STATE["cdn_port"] = cdn.server_address[1]
    try:
        t, ledger = _loopback_transport(tmp_path)
        url = (f"http://127.0.0.1:{origin.server_address[1]}/repo?Revision={REV}"
               "&FilePath=pre%2Fx.tar")
        with pytest.raises(RemoteIOError):
            t.two_hop_range(url, start=0, length=1,
                            expected_size=len(ORIGIN_BODY) + 1)
        _i, (gen, used, leases, proofs) = ledger._read_pair()
        assert not leases  # header-level reject settles at 0, nothing retained
    finally:
        origin.shutdown()
        cdn.shutdown()


def test_two_hop_cdn_must_be_credential_free(tmp_path, monkeypatch):
    class GuardedCdn(LoopbackHandler):
        mode = "cdn"

        def do_GET(self):
            if self.headers.get("Authorization") is not None:
                self.send_response(403)
                self.end_headers()
                return
            LoopbackHandler.do_GET(self)

    origin = ThreadingHTTPServer(("127.0.0.1", 0),
                                 type("O", (LoopbackHandler,), {"mode": "origin"}))
    cdn = ThreadingHTTPServer(("127.0.0.1", 0), GuardedCdn)
    threading.Thread(target=origin.serve_forever, daemon=True).start()
    threading.Thread(target=cdn.serve_forever, daemon=True).start()
    LOOPBACK_STATE["cdn_port"] = cdn.server_address[1]
    try:
        t, ledger = _loopback_transport(tmp_path)  # credential-free
        result = t.two_hop_range(
            f"http://127.0.0.1:{origin.server_address[1]}/repo?Revision={REV}"
            "&FilePath=pre%2Fx.tar", start=0, length=1,
            expected_size=len(ORIGIN_BODY))
        assert result.hop2_status == 206  # CDN saw no Authorization header
    finally:
        origin.shutdown()
        cdn.shutdown()


# --------------------------------------------- runtime repository configuration

@pytest.mark.parametrize("repo",
                         ["leafmoone/game_cg_5M", "leafmoone/konachan_full",
                          "owner2/dataset-name_v2", "a1/b_2.c-3",
                          "some_owner.some/some.repo.name"])
def test_parse_repository_accepts_runtime_owner_name(repo):
    parsed = parse_repository(repo)
    assert (parsed.owner, parsed.name) == tuple(repo.split("/"))
    assert parsed.id == repo


@pytest.mark.parametrize("bad", [
    "", "leafmoone", "leafmoone/", "/game_cg_5M", "a/b/c",
    "a..b/c", "owner/a..b", "..a/b", "a/..", "owner/../name",
    ".owner/name", "_.owner/name", "owner/-name", "owner/name_",
    "leaf%2Fmoone/game", "leaf\\moone/game", "leaf moone/game",
    "leaf\nmoone/game", "leafmoone/game_cg_5M/",
    "leafmoone/game_cg_5M?Revision=x", "leafmoone/game_cg_5M#f",
    "x" * 101 + "/y", "o/" + "y" * 101, 123, None, ("leafmoone", "game"),
])
def test_parse_repository_rejects_invalid_configuration(bad):
    with pytest.raises(RepositoryConfigError):
        parse_repository(bad)


def test_parse_repository_rejects_consecutive_dots_with_dedicated_diagnostic():
    # Explicit reviewer rule: any ".." substring in owner or name is refused
    # with a dedicated diagnostic (never silently normalized away). Single
    # interior dots remain valid syntax: this parser is not a provider
    # allowlist, so it must not over-restrict legal owner/name spellings.
    assert parse_repository("o.wner/name").owner == "o.wner"
    for bad in ("a..b/c", "owner/a..b", "..a/b", "a/..b..c"):
        with pytest.raises(RepositoryConfigError, match="consecutive dots"):
            parse_repository(bad)


def test_v2_key_rejects_malformed_repository_and_is_repo_isolated():
    with pytest.raises(TwoHopKeyError):
        two_hop_proof_key(**_v2_kwargs(repository="a..b/c"))
    with pytest.raises(TwoHopKeyError):
        two_hop_proof_key(**_v2_kwargs(repository="owner only"))
    assert two_hop_proof_key(**_v2_kwargs()) != \
        two_hop_proof_key(**_v2_kwargs(repository="leafmoone/konachan_full"))


def test_validate_two_hop_record_requires_expected_repository():
    rec = dict(schema_version="transport_validator_v1", kind="cdn_strong_etag",
               etag_value=ETAG, repository=REPO,
               repository_type="modelscope_dataset_legacy",
               origin_endpoint="https://modelscope.cn",
               origin_host="modelscope.cn", cdn_host=CDN, hop_count=2,
               revision_candidate=REV, path="pre/x.tar", size=10,
               policy_profile_descriptor="modelscope_dataset_legacy",
               observed_batch="b1")
    # The parameter is mandatory: no format-only lenient default exists.
    with pytest.raises(TwoHopKeyError, match="expected_repository"):
        validate_two_hop_record(dict(rec))
    with pytest.raises(TwoHopKeyError):
        validate_two_hop_record(dict(rec), expected_repository="a..b/c")
    # Matching configuration accepts (the old game repo is a valid config too).
    built = validate_two_hop_record(dict(rec), expected_repository=REPO)
    assert built.repository == REPO
    # A record of another repository is refused under this configuration.
    foreign = dict(rec, repository="leafmoone/konachan_full")
    with pytest.raises(TwoHopKeyError, match="match expected_repository"):
        validate_two_hop_record(foreign, expected_repository=REPO)
    # And the SAME record is readable under its own configuration.
    assert validate_two_hop_record(
        foreign, expected_repository="leafmoone/konachan_full").repository == \
        "leafmoone/konachan_full"


def _fake_probe(repository):
    result = TwoHopResult(hop1_status=302, hop2_status=206,
                          approved_host="loopback.test", etag=ETAG,
                          bytes_read=1, payload=b"z", attempts=2, batch="b1")
    return TwoHopProbe(repository=repository, revision_candidate=REV,
                       path="dir/1.tar", size=2048, result=result)


def test_probe_proof_refuses_cross_instance_reuse():
    provider = ModelScopeDataset(FakeTransport([]), "http://localhost", REPO)
    # A probe of the same object produced under a DIFFERENT configured
    # repository is refused by this instance: identity is instance-bound.
    foreign = _fake_probe("leafmoone/konachan_full")
    with pytest.raises(TwoHopKeyError, match="does not belong"):
        provider.record_probe_proof(foreign, payload_sha="0" * 64)
    # A probe carrying this instance's own repository is accepted (endpoint
    # binding is additionally enforced by the proof key downstream).
    assert provider.record_probe_proof(
        _fake_probe(REPO), payload_sha="0" * 64) == "k" * 64


def test_provider_normalizes_and_binds_repository_configuration():
    provider = ModelScopeDataset(FakeTransport([]), "http://localhost", REPO)
    assert provider.repository_id.id == REPO
    assert (provider.repository_id.owner, provider.repository_id.name) == \
        ("leafmoone", "game_cg_5M")
    url = provider.download_url(REV, "dir/1.tar")
    assert url.startswith(f"http://localhost/api/v1/datasets/{REPO}/repo?")
    for bad in ("a..b/c", "owner", "", "o/n/"):
        with pytest.raises(ValueError, match="owner/name"):
            ModelScopeDataset(FakeTransport([]), "http://localhost", bad)


def test_unknown_repo_is_provider_answer_not_parser_restriction():
    # Any syntactically valid owner/name may be configured; the parser must
    # not act as a repository allowlist. A well-formed repository that
    # ModelScope does not have is accepted at construction and surfaces as
    # a clear provider error at request time.
    unknown = ModelScopeDataset(FakeTransport([]), "http://localhost",
                                 "some-owner/never-compiled-repo")
    assert unknown.repository_id.owner == "some-owner"
    assert unknown.repository_id.name == "never-compiled-repo"
    # The configured identity drives every URL built by this instance.
    url = unknown.download_url(REV, "dir/1.tar")
    assert url.startswith(
        "http://localhost/api/v1/datasets/some-owner/never-compiled-repo/repo?")
