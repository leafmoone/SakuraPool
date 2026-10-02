"""Offline helper diagnostics; no real credentials or network."""

import importlib.util
import json
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

from sakurapool.storage.transport import RemoteIOError

SPEC = importlib.util.spec_from_file_location(
    "real_canary", Path(__file__).parents[1] / "reports/R2C3/real_canary.py"
)
canary = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(canary)
SECRET = "secret-body-https://private.invalid Authorization Bearer " + "e" * 64
FACT_KEYS = (
    "exact_object_found",
    "exact_object_size",
    "exact_object_revision",
    "provider_sha_present",
)
UNKNOWN = "NOT_ESTABLISHED"


def row(**changes):
    values = dict(path="gc5m/test.tar", size=12, revision_candidate="r", provider_sha256="a" * 64)
    return SimpleNamespace(**(values | changes))


@pytest.mark.parametrize(
    "rows,code,facts",
    [
        ([], "EXACT_OBJECT_NOT_FOUND", ("NO", UNKNOWN, UNKNOWN, UNKNOWN)),
        ([row(), row()], "EXACT_OBJECT_DUPLICATE", ("YES", UNKNOWN, UNKNOWN, UNKNOWN)),
        ([row(size=13)], "OBJECT_SIZE_MISMATCH", ("YES", "FAIL", UNKNOWN, UNKNOWN)),
        (
            [row(revision_candidate="other")],
            "OBJECT_REVISION_MISMATCH",
            ("YES", "PASS", "FAIL", UNKNOWN),
        ),
        ([row(provider_sha256=None)], "PROVIDER_SHA_UNAVAILABLE", ("YES", "PASS", "PASS", "NO")),
        ([row()], "PROVIDER_SHA_MATCH", ("YES", "PASS", "PASS", "YES")),
        ([row(provider_sha256="b" * 64)], "PROVIDER_SHA_MISMATCH", ("YES", "PASS", "PASS", "YES")),
    ],
)
def test_classification_matrix(rows, code, facts):
    classification = canary.classify_provider_object(rows, "gc5m/test.tar", 12, "r", "a" * 64)
    assert classification == code
    result = dict.fromkeys(FACT_KEYS, UNKNOWN)
    canary.record_lookup(result, classification)
    assert tuple(result[key] for key in FACT_KEYS) == facts
    assert result["real_lookup_code"] == code
    assert result["real_lookup_phase"] == (
        "EXACT_OBJECT" if code in canary.IDENTITY_CODES else "PROVIDER_DIGEST"
    )


@pytest.mark.parametrize(
    "code,fixed",
    [(key, value) for key, value in canary.REMOTE_CODES.items() if key != "provider_rejection"],
)
def test_safe_exception_codes(monkeypatch, code, fixed):
    error = RemoteIOError(SECRET, code=code, phase="provider_listing_shape")

    def fail(work, profile, result, state):
        result.update(phase="PROVIDER_LISTING", code="IN_PROGRESS")
        raise error

    monkeypatch.setattr(canary, "_run", fail)
    result = canary.run(None, None)
    assert result["phase"] == result["real_lookup_phase"] == "PROVIDER_LISTING"
    assert result["code"] == result["real_lookup_code"] == fixed
    assert all(result[key] == UNKNOWN for key in FACT_KEYS)
    assert SECRET not in json.dumps(result)
    assert "e" * 64 not in json.dumps(result)


def test_untrusted_code_and_exception_are_redacted(monkeypatch):
    error = RemoteIOError(SECRET)
    error.code = SECRET
    assert canary.failure_code(error, "FETCH_BINDING") == "REMOTE_FAILURE"

    def fail(work, profile, result, state):
        result.update(phase="FETCH_BINDING", code="IN_PROGRESS")
        state["ledger"] = object()
        raise ValueError(SECRET)

    monkeypatch.setattr(canary, "_run", fail)
    monkeypatch.setattr(canary, "ledger_snapshot", lambda ledger: {"saved_bytes": 2})
    result = canary.run(None, None)
    assert result["code"] == "FETCH_BINDING_FAILED"
    assert result["ledger_after"] == {"saved_bytes": 2}
    assert SECRET not in json.dumps(result)


def test_ledger_snapshot_distinguishes_pending():
    used = {key: 0 for key in (*canary.RESERVED, "attempts")}
    used.update(body=3, saved_bytes=2)
    lease = {key: 1 for key in canary.RESERVED}
    ledger = SimpleNamespace(
        _locked=lambda: nullcontext(),
        _read_pair=lambda: (0, (1, used, {SECRET: lease}, {SECRET: SECRET})),
        _totals=lambda used, leases: used | {key: used[key] + 1 for key in canary.RESERVED},
    )
    result = canary.ledger_snapshot(ledger)
    assert result["settled"]["body"] == 3
    assert result["pending_unknown"]["body"] == 1
    assert result["totals"]["body"] == 4
    assert result["pending_lease_count"] == 1
    assert SECRET not in json.dumps(result)


class FakeTransport:
    _token = SECRET
    _cookie = SECRET

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


@pytest.fixture
def offline_run(tmp_path, monkeypatch):
    from sakurapool.runtime import inventory, snapshot
    from sakurapool.storage import publication

    work = tmp_path / "work"
    (work / "runtime").mkdir(parents=True)
    monkeypatch.setattr(canary, "EXPECTED_WORK", work)
    monkeypatch.setattr(canary, "check_profile", lambda profile: None)
    monkeypatch.setattr(
        inventory, "load_p2_inventory", lambda p: SimpleNamespace(source_fingerprint="f")
    )
    runtime = SimpleNamespace(rid_count=121, manifest={"source_fingerprint": "f"})
    monkeypatch.setattr(snapshot.RuntimeSnapshot, "open", lambda *a, **kw: nullcontext(runtime))
    worker = SimpleNamespace(
        stat=lambda: SimpleNamespace(
            st_size=canary.EXPECTED_WORKER_STAT[0], st_mtime_ns=canary.EXPECTED_WORKER_STAT[1]
        )
    )
    monkeypatch.setattr(publication, "plain", lambda p, *a: p if isinstance(p, Path) else worker)
    ledger = SimpleNamespace(root=tmp_path / "ledger")
    ledger.root.mkdir()
    monkeypatch.setattr(canary, "BudgetLedger", lambda: ledger)
    monkeypatch.setattr(canary, "ledger_snapshot", lambda ledger: {"saved_bytes": 0})
    scope = SimpleNamespace(origin="synthetic", repo_id="synthetic", repo_type="synthetic")
    monkeypatch.setattr(canary, "load_profile", lambda *a, **kw: (None, scope, FakeTransport()))
    monkeypatch.setattr(canary, "GuardedTransport", lambda *a, **kw: nullcontext(None))
    calls = []
    provider_row = row(
        path=canary.OBJECT, size=5201920, revision_candidate=canary.REV, provider_sha256=None
    )

    def page(hub, rev, **kwargs):
        assert rev == canary.REV
        assert kwargs == dict(root="gc5m", page=1, page_size=200)
        calls.append("listing")
        return [provider_row], True

    monkeypatch.setattr(
        canary,
        "ModelScopeDataset",
        lambda *a: SimpleNamespace(legacy_hub_id=lambda: 1, legacy_tree_page=page),
    )
    manifest = dict(
        rid_count=121,
        object_count=1,
        fetchable_object_count=0,
        fetchable_rid_count=0,
        content_sha256=SECRET,
    )

    def build(*args):
        calls.append("build")
        assert json.loads((work / "remote-map.jsonl").read_text())["provider_sha256"] is None
        return manifest

    monkeypatch.setattr(canary, "build_publication", build)

    def load(*args, **kwargs):
        assert kwargs == {"full_verify": True}
        calls.append("verify")
        return nullcontext(SimpleNamespace(runtime=runtime))

    monkeypatch.setattr(canary, "load_publication", load)

    def fetch(*args, **kwargs):
        pytest.fail("null digest must not fetch")

    monkeypatch.setattr(canary, "fetch_publication_sample", fetch)
    return work, provider_row, calls


def test_null_digest_build_verify_and_skip_fetch(offline_run):
    work, _, calls = offline_run
    result = canary.run(work, None)
    assert result["status"] == "PASS"
    assert result["phase"] == "COMPLETE"
    assert result["real_lookup_phase"] == "PROVIDER_DIGEST"
    assert result["real_lookup_code"] == "PROVIDER_SHA_UNAVAILABLE"
    assert tuple(result[key] for key in FACT_KEYS) == ("YES", "PASS", "PASS", "NO")
    assert result["real_publication_canary"] == "PASS"
    assert result["provider_sha_match"] == "UNAVAILABLE"
    assert result["real_v2_fetch"] == "BLOCKED_PROVIDER_SHA_UNAVAILABLE"
    assert all(
        result[key] == "NOT_RUN" for key in ("fetch_binding", "fetch_image", "fetch_metadata")
    )
    assert result["publication"] == dict(
        rid_count=121, object_count=1, fetchable_object_count=0, fetchable_rid_count=0
    )
    assert calls == ["listing", "build", "verify"]
    assert SECRET not in json.dumps(result)
    assert canary.WHOLE not in json.dumps(result)
    assert "ledger_before" in result and "ledger_after" in result


def test_digest_mismatch_stops_before_build(offline_run):
    work, provider_row, calls = offline_run
    provider_row.provider_sha256 = "c" * 64
    result = canary.run(work, None)
    assert result["status"] == "STOP"
    assert result["phase"] == result["real_lookup_phase"] == "PROVIDER_DIGEST"
    assert result["code"] == result["real_lookup_code"] == "PROVIDER_SHA_MISMATCH"
    assert tuple(result[key] for key in FACT_KEYS) == ("YES", "PASS", "PASS", "YES")
    assert calls == ["listing"]
    assert not (work / "p2-list.json").exists()


def test_strict_parser_failure_stays_listing(offline_run, monkeypatch):
    work, _, calls = offline_run

    def page(*args, **kwargs):
        raise RemoteIOError(SECRET)

    monkeypatch.setattr(
        canary,
        "ModelScopeDataset",
        lambda *a: SimpleNamespace(legacy_hub_id=lambda: 1, legacy_tree_page=page),
    )
    result = canary.run(work, None)
    assert result["phase"] == result["real_lookup_phase"] == "PROVIDER_LISTING"
    assert result["code"] == result["real_lookup_code"] == "REMOTE_IO"
    assert all(result[key] == UNKNOWN for key in FACT_KEYS)
    assert calls == []
    assert "ledger_after" in result
    assert SECRET not in json.dumps(result)


@pytest.mark.parametrize(
    "change,duplicate,code,facts",
    [
        ({"path": "other"}, False, "EXACT_OBJECT_NOT_FOUND", ("NO", UNKNOWN, UNKNOWN, UNKNOWN)),
        ({}, True, "EXACT_OBJECT_DUPLICATE", ("YES", UNKNOWN, UNKNOWN, UNKNOWN)),
        ({"size": 1}, False, "OBJECT_SIZE_MISMATCH", ("YES", "FAIL", UNKNOWN, UNKNOWN)),
        (
            {"revision_candidate": "other"},
            False,
            "OBJECT_REVISION_MISMATCH",
            ("YES", "PASS", "FAIL", UNKNOWN),
        ),
    ],
)
def test_identity_failures_route_before_build(
    offline_run, monkeypatch, change, duplicate, code, facts
):
    work, provider_row, calls = offline_run
    for key, value in change.items():
        setattr(provider_row, key, value)
    if duplicate:
        monkeypatch.setattr(
            canary,
            "ModelScopeDataset",
            lambda *a: SimpleNamespace(
                legacy_hub_id=lambda: 1,
                legacy_tree_page=lambda *a, **kw: ([provider_row, provider_row], True),
            ),
        )
    result = canary.run(work, None)
    assert result["status"] == "STOP"
    assert result["phase"] == result["real_lookup_phase"] == "EXACT_OBJECT"
    assert result["code"] == result["real_lookup_code"] == code
    assert tuple(result[key] for key in FACT_KEYS) == facts
    assert "build" not in calls
    assert not (work / "p2-list.json").exists()


@pytest.mark.parametrize("metadata_exists", [True, False])
def test_matching_digest_fetch_metadata_filename(offline_run, monkeypatch, metadata_exists):
    work, provider_row, calls = offline_run
    provider_row.provider_sha256 = canary.WHOLE
    manifest = dict(
        rid_count=121, object_count=1, fetchable_object_count=1, fetchable_rid_count=121
    )
    monkeypatch.setattr(canary, "build_publication", lambda *a: manifest)
    catalog = SimpleNamespace(
        execute=lambda *a: SimpleNamespace(fetchone=lambda: (bytes.fromhex("a" * 32),))
    )
    pub = SimpleNamespace(runtime=SimpleNamespace(rid_count=121, _catalog=catalog))
    monkeypatch.setattr(canary, "load_publication", lambda *a, **kw: nullcontext(pub))

    def fetch(publication, record, transport, output, **kwargs):
        assert kwargs["metadata"] is True
        final = output / record
        final.mkdir()
        (final / "image.jpg").write_bytes(b"synthetic")
        if metadata_exists:
            (final / "metadata.json").write_bytes(b"")
        else:
            (final / "metadata.jsonl").write_bytes(b"not the product suffix")
        calls.append("fetch")
        return final

    monkeypatch.setattr(canary, "fetch_publication_sample", fetch)
    result = canary.run(work, None)
    assert result["status"] == "PASS"
    assert result["phase"] == "COMPLETE"
    assert result["real_lookup_phase"] == "PROVIDER_DIGEST"
    assert result["real_lookup_code"] == "PROVIDER_SHA_MATCH"
    assert tuple(result[key] for key in FACT_KEYS) == ("YES", "PASS", "PASS", "YES")
    assert result["provider_sha_match"] == "PASS"
    assert result["fetch_binding"] == result["fetch_image"] == result["real_v2_fetch"] == "PASS"
    assert result["fetch_metadata"] == ("PASS" if metadata_exists else "ABSENT")
    assert calls == ["listing", "fetch"]
    assert canary.WHOLE not in json.dumps(result)


def test_profile_failure_initializes_unestablished_facts(monkeypatch):
    def fail(profile):
        raise ValueError(SECRET)

    monkeypatch.setattr(canary, "check_profile", fail)
    result = canary.run(None, None)
    assert result["real_lookup_phase"] == result["real_lookup_code"] == "NOT_RUN"
    assert all(result[key] == UNKNOWN for key in FACT_KEYS)
    assert result["fetch_metadata"] == "NOT_RUN"
    assert SECRET not in json.dumps(result)
