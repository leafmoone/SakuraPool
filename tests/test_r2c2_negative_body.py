"""Formal negative 412 bounded discard: network bytes != business bytes."""

import json

import pytest
from test_r2c2_binding import COOKIE, ETAG, TOKEN, proofs, runner
from test_r2c2_binding import binding_loop as _binding_loop

from sakurapool.storage.production import proof_key
from sakurapool.storage.production_resources import NEGATIVE_CONDITION_BODY_CAP

binding_loop = _binding_loop
CAP = NEGATIVE_CONDITION_BODY_CAP


@pytest.mark.parametrize("size", [0, 1, 437, 439, CAP])
@pytest.mark.parametrize("framing", ["length", "eof", "chunked"])
def test_negative_discard_complete_network_accounting_no_business_payload(
    binding_loop, monkeypatch, size, framing
):
    state, ledger, transport, candidate = binding_loop
    sensitive = (TOKEN + COOKIE + ETAG + "https://secret.invalid/?Signature=hidden").encode()
    body = (sensitive * (size // len(sensitive) + 1))[:size]
    headers = (
        [("Content-Length", str(size))]
        if framing == "length"
        else ([("Transfer-Encoding", "chunked")] if framing == "chunked" else [])
    )
    state["negative"] = {"body": body, "headers": headers, "chunked": framing == "chunked"}
    original = transport._call
    artifacts = []

    def call(obj, root, **kwargs):
        result = original(obj, root, **kwargs)
        if kwargs.get("condition") == "wrong":
            artifacts.append((root / "body").stat().st_size)
            assert result["bytes"] == 0 and result["accounting"]["body"] == size
            assert result["accounting"]["complete"] is True
            assert "sha256" not in result
        return result

    monkeypatch.setattr(transport, "_call", call)
    report = runner.verify_round(transport, candidate)
    assert report["VERSION_BINDING"] == report["REAL_IF_MATCH_NEGATIVE"] == "PASS"
    assert artifacts == [0] and len(proofs(ledger)) == 1
    assert ledger.status()["body"] == size + 2
    assert ledger.status()["attempts"] == 6 and ledger.status()["inflight"] == 0
    assert report["budget_after"]["pending_leases"] == 0
    wrong = report["observations"]["wrong"]
    assert wrong["body"] == size and wrong["bytes"] == 0 and wrong["accounting_complete"]
    bound = transport.verified_object(candidate)
    assert ledger.condition_proof(proof_key(bound, test=True)) is not None
    persisted = json.dumps(report).encode() + b"".join(p.read_bytes() for p in ledger.slots)
    for secret in (
        TOKEN.encode(),
        COOKIE.encode(),
        ETAG.encode(),
        b"Signature=hidden",
        b"secret.invalid",
    ):
        assert secret not in persisted
    for origin, headers in state["calls"]:
        assert headers["user-agent"] == "SakuraMoon/1"
        assert headers["accept"] == "application/json, application/octet-stream"
        if not origin:
            assert not any(k in headers for k in ("authorization", "cookie", "referer"))


@pytest.mark.parametrize(
    "shape",
    [
        "declared_over",
        "eof_over",
        "chunked_over",
        "short439_437",
        "short437_200",
        "duplicate_length",
        "encoded",
        "cl_and_te",
        "unsupported_te",
    ],
)
def test_negative_failure_pending_no_proof_and_actual_overflow_count(binding_loop, shape):
    state, ledger, transport, candidate = binding_loop
    body = b"X" * 439
    headers = [("Content-Length", "439")]
    chunked = False
    if shape == "declared_over":
        body, headers = b"X" * (CAP + 1), [("Content-Length", str(CAP + 1))]
    if shape == "eof_over":
        body, headers = b"X" * (CAP + 1), []
    if shape == "chunked_over":
        body, headers, chunked = b"X" * (CAP + 1), [("Transfer-Encoding", "chunked")], True
    if shape == "short439_437":
        body = b"X" * 437
    if shape == "short437_200":
        body, headers = b"X" * 200, [("Content-Length", "437")]
    if shape == "duplicate_length":
        headers = [("Content-Length", "439"), ("Content-Length", "437")]
    if shape == "encoded":
        headers += [("Content-Encoding", "gzip")]
    if shape == "cl_and_te":
        headers += [("Transfer-Encoding", "chunked")]
        chunked = True
    if shape == "unsupported_te":
        headers = [("Transfer-Encoding", "gzip")]
    state["negative"] = {"body": body, "headers": headers, "chunked": chunked}
    report = runner.verify_round(transport, candidate)
    assert report["VERSION_BINDING"] == "BLOCKED"
    assert report["REAL_IF_MATCH_POSITIVE"] == "PASS"
    assert not proofs(ledger) and not transport._objects
    assert report["budget_after"]["pending_leases"] == 2
    assert ledger.status()["body"] == CAP + 1 + 2
    assert ledger.status()["inflight"] == 0
    wrong = report["observations"]["wrong"]
    assert wrong["accounting_complete"] is False
    if shape in ("eof_over", "chunked_over"):
        assert wrong["body"] == CAP + 1
    if shape == "declared_over":
        assert wrong["body"] == 0
    if shape in ("short439_437", "short437_200"):
        # Read may deliver a prefix before the truncation error; do not equate the
        # sent entity or its declared length with bytes delivered by reqwest Read.
        observed = wrong["body"]
        assert type(observed) is int and 0 <= observed <= len(body)
        after = report["budget_after"]
        assert after["known_settled"]["body"] == 2  # Only observe+positive are complete.
        # consume() records the successfully observed prefix inside pending leases;
        # it does not settle/refund them. The remaining unknown bound excludes that
        # recorded prefix while totals retain the entire reservation.
        assert after["pending_unknown_body_bound"] == CAP + 1 - observed
        assert (
            after["total_including_pending"]["body"] - after["pending_unknown_body_bound"]
            == 2 + observed
        )
        assert after["pending_leases"] == 2
        print(
            json.dumps(
                {
                    "short_entity_sent": len(body),
                    "read_observed": observed,
                    "known_settled_body": 2,
                    "unknown_pending_bound": CAP + 1 - observed,
                }
            )
        )


@pytest.mark.parametrize("status", [200, 206, 403, 404, 500])
def test_non412_never_drained_or_accepted_as_negative(binding_loop, status):
    state, ledger, transport, candidate = binding_loop
    state["negative"] = {
        "status": status,
        "body": b"not negative proof",
        "headers": [("Content-Length", "18")],
    }
    report = runner.verify_round(transport, candidate)
    assert report["VERSION_BINDING"] == "BLOCKED"
    assert report["REAL_IF_MATCH_POSITIVE"] == "PASS"
    assert report["observations"]["wrong"]["body"] == 0
    assert not proofs(ledger) and not transport._objects
    assert report["budget_after"]["pending_leases"] == 2


def test_scheduler_negative_preflight_matches_actual_reservation():
    from types import SimpleNamespace

    from test_r2c2_binding import load_c2_helpers

    scheduler, _ = load_c2_helpers()
    values = {"attempts": 0, "body": 0, "disk": 0, "inflight": 0}
    limits = {"attempts": 6, "body": 65541, "disk": 1 << 20, "inflight": 64 << 20}
    ledger = SimpleNamespace(status=lambda: values, limits=limits)
    assert scheduler.budget_gate(ledger)
    ledger.limits["body"] -= 1
    assert not scheduler.budget_gate(ledger)
    ledger.limits["body"] = 2
    assert scheduler.budget_gate(ledger, operations=1)
