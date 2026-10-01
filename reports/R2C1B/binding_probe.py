"""OFFLINE PREPARATION ONLY: real flag requires NEW direct human authorization.

Default is dependency-free HOLD, with no ledger, token, worker or network access.
Future real mode delegates all protocol decisions to formal verify_conditions.
"""

import argparse
import json
import os
import stat
from pathlib import Path

REPO = "leafmoone/game_cg_5M"
HUB_ID = 218032
ORIGIN = "https://modelscope.cn"
TARGET = "pre/gamecg-v1-pre-p02-003.tar"
SIZE = 1401159680
REVISION = "77948d890f654e6151a5ff7597b63a8337d8910e"
TOKEN_FILE = Path("D:/sm_data/ms-token.tmp")
WORKER = Path("D:/SakuraTool/SakuraPool-Fix3-20260930a/rust-target/release/sakurapool-worker.exe")
STARTED = Path(__file__).with_name("real-binding-round.started")
OUTPUT = Path(__file__).with_name("real-binding-result.json")
GATES = {
    "observe": "REAL_RANGE_CAPABILITY",
    "match": "REAL_IF_MATCH_POSITIVE",
    "wrong": "REAL_IF_MATCH_NEGATIVE",
}


def public_result(result):
    """Typed allowlist; never serialize result, validator, URL, host or exceptions."""
    out = {}
    for source, keys in (
        (result, (("status", 599), ("bytes", 2))),
        (
            result.get("observation", {}),
            (("origin_http_status", 599), ("cdn_http_status", 599), ("content_length", 2**63 - 1)),
        ),
        (result.get("accounting", {}), (("attempts", 2), ("body", 2))),
    ):
        for key, maximum in keys:
            value = source.get(key) if isinstance(source, dict) else None
            out[key] = value if type(value) is int and 0 <= value <= maximum else None
    diagnostic = result.get("diagnostic", {})
    for key in (
        "etag_present",
        "etag_is_strong",
        "content_length_present",
        "content_range_present",
        "content_encoding_present",
        "accounting_complete",
    ):
        value = diagnostic.get(key) if isinstance(diagnostic, dict) else None
        out[key] = value if type(value) is bool else None
    return out


def snapshot(ledger):
    from sakurapool.storage.budget import _disk_usage

    with ledger._locked():
        _, (_, used, leases, _) = ledger._read_pair()
        return {
            "known_settled": dict(used),
            "total_including_pending": ledger._totals(used, leases),
            "pending_leases": len(leases),
            "pending_unknown_body_bound": sum(
                v["body"] - v["consumed_body"] for v in leases.values()
            ),
            "physical_root_bytes": _disk_usage(ledger.root),
        }


def audited_transport(*args, **kwargs):
    from sakurapool.storage.production import RustProductionTransport

    class Audited(RustProductionTransport):
        """Instrumentation only; no HTTP/protocol/success decision replacement."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.binding_phase = None
            self.binding_observations = {}
            self.logical_operations = 0

        def _call(self, *args, condition="match", **kwargs):
            self.binding_phase = condition
            self.logical_operations += 1
            self.last_result = {}
            try:
                result = super()._call(*args, condition=condition, **kwargs)
                self.binding_observations[condition] = public_result(result)
                return result
            finally:
                if condition not in self.binding_observations:
                    self.binding_observations[condition] = public_result(self.last_result)

    return Audited(*args, **kwargs)


def verify_round(transport, candidate):
    """Shared real-future/offline core. Exactly ONE formal product invocation."""
    from sakurapool.storage.production import proof_key

    report = {
        "REAL_RANGE_CAPABILITY": "BLOCKED",
        "REAL_IF_MATCH_POSITIVE": "NOT_RUN",
        "REAL_IF_MATCH_NEGATIVE": "NOT_RUN",
        "VERSION_BINDING": "BLOCKED",
        "P4_COMPLETE": False,
        "control_logical_reads": 0,
        "control_attempts": 0,
        "signed_url_persisted": False,
        "credentials_persisted": False,
        "validator_persisted": False,
        "full_tar_scan": False,
        "budget_before": snapshot(transport.ledger),
    }
    try:
        bound = transport.verify_conditions(candidate)
        key = proof_key(bound, test=transport.ledger.offline_mode)
        digest = transport.ledger.condition_proof(key)
        if digest is None or transport._objects.get(bound.object_path) != bound:
            raise ValueError("formal proof registration missing")
        report.update({gate: "PASS" for gate in GATES.values()})
        report.update(
            VERSION_BINDING="PASS",
            proof_key=key,
            first_byte_sha256=digest,
            proof_recorded=True,
            object_registered=True,
        )
    except Exception:
        phase = transport.binding_phase
        for previous in ("observe", "match", "wrong"):
            if previous == phase:
                report[GATES[previous]] = "BLOCKED"
                break
            if previous in transport.binding_observations:
                report[GATES[previous]] = "PASS"
        if phase in ("match", "wrong"):
            report["if_match_limitation"] = "IF_MATCH_UNSUPPORTED_OR_UNVERIFIED"
    # Safe booleans derived only from completed FORMAL gates, never failed status inference.
    if report["REAL_RANGE_CAPABILITY"] == "PASS":
        transport.binding_observations["observe"]["content_range_exact_0_0_total"] = True
    if report["REAL_IF_MATCH_POSITIVE"] == "PASS":
        transport.binding_observations["match"].update(
            content_range_exact_0_0_total=True,
            same_digest_verified=True,
            same_validator_verified=True,
            same_scope_verified=True,
        )
    if report["REAL_IF_MATCH_NEGATIVE"] == "PASS":
        transport.binding_observations["wrong"]["same_scope_verified"] = True
    report["byte_logical_operations"] = transport.logical_operations
    report["observations"] = transport.binding_observations
    report["budget_after"] = snapshot(transport.ledger)
    return report


def claim_round(path):
    """Offline tests use OWN temp path; future real sentinel is never removed."""
    with path.open("x", encoding="ascii") as handle:
        handle.write("Exclusive one observe/correct/wrong binding batch; never retry\n")
        handle.flush()
        os.fsync(handle.fileno())


def candidate_identity():
    from sakurapool.storage.production import ProviderObject

    # Prior independently verified PUBLIC identity, not fresh/immutable proof.
    return ProviderObject(REPO, "modelscope_dataset_legacy", ORIGIN, REVISION, TARGET, SIZE)


def future_real_round():
    """MUST NOT execute in R2C1B prep, even after all offline tests PASS."""
    from sakurapool.storage.budget import BudgetLedger, is_reparse
    from sakurapool.storage.production_resources import ProductionFootprint

    ledger = BudgetLedger()
    footprint = ProductionFootprint.admit("range", 1)
    used = ledger.status()
    additions = {
        "attempts": 6,
        "body": 6,
        "disk": footprint.transfer_disk,
        "inflight": footprint.memory,
    }
    if any(used[k] + value > ledger.limits[k] for k, value in additions.items()):
        raise ValueError("finite binding budget unavailable")
    claim_round(STARTED)
    candidate = candidate_identity()
    candidate.validate()
    info = TOKEN_FILE.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or is_reparse(TOKEN_FILE)
        or info.st_size > 4096
    ):
        raise ValueError("credential unavailable")
    with TOKEN_FILE.open("r", encoding="utf-8") as handle:
        opened = os.fstat(handle.fileno())
        if (info.st_dev, info.st_ino) != (opened.st_dev, opened.st_ino):
            raise ValueError("credential identity changed")
        token = handle.read(4097).strip()
    if not token or len(token) > 4096 or any(ord(c) < 0x21 or ord(c) >= 0x7F for c in token):
        raise ValueError("credential invalid")
    transport = audited_transport(
        ledger, WORKER, origin=ORIGIN, token=token, same_origin_cookie="m_session_id=" + token
    )
    report = verify_round(transport, candidate)
    report.update(
        repo_id=REPO,
        hub_id=HUB_ID,
        object_path=TARGET,
        object_size=SIZE,
        revision_candidate=REVISION,
        identity_basis="prior_public_identity_not_fresh",
        proposed_http_attempt_cap=6,
        proposed_body_reservation_cap=6,
        proposed_accepted_body_cap=2,
        proposed_memory=footprint.memory,
        proposed_transfer_disk=footprint.transfer_disk,
    )
    with OUTPUT.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(report, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    print(json.dumps(report, sort_keys=True))
    return 0 if report["VERSION_BINDING"] == "PASS" else 3


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user-authorized-real-binding-probe", action="store_true")
    args = parser.parse_args(argv)
    if not args.user_authorized_real_binding_probe:
        print("HOLD_FOR_USER_AUTHORIZATION")
        return 0
    try:
        return future_real_round()
    except Exception:
        print("BLOCKED")
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
