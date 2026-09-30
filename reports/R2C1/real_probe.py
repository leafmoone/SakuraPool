"""ONE root-gated R2C1 round. No SDK/outer retry/extra page/alternate target.

Existing GuardedTransport <=3 attempts per metadata logical read (transient
retry/redirect included); Rust <=2 hops per byte operation, no retry. Any failed
logical gate stops. Never serialize raw results, exception text, ETag/CDN/URLs.
"""

import argparse
import json
import os
import stat
from dataclasses import replace
from pathlib import Path

from sakurapool.storage.budget import BudgetLedger, _disk_usage, is_reparse
from sakurapool.storage.modelscope import ModelScopeDataset
from sakurapool.storage.production import ProviderObject, RustProductionTransport, proof_key
from sakurapool.storage.production_resources import ProductionFootprint
from sakurapool.storage.transport import MAX_ATTEMPTS, GuardedTransport

CODE_COMMIT = "6a6010bfdb1daaff271c5ef3d4e046d5fd8616d4"
ORIGIN = "https://modelscope.cn"
REPO = "leafmoone/game_cg_5M"
HUB_ID = 218032
TARGET = "pre/gamecg-v1-pre-p02-003.tar"
SIZE = 1401159680
REVISION = "77948d890f654e6151a5ff7597b63a8337d8910e"
TOKEN_FILE = Path("D:/sm_data/ms-token.tmp")
WORKER = Path("D:/SakuraTool/SakuraPool-Fix3-20260930a/rust-target/release/sakurapool-worker.exe")
OUTPUT = Path(__file__).with_name("real-probe.json")
STARTED = Path(__file__).with_name("real-round.started")
MIB = 1 << 20
FLAGS = (
    "content_length_present",
    "content_range_present",
    "etag_present",
    "etag_is_strong",
    "content_encoding_present",
    "accounting_complete",
)


def budget(ledger):
    with ledger._locked():
        _, (_, used, leases, _) = ledger._read_pair()
        return {
            "total_including_pending": ledger._totals(used, leases),
            "known_settled": dict(used),
            "pending_leases": len(leases),
            "pending_unobserved_body_bound": sum(
                v["body"] - v["consumed_body"] for v in leases.values()
            ),
            "physical_root_bytes": _disk_usage(ledger.root),
        }


def public_probe(result):
    """Typed allowlist BEFORE serialization, never credentials/provider text."""
    out = {}
    for key, limit in (("status", 599), ("bytes", 2)):
        value = result.get(key)
        out[key] = value if type(value) is int and 0 <= value <= limit else None
    observation = result.get("observation", {})
    for key, limit in (
        ("origin_http_status", 599),
        ("cdn_http_status", 599),
        ("content_length", 2**63 - 1),
    ):
        value = observation.get(key) if isinstance(observation, dict) else None
        out[key] = value if type(value) is int and 0 <= value <= limit else None
    accounting = result.get("accounting", {})
    for key in ("attempts", "body"):
        value = accounting.get(key) if isinstance(accounting, dict) else None
        out[key] = value if type(value) is int and 0 <= value <= 2 else None
    diagnostic = result.get("diagnostic", {})
    for key in FLAGS:
        value = diagnostic.get(key) if isinstance(diagnostic, dict) else None
        out[key] = value if type(value) is bool else None
    phase = diagnostic.get("phase") if isinstance(diagnostic, dict) else None
    out["phase"] = phase if phase in ("origin", "cdn", "body", "scan") else None
    return out


class Control(GuardedTransport):
    """Instrumentation only: preserve existing finite retry/header/read behavior."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logical = self.attempts = self.accepted_bytes = 0

    def read_metadata(self, url, *, max_bytes=MIB):
        self.logical += 1
        if self.logical > 3 or max_bytes != MIB:
            raise ValueError("control round bound")
        raw = super().read_metadata(url, max_bytes=max_bytes)
        self.accepted_bytes += len(raw)
        return raw

    def _once(self, *args, **kwargs):
        self.attempts += 1
        if self.attempts > 9:
            raise ValueError("control attempt bound")
        return super()._once(*args, **kwargs)


def complete_result(result, body):
    a = result.get("accounting", {})
    return (
        type(a.get("complete")) is bool
        and a["complete"]
        and type(a.get("body")) is int
        and a["body"] == body
        and type(a.get("attempts")) is int
        and a["attempts"] == 2
    )


def run_round(ledger, token, report):
    control = transport = None
    phase = "control"
    try:
        with Control(
            ledger,
            trusted_hosts=frozenset({"modelscope.cn"}),
            token=token,
            credential_origin=ORIGIN,
            same_origin_cookie="m_session_id=" + token,
        ) as control:
            dataset = ModelScopeDataset(control, ORIGIN, REPO)
            hub = dataset.legacy_hub_id()
            if hub != HUB_ID:
                raise ValueError("fixed hub differs")
            files, _ = dataset.legacy_tree_page(hub, "master", root="pre", page_size=200)
            matches = [f for f in files if f.path == TARGET]
            if (
                len(matches) != 1
                or matches[0].size != SIZE
                or matches[0].revision_candidate != REVISION
            ):
                raise ValueError("fixed current target differs")
            files, _ = dataset.legacy_tree_page(hub, REVISION, root="pre", page_size=200)
            matches = [
                f
                for f in files
                if f.path == TARGET and f.size == SIZE and f.revision_candidate == REVISION
            ]
            if len(matches) != 1:
                raise ValueError("fixed candidate target differs")
            candidate = ProviderObject.from_tree(dataset, matches[0])
            candidate.validate()
        transport = RustProductionTransport(
            ledger, WORKER, origin=ORIGIN, token=token, same_origin_cookie="m_session_id=" + token
        )
        phase = "range"
        report["byte_logical_operations"] += 1
        transport.last_result = {}
        with transport.transfer(candidate, condition="observe") as (_, observed):
            report["range"] = public_probe(observed)
            if (
                observed.get("status") != 206
                or observed.get("bytes") != 1
                or not complete_result(observed, 1)
                or not observed.get("diagnostic", {}).get("etag_is_strong")
            ):
                raise ValueError("range proof rejected")
            # Frozen Rust requires EXACT CR0-0/SIZE, CL1, encoding, strongvalidator.
            bound = replace(candidate, validator=observed["etag"], cdn_host=observed["cdn_host"])
            bound.validate()
            if bound.validator == '"sakurapool-deliberately-wrong-r2"':
                raise ValueError("wrong validator must differ")
            digest = observed["sha256"]  # In-memory1B identity, not wholeobject digest.
            report["range"]["content_range_exact_0_0_total"] = True
        report["REAL_RANGE_CAPABILITY"] = "PASS"
        phase = "positive"
        report["byte_logical_operations"] += 1
        transport.last_result = {}
        with transport._capability_match(bound) as positive:
            report["positive"] = public_probe(positive)
            if (
                positive.get("status") != 206
                or positive.get("bytes") != 1
                or not complete_result(positive, 1)
                or positive.get("sha256") != digest
                or positive.get("etag") != bound.validator
                or positive.get("cdn_host") != bound.cdn_host
            ):
                raise ValueError("positive proof rejected")
            report["positive"].update(
                same_scope=True,
                same_byte=True,
                same_validator=True,
                content_range_exact_0_0_total=True,
            )
        report["REAL_IF_MATCH_POSITIVE"] = "PASS"
        phase = "negative"
        report["byte_logical_operations"] += 1
        transport.last_result = {}
        with transport.transfer(bound, condition="wrong") as (_, negative):
            report["negative"] = public_probe(negative)
            if (
                negative.get("status") != 412
                or negative.get("bytes") != 0
                or not complete_result(negative, 0)
                or negative.get("cdn_host") != bound.cdn_host
            ):
                raise ValueError("negative proof rejected")
            report["negative"].update(same_scope=True, intentionally_wrong_validator=True)
        ledger.record_condition_proof(proof_key(bound), digest)
        transport.register(bound)
        report.update(REAL_IF_MATCH_NEGATIVE="PASS", VERSION_BINDING="PASS", status="VERIFIED")
    except Exception:
        # Never str/repr/traceback/cause/context/rawresults/providerbody.
        report["failed_phase"] = phase
        if phase == "positive":
            report["REAL_IF_MATCH_POSITIVE"] = "BLOCKED"
            report["if_match_limitation"] = "IF_MATCH_UNSUPPORTED_OR_UNVERIFIED"
        elif phase == "negative":
            report["REAL_IF_MATCH_NEGATIVE"] = "BLOCKED"
            report["if_match_limitation"] = "IF_MATCH_UNSUPPORTED_OR_UNVERIFIED"
        if transport is not None:
            report[phase] = public_probe(getattr(transport, "last_result", {}))
    finally:
        if control is not None:
            report["control_logical_reads"] = control.logical
            report["control_attempts"] = control.attempts
            report["control_accepted_bytes"] = control.accepted_bytes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root-prerequisites-passed", action="store_true")
    args = parser.parse_args()
    if not args.root_prerequisites_passed:
        parser.error("HOLD: root prerequisites must pass before this single round")
    # Exclusive sentinel BEFOREcredentials/HTTP; failure is not permission to rerun.
    with STARTED.open("x", encoding="ascii") as handle:
        handle.write("R2C1 one authorized round; never automatically retry\n")
        handle.flush()
        os.fsync(handle.fileno())
    ledger = BudgetLedger()
    report = {
        "code_commit": CODE_COMMIT,
        "repo_id": REPO,
        "hub_id": HUB_ID,
        "object_path": TARGET,
        "object_size": SIZE,
        "revision_candidate": REVISION,
        "status": "BLOCKED",
        "REAL_RANGE_CAPABILITY": "BLOCKED",
        "REAL_IF_MATCH_POSITIVE": "NOT_RUN",
        "REAL_IF_MATCH_NEGATIVE": "NOT_RUN",
        "VERSION_BINDING": "BLOCKED",
        "byte_logical_operations": 0,
        "control_logical_reads": 0,
        "control_attempts": 0,
        "signed_url_persisted": False,
        "credentials_persisted": False,
        "full_tar_scan": False,
        "P4_COMPLETE": False,
        "budget_before": budget(ledger),
    }
    try:
        if MAX_ATTEMPTS != 3:
            raise ValueError("reviewed control ceiling changed")
        f = ProductionFootprint.admit("range", 1)
        b = ledger.status()
        if (
            b["attempts"] + 15 > ledger.limits["attempts"]
            or b["body"] + 9 * (MIB + 1) + 6 > ledger.limits["body"]
            or b["metadata"] + 9 * (MIB + 1) > ledger.limits["metadata"]
            or b["disk"] + f.transfer_disk > ledger.limits["disk"]
            or b["inflight"] + max(f.memory, 2 * (MIB + 1)) > ledger.limits["inflight"]
        ):
            raise ValueError("finite round unavailable")
        with TOKEN_FILE.open("r", encoding="utf-8") as handle:
            info = os.fstat(handle.fileno())
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or TOKEN_FILE.is_symlink()
                or is_reparse(TOKEN_FILE)
                or info.st_size > 4096
            ):
                raise ValueError("credential unavailable")
            token = handle.read(4097).strip()
        if not token or len(token) > 4096 or any(ord(c) < 0x21 or ord(c) >= 0x7F for c in token):
            raise ValueError("credential rejected")
        run_round(ledger, token, report)
    except Exception:
        report["failed_phase"] = "prerequisite"
    report["budget_after"] = budget(ledger)
    encoded = json.dumps(report, sort_keys=True)
    with OUTPUT.open("x", encoding="utf-8") as handle:
        handle.write(encoded + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    print(encoded)
    return 0 if report["status"] == "VERIFIED" else 3


if __name__ == "__main__":
    raise SystemExit(main())
