"""One new user-authorized UA/Accept Range0-0 comparison; no If-Match/retry.

Reuse prior independently tree-verified PUBLIC identity, not signedURL/validator.
Frozen product resolves fresh origin→CDN and retains strict resource/framing gates.
"""

import argparse
import importlib.util
import json
import os
import stat
from pathlib import Path

from sakurapool.storage.budget import BudgetLedger, is_reparse
from sakurapool.storage.production import ProviderObject, RustProductionTransport
from sakurapool.storage.production_resources import ProductionFootprint

spec = importlib.util.spec_from_file_location(
    "r2c1_probe", Path(__file__).with_name("real_probe.py")
)
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)
CODE_COMMIT = "f7c4658dac7ff6e5db977e8b2d53db504d5baaaf"
STARTED = Path(__file__).with_name("header-round.started")
OUTPUT = Path(__file__).with_name("header-probe.json")


def observed_pass(result):
    return (
        result.get("status") == 206
        and result.get("bytes") == 1
        and base.complete_result(result, 1)
        and result.get("diagnostic", {}).get("etag_is_strong") is True
    )


def public_identity():
    previous = json.loads(Path(__file__).with_name("real-probe.json").read_text(encoding="utf-8"))
    expected = {
        "repo_id": base.REPO,
        "hub_id": base.HUB_ID,
        "object_path": base.TARGET,
        "object_size": base.SIZE,
        "revision_candidate": base.REVISION,
        "code_commit": base.CODE_COMMIT,
        "control_logical_reads": 3,
        "control_attempts": 3,
        "failed_phase": "range",
    }
    if any(previous.get(key) != value for key, value in expected.items()):
        raise ValueError("previous independent target verification differs")
    candidate = ProviderObject(
        base.REPO, "modelscope_dataset_legacy", base.ORIGIN, base.REVISION, base.TARGET, base.SIZE
    )
    candidate.validate()
    return candidate


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root-header-prerequisites-passed", action="store_true")
    args = parser.parse_args()
    if not args.root_header_prerequisites_passed:
        parser.error("HOLD: root must verify this separately authorized header experiment")
    with STARTED.open("x", encoding="ascii") as handle:
        handle.write("One UA/Accept Range observe; no retries/If-Match/fullstream\n")
        handle.flush()
        os.fsync(handle.fileno())
    ledger = BudgetLedger()
    report = {
        "code_commit": CODE_COMMIT,
        "baseline_commit": base.CODE_COMMIT,
        "status": "BLOCKED",
        "REAL_RANGE_CAPABILITY": "BLOCKED",
        "REAL_IF_MATCH_POSITIVE": "NOT_RUN",
        "REAL_IF_MATCH_NEGATIVE": "NOT_RUN",
        "VERSION_BINDING": "BLOCKED",
        "P4_COMPLETE": False,
        "repo_id": base.REPO,
        "hub_id": base.HUB_ID,
        "object_path": base.TARGET,
        "object_size": base.SIZE,
        "revision_candidate": base.REVISION,
        "request_user_agent": "SakuraMoon/1",
        "request_accept": "application/json, application/octet-stream",
        "identity_basis": "prior_three_control_reads_exact_target; not fresh revalidation",
        "control_logical_reads": 0,
        "control_attempts": 0,
        "byte_logical_operations": 0,
        "signed_url_persisted": False,
        "credentials_persisted": False,
        "full_tar_scan": False,
        "budget_before": base.budget(ledger),
    }
    transport = None
    phase = "prerequisite"
    try:
        candidate = public_identity()
        f = ProductionFootprint.admit("range", 1)
        b = ledger.status()
        if (
            b["attempts"] + 2 > ledger.limits["attempts"]
            or b["body"] + 2 > ledger.limits["body"]
            or b["disk"] + f.transfer_disk > ledger.limits["disk"]
            or b["inflight"] + f.memory > ledger.limits["inflight"]
        ):
            raise ValueError("header round resource unavailable")
        with base.TOKEN_FILE.open("r", encoding="utf-8") as handle:
            info = os.fstat(handle.fileno())
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or base.TOKEN_FILE.is_symlink()
                or is_reparse(base.TOKEN_FILE)
                or info.st_size > 4096
            ):
                raise ValueError("credential unavailable")
            token = handle.read(4097).strip()
        if not token or len(token) > 4096 or any(ord(c) < 0x21 or ord(c) >= 0x7F for c in token):
            raise ValueError("credential rejected")
        transport = RustProductionTransport(
            ledger,
            base.WORKER,
            origin=base.ORIGIN,
            token=token,
            same_origin_cookie="m_session_id=" + token,
        )
        transport.last_result = {}
        phase = "range"
        report["byte_logical_operations"] = 1
        with transport.transfer(candidate, condition="observe", start=0, length=1) as (_, result):
            report["range"] = base.public_probe(result)
            if not observed_pass(result):
                raise ValueError("strict range header comparison rejected")
            report["range"]["content_range_exact_0_0_total"] = True
        report.update(status="RANGE_ONLY_PASS", REAL_RANGE_CAPABILITY="PASS")
        # NO If-Match/proof registration: Range is not version binding.
    except Exception:
        report["failed_phase"] = phase
        if transport is not None:
            report["range"] = base.public_probe(getattr(transport, "last_result", {}))
    report["budget_after"] = base.budget(ledger)
    encoded = json.dumps(report, sort_keys=True)
    with OUTPUT.open("x", encoding="utf-8") as handle:
        handle.write(encoded + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    print(encoded)
    return 0 if report["status"] == "RANGE_ONLY_PASS" else 3


if __name__ == "__main__":
    raise SystemExit(main())
