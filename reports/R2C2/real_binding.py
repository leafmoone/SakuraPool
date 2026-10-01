"""C2 scheduler of the FORMAL product binding path, not another HTTP protocol.

Default HOLD. Authorized execution uses append-only numbered evidence, fresh
origin/CDN resolution, ledger caps and backoff. A time health check pauses for
resumable diagnosis; it NEVER declares protocol unsupported or final BLOCKED.
"""

import argparse
import importlib.util
import json
import os
import stat
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "binding_core", ROOT / "reports/R2C1B/binding_probe.py"
)
core = importlib.util.module_from_spec(spec)
spec.loader.exec_module(core)
REAL = Path(__file__).with_name("real")
DEPENDENCIES = (
    "reports/R2C1B/binding_probe.py",
    "reports/R2C2/real_binding.py",
    "reports/R2C2/canary.py",
)


def code_identity():
    def git(*args):
        return subprocess.check_output(
            ["git", "-C", str(ROOT), *args], text=True, stderr=subprocess.DEVNULL
        ).strip()

    product = ("src", "rust", "tests", "pyproject.toml", *DEPENDENCIES)
    if git("diff", "HEAD", "--", *product) or git(
        "ls-files", "--others", "--exclude-standard", "--", *product
    ):
        raise ValueError("uncommitted product or helper source")
    if any(not git("ls-files", "--", path) for path in DEPENDENCIES):
        raise ValueError("untracked helper dependency")
    commit, tree = git("rev-parse", "HEAD"), git("rev-parse", "HEAD^{tree}")
    worker = core.WORKER.stat()
    return {
        "code_commit": commit,
        "code_tree": tree,
        "worker_path": str(core.WORKER),
        "worker_size": worker.st_size,
        "worker_mtime_ns": worker.st_mtime_ns,
    }


def numbered_evidence(directory=REAL):
    directory.mkdir(exist_ok=True)
    if directory.is_symlink():
        raise ValueError("unsafe evidence directory")
    number = 1
    while True:
        try:
            return number, (directory / f"round-{number:04d}.json").open("x", encoding="utf-8")
        except FileExistsError:
            number += 1


def finish(handle, report):
    try:
        handle.write(json.dumps(report, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    finally:
        handle.close()


def safe_snapshot(ledger):
    try:
        return core.snapshot(ledger)
    except Exception:
        return None  # Failure is explicit, never a guessed zero/refund.


def load_token():
    from sakurapool.storage.budget import is_reparse

    info = core.TOKEN_FILE.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or is_reparse(core.TOKEN_FILE)
        or info.st_size > 4096
    ):
        raise ValueError("credential unavailable")
    with core.TOKEN_FILE.open("r", encoding="utf-8") as handle:
        opened = os.fstat(handle.fileno())
        if (info.st_dev, info.st_ino) != (opened.st_dev, opened.st_ino):
            raise ValueError("credential identity changed")
        token = handle.read(4097).strip()
    if not token or len(token) > 4096 or any(ord(c) < 0x21 or ord(c) >= 0x7F for c in token):
        raise ValueError("credential malformed")
    return token


def classify(report):
    if report.get("VERSION_BINDING") == "PASS":
        return "PASS"
    positive = report.get("observations", {}).get("match", {})
    negative = report.get("observations", {}).get("wrong", {})
    # These extra booleans must be product-verified, not inferred from status/CL.
    # Current strict rejection path cannot prove them; retain UNVERIFIED instead.
    if (
        report.get("REAL_RANGE_CAPABILITY") == "PASS"
        and positive.get("accounting_complete") is True
        and positive.get("same_scope_verified") is True
        and positive.get("same_validator_verified") is True
        and positive.get("provider_transient_excluded") is True
        and positive.get("cdn_http_status") in (200, 412)
    ):
        return "SEMANTIC_FAIL"
    if (
        report.get("REAL_IF_MATCH_POSITIVE") == "PASS"
        and negative.get("accounting_complete") is True
        and negative.get("same_scope_verified") is True
        and negative.get("wrong_condition_accepted") is True
        and negative.get("cdn_http_status") == 206
    ):
        return "SEMANTIC_FAIL"
    return "TRANSIENT_OR_UNVERIFIED"


def budget_gate(ledger, *, operations=3):
    from sakurapool.storage.production_resources import ProductionFootprint

    footprint = ProductionFootprint.admit("range", 1)
    used = ledger.status()
    additions = {
        "attempts": operations * 2,
        "body": operations * 2,
        "disk": footprint.transfer_disk,
        "inflight": footprint.memory,
    }
    return all(used[k] + v <= ledger.limits[k] for k, v in additions.items())


def execute_evidenced(ledger, operation, identity, action, *, directory=REAL, public=None):
    """Close/write safe evidence on ordinary exceptions; persistence failure stops."""
    number, handle = numbered_evidence(directory)
    report = {
        **identity,
        "round": number,
        "operation": operation,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "status": "BLOCKED",
        "budget_before": safe_snapshot(ledger),
        "signed_url_persisted": False,
        "credentials_persisted": False,
        "validator_value_persisted": False,
        **(public or {}),
    }
    value = None
    report["ledger_snapshot_failed"] = report["budget_before"] is None
    identity_checked = False
    try:
        current_identity = code_identity()
        identity_checked = True
        if current_identity != identity:
            report["identity_mismatch"] = True
        elif report["budget_before"] is None:
            report["ledger_snapshot_failed"] = True
        else:
            value, details = action()
            report.update(details)
    except Exception:
        report["operation_failed"] = True  # No raw exception or chain serialization.
        # A pre-action identity check failure cannot be cleared by later recovery.
        if not identity_checked:
            report["identity_check_failed"] = True
    finally:
        try:
            post_mismatch = code_identity() != identity
            report["identity_mismatch"] = (
                bool(report.get("identity_mismatch") or report.get("identity_check_failed"))
                or post_mismatch
            )
        except Exception:
            report["identity_mismatch"] = True
        report["budget_after"] = safe_snapshot(ledger)
        report["ledger_snapshot_failed"] = (
            report["ledger_snapshot_failed"] or report["budget_after"] is None
        )
        if report["identity_mismatch"] or report["ledger_snapshot_failed"]:
            report.setdefault("origin_status", report["status"])
            report["status"] = "SOURCE_OR_LEDGER_BLOCKED"
            report["VERSION_BINDING"] = "BLOCKED"
        finish(handle, report)  # Raises on failure: caller MUST NOT retry.
    print(json.dumps(report, sort_keys=True), flush=True)
    return value, report


def outcome(status, report=None):
    """Safe control result: keep source/ledger and resumable causes across layers."""
    report = report or {}
    return {
        "status": status,
        "origin_status": report.get("origin_status", status),
        "resumable": status == "PAUSED_FOR_DIAGNOSTICS_NOT_VERIFIED",
        "protocol_conclusion": status in ("PASS", "CONDITIONAL_SEMANTIC_FAIL"),
        **{
            key: report[key]
            for key in (
                "identity_mismatch",
                "ledger_snapshot_failed",
                "settlement_failed",
                "primary_status",
                "failure_kind",
            )
            if key in report
        },
    }


def schedule(ledger, token, candidate, identity, *, directory=REAL):
    health_since, delay = time.monotonic(), 5
    while True:
        if not budget_gate(ledger):
            return None, outcome("CAPACITY_OR_BUDGET_BLOCKED")

        def action():
            transport = core.audited_transport(
                ledger,
                core.WORKER,
                origin=candidate.origin,
                token=token,
                same_origin_cookie="m_session_id=" + token,
            )
            report = core.verify_round(transport, candidate)
            report["status"] = classify(report)
            return transport, report

        transport, report = execute_evidenced(
            ledger,
            "verify_conditions",
            identity,
            action,
            directory=directory,
            public={
                "repo_id": candidate.repo_id,
                "object_path": candidate.object_path,
                "object_size": candidate.object_size,
                "revision_candidate": candidate.revision,
                "identity_basis": "prior_public_identity_not_fresh",
                "control_logical_reads": 0,
                "control_attempts": 0,
            },
        )
        if report.get("identity_mismatch") or report.get("ledger_snapshot_failed"):
            return None, outcome("SOURCE_OR_LEDGER_BLOCKED", report)
        verdict = classify(report)
        if verdict == "PASS":
            return transport, outcome("PASS", report)
        if verdict == "SEMANTIC_FAIL":
            return None, outcome("CONDITIONAL_SEMANTIC_FAIL", report)
        if report.get("operation_failed"):
            return None, outcome("RUNNER_OPERATION_BLOCKED", report)
        if time.monotonic() - health_since >= 300:
            # Resumable diagnosis node, NOT a final verdict or a round authorization cap.
            _, pause = execute_evidenced(
                ledger,
                "resumable_health_pause",
                identity,
                lambda: (
                    None,
                    {
                        "status": "PAUSED_FOR_DIAGNOSTICS_NOT_VERIFIED",
                        "resumable": True,
                        "protocol_conclusion": False,
                        "network_requests": 0,
                    },
                ),
                directory=directory,
            )
            if pause.get("identity_mismatch") or pause.get("ledger_snapshot_failed"):
                return None, outcome("SOURCE_OR_LEDGER_BLOCKED", pause)
            return None, outcome("PAUSED_FOR_DIAGNOSTICS_NOT_VERIFIED", pause)
        time.sleep(delay)
        delay = min(90, delay * 2)


def proof_use(transport, candidate, identity, *, directory=REAL):
    from sakurapool.storage.modelscope import ModelScopeDataset
    from sakurapool.storage.transport import BoundObject

    bound = transport._objects[candidate.object_path]
    retrieval = BoundObject(
        ModelScopeDataset(transport, bound.origin, bound.repo_id).download_url(
            bound.revision, bound.object_path
        ),
        bound.object_size,
        bound.revision,
        bound.validator,
        repository=bound.repo_id,
    )

    def action():
        if not budget_gate(transport.ledger, operations=1):
            return None, {"status": "CAPACITY_OR_BUDGET_BLOCKED", "network_requests": 0}
        transport.last_result = {}
        details = {"status": "BLOCKED"}
        try:
            with transport.read_range_owned(retrieval, 0, 1) as raw:
                if len(raw) != 1:
                    raise ValueError("exact Range missing")
            details.update(status="PASS", accepted_bytes=1)
        except Exception:
            pass
        details["observation"] = core.public_result(transport.last_result)
        return None, details

    _, report = execute_evidenced(
        transport.ledger,
        "registered_proof_range",
        identity,
        action,
        directory=directory,
        public={
            "repo_id": bound.repo_id,
            "object_path": bound.object_path,
            "object_size": bound.object_size,
        },
    )
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-authorized-c2", action="store_true")
    args = parser.parse_args(argv)
    if not args.run_authorized_c2:
        print("HOLD_FOR_USER_AUTHORIZATION")
        return 0
    try:
        from sakurapool.storage.budget import BudgetLedger

        identity = code_identity()  # Entire run fixed BEFORE token/network, checked each round.
        ledger = BudgetLedger()
        token = load_token()
        candidate = core.candidate_identity()
        transport, binding = schedule(ledger, token, candidate, identity)
        if binding["status"] != "PASS":
            print(json.dumps(binding, sort_keys=True))
            return 4 if binding["resumable"] else 3
        canary_spec = importlib.util.spec_from_file_location(
            "c2_canary", ROOT / "reports/R2C2/canary.py"
        )
        canary = importlib.util.module_from_spec(canary_spec)
        canary_spec.loader.exec_module(canary)
        result = canary.closure(transport, candidate, token, sys.modules[__name__], identity)
        _, report = execute_evidenced(
            ledger,
            "canary_closure_summary",
            identity,
            lambda: (None, {**result, "network_requests": 0}),
        )
        if report.get("identity_mismatch") or report.get("ledger_snapshot_failed"):
            return 3
        if result.get("resumable"):
            return 4
        return 0 if result["status"] in ("PASS", "REMOTE_READY_DOWNLOAD_CAPACITY_BLOCKED") else 3
    except Exception:
        print("RUNNER_OR_EVIDENCE_BLOCKED")
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
