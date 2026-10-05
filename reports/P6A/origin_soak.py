"""Explicit bounded real-origin diagnostics; never a task or saved delivery."""

import argparse
import json
import sqlite3
import subprocess
import time
import uuid
from pathlib import Path
from urllib.parse import urlencode

from sakurapool.storage.modelscope import ModelScopeDataset
from sakurapool.storage.production import (
    _CONSERVATIVE_FINALIZED,
    _HTTP_STATUS_FINALIZED,
    ProviderObject,
)
from sakurapool.storage.publication import load_publication
from sakurapool.storage.transport import _SAFE_CODES
from sakurapool.tasks.profile import connect_profile, read_profile
from sakurapool.workspace import Workspace

REPO = "leafmoone/webdataset_danbooru_v3"
REVISION = "73306f1dc5459238710f477b376c36da997d020c"
OBJECT = "anime_pictures/t0992-001.tar"


def identity(publication):
    db = sqlite3.connect(
        (publication / "remote_objects.sqlite").as_uri() + "?mode=ro&immutable=1", uri=True
    )
    try:
        rows = db.execute(
            "SELECT r.endpoint,r.repo_type,o.object_size,o.provider_sha256 "
            "FROM objects o JOIN repositories r USING(repo_idx) "
            "WHERE r.repo_id=? AND o.revision_candidate=? AND o.object_path=? "
            "AND o.fetchable=1",
            (REPO, REVISION, OBJECT),
        ).fetchall()
    finally:
        db.close()
    if len(rows) != 1 or rows[0][3] is None:
        raise ValueError("approved object identity unavailable")
    endpoint, kind, size, sha = rows[0]
    return ProviderObject(
        REPO, kind, endpoint, REVISION, OBJECT, int.from_bytes(size, "big")
    ), sha.hex()


def discover(profile, root, candidate, provider_sha):
    ws = Workspace.init(root)
    with connect_profile(profile, ws.ledger()) as transport:
        with transport.metadata_control() as control:
            dataset = ModelScopeDataset(control, candidate.origin, REPO)
            hub = dataset.legacy_hub_id()
            entry = dataset.find_legacy_file(
                hub, REVISION, root="anime_pictures", path=OBJECT, page_size=20
            )
            selected = ProviderObject.from_tree(dataset, entry)
            if selected != candidate or entry.provider_sha256 != provider_sha:
                raise ValueError("approved provider identity mismatch")
    return selected


def unknown_row(row, code=None):
    candidate = row.get("safe_code") if code is None else code
    row.update(
        result="TRUE_UNKNOWN",
        accounting="UNKNOWN",
        safe_code=candidate if type(candidate) is str and candidate in _SAFE_CODES else "rejected",
    )
    for key in ("accounting_basis", "actual_consumption", "accounted"):
        row.pop(key, None)


def safe_inspect(ledger):
    try:
        return ledger.inspect_policy()
    except BaseException:
        return {"pending_count": None, "accounting": "UNKNOWN"}


def tree_differential(profile, root, candidate, repeats=6):
    """Bounded fixed-request comparison; no header values or response bodies saved."""
    ws = Workspace.init(root)
    samples = []
    query = urlencode(
        {
            "Revision": REVISION,
            "Root": "anime_pictures",
            "Recursive": "True",
            "PageNumber": 1,
            "PageSize": 20,
        }
    )
    url = f"{candidate.origin}/api/v1/datasets/218254/repo/tree?{query}"
    try:
        with connect_profile(profile, ws.ledger()) as transport:
            with transport.metadata_control() as control:
                return _tree_samples(control, ws, candidate, url, samples, repeats)
    except BaseException:
        samples.append(
            {
                "result": "BLOCKED",
                "accounting": "UNKNOWN",
                "safe_code": "rejected",
                "phase": "finalization",
            }
        )
    return {"samples": samples, "ledger": safe_inspect(ws.ledger())}


def _tree_samples(control, ws, candidate, url, samples, repeats):
    # This finite loop owns samples; outer context failures append rather than replace.
    for iteration in range(1, repeats + 1):
        for variant in ("A", "B"):
            if variant == "B":
                control.session.headers["Content-Type"] = "application/json"
                control.session.headers["X-Request-ID"] = str(uuid.uuid4())
            else:
                control.session.headers.pop("Content-Type", None)
                control.session.headers.pop("X-Request-ID", None)
            before = ws.ledger().inspect_policy()["usage"]["attempts"]
            row = {
                "variant": variant,
                "iteration": iteration,
                "added_header_names": [] if variant == "A" else ["Content-Type", "X-Request-ID"],
            }
            try:
                data = ModelScopeDataset(control, candidate.origin, REPO)._data(
                    url, phase="provider_tree_shape"
                )
                if not isinstance(data, dict) or not isinstance(data.get("Files"), list):
                    row.update(result="SHAPE_REJECTED", safe_code="provider_page_shape")
                else:
                    row.update(result="PASS", http_status=200)
            except Exception as error:
                diagnostic = (
                    error.public_diagnostic() if hasattr(error, "public_diagnostic") else {}
                )
                row.update(
                    result="BLOCKED",
                    safe_code=diagnostic.get("code", "rejected"),
                    http_status=diagnostic.get("http_status"),
                    accounting=diagnostic.get("accounting", "UNKNOWN"),
                )
            samples.append(row)
            row["attempts"] = ws.ledger().inspect_policy()["usage"]["attempts"] - before
        if all(
            any(r.get("variant") == v and r.get("http_status") == 400 for r in samples)
            and any(r.get("variant") == v and r["result"] == "PASS" for r in samples)
            for v in ("A", "B")
        ):
            break
    return {"samples": samples, "ledger": safe_inspect(ws.ledger())}


def safe_accounting_basis(error):
    details = error.public_diagnostic() if hasattr(error, "public_diagnostic") else {}
    allowed = {
        "accounting_basis": "CONSERVATIVE_MAX_CHARGE",
        "actual_consumption": "UNKNOWN",
        "accounted": "CONSERVATIVE_MAX",
    }
    return {
        key: value
        for key, value in details.items()
        if key in allowed and type(value) is str and value == allowed[key]
    }


def pending_snapshot(ledger):
    # Evidence stays in parent memory, never serialized as a ledger dump.
    with ledger._locked():
        _, (_, _, leases, _) = ledger._read_pair()
        return {key: dict(value) for key, value in leases.items()}


def mode_run(profile, root, obj, mode, count, label, extra=0):
    ws = Workspace.init(root)
    ledger = ws.ledger()
    persistent = mode == "persistent"
    transport = None
    samples = []
    generation = 0
    last_worker = None
    try:
        for iteration in range(1, count + extra + 1):
            row = {"iteration": iteration, "mode": mode, "label": label}
            if transport is None:
                transport = connect_profile(profile, ledger)
                if persistent:
                    transport.enable_persistent()
            started = time.perf_counter()
            row = {"iteration": iteration, "mode": mode, "label": label}
            failed = False
            before_pending = pending_snapshot(ledger)
            resident = getattr(transport, "_lane_lease", None)
            baseline_safe = not before_pending or (
                persistent
                and set(before_pending) == {resident}
                and all(
                    before_pending[resident].get(key, 0) == 0
                    for key in ("body", "disk", "metadata")
                )
            )
            if not baseline_safe:
                unknown_row(row)
                samples.append(row)
                break
            before_usage = ledger.inspect_policy()["usage"]
            before_artifacts = set(ledger.root.glob("rust-transfer-*"))
            try:
                transport.verify_conditions(obj)
                row.update(
                    result="PASS",
                    accounting="CONFIRMED",
                    proof_steps="ALL_PASS",
                    phase="cdn",
                    status=412,
                )
            except Exception as error:
                failed = True
                code = getattr(error, "code", "rejected")
                row.update(result="FAIL", safe_code=code if code in _SAFE_CODES else "rejected")
                state = getattr(error, "accounting_state", "UNKNOWN")
                row["accounting"] = state if state in {"CONFIRMED", "UNKNOWN"} else "UNKNOWN"
                row.update(safe_accounting_basis(error))
                after_pending = pending_snapshot(ledger)
                after_usage = ledger.inspect_policy()["usage"]
                safe_recovery = (
                    state == "CONFIRMED"
                    and (
                        getattr(error, "_conservative_finalized", None) is _CONSERVATIVE_FINALIZED
                        or (
                            getattr(error, "_http_status_finalized", None) is _HTTP_STATUS_FINALIZED
                            and code in {"origin_status", "cdn_status"}
                        )
                    )
                    and after_pending == before_pending
                    and after_usage["saved_samples"] == before_usage["saved_samples"]
                    and after_usage["saved_bytes"] == before_usage["saved_bytes"]
                    and set(ledger.root.glob("rust-transfer-*")) == before_artifacts
                    and not getattr(error, "finalization_secondary", ())
                )
                row["result"] = (
                    "KNOWN_STATUS_REJECTION"
                    if safe_recovery and code in {"origin_status", "cdn_status"}
                    else "CONFIRMED_TRANSIENT"
                    if safe_recovery
                    else "TRUE_UNKNOWN"
                )
                row["pending_identity_unchanged"] = after_pending == before_pending
                row["proof_steps"] = "INCOMPLETE"
                failed = not safe_recovery
                last = getattr(transport, "last_result", {})
                diagnostic = last.get("diagnostic", {})
                row["phase"] = diagnostic.get("phase", "unavailable")
                row["status"] = diagnostic.get("http_status")
                row["accounting_complete"] = diagnostic.get("accounting_complete", False)
            worker = getattr(transport, "_request_worker", None)
            if worker is not last_worker:
                generation += 1
                last_worker = worker
            observation = getattr(transport, "last_result", {}).get("observation", {})
            row["origin_status"] = observation.get("origin_http_status")
            row["cdn_status"] = observation.get("cdn_http_status")
            row["origin_reached"] = (
                True
                if row["origin_status"] is not None or row.get("phase") in {"origin", "cdn"}
                else None
            )
            row["cdn_reached"] = (
                True if row["cdn_status"] is not None or row.get("phase") == "cdn" else None
            )
            row["transport_generation"] = getattr(transport, "_generation", None)
            proc = getattr(worker, "_proc", None)
            after_pending = pending_snapshot(ledger)
            after_usage = ledger.inspect_policy()["usage"]
            row["pending_identity_unchanged"] = after_pending == before_pending
            row["body_charged"] = after_usage["body"] - before_usage["body"]
            row["saved_unchanged"] = all(
                after_usage[key] == before_usage[key] for key in ("saved_samples", "saved_bytes")
            )
            row["no_output"] = set(ledger.root.glob("rust-transfer-*")) == before_artifacts
            if not all(
                row[key] for key in ("pending_identity_unchanged", "saved_unchanged", "no_output")
            ):
                unknown_row(row)
                failed = True
            row.update(
                worker_generation=generation if worker is not None else None,
                worker_alive=proc.poll() is None if proc is not None else None,
                artifact_ownership=("SCOPE_UNCHANGED" if row["no_output"] else "UNVERIFIED"),
                elapsed_ms=round((time.perf_counter() - started) * 1000, 3),
                pending_count=ledger.inspect_policy()["pending_count"],
            )
            samples.append(row)
            if not persistent:
                try:
                    transport.close()
                except BaseException:
                    row["close_secondary"] = "worker_close"
                    unknown_row(row)
                    failed = True
                transport = None
            print(json.dumps(row), flush=True)
            if failed:
                break
    except BaseException:
        terminal = dict(row) if "row" in locals() else {"phase": "setup"}
        unknown_row(terminal)
        if not samples or samples[-1].get("iteration") != terminal.get("iteration"):
            samples.append(terminal)
        else:
            unknown_row(samples[-1])
    finally:
        if transport is not None:
            try:
                transport.close()
            except BaseException:
                terminal = {"phase": "close"}
                unknown_row(terminal)
                samples.append(terminal)
    final_pending = safe_inspect(ledger)["pending_count"]
    if final_pending != 0:
        terminal = {"phase": "final_pending"}
        unknown_row(terminal)
        samples.append(terminal)
    terminal_rows = [r for r in samples if "iteration" not in r]
    samples = [r for r in samples if "iteration" in r]
    return {
        "terminal_failures": terminal_rows,
        "mode_safe": not terminal_rows
        and final_pending == 0
        and all(r["result"] != "TRUE_UNKNOWN" for r in samples),
        "known_status_rejection": sum(row["result"] == "KNOWN_STATUS_REJECTION" for row in samples),
        "resident_closed": getattr(transport, "_lane_lease", None) is None,
        "pass_count": sum(row["result"] == "PASS" for row in samples),
        "confirmed_transient": sum(row["result"] == "CONFIRMED_TRANSIENT" for row in samples),
        "true_unknown": sum(row["result"] == "TRUE_UNKNOWN" for row in samples),
        "pending_final": safe_inspect(ledger)["pending_count"],
        "error_counts": {
            code: sum(row.get("safe_code") == code for row in samples)
            for code in sorted(_SAFE_CODES)
            if code.startswith(("origin_", "cdn_"))
        },
        "mode": mode,
        "label": label,
        "requested": count,
        "extra_requested": extra,
        "completed": len(samples),
        "baseline_pass": not terminal_rows
        and len(samples) >= count
        and all(
            row["result"] in {"PASS", "KNOWN_STATUS_REJECTION", "CONFIRMED_TRANSIENT"}
            for row in samples[:count]
        ),
        "extra_completed": max(0, len(samples) - count),
        "all_pass": not terminal_rows
        and len(samples) == count + extra
        and all(row["result"] == "PASS" for row in samples),
        "samples": samples,
        "ledger": safe_inspect(ledger),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--publication", type=Path, required=True)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--worker", type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--extra-persistent", type=int, default=500)
    parser.add_argument("--label", choices=("original", "post-fix"), required=True)
    args = parser.parse_args()
    if args.iterations <= 0 or args.extra_persistent < 0:
        parser.error("bounded iteration counts required")
    args.root.mkdir(parents=True, exist_ok=False)
    profile = read_profile(args.profile)
    profile["worker"] = str(args.worker)
    with load_publication(args.publication.absolute(), full_verify=True) as publication:
        verified_publication = publication.inspect()
    candidate, sha = identity(args.publication.absolute())
    output = {
        "accounting_scope": "independent diagnostic workspaces; no task or saved samples",
        "code_tree": subprocess.check_output(["git", "write-tree"], text=True).strip(),
        "code_state": (
            "frozen classification-only; no accounting changes"
            if args.label == "original"
            else "frozen post-fix"
        ),
        "publication_full_verify": verified_publication,
        "worker": str(args.worker),
        "classification_only_before_core_fix": args.label == "original",
        "runs": [],
    }
    try:
        discover(profile, args.root / "discovery", candidate, sha)
    except Exception as error:
        code = getattr(error, "code", "rejected")
        state = getattr(error, "accounting_state", "UNKNOWN")
        status = getattr(error, "http_status", None)
        output["discovery"] = {
            "result": "BLOCKED",
            "safe_code": code if code in _SAFE_CODES else "rejected",
            "phase": getattr(error, "phase", "unavailable")
            if getattr(error, "code", None) in _SAFE_CODES
            else "unavailable",
            "accounting": state if state in {"CONFIRMED", "UNKNOWN"} else "UNKNOWN",
            "http_status": status if type(status) is int and 100 <= status <= 599 else None,
            "ledger": Workspace.open(args.root / "discovery").ledger().inspect_policy(),
        }
        (args.root / "results.json").write_text(json.dumps(output, indent=2), encoding="utf-8")
        print(json.dumps(output["discovery"]), flush=True)
    else:
        output["discovery"] = {"result": "PASS", "page_size": 20}
    # Publication verification and identity, not provider tree availability,
    # admit the independent data-plane diagnostic.
    obj = candidate
    try:
        output["tree_differential"] = tree_differential(
            profile, args.root / "tree-differential", candidate
        )
    except Exception:
        output["tree_differential"] = {"result": "BLOCKED", "safe_code": "rejected"}
    (args.root / "results.json").write_text(json.dumps(output, indent=2), encoding="utf-8")
    for mode in ("cold", "persistent"):
        run = mode_run(
            profile,
            args.root / mode,
            obj,
            mode,
            args.iterations,
            args.label,
            extra=args.extra_persistent if mode == "persistent" else 0,
        )
        output["runs"].append(run)
        (args.root / "results.json").write_text(json.dumps(output, indent=2), encoding="utf-8")
    (args.root / "results.json").write_text(json.dumps(output, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
