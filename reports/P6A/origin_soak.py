"""Explicit bounded real-origin diagnostics; never a task or saved delivery."""
import argparse
import json
import sqlite3
import subprocess
import time
from pathlib import Path

from sakurapool.storage.modelscope import ModelScopeDataset
from sakurapool.storage.production import ProviderObject
from sakurapool.storage.transport import _SAFE_CODES
from sakurapool.tasks.profile import connect_profile, read_profile
from sakurapool.workspace import Workspace

REPO = "leafmoone/webdataset_danbooru_v3"
REVISION = "73306f1dc5459238710f477b376c36da997d020c"
OBJECT = "anime_pictures/t0992-001.tar"


def identity(publication):
    db = sqlite3.connect((publication / "remote_objects.sqlite").as_uri()
                         + "?mode=ro&immutable=1", uri=True)
    try:
        rows = db.execute(
            "SELECT r.endpoint,r.repo_type,o.object_size,o.provider_sha256 "
            "FROM objects o JOIN repositories r USING(repo_idx) "
            "WHERE r.repo_id=? AND o.revision_candidate=? AND o.object_path=? "
            "AND o.fetchable=1", (REPO, REVISION, OBJECT)).fetchall()
    finally:
        db.close()
    if len(rows) != 1 or rows[0][3] is None:
        raise ValueError("approved object identity unavailable")
    endpoint, kind, size, sha = rows[0]
    return ProviderObject(REPO, kind, endpoint, REVISION, OBJECT,
                          int.from_bytes(size, "big")), sha.hex()


def discover(profile, root, candidate, provider_sha):
    ws = Workspace.init(root)
    with connect_profile(profile, ws.ledger()) as transport:
        with transport.metadata_control() as control:
            dataset = ModelScopeDataset(control, candidate.origin, REPO)
            hub = dataset.legacy_hub_id()
            entry = dataset.find_legacy_file(hub, REVISION, root="anime_pictures",
                                             path=OBJECT, page_size=20)
            selected = ProviderObject.from_tree(dataset, entry)
            if selected != candidate or entry.provider_sha256 != provider_sha:
                raise ValueError("approved provider identity mismatch")
    return selected


def safe_accounting_basis(error):
    details = error.public_diagnostic() if hasattr(error, "public_diagnostic") else {}
    allowed = {"accounting_basis": "CONSERVATIVE_MAX_CHARGE",
               "actual_consumption": "UNKNOWN", "accounted": "CONSERVATIVE_MAX"}
    return {key: value for key, value in details.items()
            if key in allowed and type(value) is str and value == allowed[key]}


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
            if transport is None:
                transport = connect_profile(profile, ledger)
                if persistent:
                    transport.enable_persistent()
            started = time.perf_counter()
            row = {"iteration": iteration, "mode": mode, "label": label}
            failed = False
            artifact_root = None
            try:
                with transport.transfer(obj, condition="observe", length=1) as artifact:
                    artifact_root, result = artifact
                    row["status"] = result.get("status")
                    row["phase"] = "cdn"
                row.update(result="PASS", accounting="CONFIRMED")
            except Exception as error:
                failed = True
                code = getattr(error, "code", "rejected")
                row.update(result="FAIL", safe_code=code if code in _SAFE_CODES else "rejected")
                state = getattr(error, "accounting_state", "UNKNOWN")
                row["accounting"] = state if state in {"CONFIRMED", "UNKNOWN"} else "UNKNOWN"
                row.update(safe_accounting_basis(error))
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
            row["origin_reached"] = (True if row["origin_status"] is not None
                                     or row.get("phase") in {"origin", "cdn"} else None)
            row["cdn_reached"] = (True if row["cdn_status"] is not None
                                  or row.get("phase") == "cdn" else None)
            proc = getattr(worker, "_proc", None)
            row.update(worker_generation=generation if worker is not None else None,
                       worker_alive=proc.poll() is None if proc is not None else None,
                       artifact_ownership=("CLEANED" if artifact_root is not None
                                           and not artifact_root.exists() else "UNVERIFIED"),
                       elapsed_ms=round((time.perf_counter() - started) * 1000, 3),
                       pending_count=ledger.inspect_policy()["pending_count"])
            samples.append(row)
            print(json.dumps(row), flush=True)
            if not persistent:
                transport.close()
                transport = None
            if failed:
                break
    finally:
        if transport is not None:
            transport.close()
    return {"mode": mode, "label": label, "requested": count,
            "extra_requested": extra,
            "completed": len(samples), "baseline_pass": len(samples) >= count
            and all(row["result"] == "PASS" for row in samples[:count]),
            "extra_completed": max(0, len(samples) - count),
            "all_pass": len(samples) == count + extra
            and all(row["result"] == "PASS" for row in samples),
            "samples": samples, "ledger": ledger.inspect_policy()}


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
    candidate, sha = identity(args.publication.absolute())
    output = {"accounting_scope": "independent diagnostic workspaces; no task or saved samples",
              "code_tree": subprocess.check_output(["git", "write-tree"],
                                                     text=True).strip(),
              "code_state": ("frozen classification-only; no accounting changes"
                             if args.label == "original" else "frozen post-fix"),
              "worker": str(args.worker), "classification_only_before_core_fix":
              args.label == "original", "runs": []}
    try:
        obj = discover(profile, args.root / "discovery", candidate, sha)
    except Exception as error:
        code = getattr(error, "code", "rejected")
        state = getattr(error, "accounting_state", "UNKNOWN")
        status = getattr(error, "http_status", None)
        output["discovery"] = {
            "result": "BLOCKED", "safe_code": code if code in _SAFE_CODES else "rejected",
            "phase": getattr(error, "phase", "unavailable")
            if getattr(error, "code", None) in _SAFE_CODES else "unavailable",
            "accounting": state if state in {"CONFIRMED", "UNKNOWN"} else "UNKNOWN",
            "http_status": status if type(status) is int and 100 <= status <= 599 else None,
            "ledger": Workspace.open(args.root / "discovery").ledger().inspect_policy(),
        }
        (args.root / "results.json").write_text(json.dumps(output, indent=2), encoding="utf-8")
        print(json.dumps(output["discovery"]), flush=True)
        return
    output["discovery"] = {"result": "PASS", "page_size": 20}
    for mode in ("cold", "persistent"):
        run = mode_run(profile, args.root / mode, obj, mode, args.iterations, args.label,
                       extra=args.extra_persistent if mode == "persistent" else 0)
        output["runs"].append(run)
    (args.root / "results.json").write_text(json.dumps(output, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
