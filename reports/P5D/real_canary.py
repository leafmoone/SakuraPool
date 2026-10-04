"""Approved same-TAR first3 tasks; historical pending only in parent memory."""
import argparse
import hashlib
import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

from sakurapool.runtime import RuntimeQuerySpec
from sakurapool.storage.budget import BudgetLedger
from sakurapool.storage.publication import load_publication
from sakurapool.tasks.export import export_task
from sakurapool.tasks.plan import Selection
from sakurapool.tasks.profile import connect_profile, validate_allowlist
from sakurapool.tasks.runner import create_task, run_task, verify_delivery
from sakurapool.tasks.store import TaskDB

PUB = Path("D:/SakuraTool/SakuraPool-R2C3-real-20261002/publication")


def profile():
    old = json.loads((PUB.parent / "profile.json").read_bytes())
    assert old["token_file"] == "D:/sm_data/ms-token.tmp"
    return {"format": "sakurapool-task-connection-v1", "origin": old["origin"],
            "repositories": [old["repo_id"]], "worker": old["worker"],
            "credential_ref": {"file": old["token_file"]}}


def pending(ledger):
    with ledger._locked():
        return ledger._read_pair()[1][2]


def run(task, workers, resume=False, pause=False):
    ledger, config = BudgetLedger(), profile()

    def hook(event, payload):
        if pause and event == "SETTLED":
            with TaskDB(task) as db:
                db.request("PAUSE")

    transport = connect_profile(config, ledger)
    start = time.perf_counter()
    try:
        result = run_task(task, transport, workers=workers, resume=resume,
                          fault_hook=hook, connection_profile=config)
    finally:
        transport.close()
    return {"result": result, "run_close_seconds": time.perf_counter() - start}


def verify(task_path, metadata):
    files = []
    with TaskDB(task_path, readonly=True) as task, load_publication(PUB) as pub:
        for item in task.db.execute("SELECT * FROM items ORDER BY seq"):
            verify_delivery(task, item)
            rid = pub.runtime.resolve_record(item["record_id"]).rid
            loc = pub.runtime.location(rid)
            directory = task_path / "output" / item["record_id"]
            data = next(directory.glob("image.*")).read_bytes()
            assert len(data) == loc["image_size"]
            assert hashlib.sha256(data).digest() == pub.expected_image_sha(rid)
            info = {"seq": item["seq"], "image_bytes": len(data), "image_sha_matches": True}
            if metadata:
                raw = next(directory.glob("metadata.*")).read_bytes()
                assert isinstance(json.loads(raw), dict) and len(raw) == loc["metadata_size"]
                info.update(metadata_bytes=len(raw), json_object=True,
                            metadata_sha_basis="delivery receipt; no independent publication SHA")
            files.append(info)
        attempts = task.db.execute("SELECT count(*) FROM attempts").fetchone()[0]
        assert attempts == 3 and [f["seq"] for f in files] == [0, 1, 2]
        return {"files": files, "attempts": attempts,
                "unknown": task.inspect()["unknown_accounting_count"]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume", type=Path)
    args = parser.parse_args()
    if args.resume:
        result = run(args.resume, 2, resume=True)
        result["export"] = export_task(args.resume, args.resume / "final.jsonl", BudgetLedger())
        print(json.dumps(result), flush=True)
        return
    ledger, config = BudgetLedger(), profile()
    before, old_pending = ledger.status(), pending(ledger)
    assert ledger.limits["disk"] - before["disk"] > 256 << 20
    assert ledger.limits["inflight"] - before["inflight"] >= 128 << 20
    assert ledger.limits["saved_samples"] - before["saved_samples"] >= 12
    with load_publication(PUB) as pub:
        validate_allowlist(config, pub)
        assert pub.manifest["object_count"] == 1
        for rid in range(3):
            loc = pub.runtime.location(rid)
            assert 0 < loc["image_size"] <= 8 << 20 and 0 < loc["metadata_size"] <= 8 << 20
    outcomes = []
    for metadata in (False, True):
        for workers in (1, 2):
            task = ledger.root / ("p5d-canary-" + uuid.uuid4().hex[:12])
            with create_task(PUB, task, ledger, RuntimeQuerySpec(),
                             Selection(mode="first", limit=3), metadata=metadata,
                             max_output_bytes=8 << 20):
                pass
            initial = ledger.status()
            closure = metadata and workers == 2
            first = run(task, 1 if closure else workers, pause=closure)
            if closure:
                assert first["result"]["state"] == "PAUSED"
                assert first["result"]["delivered_confirmed"] == 1
                first["partial"] = export_task(task, task / "partial.jsonl", ledger)
                child = subprocess.run([sys.executable, __file__, "--resume", str(task)],
                                       check=True, capture_output=True, text=True)
                first["fresh_resume"] = json.loads(child.stdout)
                a = [json.loads(s) for s in (task / "partial.jsonl").read_text().splitlines()]
                b = [json.loads(s) for s in (task / "final.jsonl").read_text().splitlines()]
                assert len(a) == 1 and len(b) == 3 and a == b[:1]
            else:
                assert first["result"]["delivered_confirmed"] == 3
            evidence = verify(task, metadata)
            after = ledger.status()
            assert after["inflight"] == 0 and pending(ledger) == old_pending
            assert evidence["unknown"] == 0
            outcomes.append({"task": str(task), "metadata": metadata, "workers": workers,
                             "pause_resume": closure, "run": first, "verification": evidence,
                             "global_delta_not_receipt": {k: after[k] - initial[k] for k in after},
                             "historical_pending_unchanged": True})
            print("TASK " + json.dumps(outcomes[-1]), flush=True)
    print("FINAL " + json.dumps({"tasks": outcomes, "before": before, "after": ledger.status(),
                                  "historical_pending_unchanged": pending(ledger) == old_pending,
                                  "distribution": "same TAR only"}), flush=True)


if __name__ == "__main__":
    main()
