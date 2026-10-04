"""Authorized three-source metadata pause/export/fresh-resume task."""
import argparse
import json
import subprocess
import sys
import uuid
from pathlib import Path

import real_canary as helpers

from sakurapool.runtime import RuntimeQuerySpec
from sakurapool.storage.budget import BudgetLedger
from sakurapool.storage.publication import load_publication
from sakurapool.tasks.export import export_task
from sakurapool.tasks.plan import Selection
from sakurapool.tasks.profile import validate_allowlist
from sakurapool.tasks.runner import create_task

ROOT = Path("D:/SakuraTool/SakuraPool-P5D-20261004T143905Z")
helpers.PUB = ROOT / "publication"


def profile():
    return {"format": "sakurapool-task-connection-v1", "origin": "https://modelscope.cn",
            "repositories": ["leafmoone/webdataset_danbooru_v3"],
            "worker": ("D:/SakuraTool/SakuraPool-Fix3-20260930a/"
                       "rust-target/release/sakurapool-worker.exe"),
            "credential_ref": {"file": "D:/sm_data/ms-token.tmp"}}


helpers.profile = profile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume", type=Path)
    args = parser.parse_args()
    ledger = BudgetLedger()
    if args.resume:
        result = helpers.run(args.resume, 2, resume=True)
        result["export"] = export_task(args.resume, args.resume / "final.jsonl", ledger)
        print(json.dumps(result), flush=True)
        return
    before, pending = ledger.status(), helpers.pending(ledger)
    required = {"disk": 256 << 20, "inflight": 128 << 20, "body": 32 << 20,
                "metadata": 4 << 20, "attempts": 64, "saved_samples": 3,
                "saved_bytes": 8 << 20}
    if any(ledger.limits[k] - before[k] < v for k, v in required.items()):
        print(json.dumps({"state": "RESOURCE_BLOCKED", "before": before}), flush=True)
        return
    selected = json.loads((ROOT / "real-task-selection.json").read_bytes())
    with load_publication(helpers.PUB) as pub:
        validate_allowlist(profile(), pub)
        assert len(selected) == 3 and len({r["source"] for r in selected}) == 3
        assert len({r["location"]["object_idx"] for r in selected}) == 3
        for row in selected:
            assert pub.runtime.location(row["rid"]) == row["location"]
            assert 0 < row["location"]["image_size"] <= 8 << 20
            assert 0 < row["location"]["metadata_size"] <= 8 << 20
    task = ledger.root / ("p5d-fixed-multi-" + uuid.uuid4().hex[:12])
    with create_task(helpers.PUB, task, ledger, RuntimeQuerySpec(),
                     Selection(mode="records", records=tuple(r["record_id"] for r in selected)),
                     metadata=True, max_output_bytes=8 << 20):
        pass
    print("task_created " + str(task), flush=True)
    first = helpers.run(task, 1, pause=True)
    assert first["result"]["state"] == "PAUSED"
    assert first["result"]["delivered_confirmed"] == 1
    first["partial"] = export_task(task, task / "partial.jsonl", ledger)
    child = subprocess.run([sys.executable, __file__, "--resume", str(task)],
                           check=True, capture_output=True, text=True)
    resumed = json.loads(child.stdout)
    assert resumed["result"]["state"] == "COMPLETED"
    a = [json.loads(s) for s in (task / "partial.jsonl").read_text().splitlines()]
    b = [json.loads(s) for s in (task / "final.jsonl").read_text().splitlines()]
    assert len(a) == 1 and len(b) == 3 and a == b[:1]
    verification = helpers.verify(task, True)
    after = ledger.status()
    assert after["inflight"] == 0 and helpers.pending(ledger) == pending
    assert verification["unknown"] == 0
    out = {"task": str(task), "selection": selected, "first": first,
           "fresh_resume": resumed, "verification": verification,
           "before": before, "after": after, "historical_pending_unchanged": True,
           "distribution": "three sources/three physical TAR objects",
           "metadata_sha_basis": "delivery receipt; publication provides image SHA only"}
    with (ROOT / "real-multi-object-metrics.json").open("x") as output:
        json.dump(out, output, indent=2)
    print(json.dumps(out), flush=True)


if __name__ == "__main__":
    main()
