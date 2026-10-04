import argparse
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import real_canary as helpers

from sakurapool.fs_safety import plain_entry
from sakurapool.runtime import RuntimeQuerySpec
from sakurapool.storage.budget import BudgetLedger
from sakurapool.storage.publication import load_publication
from sakurapool.tasks.export import export_task
from sakurapool.tasks.plan import Selection
from sakurapool.tasks.profile import validate_allowlist
from sakurapool.tasks.runner import check_publication_identity, create_task
from sakurapool.tasks.store import TaskDB

ROOT = Path("D:/SakuraTool/SakuraPool-P5D-20261004T143905Z")
helpers.PUB = ROOT / "publication"
RESUME_METRICS = ROOT / "real-multi-object-resume-metrics.json"


def profile():
    return {"format": "sakurapool-task-connection-v1", "origin": "https://modelscope.cn",
            "repositories": ["leafmoone/webdataset_danbooru_v3"],
            "worker": ("D:/SakuraTool/SakuraPool-Fix3-20260930a/"
                       "rust-target/release/sakurapool-worker.exe"),
            "credential_ref": {"file": "D:/sm_data/ms-token.tmp"}}


helpers.profile = profile


def attempt_rows(db):
    return [dict(row) for row in db.db.execute("SELECT * FROM attempts ORDER BY attempt_id")]


def check_history(db, history):
    current = {row["attempt_id"]: row for row in attempt_rows(db)}
    assert all(current.get(row["attempt_id"]) == row for row in history)


def task_status(task):
    with TaskDB(task, readonly=True) as db:
        status = db.inspect()
        status["attempts"] = db.db.execute("SELECT count(*) FROM attempts").fetchone()[0]
        status["unknown_attempts"] = db.db.execute(
            "SELECT count(*) FROM attempts WHERE accounting='UNKNOWN'").fetchone()[0]
        return status


def preflight_existing(task, ledger, before):
    """Local reads only; do not recover, create, rebuild, or connect a transport."""
    assert not os.path.lexists(RESUME_METRICS)
    assert task.absolute().parent == ledger.root.absolute()
    for name in ("partial.jsonl", "final.jsonl"):
        assert not os.path.lexists(task / name)
    output = plain_entry(task / "output", directory=True)
    assert not list(output.iterdir())
    config = profile()
    plain_entry(Path(config["worker"]))
    plain_entry(Path(config["credential_ref"]["file"]))
    required = {"disk": 256 << 20, "inflight": 128 << 20, "body": 32 << 20,
                "metadata": 4 << 20, "attempts": 64, "saved_samples": 3,
                "saved_bytes": 8 << 20}
    assert before["inflight"] == 0
    assert all(ledger.limits[k] - before[k] >= v for k, v in required.items())
    selected = json.loads((ROOT / "real-task-selection.json").read_bytes())
    assert len(selected) == 3 and len({r["source"] for r in selected}) == 3
    assert len({r["location"]["object_idx"] for r in selected}) == 3
    with TaskDB(task, readonly=True) as db:
        header = db.validate_plan()
        assert Path(db.meta("publication_path")) == helpers.PUB.absolute()
        assert header["metadata"] is True and header["selection"]["mode"] == "records"
        assert db.meta("max_output_bytes") == 8 << 20
        status = db.inspect()
        assert status["state"] == "BLOCKED" and status["request"] is None
        assert status["delivered_confirmed"] == 0 and status["unknown_accounting_count"] == 0
        items = [dict(row) for row in db.db.execute("SELECT * FROM items ORDER BY seq")]
        assert [row["seq"] for row in items] == [0, 1, 2]
        for index, (item, frozen) in enumerate(zip(items, selected)):
            assert all(item[key] == frozen[key] for key in ("rid", "record_id", "source"))
            assert item["state"] == "READY" and item["delivery"] == "NONE"
            assert item["receipt"] is None
            assert item["accounting"] == ("CONFIRMED" if index == 0 else "NONE")
            if index:
                assert item["attempt_id"] is None and item["code"] is None
        history = attempt_rows(db)
        assert history and items[0]["attempt_id"] in {r["attempt_id"] for r in history}
        assert all(row["seq"] == 0 and row["accounting"] == "CONFIRMED"
                   and row["delivery"] == "NONE" and row["receipt"] is None
                   and row["output_lease"] is None for row in history)
        # One full local publication load; no publication rebuild or provider access.
        with load_publication(helpers.PUB, full_verify=True) as pub:
            check_publication_identity(db, pub)
            validate_allowlist(config, pub)
            for item, frozen in zip(items, selected):
                assert pub.runtime.resolve_record(item["record_id"]).rid == item["rid"]
                assert pub.runtime.location(item["rid"]) == frozen["location"]
                assert 0 < frozen["location"]["image_size"] <= 8 << 20
                assert 0 < frozen["location"]["metadata_size"] <= 8 << 20
        return {"history_attempts": len(history),
                "plan_digest": db.meta("plan_digest"), "status": status}, history


def continue_existing(task):
    ledger = BudgetLedger()
    out = {"task": str(task), "state": "NOT_COMPLETED", "first": None,
           "partial": None, "child": {"state": "NOT_STARTED"}, "final": None,
           "verification": None, "after": None, "task_after": None,
           "historical_pending_comparison": "NOT_COMPLETED",
           "new_pending_comparison": "NOT_COMPLETED"}
    before_pending = None
    history = None
    try:
        out["before"] = ledger.status()
        before_pending = helpers.pending(ledger)
        out["preflight"], history = preflight_existing(task, ledger, out["before"])
        out["first"] = helpers.run(task, 1, resume=True, pause=True)
        assert out["first"]["result"]["state"] == "PAUSED"
        assert out["first"]["result"]["delivered_confirmed"] == 1
        with TaskDB(task, readonly=True) as db:
            check_history(db, history)
            items = list(db.db.execute("SELECT * FROM items ORDER BY seq"))
            assert (items[0]["state"], items[0]["delivery"], items[0]["accounting"]) == (
                "DONE", "PUBLISHED", "CONFIRMED")
            assert all(row["state"] == "READY" and row["delivery"] == "NONE"
                       and row["accounting"] == "NONE" and row["attempt_id"] is None
                       for row in items[1:])
            assert db.inspect()["unknown_accounting_count"] == 0
        out["partial"] = export_task(task, task / "partial.jsonl", ledger)
        assert out["partial"]["exported"] == 1
        out["child"] = {"state": "STARTED"}
        child = subprocess.run([sys.executable, __file__, "--resume", str(task)],
                               check=False, capture_output=True, text=True)
        out["child"] = {"state": "FAILED" if child.returncode else "RETURNED",
                        "returncode": child.returncode}
        if child.returncode:
            # Child stdout/stderr may contain credentials: persist neither.
            raise RuntimeError("CHILD_FAILED")
        resumed = json.loads(child.stdout)
        assert resumed["status"] == "PASS"
        out["child"].update(state="PASS", run=resumed["run"])
        out["final"] = resumed["export"]
        assert resumed["run"]["result"]["state"] == "COMPLETED"
        assert resumed["run"]["result"]["delivered_confirmed"] == 3
        a = [json.loads(s) for s in (task / "partial.jsonl").read_text().splitlines()]
        b = [json.loads(s) for s in (task / "final.jsonl").read_text().splitlines()]
        assert len(a) == 1 and len(b) == 3 and a == b[:1]
        assert [row["seq"] for row in a] == [0]
        assert [row["seq"] for row in b] == [0, 1, 2]
        assert len({row["record_id"] for row in b}) == 3
        out["verification"] = helpers.verify(task, True, history_attempts=len(history))
        with TaskDB(task, readonly=True) as db:
            check_history(db, history)
            assert db.meta("state") == "COMPLETED"
        out["state"] = "PASS"
    except BaseException as error:
        out["error"] = helpers.safe_error(error)
        if out["child"]["state"] == "STARTED":
            out["child"] = {"state": "FAILED", "error": helpers.safe_error(error)}
        raise
    finally:
        primary = sys.exc_info()[1]
        secondary = []
        for key, read in (("after", ledger.status), ("task_after", lambda: task_status(task))):
            try:
                out[key] = read()
            except BaseException as error:
                secondary.append({"phase": key, **helpers.safe_error(error)})
        try:
            if before_pending is not None and helpers.pending(ledger) == before_pending:
                out["new_pending_comparison"] = "PASS"
        except BaseException as error:
            secondary.append({"phase": "pending", **helpers.safe_error(error)})
        try:
            if history is not None:
                with TaskDB(task, readonly=True) as db:
                    check_history(db, history)
                out["history_attempt_rows_unchanged"] = True
        except BaseException as error:
            secondary.append({"phase": "history", **helpers.safe_error(error)})
        if (secondary or out["new_pending_comparison"] != "PASS" or out["after"] is None
                or out["after"]["inflight"] != 0):
            out["state"] = "NOT_COMPLETED"
        if secondary or out["after"] is None or out["task_after"] is None:
            out["new_pending_comparison"] = "NOT_COMPLETED"
        out["secondary_errors"] = secondary
        try:
            with RESUME_METRICS.open("x") as output:
                json.dump(out, output, indent=2)
        except BaseException as error:
            # Exclusive creation never clobbers evidence; preserve the original failure.
            try:
                print(json.dumps({"status": "METRICS_FAILED", **helpers.safe_error(error)}),
                      flush=True)
            except BaseException:
                pass
            if primary is None:
                raise
        if primary is None and out["state"] != "PASS":
            raise RuntimeError("FINAL_CHECK_FAILED")
    print(json.dumps(out), flush=True)


def resume_child(task):
    try:
        result = helpers.run(task, 2, resume=True)
        exported = export_task(task, task / "final.jsonl", BudgetLedger())
        print(json.dumps({"status": "PASS", "run": result, "export": exported}), flush=True)
    except BaseException as error:
        print(json.dumps({"status": "FAILED", "error": helpers.safe_error(error)}), flush=True)
        raise


def main():
    parser = argparse.ArgumentParser()
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--resume", type=Path, help="child continuation, two workers")
    modes.add_argument("--continue-existing", type=Path, metavar="TASK",
                       help="authorized parent closed loop on the original frozen task")
    args = parser.parse_args()
    if args.resume:
        resume_child(args.resume)
        return
    if args.continue_existing:
        continue_existing(args.continue_existing)
        return
    ledger = BudgetLedger()
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
    assert resumed["run"]["result"]["state"] == "COMPLETED"
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
    try:
        main()
    except BaseException as error:
        print(json.dumps({"status": "FAILED", "error": helpers.safe_error(error)}), flush=True)
        sys.exit(1)
