"""Export confirmed deliveries under the frozen task capacity."""

import os
from pathlib import Path

from ..storage.budget import Reservation
from ..storage.retrieval import _real_output_root
from .context import validate_ledger
from .plan import canonical
from .runner import verify_delivery
from .store import TaskDB, TaskError


def export_task(directory, manifest, ledger):
    manifest = Path(manifest).absolute()
    with TaskDB(directory, readonly=True) as task:
        validate_ledger(task, ledger)
        _real_output_root(manifest.parent, ledger)
        if os.path.lexists(manifest):
            raise TaskError("EXPORT_CONFLICT", "export")
        _real_output_root(task.directory, ledger)
        if manifest.parent != task.directory:
            raise TaskError("EXPORT_BASE_MISMATCH", "export")
        task.validate_plan()
        count = task.db.execute(
            "SELECT count(*) FROM items WHERE state='DONE' AND accounting='CONFIRMED'"
        ).fetchone()[0]
        cap = min(task.capacity.export_manifest_bytes, count * 8192 + 4096)
        lease = ledger.reserve(Reservation(disk=cap))
        written = 0
        with manifest.open("xb") as stream:
            for item in task.db.execute(
                "SELECT * FROM items WHERE state='DONE' AND accounting='CONFIRMED' ORDER BY seq"
            ):
                receipt = verify_delivery(task, item)
                row = {
                    "format": "sakurapool-task-export-v1",
                    "task_id": task.meta("task_id"),
                    "plan_digest": task.meta("plan_digest"),
                    "publication_digest": task.meta("header")["publication_digest"],
                    "snapshot_id": task.meta("header")["snapshot_id"],
                    "seq": item["seq"],
                    "record_id": item["record_id"],
                    "rid": item["rid"],
                    "source": item["source"],
                    "dataset": item["dataset"],
                    "post_id": item["post_id"],
                    "files": [
                        {
                            "path": f"output/{item['record_id']}/{name}",
                            "bytes": proof["bytes"],
                            "sha256": proof["sha256"],
                        }
                        for name, proof in sorted(receipt["receipt"].items())
                    ],
                }
                payload = canonical(row) + b"\n"
                written += len(payload)
                if written > cap:
                    raise TaskError("RESOURCE_BLOCKED", "export")
                stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        ledger.settle(lease)
        return {"exported": count, "bytes": written}
