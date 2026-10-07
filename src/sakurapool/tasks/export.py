"""Verified v3 delivery export; legacy export is rejected before any output write."""

import os
from pathlib import Path

from ..fs_safety import plain_entry
from ..storage.publication import load_publication
from .plan import canonical
from .runner import check_publication_identity, verify_delivery
from .store import TaskDB, TaskError


def export_task(directory, manifest):
    manifest = Path(manifest).absolute()
    with TaskDB(directory, readonly=True) as task:
        if task.version not in (3, 4):
            raise TaskError("LEGACY_TASK_MIGRATION_REQUIRED", "export")
        return _export_lightweight(task, manifest)


def _export_lightweight(task, manifest):
    with load_publication(task.meta("publication_path"), full_verify=True) as publication:
        check_publication_identity(task, publication)
        for directory in (manifest.parent, *manifest.parent.parents):
            plain_entry(directory, directory=True)
        if os.path.lexists(manifest):
            raise TaskError("EXPORT_CONFLICT", "export")
        if manifest.parent != task.directory:
            raise TaskError("EXPORT_BASE_MISMATCH", "export")
        task.validate_plan()
        where = "state='DONE' AND delivery='VERIFIED'"
        count = task.db.execute("SELECT count(*) FROM items WHERE " + where).fetchone()[0]
        cap = min(task.capacity.export_manifest_bytes, count * 8192 + 4096)
        written = 0
        with manifest.open("xb") as stream:
            for item in task.db.execute("SELECT * FROM items WHERE " + where + " ORDER BY seq"):
                receipt = verify_delivery(task, item, publication)
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
                            "path": (f"output/{name}" if task.version == 4
                                     else f"output/{item['record_id']}/{name}"),
                            "bytes": proof["bytes"],
                            "sha256": proof["sha256"],
                        }
                        for name, proof in sorted(receipt["receipt"].items())
                    ],
                }
                payload = canonical(row) + b"\n"
                written += len(payload)
                if written > cap:
                    raise TaskError("EXPORT_LIMIT", "export")
                stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        return {"exported": count, "bytes": written}
