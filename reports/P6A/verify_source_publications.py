"""Streaming source-package equivalence and actual installed-byte evidence."""

import argparse
import json
import sqlite3
from functools import lru_cache
from itertools import zip_longest
from pathlib import Path

from sakurapool.runtime import RuntimeQuerySpec
from sakurapool.storage.publication import load_publication


def record_rows(runtime, source):
    for batch in runtime.query(RuntimeQuerySpec(sources=(source,))).iter_record_batches(2048):
        yield from zip(
            batch.rid, batch.record_id, batch.source_name, batch.dataset_name, batch.post_id
        )


def compare(merged, isolated, source):
    count = 0
    object_a = lru_cache(maxsize=64)(merged.runtime.object_ref)
    object_b = lru_cache(maxsize=64)(isolated.runtime.object_ref)
    previous_a = previous_b = -1

    @lru_cache(maxsize=64)
    def remote_pair(left_idx, right_idx):
        sql = (
            "SELECT r.endpoint,r.repo_id,r.repo_type,o.dataset_id,o.object_id,"
            "o.revision_candidate,o.object_path,o.object_size,o.content_sha256,"
            "o.provider_sha256,o.fetchable FROM objects o JOIN repositories r "
            "USING(repo_idx) WHERE o.object_idx=?"
        )
        if (
            merged.catalog.execute(sql, (left_idx,)).fetchone()
            != isolated.catalog.execute(sql, (right_idx,)).fetchone()
        ):
            raise ValueError("source remote object identity mismatch")
        return True

    for left, right in zip_longest(
        record_rows(merged.runtime, source), record_rows(isolated.runtime, source)
    ):
        if left is None or right is None or left[1:] != right[1:]:
            raise ValueError("source record identity/ordering mismatch")
        # RID order is canonical within a dataset: object_id/sample_path/record_id.
        # Source filtering preserves that order; duplicate post IDs are retained.
        if left[0] <= previous_a or right[0] <= previous_b:
            raise ValueError("duplicate or nonmonotonic RID")
        previous_a, previous_b = left[0], right[0]
        if left[3] != source + "_v3":
            raise ValueError("unexpected source dataset")
        a, b = merged.runtime.location(left[0]), isolated.runtime.location(right[0])
        for key in ("image_offset", "image_size", "metadata_offset", "metadata_size", "flags"):
            if a[key] != b[key]:
                raise ValueError("source extent mismatch")
        oa, ob = object_a(a["object_idx"]), object_b(b["object_idx"])
        remote_pair(a["object_idx"], b["object_idx"])
        if oa != ob:
            raise ValueError("source object identity mismatch")
        if merged.runtime.image_format(a["format_id"]) != isolated.runtime.image_format(
            b["format_id"]
        ):
            raise ValueError("source image format mismatch")
        if merged.expected_image_sha(left[0]) != isolated.expected_image_sha(right[0]):
            raise ValueError("source image SHA mismatch")
        count += 1
    return count


def representative_queries(merged, isolated, source):
    rows = isolated.runtime._catalog.execute(
        "SELECT n.namespace,t.value FROM tags t JOIN namespaces n USING(namespace_id) "
        "ORDER BY t.cardinality DESC LIMIT 3"
    ).fetchall()
    counts = []
    for namespace, tag in rows:
        for kind in ("all_tags", "none_tags"):
            spec = RuntimeQuerySpec(sources=(source,), **{kind: ((namespace, tag),)})
            left = merged.runtime.query(spec)
            right = isolated.runtime.query(spec)
            a = (value for batch in left.iter_record_batches(2048) for value in batch.record_id)
            b = (value for batch in right.iter_record_batches(2048) for value in batch.record_id)
            if any(x != y for x, y in zip_longest(a, b)):
                raise ValueError("representative semantic query mismatch")
            counts.append({"namespace": namespace, "tag": tag, "kind": kind, "records": len(right)})
    return counts


def measure(publication):
    root = publication.root
    files = {
        str(path.relative_to(root)): path.stat().st_size
        for path in root.rglob("*")
        if path.is_file()
    }
    catalogs = {}
    for path in root.rglob("catalog.sqlite"):
        with sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True) as db:
            catalogs[str(path.relative_to(root))] = {
                "allocated_bytes": dict(
                    db.execute("SELECT name,sum(pgsize) FROM dbstat GROUP BY name")
                ),
                "rows": {
                    name: db.execute('SELECT count(*) FROM "' + name + '"').fetchone()[0]
                    for (name,) in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
                },
            }
    for catalog in catalogs.values():
        allocated = sum(catalog["allocated_bytes"].values())
        catalog["allocated_share"] = {
            name: size / allocated for name, size in catalog["allocated_bytes"].items()
        }
    return {"total_bytes": sum(files.values()), "files": files, "catalogs": catalogs}


def verify(merged_root, output):
    results = {}
    with load_publication(merged_root, full_verify=True) as merged:
        for source in ("danbooru", "konachan"):
            with load_publication(output / source / "publication", full_verify=True) as isolated:
                count = compare(merged, isolated, source)
                evidence = measure(isolated)
                evidence["representative_queries"] = representative_queries(
                    merged, isolated, source
                )
                if count != isolated.runtime.rid_count:
                    raise ValueError("source records do not cover isolated runtime")
                evidence.update(records=count, bytes_per_record=evidence["total_bytes"] / count)
                results[source] = evidence
                print(source, count, evidence["total_bytes"], "STREAM_EQUIVALENT", flush=True)
    (output / "verification.json").write_text(json.dumps(results, indent=2), encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--merged", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    verify(args.merged, args.output)
