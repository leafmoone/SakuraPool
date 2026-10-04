"""Read-only public API P5-D measurement. --smoke before --publication PATH.
No network; formal publication full verification includes runtime verification.
"""
import argparse
import json
import random
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
from contextlib import closing
from pathlib import Path

from sakurapool.runtime import RuntimeQuerySpec
from sakurapool.runtime.errors import UnknownQueryValueError
from sakurapool.storage.publication import load_publication


def timed(call):
    start = time.perf_counter()
    value = call()
    return value, time.perf_counter() - start


def measure(path, label):
    child = subprocess.run([sys.executable, __file__, "--fast-child", str(path)],
                           check=True, capture_output=True, text=True)
    pub, full = timed(lambda: load_publication(path, full_verify=True))
    with pub:
        rt = pub.runtime
        tags = rt._catalog.execute("SELECT n.namespace,t.value,t.cardinality FROM tags t "
                                   "JOIN namespaces n USING(namespace_id) "
                                   "ORDER BY cardinality DESC,tag_id LIMIT 1").fetchall()
        tags += rt._catalog.execute("SELECT n.namespace,t.value,t.cardinality FROM tags t "
                                    "JOIN namespaces n USING(namespace_id) "
                                    "WHERE cardinality > 0 "
                                    "ORDER BY cardinality ASC,tag_id LIMIT 1").fetchall()
        sources = [r[0] for r in rt._catalog.execute("SELECT name FROM sources ORDER BY name")]
        specs = {"all": RuntimeQuerySpec()}
        if tags:
            a, b = tags[0][:2], tags[-1][:2]
            specs.update(common=RuntimeQuerySpec(all_tags=(a,)),
                         rare=RuntimeQuerySpec(all_tags=(b,)),
                         intersection=RuntimeQuerySpec(all_tags=(a, b)),
                         any_of=RuntimeQuerySpec(any_of=(RuntimeQuerySpec(all_tags=(a,)),
                                                       RuntimeQuerySpec(all_tags=(b,)))),
                         none=RuntimeQuerySpec(none_tags=(a,)),
                         zero=RuntimeQuerySpec(all_tags=(a,), none_tags=(a,)))
        if len(sources) > 1:
            specs["multi_source"] = RuntimeQuerySpec(sources=tuple(sources[:2]))
        queries = {}
        for name, spec in specs.items():
            count, first = timed(lambda: rt.query(spec).count())
            samples = [timed(lambda: rt.query(spec).count())[1] for _ in range(5)]
            queries[name] = {"count": count, "first_seconds": first,
                             "warm_n": 5, "warm_median_seconds": statistics.median(samples),
                             "warm_p80_seconds": sorted(samples)[3]}
        unknown = "NO_TAG_NAMESPACE"
        if tags:
            assert queries["intersection"]["count"] <= min(
                queries["common"]["count"], queries["rare"]["count"])
            assert queries["any_of"]["count"] == (queries["common"]["count"]
                + queries["rare"]["count"] - queries["intersection"]["count"])
            namespace_id = rt._catalog.execute(
                "SELECT namespace_id FROM namespaces WHERE namespace=?", (a[0],)
            ).fetchone()[0]
            known = rt._bitmaps.execute(
                "SELECT cardinality FROM bitmaps WHERE kind='namespace' AND id=?",
                (namespace_id,),
            ).fetchone()[0]
            assert queries["none"]["count"] + queries["common"]["count"] == known
            assert queries["zero"]["count"] == 0
            try:
                rt.query(RuntimeQuerySpec(all_tags=((tags[0][0], "__p5d_unknown__"),)))
                unknown = "ACCEPTED"
            except UnknownQueryValueError:
                unknown = "UnknownQueryValueError"
        result = rt.query(RuntimeQuerySpec())
        records, rs = timed(lambda: next(result.iter_record_batches(batch_size=32), None))
        locations, ls = timed(lambda: next(result.iter_location_batches(batch_size=32), None))
        rids = random.Random(5).sample(range(rt.rid_count), min(16, rt.rid_count))
        _, random_seconds = timed(lambda: [rt.location(rid) for rid in rids])
        files = []
        for file in sorted(Path(path).rglob("*")):
            if file.is_symlink():
                raise ValueError("symlink in capacity input")
            if file.is_file():
                files.append({"component": str(file.relative_to(path)),
                              "bytes": file.stat().st_size})
        total = sum(f["bytes"] for f in files)
        for f in files:
            f.update(percent=100 * f["bytes"] / total, bytes_per_record=f["bytes"] / rt.rid_count)
        dbstat = {}
        for f in files:
            if not f["component"].endswith(".sqlite"):
                continue
            with closing(sqlite3.connect((Path(path) / f["component"]).as_uri()
                                         + "?mode=ro&immutable=1", uri=True)) as db:
                try:
                    rows = db.execute("SELECT name,pgsize,payload,unused FROM dbstat "
                                      "WHERE aggregate=TRUE ORDER BY pgsize DESC").fetchall()
                    dbstat[f["component"]] = {"btree": rows,
                        "file_minus_btree_bytes": f["bytes"] - sum(r[1] for r in rows),
                        "freelist_pages": db.execute("PRAGMA freelist_count").fetchone()[0]}
                except sqlite3.OperationalError:
                    dbstat[f["component"]] = "UNAVAILABLE"
        return {"label": label, "publication": str(path), "content_digest": pub.content_digest,
                "snapshot": rt.snapshot_id, "records": rt.rid_count, "sources": sources,
                "objects": pub.manifest["object_count"],
                "fetchable_objects": pub.manifest["fetchable_object_count"],
                "fetchable_rids": pub.manifest["fetchable_rid_count"],
                "capacity": {"logical_bytes": total, "bytes_per_record": total / rt.rid_count,
                             "components": files, "allocated_bytes": "NOT_MEASURED",
                             "p3_duplicate_added": False, "p2": "NOT_MEASURED_BY_THIS_HARNESS",
                             "dbstat": dbstat},
                "fast_open": json.loads(child.stdout), "full_verify_seconds": full,
                "query_evidence_tags": tags[:1] + tags[-1:], "queries": queries,
                "unknown_tag": unknown, "multi_source": len(sources) > 1,
                "record_batch": {"count": len(records.rid) if records else 0, "seconds": rs},
                "location_batch": {"count": len(locations.rid) if locations else 0, "seconds": ls},
                "random_locations": {"count": len(rids), "seconds": random_seconds},
                "correctness": "COUNT_RELATIONS; INDEPENDENT_P2_REFERENCE_SEPARATE",
                "rss": "NOT_MEASURED", "cache": "OS cache uncontrolled; same-handle warm query"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--publication", type=Path)
    parser.add_argument("--label", default="canary")
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--fast-child", type=Path)
    args = parser.parse_args()
    if args.fast_child:
        pub, seconds = timed(lambda: load_publication(args.fast_child))
        with pub:
            print(json.dumps({"seconds": seconds, "records": pub.runtime.rid_count,
                              "digest": pub.content_digest}))
    elif args.smoke:
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tests"))
        from test_publication import inputs

        from sakurapool.storage.publication import build_publication
        with tempfile.TemporaryDirectory(prefix="p5d-api-smoke-") as directory:
            rt, roots, mapping, out, _ = inputs.__wrapped__(Path(directory))
            build_publication(rt, roots, mapping, out)
            result = measure(out, "synthetic_smoke")
            assert result["records"] == result["record_batch"]["count"] == 1
            assert result["capacity"]["logical_bytes"] > 0
            print(json.dumps(result))
    elif args.publication:
        print(json.dumps(measure(args.publication, args.label)))
    else:
        parser.error("--publication required")


if __name__ == "__main__":
    main()
