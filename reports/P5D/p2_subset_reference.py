"""Bounded independent P2 annotation reference: five objects, <=320 rows."""
import json
from pathlib import Path

import pyarrow.parquet as pq

from sakurapool.indexer import ANNOTATIONS_SCHEMA
from sakurapool.runtime import RuntimeQuerySpec
from sakurapool.storage.publication import load_publication

ROOT = Path("D:/SakuraTool/SakuraPool-P5D-20261004T143905Z")


def main():
    samples = []
    selected = set()
    for raw in json.loads((ROOT / "p2-roots.json").read_bytes()):
        root = Path(raw)
        contract = json.loads((root / "INPUT.json").read_bytes())
        dataset = contract["adapter"]["dataset"]
        if dataset in selected:
            continue
        fragment = next(iter(sorted(root.glob("*.annotations.parquet"))))
        with pq.ParquetFile(fragment) as parquet:
            assert parquet.schema_arrow == ANNOTATIONS_SCHEMA
            if not parquet.num_row_groups:
                continue
            batch = next(parquet.iter_batches(batch_size=64))
            for row in batch.to_pylist():
                row["source"] = contract["adapter"]["source"]
                samples.append(row)
        selected.add(dataset)
    assert len(selected) == 5 and len(samples) <= 320
    with load_publication(ROOT / "publication") as publication:
        rt = publication.runtime
        tags = []
        for order in ("DESC", "ASC"):
            tags.append(rt._catalog.execute(
                "SELECT n.namespace,t.value FROM tags t JOIN namespaces n USING(namespace_id) "
                "WHERE cardinality>0 ORDER BY cardinality " + order + ",tag_id LIMIT 1"
            ).fetchone())
        a, b = tags
        specs = {"all": RuntimeQuerySpec(), "common": RuntimeQuerySpec(all_tags=(a,)),
                 "rare": RuntimeQuerySpec(all_tags=(b,)),
                 "intersection": RuntimeQuerySpec(all_tags=(a, b)),
                 "any_tags": RuntimeQuerySpec(any_tags=(a, b)),
                 "any_of": RuntimeQuerySpec(any_of=(RuntimeQuerySpec(all_tags=(a,)),
                                                     RuntimeQuerySpec(all_tags=(b,)))),
                 "none": RuntimeQuerySpec(none_tags=(a,)),
                 "zero": RuntimeQuerySpec(all_tags=(a,), none_tags=(a,))}
        for source in sorted({r["source"] for r in samples}):
            specs["source:" + source] = RuntimeQuerySpec(sources=(source,))
        identities = []
        category_checks = 0
        for row in samples:
            found = rt._catalog.execute(
                "SELECT r.rid,s.name,d.name FROM records r JOIN sources s USING(source_id) "
                "JOIN datasets d USING(dataset_id) WHERE r.record_id=? LIMIT 2",
                (bytes.fromhex(row["record_id"]),),
            ).fetchmany(2)
            assert len(found) == 1
            rid, source, dataset = found[0]
            assert source == row["source"] and dataset == row["dataset_id"]
            identities.append(rid)
            for tag in row["tags"] or []:
                if tag["category"] is not None:
                    assert rt._catalog.execute(
                        "SELECT 1 FROM tag_categories c JOIN tags t USING(tag_id) "
                        "JOIN namespaces n USING(namespace_id) "
                        "WHERE n.namespace=? AND t.value=? AND c.category=? LIMIT 1",
                        (row["namespace"], tag["value"], tag["category"]),
                    ).fetchone()
                    category_checks += 1
        evidence = {}
        for name, spec in specs.items():
            result = rt.query(spec)
            hits = 0
            for row, rid in zip(samples, identities):
                known = row["tags_state"] in ("known", "empty")
                values = {t["value"] for t in row["tags"] or []} if known else set()
                has_a = row["namespace"] == a[0] and a[1] in values
                has_b = row["namespace"] == b[0] and b[1] in values
                expected = {"all": True, "common": has_a, "rare": has_b,
                            "intersection": has_a and has_b,
                            "any_tags": has_a or has_b, "any_of": has_a or has_b,
                            "none": row["namespace"] == a[0] and known and not has_a,
                            "zero": False}.get(name, row["source"] in spec.sources)
                assert (rid in result._bitmap) == expected, (name, rid)
                hits += expected
            evidence[name] = {"sample_hits": hits, "checked": len(samples)}
        out = {"status": "PASS_SUBSET_ONLY", "samples": len(samples),
               "datasets": sorted(selected), "catalog_tags_limit1": tags,
               "category_checks": category_checks, "queries": evidence,
               "content_digest": publication.content_digest,
               "limitation": "one annotation fragment/object per dataset; "
                             "not full corpus semantics"}
        print(json.dumps(out), flush=True)
        with (ROOT / "p2-subset-reference.json").open("x") as output:
            json.dump(out, output, indent=2)


if __name__ == "__main__":
    main()
