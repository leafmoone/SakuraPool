"""Deterministic locality-friendly records preparation, no provider I/O."""

import argparse
import hashlib
import itertools
import json
import time
from pathlib import Path

import numpy as np

from sakurapool.storage.publication import load_publication

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--publication", required=True, type=Path)
parser.add_argument("--output", required=True, type=Path)
parser.add_argument("--seed", default="linux-danbooru-1000-v1")
args = parser.parse_args()
A = args.output
A.mkdir(parents=True, exist_ok=False)
SEED = args.seed


def rank(kind, text):
    return hashlib.sha256((SEED + "\0" + kind + "\0" + text).encode()).digest()


start = time.monotonic()
seen = set()
lanes = []
objects = []
with load_publication(args.publication, full_verify=True) as p:
    if {row[0] for row in p.runtime._catalog.execute("select name from sources")} != {
        "danbooru"
    }:
        raise ValueError("Danbooru-only publication required")
    rows = p.catalog.execute(
        "select object_idx,object_path from objects where fetchable=1"
    ).fetchall()
    rows.sort(key=lambda row: rank("tar", row[1]))
    formats = {
        i
        for i, name in p.runtime._catalog.execute("select format_id,format from formats")
        if name in ("jpg", "jpeg", "png", "webp", "avif")
    }
    for obj, path in rows:
        need = 167 if len(lanes) < 4 else 166
        candidates = []
        for nrid in np.flatnonzero(p.runtime._locations["object_idx"] == obj):
            rid = int(nrid)
            loc = p.runtime.location(rid)
            if loc["format_id"] not in formats:
                continue
            sha = p.expected_image_sha(rid).hex()
            if sha in seen:
                continue
            record = (
                p.runtime._catalog.execute("select hex(record_id) from records where rid=?", (rid,))
                .fetchone()[0]
                .lower()
            )
            candidates.append(
                (
                    rank("record", record),
                    {
                        "rid": rid,
                        "record_id": record,
                        "sha256": sha,
                        "bytes": loc["image_size"],
                        "object_idx": obj,
                        "object_path": path,
                    },
                )
            )
        candidates.sort(key=lambda pair: pair[0])
        chosen = []
        local = set()
        for _, item in candidates:
            if item["sha256"] in local:
                continue
            local.add(item["sha256"])
            chosen.append(item)
            if len(chosen) == need:
                break
        if len(chosen) != need:
            continue
        seen.update(local)
        lanes.append(chosen)
        objects.append({"object_idx": obj, "object_path": path, "records": need})
        if len(lanes) == 6:
            break
    if len(lanes) != 6 or len(seen) != 1000:
        raise ValueError("Six eligible TARs and 1000 unique image hashes required")
    selected = [
        item for group in itertools.zip_longest(*lanes) for item in group if item is not None
    ]
    if len(selected) != 1000 or len({x["record_id"] for x in selected}) != 1000:
        raise ValueError("Selection must contain exactly 1000 unique records")
    (A / "danbooru-records.json").write_text(json.dumps([x["record_id"] for x in selected]))
    (A / "danbooru-query.json").write_text('{"sources":["danbooru"]}\n')
    (A / "selection.json").write_text(
        json.dumps(
            {
                "seed": SEED,
                "method": (
                    "sha256-rank eligible TARs; choose 6, rank unique records per TAR; "
                    "round-robin interleave [167,167,167,167,166,166]"
                ),
                "publication_id": p.manifest["publication_id"],
                "objects": objects,
                "records": selected,
            },
            indent=2,
        )
        + "\n"
    )
    print(
        json.dumps(
            {
                "records": len(selected),
                "unique_image_sha256": len(seen),
                "distinct_tars": len(objects),
                "expected_image_bytes": sum(x["bytes"] for x in selected),
                "objects": objects,
                "seconds": time.monotonic() - start,
            }
        ),
        flush=True,
    )
