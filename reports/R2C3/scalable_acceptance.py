"""Bounded P2 scale generator: one object's 44 records at a time; no network."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "tests"))


def generate(work: Path):
    import pyarrow.parquet as pq
    from synthetic_p2 import ObjectSpec, _input_digest, _object_id, _object_row, _record_id, _table

    from sakurapool.indexer import BUILDER, FORMAT_VERSION, SCHEMAS, _json
    from sakurapool.registry import DatasetAdapter

    root = work / "p2"
    root.mkdir(parents=True, exist_ok=False)
    dataset, source = "scalable_v2", "synthetic"
    inputs = {}
    for number in range(22792):
        rel = f"gc5m/object-{number:05d}.tar"
        inputs[rel] = dict(
            size=131072,
            mtime_ns=0,
            sha256=_input_digest(rel, dataset),
            strength="strong:sha256",
            version=1,
        )
    contract = dict(
        format_version=FORMAT_VERSION,
        builder=BUILDER,
        adapter=DatasetAdapter(dataset=dataset, source=source).to_dict(),
        hash_images=True,
        inputs=inputs,
    )
    (root / "INPUT.json").write_bytes(_json(contract))
    contract_sha = hashlib.sha256(_json(contract)).hexdigest()
    with (work / "remote-map.jsonl").open("x", encoding="utf-8") as remote:
        for number in range(22792):
            rel = f"gc5m/object-{number:05d}.tar"
            digest = inputs[rel]["sha256"]
            oid = _object_id(rel, digest)
            object_rows = {
                k: [v]
                for k, v in _object_row(
                    dataset, source, ObjectSpec(rel, size=131072), digest
                ).items()
            }
            samples = []
            annotations = []
            for n in range(44):
                path = f"image-{n:03d}.png"
                record = _record_id(dataset, oid, path)
                sha = hashlib.sha256(f"synthetic-image-{number}-{n}".encode()).hexdigest()
                samples.append(
                    dict(
                        record_id=record,
                        dataset_id=dataset,
                        object_id=oid,
                        sample_path=path,
                        source=source,
                        post_id=f"{number}-{n}",
                        image_path=path,
                        offset_data=512 + n * 1024,
                        size=32,
                        json_path=None,
                        json_offset_data=None,
                        json_size=None,
                        text=None,
                        tags_state="empty",
                        tags=[],
                        hash_source="computed:sha256",
                        sha256=sha,
                        image_format="png",
                        width=1,
                        height=1,
                        has_alpha=False,
                        hash_kind="sha256",
                        status="ok",
                    )
                )
                annotations.append(
                    dict(
                        record_id=record,
                        dataset_id=dataset,
                        object_id=oid,
                        sample_path=path,
                        namespace="synthetic",
                        origin="synthetic",
                        tags_state="empty",
                        tags=[],
                    )
                )
            shard = hashlib.sha256(_json([dataset, rel])).hexdigest()
            files = {}
            for name, rows in [
                ("objects", object_rows),
                ("samples", samples),
                ("annotations", annotations),
                ("errors", []),
            ]:
                if isinstance(rows, list):
                    rows = {f.name: [r[f.name] for r in rows] for f in SCHEMAS[name]}
                table = _table(name, rows)
                p = root / f"{shard}.{name}.parquet"
                pq.write_table(table, p)
                h = hashlib.sha256()
                with p.open("rb") as stream:
                    for block in iter(lambda: stream.read(1 << 20), b""):
                        h.update(block)
                files[name] = dict(
                    path=p.name, sha256=h.hexdigest(), bytes=p.stat().st_size, rows=table.num_rows
                )
            commit = dict(
                schema=FORMAT_VERSION,
                builder=BUILDER,
                dataset_id=dataset,
                object_id=oid,
                created_at="2026-10-02T00:00:00+00:00",
                input=inputs[rel],
                files=files,
                contract_sha256=contract_sha,
            )
            (root / f"{shard}.COMMIT").write_bytes(_json(commit))
            remote.write(
                json.dumps(
                    dict(
                        dataset_id=dataset,
                        endpoint="https://modelscope.cn",
                        repo_id="synthetic/scalable",
                        repo_type="modelscope_dataset_legacy",
                        revision_candidate="b" * 40,
                        object_path=rel,
                        object_size=131072,
                        provider_sha256=digest,
                    )
                )
                + "\n"
            )
    (work / "p2-roots.json").write_text(
        json.dumps(dict(format="sakurapool-p2-root-list-v1", roots=[str(root.resolve())])),
        encoding="utf-8",
    )
    return dict(objects=22792, records=1002848, max_materialized_sample_rows=44, real_images=False)


def peak_rss_bytes():
    import ctypes
    from ctypes import wintypes

    class Counters(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
        ]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    psapi.GetProcessMemoryInfo.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(Counters),
        wintypes.DWORD,
    ]
    value = Counters()
    value.cb = ctypes.sizeof(value)
    if not psapi.GetProcessMemoryInfo(kernel.GetCurrentProcess(), ctypes.byref(value), value.cb):
        raise ctypes.WinError(ctypes.get_last_error())
    return value.PeakWorkingSetSize


def publish_phase(work):
    from sakurapool.storage.publication import build_publication, load_publication

    start = time.perf_counter()
    result = build_publication(
        work / "runtime", work / "p2-roots.json", work / "remote-map.jsonl", work / "publication"
    )
    build_seconds = time.perf_counter() - start
    with load_publication(work / "publication", full_verify=True) as p:
        assert p.runtime.rid_count == 1002848
        assert p.manifest["object_count"] == 22792
        import random

        randoms = random.Random(20261002)
        for _ in range(100):
            rid = randoms.randrange(p.runtime.rid_count)
            location = p.runtime.location(rid)
            obj = p.catalog.execute(
                "SELECT object_idx, content_sha256 FROM objects WHERE object_idx=?",
                (location["object_idx"],),
            ).fetchone()
            assert obj is not None and len(obj[1]) == 32
            assert len(p.hashes[rid].tobytes()) == 32
    return dict(
        objects=result["object_count"],
        records=result["rid_count"],
        publication_manifest_bytes=(work / "publication/PUBLICATION.json").stat().st_size,
        remote_catalog_bytes=result["remote_objects_bytes"],
        image_hash_bytes=result["image_sha256_bytes"],
        build_seconds=build_seconds,
        peak_rss_bytes=peak_rss_bytes(),
        result="PASS",
        production_capacity_claim=False,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["generate", "compile", "publish"])
    parser.add_argument("--work", type=Path, required=True)
    args = parser.parse_args()
    start = time.perf_counter()
    if args.phase == "generate":
        args.work.mkdir(parents=True, exist_ok=True)
        result = generate(args.work)
    elif args.phase == "publish":
        result = publish_phase(args.work)
    else:
        from sakurapool.runtime.compiler import compile_runtime
        from sakurapool.runtime.inventory import load_p2_inventory

        result = dict(
            result=str(compile_runtime(load_p2_inventory(args.work / "p2"), args.work / "runtime"))
        )
    print(json.dumps(dict(phase=args.phase, wall_seconds=time.perf_counter() - start, **result)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
