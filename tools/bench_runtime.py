"""P3 runtime benchmark: 100k correctness, 1M performance, 5M scale.

Synthetic corpus (deterministic seed per scale): hot/medium/rare tag
distribution, correlated and mutually exclusive pairs, mixed
tags_state, shared namespaces across sources. Each phase runs in a
child process for clean peak-RSS attribution:

    gen      build synthetic committed P2 directories
    compile  load + compile + publish; track wall, RSS, temp disk peak
    query    cold/warm query families, large results, cache eviction
    diff     reference-vs-bitmap differential (100k by default)

Gates (5M): warm typical count p95 <= 200ms, first-128 p95 <= 500ms,
10k locations <= 1s. A failed gate exits 2 with PERFORMANCE_GATE_FAIL.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import platform
import random
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "tests"))

from synthetic_p2 import ObjectSpec, SampleSpec, build_p2_directory  # noqa: E402

SCALE_PARAMS = {
    "100k": dict(objects_per_dir=2, samples_per_object=25000),
    "1M": dict(objects_per_dir=4, samples_per_object=125000),
    "5M": dict(objects_per_dir=4, samples_per_object=625000),
}
DIRS = (("src_a", "ds_a"), ("src_b", "ds_b"))
HOT = 500
MEDIUM = 200
RARE = 50
CORRELATED = [(f"hot{i:03d}", f"hot{i + 1:03d}") for i in range(0, 20, 2)]
EXCLUSIVE = ("solo", "group")
SEED_BASE = 20250101


def peak_rss_bytes() -> int:
    if os.name == "nt":
        from ctypes import wintypes

        class Counters(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD),
                        ("PageFaultCount", wintypes.DWORD)] + [
                (name, ctypes.c_size_t) for name in (
                    "PeakWorkingSetSize", "WorkingSetSize",
                    "QuotaPeakPagedPoolUsage", "QuotaPagedPoolUsage",
                    "QuotaPeakNonPagedPoolUsage", "QuotaNonPagedPoolUsage",
                    "PagefileUsage", "PeakPagefileUsage")]

        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.GetCurrentProcess.restype = wintypes.HANDLE
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
        if not psapi.GetProcessMemoryInfo(
                kernel.GetCurrentProcess(), ctypes.byref(counters),
                counters.cb):
            raise ctypes.WinError(ctypes.get_last_error())
        return counters.PeakWorkingSetSize
    import resource

    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


def vocab(scale: str) -> dict:
    return {
        "hot": [f"hot{i:03d}" for i in range(HOT)],
        "medium": [f"medium{i:03d}" for i in range(MEDIUM)],
        "rare": [f"rare{i:03d}" for i in range(RARE)],
        "correlated": CORRELATED,
        "exclusive": list(EXCLUSIVE),
        "seed": SEED_BASE + len(scale),
    }


def sample_tags(rng, hot, medium, rare) -> tuple[list[str], str]:
    tags: list[str] = []
    roll = rng.random()
    if roll < 0.70:
        tags.append(hot[rng.randrange(len(hot))])
    elif roll < 0.80:
        tags.append(medium[rng.randrange(len(medium))])
    elif roll < 0.801:
        tags.append(rare[rng.randrange(len(rare))])
    if rng.random() < 0.20:
        if tags and rng.random() < 0.5:
            partner = {a: b for a, b in CORRELATED}
            partner.update({b: a for a, b in CORRELATED})
            extra = partner.get(tags[0])
        else:
            extra = hot[rng.randrange(len(hot))]
        if extra and extra not in tags:
            tags.append(extra)
    pair = rng.random()
    if pair < 0.05:
        tags.append(EXCLUSIVE[0])
    elif pair < 0.10:
        tags.append(EXCLUSIVE[1])
    if rng.random() < 0.03:
        return [], "missing"
    if rng.random() < 0.02:
        return [], "invalid"
    if not tags and rng.random() < 0.5:
        return [], "empty"
    return sorted(set(tags)), "known"


def phase_gen(workdir: Path, scale: str) -> dict:
    params = SCALE_PARAMS[scale]
    vb = vocab(scale)
    rng = random.Random(vb["seed"])
    root = workdir / "p2"
    manifest = dict(scale=scale, **vb, dirs={},
                    expected_samples=params["objects_per_dir"] * 2
                    * params["samples_per_object"])
    for source, dataset in DIRS:
        objects = []
        for obj_no in range(params["objects_per_dir"]):
            namespace = "gelbooru" if (dataset == "ds_a"
                                       and obj_no
                                       == params["objects_per_dir"] - 1) \
                else "danbooru"
            samples = []
            for i in range(params["samples_per_object"]):
                values, state = sample_tags(
                    rng, vb["hot"], vb["medium"], vb["rare"])
                samples.append(SampleSpec(
                    path=f"{i}.jpg",
                    post_id=f"{source}-{obj_no}-{i}",
                    tags=[(value, "general") for value in values],
                    tags_state=state))
            objects.append(ObjectSpec(
                f"{dataset}-{obj_no}.tar", samples,
                namespace=namespace,
                origin=f"origin-{source}-{obj_no}"))
        d = root / dataset
        counts = build_p2_directory(
            d, dataset=dataset, source=source, objects=objects,
            created_at="2025-01-01T00:00:00+00:00")
        manifest["dirs"][dataset] = {"source": source, **counts}
    (workdir / "manifest.json").write_text(json.dumps(manifest,
                                                       sort_keys=True))
    return manifest


def _dir_bytes(path: Path) -> int:
    total = 0
    for entry in path.rglob("*"):
        if entry.is_file():
            total += entry.stat().st_size
    return total


def phase_compile(workdir: Path, scale: str) -> dict:
    from sakurapool.runtime.compiler import compile_runtime
    from sakurapool.runtime.inventory import combine_inventories, load_p2_inventory

    inventories = [load_p2_inventory(workdir / "p2" / dataset)
                   for _, dataset in DIRS]
    root = workdir / "rt"
    staging_peak = {"peak": 0}

    import threading

    def poll() -> None:
        while True:
            for entry in root.glob(".staging-*"):
                staging_peak["peak"] = max(staging_peak["peak"],
                                           _dir_bytes(entry))
            time.sleep(0.2)
            if not list(root.glob(".staging-*")):
                return

    root.mkdir(parents=True, exist_ok=True)
    watcher = threading.Thread(target=poll, daemon=True)
    watcher.start()
    started = time.perf_counter()
    summary = compile_runtime(combine_inventories(inventories), root)
    wall = time.perf_counter() - started
    watcher.join(timeout=5)
    sizes = {}
    snap_dir = summary.path
    if snap_dir is None:
        snap_dir = root / "snapshots" / summary.snapshot_id
    for entry in snap_dir.iterdir():
        sizes[entry.name] = entry.stat().st_size
    return dict(scale=scale, wall_s=round(wall, 3),
                rid_count=summary.rid_count,
                snapshot_id=summary.snapshot_id,
                peak_rss_bytes=peak_rss_bytes(),
                peak_temp_bytes=staging_peak["peak"],
                final_bytes=sizes)


def _source_fingerprint() -> str:
    """Bind every cached result to the EXACT code and environment that
    produced it: the whole source tree (including the P2 contract modules
    the runtime imports), the bench tool, the corpus generator, the Python
    interpreter and the pinned dependency versions. A stale JSON from a
    previous tree or a different environment must never masquerade as a
    final-tree measurement - any change invalidates every cached phase.
    """
    import numpy
    import pyarrow
    import pyroaring

    digest = hashlib.sha256()
    for path in sorted((REPO / "src").rglob("*.py")):
        rel = path.relative_to(REPO).as_posix()
        digest.update(rel.encode())
        digest.update(path.read_bytes())
    for rel in ("tools/bench_runtime.py", "tests/synthetic_p2.py"):
        path = REPO / rel
        if path.exists():
            digest.update(rel.encode())
            digest.update(path.read_bytes())
    digest.update(json.dumps({
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "numpy": numpy.__version__,
        "pyarrow": pyarrow.__version__,
        "pyroaring": pyroaring.__version__,
    }, sort_keys=True).encode())
    return digest.hexdigest()


def _run_fingerprint(args) -> str:
    payload = dict(source=_source_fingerprint(),
                   scale=list(SCALE_PARAMS),
                   cache_bytes=args.cache_bytes,
                   n_specs=args.n_specs,
                   seed_base=SEED_BASE, hot=HOT, medium=MEDIUM, rare=RARE)
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()
                          ).hexdigest()


_FAMILY_ORDER = ("source_only", "hot", "rare", "hot_and_medium",
                 "three_tag_and", "three_tag_or", "not", "source_2tag",
                 "two_source_2tag", "any_of_2")


def _family_specs(manifest: dict) -> dict:
    hot, medium, rare = manifest["hot"], manifest["medium"], manifest["rare"]
    return {
        "source_only": dict(sources=("src_a",)),
        "hot": dict(namespace="danbooru", all_tags=(hot[0],)),
        "rare": dict(namespace="danbooru", all_tags=(rare[0],)),
        "hot_and_medium": dict(namespace="danbooru",
                               all_tags=(hot[1], medium[0])),
        "three_tag_and": dict(namespace="danbooru",
                              all_tags=(hot[2], hot[3], hot[4])),
        "three_tag_or": dict(namespace="danbooru",
                             any_tags=(hot[5], hot[6], hot[7])),
        "not": dict(namespace="danbooru", all_tags=(hot[8],),
                    none_tags=(hot[9],)),
        "source_2tag": dict(sources=("src_b",), namespace="danbooru",
                            all_tags=(hot[10], hot[11])),
        "two_source_2tag": dict(sources=("src_a", "src_b"),
                                namespace="danbooru",
                                all_tags=(hot[12], medium[1])),
        "any_of_2": dict(any_of=(
            dict(namespace="danbooru", all_tags=(hot[13],)),
            dict(sources=("src_a",), namespace="danbooru",
                 all_tags=(medium[2],)),
        )),
    }


def _spec_from_parts(parts: dict):
    from sakurapool.runtime.query import RuntimeQuerySpec

    if "any_of" in parts:
        return RuntimeQuerySpec(
            any_of=tuple(_spec_from_parts(b) for b in parts["any_of"]))
    return RuntimeQuerySpec(**parts)


def _timings(values: list[float]) -> dict:
    values = sorted(values)
    return {
        "n": len(values),
        "p50_ms": round(values[len(values) // 2] * 1000, 3),
        "p95_ms": round(values[min(len(values) - 1,
                                   int(len(values) * 0.95))] * 1000, 3),
        "max_ms": round(values[-1] * 1000, 3),
    }


def _drain_10k(r) -> float:
    """Time draining up to exactly 10_000 rows (never more): the batch
    size is capped so 8192+8192=16384 can no longer accumulate."""
    t0 = time.perf_counter()
    got = 0
    for batch in r.iter_location_batches(8192):
        if got >= 10000:
            break
        take = min(len(batch.rid), 10000 - got)
        got += take
        if take < len(batch.rid):
            break
    return time.perf_counter() - t0


def phase_query(workdir: Path, scale: str, run_id: str,
                cache_bytes: int | None = None) -> dict:
    from sakurapool.runtime.snapshot import RuntimeSnapshot

    manifest = json.loads((workdir / "manifest.json").read_text())
    root = workdir / "rt"
    kwargs = dict(cache_bytes=cache_bytes) if cache_bytes else {}
    # COLD pass: one independent snapshot (and cache) per family, in a
    # fresh process - no family may inherit another family's warm cache.
    cold = {}
    for name in _FAMILY_ORDER:
        rt = RuntimeSnapshot.open(root, **kwargs)
        parts = _family_specs(manifest)[name]
        spec = _spec_from_parts(parts)
        t0 = time.perf_counter()
        r = rt.query(spec)
        plan_s = time.perf_counter() - t0
        count = r.count()
        t128 = t10k = None
        if count:
            t0 = time.perf_counter()
            next(r.iter_location_batches(128))
            t128 = time.perf_counter() - t0
            t10k = _drain_10k(r)
        cold[name] = dict(
            cold_plan_ms=round(plan_s * 1000, 3),
            cold_first128_ms=(None if t128 is None
                              else round(t128 * 1000, 3)),
            cold_10k_ms=(None if t10k is None else round(t10k * 1000, 3)),
            count=count)
        rt.close()
    # WARM pass: one snapshot, shared cache, 5 repeats per family.
    opened = time.perf_counter()
    rt = RuntimeSnapshot.open(root, **kwargs)
    open_s = time.perf_counter() - opened
    result = dict(scale=scale, run_id=run_id, open_s=round(open_s, 3),
                  rid_count=rt.rid_count,
                  cache_bytes=rt.cache.byte_limit,
                  families={}, large=None, locations100k=None,
                  cache_stats=None)
    for name in _FAMILY_ORDER:
        spec = _spec_from_parts(_family_specs(manifest)[name])
        plans, extract128, e2e128, t10k = [], [], [], []
        empty = cold[name]["count"] == 0
        for _ in range(5):
            # span 1: plan only (query() up to the bitmap result)
            t0 = time.perf_counter()
            r = rt.query(spec)
            plans.append(time.perf_counter() - t0)
            if empty:
                continue
            # span 2: extraction only (first 128 locations from a ready
            # result) - reported, not gated
            t0 = time.perf_counter()
            next(r.iter_location_batches(128))
            extract128.append(time.perf_counter() - t0)
            # span 3 (gate scope): full query -> first 128 end to end
            t0 = time.perf_counter()
            r2 = rt.query(spec)
            next(r2.iter_location_batches(128))
            e2e128.append(time.perf_counter() - t0)
            t10k.append(_drain_10k(r2))
        result["families"][name] = dict(cold[name])
        result["families"][name].update(
            warm_plan=_timings(plans),
            warm_first128_extract=({"n": 0} if empty
                                   else _timings(extract128)),
            warm_first128_e2e=({"n": 0} if empty else _timings(e2e128)),
            warm_10k=({"n": 0} if empty else _timings(t10k)))
    # large result: half-corpus source term (>1M rids at 5M scale)
    big = _spec_from_parts({"sources": ("src_a",)})
    r = rt.query(big)
    t0 = time.perf_counter()
    count = r.count()
    first100 = r.limit(100)
    t_big = time.perf_counter() - t0
    result["large"] = dict(count=count, first100=len(first100),
                           first100_s=round(t_big, 3))
    # 100k-row location drain: the required locations-scale evidence.
    # Exactly 100_000 rows are consumed (partial last batch counted
    # against the cap), never an extra batch.
    t0 = time.perf_counter()
    got = 0
    for batch in r.iter_location_batches(8192):
        take = min(len(batch.rid), 100000 - got)
        got += take
        if take < len(batch.rid):
            break
    result["locations100k"] = dict(
        rows=got, s=round(time.perf_counter() - t0, 3))
    # cache stats / eviction behavior
    result["cache_stats"] = dict(
        hits=rt.cache.hits, misses=rt.cache.misses,
        evictions=rt.cache.evictions,
        resident_bytes=rt.cache.resident_bytes(),
        resident_le_limit=rt.cache.resident_bytes()
        <= rt.cache.byte_limit)
    rt.close()
    result["peak_rss_bytes"] = peak_rss_bytes()
    return result


def phase_diff(workdir: Path, scale: str, n_specs: int = 300) -> dict:
    """Reference-vs-bitmap over a catalog-derived rid map."""
    import pyarrow.parquet as pq

    from sakurapool.runtime.snapshot import RuntimeSnapshot

    manifest = json.loads((workdir / "manifest.json").read_text())
    root = workdir / "rt"
    with RuntimeSnapshot.open(root) as rt:
        rows = rt._catalog.execute(  # noqa: SLF001 - benchmark boundary
            "SELECT rid, post_id, dataset_id, source_id FROM records"
        ).fetchall()
        post_of_rid = {row[0]: row[1] for row in rows}
        ds_names = {row[0]: row[1] for row in rt._catalog.execute(
            "SELECT dataset_id, name FROM datasets")}
        src_names = {row[0]: row[1] for row in rt._catalog.execute(
            "SELECT source_id, name FROM sources")}
        ns_names = {row[0]: row[1] for row in rt._catalog.execute(
            "SELECT namespace_id, namespace FROM namespaces")}
        # post_id is unique per corpus; rebuild reference from parquet
        meta: dict[str, dict] = {}
        for rid, post_id, ds, src in rows:
            meta[post_id] = dict(dataset=ds_names[ds],
                                 source=src_names[src])
        for _, dataset in DIRS:
            for shard in (workdir / "p2" / dataset).glob(
                    "*.samples.parquet"):
                table = pq.read_table(shard, columns=[
                    "post_id", "tags_state", "tags"])
                post = table.column("post_id").to_pylist()
                states = table.column("tags_state").to_pylist()
                tags = table.column("tags").to_pylist()
                for p, state, structs in zip(post, states, tags,
                                             strict=True):
                    row = meta[p]
                    row["tags"] = {s["value"] for s in structs}
                    row["known"] = state in ("known", "empty")
        # rid -> namespace so the reference can scope none_tags exactly
        rid_ns: dict[int, str] = {}
        for ns_id, name in ns_names.items():
            for rid in rt._get_bitmap("namespace", ns_id):  # noqa: SLF001
                rid_ns[rid] = name
        for rid, post_id in post_of_rid.items():
            meta[post_id]["namespace"] = rid_ns.get(rid)
        # (namespace, value) -> posts: tag terms are namespace-scoped
        tag_ns: dict[tuple[str, str], set[str]] = {}
        for post_id, row in meta.items():
            for value in row["tags"]:
                key = (row["namespace"], value)
                tag_ns.setdefault(key, set()).add(post_id)
        # per-namespace tag pools straight from the catalog
        ns_pool: dict[str, set[str]] = {}
        for ns, value in rt._catalog.execute(
                "SELECT n.namespace, t.value FROM tags t JOIN namespaces n "
                "ON n.namespace_id = t.namespace_id"):
            ns_pool.setdefault(ns, set()).add(value)
        ns_pool = {ns: sorted(values) for ns, values in ns_pool.items()}
        ref_all = set(meta)

        def match(predicate) -> set:
            return {p for p, row in meta.items() if predicate(row)}

        def tag_set(ns: str, value: str) -> set:
            return tag_ns.get((ns, value), set())

        def ref_branch(parts: dict) -> set:
            result = set(ref_all)
            ns = parts.get("namespace")
            if parts.get("sources"):
                result &= set().union(
                    *(match(lambda row, s=s: row["source"] == s)
                      for s in parts["sources"]))
            if parts.get("datasets"):
                result &= set().union(
                    *(match(lambda row, d=d: row["dataset"] == d)
                      for d in parts["datasets"]))
            for tag in parts.get("all_tags", ()):
                result &= tag_set(ns, tag)
            if parts.get("any_tags"):
                result &= set().union(*(tag_set(ns, t)
                                        for t in parts["any_tags"]))
            if parts.get("none_tags"):
                known_ns = {p for p, row in meta.items()
                            if row["namespace"] == ns and row["known"]}
                excluded = set().union(*(tag_set(ns, t)
                                         for t in parts["none_tags"]))
                result &= known_ns - excluded
            return result

        def ref_spec(parts: dict) -> set:
            if "any_of" in parts:
                return set().union(*(ref_spec(b) for b in parts["any_of"]))
            return ref_branch(parts)

        ns_choices = [ns for ns in ns_pool if ns_pool[ns]]
        rng = random.Random(manifest["seed"] + 7)
        specs = []
        for i in range(n_specs):
            family = rng.randrange(6)
            ns = rng.choice(ns_choices)
            pool = ns_pool[ns]

            def pick(k):
                return tuple(rng.sample(pool, min(k, len(pool))))

            if family == 0:
                parts = dict(sources=(rng.choice(["src_a", "src_b"]),))
            elif family == 1:
                s = pick(rng.randint(1, 3))
                parts = dict(all_tags=s[:rng.randint(1, len(s))],
                             namespace=ns)
            elif family == 2:
                parts = dict(any_tags=pick(3), namespace=ns)
            elif family == 3:
                parts = dict(all_tags=pick(1), none_tags=pick(1),
                             namespace=ns)
            elif family == 4:
                parts = dict(sources=("src_a", "src_b"),
                             all_tags=pick(rng.randint(1, 3)),
                             namespace=ns)
            else:
                parts = dict(any_of=(
                    dict(all_tags=pick(1), namespace=ns),
                    dict(sources=("src_a",))))
            specs.append((f"spec{i}", parts))
        mismatches = 0
        for name, parts in specs:
            expected = ref_spec(parts)
            rids = rt.query(_spec_from_parts(parts)).iter_rids()
            actual = {post_of_rid[rid] for rid in rids}
            if expected != actual:
                mismatches += 1
                print(f"DIFF MISMATCH {name}: "
                      f"missing={list(expected - actual)[:5]} "
                      f"extra={list(actual - expected)[:5]}",
                      file=sys.stderr)
        result = dict(scale=scale, n_specs=len(specs),
                      rid_count=rt.rid_count, mismatches=mismatches,
                      peak_rss_bytes=peak_rss_bytes())
    return result


def _gates(report: dict) -> dict:
    gates = {}
    if report["scale"] != "5M":
        return {"note": "gates apply at 5M scale only"}
    families = report["families"]
    warm = [f["warm_plan"] for f in families.values()]
    p95s = [t["p95_ms"] for t in warm]
    gates["warm_count_p95_200ms"] = max(p95s) <= 200.0
    with_locations = [f for f in families.values()
                      if f["warm_first128_e2e"]["n"]]
    # gate scope is the query->first128 path, measured end to end on the
    # warm pass; plan-only and extract-only spans are reported separately.
    gates["first128_e2e_p95_500ms"] = (
        max(f["warm_first128_e2e"]["p95_ms"] for f in with_locations)
        <= 500.0) if with_locations else True
    gates["ten_k_1s"] = (max(f["warm_10k"]["p95_ms"]
                             for f in with_locations)
                         <= 1000.0) if with_locations else True
    gates["locations100k_reported"] = bool(
        report.get("locations100k", {}).get("rows") == 100000)
    gates["cache_resident_bounded"] = report["cache_stats"][
        "resident_le_limit"]
    return gates


def _run_phase(phase: str, scale_dir: Path, scale: str, args) -> None:
    """Execute one phase in-process (child mode)."""
    out = scale_dir / f"{phase}.json"
    run_id = _run_fingerprint(args)
    if out.exists():
        cached = json.loads(out.read_text())
        if cached.get("run_id") == run_id:
            print(f"[{scale_dir.name}] {phase}: cached (run {run_id[:12]})",
                  flush=True)
            return
        print(f"[{scale_dir.name}] {phase}: stale cache "
              f"(run {str(cached.get('run_id'))[:12]} != {run_id[:12]}), "
              "re-running", flush=True)
    if phase == "gen":
        data = phase_gen(scale_dir, scale_dir.name)
        data["run_id"] = run_id
        print(f"[{scale_dir.name}] gen: {data['expected_samples']} samples",
              flush=True)
    elif phase == "compile":
        print(f"[{scale_dir.name}] compile: starting (child process)",
              flush=True)
        data = phase_compile(scale_dir, scale_dir.name)
        data["run_id"] = run_id
        print(f"[{scale_dir.name}] compile: {data['wall_s']}s "
              f"rss={data['peak_rss_bytes'] // 2**20}MiB "
              f"temp={data['peak_temp_bytes'] // 2**20}MiB", flush=True)
    elif phase == "query":
        print(f"[{scale_dir.name}] query: starting (child process)",
              flush=True)
        data = phase_query(scale_dir, scale_dir.name, run_id,
                           cache_bytes=args.cache_bytes)
        print(f"[{scale_dir.name}] query: done", flush=True)
    elif phase == "diff":
        if args.n_specs <= 0:
            return
        print(f"[{scale_dir.name}] diff: {args.n_specs} specs "
              "(child process)", flush=True)
        data = phase_diff(scale_dir, scale_dir.name, n_specs=args.n_specs)
        data["run_id"] = run_id
        print(f"[{scale_dir.name}] diff: mismatches={data['mismatches']}",
              flush=True)
    else:
        raise SystemExit(f"unknown phase {phase}")
    out.write_text(json.dumps(data, sort_keys=True))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", required=True)
    parser.add_argument("--scale", choices=sorted(SCALE_PARAMS))
    parser.add_argument("--phases", default="gen,compile,query,diff")
    parser.add_argument("--cache-bytes", type=int, default=None)
    parser.add_argument("--n-specs", type=int, default=300)
    parser.add_argument("--run-phase", default=None,
                        help=argparse.SUPPRESS)
    args = parser.parse_args()
    workdir = Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    if args.scale is None:
        scales = ["100k", "1M", "5M"]
    else:
        scales = [args.scale]
    if args.run_phase:
        _run_phase(args.run_phase, workdir / args.scale, args.scale, args)
        return 0
    report_path = workdir / "report.json"
    if report_path.exists():
        report = json.loads(report_path.read_text())
    else:
        report = dict(platform=platform.platform(),
                      python=sys.version.split()[0], scales={})
    failed = False
    default_phases = args.phases == "gen,compile,query,diff"
    for scale in scales:
        scale_dir = workdir / scale
        scale_dir.mkdir(exist_ok=True)
        phases = args.phases.split(",")
        if default_phases and scale != "100k" and "diff" in phases:
            phases.remove("diff")
        for phase in phases:
            phase = phase.strip()
            out = scale_dir / f"{phase}.json"
            if out.exists():
                print(f"[{scale}] {phase}: cached", flush=True)
                continue
            cmd = [sys.executable, str(Path(__file__).resolve()),
                   "--workdir", str(workdir), "--scale", scale,
                   "--run-phase", phase, "--n-specs", str(args.n_specs)]
            if args.cache_bytes:
                cmd += ["--cache-bytes", str(args.cache_bytes)]
            subprocess.run(cmd, check=True)
        parts = {}
        for phase in ("compile", "query", "diff"):
            out = scale_dir / f"{phase}.json"
            if out.exists():
                parts[phase] = json.loads(out.read_text())
        if "query" in parts:
            parts["gates"] = _gates(parts["query"])
        if "diff" in parts and parts["diff"]["mismatches"] != 0:
            failed = True
        if "gates" in parts and any(
                isinstance(v, bool) and not v for v in
                parts["gates"].values()):
            failed = True
        report["scales"][scale] = parts
    # cross-scale memory growth gate (1M -> 5M must stay well below 5x)
    c1m = report["scales"].get("1M", {}).get("compile", {})
    c5m = report["scales"].get("5M", {}).get("compile", {})
    if c1m.get("peak_rss_bytes") and c5m.get("peak_rss_bytes"):
        ratio = c5m["peak_rss_bytes"] / c1m["peak_rss_bytes"]
        report["memory_growth_5m_over_1m"] = round(ratio, 2)
        if ratio >= 5.0:
            failed = True
    report["platform"] = platform.platform()
    report["python"] = sys.version.split()[0]
    # Full environment + source basis recorded in the artifact: the run
    # fingerprint below is the sha256 over the whole src tree, the bench
    # tool, the generator, the Python version and the pinned dependency
    # versions, so a cached phase from another tree or environment can
    # never masquerade as this run.
    import numpy
    import pyarrow
    import pyroaring

    report["env"] = {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "numpy": numpy.__version__,
        "pyarrow": pyarrow.__version__,
        "pyroaring": pyroaring.__version__,
    }
    report["run_fingerprint"] = _run_fingerprint(args)
    report_path.write_text(json.dumps(report, sort_keys=True))
    print(json.dumps(report, sort_keys=True, indent=1))
    if failed:
        print("PERFORMANCE_GATE_FAIL")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
