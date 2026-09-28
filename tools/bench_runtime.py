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
import math
import os
import platform
import random
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "tests"))


# Two EXPLICIT import modes (never conflated in the evidence):
#   installed - sakurapool is importable from the environment (a real
#               wheel in site-packages); the repo src tree must NOT be
#               on sys.path, and the resolved module path must be in
#               site-packages. The assertion below enforces both.
#   worktree  - dev mode: fall back to the repo src tree. Recorded as
#               such, never claimed as an installed/wheel measurement.
# The mode and the real module path are written into every phase JSON
# and the final report; a parent run verifies its subprocess phases ran
# with the identical module path.
def _bootstrap_module() -> tuple[str, str]:
    src_dir = (REPO / "src").resolve()
    try:
        import sakurapool as _sp
    except ModuleNotFoundError:
        sys.path.insert(0, str(REPO / "src"))
        import sakurapool as _sp
        mode = "worktree"
    else:
        mode = "installed"
    mod_path = Path(_sp.__file__).resolve()
    if mod_path.is_relative_to(src_dir):
        mode = "worktree"
    if mode == "installed":
        for p in sys.path:
            if not p:
                continue
            try:
                if Path(p).resolve().is_relative_to(src_dir):
                    raise AssertionError(
                        f"installed mode polluted by src on sys.path: {p}")
            except OSError:
                continue
        if "site-packages" not in mod_path.parts:
            raise AssertionError(
                f"installed mode module not in site-packages: {mod_path}")
    return mode, str(mod_path)


MODULE_MODE, MODULE_PATH = _bootstrap_module()

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
ANCHORS = ("hot_anchor", "medium_anchor", "rare_anchor")
CORR_TAGS = ("corr_a", "corr_b", "exclude_anchor")
WARM_REPEATS = 100
FREQUENCY_BANDS = {"hot_anchor": (0.68, 0.72),
                   "medium_anchor": (0.09, 0.11),
                   "rare_anchor": (0.0007, 0.0013)}
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
        "anchors": list(ANCHORS),
        "correlated_anchors": list(CORR_TAGS),
        "exclusive": list(EXCLUSIVE),
        "seed": SEED_BASE + len(scale),
    }


def sample_tags(rng, hot, medium, rare) -> tuple[list[str], str]:
    """Independent per-known-record anchor draws; tags may overlap.

    The denominator is *only* danbooru records with tags_state=known.
    Missing, invalid and empty are excluded; they are chosen before
    anchor sampling so labels never count toward an excluded state.
    """
    state_roll = rng.random()
    if state_roll < 0.03:
        return [], "missing"
    if state_roll < 0.05:
        return [], "invalid"
    if state_roll < 0.07:
        return [], "empty"
    tags: list[str] = []
    if rng.random() < 0.70:
        tags.append(ANCHORS[0])
        if rng.random() < 0.45:
            tags.append(CORR_TAGS[0])
            if rng.random() < 0.45:
                tags.append(CORR_TAGS[1])
        if rng.random() < 0.20:
            tags.append(CORR_TAGS[2])
    if rng.random() < 0.10:
        tags.append(ANCHORS[1])
    if rng.random() < 0.001:
        tags.append(ANCHORS[2])
    if rng.random() < 0.70:
        tags.append(hot[rng.randrange(len(hot))])
    if rng.random() < 0.10:
        tags.append(medium[rng.randrange(len(medium))])
    if rng.random() < 0.001:
        tags.append(rare[rng.randrange(len(rare))])
    pair = rng.random()
    if pair < 0.05:
        tags.append(EXCLUSIVE[0])
    elif pair < 0.10:
        tags.append(EXCLUSIVE[1])
    return sorted(set(tags)), "known"


def _validate_frequencies(manifest: dict) -> None:
    known = manifest["known_records"]
    if not known:
        raise ValueError("benchmark has no tag-known records")
    for name, (low, high) in FREQUENCY_BANDS.items():
        count = manifest["anchor_counts"][name]
        frequency = manifest["anchor_frequencies"][name]
        if not (count > 0 and low <= frequency <= high
                and frequency == count / known):
            raise ValueError(f"anchor frequency outside {low}..{high}: "
                             f"{name}={count}/{known}={frequency}")


def phase_gen(workdir: Path, scale: str) -> dict:
    params = SCALE_PARAMS[scale]
    vb = vocab(scale)
    rng = random.Random(vb["seed"])
    root = workdir / "p2"
    manifest = dict(scale=scale, **vb, dirs={},
                    frequency_denominator="danbooru tags_state=known records",
                    known_records=0,
                    anchor_counts={name: 0 for name in ANCHORS},
                    expected_samples=params["objects_per_dir"] * 2
                    * params["samples_per_object"])
    for source, dataset in DIRS:
        objects = []
        for obj_no in range(params["objects_per_dir"]):
            namespace = "danbooru"  # both sources share one tag namespace
            samples = []
            for i in range(params["samples_per_object"]):
                values, state = sample_tags(
                    rng, vb["hot"], vb["medium"], vb["rare"])
                if state == "known":
                    manifest["known_records"] += 1
                    for anchor in ANCHORS:
                        manifest["anchor_counts"][anchor] += anchor in values
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
    if not manifest["known_records"]:
        raise ValueError("benchmark has no tag-known records")
    manifest["anchor_frequencies"] = {
        name: count / manifest["known_records"]
        for name, count in manifest["anchor_counts"].items()}
    _validate_frequencies(manifest)
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


def _module_block() -> dict:
    return {"mode": MODULE_MODE, "module_path": MODULE_PATH}


_FAMILY_ORDER = ("source_only", "hot", "rare", "hot_and_medium",
                 "three_tag_and", "three_tag_or", "not", "source_2tag",
                 "two_source_2tag", "any_of_2")


def _family_specs(manifest: dict) -> dict:
    hot, medium, rare = ANCHORS
    corr_a, corr_b, excluded = CORR_TAGS
    shared = dict(namespace="danbooru")
    return {
        "source_only": dict(sources=("src_a",)),
        "hot": dict(**shared, all_tags=(hot,)),
        "rare": dict(**shared, all_tags=(rare,)),
        "hot_and_medium": dict(**shared, all_tags=(hot, medium)),
        "three_tag_and": dict(**shared, all_tags=(hot, corr_a, corr_b)),
        "three_tag_or": dict(**shared, any_tags=(hot, medium, rare)),
        "not": dict(**shared, all_tags=(hot,), none_tags=(excluded,)),
        "source_2tag": dict(**shared, sources=("src_b",),
                            all_tags=(hot, medium)),
        "two_source_2tag": dict(**shared, sources=("src_a", "src_b"),
                                all_tags=(hot, medium)),
        "any_of_2": dict(any_of=(
            dict(**shared, sources=("src_b",), all_tags=(hot, medium)),
            dict(**shared, sources=("src_a",), all_tags=(hot, corr_a)),
        )),
        "empty_boundary": dict(**shared, all_tags=EXCLUSIVE),
    }


def _spec_from_parts(parts: dict):
    from sakurapool.runtime.query import RuntimeQuerySpec

    if "any_of" in parts:
        return RuntimeQuerySpec(
            any_of=tuple(_spec_from_parts(b) for b in parts["any_of"]))
    return RuntimeQuerySpec(**parts)


def _timings(values: list[float]) -> dict:
    """Nearest rank: sorted[ceil(p*n)-1], for every reported percentile."""
    if not values:
        raise ValueError("cannot report empty timing sample")
    values = sorted(values)
    def rank(p):
        return round(values[math.ceil(p * len(values)) - 1] * 1000, 3)
    return {"n": len(values), "percentile_algorithm": "nearest_rank",
            "p50_ms": rank(0.50), "p95_ms": rank(0.95),
            "p99_ms": rank(0.99), "max_ms": round(values[-1] * 1000, 3)}


def _drain_10k(r) -> tuple[float, int]:
    """Return elapsed seconds AND actual materialized rows (<= 10000)."""
    t0 = time.perf_counter()
    got = 0
    batches = iter(r.iter_location_batches(2000))
    while got < 10000:
        batch = next(batches, None)
        if batch is None:
            break
        got += len(batch.rid)
    return time.perf_counter() - t0, got


def _capacity(rt, snapshot_dir: Path) -> dict:
    """Measure serialized tag payload, NOT SQLite container overhead."""
    unique_tags = rt._catalog.execute("SELECT COUNT(*) FROM tags").fetchone()[0]
    memberships, payload = rt._bitmaps.execute(
        "SELECT COALESCE(SUM(cardinality),0), COALESCE(SUM(length(blob)),0) "
        "FROM bitmaps WHERE kind='tag'").fetchone()
    assert unique_tags > 0 and memberships > 0
    file_bytes = {n: (snapshot_dir / n).stat().st_size
                  for n in ("catalog.sqlite", "bitmaps.sqlite", "locations.npy")}
    total = sum(p.stat().st_size for p in snapshot_dir.iterdir() if p.is_file())
    baseline = memberships * 4
    return dict(rid_count=rt.rid_count, unique_tags=unique_tags,
                tag_memberships=memberships,
                avg_tag_memberships_per_sample=memberships / rt.rid_count,
                tag_bitmap_blob_bytes=payload,
                bytes_per_tag_membership=payload / memberships,
                uint32_baseline_bytes=baseline,
                bitmap_compression_ratio=payload / baseline,
                bitmap_savings_fraction=1 - payload / baseline,
                compression_ratio_definition=(
                    "tag Roaring payload / (tag memberships * 4); lower is better"),
                snapshot_file_bytes=file_bytes, total_snapshot_bytes=total,
                bytes_per_sample=total / rt.rid_count)


def phase_query(workdir: Path, scale: str, run_id: str,
                cache_bytes: int | None = None) -> dict:
    from sakurapool.runtime.snapshot import RuntimeSnapshot

    manifest = json.loads((workdir / "manifest.json").read_text())
    root = workdir / "rt"
    kwargs = dict(cache_bytes=cache_bytes) if cache_bytes else {}
    _validate_frequencies(manifest)
    specs = _family_specs(manifest)
    # Preflight all families BEFORE formal timing: no zero-result workload
    # may silently pass; empty_boundary is an explicitly separate test.
    with RuntimeSnapshot.open(root, **kwargs) as preflight:
        counts = {name: preflight.query(_spec_from_parts(parts)).count()
                  for name, parts in specs.items()}
        if counts["empty_boundary"] != 0:
            raise ValueError(f"empty_boundary must be zero: {counts}")
        if any(counts[name] <= 0 for name in _FAMILY_ORDER):
            raise ValueError(f"representative family is empty: {counts}")
        branches = specs["any_of_2"]["any_of"]
        branch_counts = [preflight.query(_spec_from_parts(branch)).count()
                         for branch in branches]
        if len(branch_counts) != 2 or any(n <= 0 for n in branch_counts):
            raise ValueError(f"any_of_2 has an empty branch: {branch_counts}")
        if counts["hot"] != manifest["anchor_counts"][ANCHORS[0]]:
            raise ValueError("hot query count does not equal actual generation count")
        capacity = _capacity(preflight, preflight.path)
    # COLD pass: one independent snapshot (and cache) per family.
    cold = {}
    for name in _FAMILY_ORDER:
        rt = RuntimeSnapshot.open(root, **kwargs)
        parts = specs[name]
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
            t10k = _drain_10k(r)[0]
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
                  frequency={key: manifest[key] for key in (
                      "frequency_denominator", "known_records", "anchor_counts",
                      "anchor_frequencies")},
                  capacity=capacity,
                  empty_boundary={"query_spec": specs["empty_boundary"],
                                  "result_cardinality": counts["empty_boundary"]},
                  any_of_2_branch_cardinalities=branch_counts,
                  percentile_algorithm="nearest_rank ceil(p*N)-1",
                  families={}, large=None, locations100k=None,
                  cache_stats=None)
    for name in _FAMILY_ORDER:
        spec = _spec_from_parts(specs[name])
        # Explicit UNTIMED warmup per family. No first cold/missing-cache
        # query may be relabeled as a warm timing sample.
        warmup = rt.query(spec)
        next(warmup.iter_location_batches(128))
        _, warmed_rows = _drain_10k(warmup)
        if warmed_rows != min(10000, counts[name]):
            raise ValueError(f"warmup materialized {warmed_rows} unexpected rows")
        plans, extract128, e2e128, materialization = [], [], [], []
        actual_rows = min(10000, counts[name])
        for _ in range(WARM_REPEATS):
            # span 1: plan only (query() up to the bitmap result)
            t0 = time.perf_counter()
            r = rt.query(spec)
            plans.append(time.perf_counter() - t0)
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
            elapsed, rows = _drain_10k(r2)
            if rows != actual_rows:
                raise ValueError(f"materialized {rows}, expected {actual_rows}")
            materialization.append(elapsed)
        result["families"][name] = dict(cold[name])
        result["families"][name].update(
            family=name, query_spec=specs[name],
            untimed_warmup=True, result_cardinality=counts[name],
            warm_repeats=WARM_REPEATS,
            warm_plan=_timings(plans),
            warm_first128_extract=_timings(extract128),
            warm_first128_e2e=_timings(e2e128),
            warm_materialization=dict(requested_rows=10000,
                                      actual_rows=actual_rows,
                                      **_timings(materialization)))
        if actual_rows == 10000:
            result["families"][name]["warm_10k"] = _timings(materialization)
    # large result: half-corpus source term (>1M rids at 5M scale)
    big = _spec_from_parts({"sources": ("src_a",)})
    r = rt.query(big)
    t0 = time.perf_counter()
    count = r.count()
    first100 = r.limit(100)
    t_big = time.perf_counter() - t0
    result["large"] = dict(count=count, first100=len(first100),
                           first100_s=round(t_big, 3))
    # Exactly 100,000 rows actually materialized; never request another
    # iterator batch after reaching the target.
    t0 = time.perf_counter()
    got = 0
    batches = iter(r.iter_location_batches(2000))
    while got < 100000:
        batch = next(batches, None)
        if batch is None:
            break
        got += len(batch.rid)
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
    if set(families) != set(_FAMILY_ORDER) or any(
            f["result_cardinality"] <= 0 or f["warm_repeats"] < 100
            for f in families.values()):
        raise ValueError("5M representative families must be nonempty and timed")
    warm = [f["warm_plan"] for f in families.values()]
    p95s = [t["p95_ms"] for t in warm]
    gates["warm_count_p95_200ms"] = max(p95s) <= 200.0
    with_locations = list(families.values())
    # gate scope is the query->first128 path, measured end to end on the
    # warm pass; plan-only and extract-only spans are reported separately.
    gates["first128_e2e_p95_500ms"] = max(
        f["warm_first128_e2e"]["p95_ms"] for f in with_locations) <= 500.0
    ten_k = [f for f in with_locations
             if f["warm_materialization"]["actual_rows"] == 10000]
    if not ten_k:
        raise ValueError("5M contains no true 10k materialization family")
    gates["ten_k_1s"] = max(
        f["warm_materialization"]["p95_ms"] for f in ten_k) <= 1000.0
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
    data["module"] = _module_block()
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
                # Parent asserts the child process measured with the
                # IDENTICAL module (mode + resolved path): a phase whose
                # child imported a different sakurapool would corrupt the
                # attribution of the whole run.
                child = parts[phase].get("module", {})
                if (child.get("mode") != MODULE_MODE
                        or child.get("module_path") != MODULE_PATH):
                    raise SystemExit(
                        f"[{scale}] {phase}: module mismatch between "
                        f"parent ({MODULE_MODE} {MODULE_PATH}) and child "
                        f"({child.get('mode')} {child.get('module_path')})")
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
    report["module"] = _module_block()
    report["run_fingerprint"] = _run_fingerprint(args)
    report_path.write_text(json.dumps(report, sort_keys=True))
    print(json.dumps(report, sort_keys=True, indent=1))
    if failed:
        print("PERFORMANCE_GATE_FAIL")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
