"""Random differential: Python-set reference vs Roaring bitmaps, per rid."""

import random

import pytest
from synthetic_p2 import ObjectSpec, SampleSpec, build_p2_directory

from sakurapool.runtime.inventory import load_p2_inventory
from sakurapool.runtime.query import RuntimeQuerySpec
from sakurapool.runtime.snapshot import RuntimeSnapshot

RNG_SEED = 20250101
N_SPECS = 1000


def _build_corpus(base):
    """Two sources, two datasets, three namespaces, mixed tag states.

    Returns per-(source, dataset) object lists plus post_id-keyed metadata;
    rid assignment is read back from the catalog, never re-derived here.
    """
    rng = random.Random(RNG_SEED)
    hot = [f"hot{i}" for i in range(12)]
    medium = [f"medium{i}" for i in range(6)]
    rare = [f"rare{i}" for i in range(3)]
    pairs = [("src_a", "ds_a"), ("src_b", "ds_b")]
    objects_by_dir = {}
    by_post = {}
    for source, dataset in pairs:
        objects = []
        for obj_no in range(3):
            if source == "src_a" and obj_no == 2:
                ns = "gelbooru"
            elif source == "src_b" and obj_no == 0:
                ns = "danbooru"
            else:
                ns = "tags"
            samples = []
            for i in range(300):
                tags = []
                if rng.random() < 0.70:
                    tags.append((hot[rng.randrange(len(hot))], "general"))
                if rng.random() < 0.10:
                    tags.append((medium[rng.randrange(len(medium))],
                                 "general"))
                if rng.random() < 0.005:
                    tags.append((rare[rng.randrange(len(rare))], "general"))
                if "hot3" in [value for value, _ in tags]:
                    tags.append(("hot4", "general"))  # correlated
                if rng.random() < 0.05:
                    tags.append(("solo", "general"))
                if rng.random() < 0.05:
                    tags.append(("group", "general"))  # exclusive pair
                # Same-rid category overlap plus different-rid category variation.
                if i % 3 == 0:
                    tags.extend([("multicat", "artist"), ("multicat", "general")])
                else:
                    tags.append(("multicat", "character"))
                tags_state = "known"
                if rng.random() < 0.03:
                    tags_state = "missing"
                elif rng.random() < 0.02:
                    tags_state = "invalid"
                elif not tags and rng.random() < 0.5:
                    tags_state = "empty"
                if tags_state != "known":
                    tags = []
                post_id = f"{source}{obj_no}{i}"
                samples.append(SampleSpec(f"{i}.jpg", post_id,
                                          sorted(set(tags)), tags_state))
                by_post[post_id] = dict(
                    source=source, dataset=dataset, namespace=ns,
                    tags={value for value, _ in tags},
                    known=tags_state in ("known", "empty"))
            objects.append(ObjectSpec(f"{dataset}-{obj_no}.tar", samples,
                                      namespace=ns,
                                      origin=f"origin-{source}-{obj_no}"))
        objects_by_dir[(source, dataset)] = objects
    return base, objects_by_dir, by_post, hot, medium, rare, pairs


class Reference:
    """Plain-Python set evaluator over the corpus metadata."""

    def __init__(self, meta):
        self.meta = meta
        self.rids = set(meta)

    def _by(self, predicate):
        return {rid for rid, row in self.meta.items() if predicate(row)}

    def _tag(self, namespace, value):
        return self._by(lambda row: row["namespace"] == namespace
                        and value in row["tags"])

    def _known(self, namespace):
        return self._by(lambda row: row["namespace"] == namespace
                        and row["known"])

    @staticmethod
    def _key(spec, tag):
        return tag if isinstance(tag, tuple) else (spec.namespace, tag)

    def branch(self, spec):
        result = set(self.rids)  # copy: &= mutates in place
        if spec.sources:
            result &= set().union(
                *(self._by(lambda row, s=s: row["source"] == s)
                  for s in spec.sources))
        if spec.datasets:
            result &= set().union(
                *(self._by(lambda row, d=d: row["dataset"] == d)
                  for d in spec.datasets))
        for tag in spec.all_tags:
            result &= self._tag(*self._key(spec, tag))
        if spec.any_tags:
            result &= set().union(
                *(self._tag(*self._key(spec, tag)) for tag in spec.any_tags))
        if spec.none_tags:
            by_ns = {}
            for tag in spec.none_tags:
                namespace, value = self._key(spec, tag)
                by_ns.setdefault(namespace, set()).add(value)
            for namespace, values in by_ns.items():
                excluded = set().union(*(self._tag(namespace, v)
                                         for v in values))
                result &= self._known(namespace) - excluded
        return result

    def spec(self, spec):
        if spec.any_of:
            return set().union(*(self.branch(b) for b in spec.any_of))
        return self.branch(spec)


def random_specs(rng, ns_tags, sources, n):
    """Specs only reference tags that exist in their namespace."""
    source_names = [s for s, _ in sources]
    datasets = [d for _, d in sources]
    namespaces = [ns for ns, values in ns_tags.items() if values]
    specs = []
    for _ in range(n):
        family = rng.randrange(10)
        ns = rng.choice(namespaces)
        pool = ns_tags[ns]

        def tag():
            return rng.choice(pool)

        def sample(k):
            return tuple(rng.sample(pool, min(k, len(pool))))

        if family == 0:
            spec = RuntimeQuerySpec(namespace=ns,
                                    sources=tuple(
                                        rng.sample(source_names, 1)))
        elif family == 1:
            spec = RuntimeQuerySpec(namespace=ns, all_tags=(tag(),))
        elif family == 2:
            spec = RuntimeQuerySpec(namespace=ns, all_tags=sample(2))
        elif family == 3:
            spec = RuntimeQuerySpec(namespace=ns, any_tags=sample(3))
        elif family == 4:
            spec = RuntimeQuerySpec(namespace=ns, none_tags=sample(1))
        elif family == 5:
            spec = RuntimeQuerySpec(
                namespace=ns, sources=tuple(
                    rng.sample(source_names, rng.randint(1, 2))),
                all_tags=sample(2))
        elif family == 6:
            spec = RuntimeQuerySpec(
                namespace=ns,
                datasets=tuple(rng.sample(datasets, rng.randint(1, 2))),
                any_tags=sample(2))
        elif family == 7:
            spec = RuntimeQuerySpec(
                namespace=ns,
                sources=tuple(rng.sample(source_names, 2)),
                all_tags=(tag(),), none_tags=sample(1))
        elif family == 8:
            spec = RuntimeQuerySpec(any_of=(
                RuntimeQuerySpec(namespace=ns, all_tags=(tag(),)),
                RuntimeQuerySpec(
                    sources=tuple(rng.sample(source_names, 1))),
            ))
        else:
            spec = RuntimeQuerySpec(
                namespace=ns,
                all_tags=(tag(),), any_tags=sample(2), none_tags=sample(1))
        specs.append(spec)
    return specs


def test_random_differential_reference_matches_bitmaps(tmp_path):
    base, objects_by_dir, by_post, hot, medium, rare, pairs = \
        _build_corpus(tmp_path)
    rng = random.Random(RNG_SEED + 1)
    inventories = []
    for (source, dataset), objects in objects_by_dir.items():
        idx = base / f"p2-{dataset}"
        build_p2_directory(idx, dataset=dataset, source=source,
                           objects=objects,
                           created_at="2025-01-01T00:00:00+00:00")
        inventories.append(load_p2_inventory(idx))
    import sakurapool.runtime.compiler as compiler
    from sakurapool.runtime.inventory import combine_inventories
    compiler.compile_runtime(combine_inventories(inventories),
                             base / "rt")
    with RuntimeSnapshot.open(base / "rt") as rt:
        assert rt.rid_count == len(by_post)
        rows = rt._catalog.execute(  # noqa: SLF001 - test boundary
            "SELECT rid, post_id FROM records").fetchall()
        meta = {rid: by_post[post_id] for rid, post_id in rows}
    ns_tags = {}
    for row in meta.values():
        for value in row["tags"]:
            ns_tags.setdefault(row["namespace"], set()).add(value)
    ns_tags = {ns: sorted(values) for ns, values in ns_tags.items()}
    assert ns_tags, "corpus must produce at least one tag"
    reference = Reference(meta)
    specs = random_specs(rng, ns_tags, pairs, N_SPECS)
    from sakurapool.runtime.errors import UnknownQueryValueError
    with RuntimeSnapshot.open(base / "rt") as rt:
        for i, spec in enumerate(specs):
            expected = reference.spec(spec)
            actual = set(rt.query(spec).iter_rids())
            assert actual == expected, (
                f"spec {i} family mismatch: spec={spec}\n"
                f"missing={sorted(expected - actual)[:10]}\n"
                f"extra={sorted(actual - expected)[:10]}")
        # unknown (namespace, tag) must raise, not silently match empty
        some_ns = next(iter(ns_tags))
        for probe in (RuntimeQuerySpec(namespace=some_ns,
                                       all_tags=["zz_missing"]),
                      RuntimeQuerySpec(namespace="no_such_ns",
                                       all_tags=["1girl"])):
            with pytest.raises(UnknownQueryValueError):
                rt.query(probe)
