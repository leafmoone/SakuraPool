# SakuraPool

Typed P1 reference queries, a P2 local uncompressed TAR index builder, and a P3
read-only runtime (compile committed P2 into a bitmap snapshot, query, resolve).
No data service access, image decoding, production throughput claims, or model
execution.

## Installation and CLI

The declared supported runtime is Python 3.10–3.13 with pinned PyArrow 18.1.0.
Python 3.14 is explicitly excluded: Arrow 18.1.0 has no matching wheel. The host's
Python 3.14 / Arrow 25 test run is diagnostic only, not certification of these pins.
Use a compatible Python and normal dependency resolution; never transplant Arrow
into a venv or use `--no-deps` to claim an installation passed.

```console
python -m pip install -e '.[dev]'
sakura --version
sakura config validate --config examples/index-config.json
sakura index scan --config examples/index-config.json --dataset synthetic --input /local/tars --output /local/index
sakurapool validate examples/query.json
sakurapool evaluate examples/query.json examples/rows.json
```

`sakurapool` remains an alias. `index scan INPUT OUTPUT` remains supported. Input may
be one uncompressed TAR or a directory recursively containing TARs. Compression is
rejected by known extension, content magic, and the uncompressed TAR reader, including
compressed content disguised as `.tar`. Inputs/outputs must not overlap.

## P1_CONTRACT_CHANGE

**Original definition:** P1 implemented only `DEFAULT_SCHEMA(id, value, label)` and
query/model/schema registries. It did not implement the required ObjectRef, MemberRef,
RecordKey or namespaced tag contract. Initial P2 incorrectly defined an Object as an
image occurrence, hashed dataset/source/shard/member/extents with SHA256, and made
samples one-to-one with objects. Tags were bare strings.

**Problem:** one TAR contains many physical records; moving extents changed identity;
there were no JSON member extents or registry-defined tag namespace/origin/category.
P1's toy query schema is not evidence that these original domain requirements existed.

**New definition (on-disk schema version 4):** one Object is one TAR shard, identified
by a stable storage profile plus a content-bound object_id. Each successfully paired
physical sample is one samples row; invalid/unpaired candidates produce errors while
required missing metadata retains the sample row.
`RecordKey=(dataset_id, object_id, sample_path)`, where sample_path is the adapter's
full logical member stem, not an image byte offset. `record_id` is exactly:

```python
hashlib.blake2b(json.dumps(
    ["sakurapool-record-v1", dataset_id, object_id, sample_path],
    ensure_ascii=False, separators=(",", ":")
).encode("utf-8"), digest_size=16).hexdigest()
```

The object_path is a canonical relative POSIX path; object_id is content-bound and
includes the SHA256 object version. Canonical paths are nonempty and have no empty,
dot, dot-dot, backslash, or colon components. Noncanonical IDs are rejected, never silently mapped.
Original key fields are retained with every sample/annotation. Duplicate/colliding
keys fail without publishing that shard. This is physical-record identity, not dedup.
`records.ObjectRef` is a self-contained retrieval object descriptor with
`storage_id/object_id/object_path/object_size/object_version/validator/backend/repo_type/
archive_format/validator_kind/validator_strength`; `archive_format` is keyword-only to
preserve the pre-existing positional ObjectRef ABI. `MemberRef` adds member path and
uint64 extent without opening or decoding images. P2 local objects use
`repo_type=local` and `archive_format=tar`; remote revision retrieval is not implemented.

**Migration impact:** version 1 outputs must not be reused. Build a new output directory;
there is no in-place migration. Object IDs, record IDs, table columns, error codes,
summary counts, and crash hooks changed. Existing P1 query APIs remain unchanged but
are not automatically a query engine for the four domain tables. Historical P2 reports
and `tools/verify_p2.py` describe the obsolete v1 protocol, not current certification.

## Four Arrow tables

- **objects:** stable `storage_id` profile, content-bound `object_id`, backend, nullable
  `repo_type`, `archive_format=tar`, dataset, object path/size/version, validator, and scan status. Local absolute build paths are never stored in logical identity.
- **samples:** retained RecordKey and record_id, source/post_id, image path/offset_data/size,
  nullable JSON path/offset_data/size, nullable text and physical image metadata, tags_state,
  tags:list<struct<value, category>>, hash_source and nullable sha256.
  Both image and JSON extents use uint64. No image decoder is imported.
- **annotations:** retained identity plus namespace/origin/tags_state and
  tags:list<struct<value, category>>; one grouped row per record/namespace/origin.
- **errors:** dataset_id, object_id, registry source, member/logical path, code, detail.
  Empty tables retain their exact schema.

Tags states: `known` is a nonempty list of strings, `empty` is `[]`, `missing` has no
configured metadata key, `invalid` has a wrong type. Missing/invalid tags are null.
Tag origin is `declared:json.<field>`; namespace/category come from the adapter.
Metadata SHA256 is only `declared:json.sha256`, never content proof. `--hash-images`
explicitly computes image SHA256 and marks `computed:sha256`. Default scanning does
not separately read images to hash them, **but the strong object validator reads the
whole TAR multiple times** (before scanning, before commit, and final input validation).
Size/mtime are change hints, not strong content proof. Validator version 1 explicitly
names strength `strong:sha256`.

## Registry and pairing

Source is mandatory registry data and never inferred from paths. Pairing preserves the
full logical stem. `image_prefix="images/"` and `metadata_prefix="meta/"` allow
`images/7.jpg` + `meta/7.json` to pair as `7`; remaining directory components are kept.
Prefix removal is explicit and collisions are rejected. `metadata_required` defaults
to true and can be false. `numeric_post_id` optionally requires decimal post stems.
`tags_field`, `text_field`, `max_json_bytes`, `tag_namespace`, `tag_category`, and
`image_extensions` are explicit options; defaults are `.jpg`, `.jpeg`, `.png`, `.webp`, and `.avif`. README.md/manifest.json, hidden paths,
directories, and unrecognized suffixes are ignored; `ignored_names` is configurable.

Error vocabulary: invalid_post_id, missing_image, missing_metadata, metadata_invalid,
source_mismatch, ambiguous_image_member, ambiguous_metadata_member, unsupported_member,
unsupported_archive, record_identity_conflict. Fatal archive/conflict/storage failures
raise and CLI returns error JSON/nonzero; per-record errors are stored and return 1.
Normal CLI returns 0; argument/fatal errors return 2.

## Publication, resume, and counts

Single writer, quiescent local inputs. Each shard writes four Parquet partial files in
batches, flushes/fsyncs and closes, then renames finals. COMMIT is last, atomically
published after fsync and records file path/size/SHA256/row counts, input validator,
schema/builder/dataset/object and created_at. POSIX fsyncs directories. Windows CRT
has no directory fsync: process recovery is tested, power-loss durability is not promised.
Readers must verify COMMIT before consuming files, never simply glob uncommitted finals.

Crash hooks: A=half samples partial before footer; B=all Parquet closed, no rename;
C=first final rename only; D=all final files without marker; E=marker published.
A–D rebuild; E validates all hashes/schemas/counts before skipping. Corrupt committed
outputs fail `CORRUPT_COMMIT` and are not silently rebuilt. Changed input content,
mtime, inventory, or configuration rejects existing offsets. Only reserved builder
partial names are removed; unrelated user files are preserved. Do not place your own
files in builder-reserved names. COMMIT hashes are not authenticated against an attacker.

Summary objects_seen/objects_committed/objects_skipped count TAR shards. `objects` is
the total shard rows including skipped shards; `samples` counts indexed physical sample
rows, not shards, tags, or invalid candidates. `skipped` is a compatibility alias of
objects_skipped. created_at and timings are nondeterministic; logical rows/IDs/counts
are deterministic. Memory still scales with the largest shard (headers and rows are
materialized); batched Parquet writing is not a bounded-memory streaming claim.

## Verification

```console
PYTHONPATH=src python -m pytest -q
python -m ruff check .
git diff --check
git diff 52358d6fca728d2bba12814490e0974a6907b218..HEAD --check
python -m build --wheel --outdir /tmp/sakurapool-p2-wheel
PYTHONPATH=src python tools/benchmark_indexer.py
```

The current on-disk contract is `FORMAT_VERSION=4`, builder `sakurapool-p2-v4`.
`storage_id` is a stable configured profile identifier (default `local`), independent of
content-bound `object_id`. ObjectRef carries storage_id, object_id, object_path,
object_size, object_version, and validator. `archive_format=tar` is separate from
`repo_type=local`. Version 3 outputs are incompatible and must not be reused; build a
new output directory. Future ModelScope validation MUST NOT use remote full-object SHA
rereads as the normal validation path; use fixed revisions and object validators.
P4 MUST NOT use remote full-object SHA rereads as the normal ModelScope validation path.

The benchmark generates 10,000 synthetic samples in **one** TAR. It reports
header/json/parquet/total seconds and process peak RSS including fixture generation
and imports; total excludes fixture generation but includes whole-TAR validation.
No production performance inference is valid. See the revision report for actual
command logs, compatible installation failures/successes, and remaining gates.

## P3 runtime (dev)

The runtime compiles one or more committed P2 directories into an immutable
snapshot and answers queries from bitmaps without touching original archives.

```console
sakura runtime compile INDEX_DIR [INDEX_DIR ...] RUNTIME_ROOT
sakura runtime verify RUNTIME_ROOT [--full-verify]
sakura runtime inspect RUNTIME_ROOT [--full-verify]
sakura runtime lookup RUNTIME_ROOT --source SRC --post-id ID [--dataset DS]
```

Layout per `RUNTIME_ROOT`: `current.json` (atomic pointer) plus
`snapshots/<snapshot_id>/{catalog.sqlite, bitmaps.sqlite, locations.npy,
SNAPSHOT.json, READY}`. Nothing is openable before `READY`; compilation is
stage-resumable (`STAGE.txt` marks stage1/catalog/bitmaps/locations/snapshot/ready)
and a second `compile` against the same published snapshot is a no-op. `rid` is a
dense `uint32` in canonical order `(dataset_id, object_id, sample_path,
record_id)`. `locations.npy` is a rid-ordered numpy memmap
(`object_idx u4, image_offset u8, image_size u8, metadata_offset u8,
metadata_size u8, format_id u2, flags u1`); `catalog.sqlite` holds names/IDs
(records, sources, datasets, namespaces, tags with categories) and every lookup
resolves via index (asserted by `EXPLAIN QUERY PLAN` tests). Tag identity is
`(namespace, value)`; one namespace may be served by multiple sources/origins
(bitmap union). `tags_state in (known, empty)` defines a record's known set;
`missing`/`invalid` records contribute no tags. Conflicting categories for one
`(namespace, value)` fail the compile with `TAG_CATEGORY_CONFLICT`.

Query domain (`RuntimeQuerySpec`): `sources` OR, `datasets` OR, then AND with
per-tag `all_tags`, one OR group of `any_tags`, and per-namespace `none_tags`
(`known_ns − excluded`); `any_of` is a union of flat branches. `query()` returns a
lazy bitmap result: `count()`, `iter_rids()`, `limit(n)`,
`iter_location_batches(size)`, `iter_record_batches(size)`; point resolution is
`lookup_rids(source, post_id[, dataset])` / `resolve_one(source, post_id,
[dataset])` (ambiguous matches raise `AmbiguousRecordError`). Bitmaps are cached
in a byte-budget LRU (`cache_bytes` on `RuntimeSnapshot.open`, 256 MiB default)
with hits/misses/evictions counters.

Benchmark (`tools/bench_runtime.py`, synthetic corpus, deterministic seed):

```console
PYTHONPATH=src python tools/bench_runtime.py --workdir build/bench --scale 100k
# scales: 100k (correctness + 300-spec reference differential), 1M, 5M
# phases: gen, compile, query, diff — each phase is a child process, so peak RSS
# is attributed per phase; results merge into build/bench/report.json
```

Host Python 3.14.5, Windows 11, one run: compile 100k 2.2 s / 1M 46 s / 5M 285 s
with peak RSS 143 / 272 / 795 MiB (5M/1M growth 2.92×) and peak temp 35 / 361 /
1838 MiB; published snapshot 15 / 156 / 794 MiB (≈165 B/sample: catalog 119,
locations 37, bitmaps 2). Query process RSS stays 51–150 MiB at 5M (memmap +
byte-LRU); warm typical-count p95 ≤ 0.52 ms, first-128-locations p95 ≤ 0.10 ms,
10k-locations p95 ≤ 3.4 ms; a 2.5 M-rid result counts and returns its first 100
rows in <1 ms. With a 1 MiB cache limit the LRU evicts (35 evictions), keeps
resident ≤ limit, and plans stay <0.7 ms. The 100k corpus passes a 300-spec
Python-set differential with 0 mismatches; the unit suite adds a 1000-spec
differential on a mixed-state corpus, crash/resume at every stage boundary,
READY reuse, and fresh-process reopen. All numbers are synthetic and
diagnostic-only; no production performance inference is valid.
