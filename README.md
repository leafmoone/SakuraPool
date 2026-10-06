# SakuraPool

## Raw image delivery settings

Publication/task downloads support registered `.jpg`, `.jpeg`, `.png`, `.webp`,
`.avif`, and `.gif` formats. The default remains the first five; GIF is opt-in.
`publication fetch` and `task create` accept `--image-extensions` as comma-separated
suffixes. Bytes are verified and delivered unchanged; no decoding or conversion
is performed. Unknown formats and unsafe filenames are rejected. Index adapter
`image_extensions` controls scanning and does not automatically enable download
formats.

Existing tasks can explicitly expand their format selection and adjust their
cumulative output ceiling without reselecting records:

```text
sakura task inspect /workspace/tasks/task
sakura task update /workspace/tasks/task --expected-settings-version 0 --image-extensions .jpg,.jpeg,.png,.webp,.avif,.gif --max-output-bytes 1610612736
```

Use the actual `settings_version` from inspect. Old tasks start at version zero
with the original five defaults. Updates use compare-and-swap, reject format
removal, active runners/items, UNKNOWN accounting, or any workspace pending
reservation/inflight use. Both settings and their old/new audit event commit in
one transaction. Validation/CAS/SQL failure rolls back both settings and audit.
Selection/seed/plan, deliveries and charged network accounting are unchanged.
If SQLite commits but reservation settlement fails, settings **are already
committed**: `TASK_RESOURCE_SETTLEMENT_UNKNOWN` retains the unresolved lease and
blocks further updates. Inspect the actual settings version; do not blindly
retry or clear reservations. A primary error plus settlement failure preserves
both diagnostics.

`max_output_bytes` is a finite total ceiling including already confirmed images
and requested metadata, not an additional allowance. It must be an integer at
least the confirmed cumulative bytes and below 2^63; zero means zero, not
unlimited. Workspace resource policy and per-image/chunk capacity still apply.
Settings never automatically raise quotas.


SakuraPool builds reusable P2 TAR indexes, compiles read-only P3 query snapshots,
and distributes runtime-first publication v2 for verified selective Range retrieval.
Local, download and remote builders and explicit budgeted Rust production profiles
are available; credentials and live binding proofs are never distributed.
No image decoding, model execution or production throughput claim.

P5-A adds bounded publication construction and a serial Python reader:
`from sakurapool.storage.publication_session import PublicationSession`.
A session fully verifies once, serves multiple record fetches and closes its bounded
binding cache. It owns the publication handle, not the caller's transport/ledger.
PublicationSession itself is serial and same-thread: no cross-thread SQLite use.
The task coordinator separately supports bounded concurrent lanes; this is not a
concurrent scheduler inside a shared PublicationSession.
See [P5 roadmap and task design](docs/P5_ROADMAP.md). P5-B serial task and real
closure certification is complete; see the [closure report](reports/P5B/REAL_CLOSURE_REPORT.md).
P5-C bounded pipeline is approved and merged. P5-D validation is complete for the
fixed `20261004T143905Z-upload` scope and merged at
`62f86cc487af41de7d84bfb5a14d2fb1e0e172ac`; independent semantic comparison
remains subset-only, and global index completion is not claimed. See
[the P5-D report](reports/P5D/REPORT.md). P6-A now implements explicit user
workspaces and resource/capacity policy; its current route is [plan.md](plan.md). Default task workers=1;
2/4 are explicit upper bounds, with actual concurrency limited by object distribution
and resource admission. Budget/ledger v2 migration is not implemented. Earlier P3/P4 CLI examples below are historical/admin
interfaces, not the default task workflow.

## P6-A workspace and implementation boundaries

```text
sakura workspace init WORKSPACE
sakura workspace inspect WORKSPACE
sakura doctor --workspace WORKSPACE --profile PROFILE.json
sakura task create --workspace WORKSPACE --publication PUB --query QUERY.json --selection first --limit 3 --metadata --task-dir WORKSPACE/tasks/TASK
```

The immutable capacity/layout belongs to workspace manifest v1; workspace ledger
v3 and TaskDB v2 retain physical-domain, identity and lineage binding. ResourcePolicy
is mutable with a durable epoch (`--expected-epoch` is explicit CAS; omission is
last-writer-wins). New cumulative quotas default to JSON null, not legacy P4 limits;
disk/inflight remain bounded. No legacy ledger or UNKNOWN task migration is provided.

Metadata is validated as a JSON object without materializing its object graph: strict
UTF-8, no BOM, NaN or Infinity, depth at most 128 and at most 1,000,000 nodes.
These are validator implementation boundaries, not adjustable metadata byte capacity.
Duplicate keys and escaped unpaired surrogates remain accepted. Flagged empty metadata
is invalid. This is intentionally narrower than every input accepted by `json.loads`.
The fixed validator working reservation is 8,320 bytes; parse failure does not erase
operation UNKNOWN or release a lease whose ownership has escaped.

RPC capacity is negotiated, with a separate 64 KiB bootstrap and bounded queue/parser.
The pinned reqwest/hyper HTTP/1 implementation has a 417,792-byte incomplete-head
threshold and an independent 100-field limit. Configured decoded header bytes above
417,760 are rejected before worker start. The boundary test reaches 417,760 with a
canonical HTTP/1.1 206 status line; extra wire whitespace or a longer reason phrase
can hit the underlying parser boundary earlier. It is not a universal response guarantee.

Ledger and protocol admission model live working allocations and deduct ledger
headroom once from available inflight. Python object sizes vary by interpreter;
headroom is not a universal fixed number. The protocol header factor covers conservative
pinned hyper 1.11.1 / bytes 1.12.1 growth/copy scenarios, not a formal allocator or RSS
upper bound. Native/unrelated process memory and allocator caches are outside this model.
Public ledger calls return small values and clear operation frames while locked;
arbitrary external retention of private `_read_pair` results is outside the model.

P6-A real-workspace metadata validation is **PARTIAL**: two of three items were
published and exported after formal pause; fresh-process resume failed, and a subsequent
resume retained one operation UNKNOWN and two pending leases. No final three-item
closure is claimed. See the final P6-A report for the execution deviation and evidence.

## Task API (recoverable task workflow)

Task creation freezes the publication content/snapshot identity, normalized P3
query and deterministic selection into a SQLite TaskDB. Resume uses those exact
seq rows, never repeats selection or sampling. `task_id` distinguishes instances;
`plan_digest` binds the reproducible plan. Modes are `all`, `first`, explicit
`records`, and versioned SHA256 top-K `sample` with an explicit seed.

```text
sakura task create --publication PUB --query QUERY.json --selection first --limit 3 --task-dir TASK
sakura task create --publication PUB --query QUERY.json --selection sample --limit 3 --seed seed --task-dir TASK
sakura task inspect TASK
sakura task run TASK --profile TASK_PROFILE.json
sakura task pause TASK
sakura task cancel TASK
sakura task resume TASK --profile TASK_PROFILE.json
sakura task export TASK --manifest OUTPUT.jsonl
```

Creation/inspection are local: no HTTP or token reading. Task/output/export paths
must be contained in their validated workspace physical domain; publication can be read
outside it. Omit `--workspace` only for an existing workspace-bound task (automatic
identity discovery) or the unchanged legacy P4 workflow. TaskDB uses DELETE journal and FULL synchronization with short explicit
transactions, plus a real single-runner OS file lock. One serial session fully
verifies the publication once per runner process. Pause/cancel CLI returns a
**requested** state: the bounded current item finishes before another is claimed.
Cancellation preserves deliveries and requires explicit resume. With concurrent
lanes, new claims stop when pause/cancel is observed; already-admitted active items
drain before the run returns. Same-object affinity does not promise multiple
concurrent streams from a single TAR.

Task connection profiles are separate from strict old single-object profiles:
`{"format":"sakurapool-task-connection-v1","origin":"https://modelscope.cn",
"repositories":["owner/dataset"],"worker":"ABSOLUTE_WORKER_PATH"}`.
Optional `credential_ref` is `{"env":"MODELSCOPE_API_TOKEN"}` or a bounded
`{"file":"ABSOLUTE_TOKEN_FILE"}` reference, not an embedded credential.
Object paths/revisions/digests come from the pinned publication, not this profile.

Successful durable per-attempt delivery/settlement receipts recover without
re-downloading or incrementing saved counts. An output with unknown settlement
is preserved and blocked, not retried or refunded. Lease absence and global
counter differences are not proof. There is no end-to-end exactly-once claim.
Exports stream only verified confirmed deliveries with source/plan identity,
relative task paths and delivery hashes, without image copies or archives.
The manifest must be directly inside TASK (for example TASK/subset.jsonl), so
its `output/<record_id>/...` paths resolve relative to the manifest's directory;
a different export base is explicitly rejected.
Legacy root, saved/body/attempt caps and binary ledger format remain unchanged;
RESOURCE_BLOCKED does not mean a user's authorized real request was exhausted.
Local resource diagnostics state effective limits, actual remaining and required
admission. Network body/attempt estimates before listing are **lower bounds**;
each later request still needs the legacy ledger's own reservation. No exact
per-task network consumption is inferred from global counters.

Explicit current task bounds: 100,000 frozen entries, 10,000 in-memory sample
heap entries, 512-record identity batches, 32 MiB TaskDB, 33 MiB journal allowance,
64 KiB plan header and 32 MiB exported manifest. SQLite temp work uses memory;
the maximum DB page count is derived from its actual page size. Task admission
also reserves growth/journal overhead under the legacy physical-root budget.
Windows file/SQLite fsync is used; portable Windows directory power-loss durability
is not claimed. Larger than 8 MiB members remain unsupported. Bounded task
concurrency is available; general workspace/ledger v2 migration is not implemented.
Production-scale validation is tracked separately.

## Publication v2 (runtime-first distribution)

Publication v2 is separate from the historical package v1. It preserves durable
P2 format 4 and runtime format 2; its default distribution contains no durable
partitions. Maintainers retain P2 separately for audit and recompilation.

```text
sakura publication build --runtime RUNTIME --p2-list p2-roots.json --remote-map remote-map.jsonl --output NEW_PUBLICATION
sakura publication inspect NEW_PUBLICATION
sakura publication verify NEW_PUBLICATION --full
sakura publication fetch --publication NEW_PUBLICATION --record-id RECORD_ID --profile PROFILE --output P4_OUTPUT --metadata
```

The P2 list is `{"format":"sakurapool-p2-root-list-v1","roots":["absolute/path"]}`.
Each bounded JSONL remote-map row contains `dataset_id`, `endpoint`, `repo_id`,
`repo_type`, `revision_candidate`, `object_path`, `object_size`, and
`provider_sha256` (64 lowercase hex or null). Rows must exactly cover runtime
objects. Only provider SHA equal to the indexed whole-TAR SHA makes an object
fetchable. Null provider SHA keeps query/location usable but blocks remote fetch.

The publication contains a bounded `PUBLICATION.json`, pinned runtime snapshot,
read-only `remote_objects.sqlite`, and mmap `image_sha256.npy` (`V32`, indexed by
rid). Build requires reliable computed image SHA in every P2 sample. Fast open
checks structure and identity, not complete content hashes; full verify streams
all hashes and cross-checks catalog identities. Fetch requires a full-verified
publication, fresh exact provider lookup, then its own positive/negative
conditional binding before Rust Range reads. Neither live ETag/proof nor signed
URL is persisted. Publication storage may be outside the network work root;
fetch outputs and accounting remain governed by the existing production ledger.

Image mismatch prevents delivery. A ledger settlement failure *after* atomic
rename may leave verified output visible with conservative pending accounting;
this is an error, not successful settlement, and the output is never overwritten.
Synthetic million-record measurements are not a production capacity guarantee.

## Project workflow and boundaries

[Project rules](docs/PROJECT_RULES.md) are the canonical current workflow and
boundary reference. The intended administrator flow is remote TAR indexing →
durable index → runtime compile/verify → versioned index publication. The user
flow is install/verify a published index → query → locate image/JSON → Range
retrieval → a small local dataset. Earlier P3 descriptions of remote scanning,
publication and retrieval as future work are historical; current explicit interfaces
are described above and do not imply a complete production index is available.

Ordinary users do not need complete local TARs; LocalTarScanner is for local
inputs, testing and validation. No ready-to-import index currently exists; do not
search for old hfutils/CheeseChaser indexes as a prerequisite. Large-repository
index construction requires separate authorization after development and acceptance.
The later P4 integration target is `leafmoone/game_cg_5M`; naming it grants no
access, full scan or write permission. See the rules for bounded canary planning,
object-version binding, component boundaries and dev/main review requirements.

P4-R2 adds an explicit Rust production transport and two administrator pipeline
interfaces; see [R2 transport](docs/R2_TRANSPORT.md) for configuration, gates and
whole-TAR spool/memory limits. The early minimal live capability batch stopped
BLOCKED (historical R2 evidence); subsequent approved small real closure evidence is
linked above. Neither those small closures nor these interfaces certify a complete
production-scale index. Ordinary fetch still requires a verified local package
and must not implicitly scan. No real full-TAR download or large-repo build was run.

## Installation and CLI

Python 3.10 or newer is required by the code and dependency minimums; there is no
project-imposed upper version bound or mandatory Python 3.12 certification policy.
Use the newest stable interpreter that actually installs and passes verification
with the pinned dependencies, not simply the highest version number. On Windows
x86-64, the current PyArrow 18.1.0 / NumPy 2.2.6 pins provide CPython 3.13 wheels
but not 3.14 wheels. Removing the metadata upper bound does not certify 3.14 or
permit silently upgrading those pins, transplanting Arrow, or using `--no-deps`.

The current single-project-environment selection and independent certification
are recorded in `reports/Python-venv-20260930/`; historical Fix3 Python 3.12 logs
remain unchanged and do not certify this later contract. Verification subprocesses
use the invoking interpreter (`sys.executable`) rather than a retired venv path.

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

## P3 runtime

The runtime compiles one or more committed P2 directories into an immutable
snapshot and answers queries from bitmaps without touching original archives.
The runtime format is 2, compiler `sakurapool-p3-v2`; v1 runtime snapshots are
rejected and must be recompiled. P2 durable FORMAT4 is unchanged, including
readability of partition build provenance recording runtime format 1. Snapshot
identity includes runtime format and compiler, so v1 and v2 do not collide.

```console
sakura runtime compile [--index DIR ...] [P2_DIR ...] RUNTIME_ROOT
sakura runtime query RUNTIME_ROOT [--source S] [--dataset D] [--namespace NS]
                 [--all-tag T] [--any-tag T] [--none-tag T] [--limit N] --full
sakura runtime query --snapshot RUNTIME_ROOT --spec SPEC.json
sakura runtime verify RUNTIME_ROOT [--full]
sakura runtime inspect RUNTIME_ROOT [--full]
sakura runtime lookup RUNTIME_ROOT --source SRC --post-id ID [--dataset DS]
```

`query` and `verify/inspect` also accept `--index`/`--snapshot` as aliases for
the positional root; `--spec SPEC.json` reads a spec object (including an
`any_of` list) and cannot be mixed with term flags. An unknown namespace is a
hard error even when no tags are given. `--full` runs the streaming full
verification (payload sha256 of every published file), while a plain open only
checks metadata and identity (below).

Layout per `RUNTIME_ROOT`: `current.json` (atomic pointer) plus
`snapshots/<snapshot_id>/{catalog.sqlite, bitmaps.sqlite, locations.npy,
SNAPSHOT.json, READY}`. Nothing is openable before `READY`; compilation is
stage-resumable (`STAGE.txt` marks stage1/catalog/bitmaps/locations/snapshot/ready).
A second `compile` reuses a published snapshot only after validating the READY
marker, the manifest, and a streamed sha256 of every published file; a broken
`current.json` is repaired, foreign `.staging-<other-id>` directories are never
deleted, and the P2 inventory fails closed on any undeclared file (stray
partials included), on `object_id != rel@sha256(input)`, and on fragment rows
that claim a different (dataset, object). `rid` is a dense `uint32` in
canonical order `(dataset_id, object_id, sample_path, record_id)`.
`locations.npy` is a rid-ordered numpy memmap (`object_idx u4, image_offset u8,
image_size u8, metadata_offset u8, metadata_size u8, format_id u2, flags u1`)
with an 82-byte identity trailer (`SAP3LOC1`, snapshot_id, rid_count) appended
after the payload: a fast open binds the whole file to the snapshot identity
(a same-shape `locations.npy` from another snapshot raises `SnapshotMixError`,
as does a whole-file swap of `bitmaps.sqlite`/`catalog.sqlite`), while an
in-place same-size payload edit is only detected by `--full` streaming
verification. `catalog.sqlite` holds names/IDs (records, sources, datasets,
namespaces, tags, tag_categories) and every lookup resolves via index
(asserted by `EXPLAIN QUERY PLAN` tests). Tag identity is `(namespace, value)`;
one namespace may be served by multiple sources/origins (bitmap union).
`tags_state in (known, empty)` defines a record's known set; `missing`/`invalid`
records contribute no tags. Categories are metadata: distinct non-null categories
are unioned into `tag_categories(tag_id, category)` (composite primary key,
declared FK to tags, index on `(category, tag_id)`). The compiler explicitly
validates references; it does not rely on SQLite FK enforcement. `tags` has no
category column. Staging `CATEGORIES.json` contains `[tag_id, category]` pairs
sorted by ID then category. `STAGE2-COUNTS.json` reports raw `tag_occurrences`;
formal `tag_memberships` sums final tag bitmap cardinalities, deduplicated across
categories, origins and chunks. `rt.tag_categories(namespace, value)` returns a
sorted tuple (empty for null-only categories); unknown tags/namespaces raise
`UnknownQueryValueError`. No category filter APIs are provided; queries remain
value-based. Offsets/sizes are stored lossless
beyond 2**63 and metadata presence is independent of its size (a zero-length
metadata keeps `HAS_METADATA`). The snapshot fingerprint excludes the P2
`created_at` timestamps by design, so re-timestamped identical inputs produce
the same `snapshot_id`.

Query domain (`RuntimeQuerySpec`): `sources` OR, `datasets` OR, then AND with
per-tag `all_tags`, one OR group of `any_tags`, and per-namespace `none_tags`
(`known_ns − excluded`); `any_of` is a union of flat branches. AND terms are
planned by their stored cardinality before any bitmap loads and materialization
stops once the intersection is empty. `query()` accepts a spec object or
keyword form (`rt.query(sources=..., namespace=..., all_tags=...)`) and returns
a lazy bitmap result: `count()` / `len()`, `iter_rids()`, `limit(n)`,
`iter_location_batches(size)`, `iter_record_batches(size)` (record batches carry
`source_name`/`dataset_name` alongside the numeric IDs); point resolution is
`lookup_rids(source, post_id[, dataset])` / `resolve_one(source, post_id,
[dataset])` (ambiguous matches raise `AmbiguousRecordError`) and
`object_ref(object_idx)` returns the full validated `ObjectRef`. Bitmaps are
cached in a byte-budget LRU (`cache_bytes` on `RuntimeSnapshot.open`, 256 MiB
default) with hits/misses/evictions counters; a single blob larger than the
budget is never cached.

Benchmark (`tools/bench_runtime.py`, synthetic corpus, deterministic seed):

```console
PYTHONPATH=src python tools/bench_runtime.py --workdir build/bench --scale 100k
# scales: 100k (correctness + 300-spec reference differential), 1M, 5M
# phases: gen, compile, query, diff — each phase is a child process, so peak RSS
# is attributed per phase; results merge into build/bench/report.json
```

The current P3 performance evidence is
[`reports/P3/bench-repair/benchmark.json`](reports/P3/bench-repair/benchmark.json),
with methodology and validation in [`reports/P3/report.md`](reports/P3/report.md)
and separate [installed-package evidence](reports/P3/bench-repair/benchmark-installed.json).
Earlier benchmark artifacts are retained as history, not current acceptance evidence.
These results cover synthetic workloads only, not network performance, full-repository
coverage or a guarantee for 21M records. Generator/options/code/environment fingerprints
must match; use a fresh benchmark workdir after workload changes rather than assuming
old phase files automatically invalidate. Do not rerun large benchmarks without the
applicable stage authorization.
