# Local partition builder

This workflow is separate from P4 remote retrieval. It consumes already-downloaded
local TARs; it neither downloads a repository nor deletes local inputs.

## Adapter and partition contract

`examples/webdataset-danbooru-v3.json` declares seven independent datasets with
`nested_json_v1`. Provenance validates the configured source; it cannot select or
change that source. Missing paths differ from JSON null. Captions use only
`captions.nl2`, with no fallback. Auxiliary character/dropout tags are not indexed.
Image hashes are computed by Rust, never taken from JSON provenance.

`partition_objects(repository, dataset, source, objects)` sorts full repository
paths and groups 32 TARs, allowing a short tail. Duplicate repository paths are
rejected rather than deduplicated. Each input object declares an absolute local
path, positive size, and lowercase provider SHA256 or null (UNKNOWN).
`PartitionManifest.to_dict()` produces a CLI manifest. Durable INPUT deliberately
excludes local filesystem paths.

## CLI

```text
sakurapool index build-partition --config adapters.json --manifest part.json \
  --worker /absolute/path/sakurapool-worker --work-dir /owned/work/part \
  --output /owned/durable/part --code-sha FULL_CANDIDATE_OR_APPROVED_SHA
```

A persistent worker scans each TAR once for whole SHA, member extents and image
hashes. The provider SHA is checked before publication. Python reads only JSON
extents; object rows are spooled and fragments published per object. JSON over
`adapter.max_json_bytes` is rejected, not truncated.

Local file reads use a fixed 64 KiB buffer below the scanner's hash/count reader.
Hashes, member offsets and trailing-data checks count bytes consumed by the
parser. A rejected scan may have prefetched up to one buffer beyond that logical
position; `max_bytes` is not an exact physical-read cap. This local read-ahead
does not change HTTP body accounting or retain complete TAR contents in memory.

The code SHA supplied by the build orchestrator must identify the actual installed
wheel/source used. Syntax validation alone does not attest the deployment. Worker
version and executable SHA, adapter contract and durable/runtime format versions
are recorded in INPUT alongside the fixed partition plan.

## Resume and release

Complete COMMITs are validated before reuse. Partial objects are rerun under the
fixed INPUT contract. Unknown durable or stage files fail closed. Keep the owned
work directory and recovery receipts with an interrupted run; do not replace the
contract to make incompatible data resume.

Machine-readable completion receipts identify repository path, local path and
committed content SHA. Only a positively verified local input may be reported
`SAFE_TO_RELEASE_LOCAL_OBJECT=true`; reused unverified local content must not be
interpreted as release authorization. The builder never deletes TARs. A missing
provider SHA/revision remains UNKNOWN, not proof of safe remote Range publication.

## Multipart runtime

Load every durable root with `load_p2_inventory`, then use
`combine_inventories` and `compile_runtime`. Same-dataset roots require equivalent
canonical adapters and hash contracts, plus unique object IDs and repository
paths. Physical duplicate source/post IDs remain separate records; resolving an
ambiguous logical ID must not silently pick one. The runtime is the unified
publication, not a giant merged durable Parquet.

## Replace changed TARs without rescanning unchanged TARs

`index replace-tars` supports one or several **same-path** replacements in an
existing P2 input set. Supply the dataset, the changed repository-relative TAR
paths, and a local directory containing those paths. There is no image-ID list,
and local sizes, whole-file hashes, member offsets, image hashes, JSON metadata
and dimensions are derived by the existing native partition builder.

```text
sakura index replace-tars --p2-list old-p2-roots.json --dataset danbooru_native \
  --tar danbooru/2025_1/changed-a.tar --tar danbooru/2025_1/changed-b.tar \
  --local-root /downloaded-changes --worker /absolute/path/sakurapool-worker \
  --work-dir /owned/new-replacement-work --output /owned/new-p2-input-set \
  --code-sha FULL_INSTALLED_SOURCE_SHA
```

In this example the first local file is
`/downloaded-changes/danbooru/2025_1/changed-a.tar`. Alternatively use
`--tar-list changed-tars.json`, a JSON array of the same repository-relative
paths, instead of repeated `--tar`. The list is bounded to 4 MiB and 65,536
targets. Targets must be unique and already present in the selected dataset.
Renames, additions of previously unknown object paths, and repository migrations
are not replacements. Selected inputs must have hashed `nested_json_v1`
`build-partition` provenance; their adapter and repository contract are retained.

The old root-list format is the ordinary publication input format:

```json
{"format":"sakurapool-p2-root-list-v1","roots":["/absolute/old/part-000000","/absolute/old/part-000001"]}
```

Only listed local TARs are opened/scanned. Original TAR files are not needed.
Old P2 inputs are fully validated and never modified. Entire unaffected P2
roots are referenced by absolute path and **must remain available and immutable**.
For an affected partition, retained objects' Parquet files are independent,
byte-identical copies; their enclosing INPUT/COMMIT contract hashes are updated.
A fully replaced old partition is omitted. Changed content gets a new object
SHA identity and therefore new physical record IDs, even when post IDs match.
Unchanged physical record IDs remain stable. Runtime numeric RIDs may change
when the unified snapshot is recompiled.

Successful output contains `p2-roots.json`, `REPLACEMENT.json` and `READY`, plus
the newly built/recomposed P2 roots. Compile the complete new set normally:

```text
sakura runtime compile --p2-list /owned/new-p2-input-set/p2-roots.json \
  --output /owned/new-runtime
```

This saves TAR scanning, not all index work: existing P2 fragments are still
validated, and the complete unified runtime/publication is compiled normally.
`--p2-list` requires `--output` and cannot be mixed with positional directories
or `--index`.

### Remote revision may be supplied later

P2 replacement does not need a remote revision. Without explicit complete remote
binding inputs it emits `remote-bindings.pending.json` with `UNRESOLVED` status,
old/new object IDs, local sizes and **local content** SHA256 values. It does not
emit a publishable full remote-map. A local hash is not a provider identity or
proof that an upload exists at a revision. After upload, obtain explicit remote
bindings and use the existing publication builder with the new P2-root list;
there is no need to rescan the changed TARs merely to compile/publish them.

If both binding files are already available, add:

```text
  --remote-map old-full-remote-map.jsonl \
  --replacement-map changed-only-remote-map.jsonl
```

These use the existing [publication remote-map schema](../README.md). The old
map must cover exactly the old P2 objects. The replacement map must cover exactly
the selected dataset/path pairs, retain endpoint/repository/type, and name a new
fixed revision. Sizes and any supplied provider SHA are checked against the new
local scan. The generated full `remote-map.jsonl` preserves every unchanged row's
values, including its original revision; it never globally changes a repository
revision. `SUPPLIED` means explicit binding data was supplied, not remotely
verified by this offline command. A null provider SHA remains UNKNOWN and those
objects remain fetch-blocked in a publication. Download-time metadata and lane
proof checks remain mandatory.

```text
sakura publication build --runtime /owned/new-runtime \
  --p2-list /owned/new-p2-input-set/p2-roots.json \
  --remote-map /owned/new-p2-input-set/remote-map.jsonl \
  --output /owned/new-publication
```

### Failure and lifetime boundaries

Inputs must stay immutable during replacement. Work and output directories must
be fresh, disjoint from each other and from input roots. Symlink/reparse aliases,
missing/duplicate/wrong targets and conflicting bindings fail closed. Index
errors in any changed TAR reject the whole replacement. New data is validated
and synchronized in a private sibling stage, then moved atomically without
replacing an existing output. An interruption before that move exposes no final
input set. If a persistence error occurs after the move, a complete output may
already exist; retain and inspect it rather than overwrite it. POSIX directory
fsync barriers are used; Windows does not claim portable power-loss durability.

Failed private work/stages are retained for inspection. This command does not
resume, clean up, delete old data, download TARs, or update remote indexes. Use
fresh work/output paths for a retry. The immutable-input contract does not claim
protection against arbitrary concurrent external writes.

## Operational boundaries

Canary or representative measurements do not establish complete repository counts.
Report only the actual validated input scope; unvalidated coverage remains UNKNOWN.

`build-partition` processes the explicit local partition manifest. It does not
download a repository, schedule a repository-wide build, delete local inputs, or
change Git branches. Prepare matching source/wheel and worker before starting an
administrator-controlled build.
