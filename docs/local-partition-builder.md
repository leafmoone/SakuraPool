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

## Operational boundaries

Canary or representative measurements do not establish complete repository counts.
Report only the actual validated input scope; unvalidated coverage remains UNKNOWN.

`build-partition` processes the explicit local partition manifest. It does not
download a repository, schedule a repository-wide build, delete local inputs, or
change Git branches. Prepare matching source/wheel and worker before starting an
administrator-controlled build.
