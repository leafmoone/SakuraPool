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

## Review gate

Canaries are not production indices. Seven-source representative measurements
are estimates, not measured full-repository sizes. Known full sample counts remain
UNKNOWN unless supported by authorized evidence. Do not scan the repository just
to manufacture a denominator.

This index machine delivers a candidate branch only. A full build requires
external review, integration by the primary developer, an approved SHA and fresh
wheel/worker rebuild. No automatic dev/main merge, full build, R2 or P5 is part of
INDEX-BUILDER-PREP.
