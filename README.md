# SakuraPool

SakuraPool provides typed P1 reference-query contracts and a P2 local uncompressed
TAR/WebDataset index builder. No production data, downloads, image decoding,
deduplication, model execution, or P3 runtime is included.

## P1 reference layer (unchanged)

- Pinned Python package metadata and PyArrow schema conversion.
- Structured `QuerySpec` and `FilterSpec` validation.
- Schema/model registries with duplicate protection.
- In-memory `filter`, `select`, and `count` evaluation.

```console
python -m pip install -e '.[dev]'
sakurapool validate examples/query.json
sakurapool evaluate examples/query.json examples/rows.json
```

The evaluator preserves input order and applies filters before limits. It remains
a small-data correctness reference, not a production execution engine.
The historical P1 boundary and evidence are in [reports/P1.md](reports/P1.md).

## P2 local index

```console
sakurapool index scan --config examples/index-config.json --dataset synthetic --input /local/tars --output /local/index
```

Without a config, `--dataset local` selects the built-in local adapter. The previous
positional `index scan INPUT OUTPUT` syntax remains supported. Inputs and outputs
must be disjoint local directories. Config selects canonical source, image suffixes,
metadata tag/text fields, and the JSON byte limit. Pairing uses the full POSIX member
path without its final suffix, not basename-only matching; nested paths and dotted
keys work. Duplicate members, ambiguous images, unsafe paths, unsupported members,
missing/orphan metadata, and source mismatch are diagnosed without guessing.

Only uncompressed TAR is supported, including PAX long paths. Compression is rejected
by recognized extension and by `tarfile`'s `r:` format check. Members are never
extracted. `offset_data` and `size` refer to the original TAR's physical byte extent;
no image decoder is imported. Default indexing reads headers and metadata (the input
validator also hashes all archive bytes). Optional `--hash-images` additionally hashes
image member bytes; it does not decode them.

### Version 1 on-disk contract

P1's `DEFAULT_SCHEMA`, query, evaluator, schema registry, and model contracts remain
unchanged. The following four schemas are **new P2 contracts**, not replacements for
P1's `id/value/label` table. Package metadata and `__version__` remain aligned at 0.1.0.

| Table | Columns |
| --- | --- |
| objects | object_id, source, dataset, shard, image_path, json_path, offset_data:uint64, size:uint64, tags_state, tags:list<string>, hash_source, sha256 |
| samples | sample_id, object_id, text (nullable string) |
| annotations | object_id, key, value (canonical JSON string) |
| errors | source (relative shard), path, code, detail |

`tags_state` is missing/invalid/empty/known. Only lists of strings are valid tags;
missing and invalid are null, whereas empty is an actual empty list. A syntactically
valid 64-hex JSON sha256 is normalized and marked `declared:json.sha256`, never called
verified. Computed hashes are `computed:sha256`; absent/invalid hashes are `missing`.
Invalid declared hashes produce an error row while retaining the object.

Object identity is SHA256 of canonical JSON containing dataset, canonical source,
relative POSIX shard path, member path, offset, and size. It identifies a physical
occurrence, not content equivalence. Samples currently map one-to-one to objects.
Relocating an unchanged input directory preserves identity; changing the dataset,
source, member layout or relative shard path does not. Input content changes are
rejected for an existing output even if size and mtime are preserved.

### Publication and resume

Each shard publishes exactly four `<shard-id>.<table>.parquet` fragments (including
schemaful empty tables) and `<shard-id>.COMMIT`. Files are written in the output
directory to `.partial`, flushed and fsynced through **writable descriptors**, then
atomically renamed. COMMIT is published last using the same protocol and contains
fragment SHA256/byte/row counts, input size/mtime_ns/SHA256, and an INPUT contract hash.
POSIX additionally fsyncs the directory. Windows does not promise directory-fsync or
power-loss durability; process-crash recovery is tested. This is a **single-writer**
contract, not a concurrent writer service.

Resume verifies the complete input set and configuration, all committed fragment
hashes, schemas, and row counts. A corrupt commit fails closed. An uncommitted shard
is rebuilt; stale `.partial` files are removed. Readers must consume only fragments
listed by verified COMMIT markers, never glob all Parquet files during publication.
The abandoned pre-review per-object layout is intentionally not migrated: choose a
fresh output. `INPUT.json` locks configuration, including `--hash-images`.

Summary `objects` and `errors` are totals including resumed shards; `skipped` counts
shards, not objects. Timing fields are measurements and excluded from determinism
claims. CLI success returns JSON/0; row errors return summary JSON/1; fatal validation
or I/O failures return error JSON/2 (argparse syntax errors use argparse's stderr/2).
Fatal storage failures are not misreported as metadata/archive error rows.

## Offline validation and limitations

```console
python tools/verify_p2.py
```

This runs full/specialized pytest, ruff, diff-check, an offline no-isolation build,
a 10,000-pair synthetic benchmark, and isolated installed-wheel CLI smoke. Raw logs,
commands, exit codes, SHA256 and sizes are in `reports/P2/evidence-manifest.json`.
The current host has Python 3.14.5 / Arrow 25.0.1, **not pinned Arrow 18.1.0**.
The offline wheel smoke transplants only the installed Arrow dependency into a fresh
venv, does not expose source/global site-packages, and intentionally records a failing
`pip check`. This is not a successful pinned dependency installation certification.

See [reports/P2/report.md](reports/P2/report.md) for the implementation audit and
remaining review gates. Peak memory scales with the largest shard; this builder is
not a bounded-memory production streamer. Inputs are assumed quiescent during a scan;
validators detect observed changes, not adversarial racing writers.
