# SakuraPool P2 final submission report

## Revision identity and scope

- BASE/main: `52358d6fca728d2bba12814490e0974a6907b218`
- Final IMPLEMENTATION: `9c5c4e084b3cd7b08718204e0536a4129418d9ef`
- SUBMISSION: the report-only commit immediately following implementation; its full SHA is reported externally after commit to avoid self-reference.
- Branch: development and validation were performed only on `dev`; `main` remained at BASE.
- Boundary: no P3, merge, production data, production TAR, image decode, download/runtime/dedup implementation, reset, rebase, force push, or amend.

The P2 contract is documented in `README.md` under **Installation and CLI**, **P1_CONTRACT_CHANGE**, **Four Arrow tables**, **Registry and pairing**, **Publication, resume, and counts**, and **Verification**. The executable implementation is in `src/sakurapool/indexer.py`, `records.py`, `registry.py`, and `cli.py`.

## Delivered behavior

- Recursively discovers local `*.tar` inputs or accepts a single TAR.
- Rejects compressed archives by extension, magic, and uncompressed TAR parsing with `UnsupportedArchiveError`.
- Uses `TarInfo.offset_data` and `size`; metadata reads perform a real seek/read and an immediate second seek/read equality verification. Independent extent probes seek the source TAR and compare against `tarfile.extractfile` and the original fixture bytes.
- Uses explicit dataset adapters and source registry data. Full logical stems are retained; role prefixes allow `images/...` and `meta/...` pairing without path guessing.
- Handles missing image/metadata, ambiguous image/metadata, subdirectory roles, source mismatch, invalid metadata, invalid post IDs, unsupported members, and identity conflicts with stable error vocabulary.
- Uses stable `RecordKey(dataset_id, object_id, sample_path)` identity and BLAKE2b-16 record IDs independent of physical offsets.
- Preserves P1 query/model/schema APIs while adding four P2 Arrow/Parquet tables: objects, samples, annotations, and errors.
- Stores image and JSON offsets/sizes as `uint64`; nullable JSON extents and metadata remain null when absent. A `>2^63` Parquet roundtrip test covers values above the 4 GiB boundary.
- Implements `tags_state` values `known`, `empty`, `missing`, and `invalid`; tag rows retain namespace, origin, and category. Hash provenance is `declared:json.sha256`, `computed:sha256`, or `missing`.
- Does not import or invoke an image decoder. Image bytes are read separately only when `--hash-images` is requested; whole-TAR strong validation is disclosed.
- Writes immutable per-TAR-object fragments through `.partial`, file flush/fsync, close, atomic rename, and last-published COMMIT marker. POSIX directory fsync is used; Windows directory fsync/power-loss durability is not claimed.
- Resume verifies contract, validators, fragment names, SHA256, bytes, schema, and row counts before skip. Owned partials are cleaned; unrelated user partials are preserved. Corrupt committed output is rejected rather than silently rebuilt.
- Detects changed content even when size/mtime are restored, inventory/config changes, and changes during scanning.
- Provides `sakura index scan` JSON summaries. Clean scans return 0, row errors return 1, and fatal/argument/archive failures return 2.

## Fixture and recovery results

All results below are asserted by the final 66-test suite in both the host diagnostic environment and the pinned installed-wheel environment.

| Fixture | Input condition | Result |
|---|---|---|
| A | two valid image+JSON pairs | 1 object, 2 samples, no errors |
| B | image without required metadata | `missing_metadata` |
| C | metadata without image | `missing_image` |
| D | duplicate image roles for one key | `ambiguous_image_member` |
| E | invalid JSON | `metadata_invalid` |
| F | `images/7.jpg` + `meta/7.json` with adapter prefixes | paired sample |
| G | declared metadata source differs from registry source | `source_mismatch` |
| H | README/manifest/auxiliary/hidden members | ignored, no sample/error |

Crash checkpoints A-E are covered by exception injection and real subprocess `os._exit(73)`:

- A: partial samples fragment before complete footer.
- B: all four Parquet partials closed/fsynced, no rename.
- C: first final renamed.
- D: all finals renamed, no marker.
- E: marker published; resume validates and skips.

A-D rebuild successfully; E validates committed hashes and skips. Storage-fsync failure, readable Parquet tampering, all fragment/marker corruption cases, input changes, and identity collision are also covered.

## Final commands and exits

`reports/P2-final/manifest.json` records exact argv, cwd, exit code, byte count, and SHA256 for every unified gate. All 18 unified entries exited 0:

| Gate | Result |
|---|---|
| implementation git inventory | exit 0 |
| host environment capture | exit 0 |
| host `pytest -q -rA` | exit 0, 66 passed |
| host ruff | exit 0 |
| worktree `git diff --check` | exit 0 |
| BASE..implementation diff-check | exit 0 |
| isolated wheel build | exit 0 |
| 10k synthetic benchmark | exit 0 |
| physical extent probe | exit 0 |
| Python 3.12 clean venv | exit 0 |
| pinned normal dependency install | exit 0 |
| `pip check` | exit 0, no broken requirements |
| pinned installed-wheel pytest | exit 0, 66 passed |
| pinned ruff 0.9.2 | exit 0 |
| installed `sakura --version` | exit 0 |
| installed config validation | exit 0 |
| installed `sakura index scan` | exit 0 |

Additional installed-wheel failure semantics:

- missing-metadata row scan: exit 1 with JSON summary containing `errors: 1`.
- compressed TAR scan: exit 2 with JSON `unsupported_archive` error.
- pinned Python 3.12 build: exit 0.

The first long network install attempt timed out while downloading pinned PyArrow/Ruff. This was resolved without changing pins or using `--no-deps`: the official PyPI wheels were downloaded with resume, checked against PyPI JSON SHA256, and passed to normal `uv pip install`. `wheelhouse-provenance.log` records URLs, sizes, expected hashes, actual hashes, and matches. The verifier independently checks these hashes before installation.

## Environments

Host diagnostic run:

- Python 3.14.5
- PyArrow 25.0.1
- Windows 11

Pinned certification run:

- Python 3.12.13
- PyArrow 18.1.0
- pytest 8.3.4
- ruff 0.9.2
- build 1.2.2.post1
- setuptools build backend 75.6.0

The pinned wheel installed with normal dependency resolution and `pip check` passed. The project does not claim Python 3.14 support.

## Physical seek evidence

`extents.log` records six successful image/JSON extent checks. Representative values:

- `1.jpg`: offset 512, length 3, equality true.
- `1.json`: offset 1536, length 14, equality true.
- `images/7.jpg`: offset 4608, length 5, equality true.
- `meta/7.json`: offset 5632, length 2, equality true.

Each extent compares direct source-file seek bytes, tarfile extraction, and original fixture payload. A separate regression test forces the second read to differ and confirms `offset verification failed`.

## Synthetic benchmark

The benchmark is explicitly synthetic: one generated TAR shard containing 10,000 image/JSON pairs. Final observed host values:

- objects: 1
- samples: 10,000
- errors: 0
- header: 0.22845950000919402 s
- JSON: 0.059667797409929335 s
- Parquet: 0.06992409995291382 s
- total: 0.7340571999084204 s
- peak RSS: 90,103,808 bytes

RSS includes imports and fixture generation. Total excludes fixture generation but includes whole-TAR SHA256 validation. No production throughput or bounded-memory claim is made.

## Evidence

- Unified command manifest: `reports/P2-final/manifest.json`
- Artifact/hash manifest: `reports/P2-final/evidence-manifest.json`
- Test summary: `reports/P2-final/test-summary.json`
- Raw logs: every `reports/P2-final/*.log`
- Earlier failed/repair evidence remains preserved under `reports/P2`, `reports/P2-revision`, and `reports/P2-revision-final`; it is not presented as final certification.

No available legal tool or configuration surface exposes the actual `reasoning_effort` setting for this assistant or the earlier worker. It is therefore recorded as unavailable and is not guessed.

Final state after the following report commit is `WAITING_REVIEW`; no merge is authorized or performed.
