# P2 Reviewer Repair Report

## Identity

- BASE/main: `52358d6fca728d2bba12814490e0974a6907b218`
- Previous submission: `7f8574b0c832ab7bd2c47725edfc8d8eaabce6fb`
- Repair implementation commits:
  - `b6105f045b1bc740801981b80f4c4e91b5847fa8`
  - `791f588d209b5ed5e7045b90d93ea7a2fa307354`
- Final implementation tree: `791f588d209b5ed5e7045b90d93ea7a2fa307354`
- Final report commit is created after this file and is reported externally to avoid self-reference.

No `main`, P3, reset, rebase, amend, force push, or production data was touched. Root-owned `.gitignore` and untracked `agents.md` were not staged.

## Reviewer findings addressed

### 1. Content-bound ObjectId and RecordId

`object_id` is now `canonical-relative-path@sha256-<strong-input-sha256>`. The same TAR path scanned with different bytes produces different ObjectId and RecordId values. The commit marker validates the content-bound object identity. Regression test: `test_same_path_different_content_changes_object_and_record_ids`.

### 2. Annotation grouping and tags schema

The four-table schema retains `tags:list<struct<value,namespace,origin,category>>`. Metadata annotation rows carry the sample RecordKey and one `(record_id, namespace, origin)` document group. Runtime validation rejects annotations whose RecordKey is absent from samples. Regression coverage asserts uniqueness of annotation groups and schema roundtrip.

### 3. ObjectRef/RecordKey closure across four tables

Before Parquet publication, `_validate_references` checks that every samples, annotations, and errors ObjectRef exists in the objects table and every annotation RecordKey exists in samples. The test fixture verifies closure for all four tables.

### 4. Required metadata missing

An image with missing required JSON now produces both a `missing_metadata` error and a retained image sample. Its JSON path/offset/size and metadata/text are null, tags state is `missing`, and the physical image extent remains indexed. Fixture B asserts this behavior.

### 5. Bounded batch staging

Parquet conversion uses `_table_from_rows` and `BATCH_SIZE` chunks. The first crash checkpoint writes at most one batch, then continues in bounded chunks. The cross-batch regression fixture creates `2 * BATCH_SIZE + 3` samples and asserts every conversion batch is at most `BATCH_SIZE`.

### 6. Installed-wheel pytest isolation

The verifier removes `PYTHONPATH`, runs from the clean venv directory, explicitly imports `sakurapool`, asserts its module path is under `sys.prefix`, and asserts the repository `src` path is absent from `sys.path`. Pinned pytest then receives the repository tests by absolute path and exits 0. `wheel-import.log` and `pinned-pytest.log` are the direct evidence.

### 7. Artifact hashes use Git blob bytes

This report's evidence manifest is generated after staging and hashes each artifact through `git cat-file blob :path`, not through the working-tree file bytes. The manifest records both blob byte counts and SHA256 values. Raw command logs retain their exact bytes; their committed artifact hashes are separately recorded from the Git index.

## Verification

Final repair tree verification:

- `PYTHONPATH=src python -m pytest -q`: exit 0, 65 passed.
- `python -m ruff check .`: exit 0.
- `git diff --check`: exit 0.
- BASE-to-final implementation diff-check: exit 0.
- Isolated wheel build: exit 0.
- Synthetic benchmark and physical extent probe: exit 0.
- Python 3.12 pinned venv and normal dependency installation: exit 0.
- `pip check`: exit 0.
- Pinned installed-wheel pytest: exit 0, 65 passed.
- Pinned ruff: exit 0.
- Installed `sakura --version`, config validation and index scan: exit 0.
- Installed wheel import isolation assertion: exit 0.

The complete command manifest and raw logs are in `reports/P2-final/manifest.json` and `reports/P2-final/*.log`. `test-summary.json` contains the structured gate summary. The synthetic benchmark is not a production throughput claim.

## Known platform boundary

Windows does not provide the POSIX directory fsync guarantee; process-crash recovery is tested, but power-loss durability is not claimed. Inputs are assumed quiescent and scanning is single-writer. No image decoder is imported or invoked.

Final state after the report commit: `WAITING_REVIEW`; no merge is performed.
