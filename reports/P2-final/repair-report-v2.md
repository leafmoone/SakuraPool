# P2 Reviewer Repair Report v2

## Scope and commits

This is the report for the second reviewer-directed repair. The prior `repair-report.md`, `repair-summary.json`, and old `test-summary.json` remain historical artifacts and are not used for this report's test counts. This report corresponds to the final implementation tree below.

- BASE/main: `52358d6fca728d2bba12814490e0974a6907b218`
- Prior submission: `2dd9ca16bb4b0a1160270c70a7e28209c96460f9`
- Implementation commits:
  - `f24ab109c4ae6817027c895670a737930dd27aaa`
  - `dff6906bd1b861d14e5e9ca6ba9b80cbe2f5bc04`
- Final implementation tree before this report commit: `dff6906bd1b861d14e5e9ca6ba9b80cbe2f5bc04`
- Branch: `dev`; `main` unchanged.

No amend, reset, rebase, force push, merge, P3 work, production data, or root-owned document changes were performed.

## Reviewer requirements implemented

### Annotation contract

`ANNOTATIONS_SCHEMA` now contains the RecordKey fields plus exactly `namespace`, `origin`, `tags_state`, and `tags`. `tags` is `list<struct<value:string,category:string nullable>>`; it no longer contains namespace/origin inside each tag. The old annotation `category=document` and JSON `value` implementation was removed. Every image candidate emits one annotation group, including known, empty, missing, and malformed tag states. Tags from different declared origins remain separate groups. Regression tests inspect the explicit Arrow schema and actual tag value `[{"value":"a","category":"general"}]`, and assert the sample no longer has a `metadata` JSON field.

### Storage and physical metadata

Objects now carry `storage_id`, `backend`, `repo_type`, `dataset`, `object_path`, `object_size`, `object_version`, `validator_kind`, `validator_value`, and `scan_status`, in addition to compatibility identity fields. Samples carry nullable `image_format`, `width`, `height`, `has_alpha`, `hash_kind`, and `status` fields. PNG dimensions and alpha are read from the PNG header only; JPEG dimensions are read from SOF headers. The regression fixture writes an actual PNG header and asserts width `12`, height `9`, and alpha `true`.

Errors now include bounded `error_detail` (4096 bytes), `object_path`, `member`, `post_id`, and `record_id`, alongside source and error code. `ObjectRef` now validates backend, repository type, object version, validator kind, and validator strength. The strong SHA256 validator remains required for the current local TAR backend.

### Bounded scanning and staging

The scanner no longer calls `TarFile.getmembers()` and no longer creates a full-shard `rows` list. TAR members are consumed with `TarFile.next()` and stored in a temporary SQLite member spool. Candidate keys are read through a SQLite cursor; per-key candidate retrieval is limited to two records for ambiguity detection. Output rows are written into a temporary SQLite row spool. Four Parquet writers consume `fetchmany(BATCH_SIZE)` batches, each at most 1024 rows. The implementation therefore bounds both member/candidate staging and Parquet conversion rather than only chunking an already materialized list.

### Test process isolation

Source-tree tests may explicitly set `PYTHONPATH=src`. Crash and determinism child processes no longer unconditionally inject that path; when `SAKURAPOOL_EXPECT_INSTALLED=1`, they inherit the isolated environment and assert `sakurapool.__file__` is under `sys.prefix`. The wheel verifier removes `PYTHONPATH`, sets that marker, runs wheel-import validation, and runs pinned pytest from the clean venv directory. Source tests and installed-wheel evidence are separate.

### Machine-derived statistics

The verifier no longer uses pytest `-q` configuration. It runs `-rA --junitxml=...`, aggregates either a `<testsuite>` or `<testsuites>` XML root, and records machine-readable test statistics. Counts in this report are copied from the final JUnit files, not hand-entered.

## Final verification evidence

The final verifier invocation used an explicit 1200-second timeout and returned exit 0. The final `reports/P2-final/host-junit.xml` and `pinned-junit.xml` each report:

- `tests=70`
- `failures=0`
- `errors=0`
- `skipped=0`

Other final gates:

- host pytest command: exit 0
- pinned Python 3.12 pytest: exit 0
- host and pinned ruff: exit 0
- working-tree and BASE diff checks: exit 0
- wheel build/install: exit 0
- installed-wheel import isolation: exit 0
- pip check: exit 0
- installed version/config/index scan: exit 0
- synthetic benchmark and TAR extent probe: exit 0

The exact command argv, exit code, byte count, and SHA256 for raw logs are in the final `manifest.json`. The artifact manifest uses Git index blob bytes via `git cat-file blob :path`; it is regenerated after staging the report files. The benchmark uses synthetic data and is not a production throughput claim.

## Limitations and final state

The implementation supports uncompressed TAR and does not decode image pixels. Windows power-loss fsync guarantees are not claimed. Inputs are assumed quiescent and indexing is single-writer. The final state after the report commit is `WAITING_REVIEW`; no merge or P3 start is performed.
