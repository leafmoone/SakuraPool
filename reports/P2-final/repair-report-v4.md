# P2 Reviewer Repair Report v4

## Final identity

- BASE/main: `52358d6fca728d2bba12814490e0974a6907b218`
- Prior submission: `d159c6b2cbfe78693bb3e933f37955f2611d09d7`
- Implementation commits:
  - `b6bb9a6d9eea8048ab9d1c0251e44bf7a21a5056`
  - `9f70c4c2e7ca73bacce1ab75d2ba064556e1defb`
- Final implementation tree before this report: `9f70c4c2e7ca73bacce1ab75d2ba064556e1defb`
- Branch: `dev`; `main` unchanged.

No amend, reset, rebase, force push, merge, P3 work, production data, or root-owned document changes were performed.

## Reviewer-directed fixes

### TarFile cache and member staging

The scanner uses `_next_member()`, which calls `TarFile.next()` and immediately clears Python 3.12's `TarFile.members` cache. All 2051-member cache regression observations assert a peak retained `TarInfo` cache of at most 1. Members are retained only as scalar name/suffix/offset/size fields in the SQLite spool. Candidate retrieval uses the indexed `(key, is_image, name)` index and `LIMIT 2` for ambiguity detection.

### Parquet verification and complete publication

All four writers drain every `fetchmany(BATCH_SIZE)` batch. After writer close and fsync, `_verify_fragment` uses `ParquetFile.schema_arrow` and `metadata.num_rows`, then iterates row groups in batches of at most 1024 to compare actual rows. Empty files return from metadata without invoking the PyArrow 18.1 zero-row-group iterator bug. Resume validation uses the same bounded verifier. It never calls `pq.read_table`; a regression monkeypatch rejects any such call during immediate resume.

The boundary matrix reads samples and annotations for `0/1/1023/1024/1025/2051`, and errors for `1025/2051`, followed by immediate resume. A mismatched schema or row count prevents rename and COMMIT publication.

### Indexed SQL and elimination of quadratic rewrites

`members_lookup(key, is_image, name)` is created and the test runs `EXPLAIN QUERY PLAN`; the lookup plan contains `members_lookup` and no table scan. The row spool has a structured `record_id` column and `rows_record_lookup(kind, record_id)` index. Annotation-to-sample closure uses an indexed SQL left join; it does not scan JSON payloads with `LIKE`. The test asserts the join plan uses `rows_record_lookup` and does not mention payload.

The prior per-missing-metadata loop that scanned all errors and issued `UPDATE` statements was removed. `error()` now accepts post_id and record_id, and known source mismatch, malformed/oversized metadata, invalid declared hash, invalid text, and missing metadata errors are created with their known identity fields. Only genuinely unknown member-level errors retain null identity fields.

### Growth evidence

The synthetic growth tool was run for 1025, 2051, and 4102 pairs after the implementation changes:

```text
1025: 0.203405 s, samples=1025, errors=0
2051: 0.333487 s, samples=2051, errors=0
4102: 0.628787 s, samples=4102, errors=0
```

The main synthetic benchmark was also rerun after the final implementation tree: 10000 samples, 0 errors, total 1.424028 s, header/staging 0.435877 s, JSON 0.070378 s, Parquet 0.175292 s, peak RSS 84,422,656 bytes. These are synthetic measurements, not production performance claims.

### Previously accepted requirements retained

Content-bound ObjectId/RecordId, required-metadata sample retention, explicit annotation tags schema/grouping, image metadata priority, UTF-8 error limit, ObjectRef bounds, wheel import isolation, and Git blob artifact hashing remain in the final tree and were included in the full verification.

## Final verification

The final verifier was run against the final implementation tree with an explicit 1500-second timeout and returned exit 0. Machine-derived JUnit statistics:

- `host-junit.xml`: `tests=83`, `failures=0`, `errors=0`
- `pinned-junit.xml`: `tests=83`, `failures=0`, `errors=0`

All final gates exited 0:

- host and pinned pytest
- host and pinned ruff
- worktree diff check excluding preserved raw report logs
- BASE-to-HEAD implementation diff check
- wheel build/install
- installed-wheel import isolation
- pip check
- installed CLI version/config/scan
- synthetic benchmark
- TAR extent probe

The raw command manifest records argv, exit code, byte count, and SHA256. The v4 evidence manifest is generated from Git index blob bytes using `git cat-file blob :path`.

## Final state

After the report commit, `dev` remains `WAITING_REVIEW`; `main` is unchanged and P3 has not started. Root-owned `.gitignore` and `agents.md` are untouched and unstaged.
