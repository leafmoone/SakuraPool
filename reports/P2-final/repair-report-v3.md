# P2 Emergency Repair Report v3

## Final identity

- BASE/main: `52358d6fca728d2bba12814490e0974a6907b218`
- Prior reviewer submission: `2e1ec223f672a66fb998ef8a6330f9bc12123e16`
- Implementation commits:
  - `7622bd0882f8b02b7bfb026500ab4164ad01e531`
  - `6476956a8a8ddaaf2d99fa1e079c9d6df59a4ff0`
- Final implementation tree before this report: `6476956a8a8ddaaf2d99fa1e079c9d6df59a4ff0`
- Branch: `dev`; `main` unchanged.

This was an emergency reviewer-directed repair. No authorization wait, merge, amend, reset, rebase, force push, P3 work, production data access, or root-owned document modification occurred.

## Defects fixed with regression evidence

### Complete four-table publication

`_write_fragments` now writes the first batch and then drains the same `fetchmany(BATCH_SIZE)` iterator for all four tables, not only samples. After each Parquet writer is closed and fsynced, the implementation reopens the fragment, checks the exact Arrow schema and actual `num_rows` against the SQLite spool count, and refuses all rename/marker publication on mismatch.

Regression coverage includes every table at counts `0`, `1`, `1023`, `1024`, `1025`, and `2051`. Samples and annotations are read back at every boundary. Errors are read back at `1025` and `2051`, and each test immediately rescans the output to verify valid COMMIT resume. The earlier `remaining=iter((first,))` truncation path is gone.

### Image reads and dimensions

Default scans no longer call `_read_member` for image payloads. Width, height, and alpha are nullable metadata fields read from the JSON object; metadata values `width=99`, `height=88`, `has_alpha=false` are asserted to win over a PNG header containing `12x9 alpha=true`. `hash_images=True` uses `_hash_member`, reading the image in 1 MiB chunks rather than materializing the entire payload.

Offset second-read verification is now opt-in through `scan(..., verify_offsets=True)`. The unstable-file regression invokes that explicit mode. Ordinary scans perform one metadata read and do not impose the fixture-only double-read behavior.

### TAR and candidate staging

The scanner does not call `TarFile.getmembers()`. It consumes members with `TarFile.next()` into a disk SQLite member spool and iterates keys with a SQLite cursor. Candidate lookup is limited to two image and two metadata members, retaining the original names and extents for ambiguity errors. The old full-shard `seen` identity dictionary was removed; SQLite has a primary-key identity table and raises `IdentityConflictError` on duplicate RecordId.

The row spool remains disk-backed and Parquet conversion uses batches no larger than 1024. The cross-batch tests verify actual Parquet row counts rather than only conversion batch sizes.

### ObjectRef and error integrity

`ObjectRef` now requires uint64 `object_size`, a non-empty strong object version, and a strong validator; object version must match the SHA256 validator. `MemberRef` validates uint64 offset/size and rejects extents beyond `object_size`.

Errors are UTF-8 truncated to at most 4096 bytes without splitting a code point. A 5000-character Chinese error regression verifies the byte limit and UTF-8 validity. Known errors receive `post_id`; missing-required-metadata errors also receive the known `record_id` after identity registration. Metadata size limits remain adapter-configurable and oversized/invalid metadata is isolated as an error rather than aborting the shard.

### Timing and performance

`header_seconds` now starts before TAR member iteration and is updated after the complete member scan and SQLite member staging, so it includes actual header/member traversal and staging. The final synthetic benchmark is explicitly synthetic: 10000 pairs in one TAR, 10000 samples, 0 errors, 111.862 seconds total, 0.420 seconds header/staging, 0.148 seconds JSON, 0.301 seconds Parquet, peak process RSS 99,819,520 bytes on Windows 11/Python 3.14/PyArrow 25.0.1. This is not a production throughput claim.

## Final verification

The final verifier was run with an explicit 1500-second timeout and returned exit 0. Final JUnit XML is machine-derived:

- `host-junit.xml`: `tests=80`, `failures=0`, `errors=0`
- `pinned-junit.xml`: `tests=80`, `failures=0`, `errors=0`

All final gates exited 0:

- host pytest and pinned Python 3.12 pytest
- host and pinned ruff
- working-tree diff and BASE diff checks
- wheel build and normal dependency installation
- installed-wheel import isolation and `pip check`
- installed CLI version, config validation, and scan smoke
- synthetic benchmark and TAR extent probe

The final `manifest.json` records each command argv, exit code, output byte count, and SHA256. The v3 evidence manifest is calculated from Git index blob bytes with `git cat-file blob :path`.

The earlier intermediate benchmark timeout was not reported as success; it exposed O(n²) error-spool patching, which was removed before the final implementation commit and final verification. The final benchmark log is exit 0.

## Final state

After the report commit, `dev` is `WAITING_REVIEW`; `main` remains at the BASE SHA. Root-owned `.gitignore` and `agents.md` are untouched and unstaged. No P3 work has started.
