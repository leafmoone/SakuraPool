# INDEX_PRODUCTION_HOTFIX_INTEGRATION WAITING_REVIEW

## Scope and code binding

Worktree `D:/SakuraTool/SakuraPool-index-hotfix-integration`; branch `integrate-index-production-hotfix-20261001`. Current direct37-section order authorizes this independent integration, not dev/main updates.

- Actual BASE/origin dev: `de5b311bf7116e95ec4084f577c7c7cef5147cfa`, tree `892128ea2369a8b06d3ab0a5fcb08b3a6108b187`.
- Fixed candidate: `f89bf7d2baaa6c6823ead2047b71d66219aacd13`, tree `424f1eef0444d046db30aa198c8ccd85fc276013`.
- Candidate base: `76ccce256ae08c5cd6a6569f4a78c874f84076e3`.
- START=END implementation: `b17357430356462776650de474c41ef496735dbf`, tree `5aeedacc4ab4d722b2fae70f066a277eac1b6d4f`.
- Exactly one no-ff merge, parents BASE and fixed candidate, full candidate history preserved. No other implementation commits.
- SUBMISSION: subsequent evidence-only commit containing this file; actual SHA/tree printed in continuous final delivery after commit/push, avoiding self-reference. Implementation→submission product/tests diff zero is required and independently checked.
- Main: `edc72fbaaddc4dc8f9865ddfa576b737c7b15f5a`, unchanged.

New path/branch were absent, created from actual fetched origin/dev. Fixed candidate SHA/tree independently verified; no drift. First merge invocation failed before merge because committer identity missing; used per-command identity consistent with repository existing author, no global config mutation. Only integration pushed; all four remote refs checked after pushes.

Protected old worktrees/index machine untouched. dev-finalize C2 dirty remains modified production.py plus untracked reports/R2C1B, reports/R2C2, test_r2c2_binding.py, not stashed/cleaned/copied/staged/certified. C2 stopped. Its20pass1fail/STREAM_MEMORY draft fix is not integration evidence.

## Semantic merge and contract

Candidate seven product files and five tests retained: indexer/metadata/runtime compiler+inventory/package/remote_index/rust_index; empty_fragments/empty_package/nested_metadata/nested_null_indexer/staged_lookup. Three conflicts resolved locally, not whole-file ours/theirs:

1. inventory: footer-only guard plus dev configurable batch_size; upstream SHA/size/schema/declared counts stay strict.
2. remote_index: preserve existing cursor.close→db.close/comment without invented new behavior; add JSON_OFFSET_INDEX_SQL after table creation.
3. rust_index: JSON offset partial index plus dev callable producer/members_extents/boundedSQLite/privatefreshOFF/close-fsync-hash-marker/resource behavior.

Dev _SpoolScope(limits), caps/journals/boundedreaders/R2C1/trueRemote sidecars+noncollect preserved. Product production.py, budget.py, Rust and dependency contracts unchanged from BASE. Added tests independently cover cleanup retry and callable production stage with both indexes.

NULL_CAPTION=PASS: optional captions/nl2 absent/null→textNone; empty/string retained; no nl3 fallback or literal null string; invalid nonnull rejects.
NULL_DIMENSIONS=PASS: independently absent/null→None, not zero or pixeldecode;0/uint32max valid;bool/string/float/list/dict/negative/>=2**32 reject.
NULL_TAG_FIELDS=PASS: tags absent/null→missing; present object with all categories absent/null/empty→empty; categories missing/null contribute no tags.
NULL_TAG_ELEMENTS=PASS: null ignored, blue/null/hair→blue/hair; no stringified null, siblings retained.
INVALID_NON_NULL_TYPES_REJECTED=PASS; required schema_version/id/source/source.dataset/image/image.format absent/null remain requiredmissing rejection.
P2=4/P3=2/nested_json_v1 unchanged; existing nullable schema correction, not migration. TagKey/categories/ObjectId/RecordId/workerNDJSON retained.

## Specialized evidence

P2_P3_NULL_ROUNDTRIP=PASS: synthetic undecodable images, full missing/null/0/known dimensions matrix in P2v4/inventory then P3v2 query/recordids/image+JSON locations; bad nonnull dimensions stored as metadata_invalid and not accepted samples. P2 inputs unchanged by compilation.

ZERO_ROW_PARQUET=PASS: exact-schema0rows/0groups and1emptygroup legal; hash/declaredrows mismatches still rejected; committed footer-only inventory/compiler/runtime succeeds with/without samples.
EMPTY_PACKAGE=PASS: zero samples legal, orphan audit still rejects; chain/bindings/inventory integrity preserved.
JSON_EXTENT_INDEX=PASS: EXPLAIN reports SEARCH members USING INDEX members_json_offset, no SCANmembers.64/8192 JSON members plus paired images, progress handler interval1 shows VMsteps<count*64. Split reads/reseek/invalid offsets/readonly contract preserved. Production callable stage has BOTH members_json_offset and members_extents, no TEMP B-TREE for offset/name order; OFF checked in actual producer connection, not assumed persisted on reopen.

CLEANUP_EXTRA_FIX=PASS separately from metadata. Normalclose beforeunlink; firstclosefault preserves actual path/resourceinscope/pendinghandle; controlled secondclose/retry deletespath/discardsresource/clearshandle. Primary scanRuntimeError survives with cleanupnotes and exposedretryhandle. Productionlimitedscope and unrelateduserfile safety covered. No globalcleanup/DBclose/ownership/crossprocess/startupscavenger/quota redesign. Fault evidence source targeted/full; wheel normalcleanup only.

LOCAL_BUILDER=PASS; TRUE_REMOTE_STREAM=PASS (syntheticloopback, not ModelScope); R2C1_RESOURCE=PASS; PACKAGE_REGRESSION=PASS.

## Commands, outcomes and failure history

Sole keeper `D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe`, Python3.13.15, temporarily editable-bound to integration; actual -I import verified. PYTHONPATH unset; explicit worker `D:/SakuraTool/SakuraPool-Fix3-20260930a/rust-target/release/sakurapool-worker.exe`, sole target same rust-target; Rust/cargo1.98.1 WindowsGNU. Relevant processes checked exclusive before tests.

- Targeted `python -m pytest tests/test_hotfix_integration.py tests/test_nested_metadata.py tests/test_nested_null_indexer.py tests/test_empty_fragments.py tests/test_empty_package.py tests/test_staged_lookup.py tests/test_indexer.py -q -p no:cacheprovider -x`:234passed/0skipped/0failed,34.01s,exit0. First55pass1fail0.59s was newtest nonexistent _journal_off argument; corrected formal callableautoOFF+producerconnection assertion, no product fix/index assertion weakening.
- Wider modules local_partition_builder; runtime/remediation/differential/multicategory; integration_index_builder_r2; r2_production; r2c1_errors/resources/spools/stage_errors; p4_package/two_hop/transport using same pytest flags:355passed/3skipped/0failed/1warning,2579.77s,exit0. Previous command absent test_r1_runtime_e2e.py exit4/no tests, corrected actual list.
- Warning in128MiBRemote observer _disk_usage raced fixture's normal transferdir deletion: Win3 in sampler. Damaged sampled evidence NOT accepted from pytest exit0; no external-process attribution. Minimal integration-only test_r2c1_resources.py repair: threading.Lock only around fixture disk measurement and actual _delete_owned. Network/scan/stage never locked; errors collected to mainassert; stop+joinfinally; no productbudget/ledger changes.
- `python -m pytest tests/test_r2c1_resources.py::test_large_true_stream_no_tar_disk_and_bounded_rss -q -s -p no:cacheprovider -W error::pytest.PytestUnhandledThreadExceptionWarning`:4passed/0failed/0warnings,115.66s,exit0.64/128MiB×Remote/Download independently revalidated.
- Frozen full `python -m pytest tests -q -p no:cacheprovider`:814passed/3skipped/0failed/no warnings,pytest2322.29s,measuredwall2323s,exit0. Codecommit b173574…/tree5aeedacc… and actualintegrationimport. full-python.log rawbytes retained. Three Windowssymlinkprivilege skips local_partition_builder:337,p4_package:179,runtime_remediation:787. Explicitworker tests ran, not silentlyskipped.
- `python -m ruff check .`, `git diff --check origin/dev...HEAD`, workingdiffcheck:PASS/exit0.
- Exact Rust commands allPASS/exit0 from actualintegration:
  - `cargo fmt --manifest-path rust/Cargo.toml --all -- --check`
  - `cargo test --manifest-path rust/Cargo.toml --locked --all-targets`:61passed/0failed.
  - `cargo clippy --manifest-path rust/Cargo.toml --locked --all-targets -- -D warnings`
  - `cargo build --manifest-path rust/Cargo.toml --locked --release --bin sakurapool-worker`

FRESH_WHEEL=PASS: newintegrationwheel outside repo, standalone temporaryenv installed noneditable wheel+declareddependencies, externalcwd,-I,noPYTHONPATH,currentexplicitreleaseworker. `wheel_verify.py --repository <integration> --wheel <newwheel> --code-commit b17357430356462776650de474c41ef496735dbf`:121passed/0skipped/0failed,277.37s,build/install/verifywall309s,exit0. Covers nestednull/P2P3/footeronly/packageorphannegative/8192seek/twoindexes/normalcleanup/local+Remote+Download.
35 source-vs-packed files compared after CRLF→LF normalization; installed-vs-packed are raw-byte equal. Worker provenance from outer freshreleasebuild, no extra workerhash. Wheel does NOT claim faultmonkeypatch verification.
First wheelbootstrap--system-site-packages inherited BASE interpreter not keeper packages, missingpytest before anytests:exit1/wall12s. Corrected standaloneenv installed actualdependencies. Raw initialtraceback preserved in fresh-wheel.log; correctedmarker buffered and appears AFTERPASS, not rewritten. Success exit0/wall309 comes from outertool, not fake rawfooter. Temporaryenv/wheel artifacts recycled. Solekeeper restored to dev-finalize with actual-I import verified; dirtyC2source is not integrationtree certification.

## Performance/safety/stop

Synthetic TAR67112960/134225920B, revalidated Remote sampled job growth36864B each/WHOLE_TAR_SPOOL=NO; Download67149824/134262784B/WHOLE_TAR_SPOOL=YES. WorkerRSS Remote23138304/23228416B, Download22827008B; reservation134217728B.5ms observations with narrowremovalLock are NOT absolutepeak, Python/nativeglobalRSS theorem, productionthroughput or pilotbenchmark. Existing admission/conservativephysical+futureaccounting unchanged.

REAL_MODELSCOPE_REQUESTS=0;REAL_DATA_REQUESTS=0;nocredentialsread;noindexmachineoperations/datacopy;no224TAR277GiBpilotrerun. Git/dependencydevelopmentoperations are not datarequests. Historical719pass1skip/Rust58/pilot224TAR851288samples0currenterrors/DANBOORU1077/UNKNOWNDIM36/GAMECG690154 are mixedversionreferences, NOT this-tree certification.

INDEX_FORMAT_CHANGED=NO;P2=4;P3=2;DEV_UPDATED=NO;MAIN_UPDATED=NO;FULL_INDEX_CONTINUE_AUTHORIZED=NO;PART_000001_STARTED=NO. Onlyintegration submitted. Stop WAITING_REVIEW; no automaticdev/mainmerge, C2resume or fullindex continuation.
