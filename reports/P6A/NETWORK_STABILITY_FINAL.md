# P6-A NETWORK STABILITY / UNKNOWN RECOVERY — final evidence

## Scope/status

Only D:/SakuraTool/SakuraPool-p6-origin-stability, work-p6-origin-stability, base c2db79263e4ad8927ad42d0b7516ad81bd7af0b0. Initial sole candidate f7e7e933a8858573e88b9095eee3a7420c29df68 required three changes. Root conveyed sole PASS on corrected implementation tree 4fc700eaa2c575d5cf22ebaff0e86b6cf371a910. Documentation delta awaits limited factual review; no commit yet; intermediate commits NONE. Final commit/remote SHA supplied after gate, not invented here.

CONSERVATIVE_ACCOUNTING=PASS_OFFLINE; TRUE_UNKNOWN_PRESERVED=PASS_OFFLINE. COLD/PERSISTENT=NOT_RUN_DISCOVERY_BLOCKED original and post-fix; extra500 NOT_RUN. REAL_FINAL_TASK=BLOCKED_DEPENDENCY; no acceptance/resume/export. NEW_PENDING_FROM_CONFIRMED_TRANSIENT=0_IN_OFFLINE_TESTS / REAL_NOT_EXERCISED. Discovery pending0 is separate, not real transient recovery evidence.

## Contract

Rust safe timeout/connect/request/transport predicate classification preserves origin/CDN phases and excludes raw URL/body/credentials. Python allowlists synchronize codes. Pool policy unchanged; discovery establishes no pool cause.

Only terminal envelope plus owned cleanup and completed ledger gates can mint finalized evidence. Actual admitted maximum minus observed bytes is charged; attempts/chunks not charged twice. basis=CONSERVATIVE_MAX_CHARGE, actual_consumption=UNKNOWN, accounted=CONSERVATIVE_MAX. No delivery receipt invented. No envelope remains UNKNOWN even after process exit. Persistent terminal request can finish with resident lease retained; process liveness alone cannot prove termination.

Candidate used before mutation. Consume-success-then-raise/partial settlement preserve actual charges and pending without refund, rollback or second settlement. Transfer and capability/proof uncertainty latch channel; later requests cannot wash it. Outer published/cleanup/output/secondary gates have real-token negative tests and positive control.

Confirmed unpublished failures require explicit resume. Basis INSERT and attempt update share TaskDB transaction; conflicting key rolls back without overwrite. Persistence failure yields public UNKNOWN/nonrecoverable and removes basis. No old schema migration. Soak failure rows whitelist the three basis fields.

## Commands/results, separate runs not accumulated

P=D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe. Tests used cwd new worktree, PYTHONPATH=new worktree/src, TMP/TEMP/TMPDIR=D:/SakuraTool/SakuraPool-P6-origin-soak; `P -m pytest <files> -q -p no:cacheprovider --basetemp=<dedicated test directory>`.

|Job|Scope|Exit|Statistics/reason|
|---|---|---:|---|
|121631ce|network_finalization initial|1|8 failed/4 passed/2.26s; missing fixture send_raw, no product verdict|
|08e976a2|fixture correction|1|8 failed/4 passed/2.55s; legacy/workspace inspect and crash exception expectation|
|8d60a21f|network_finalization|0|12 passed/2.49s|
|e8231d5f|network_finalization+task_recovery|1|13 passed/1 failed; duration NOT_RECORDED; real missing basis INSERT|
|0d545949|task_recovery+network_workspace|1|2 failed/8.55s; same real bug plus fixture profile mismatch|
|d869b96b|network_finalization+task_recovery+network_workspace|0|16 passed/17.58s|
|b4ec9cec|network_task_persistence|0|1 passed/7.04s|
|ddf632ec|four network files|1|18 passed/1 failed/29.36s; fixture bypassed enable_persistent|
|fbe48793|four network files|0|19 passed/34.75s; basetemp final-core-persistent-init|
|13df9a16|existing eight direct consumers|0|210 passed/430.21s; basetemp bounded-direct-consumers|
|8e205b3a|post-sole network_finalization|0|20 passed/5.07s; basetemp sole-three-fixes|

Existing eight files: tests/test_r2_production.py tests/test_r1_bridge.py tests/test_p4_transport.py tests/test_p4_budget.py tests/test_task_runner.py tests/test_task_store.py tests/test_p6_followup_core.py tests/test_task_metadata.py. SAKURAPOOL_RUST_WORKER selected dedicated current worker. Four new files are tests/test_network_finalization.py tests/test_network_task_recovery.py tests/test_network_task_persistence.py tests/test_network_workspace.py. No full matrix/Linux/wheel/index run.

Early build/environment trajectory (retained failures, no rerun to invent evidence): 73242d0d build exit101 missing dlltool; 91d1 (full jobid NOT_RECORDED) MSVC attempt exit101 because PATH selected GNU link rather than MSVC linker. After locating C:/msys64/ucrt64/bin/dlltool.exe,3bbac81e explicit GNU PATH/dedicated target build exit0/26.20s, no test statistics; predicate/test refactoring occurred during that build, so this artifact is NOT frozen baseline-worker evidence, and its tree is NOT_RECORDED. 69769c5a chain exit1 at git diff quiet after cargo fmt changed two http.rs formatting sites; preceding Ruff exit0, build/tests NOT_EXECUTED. The two self-generated formatting changes were precisely reverted. Initial tool-Python smoke failed its pyarrow prerequisite (job/exit NOT_RECORDED); subsequent designated keeper identity and lifecycle smoke each exit0.

Rust target D:/SakuraTool/SakuraPool-p6-origin-stability-target:
- d2c1cad8 exit0 at frozen classification tree d993a102b365750ecfcb8510ccaaf26f4c336ccc: build1.72s, predicate1 test covering16 combinations passed/0 failed/0.02s; other tests filtered, not run.
- `cargo test --manifest-path rust/Cargo.toml --target-dir <target> production:: -- --nocapture`: exit0,4 passed/0 failed/0.00s; other suites filtered not coverage.
- `cargo test --manifest-path rust/Cargo.toml --target-dir <target> --bin sakurapool-worker --test worker_protocol`:1b4d7329 exit0; worker8 passed/0.05s, protocol12 passed/0.09s,0 failed.

Scoped changed product/four tests Ruff exit0 before freeze. After sole delta `P -m ruff check tests/test_network_finalization.py reports/P6A/origin_soak.py src/sakurapool/storage/production.py` exit0. Diff/cached checks and worktree-index equality exit0. Doc-only closure does not rerun code matrix.

## Real diagnostics

Fixed repo leafmoone/webdataset_danbooru_v3, pin73306f1dc5459238710f477b376c36da997d020c, object anime_pictures/t0992-001.tar. Publication D:/SakuraTool/SakuraPool-P5D-20261004T143905Z/publication; profile D:/SakuraTool/SakuraPool-P6A 验收 #/profile.json. Credential refs read into memory, not disclosed. Worker D:/SakuraTool/SakuraPool-p6-origin-stability-target/debug/sakurapool-worker.exe overrides old profile worker in memory.

Original a9b40f1a exit1 before modes: metadata gate rejection lacked safe code/status; encoding vs HTTP undetermined. Repaired baseline30fa1e97 structured BLOCKED provider_tree_request/http_status400/CONFIRMED/pending0/inflight0/attempts5. Both modes unstarted.

Post-fix command, cwd new worktree and explicit PYTHONPATH=new src:

`P reports/P6A/origin_soak.py --root D:/SakuraTool/SakuraPool-P6-origin-soak/post-fix-4fc700ea --publication D:/SakuraTool/SakuraPool-P5D-20261004T143905Z/publication --profile "D:/SakuraTool/SakuraPool-P6A 验收 #/profile.json" --worker D:/SakuraTool/SakuraPool-p6-origin-stability-target/debug/sakurapool-worker.exe --iterations 100 --extra-persistent 500 --label post-fix`

Job e881bcc8 exit0 for structured BLOCKED, NOT soak PASS; tree4fc700eaa2c575d5cf22ebaff0e86b6cf371a910. results.json preserved in root. Discovery safe_code=http_status, phase=provider_tree_request, HTTP400, accounting=CONFIRMED. Ledger attempts13/body76556/metadata76556/disk2105344/inflight0/pending0/saved_samples0/saved_bytes0. Cold/persistent never started. STOP no guessed parameter/repeated discovery/acceptance.

One discovery denotes a bounded exact-lookup invocation, not two HTTP requests. Existing find_legacy_file uses bounded walk_legacy_tree_pages with PageSize20/PageNumber sequence; ledger counts admitted attempts. Aggregate13 does not identify rejected page or redirect count; neither is fabricated. No pool cause asserted.

## Resources/old state/branches

Official `P -m sakurapool workspace inspect "D:/SakuraTool/SakuraPool-P6A 验收 #/workspace"` before/after exit0: cap1/policy1,ledgerv3,disk4294967296/inflight268435456. Diagnostic defaults match, no expansion. Identity ee03b319474d43d1abc64bfb67ed9d26; lineage5a9a80dd77534cd6801cf1b91da527c4.

Before/after aggregate identical: pending6/attempts81/body3119045/metadata1522449/disk2760704/inflight0/saved_samples2/saved_bytes515084/records0. Per-lease before identities not captured: no full identity comparison claim. Old3UNKNOWN frozen; no resume/reset/refund/receipt edit. Synthetic tests use new temporary workspaces; old leases not settled. p5a-clean/uv.lock untouched.

Precommit `git ls-remote origin refs/heads/dev refs/heads/main refs/heads/work-p6-origin-stability`: dev c2db79263e4ad8927ad42d0b7516ad81bd7af0b0; main62f86cc487af41de7d84bfb5a14d2fb1e0e172ac; work branch not yet published. No dev/main push/merge, force/amend/reset/deletion.

## Explicit safety state

P6_ORIGIN_STABILITY=WAITING_REVIEW (delivery stop after reviewed commit/push verification).
MAIN_MODIFIED=NO; DEV_MODIFIED=NO; INDEX_MACHINE_OPERATIONS=0; OLD_UNKNOWN_TASKS_MODIFIED=NO.
Dedicated target and real diagnostic roots are retained for review; no deletion of diagnostic evidence, UNKNOWN state, other worktrees or environments.

## Limits/performance

Offline synthetic correctness is not production performance/real task success. Durations are test wall time, not throughput. Cold100/persistent100/same-worker500 unexercised due discovery rejection. No indexing/large-data/download/source rebuild/acceptance export. Original/postfix failures retained, not rewritten as success. After limited doc review and ordinary branch-only commit/push+remote verify, stop WAITING_REVIEW / VALIDATION_PARTIAL; no next stage.
