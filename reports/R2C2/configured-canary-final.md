# Configured gamecg_v3 C2 delivery

## Identity and scope
BASE 5e075db53c6ab50fcebd777d1073be28ae4f7291; tree 3eece512a244a72e00fcdac3adeb2a164c557255.
START=END implementation 1872789d3065c7058641fee71d4266cfa1a26b4a; tree 078debaef55b672ed81209a098be4114d4c3730f.
One implementation commit, no intermediate implementation commits. Work branch work-r2c2-configured-gamecg, clean worktree D:/SakuraTool/SakuraPool-r2c2-clean. Reports-only SUBMISSION SHA/tree will be supplied in final reply (cannot embed self-referential SHA). origin/dev verified at implementation SHA before reports submission; main unchanged edc72fbaaddc4dc8f9865ddfa576b737c7b15f5a.

Repository leafmoone/webdataset_danbooru_v3, bounded root gamecg, dataset gamecg_v3. Checked-in examples/webdataset-danbooru-v3.json loaded by formal AdapterRegistry; only storage_id replaced by modelscope-c2. Source gamecg, namespace gamecg_native, provenance gamecg-2D, nested_json_v1 metadata paths/tag contracts preserved. New candidate requires own binding/proof-use; configured path does not infer adapter. P3 runtime not network readiness gate; formal P2 durable reopen/audit required, independent Download admission and streaming equivalence retained.
Only product change: fixed metadata_limit diagnostic through Rust and Python safe allowlist; other scan errors redacted. No cap, format, normalization or acceptance rule changed. Offline tests select owned temporary domain before ledger constructor, actual disk accounting/caps retained, shared pressure negative test retained.

## Verification evidence
Approved sole PY D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe, CPython3.13.15. Source validation import clean/src, pip check0. Fresh wheel same environment noneditable installed site-packages; no second environment. Process exclusivity read-only Get-CimInstance; unknown Cargo waited naturally, not killed or attributed to shared target.
Commands from clean repo with unset PYTHONPATH, SAKURAPOOL_RUST_WORKER=D:/SakuraTool/SakuraPool-Fix3-20260930a/rust-target/release/sakurapool-worker.exe:

- `$PY -m pytest tests -q -p no:cacheprovider`: exit0, 948 passed/3 skipped/0 failed/errors, pytest819.47s; tool bash_25ee200d.
- Resource8modules p4_transport/package/empty_package/tar_faults/staged_lookup/r1_gate_e/rust_index_audit/p4_budget: exit0,141passed1skip242.69s; bash_5e9fd342.
- Configured+exact mismatch: exit0,18passed14.13s; bash_8d828782.
- Earlier affected10modules: exit0,211passed283.50s; bash_79dde260, before later fixture changes; final full covers changes.
- `cargo fmt --manifest-path rust/Cargo.toml --all -- --check`: exit0.
- `cargo test --manifest-path rust/Cargo.toml --locked --all-targets`: exit0,62passed(10+6+10+7+13+4+12),0failed; compile10.01s, test binaries0.89/0.05/0.04/0.06/0.03/0.00/0.67s.
- `cargo clippy --manifest-path rust/Cargo.toml --locked --all-targets -- -D warnings`: exit0,cargo2.20s.
- `cargo build --manifest-path rust/Cargo.toml --locked --release --bin sakurapool-worker`: exit0,cargo1.55s. Rustchain bash_93718b41.
- `$PY -m ruff check .`, `git diff --check`: exit0.
- `$PY -m build --wheel --no-isolation --outdir D:/SakuraTool/SakuraPool-Fix3-20260930a/configured-wheel-dist`, release rebuild, pip --no-deps --force-reinstall wheel, repository-external `$PY -I reports/R2C2/wheel_verify.py --repository D:/SakuraTool/SakuraPool-r2c2-clean --wheel D:/SakuraTool/SakuraPool-Fix3-20260930a/configured-wheel-dist/sakurapool-0.1.0-py3-none-any.whl --code-commit 1872789d3065c7058641fee71d4266cfa1a26b4a` (helper absolute path): exit0,205passed155.78s; bash_ac4c3bf1. Installed35sourcefiles byte verified, isolated/noneditable/noPYTHONPATH/newworker. Registry gitshow frozen HEAD bytes copied external temp and explicitly loaded; helper drift checked.

These are tool stdout evidence, no invented log files. Independent total wall time NOT_RECORDED; pytest/cargo durations not claimed as wall. No extra hashes.
Synthetic actual ownbinding→Remote Rust scan→P2 reopen/audit→Download→reopen→streamed equivalence passed for small metadata; provenance/wrong adapter rejection, ownproof/inference-forbidden/largecounts/CLI/realflag/overcap/OOM/terminal tests passed. 2MiB production cap refusal maintains pending accounting; metadata_limit and unknown-code privacy/account-before-rejection explicitly tested. No production throughput claimed.

## Failed attempts and corrections (not final certification)
7pass1fail24.56s synthetic shared disk pressure; isolated synthetic domain, then10pass11.27s earlier tree. Added2MiB case8pass1fail7.60s: located1MiB production cap, retained cap and expected refusal, added typed enum propagation.14pass1fail38.38s: CLI test module absent sys.modules; registered synthetic module; next26pass135.83s.
Affected17pass1fail92.81s and93.98s shared disk pressure; first repair wrongly targeted binding_loop rather than twohop alias; traced test_r2_production.twohop, exact1pass4.69s.80pass1fail179.47s old mocked validator lacked new kw; mock kwargs repair exact1pass0.27s.147pass1error318.46s constructor traversed disappearing shared temporary Rust archive, no attribution claimed; selected domain before constructor, exact3pass5.92s.191pass1fail222.65s package lazy-import root lifetime; bind package domain per fixture, exact1pass14.09s.
First full916pass3skip18fail14error1884.10s; traces: transport12+tar_fault3+stagedlookup1 use http_and_budget; package12+empty_package2 yield same fixture; gate-e1 and Rust audit positive1 own fixtures. Test-only pre-construction isolation preserves disk/caps; exact5pass3.65s. Related29pass1fail43.93s alternate mismatch ledger created outside new domain; create alternate within same owned physical domain, mismatch rejection retained. Deleted2redundant postconstruction overrides. Final18exact/141resource/full948 certify resulting tree.
Initial formatting/import errors fixed before final checks; Rustfmt firstfailed then formatted and checked. First commit exit128 authoridentityunknown; subsequent show-origin confirmed approved repository-local author identity, normal commit succeeded. No amend/reset/rebase/force push. Historical54fail50pass not final evidence; old typed patch remains unapplied.

## Actual real capacity preflight and stop
No new target listing/Hub request/token load/binding/Range/fullscan/Download/realP2reopen; no candidate selected. Historic game_cg_5M transport/bindingPASS; autohelperINCONCLUSIVE, builderNOT_ATTEMPTED. Old dirty3helper untouched, typed patch RETAINED_NOT_APPLIED.
Production ledger D:/SakuraTool/SakuraPool-P4-work offlineFalse; counters before/after unchanged attempts87, settledbody357563, metadata356153, pending16, unknownbody16, totalbody357579, inflight0. No pending retrospectively settled. Physical4266446848, hardcap4294967296, remaining28520448. Even minimal1024byte TAR Remote additionaldisk1342193664; independent Download1409324032; both reject disk, includes actualphysical+pending reservations. Not request-count gate or service failure.
Read-only current distribution: rust-target2720489472(debug2471714816/release248643584/cxx126976); offline-twohop509dirs1061134336; p4-cert56f40c4 459722752; otheroffline8728576; p4-cert7f8afbc8728576;2wheel each2277376;2ledger slots each1048576; other942080; 49152 bytes not reconciled by this sequential per-entry diagnostic breakdown (no ownership inference). The formal ledger snapshot total remains authoritative. Oldround20physical2938793984, increase1327652864; absent prior per-directory/ownership record cannot attribute or delete. Current cargo uses Fix3/rust-target, not shared P4 target. Unknown directories/history/ledger untouched; no root switch/cap raise.
Formal execute_evidenced explicitdirectory appended existing oldcurrent/reports/R2C2/real/round-0021.json, old1-20 untouched. Operation configured_resource_preflight, CAPACITY_BLOCKED/resource_capacity, zero network/token false/candidate false, codeidentity valid. Copy included in reports-only submission; NOT a realbindinground/object certificate.
External memory-only wrapper tested on20dummyfiles: registermodule, inject explicitdirectory to original execute_evidenced on each invocation, reject conflictingdirectory, no __defaults__ mutation, append21/old20unchanged/cleanrealunchanged, APPEND_ROUTING_PASS/realrequests0. Actual preflight direct explicitdirectory; configured main launcher NOT invoked.

## Limits and status
Registry16MiB vs production perJSON1MiB; supported intersection only, no full16MiB compatibility claim. Aggregate metadata32MiB/record32MiB/line32KiB/depth32/tokens32768/stage128MiB/durable row512KiB remain. metadata_limit denotes individual OR aggregate capacity, not unique size cause. P2rows streambatch1, object/fragment descriptors materialize; auditbatch3 decodedJSON/nativeArrow not generalRAM theorem. Live reservation retained; RSS not sampled, no OSRSS≤128MiB claim. Equiv one-row cursors. Realreopen/audit NOT_RUN; realcounts unavailable.

WAITING_REVIEW
VERSION_BINDING=PASS(historical/synthetic, not newrepository)
AUTO_ADAPTER_INFERENCE_REQUIRED=NO
CONFIGURED_ADAPTER_PATH=PASS(offline)
CONFIGURED_DATASET=gamecg_v3 SOURCE=gamecg PROVENANCE=gamecg-2D
OLD_TRANSPORT=PASS OLD_AUTOHELPER=INCONCLUSIVE OLD_TYPED_PATCH=RETAINED_NOT_APPLIED
REAL_REMOTE_STREAM_CANARY=BLOCKED(RESOURCE_CAPACITY)
REAL_P2_REOPEN=NOT_RUN REAL_P2_COUNTS=NOT_AVAILABLE
REAL_NETWORK_CANARY_RUNTIME=NOT_REQUIRED_FOR_NETWORK_READINESS
DOWNLOAD=CAPACITY_BLOCKED(independentpreflight) EQUIVALENCE=NOT_AVAILABLE
REMOTE=BLOCKED DOWNLOAD_READINESS=CAPACITY_BLOCKED LOCAL=READY
REMOTE_WHOLE_TAR_SPOOL=NO(design/synthetic; realscanNOT_RUN)
FORMATS_CHANGED=NO P2=4 P3=2 PUBLICATION=NO P4_COMPLETE=NO MERGE_AUTHORIZED=NO P5_STARTED=NO
Reports-only evidence submission; stop here, no nextstage/mainmerge.
