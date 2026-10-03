# P5-A QUERY BATCH COMPATIBILITY AND ZERO-LIMIT FIX

## Identity and scope
BASE `2009ff044ede9b4e716f0f60fbf59228d792b4ff`; implementation `08f9019264989748e20b793908d4823ba1232cff`; certified implementation tree `030dc3cd335473641a754696c50247a15e6ef213`; branch work-p5a-foundation, existing worktree reused. Exactly src/sakurapool/runtime/query.py and tests/test_query_batch_compat.py. Historical evidence unchanged. Sole reviewer actual030dc/two paths PASS, not independent test execution. Separate QUERY_FIX_REPORT-only evidence commit final SHA/tree/parent/refs supplied in delivery. Implementation normally pushed/remote verified origin/dev08f901; main fixed `f4a42550893bae51acb14d2b571c197183f1ba7d`.

Normal import sqlite3. getlimit present: read current limit, nonpositive stable ValueError("SQLite variable limit must be positive") before chunking, including empty set. Absent API: initial batch<=999, only exact too many SQL variables OperationalError reduces current un-emitted portion; advance offset only after successful fetch, no lost/duplicate rows. Size1 still failing propagates; all other OperationalError propagates without retry. No product setlimit/compileoptions, fixed perRID fallback or general retry framework. >=3.10 declaration, data/formats/budget/provider/publication unchanged.

## Gates
GETLIMIT_ABSENT_COMPATIBILITY=PASS: execute-only wrapper genuinely lacks API, real queries, normal3/2 batches and actual SQLite limit2 fallback retain fields/order.
ZERO_VARIABLE_LIMIT_EXPLICIT_FAILURE=PASS: actual setlimit0/five preselected RIDs explicit stable exception, not silent []. Valid-limit empty returns[]; zero-limit empty explicitly fails consistently.
LIMIT_ONE_NO_RECORD_LOSS=PASS: actual limit1, five ordered RIDs preserved.
NORMAL_BATCH_ORDER_EQUIVALENCE=PASS: limits1/2/100 x batch1/3/8 all fields/reference/order/last partial batch.
INVALID_BATCH_TYPES=PASS: True/False/0/negative/float/string reject.
PY310_ACTUAL_RUNTIME_TEST=PASS, limited actual8 query tests, not wrapper masquerading as runtime.

## Reproduction, execution and statistics
Initial before product edits `python -m pytest tests/test_query_batch_compat.py -q -p no:cacheprovider --tb=short` BASE: **5failed/15passed0.40s, pytest exit1**. AttributeError absent getlimit and limit0 DID NOT RAISE reproduced. Outer shell continued interpreter inventory, not substituted as pytest success.
After fix20passed0.16s; ruff initially failed I001 import order/E501 testSQL line, corrected. Final ready target **20passed/1skipped0.27s exit0**, wholeRuff/diffcheck exit0; staged only2 paths, unstagedempty.

Actual existing CPython3.10.21 `C:/Users/PC/AppData/Roaming/uv/python/cpython-3.10-windows-x86_64-none/python.exe`; short Tempenv with declared numpy2.2.6/pyarrow18.1.0/pyroaring1.1.0/pytest8.3.4, real product source p5a-clean/src and real SQLite getlimit absent. `python -m pytest tests/test_query_batch_compat.py -k 'native_python310 or invalid_batch_types or other_operational' -q -p no:cacheprovider --tb=short`: **8passed/13deselected0.21s exit0**. Native allrows/order/batch3+2, invalid types and unrelated SQL error tested. Deselected modern setlimit-specific gates not claimed3.10 passes. Keeper/other projects not downgraded.

Keeper CPython3.13.15 absolute `D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe`; explicit actual SOURCE D:/SakuraTool/SakuraPool-p5a-clean/src/sakurapool/__init__.py. Fix3/rust-target/release/sakurapool-worker.exe absolute: size4388952/mtime_ns1790941121849044400 unchanged. Each child unset CARGO_TARGET_DIR; no shared User/Machine setting created.

First full `python -m pytest -q -p no:cacheprovider --tb=short`, bash_4d381aa8: **2failed/1102passed/4skipped1227.94s exit1**. Unchanged test_r1_bridge.py::test_cancel_kills_and_waits and ::test_cancel_is_immediate_kill_and_wait assert os.kill(pid,0) should raise after close/cancel; observed assertions failed. Original cause **UNRESOLVED**, no knownflaky/environment-proven/stability-fixed claim. Bridge code/tests not changed/skipped. Immediate same-env/tree targets **2passed/22deselected1.04s exit0**. Readonly Windows relevant worker/cargo/rustc/pytest/perf process count0 before supplemental full; no kill of unknown process. This does not prove original cause.

Authorized supplemental same-tree full, strictly serial, bash_ae87fa6b, same pytest command: **1104passed/4skipped657.90s exit0**. Whole `python -m ruff check .`, git diff --check/unstaged check exit0; tree030dc before/after identical. Durations are pytest wall, not full shell/build elapsed. First failure not erased by later pass.

Fresh wheel keeper `python -m build --wheel --no-isolation --outdir <existing wheel artifact directory>` exit0; noneditable installed short CPython3.13.15 wheelenv. Repository-external Temp `python -I`, no PYTHONPATH, actual sitepackages import, explicit worker. `python -I -m pytest <absolute querycompat/P5Afoundation/publication/P4package paths> -k 'not performance_harness_actual_small_phases' -q -p no:cacheprovider --tb=short`: **123passed/2skipped/1deselected57.21s exit0**, bash_fe114b55. Query/session/pub/oldv1compat covered. Sole deselection source/tree-bound small performance unit, covered source full; no million benchmark/publication rebuild.

Full4 skips explicitly checked afterward -rs targets **1passed/4skipped/97deselected0.55s exit0**: localpartition lock symlink WinError1314; package lacks symlink privilege OSError; runtime staging OS/user cannot create file symlinks; native getlimit-absent test requires legacy runtime, keeper API present. Wheel2 skips package privilege and nativelegacyAPI; actual3.10 gate passed separately.

## Non-actions, cleanup and limits
PERF_BENCH_RERUN=NO; earlier265ecc benchmark stays bound to that prior tree, not new patch. Existing tiny performance unit ran in full only. PROVIDER_REQUESTS=0: no ModelScope/realprovider scanfetch/upload; local loopback requests not provider requests. INDEX_MACHINE_OPERATIONS=0; readonly index branch SHA `57864e5dd456251b457238b196e8fed5668d909b` unchanged. RUST_NEW_RUN=NO: no release/clippy/newCargo suite; prior62/fmt evidence reused with actual binaryidentity, not newRustPASS. BUDGET_MIGRATION=NOT_STARTED; caps/root/pending unchanged. FORMATS_CHANGED=NO; MAIN_CHANGED=NO; P5B_IMPLEMENTATION=NO. No amend/reset/rebase/forcepush/add./-A/newworktree/longtermenv. Owned shortpy310/wheelenvs reclaimed after evidence review, existing interpreters/keeper/otherprojectfiles preserved.

Final ordinary evidencecommit/normaldevpush/remoteverified state: **P5_A_QUERY_FIX_WAITING_REVIEW**; STOP no nextstage.
