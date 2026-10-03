# P5-A tests-only: WORKER EXIT ASSERTION CORRECTION

## Identity, scope and review
BASE_SHA = eec713e8319279560eb327796f417797b3cdafe3
QUERY_IMPLEMENTATION_SHA = 08f9019264989748e20b793908d4823ba1232cff
TEST_FIX_SHA = 27b8d82b613cb3096c858d792afa2ede50782cf8
TEST_FIX_TREE = 9bf708ae0364a71fa81ff7cd3941d5b5f2b03a2a
EXPECTED_MAIN = f4a42550893bae51acb14d2b571c197183f1ba7d
INDEX_BUILD_SHA = 57864e5dd456251b457238b196e8fed5668d909b

Preflight actual HEAD/origin/dev both BASE, clean; origin/main and local index integration branch match expected. Reused existing SakuraPool-p5a-clean/work-p5a-foundation, no new worktree/environment. Exactly tests/test_r1_bridge.py changed (81 additions/19 removals). No src/rust/query/dependency/budget/format/historical-evidence change. Sole formal reviewer actual9bf single-file read-only review PASS; not independent test execution. Ordinary test commit normally pushed, remote origin/dev27b8 and main expected verified, clean. This report is a separate explicit REPORT-only commit; SUBMISSION_SHA/tree/parent and final remote refs are supplied in delivery (not recursive self-SHA in this file).

## Assertion method and coverage
Both existing test_cancel_kills_and_waits and test_cancel_is_immediate_kill_and_wait retain the real worker._proc, both actual reader thread objects and actual stdin/stdout/stderr objects BEFORE close/cancel; assert original process running. New exit helper reads returncode BEFORE poll, then checks both non-None, worker.pid None, retained readers not alive and created pipes closed. The failure payload contains observed returncode-before-poll/poll/pid/readers/pipes. No extra wait/sleep/kill precedes acceptance, no PID-only/reference-only proof. No fabricated poll/returncode, POSIX-specific negative-code requirement or returncode==0 condition.

Real cancel is verified independently: instance spies record kill/wait and invoke the retained original methods; kill->wait observed before success. Negative tests start only their own real workers and prove exit assertion fails while live, leaving worker still running; parametrized normal close and cancel then pass, mixed repeated close/cancel calls remain idempotent with same retained resources. finally cleanup can use only retained Popen for that test's child; no unknown PID/process kill. No sleep additions, timeout loosening, xfail/skip additions or removal of required lifecycle assertions. No new exit assertion failure occurred; no failure-loop reruns.

REAL_POPEN_IDENTITY_RETAINED = PASS
LIVE_WORKER_NEGATIVE_ASSERTION = PASS
CLOSE_EXIT_AND_REAP = PASS
CANCEL_EXIT_AND_REAP = PASS
READER_THREADS_JOINED = PASS
PIPES_CLOSED = PASS
REPEATED_CLOSE_CANCEL = PASS

## Historical failure boundary
ORIGINAL_TWO_FAILURES_ROOT_CAUSE = UNRESOLVED
WINDOWS_OS_KILL_ZERO_TEST_METHOD = INCORRECT
WINDOWS_EXIT_TEST_METHOD_CORRECTED = YES

The prior Query-fix full had two old PID-probe assertion failures; those results remain in QUERY_FIX_REPORT, not rewritten. No assertion that original failure was PID reuse, interference, or Rust product fault. This corrects FUTURE acceptance, not reconstructs missing historical Popen/thread/pipe state. Python3.13 official os.kill documentation specifies Windows non-CTRL signals use TerminateProcess, exit code set to sig; zero is not a harmless POSIX existence check. Documentation consulted: https://docs.python.org/3.13/library/os.html#os.kill and CPython3.13 Doc/library/os.rst. No os.kill-zero experiment performed against any process.

## Actual verification
Approved keeper D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe, CPython3.13.15. Source actual D:/SakuraTool/SakuraPool-p5a-clean/src/sakurapool/__init__.py. Explicit SAKURAPOOL_RUST_WORKER D:/SakuraTool/SakuraPool-Fix3-20260930a/rust-target/release/sakurapool-worker.exe, actual size4388952/mtime_ns1790941121849044400 unchanged. CARGO_TARGET_DIR child inherited value unset, no User/Machine edits. Strictly serial, no wheel/other shared-root test job parallel.

Executed once after change (bash_3922c22a):
`"$PY" -m pytest tests/test_r1_bridge.py tests/test_r1_stdout_backpressure.py tests/test_query_batch_compat.py -q -p no:cacheprovider --tb=short -rs`
TARGETED_TESTS: passed=56, skipped=1, failed=0, exit=0; pytest reported duration31.84s (not complete shell wall).
Only skip tests/test_query_batch_compat.py:65, native getlimit-absent runtime required on keeper with modern API. Worker was present; exit/negative/cancel tests not skipped. Native3.10 evidence stays attached to prior query-fix run, not rerun here.
Then `"$PY" -m ruff check .`: exit0, PASS. `git diff --check`: exit0, PASS. Staging exact one test, cached diff check and unstaged-empty checks pass. Product path diff versus BASE empty.

## Non-actions and final state
PRODUCT_CODE_CHANGED = NO
RUST_CHANGED = NO
FULL_REGRESSION_RERUN = NO
WHEEL_REBUILT = NO
PY310_RERUN = NO
PERFORMANCE_RERUN = NO
PROVIDER_REQUESTS = 0
MAIN_MODIFIED = NO
INDEX_MACHINE_OPERATIONS = 0
INDEX_BUILD_SHA_CHANGED = NO
P5_B_STARTED = NO

No new environment, transient scripts or cleanup targets generated. No keeper/unknown-directory/ledger/pending deletion, budget mutation, provider scan/fetch/upload, Rust build, history rewrite or force push. Prior product/wheel/Query/performance evidence retains original commit/tree binding, not recertified by this tests-only run. Test-only ordinarycommit and independent report ordinarycommit use normal push dev; main remains fixed.
Final delivery after report push/remote verification: P5_A_EXIT_TEST_FIX = WAITING_REVIEW. Stop; no main merge/B/next-stage execution.
