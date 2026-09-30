# R2 verification record — implementation certified offline, production blocked

BASE `249dc9bebdc3beed40dd81c6067a7a2f8a814ae3`; implementation
`b2af1f1b6049129a27dcb9e7a8498c50eda56791`, tree
`3e0c4ec02257c364719095942a81a3db0360311e`, normally pushed to `origin/dev`.
Protected unrelated dirty/untracked files were not staged or removed.
Final full Python regression and necessary installed-wheel verification have now
completed on this product/test source. Earlier failed/unknown attempts below are
preserved and not counted as passes. P4_COMPLETE = NO; production binding blocked.

Initial evidence commit `27a0fb49e79d4d50c1e120a331887b95af23d65e` was normally
pushed. A final raw-byte comparison found both logs were initially staged with
CRLF normalization before the local -text attributes existed. Explicit-path
`git add --renormalize` restaged the actual original capture bytes under -text;
index comparisons now match. Result JSON already matched. A new evidence-only
submission preserves this correction, with no amend, source changes or new hashes.

## Latest independent Rust verification

Environment: rustc/cargo 1.98.1, x86_64-pc-windows-gnu;
`PATH=/c/Users/PC/.cargo/bin:/c/msys64/ucrt64/bin:$PATH`;
`CARGO_TARGET_DIR=D:/SakuraTool/SakuraPool-Fix3-20260930a/rust-target`.
CC, RUSTFLAGS and CARGO_ENCODED_RUSTFLAGS were unset. Cargo manifest still uses
crate `sakurapool-rust` / default lib `sakurapool_rust`; reqwest 0.12.28 is locked
with blocking/rustls and no default features. Rust test file fixtures use
`std::env::temp_dir()` and loopback endpoints, not fixed P4 data-root fixtures.

An initial `cargo test --locked --manifest-path rust/Cargo.toml` exited 101 with
E0463 in `tests/tar_stream_bounds.rs:2` (cannot find crate `sakurapool_rust`).
The cause was NOT established; no external-target conflict is inferred from it.
Non-destructive diagnosis, with no code changes or clean/new target:

| Command | Exit | Actual result |
| --- | --- | --- |
| `cargo check --locked --lib --manifest-path rust/Cargo.toml -v` | 0 | library check; explicit crate-name and dependency paths correct |
| `cargo test --no-run --locked --manifest-path rust/Cargo.toml -vv` | 0 | rebuilt `tar_stream_bounds`; explicit `--extern sakurapool_rust=...rlib`; all 7 test executables produced |
| `cargo test --locked --manifest-path rust/Cargo.toml` | 0 | latest 58 passed, 0 failed, 0 ignored; doc tests 0 |
| `cargo fmt --check --manifest-path rust/Cargo.toml` | 0 | latest formatting passed |
| `cargo clippy --all-targets --locked --manifest-path rust/Cargo.toml -- -D warnings` | 0 | latest lint passed |
| `cargo build --release --locked --manifest-path rust/Cargo.toml` | 0 | existing explicit release worker built |
| keeper Python `-m ruff check .` | 0 | latest Python lint passed |
| `git diff --check -- README.md docs/R2_TRANSPORT.md rust src tests reports/R2` | 0 | authorized changed paths whitespace check passed |

58 = library 6 + worker 6 + http_client 10 + http_stream 7 + tar_scan 13 +
tar_stream_bounds 4 + worker_protocol 12. These are newly executed results,
not inherited historical certification. No `cargo clean`, artifact deletion,
new permanent target/environment, dependency change or source workaround occurred.

## Python verification boundaries and failures

Earlier intermediate R2 trees passed 32 R2 tests, and a previous combined R2/package
run passed 59 with 1 host-symlink-privilege skip in 480.83s. Neither certifies the
later final package-memory/cookie-value/fresh-process additions.

After read-only review fixes, an isolated R2 run completed 34 passed / 1 failed in
473.83s: encoded-cookie rejection correctly stopped before CDN but the fixture's
expected error-code list omitted `location_encoding`. The fixture was corrected,
and plain and encoded cookie-value cases now have separate tests.

Intermediate targeted command (keeper Python, explicit release worker):
`-m pytest tests/test_r2_production.py -k 'independent_cookie or two_admin_modes or package_memory' -q -rA --maxfail=1`

Actual result: **5 passed, 1 failed, 30 deselected**, 127.73s, exit 1.
The 5 passed cover plain-cookie/encoded-cookie/ETag-secret rejection, package
inflight/invalid-package-before-credentials gate, and download-then-scan -> P2 ->
P3 query -> audited package -> Rust image/JSON -> new `python -I` process exact
image/JSON fetch. The remote-stream case failed before reaching its newly added
fresh-process assertions because physical-root accounting encountered disappearing
`offline-transport-52o2idqe`. This failed attempt did not certify that branch.
After the user resumed verification, the exact remote-stream test passed separately
(1 passed, 44.15s, exit 0), then passed in the final full suite and installed wheel.

A separate earlier attempted R2 run also encountered disappearing `offline-transport`
directories. Single read-only process checks established an active external
`SakuraPool-preflight` keeper-Python suite during one collision; later a new
preflight-related Python task appeared, but its exact action was not established.
No unknown process was killed and no external suite statistics were used. Root
confirmed no cross-graph coordination tool; no notification to that external
instance is claimed. Shared-root verification was temporarily deferred, then resumed
on explicit user instruction after a single preflight process check; no fail-closed
filesystem accounting was relaxed and no budget root was changed.

## Final formal regression and wheel

An earlier background full-suite call returned a start receipt but no terminal
result was retrieved. No matching process was later observed; its final status
remains UNKNOWN. It supplies no test statistics and no passing certification.
The user explicitly instructed execution of a recoverable formal run.

`K = D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe`.
`run_regression.py` performs one child `K -m pytest -q -ra`, captures combined
stdout/stderr and writes fsynced exit evidence; it never retries or overwrites.

| Command | Exit | Actual result |
| --- | --- | --- |
| `K reports/R2/run_regression.py` | 1 | 556 passed / 2 skipped / 1 failed, 1895.19s; log + result retained |
| `K -m pytest tests/test_p4_transport.py::test_production_scan_build_package_block_before_any_http_or_artifact -q -ra` after fix | 0 | 1 passed, 0.25s |
| `K reports/R2/run_regression.py --final` | 0 | **557 passed / 2 skipped**, 1815.63s; 559 terminal nodes, no deselection or ordering plugin |
| `K -m build --wheel --no-isolation --outdir D:/SakuraTool/SakuraPool-Fix3-20260930a/r2-wheel` | 0 | same keeper built new wheel |
| `uv pip install --python K --reinstall-package sakurapool <new wheel>` | 0 | normal dependency resolution, noneditable install in same keeper |
| outside repo `K -I .../reports/R2/wheel_verify.py --repository D:/SakuraTool/SakuraPool --wheel <new wheel>` | 0 | **7 passed, 123.01s**, noneditable site-packages; all 30 Python source bytes verified against wheel/source (only CRLF normalization for source); both mode/fresh chains |
| `uv pip install --python K --reinstall-package sakurapool -e '.[dev,remote]'` | 0 | editable restored, original pins unchanged |
| `K -m pip check`; `K -I -c <actual import path>` | 0 / 0 | no broken requirements, current module again from repo `src` |

The sole full-suite failure was an input-admission regression: `write_staged_v4`
accessed `offline_mode` on a non-BudgetLedger stand-in. Minimal product correction
checks type before attribute access and preserves `BudgetExceeded/BLOCKED` failure
vocabulary for build/package gates; the existing test was not weakened. Final full
suite and wheel ran after that correction; implementation commit source was unchanged.

Two final suite skips: `test_p4_package.py:179` lacks symlink privilege;
`test_runtime_remediation.py:787` cannot create file symlinks. Neither hides a
production gate failure. New R2 acceptance nodes all ran, including actual Python
`-I` subprocess package verification and Rust exact bytes in both pipeline modes.
The 7 wheel cases are a necessary selected acceptance test, not a full wheel suite.
No second venv, no no-deps install, no complete remote TAR, no full-file hash list.

## Read-only final review limitation

BASE to implementation-SHA read-only review found no blocking defect, but reports
a nonblocking failure-classification limitation: production failures can return
`ok=true` with `result.production_error`; Python `_call` does not explicitly map
that field to a single RemoteIOError path. Some known-empty-body rejects can later
raise KeyError on missing success fields. Artifact/proof admission stays fail-closed;
this is not complete stable error semantics. No frozen source/test changes were
made after review, so the recorded full/wheel/Rust source binding remains valid.

## Real capability batch (stopped)

The one announced minimal batch and exact result are preserved in
`real-result.json`; no subsequent live request was made.
3 metadata operations + 2 Rust hop attempts; known metadata increment 21,709 B.
Rust reported 0 entity bytes with incomplete accounting, leaving 2 pending leases
and 2 B unobserved-body reservation bound. That bound is NOT actual consumption.
Final CDN numeric HTTP status was not retained and remains UNKNOWN; no token,
permission or expiry diagnosis is invented. Post-run offline safe diagnostics do
not retroactively supply it. Candidate echo and incomplete pages do not prove
immutability; no completed positive/negative conditional proof or approved
alternative exists. Historical unledgered totals stay unknown. Pending untouched.

Production version binding/Range and both actual administrator builders remain
BLOCKED by binding and/or conservative fixed-budget feasibility. Interfaces and
small offline pipelines are not a real-target READY verdict. No full remote TAR
request, large repository scan, performance benchmark, main merge or P5 work ran.
