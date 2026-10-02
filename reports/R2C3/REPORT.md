# P4-R2C3 certification

## REAL PUBLICATION CLOSURE (current authorized round)

BASE1bccbc2dabb3bb118856669b49bb917601e0d3b4; diagnostic implementation217fd10ba80c0ce4fa15817d623de04daec3dec4, normal push origin/dev verified. Only real_canary.py and diagnostic tests changed, no src/rust/product semantics. Fixed safe phases/codes, seven-case pure classifier, null-SHA supported publication path, safe ledger before/after and output redaction. Correct-path readonly review PASS. Target `python -m pytest tests/test_publication.py tests/test_real_canary_diagnostics.py -q -p no:cacheprovider`:75passed25.99s exit0; ruff/diffcheck exit0. Full998/3 and Rust62 NOT_RERUN_HELPER_ONLY / NOT_RERUN_NO_CHANGE; previously approved scaling/wheel remain PASS.

Pre-execution publication/p2-list.json/remote-map.jsonl all absent; no concurrent release worker observed. Exactly one authorized same pinned lookup executed via externalpython-I approved wheel product+committed helper. Exit1 STOP phasePROVIDER_LISTING codeREMOTE_IO, provider_pages0 (no page successfully parsed). No retry. This is a typed listing-stage failure, not evidence of network transport failure, target identity mismatch or unavailable SHA; provider parser may raise default remote_io for shape/identity/digest rejection. Underlying specific rejection is NOT_ESTABLISHED; no raw exception/response/digest/URL/headers emitted or recovered.

EXACT_OBJECT_FOUND=NOT_ESTABLISHED (not NO); size/revisionNOT_ESTABLISHED; PROVIDER_SHA_PRESENT=NOT_ESTABLISHED; REAL_PROVIDER_SHA_MATCH=NOT_ESTABLISHED; REAL_PUBLICATION_CANARY=BLOCKED; REAL_V2_FETCH=NOT_ATTEMPTED; ownbinding/imageSHA/metadataNOT_RUN. P4_COMPLETE NO. No proof/Range/whole TAR GET/saved sample/publication after control lookup failure. Publication product integrity and null-SHA support unchanged; offline null branch tested, not certified real.

Ledger before settled attempts107/body11077072/meta671380/records242/saved0, pendingcount16 unknownbody16,totalbody11077088,disk1610067968,inflight0. After109/body11181422/meta775730/records242/saved0,pending16 unknownbody16,totalbody11181438,disk1610067968,inflight0. HTTPdelta2; settledbodydelta104350; metadatadelta104350; pendingdelta0/unknownbodydelta0/savedsampledelta0/savedbytesdelta0/diskdelta0.

P2=4/P3=2/publication=2; INDEX_MACHINE_OPERATIONS0/mainmodifiedNO/P5startedNO. Stop WAITING_REVIEW. Earlier sections below are previous-stage evidence, not the new lookup result.

## Previous-stage certification (historical)

WAITING_REVIEW; P4_COMPLETE NO. REAL_PUBLICATION_CANARY=BLOCKED(FAIL_CLOSED BEFORE_PUBLICATION); REAL_PROVIDER_SHA_MATCH=NOT_ESTABLISHED, availabilityUNKNOWN; REAL_V2_FETCH=BLOCKED_NOT_ATTEMPTED; ownbinding/imageshaNOTRUN.

Evidence commit8ac29fd57cf84d6f12d855dd07e4d20637a55557 included reports/R2C3/REPORT.md and root plan.md, violating the narrower reports-directory-only closing instruction; calling it strictly reports-only was inaccurate. Its plan changes were phase-status documentation, not product code. A subsequent ordinary correction restores plan.md exactly to implementation39a71d2; cumulative final evidence diff from implementation contains only this report. No amend/reset/forcepush or certification-source drift. Restored plan is the historical implementation snapshot, not current progress.

BASE032f353cf89ab6d20de5e6a6408d365723e3371b; sole implementation39a71d2bb72830ab7dfd8822edec74b4662df0ab, tree decc527d502147e4366c836f6055269876bb1202, normally pushed HEAD:dev and origin/dev verified equal. Main unchanged edc72fbaaddc4dc8f9865ddfa576b737c7b15f5a.

Independent publication2: bounded manifest, readonlySQLite remote catalog, mmap rid imageSHA, runtime-first/no P2 by default. P2v4/P3v2/package1/registry/record/object/rid frozen. Full-verified admission, provider wholeSHA equality, fresh exactlookup/own conditionalproof/bounded Range/imageSHA/atomic delivery mandatory. No fabricated v1 rawmetadataSHA. Production rejects external control injection. Nullprovider nonfetchable, no fallback. Rename can precede ledger settlement failure, leaving delivery and pending; do not claim all failures zero-delivery.

## Final code certification

CPython3.13.15 approved Fix3 python-env. Explicit SAKURAPOOL_RUST_WORKER approved Fix3 rust-target/release worker,4388952bytes/mtime_ns1790941121849044400 unchanged. Actual full module source cleanworktree.
`python -m pytest tests -q -p no:cacheprovider`: exit0,998passed3skipped1169.72s, wrapperwall1170.0899268s. Tree frozen throughout.
`cargo fmt --manifest-path rust/Cargo.toml --all -- --check`; `cargo test --manifest-path rust/Cargo.toml --locked --all-targets`; `cargo clippy --manifest-path rust/Cargo.toml --locked --all-targets -- -D warnings`; `cargo build --manifest-path rust/Cargo.toml --locked --release --bin sakurapool-worker`: all exit0,62testsPASS approved external target. git unstageddiff empty/stageddiffcheck/write-tree bound unchanged candidate.
READY fix read65 cap64+1 requires exact64bytes: fast/full reject newline/trailing/invalidencoding/oversized. Target48passed26.63s before finalfull; relevant ruff/diffcheck exit0.

Fresh wheel `pip wheel --no-deps --no-build-isolation`, noneditable force-reinstall same sole env. Repo-external cwd, python-I, noPYTHONPATH; R2C2 wheel verifier exactimplementation compared37sourcefiles wheel/install,207passed139.53s; publication48passed27.07s. Combinedexit0. Actualsource Fix3/python-env/Lib/site-packages/sakurapool/__init__.py. pipcheck/CLIhelp exit0.

## Synthetic scaling

22792objects/1002848records; no realimages, max44materializedsample rows. Generate608.8210188s/compile1167.8011161s; snapshot f6202eb025d35f372d8d83407cfb98cad646f30a35b0a83256096c9157134622.
Finalcode publication-final rebuild/fullverify/seed20261002 100RID location/hash/object crosslookup: exit0 PASS; build1082.6543696000008s, process peakworking set300142592bytes (build+verify), manifest1194/catalog8994816/hash32091264bytes. Synthetic only, not production throughput or28Mcapacity promise.

## Real canary: fail closed

Retained R2C2 P2 compiled without TAR rescan:1object121records751tags4181memberships, snapshot329baa6aa8b8b58677bccd2ef3425ac9bf174aecd5594aaecc7d1791db19f6ae. Fixed gc5m/t0111-002.tar5201920bytes, leafmoone/webdataset_danbooru_v3 revision409a704627c429cdf55fd7b156eb23e61232a331. Formal modelscope_https_v1 profile/fixed credential filepath reviewed, credential contents never printed/archive. IndexedSHA only expectation, not provider evidence.
Authorized externalpython-I freshwheel real_canary.py --execute with fixedwork/profile, CIM no concurrent releaseworker observed: exit1 {"status":"STOP","phase":"REAL_CANARY","code":"FAIL_CLOSED"}. Offline-only diagnosis profile/work/inventory/fullruntime121fingerprint PASS. Publication/p2-list/remotemap absent. No network retry. Sanitized runner retained neither exactexception norresponse; cannot assert digestunavailable or exactfailure cause. Providerdigest UNKNOWN; realpublication/fetch NOT_CERTIFIED/P4_COMPLETE NO.
Shared formal ledger D:/SakuraTool/SakuraPool-P4-work reused without capraise/refund. Before attempts105/settledbody10972722/unknownpending16/totalbody10972738/meta567030/records242/disk1610067968/inflight0/saved0. After107/settledbody11077072/unknownpending16/totalbody11077088/meta671380/records242/disk1610067968/inflight0/saved0,pendingcount16 unchanged. Two recorded metadata requests+104350bytes; no saved sample. Earlier disk1547030528→1610067968 cause unknown, not attributed to newsettledHTTP. Lookup failure stopped before map/publication/proof/Range/wholeGET.

## Intermediate non-final evidence and failures

First full missingworker exit1:800passed29skipped153setuperrors817.92s. Correct preREADY990passed3skipped1169.23s exit0 onlyintermediate. Giant READY automaticparamid caused4Windows fixturepath setuperrors; explicitids fixed and final48/998passed. Wrong nonexistent test_runtime_compiler.py exit4 then corrected targets93passed1skip39.65s intermediate. pwsh unavailable and bad bash PowerShellquoting diagnostic failed; corrected readonlyCIM succeeded. Commitcommand locktimeout notstarted; safeconcurrentretry succeeded. No false PASS.

Main/unknown olddirtyfiles untouched; no amend/reset/forcepush/deletebranch. INDEX_MACHINE_OPERATIONS=0, ongoingindexing untouched, no productionrescan/bulkreal task. Historicalpending preserved. No secrets in evidence. Complete copyable final report belongs in review response; this repository note supplements it, not a diff attachment. Stop WAITING_REVIEW, no automatic nextstage.
