# P6-A HTTP STATUS FINALIZATION — final evidence

## Verdict / identity

WAITING_REVIEW. Product sole corrective delta PASS; final user task acceptance NOT PASSED. Product `1401949b743f091853f82ac94d39435818c07169`, parent `e049a7c84031301faef592cec3f11ce428fde640`, reviewed tree `a9938676eb534c3ea38c2d2d701786e94eef0bba`. No intermediate product commits. Ordinary feature push and ls-remote match, exit0. Documentation is a separate ordinary commit, no amend.

Bounded control drain/trusted framing/EOF, independent payload length/SHA, checked body budgets, finalized actual HTTP status consumption, validated legacy tree shared retry, and four-class harness implemented. HTTP errors never conservative-max candidates. Explicit timeout/connect send errors preserve eligibility; ambiguous generic send errors become existing network_ambiguous outside eight-code allowlist. Real send no longer claims to emit all eight codes. Existing UNKNOWN never cleared. Formatting noise disclosed and nonblocking in sole review.

## Verification (runs separate)

All commands from feature worktree, PYTHONPATH=src. Rust GNU/MSYS2 PATH, global CARGO_TARGET_DIR unset; cargo build/test --locked --manifest-path rust/Cargo.toml --target-dir D:/SakuraTool/SakuraPool-p6-origin-stability-target. Dedicated worker debug/sakurapool-worker.exe.

966ed27b exit0 HTTP+four network tests74passed42.90s/Ruffdiff0. 4414a470 exit0 control-prefix scan SHA/Location not recoverable2passed66deselected2.75s. dd54bc91 exit0 R2+HTTP100passed118.15s/Ruffdiff0 before correction. 7544d428 build1.60s production5passed0.02s includes twelve TCP framing cases. Earlier separate worker8/protocol12 passed, not current-tree combined run.

Corrective de15bf80 build1.78s/Rustprod5passed0.02s then exit1 six fixture failures (inspect incompatibility),6failed68deselected7.01s. f06b88ff exit1 missing secondary fixture6failed68deselected6.96s. Fixtures fixed. 17771ce4 exit0 actual rebuilt worker/Python Origin and CDN invalidCL/CLTE/unsupportedTE6passed100deselected6.96s: UNKNOWN,networkpending2,reservation131074,no tokens/output,Publication UNKNOWN. -k deselected HTTP32, not falsely rerun. Subsequent Ruff failed two old E501; line wraps fixed, final R2Ruff/working+cacheddiff0. Sole corrective delta PASS.

Earlier failed validations retained: missing venv127; missing module collection2;9failed33passed old charges;2failed51passed StreamPlan fields then53passed;4failed15passed fixture method/stream then1failed18passed contextmanager then19passed before Ruff failure; nonexistent test path4; consumer140passed213.12s; Windows large parameter/tuple fixture failure;11failed55passed R2 assertions then1failed65passed child import then66passed108.77s before Ruff aliases failure; subsequent28passed/32passed6.55s. Not final-tree aggregate evidence. No full matrix/Linux/wheel/stress acceptance.

## Diagnostics

84e78b24 exit0, dedicated locked build0.14s then:

`PYTHONPATH=src python reports/P6A/origin_soak.py --root D:/SakuraTool/SakuraPool-P6-origin-soak/http-status-a9938676 --publication D:/SakuraTool/SakuraPool-P5D-20261004T143905Z/publication --profile "D:/SakuraTool/SakuraPool-P6A 验收 #/profile.json" --worker D:/SakuraTool/SakuraPool-p6-origin-stability-target/debug/sakurapool-worker.exe --iterations 100 --extra-persistent 500 --label post-fix`

Fresh results.json binds a993. Publication fullverify PASS, digest c186f0a93d9bcf036f54a926ddae3b43aef35d6cfac4eb6dae2151abebcbd0eb,snapshot226a63935e73dc3d628024615425ad9d75b985686646d15dec151aec205feede. Independent discoveryPASS/page20. Fixed publication candidate repo leafmoone/webdataset_danbooru_v3,revision73306f1dc5459238710f477b376c36da997d020c,object anime_pictures/t0992-001.tar,not discovery-substituted.

Cold10072PASS28KNOWN(400). Persistent600449PASS151KNOWN(150x400+1x403),baseline10080PASS20KNOWN,extra500completed. Both CONFIRMED_TRANSIENT0/TRUE_UNKNOWN0/finalpending0/inflight0/saved0/mode_safe true/terminal failures empty. Terminal rows separate,physicalgenerations1..7. Natural conservative NOT_OBSERVED. Raw resident_closed=false retained pointer,not ledgerpending. Raw unchanged. Functional safety evidence,not production throughput/pool causality.

## Task failed acceptance / execution error

559c00e5 exit2. Run environment PYTHONPATH=src,SAKURAPOOL_RUST_WORKER=D:/SakuraTool/SakuraPool-p6-origin-stability-target/debug/sakurapool-worker.exe. Commands `python -m sakurapool task create --publication D:/SakuraTool/SakuraPool-P5D-20261004T143905Z/publication --query D:/SakuraTool/SakuraPool-P6-origin-soak/http-status-a9938676/query.json --selection records --records D:/SakuraTool/SakuraPool-P6-origin-soak/http-status-a9938676/approved-records.json --metadata --workspace "D:/SakuraTool/SakuraPool-P6A 验收 #/workspace" --task-dir "D:/SakuraTool/SakuraPool-P6A 验收 #/workspace/tasks/metadata-http-status-final"` then `python -m sakurapool task run "D:/SakuraTool/SakuraPool-P6A 验收 #/workspace/tasks/metadata-http-status-final" --profile "D:/SakuraTool/SakuraPool-P6A 验收 #/profile.json" --workers 1`.

Task7fc5647523854140bacd59569b42219b; approved records bf0d3acda977634327904e3a01308469,98117af5261583921f9b3eb65f58a051,572fe3c5e783359c9d5d73ab1bd141f4. First DONE/PUBLISHED/CONFIRMED image+metadata; second BLOCKED/UNKNOWN; third READY. First attempt SETTLED/CONFIRMED/PUBLISHED,second OUTPUT_RESERVED/UNKNOWN. Error publication_range,cause rejected/origin400/causeaccountingUNKNOWN,accountingUNKNOWN,cleanupSAFE,outputCONFIRMED,secondary[],recoverablefalse,chunk0/image.

Actual configured profile.worker D:/SakuraTool/SakuraPool-p6-stream-target/debug/sakurapool-worker.exe. tasks/profile.py83-85 directly passes Path(profile['worker']); env does not override. This proves selected configuration,not captured process trace/binary hash. Writer and coordinating review failed to check prerequisite. Cannot attribute old-worker rejection to a993; independent soak explicit worker valid. No resume/replacement/refund/worker-substitution rerun/receipt settlement. New UNKNOWN pending protected.

Final export external diagnostics path exit2TASK_FAILED (manifest must be under taskdir). Correct `python -m sakurapool task export "D:/SakuraTool/SakuraPool-P6A 验收 #/workspace/tasks/metadata-http-status-final" --manifest "D:/SakuraTool/SakuraPool-P6A 验收 #/workspace/tasks/metadata-http-status-final/final.jsonl"` exit0/exported1/788bytes,only confirmed delivery,does not drive task.

Final readonly ledger pending8/inflight0/saved3/savedbytes685718/attempts110/body4200704/metadata2026735/disk2981888/records0. Historical baseline NOT immediate pre-run: pending6/saved2/515084bytes/attempts81/body3119045/metadata1522449/disk2760704. Arithmetic differences +2pending/+1saved/+170634savedbytes/+29attempts/+1081659body/+504286metadata/+221184disk,NOT strict task-only charges. DB/export directly proves one metadata delivery. Old three user and two diagnostic UNKNOWN domains unchanged; new task protected.

## Branches / safety

Product feature remote1401949b743f091853f82ac94d39435818c07169. Local devc2db79263e4ad8927ad42d0b7516ad81bd7af0b0;local main edc72fbaaddc4dc8f9865ddfa576b737c7b15f5a. Fresh writer ls-remote origin/main62f86cc487af41de7d84bfb5a14d2fb1e0e172ac;root also verified origin/devc2db. Local main reflog shows external fast-forward merges,attribution unknown; do not call local main origin or assert whole-phase unchanged. No main/dev push/merge/reset/correction by this writer,no fetch in final sequence. No amend/force/rebase/delete;uv.lock/p5a-clean untouched. Credentials not echoed. No production requests after TRUE_UNKNOWN. Root ignored MEMORY now records profile worker precedence. Documentation-only closure does not rerun product matrix. WAITING_REVIEW; user-task acceptance NOT PASSED.
