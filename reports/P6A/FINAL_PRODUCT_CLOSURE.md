# P6-A FINAL PRODUCT CLOSURE

P6_ORIGIN_STABILITY = WAITING_REVIEW
BASE = ea37eae07031322646ecb44da7ad37eb3aa11e46
FINAL_SHA = documentation successor; full SHA in final continuous report
PRODUCT_SHA = da56ba3d6e20c8eca6ffb698157cc608aeaf441f
PRODUCT_TREE = 694d645d62f2858ff759d24123cc4ea25b6d11db
WORKER_CAPABILITY_GATE = PASS
STALE_WORKER_NETWORK_REQUESTS = 0
ORIGIN_STATUS_RETRY = PASS
RETRY_REAL_SOAK: COLD=50/50 PASS PERSISTENT=100/100 PASS RETRY_SUCCESS_OBSERVED=YES (7/25 iterations) RETRY_EXHAUSTED=0 TRUE_UNKNOWN=0 PENDING_FINAL=0/0
HTTP_STATUS_FINALIZATION = PASS
CONSERVATIVE_ACCOUNTING = PASS_OFFLINE
REAL_FINAL_TASK = PASS
REAL_DELIVERED = 3
REAL_UNKNOWN = 0
REAL_FINAL_EXPORT = 3
HISTORICAL_PENDING_MODIFIED = NO
DEV_MODIFIED = NO
MAIN_MODIFIED = NO
INDEX_MACHINE_OPERATIONS = 0

## Scope

Sole exact design and frozen BASE-to-694d delta PASS/no defects. Production actual same cold/persistent worker requires production_transfer_v2+production_http_status_v1 before networkreserve/send. Fixed missing-cap code survives conversion; close child/resident,zero pending assertion after normal transport.close. Legacy/profile.worker unchanged. Rust only completed legal headers/trusted boundedEOF400/403 retrymax3includingfirst drop then1s2s; identicalrequestclone. Resetcompletion/currenthop every send,cumulativebody retained;302Location then singleCDN. No otherstatus/network/framing/overcap/read/header/Location/CDN/UNKNOWN retry. Sessionrequest+operation2/proof6 distinct actualHTTPmax4/ledger4proof12nonrefund,metadata independent. Checkedsharedbody3*(cap+probe)+max(business+probe,control);wrong4control,all direct consumers. Fourlease reservefail cleans body/pending preservesadmittedattempts; kthsettle preservescharge/pendingUNKNOWN+latch. HTTPoutsideconservative and ambiguoussendfailclosed maintained. Boundedretry observation counters,no error body log. LocalRustAPI sleep injectable,production real sleep.

## Offline commands and separate results

PYTHONPATH=src,explicitdedicatedworker D:/SakuraTool/SakuraPool-p6-origin-stability-target/debug/sakurapool-worker.exe. RustGNU/MSYS2PATH unsetglobalCARGO_TARGET_DIR,cargo build/test --locked --manifest-path rust/Cargo.toml --target-dir dedicatedtarget. Onlydirectsuites;nofullmatrix/Linuxwheel/sourcepackage/stress. Readonlydesign subwriter returned; implementation subwriter lacked targetededittool,nochanges. Controller then solewriter,toolinterruption not productfailure.

28ab3555 build2.25s/prod5PASS0.02s thenexit1 23failed83passed142.21s oldbudgets/capfixtures+safecodeomission;fixed. e08fddc7 HTTP32PASS7.85s. a8fc89ef retry6PASS74deselect18.17s. e76b3428 stale2PASS32deselect0.64s. 1dbd88d7 build1.99s then1failed113passed164.23s oldattemptassert;4e382fae node1PASS79deselect1.54s thenRuff2errors fixed. 0dbf641d malformed/secondread7PASS112deselect9.23s. fcaa5190 oldleaseassert11failed31passed37.95s;88b94b12 correctedfournetwork42PASS38.82s/RuffdiffPASS. dc9fb2e4 admissionfixture3failed34deselect1.27s;deeee33a3PASS34deselect0.99s. cf564109 build1.95s/worker8PASS0.04s/protocol12PASS0.13s/R2HTTP118PASS166.15s beforelaterlatchfix. 6b4cb1df runner/metadata/followup22PASS1failed137.81s oldplanassert;d9193fd8 followup6PASS1.51s/RuffdiffPASS. 476a5e02 latestRustbuild1.63s/prod6PASS0.02s thenwrongbudgetfixture1fail81deselect1.33s;db3ee07f1PASS81deselect1.38s. c6f7fd2d realoldstreambinarycoldpersistent2PASS82deselect2.35s/RuffPASS noHTTP/body/attempt/pendingafterclose. 21d63c4f partialsettle4failed3passed34deselect2.28s foundmissinglatch,fixUNKNOWN+latch;0cd96bbeHTTP/network79PASS21.19s. 8463357a currentworkerretry/stale/wrongbudget/readfailure10PASS121deselect23.37s (-k excludes other suites). Final06bf2636HTTP/followup47PASS11.57s allchangedPython/harnessRuffdiffPASS/tree694d. Runsnotfalselyaggregated. Reserve2/3/4 faults pendingbody0 attempts1/2/3;settle1..4 pending4/3/2/1UNKNOWN. Paritysmall/wrong262148/8Mi8585220/overflowreject.

## Real soak

d21879c8 exit0 localtwo-capreadywithouttoken/provider then `PYTHONPATH=src python reports/P6A/origin_soak.py --root D:/SakuraTool/SakuraPool-P6-origin-soak/final-closure-694d645d --publication D:/SakuraTool/SakuraPool-P5D-20261004T143905Z/publication --profile "D:/SakuraTool/SakuraPool-P6A 验收 #/profile.json" --worker D:/SakuraTool/SakuraPool-p6-origin-stability-target/debug/sakurapool-worker.exe --iterations 50 --extra-persistent 50 --label post-fix`.

Rawtree694d/fullverifydiscoveryPASS. Fixedrepo leafmoone/webdataset_danbooru_v3 revision73306f1dc5459238710f477b376c36da997d020c objectanime_pictures/t0992-001.tar. Publicationdigestc186f0a93d9bcf036f54a926ddae3b43aef35d6cfac4eb6dae2151abebcbd0eb snapshot226a63935e73dc3d628024615425ad9d75b985686646d15dec151aec205feede.

Cold50PASS retry7successfuliterations7;persistent100PASS retry28successfuliterations25. Exhausted0/known0/transient0/UNKNOWN0/terminal[]/mode_safetrue/finalpending0/inflight0/saved0. Residentrunningpending1released,rawresident_closedfalse pointer notpending. Noextra300 since retryobserved. Naturalconservative NOT_OBSERVED. Functional safety notthroughput/poolcausality. Readsummaryinitialwrongfinal_pendingKeyError correctedactualpending_final,no rawedited.

## Task

Newexternalprofile-http-final.json whitecopiesformat/origin/repositories/credential_ref onlyworkerchanged,no token/originalprofilechange. f3c69ffc exit0 precreate read_profile resolvedpath exact dedicated,otherfieldsequal,localtwo-cap withoutcredential/provider thencreate/run.

`python -m sakurapool task create --publication D:/SakuraTool/SakuraPool-P5D-20261004T143905Z/publication --query D:/SakuraTool/SakuraPool-P6-origin-soak/http-status-a9938676/query.json --selection records --records D:/SakuraTool/SakuraPool-P6-origin-soak/http-status-a9938676/approved-records.json --metadata --workspace "D:/SakuraTool/SakuraPool-P6A 验收 #/workspace" --task-dir "D:/SakuraTool/SakuraPool-P6A 验收 #/workspace/tasks/metadata-http-status-final2"`
`python -m sakurapool task run "D:/SakuraTool/SakuraPool-P6A 验收 #/workspace/tasks/metadata-http-status-final2" --profile "D:/SakuraTool/SakuraPool-P6A 验收 #/profile-http-final.json" --workers 1`

Taske688ac57f4a94296bbb7dcdfa4067a3e COMPLETED3DONE/PUBLISHED/CONFIRMED UNKNOWN0/error0,noresume/pause/replacement. Approvedrecordsbf0d3acda977634327904e3a01308469,98117af5261583921f9b3eb65f58a051,572fe3c5e783359c9d5d73ab1bd141f4.

`python -m sakurapool task export "D:/SakuraTool/SakuraPool-P6A 验收 #/workspace/tasks/metadata-http-status-final2" --manifest "D:/SakuraTool/SakuraPool-P6A 验收 #/workspace/tasks/metadata-http-status-final2/final.jsonl"` exit0 exported3/2347bytes. Three metadataJSONparsePASS/exportlines3. Immediatebeforeusage+all8pending in newdomain/task-before.json;afterpendingdict exactlysame8. Beforebody4200704 metadata2026735 disk2981888 saved3 bytes685718 attempts110 inflight0;afterbody6321745 metadata2717258 disk4472832 saved6 bytes2114559 attempts184 inflight0 records0. Saved+3/+1428841bytes. Oldfourtasks/old8pending/olddiag protected,noresume/refund/reset/settle/receiptedit.

## Git / safety

2a463ece productcommit/push/lsremote exit0 da56ba3d6e20c8eca6ffb698157cc608aeaf441f parentBASE tree694d. No intermediatecommits;docsordinarysuccessor,noamend/rebase/force. Remote devc2db79263e4ad8927ad42d0b7516ad81bd7af0b0/main62f86cc487af41de7d84bfb5a14d2fb1e0e172ac unchanged,notpushedmerged;historicallocalmaindivergenceunmodified. uv.lock/p5a-clean untouched,noindexmachine/largejobs/credentials echo/errorbodyoutput/extrahashlists. Onlyauthorizedshortsoak+one3recordtask. Full documentationSHA and remoteverification supplied aftercommit. StopWAITING_REVIEW.
