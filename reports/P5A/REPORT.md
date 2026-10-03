# P5-A final certification — A only

## Scope and identities
BASE `f4a42550893bae51acb14d2b571c197183f1ba7d`; implementation `5b13617acab9f9b7647bc84e37337c63547ff36f`; certified implementation tree `265eccd4b80d32700131476b1b9bd22b2c5eaf74`; branch work-p5a-foundation. Implementation normally pushed to origin/dev and remote SHA verified; origin/main remains BASE. No intermediate implementation commits, amend/reset/rebase/force-push. Evidence is a separate REPORT-only commit; its SHA is supplied in delivery, not recursively in this file.

Only A: batch publication hashes/indexed streaming join/object fetchability; raw manifest identity; contained owned stages/shared no-replace/fsync cleanup; serial pinned PublicationSession; full structural bounded transport cache/empty clones/fresh worker proofs; typed safe errors/ordered bounded queries. B design only, no budget migration/provider expansion. 21 implementation paths include minimal compatibility changes in executable reports/R2C1B/binding_probe.py, reports/R2C2/canary.py/real_binding.py and related tests. Historical evidence unchanged. docs/P5A_PLAN.md retains intermediate checklist/status, NOT_BOUND and Linux NOT_RUN observations: this final report supersedes their current-status interpretation without modifying certified tree.

## Final commands and verification
Python `D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe`, CPython3.13.15; actual source `D:/SakuraTool/SakuraPool-p5a-clean/src/sakurapool/__init__.py`. Worker absolute `D:/SakuraTool/SakuraPool-Fix3-20260930a/rust-target/release/sakurapool-worker.exe`, size4388952/mtime_ns1790941121849044400 rechecked before full. All child calls unset CARGO_TARGET_DIR; Cargo explicit approved Fix3 --target-dir. Shared budget-root tests/wheel/real strictly serial following the recorded scheduling mistake.

- Final `python -m pytest -q -p no:cacheprovider --tb=short`: exit0 **1084passed/3skipped1187.69s**, bash_0e1c2c96. Tree265ecc before/after identical, unstaged diff empty.
- Focused pytest Rustproduction/P5A/publication/v1package: exit0 **142passed/1skip123.08s**, bash_1fdf944e. Whole ruff/diff check exit0.
- Rust unchanged: fmt PASS and offline62passed using explicit external target; no Rust/shared worker protocol changes after that evidence. Worker identity connects final execution.
- Fresh wheel `python -m build --wheel --no-isolation --outdir <existing temporary wheel directory>`, force-reinstall --no-deps existing temporary wheelenv: exit0. Paths retain old0f2 names but contents rebuilt from265ecc, no newenv.
- Repo-external installed `python -I -m pytest <absolute paths> -k 'not performance_harness_actual_small_phases' -q -p no:cacheprovider --tb=short`, noPYTHONPATH, explicitworker/sitepackages actual: exit0 **242passed/1skip/1deselected229.25s**, bash_45c1877d. Scope P5A/publication(all8 formerlyblockedfetch)/v1package/Rustproduction/R2C2binding/negativebody/canaryadmission. Only source/tree-bound smallperformance test deselected, covered in source full and final performance. Package skip independently established host lacks symlink privilege OSError, not PASS.
- Installed final wheel oldbaseline/newv2 fullverify and complete equality: manifest, all1002848imagehashes/RIDrecordmappings/locations, all22792objects/allrepositories/meta; exit0 bash_27b81665. No sampled equality claim.
- Sole formal reviewer actual265ecc/21paths PASS after resolving cloneconsumer block; duplicate critic cancelled, not certification evidence.

## Actual performance: retained synthetic input, not production
`python reports/P5A/publication_perf.py --output D:/SakuraTool/SakuraPool-P5A-final-publication-265ecc --implementation-tree 265eccd4b80d32700131476b1b9bd22b2c5eaf74`, exit0 bash_af989527. Retained readonly SakuraPool-R2C3-scale-20261002 P2/runtime, no regeneration/TARrescans. 22792objects/1002848RIDs, measurement-time index/source/drift gates.

| Measure | Actual baseline | Final265ecc |
|---|---:|---:|
| Build wall s |1026.7007735999941|678.9868226000108|
| Standalone fullverify s |4.945279900013702|1.0904143000079785|
| Peak working set bytes |INVALID/NOT_MEASURED: zero instrumentation failure|301707264 VALID|
| Preflight s |620.8291171|602.7889078000153|
| Mapping s |292.159092|1.7765720999741461|
| Hash sidecar s |101.2363834|72.86645100000896|
| Fetchability s |5.7455121|0.04939420000300743|
| Manifest s |earlier breakdown included elsewhere|0.0665803000156302|
| Buildverify/publish s |6.6788 earlier boundary|1.438917199993739|

Final contiguous phases sum buildwall, finalbuildverify includes fsync/publish; standalone phase equals standalone wall. SQL execute excludes cursor iteration. Hash22792Pythonexecutemany/1002848SQLiteINSERTtraces/1streamJOIN; fetchability1count+1objectSELECT, no perRID. Mapping22792lookups SQLexecute0.1332670997362584s versus baseline147.7974339s. Baseline1002848Pythoninsert/hash/fetchability lookups. Not controlled fixed overall speedup: host/cache/preflight differ. Earlier735.3156378s stays NOT_BOUND; earlier0f2 build235.4717615/verify1.0443122/RSS302092288 was bound to a tree with later consumer failures, not finaltree certification.

## Real small session and budget
bash_6624005c exit0: existing authorized R2C3 real profile/publication firstrecord only, freshRusttransport initiallyempty, PublicationSession fullverify/ownbinding/imageSHAverified/metadata PASS. No newindex/provider scan/TARscan/upload/largeproduction task. Temporary sample naturally removed, saved quota retained, no refund.

Before status body11567308 and after11744478 are **totals including pending16**, not settled. Before settled11567292; independently read after settled11744462; consistent delta177170. Metadata1088780->1193130(delta104350), attempts125->137(delta12), records242 unchanged, saved1->2/bytes72381->144762, inflight0. Disk1715208192 before/liveafter1715286016/aftercleanup1715208192. Final locked state16pendingleases/body16/allotherpending0. No unknown settlement/refund/recovery.

## Resource recovery and environment
Explicit user-approved reversible same-D-volume exactrename P4-work/rust-target -> `D:/SakuraTool/SakuraKnowledge-cargo-cache-recovery-20261003`, previously absent. All5247files/4674869064logicalbytes retained, sourceabsent, inode/size/mtime inventory equal, no reparse/protected checked budget/P2markers. .d files identify SakuraKnowledge sources; historical creatorPID unknown. Actual RestartManager registeredallfiles/GetList rc0/users0, not only nocargo.

Original bash_bc755424 exit1 **after rename**: script erroneously compared entire status including expected physicaldisk change ('ledger status changed; archive retained'). No secondmove/force/rollback. Subsequent readonly verification exit0 confirms archive/capacity/pending. Immediate before status NOT_RETAINED/NOT_RECONSTRUCTED; earlier observed6363131904bytes -> postmove1673183232 below original4294967296cap. Relocation reduces fixed budget-root usage, **not total D-volume bytes**.

Prewrite actual User=null/Machine=null/Process inherited `D:\SakuraTool\SakuraPool-P4-work\rust-target`. User deletion actor UNKNOWN/already absent, no repeated Userwrite or claimed deletion. Child after unset absent. Existing parent processes/other sessions not retroactively changed; every command clears inheritance/explicittargets. No other variable/machine registry/admin modification.

## Failed/intermediate evidence retained
- Initial ATTACH/mmap34failed/14passed10.41s actual bash_ygpi1q_h.log. Five visible fixtureconstruction rounds each1failed/15passed(2.17,2.13,2.11,2.11,2.15s), no invented sixth.
- Precapacity source98passed/8deselected21.66s; restrictedwheel97passed/9deselected21.54s, deselections not passes. Supplemental32passed/1skip/1failed20.34s exit1 actual budgetbootstrapgate.
- Execution scheduling mistake parallel wheel/full shared physicalroot7failed/1passed/40deselected10.05s, FileNotFoundError on deleted offlinefixture. Not capacityfailure; subsequent serial revalidation.
- Old0f2 full37failed/1045passed/3skip1156.48s genuine structuralcacheconsumer regression, not blanketinvalidated as interference. Two CLI generic local_or_unclassified exact old cause unestablished, later serial target/full pass.
- Consumerrepair18failed/178passed192.81s located R2C1B pathcache lookup and canary ValueError scopecontract; then101passed103.33s.
- Accessortarget2failed/197passed/1skip140.80s: testexpectedRemoteIOError but invalidorigin validlyraisesValueError; smallperf correctlyrejectsunstagedtree. Fixedtestexpectation/explicitstage, no relaxed validation; then104passed/1skip51.02s.
- New Rustclonefixture1failed/36deselected1.91s(outputdirectoryabsent), then1passed/36deselected2.81s; expanded1failed/141passed/1skip120.02s(headercaseassertion), fixed normalization plus nonempty observation; final142/1 above.
- Equalityharness exit1 StopIteration guessed record_id.npy absent; corrected actual catalog fullRID comparison exit0, no product change.
- Linuxharness initial missing os namespace exit1; corrected unchanged-function execution exit0.

## Platform and safety boundaries
Actual WSLUbuntu tiny /tmp verification executes frozen _publish_directory AST unchanged and fs_safety.sync_directory: existingtarget FileExistsError preservesbothdirs; successrename PASS; stage/parent dirfsync PASS. Actual **Linux primitives only**, not Linuxfullsuite/powerlossdurability proof. Windows reparse/junction/no-replace/corruption covered by final suite; privilege skips explicit.

No main/B/providerexpansion/productionindexing/largeproductiontask/upload/secrets/budgetcaprootchange/pendingcleanup/unknownfiledelete/processkill/remote delete/forcepush. Future sharedUserMachineCARGOTARGET not recreated. Current frozen worktree retained without migration; future work reuses user-designated mainroot with known changes Gitpreserved/unknownunstaged, not perstageclone. Historical dirtyroot/unknownarchives untouched. Owned transient environment/scripts/redundant perf outputs eligible for lifecycle cleanup after verification; minimal baseline/final evidence retained.

After evidencecommit/normaldevpush/remoteverification: **P5_A_WAITING_REVIEW**; no automatic next phase.
