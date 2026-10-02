# Task: P4-R2C3 scalable publication v2

Goal: Independently publish runtime-first scalable v2 while preserving P2v4/P3v2/packagev1; certify synthetic scalability and real121sample canary, submit implementation and separate evidence then WAITING_REVIEW.

## Open
- [x] Implement preliminary bounded manifest/catalog/hash compiler/readonly loader/CLI/fresh-bound fetch; containment/fullverify/profile hardening added.
- [x] Final code certification: 998 Python passed/3 skipped; Rust62 passed; isolated fresh-wheel207+48 passed.
- [x] Final-tree scale:22792 objects/1002848records, build1082.6543696s/peakRSS300142592bytes; full verify+100seedRID checks.
- [x] Retained real121runtime compiled/fullverified, no rescan. Authorized fresh lookup failed closed after2metadata requests; realpublication/fetch NOT_CERTIFIED, P4_COMPLETE NO; no networkretry.
- [x] Correct-path readonlyreview closed READY; all final code tests passed at39a71d2 tree decc527. Realcanary failure not disguised as PASS.
- [x] Implementation39a71d2 normally pushed origin/dev, main unchanged.
- [ ] Reports-only submission/push, final complete copyable report and WAITING_REVIEW.

## Done
- [x] Fetch BASE032f353/mainfixed and create cleanworktree work-r2c3-publication-v2; preserve olddirtytrees.
