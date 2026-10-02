# Task: P4-R2C3 scalable publication v2

Goal: Independently publish runtime-first scalable v2 while preserving P2v4/P3v2/packagev1; certify synthetic scalability and real121sample canary, submit implementation and separate evidence then WAITING_REVIEW.

## Open
- [x] Implement preliminary bounded manifest/catalog/hash compiler/readonly loader/CLI/fresh-bound fetch; containment/fullverify/profile hardening added.
- [ ] Validate corruption/security and v1/P2/P3 regressions.
- [ ] Final-tree revalidation of synthetic scale; intermediate actual 22792 objects / 1002848 records, manifest1194 bytes/catalog8994816/hash32091264, build1051.3704s/RSS298512384 bytes.
- [ ] Compile retained real121sample P2; digest-gated publication/fetch, no rescan.
- [ ] FullPython/Rust/freshwheel validation and readonlyreview (Python full and Rust commands running; targeted93PASS/1skip; finalwheel pending).
- [ ] Implementation/push then reports-only submission/push and finalreport.

## Done
- [x] Fetch BASE032f353/mainfixed and create cleanworktree work-r2c3-publication-v2; preserve olddirtytrees.
