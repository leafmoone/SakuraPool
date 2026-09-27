# Task: P3 Runtime Snapshot / Bitmap Query Engine

Goal: Implement and verify all P3 execution-plan sections 0-75 on `dev`, preserving P2 v4 durable inputs and contracts, then push implementation and evidence commits to `origin/dev` and stop at WAITING_REVIEW.

## Open
- [ ] 0-4: branch/remote discipline, P2 boundary, dependencies, runtime version/compiler constants
- [ ] 5-7: committed P2 INPUT/COMMIT validator, multi-directory inventory, deterministic source fingerprint
- [ ] 8-15: uint32 rid canonical ordering, streaming compiler staging, catalog schema, lookup, mmap locations, ObjectRef reconstruction
- [ ] 16-25: TagKey/category/origin semantics, known bitmap, Roaring32 partial spill/merge, source/dataset sets, byte LRU
- [ ] 26-33: independent RuntimeQuerySpec, validation/planning, lazy QueryResult, LocationBatch and record batches
- [ ] 34-40: immutable staging publication, READY/current atomicity, SQLite readonly, identity trailer/open/full verify, CLI compile/query/inspect/verify/lookup
- [ ] 41-47: reference evaluator, random differential, NOT/multi-source/multi-dataset cases, fresh-process and corruption checks
- [ ] 48-50: crash/resume/current failure/cleanup ownership and mixed-snapshot rejection
- [ ] 51-60: 100k correctness, 1M performance, 5M scaling, skewed tags, cold/warm families, cache and memory gates
- [ ] 61-67: snapshot/result lifecycle, P2/P1 102-test regression, full corruption/recovery matrix
- [ ] 68-70: pinned dependency certification, commit/push/remote SHA discipline
- [ ] 71-75: complete report/evidence, command bytes/hashes, final remote state, WAITING_REVIEW and P4 boundary
- [ ] Reviewer risk items: npy identity trailer, exact P2 committed inventory, multi-origin known union, streaming parts, cache non-mutation, fresh-process verification
- [ ] Implementation commits pushed to origin/dev and full host/pinned verification complete
- [ ] Final report/evidence commit pushed; final SHA and remote SHA verified

## Done
- [x] Read complete P3-Execution-Plan.md sections 0-75
- [x] Read reviewer risk analysis and confirmed no P2 schema changes
- [x] Verified local dev/main/origin SHA at P3 BASE `6c45548e01b0856d6d4bbce18e240be0b22ae7e7`
- [x] Confirmed root-owned uncommitted files remain untouched
