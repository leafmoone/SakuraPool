# Conservative Network Finalization — design for limited review

Scope: new worktree only, BASE c2db79263e4ad8927ad42d0b7516ad81bd7af0b0. No historical task/lease migration. Sole limited design review PASS has been relayed by root; core implementation is authorized after the classification-only baseline is frozen and observed.

## Evidence and ownership boundary

Rust structured send errors classify timeout/connect/request/transport separately for origin/CDN. Classification does not change Accounting.complete: observed consumption remains unknown on incomplete requests. Only a validated request-id-matched terminal envelope with validated accounting and an eligible structured network error creates a Python deferred candidate. EOF/crash/malformed envelope never qualifies, even if the process subsequently exits. No inference from status, cleanup SAFE, or output lease CONFIRMED alone.

`production.py::_call_accounted` owns the actual network reservations; retain their exact IDs, originally approved body bounds and admitted attempt charges in the candidate. Do not charge future unadmitted chunks. Previously confirmed chunks remain charged once. Observed body already consumed must be subtracted from the original approved max when charging the remainder, not charged twice. Attempts are charged at reservation admission; finalization must not add another attempt count or invent a fixed new bound.

`_transfer_owned_body` is the resource gate: after the failed request unwinds, require no retained raw payload, no live request, owned artifact cleanup, memory/disk finalization and no secondary failure. Persistent process-alive alone neither proves nor disproves request terminality. A validated sequential terminal response should establish request completion; if lifecycle verification cannot prove no subsequent writes, close the channel and confirm process exit before finalization. Failure to close/confirm leaves UNKNOWN. A lost envelope is always UNKNOWN irrespective of later exit.

## Deferred charge and settlement

Do not settle incomplete network leases in `_call_accounted`. At the successful resource gate, consume the remainder to the exact original max and settle each originally reserved network lease. Any consume/settle failure leaves the operation UNKNOWN, retains unclosed leases, and preserves already successful mutations without rollback, repeated charge, or replay. Preserve the original network error; append fixed settlement/cleanup secondary codes. Mark a candidate finalized only after every required mutation succeeds. The candidate is request-local and single-use; it cannot be used to reconcile historical leases or a subsequent request.

Expose accounting=CONFIRMED, accounting_basis=CONSERVATIVE_MAX_CHARGE, actual_consumption=UNKNOWN, accounted=CONSERVATIVE_MAX only after successful finalization. This denotes conservative quota reconciliation, not measured consumption. Keep observed accounting in diagnostics distinguishable from charged accounting.

## Outer publication and task gate

`publication_fetch.py` may report operation CONFIRMED only if delivery is NOT_PUBLISHED, owned stage cleanup succeeds, output lease settlement is confirmed, the network candidate is finalized and every resource secondary is absent. If output settlement fails after the network charge, outer operation remains UNKNOWN; never undo the network charge. PUBLISHED/saved settlement failure remains UNKNOWN, independently of network confirmation. Retained payload/exception resources remain UNKNOWN. Safe fields propagate through the publication error and task diagnostic allowlists.

Task failed item returns READY with recoverable=true only for this confirmed unpublished case; run stops BLOCKED and requires explicit resume. No automatic network retry. UNKNOWN remains BLOCKED_ACCOUNTING and is never replayed. Durable attempt accounting should retain the conservative basis without claiming a successful delivery or a normal SETTLED delivery receipt. Explicit resume creates a new attempt; previous failed attempt and charged quota remain. If quota is exhausted, subsequent admission is RESOURCE_BLOCKED; no quota refund.

## Required fault tests

- Terminal origin connect/timeout: full original quota charge, no pending request lease, unpublished READY/recoverable.
- Terminal CDN failure after successful origin: full approved request quota, observed consumption unknown.
- Crash before terminal envelope: UNKNOWN even after confirmed process exit.
- Alive request/unknown process, unsafe artifact ownership, cleanup failure, retained payload: UNKNOWN.
- Partial network lease settlement then failure: overall UNKNOWN, remaining pending preserved, already settled charge unchanged, no replay/double charge.
- Outer output lease failure and PUBLISHED saved settlement failure: UNKNOWN.
- Confirmed conservative failed attempt followed by explicit resume success: exactly one new saved sample, old failed quota/attempt retained.
- Quota exhaustion after conservative charge: RESOURCE_BLOCKED with unchanged charged quota.

Soak remains independent of acceptance workspace; original accounting baseline is collected before core implementation. Mode UNKNOWN stops that mode. persistent100 success alone enables extra500. Cold/persistent pool policy changes require actual comparative evidence.
