# P5 roadmap and B task design

A: publication construction, serial reader, safe diagnostics and foundations.
B: recoverable serial task plan/state; implementation in development, not phase-certified.
C: bounded local pipeline/concurrency; NOT STARTED.
D: production-scale validation; NOT STARTED. Directions, not approvals.

## B design contract

Task identity: SHA256 of canonical versioned JSON containing verified publication
content digest (not only snapshot-derived publication_id), runtime snapshot,
normalized typed query, selected record identities/order, selection policy/seed,
metadata choice and output/no-overwrite policy. Missing seed/ambiguous selection
fails before IO. Selection is reproducible independent of JSON key order.
No live URL/ETag or credentials persisted. Recovery pins content and refuses drift.

Proposed workflow: typed QueryResult → freeze explicit selection (identities/order/
seed and content pin) → persist task plan → execute → pause/cancel/resume with
reconciliation → export verified delivered subset plus pinned provenance.
Subset export never silently expands selection or treats failed records as delivered.
B task/output/export paths stay within the existing fixed P4 production root;
publication remains read-only outside it. No ignored `--workspace` or production
ledger substitution is provided. More restrictive per-task limits do not override
legacy global caps. Configurable roots/ledger v2 remain a future resource stage.

TaskDB state describes intent, selection, delivery visibility and retry/resume state;
BudgetLedger remains authoritative for consumed/pending quota. Task markers cannot
refund leases or erase unknown consumption. Idempotency reconciles delivered
bytes/SHA, atomic delivery and ledger settlement, not blind success markers.
Crash after rename before settlement means visible-output+pending: not permission
to re-download, double-count or refund. B persists a pinned ownership/content
receipt before publication, then settlement confirmation only after every related
network context and output settlement returns successfully. A durable per-attempt
SETTLED receipt before final item DONE permits zero-network recovery. The legacy
ledger deletes settled lease rows: a crash after settle but before any task receipt
is unprovable and stays BLOCKED_ACCOUNTING. This is not end-to-end exactly-once.
A new runner session uses its own fresh proof for each new network object; verified
already-confirmed output reconciliation itself requires no HTTP.

Paths: workspace-relative under owned validated roots, no links/reparse, strict
no-overwrite/bounded state. Legacy P4 ledger root/caps stay unchanged.
Budget migration NOT STARTED: future explicit versioned design must specify
compatibility, pending preservation, no credentials, atomic recovery; never
implicit conversion by opening a task.

Budget persistence costs, NOT remote scan chunking: full-root physical disk
scanning and each ledger slot's 1MiB write with two fsync operations require future
measurement. Planned synthetic measurements separate root entry-count/bytes,
reservation/settlement latency, slot-write bytes, fsync count/time and crash safety;
compare cold/warm roots and filesystem/platform without production throughput claims.
A changes none of this. Later design may reduce scans or durable writes only with
an explicit correctness proof preserving physical admission, pending and crash
recovery; no premature aggregation, refund or version migration.
Index-machine scanning remains operationally separate.
Pinned index build reference: 57864e5dd456251b457238b196e8fed5668d909b.
A performs zero index-machine operations.

## Foundation limits

Publication v2 backward compatible, no mandatory new manifest key; content_digest
comes from bounded verified raw manifest bytes and READY. Serial session owns
readonly SQLite/mmap/bounded proof cache, no scheduler. Publication publish reuses
atomic no-replace Windows/Linux and fsyncs owned output files; unsupported OS fails
closed. Directory fsync/Windows power-loss durability are not claimed.
Unknown pending/caps unchanged. Synthetic performance is not production throughput.
