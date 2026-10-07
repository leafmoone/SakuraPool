# Rust transport and administrator interfaces

This page describes ledger-backed administrator/indexing operations and the
legacy canary-package fetch interface. Current TaskDB v4 downloads use the
[lightweight download contract](../README.md#lightweight-downloads-current-contract)
and do not instantiate this administrator budget ledger.

These interfaces do not grant blanket network/index-build authorization.
The actual production worker must advertise `production_transfer_v2` and
`production_http_status_v1` before network attempt reservation. `profile.worker`
is authoritative and is not overridden by an environment variable.
Only trusted, fully drained Origin 400/403 responses may be retried, at most three
Origin requests followed by one CDN request. Other statuses and ambiguous failures
are not retried; UNKNOWN accounting stays pending.

## Boundaries

- Python `ModelScopeDataset` controls repo identity, legacy numeric-ID
  resolution, revision candidates, bounded tree pages and canonical
  object paths. The audited `modelscope-hub 0.4.0` routes are a reference only;
  the SDK is not imported or required.
  Numeric IDs are not public repo identity; a candidate/echo is not immutable proof.
- `ProviderObject` carries `repo_id`, `repo_type`, `origin`, `revision`,
  `object_path`, `object_size`, `validator` and `cdn_host`. Repository revision,
  strong provider ETag and whole-content SHA256 are separate facts.
- `RustProductionTransport` uses explicit `modelscope_https_v1`, the configured
  `https://modelscope.cn` origin and fixed worker. No Python byte-transfer fallback.
  The test-only loopback profile cannot issue HTTPS requests or production proof.
- Existing NDJSON v1 hello/ready and operation names stay unchanged. Production
  fields and restricted output artifacts are additive. Two hops are internal to
  one logical worker operation; no externally coordinated continuation protocol.
- Origin receives Bearer plus optional SDK `m_session_id` cookie. The next request
  uses a separate credential-free client: no Authorization, Cookie or Referer.
  Production clients honor standard environment proxy configuration; synthetic
  loopback transport disables proxies. Automatic redirects and HTTP-library
  retries are disabled. The Rust CDN client reuses a bounded pool within a lane:
  at most one idle connection per host, a 15-second idle timeout, with the pool
  cleared on a scheme change or after more than eight distinct CDN authorities. Only the gated first 302 Location is accepted, and no further CDN redirect is followed.
- Strict Location checks reject userinfo, fragments, encoded-host/path escapes,
  unexpected hosts, credential echo and uncertain/expired signatures. Signed URLs
  exist only in memory/anonymous pipes, never in config, argv, logs or manifests.
- Range: only 206, exact Content-Range, total size, Content-Length, identity
  encoding and strong validator, with short/long/200 rejected. It creates actual
  bounded bytes, not just a digest. Conditional capability requires correct
  If-Match -> strict 206 and wrong If-Match -> empty 412 at the same body endpoint.
  Legacy plain-206 proofs cannot authorize the new proof domain.
- Reserve both attempts before worker launch. Known consumption is durably charged
  even on rejection. Header rejection with an unread nonempty/unknown body,
  malformed accounting, timeout, death or an early stream-parser stop leaves body
  leases pending. Only trustworthy complete accounting is settled.
- Post-canary diagnostics expose only phase, numeric HTTP status, read bytes and
  accounting completeness. They do not retroactively diagnose the real batch.

## Timeouts and output durability

Each production Rust transfer RPC has a 125-second total deadline, covering up
to three 30-second Origin attempts, 1-second and 2-second backoffs, one 30-second
CDN attempt, and two seconds of internal headroom. Every request/body timeout
remains at most 30 seconds and is clamped to the remaining operation time. The
Python response watchdog is 130 seconds, allowing five seconds for IPC and
teardown; the handshake retains its separate 60-second timeout. Blocking local
filesystem calls or pipe writes are not a hard real-time cancellation guarantee.
Range, negative-proof and whole-object scan operations share this deadline; it
is not a promise that an arbitrarily large object can complete within it.

Python provider-metadata lookups precede or accompany these operations through
Requests. Their 10-second connection and 60-second socket-inactivity timeouts
do not impose a total wall-clock deadline on slow headers or a trickling body.
The 125/130-second Rust RPC/watchdog contract does not cover those metadata
requests; their response-byte and pagination limits remain independent bounds.

POSIX publication and owned-stage cleanup synchronize the affected directories
before recording completion; directory-sync failures fail closed. Windows has
no portable directory-fsync equivalent here: process-crash recovery is supported,
but directory power-loss durability is not guaranteed.

## Configuration

The JSON config is at most 64 KiB, uses precisely the explicit profile and one
verified object, and contains no token value or signed download URL:

```json
{
  "profile": "modelscope_https_v1",
  "origin": "https://modelscope.cn",
  "repo_id": "leafmoone/game_cg_5M",
  "revision": "<validated hex candidate, separately assessed binding>",
  "worker": "<absolute current release worker path>",
  "token_file": "<existing authorized credential file path>",
  "object": {
    "repo_id": "leafmoone/game_cg_5M",
    "repo_type": "modelscope_dataset_legacy",
    "origin": "https://modelscope.cn",
    "revision": "<same hex candidate>",
    "object_path": "<actual validated canonical tree path>",
    "object_size": 123,
    "validator": "\"<strong provider ETag>\"",
    "cdn_host": "cdn-lfs-cn-1.modelscope.cn"
  }
}
```

This is explanatory placeholder syntax, **not an executable real-target binding**.
Token comes from the authorized file or `MODELSCOPE_API_TOKEN`, never argv. Config
shape alone supplies no proof: the unchanged fixed-root durable ledger must have
an exact-scope new conditional proof. `verify_conditions()` is a bounded Rust
capability interface, not automatic permission for repeated live probing.

## Administrator modes

The explicit plan contains `adapter` (existing `DatasetAdapter` fields) and a
fresh `stage` path inside `D:/SakuraTool/SakuraPool-P4-work`. `--output-package`
currently names a fresh **P2 durable directory**; it does not automatically compile
P3 or publish a user package. Fresh paths and the maximum simultaneous transfer,
stage and durable-build footprint are checked before metadata/body HTTP. Provider
binding and conditional proof gates still apply before an authorized transfer.

```console
sakura index scan-remote --config production.json --plan admin-plan.json --output-package D:/SakuraTool/SakuraPool-P4-work/new-durable --mode download-then-scan
sakura index scan-remote --config production.json --plan admin-plan.json --output-package D:/SakuraTool/SakuraPool-P4-work/new-durable --mode remote-stream-scan
```

`download-then-scan` writes and synchronizes a bounded whole-TAR spool before
scanning it. Local scanning uses a fixed 64 KiB read buffer beneath the scanner's
hash/count reader; the deadline reader remains outside that buffer.
`remote-stream-scan` feeds the counted HTTP body directly to the scanner and does
**not** retain a whole-TAR spool. Both modes write bounded member-record and JSON
sidecars, then build the existing `members.sqlite` / `stage.complete` contract and
P2 v4 output. Streaming still needs local sidecar, stage and durable-output space.

Python checks sidecar/footer identities, counts, extents and metadata hashes. For
download-then-scan it additionally rereads the retained TAR to audit the whole
digest and every member extent. In remote-stream-scan, image bytes have been
discarded: their hashes and the whole-TAR hash come from the trusted Rust scanner,
not an independent Python reread. The completion stamp is written only after all
mode-specific stream, extent, size and hash checks pass.

After separately authorized P3 compile/verify, `publish_local_package(...,
production_transport=...)` verifies the same frozen game repository chain and new
proof. Package contract, P2 schemas/ObjectId/RecordKey and P3 snapshot identities
are unchanged. The offline acceptance tests exercise both modes -> P2 -> P3
query -> audited package -> exact Rust image/JSON Range fetch.

The shared Python/Rust resource contract uses the configured protocol/header
allocation allowance `P = protocolmemory(capacity)` in addition to payload memory:

- Range inflight: `32 MiB + 2*N + P`, where `N` is the requested chunk length.
- Either scan mode inflight: `128 MiB + P`, independent of TAR length.
- Scan sidecars: at most 32 MiB of member records, 32 MiB of JSON metadata and a
  4 KiB footer. Per-line, per-path, per-JSON and 100,000-member limits also apply.
- Transfer disk `T`: sidecar capacity plus 16 KiB allocation overhead; only
  download-then-scan additionally includes the complete TAR length.
- Stage disk: the transfer artifacts plus a 128 MiB SQLite stage and 1 MiB stage
  allowance, or `T + 129 MiB`.
- Durable phase: a 128 MiB stage, the 1152 MiB durable-build allowance and 16 KiB
  allocation overhead. Download-then-scan also retains `T`; remote-stream-scan
  releases its transfer artifacts before this phase.

Administrator preflight admits the maximum of these phase footprints against
existing root occupancy and pending leases. It does not sum allocations from
nonoverlapping phases or reserve the durable-build allowance throughout staging.
The legacy 256 MiB inflight and 4 GiB disk ceilings remain unchanged. A valid
footprint alone does not establish production readiness: current occupancy,
proof/binding, record/metadata bounds, deadline and all validation gates must also
pass. Passing a synthetic or loopback check is not a production throughput claim.

## User fetch

```console
sakura fetch --index-package <verified-local-package> --config production.json --record-id <query-record-id> --output <fresh-budget-root-directory>
```

Missing/invalid package refuses before network; fetch never implicitly scans. A
valid package validates the P2/P3/audit chain, exact configured object scope and
current durable proof before Rust bytes. Merging respects the Rust 8 MiB Range
cap; separate concurrent reads use scoped transport clones and durable admission.
Any unmet profile/binding/budget gate remains BLOCKED rather than silently
switching to Python HTTP. Query itself is local P3 runtime work.
Production package verification is currently limited to a small canary package:
at most 4096 entries and 1 MiB total files, no links, with an inflight reservation
of `64 MiB + 128 * total_file_bytes` retained through fetch. Parquet footer/row
bounds and declared uncompressed expansion are checked before row decoding. The
CLI verifies a package once before reading configuration or credentials; it does
not keep two decoded audit copies. This is an additive production admission
restriction, not a package-schema migration or certification of large packages.
Independent cookie values (not just the complete Cookie header) are protected
against Location/ETag echo. No real canary was repeated after these offline fixes.
