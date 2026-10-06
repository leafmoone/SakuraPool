# Rust transport and administrator interfaces

These interfaces do not grant blanket network/index-build authorization.
The actual production worker must advertise `production_transfer_v2` and
`production_http_status_v1` before network attempt reservation. `profile.worker`
is authoritative and is not overridden by an environment variable.
Only trusted, fully drained Origin 400/403 responses may be retried, at most three
Origin requests followed by one CDN request. Other statuses and ambiguous failures
are not retried; UNKNOWN accounting stays pending.

## Boundaries

- Python `ModelScopeDataset` controls repo identity, SDK legacy numeric-ID
  resolution, revision candidates, bounded tree pages and canonical object paths.
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
  uses a fresh credential-free client: no Authorization, Cookie or Referer. Both
  clients disable proxies, automatic redirects and retries. Only the gated first
  302 Location is accepted, and no further CDN redirect is followed.
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
P3 or publish a user package. Combined paths, proof, raw spool + stage + durable
capacity and memory feasibility are checked before metadata/body HTTP.

```console
sakura index scan-remote --config production.json --plan admin-plan.json --output-package D:/SakuraTool/SakuraPool-P4-work/new-durable --mode download-then-scan
sakura index scan-remote --config production.json --plan admin-plan.json --output-package D:/SakuraTool/SakuraPool-P4-work/new-durable --mode remote-stream-scan
```

`download-then-scan`: Rust completes a bounded raw TAR spool before invoking the
unchanged file scanner. `remote-stream-scan`: Response Read goes through a counting
tee directly into the unchanged reader scanner, while writing the same raw spool.
Python audits file extents, per-member digests and JSON payload, builds the shared
`members.sqlite` / `stage.complete` contract, and invokes unchanged P2 v4 writing.
Both modes use a **whole local TAR spool** temporarily; neither is a diskless build.
The stamp is last, only after complete stream and extent/hash/size validation.

After separately authorized P3 compile/verify, `publish_local_package(...,
production_transport=...)` verifies the same frozen game repository chain and new
proof. Package contract, P2 schemas/ObjectId/RecordKey and P3 snapshot identities
are unchanged. The offline acceptance tests exercise both modes -> P2 -> P3
query -> audited package -> exact Rust image/JSON Range fetch.

Conservative transport inflight reservations are `4*N + 65,536` for Range and
`64*object_size + 4,194,304` for FullStream/report audit. The latter accounts for
existing scanner report/JSON expansion, not whole Python raw-TAR buffering. The
fixed 256 MiB inflight and 4 GiB disk limits are not increased. Together with
1152 MiB stage and 1152 MiB durable allowance, the observed 1,401,159,680 B real TAR
fails feasibility; both production builders remain BLOCKED for that target.
The administrator gate conservatively retains the durable-build allowance across
staging: existing root occupancy plus the two 1152 MiB leases currently also exceeds
the 4 GiB cap for a small object. The lower-level offline pipelines passing is not
an administrator production READY verdict. No root swap, cleanup, cap increase or
new dynamic stage-cap contract is used to evade this limit.
No synthetic throughput or whole-TAR streaming benchmark is a production claim.

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
