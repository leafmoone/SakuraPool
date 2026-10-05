# HTTP status finalization — limited safety design

Base e049a7c84031301faef592cec3f11ce428fde640 preserved. Old user and diagnostic UNKNOWN leases frozen. Exactly eight conservative network codes remain; HTTP status never enters conservative maximum classification.

## Drain and payload

Explicit protocol constant CONTROL_RESPONSE_BODY_CAP=65536 in Rust production.rs and Python production_resources.py with cross-language consistency test. One helper validates singleton decimal Content-Length, no CL+TE, TE only chunked, identity encoding. No CL/TE may complete only through EOF. Declared overcap rejects incomplete; actual cap+1 probe counts then rejects. Read/framing/noEOF failures UNKNOWN. Fixed scratch bytes discarded without parsing/logging/output.

Origin headers/status/drain precede302 Location checks; only non302 with finished drain may be origin_status. Location/framing failures cannot receive status evidence. CDN starts complete=false, preventing Origin EOF from masking failure. Unexpected CDN status uses same helper; wrong-condition accepts only412, other status is rejection, not proof. Expected412 uses helper, expected206/200 keeps existing payload validation.

Account body is control+payload. Range and scan payload count/hash/size are separate; CountTee aggregates both counters including failures. Origin control never contaminates returned payload bytes or SHA.

## Shared budget

production_resources helper uses origin_max65537 plus cdn_max=max(business_size+1,65537), wrong-condition cdn_max65537. Checked uint64. Range1/wrong131074;8MiB8454146; proof observe1+positive1+wrong1=393222. Scan existing size+1 overflow retained. Reserve maxima, consume actual only, unused settled.

Apply to _call_accounted/admit_generation/predict_warm/warm admission, StreamPlan.generation_body, proof-group admission and runner preflight. Metadata listing budget distinct. Generation credit256*maxrequest checked; not user quota; no reduced overflow probe.

## Completed status

Valid matching envelope+completeTrue+actual consume+both network settlements required before narrow status candidate. Only origin_status/cdn_status with matching phase/actual rejected status. Owner finalizer completes resource gates before status evidence token, without conservative basis. Earlier uncertainty/cleanup/worker/output/settlement failure =>UNKNOWN. Partial settlement preserved. Persistent defer only owner finalizer; capability owner needs its own closure, not transfer masquerade. Complete token allows next diagnostic iteration; task current run stops READY and requires explicit resume. Outer NOT_PUBLISHED/cleanup/output/no-secondary/earlierUNKNOWN gates remain.

## Tree retry ownership

Validated legacy_tree_page alone passes explicit400/403 policy through _data to read_metadata/_response. Existing one MAX_ATTEMPTS loop shares redirects, no outer retry. Intermediate response is closed and lease settled before continue. At final attempt return unclosed/unsettled response to read_metadata, which alone closes/settles and raises confirmed http_status; alternatively direct confirmed exception after single ownership closure, never both. Explicit tree path AmbiguousRead stops immediately preserving UNKNOWN/pending, no change to non-tree historical retry semantics.401/other4xx/shape/entry/digest failures do not retry;429/5xx retained.

## Paths/tests

rust/src/production.rs and production/loopback status tests; Python storage/production_resources.py,prepared_fetch.py,production.py,transport.py,modelscope.py,publication_fetch.py,tasks/runner.py; pipeline only if safe propagation requires. reports/P6A/origin_soak.py distinguishes actual completed-status evidence from conservative evidence. No private response bodies/header secret values in reports.

Framing/EOF/overflow/readerror,302 empty/CL/chunked,400/404 complete/incomplete,412 compatibility,payload separation,lease failures,persistent positive path,400/403 sequences/exhaust/auth/shape/ambiguous/sharedlimit/4295xx tests. Four network plus affected consumers only. Explicit GNU PATH C:/msys64/ucrt64/bin, unset global CARGO_TARGET_DIR, locked dedicated build and Rustworker/protocol tests after source changes. New real diagnostics only after review/test gates; never replay previous UNKNOWN.
