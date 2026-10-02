"""Explicit, fail-closed real canary; stdout contains only fixed diagnostics/counts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from sakurapool.storage.budget import RESERVED, BudgetLedger
from sakurapool.storage.modelscope import MAX_PAGES, ModelScopeDataset
from sakurapool.storage.production_cli import load_profile
from sakurapool.storage.publication import bounded, build_publication, load_publication
from sakurapool.storage.publication_fetch import fetch_publication_sample
from sakurapool.storage.transport import GuardedTransport, RemoteIOError

P2 = Path("D:/SakuraTool/SakuraPool-P4-work/r2c2-canary-814e29b819204789a69b7a66e775d81c/p2")
OBJECT = "gc5m/t0111-002.tar"
WHOLE = "b5c99d540346c6d93bf1d239d04a220f74240eb540b169aea3e7643ccd401234"
REV = "409a704627c429cdf55fd7b156eb23e61232a331"
EXPECTED_WORK = Path("D:/SakuraTool/SakuraPool-R2C3-real-20261002")
EXPECTED_WORKER_STAT = (4388952, 1790941121849044400)
PHASES = (
    "PROFILE",
    "LOCAL_RUNTIME",
    "LEDGER",
    "PROVIDER_HUB",
    "PROVIDER_LISTING",
    "EXACT_OBJECT",
    "PROVIDER_DIGEST",
    "PUBLICATION_BUILD",
    "PUBLICATION_VERIFY",
    "FETCH_BINDING",
    "FETCH_IMAGE",
    "FETCH_METADATA",
    "COMPLETE",
)
IDENTITY_CODES = frozenset(
    {
        "EXACT_OBJECT_NOT_FOUND",
        "EXACT_OBJECT_DUPLICATE",
        "OBJECT_SIZE_MISMATCH",
        "OBJECT_REVISION_MISMATCH",
    }
)
REMOTE_CODES = {
    "remote_io": "REMOTE_IO",
    "network_ambiguous": "NETWORK_AMBIGUOUS",
    "http_status": "HTTP_STATUS",
    "metadata_encoding": "METADATA_ENCODING",
    "redirect_policy": "REDIRECT_POLICY",
    "invalid_json": "INVALID_JSON",
    "provider_shape": "PROVIDER_SHAPE",
    "provider_rejection": "PROVIDER_REJECTION",
    "retry_policy": "RETRY_POLICY",
    **{
        code: code.upper()
        for code in (
            "provider_page_shape",
            "provider_entry_shape",
            "provider_entry_type",
            "provider_entry_path",
            "provider_entry_duplicate",
            "provider_entry_scope",
            "provider_entry_size",
            "provider_entry_revision_shape",
            "provider_entry_digest",
        )
    },
}
REMOTE_PHASES = frozenset(
    {
        "transport",
        "metadata_send",
        "metadata_headers",
        "metadata_body",
        "provider_revision_shape",
        "provider_listing_shape",
        "provider_tree_shape",
        "provider_repository_shape",
        "response_headers",
        "two_hop_redirect",
    }
)
PUBLICATION_COUNTS = (
    "rid_count",
    "object_count",
    "fetchable_object_count",
    "fetchable_rid_count",
)


def classify_provider_object(rows, expectedpath, size, rev, contentSHA):
    """Classify already-parsed rows without returning any provider values."""
    matches = [row for row in rows if row.path == expectedpath]
    if not matches:
        return "EXACT_OBJECT_NOT_FOUND"
    if len(matches) != 1:
        return "EXACT_OBJECT_DUPLICATE"
    row = matches[0]
    if row.size != size:
        return "OBJECT_SIZE_MISMATCH"
    if row.revision_candidate != rev:
        return "OBJECT_REVISION_MISMATCH"
    if row.provider_sha256 is None:
        return "PROVIDER_SHA_UNAVAILABLE"
    if row.provider_sha256 == contentSHA:
        return "PROVIDER_SHA_MATCH"
    return "PROVIDER_SHA_MISMATCH"


def record_lookup(result, classification):
    result["provider_object_code"] = classification
    result["real_lookup_code"] = classification
    result["real_lookup_phase"] = (
        "EXACT_OBJECT" if classification in IDENTITY_CODES else "PROVIDER_DIGEST"
    )
    result["exact_object_found"] = "NO" if classification == "EXACT_OBJECT_NOT_FOUND" else "YES"
    # Duplicate rows establish existence, but no unique object's identity/digest.
    if classification in {"EXACT_OBJECT_NOT_FOUND", "EXACT_OBJECT_DUPLICATE"}:
        return
    result["exact_object_size"] = "FAIL" if classification == "OBJECT_SIZE_MISMATCH" else "PASS"
    if classification == "OBJECT_SIZE_MISMATCH":
        return
    result["exact_object_revision"] = (
        "FAIL" if classification == "OBJECT_REVISION_MISMATCH" else "PASS"
    )
    if classification == "OBJECT_REVISION_MISMATCH":
        return
    result["provider_sha_present"] = "NO" if classification == "PROVIDER_SHA_UNAVAILABLE" else "YES"


def failure_code(error, phase):
    # The helper phase is authoritative: a strict full-page parser can fail before
    # exact-object classification. Never infer identity/digest failures from text.
    if isinstance(error, RemoteIOError):
        diagnostic = error.public_diagnostic()
        if diagnostic.get("phase") in REMOTE_PHASES:
            return REMOTE_CODES.get(diagnostic.get("code"), "REMOTE_IO")
    return phase + "_FAILED"


def safe_counts(values, keys):
    result = {key: values[key] for key in keys}
    if any(type(value) is not int or value < 0 for value in result.values()):
        raise ValueError("invalid diagnostic counters")
    return result


def ledger_snapshot(ledger):
    # status() combines settled usage and pending reservations. Read its same
    # locked state to distinguish those without exposing lease IDs or proofs.
    with ledger._locked():
        _index, (_generation, used, leases, _proofs) = ledger._read_pair()
        totals = ledger._totals(used, leases)
        pending = {key: sum(row[key] for row in leases.values()) for key in RESERVED}
        return dict(
            totals=safe_counts(totals, (*RESERVED, "attempts")),
            settled=safe_counts(
                used, ("body", "metadata", "records", "saved_samples", "saved_bytes", "attempts")
            ),
            pending_unknown=safe_counts(pending, RESERVED),
            pending_lease_count=len(leases),
        )


def check_profile(profile):
    config = bounded(profile, 1 << 16)
    expected = dict(
        profile="modelscope_https_v1",
        origin="https://modelscope.cn",
        repo_id="leafmoone/webdataset_danbooru_v3",
        revision=REV,
        worker="D:/SakuraTool/SakuraPool-Fix3-20260930a/rust-target/release/sakurapool-worker.exe",
        token_file="D:/sm_data/ms-token.tmp",
    )
    if not isinstance(config, dict) or set(config) != set(expected) | {"object"}:
        raise ValueError("profile scope rejected")
    if any(config[k] != v for k, v in expected.items()):
        raise ValueError("profile scope rejected")
    obj = config["object"]
    required = dict(
        repo_id=expected["repo_id"],
        repo_type="modelscope_dataset_legacy",
        origin=expected["origin"],
        revision=REV,
        object_path=OBJECT,
        object_size=5201920,
    )
    if not isinstance(obj, dict) or set(obj) != set(required) | {"validator"}:
        raise ValueError("profile object scope rejected")
    if (
        any(obj.get(k) != v for k, v in required.items())
        or obj.get("validator") is not None
        or obj.get("cdn_host") is not None
    ):
        raise ValueError("profile object scope rejected")


def run(work, profile):
    result = dict(
        status="STOP",
        phase="PROFILE",
        code="PROFILE_FAILED",
        real_lookup_phase="NOT_RUN",
        real_lookup_code="NOT_RUN",
        exact_object_found="NOT_ESTABLISHED",
        exact_object_size="NOT_ESTABLISHED",
        exact_object_revision="NOT_ESTABLISHED",
        provider_sha_present="NOT_ESTABLISHED",
        real_publication_canary="NOT_RUN",
        real_v2_fetch="NOT_RUN",
        provider_sha_match="NOT_RUN",
        fetch_binding="NOT_RUN",
        fetch_image="NOT_RUN",
        fetch_metadata="NOT_RUN",
        index_machine_operations=0,
        production_rescan=False,
    )
    state = {"ledger": None}
    try:
        _run(work, profile, result, state)
    except Exception as error:
        result["status"] = "STOP"
        if result["code"] == "IN_PROGRESS":
            result["code"] = failure_code(error, result["phase"])
        if result["phase"] in {
            "PROVIDER_HUB",
            "PROVIDER_LISTING",
            "EXACT_OBJECT",
            "PROVIDER_DIGEST",
        }:
            result["real_lookup_phase"] = result["phase"]
            result["real_lookup_code"] = result["code"]
    finally:
        if state["ledger"] is not None:
            try:
                result["ledger_after"] = ledger_snapshot(state["ledger"])
            except Exception:
                result["ledger_after_status"] = "UNAVAILABLE"
                if result["status"] == "PASS":
                    result.update(status="STOP", phase="LEDGER", code="LEDGER_AFTER_FAILED")
    return result


def _run(work, profile, result, state):
    result["code"] = "IN_PROGRESS"
    check_profile(profile)  # Public scope first; no credentials read before this gate.
    from sakurapool.runtime.inventory import load_p2_inventory
    from sakurapool.runtime.snapshot import RuntimeSnapshot
    from sakurapool.storage.publication import plain

    result["phase"] = "LOCAL_RUNTIME"
    if work != EXPECTED_WORK:
        raise ValueError("real workspace rejected")
    plain(work, True)
    plain(work / "runtime", True)
    for path in work.rglob("*"):
        plain(path, path.is_dir())
    for name in ("publication", "p2-list.json", "remote-map.jsonl"):
        if (work / name).exists() or (work / name).is_symlink():
            raise ValueError("real output already exists")
    inventory = load_p2_inventory(P2)
    with RuntimeSnapshot.open(work / "runtime", full_verify=True) as runtime:
        if (
            runtime.rid_count != 121
            or runtime.manifest["source_fingerprint"] != inventory.source_fingerprint
        ):
            raise ValueError("real runtime identity rejected")
    worker = plain(
        "D:/SakuraTool/SakuraPool-Fix3-20260930a/rust-target/release/sakurapool-worker.exe"
    )
    worker_stat = worker.stat()
    if (worker_stat.st_size, worker_stat.st_mtime_ns) != EXPECTED_WORKER_STAT:
        raise ValueError("release worker changed")
    result["phase"] = "LEDGER"
    ledger = state["ledger"] = BudgetLedger()
    result["ledger_before"] = ledger_snapshot(ledger)
    output = ledger.root / "r2c3-publication-fetch"
    if output.exists() or output.is_symlink():
        raise ValueError("real fetch output exists")
    result["phase"] = "PROFILE"
    _config, scope, transport = load_profile(profile, ledger=ledger)
    with transport:
        result["phase"] = "PROVIDER_HUB"
        with GuardedTransport(
            ledger,
            trusted_hosts=frozenset({"modelscope.cn"}),
            token=transport._token,
            credential_origin=scope.origin,
            same_origin_cookie=transport._cookie,
        ) as control:
            provider = ModelScopeDataset(control, scope.origin, scope.repo_id)
            hub = provider.legacy_hub_id()
            matches = []
            result["provider_pages"] = 0
            for page in range(1, MAX_PAGES + 1):
                result["phase"] = "PROVIDER_LISTING"
                rows, complete = provider.legacy_tree_page(
                    hub, REV, root="gc5m", page=page, page_size=200
                )
                result["provider_pages"] += 1
                matches = [row for row in rows if row.path == OBJECT]
                if matches or complete or not rows:
                    break
            result["phase"] = "EXACT_OBJECT"
            classification = classify_provider_object(matches, OBJECT, 5201920, REV, WHOLE)
            record_lookup(result, classification)
            if classification in IDENTITY_CODES:
                result["code"] = classification
                raise ValueError("exact object rejected")
            result["phase"] = "PROVIDER_DIGEST"
            if classification == "PROVIDER_SHA_MISMATCH":
                result.update(code=classification, provider_sha_match="FAIL")
                raise ValueError("provider digest rejected")
            digest = matches[0].provider_sha256
            result["provider_sha_match"] = "UNAVAILABLE" if digest is None else "PASS"
        result["phase"] = "PUBLICATION_BUILD"
        roots = work / "p2-list.json"
        with roots.open("x", encoding="utf-8") as stream:
            stream.write(json.dumps(dict(format="sakurapool-p2-root-list-v1", roots=[str(P2)])))
        mapping = work / "remote-map.jsonl"
        row = dict(
            dataset_id="gamecg_v3",
            endpoint=scope.origin,
            repo_id=scope.repo_id,
            repo_type=scope.repo_type,
            revision_candidate=REV,
            object_path=OBJECT,
            object_size=5201920,
            provider_sha256=digest,
        )
        with mapping.open("x", encoding="utf-8") as stream:
            stream.write(json.dumps(row) + "\n")
        m = build_publication(work / "runtime", roots, mapping, work / "publication")
        result["publication"] = safe_counts(m, PUBLICATION_COUNTS)
        result["phase"] = "PUBLICATION_VERIFY"
        with load_publication(work / "publication", full_verify=True) as pub:
            if pub.runtime.rid_count != 121:
                raise ValueError("publication real count mismatch")
            if digest is None:
                if m["fetchable_object_count"] != 0 or m["fetchable_rid_count"] != 0:
                    raise ValueError("null provider fetchable mismatch")
                result["real_v2_fetch"] = "BLOCKED_PROVIDER_SHA_UNAVAILABLE"
            result["real_publication_canary"] = "PASS"
            if digest is not None:
                result["phase"] = "FETCH_BINDING"
                output.mkdir(exist_ok=False)
                record = (
                    pub.runtime._catalog.execute(
                        "SELECT record_id FROM records ORDER BY rid LIMIT 1"
                    )
                    .fetchone()[0]
                    .hex()
                )
                # No product callbacks: any invocation failure remains FETCH_BINDING.
                # Successful return guarantees own binding, image SHA and settlement.
                final = fetch_publication_sample(
                    pub, record, transport, output, metadata=True, scope=scope
                )
                result["fetch_binding"] = "PASS"
                result["phase"] = "FETCH_IMAGE"
                result["fetch_image"] = "PASS"
                result["phase"] = "FETCH_METADATA"
                result["fetch_metadata"] = (
                    "PASS" if (final / "metadata.json").is_file() else "ABSENT"
                )
                result["real_v2_fetch"] = "PASS"
    result.update(status="PASS", phase="COMPLETE", code="PASS")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if not args.execute:
        print("HOLD: explicit --execute required")
        return 0
    result = run(args.work, args.profile)
    try:
        print(json.dumps(result, sort_keys=True))
    except Exception:
        return 1  # Evidence failure stops; never continue network or claim PASS.
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
