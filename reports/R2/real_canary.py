"""Single minimal R2 capability batch, no full-TAR/scan/benchmark or raw URL output.

Fixed target is the user-requested game repository, no historical object path.
This script must only run after the finite plan has been announced.
"""

from __future__ import annotations

import json
from dataclasses import asdict, replace
from pathlib import Path

from sakurapool.storage.budget import BudgetLedger, _disk_usage, is_reparse
from sakurapool.storage.modelscope import ModelScopeDataset
from sakurapool.storage.production import ProviderObject, RustProductionTransport, proof_key
from sakurapool.storage.transport import GuardedTransport

ORIGIN = "https://modelscope.cn"
REPO = "leafmoone/game_cg_5M"
TOKEN_FILE = Path("D:/sm_data/ms-token.tmp")
WORKER = Path("D:/SakuraTool/SakuraPool-Fix3-20260930a/rust-target/release/sakurapool-worker.exe")
MIB = 1 << 20


def budget(ledger):
    with ledger._locked():
        _index, (_generation, used, leases, _proofs) = ledger._read_pair()
        return {
            "total_including_pending": ledger._totals(used, leases),
            "known_settled": used,
            "pending_leases": len(leases),
            "pending_unobserved_body_bound": sum(
                v["body"] - v["consumed_body"] for v in leases.values()
            ),
            "physical_root_bytes": _disk_usage(ledger.root),
        }


def main():
    report = {
        "repo_id": REPO,
        "origin": ORIGIN,
        "plan": {
            "max_control_attempts": 9,
            "max_control_entity_bytes": 9 * (MIB + 1),
            "max_data_attempts": 6,
            "max_data_entity_bytes": 6,
            "requested_canary_bytes_per_positive": 1,
            "fullstream_or_scan": False,
        },
        "historical_unledgered_requests_and_bytes": "unknown",
        "repository_revision_immutability": "unknown",
        "provider_validator": "unknown",
        "content_sha256": "unknown",
        "conditional_semantics": "unknown",
        "range": "unknown",
    }
    ledger = BudgetLedger()
    report["budget_before"] = budget(ledger)
    transport = None
    try:
        # Check finite incremental admission headroom before touching any credential/network.
        before = ledger.status()
        if (
            before["attempts"] + 15 > ledger.limits["attempts"]
            or before["body"] + 9 * (MIB + 1) + 6 > ledger.limits["body"]
            or before["metadata"] + 9 * (MIB + 1) > ledger.limits["metadata"]
            or before["disk"] + 16384 > ledger.limits["disk"]
            or before["inflight"] + 2 * MIB > ledger.limits["inflight"]
        ):
            raise ValueError("finite batch exceeds durable limits")
        if not TOKEN_FILE.is_file() or is_reparse(TOKEN_FILE) or TOKEN_FILE.stat().st_size > 4096:
            raise ValueError("authorized credential file unavailable")
        token = TOKEN_FILE.read_text(encoding="utf-8").strip()
        if not token or any(ord(c) < 0x21 or ord(c) >= 0x7F for c in token):
            raise ValueError("credential file invalid")
        with GuardedTransport(
            ledger,
            trusted_hosts=frozenset({"modelscope.cn"}),
            token=token,
            credential_origin=ORIGIN,
            same_origin_cookie="m_session_id=" + token,
        ) as control:
            dataset = ModelScopeDataset(control, ORIGIN, REPO)
            hub_id = dataset.legacy_hub_id()  # SDK routing, exact live owner/name match
            report["current_hub_id"] = hub_id
            # 'pre' is a previously observed folder, revalidated here, NOT an invented TAR path.
            files, complete = dataset.legacy_tree_page(hub_id, "master", root="pre")
            report["discovery_entries_observed"] = len(files)
            report["discovery_complete"] = complete
            tars = [f for f in files if f.path.endswith(".tar") and 0 < f.size <= 2 * (1 << 30)]
            if not tars:
                raise ValueError("bounded current tree supplies no actual TAR path")
            first = min(tars, key=lambda f: (f.size, f.path))
            files, complete = dataset.legacy_tree_page(hub_id, first.revision_candidate, root="pre")
            matches = [
                f
                for f in files
                if f.path == first.path
                and f.size == first.size
                and f.revision_candidate == first.revision_candidate
            ]
            if len(matches) != 1:
                raise ValueError("current candidate tree differs")
            report["tree_entries_observed"] = len(files)
            report["tree_complete"] = complete
            candidate = ProviderObject.from_tree(dataset, matches[0])
            if token in json.dumps(asdict(candidate)):
                raise ValueError("control object echoed credential")
            report["object"] = asdict(candidate)
        transport = RustProductionTransport(
            ledger, WORKER, origin=ORIGIN, token=token, same_origin_cookie="m_session_id=" + token
        )
        with transport.transfer(candidate, condition="observe") as (_, observed):
            report["observed"] = observed
            report["range"] = "observed"
            report["provider_validator"] = "observed"
            selected = replace(candidate, validator=observed["etag"], cdn_host=observed["cdn_host"])
            digest = observed["sha256"]  # 1-byte digest, explicitly NOT whole-object SHA
        with transport._capability_match(selected) as positive:
            report["positive"] = positive
            if positive["sha256"] != digest:
                raise ValueError("positive probe changed bound byte")
        with transport.transfer(selected, condition="wrong") as (_, negative):
            report["negative"] = negative
            if negative["status"] != 412:
                raise ValueError("negative probe did not reject")
        ledger.record_condition_proof(proof_key(selected), digest)
        transport.register(selected)
        report["conditional_semantics"] = "verified"
        report["range"] = "verified"
        report["conditional_object"] = asdict(selected)
        report["status"] = "MINIMAL_CAPABILITY_VERIFIED"
    except Exception:
        # No exception strings, provider messages, metadata bodies, signed URLs or credentials.
        report["status"] = "BLOCKED"
        if transport is not None and hasattr(transport, "last_result"):
            report["last_rust_result"] = transport.last_result
            if transport.last_result.get("production_error") == "conditional_unsupported":
                report["conditional_semantics"] = "unsupported"
        report["approved_alternative_binding"] = "none; no fallback authorized"
    finally:
        report["budget_after"] = budget(ledger)
    print(json.dumps(report, sort_keys=True))
    return 0 if report["status"] == "MINIMAL_CAPABILITY_VERIFIED" else 3


if __name__ == "__main__":
    raise SystemExit(main())
