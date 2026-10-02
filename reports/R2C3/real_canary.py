"""Retained P2 publication canary, fresh exact provider digest, no rescans."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from sakurapool.storage.budget import BudgetLedger
from sakurapool.storage.modelscope import MAX_PAGES, ModelScopeDataset
from sakurapool.storage.production_cli import load_profile
from sakurapool.storage.publication import bounded, build_publication, load_publication
from sakurapool.storage.publication_fetch import fetch_publication_sample
from sakurapool.storage.transport import GuardedTransport

P2 = Path("D:/SakuraTool/SakuraPool-P4-work/r2c2-canary-814e29b819204789a69b7a66e775d81c/p2")
OBJECT = "gc5m/t0111-002.tar"
WHOLE = "b5c99d540346c6d93bf1d239d04a220f74240eb540b169aea3e7643ccd401234"
REV = "409a704627c429cdf55fd7b156eb23e61232a331"
EXPECTED_WORK = Path("D:/SakuraTool/SakuraPool-R2C3-real-20261002")
EXPECTED_WORKER_STAT = (4388952, 1790941121849044400)


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
    check_profile(profile)  # Public scope first; no credentials read before this gate.
    from sakurapool.runtime.inventory import load_p2_inventory
    from sakurapool.runtime.snapshot import RuntimeSnapshot
    from sakurapool.storage.publication import plain

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
    ledger = BudgetLedger()
    before = ledger.status()
    if (ledger.root / "r2c3-publication-fetch").exists():
        raise ValueError("real fetch output exists")
    _config, scope, transport = load_profile(profile, ledger=ledger)
    with transport:
        with GuardedTransport(
            ledger,
            trusted_hosts=frozenset({"modelscope.cn"}),
            token=transport._token,
            credential_origin=scope.origin,
            same_origin_cookie=transport._cookie,
        ) as control:
            provider = ModelScopeDataset(control, scope.origin, scope.repo_id)
            hub = provider.legacy_hub_id()
            found = None
            for page in range(1, MAX_PAGES + 1):
                rows, complete = provider.legacy_tree_page(
                    hub, REV, root="gc5m", page=page, page_size=200
                )
                matches = [r for r in rows if r.path == OBJECT]
                if matches:
                    if len(matches) != 1:
                        raise ValueError("duplicate exact provider object")
                    found = matches[0]
                    break
                if complete or not rows:
                    break
            if found is None or found.size != 5201920 or found.revision_candidate != REV:
                raise ValueError("provider exact identity mismatch")
            digest = found.provider_sha256
        if digest is not None and digest != WHOLE:
            raise ValueError("provider digest mismatch")
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
        with load_publication(work / "publication", full_verify=True) as pub:
            if pub.runtime.rid_count != 121:
                raise ValueError("publication real count mismatch")
            fetch = "BLOCKED_PROVIDER_DIGEST_UNAVAILABLE"
            if digest is None:
                if m["fetchable_object_count"] != 0 or m["fetchable_rid_count"] != 0:
                    raise ValueError("null provider fetchable mismatch")
            if digest is not None:
                output = ledger.root / "r2c3-publication-fetch"
                output.mkdir(exist_ok=False)
                record = (
                    pub.runtime._catalog.execute(
                        "SELECT record_id FROM records ORDER BY rid LIMIT 1"
                    )
                    .fetchone()[0]
                    .hex()
                )
                fetch_publication_sample(pub, record, transport, output, metadata=True, scope=scope)
                fetch = "PASS"
    return dict(
        real_publication_canary="PASS",
        real_v2_fetch=fetch,
        provider_sha_match="UNAVAILABLE" if digest is None else "PASS",
        publication=m,
        ledger_before=before,
        ledger_after=ledger.status(),
        index_machine_operations=0,
        production_rescan=False,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if not args.execute:
        print("HOLD: explicit --execute required")
        return 0
    try:
        result = run(args.work, args.profile)
    except Exception:
        # Never print exception, chain, request data, profile or secret file contents.
        print(json.dumps(dict(status="STOP", phase="REAL_CANARY", code="FAIL_CLOSED")))
        return 1
    try:
        encoded = json.dumps(result, sort_keys=True)
        print(encoded)
    except Exception:
        return 1  # Evidence failure stops; never continue network or claim PASS.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
