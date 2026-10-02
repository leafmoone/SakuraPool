"""Configured canaries preserve registry policy and never infer an adapter."""

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_r2c2_binding import load_c2_helpers
from test_r2c2_binding import twohop as _twohop
from test_r2c2_identification import archive, configure, nested

from sakurapool.registry import AdapterRegistry

twohop = _twohop


def test_configured_adapter_changes_only_storage():
    _, canary = load_c2_helpers()
    root = Path(__file__).resolve().parents[1]
    registry = AdapterRegistry.from_dict(
        json.loads((root / "examples/webdataset-danbooru-v3.json").read_text(encoding="utf-8"))
    )
    adapter = canary.configured_adapter()
    assert adapter == replace(registry.get("gamecg_v3"), storage_id="modelscope-c2")
    assert adapter.source == "gamecg"
    assert adapter.tag_namespace == "gamecg_native"
    assert adapter.allowed_provenance == ("gamecg-2D",)


def test_configured_prefix_uses_existing_provider_canonical_gate():
    from sakurapool.storage.modelscope import ModelScopeDataset

    class Control:
        def __init__(self):
            self.calls = []

        def _host(self, url):
            return "modelscope.cn"

        def read_metadata(self, url):
            self.calls.append(url)
            if "/repo/tree?" in url:
                return json.dumps({"Code": 200, "Data": {"Files": [], "TotalCount": 0}}).encode()
            return json.dumps(
                {
                    "Code": 200,
                    "Data": {
                        "Namespace": "leafmoone",
                        "Name": "webdataset_danbooru_v3",
                        "Id": 17,
                        "Type": 4,
                    },
                }
            ).encode()

    control = Control()
    provider = ModelScopeDataset(
        control, "https://modelscope.cn", "leafmoone/webdataset_danbooru_v3"
    )
    assert provider.legacy_hub_id() == 17
    assert provider.legacy_tree_page(17, "master", root="gc5m") == ([], True)
    with pytest.raises(ValueError):
        provider.legacy_tree_page(17, "master", root="gc5m/")


def test_configured_repository_prefix_differs_from_logical_source():
    _, canary = load_c2_helpers()
    adapter = canary.configured_adapter()
    assert canary.CONFIGURED_ROOTS == ("gc5m",)
    assert canary.CONFIGURED_DATASET == "gamecg_v3"
    assert adapter.source == "gamecg"
    assert adapter.allowed_provenance == ("gamecg-2D",)
    assert canary.CONFIGURED_ROOTS[0] != adapter.source


@pytest.mark.parametrize("proof_status", ["PASS", "SOURCE_OR_LEDGER_BLOCKED"])
@pytest.mark.parametrize(
    "download_status,terminal",
    [
        ("PASS", False),
        ("CAPACITY_BLOCKED", False),
        ("CAPACITY_BLOCKED", True),
    ],
)
def test_configured_closure_uses_own_proof_without_inference(
    monkeypatch, proof_status, download_status, terminal
):
    scheduler, canary = load_c2_helpers()
    candidate = SimpleNamespace(repo_id=canary.CONFIGURED_REPOSITORY)
    transport = SimpleNamespace(ledger=object())
    adapter = canary.configured_adapter()
    calls = []

    def discovery(*args, **kwargs):
        assert kwargs == {
            "repository": canary.CONFIGURED_REPOSITORY,
            "roots": canary.CONFIGURED_ROOTS,
        }
        calls.append("discovery")
        return candidate, {"status": "PASS"}

    def schedule(*args):
        assert args[2] is candidate
        calls.append("own_binding")
        return transport, {"status": "PASS"}

    def proof(*args):
        assert args[1] is candidate
        calls.append("own_proof")
        return {"status": proof_status}

    def build(*args, **kwargs):
        assert args[2] is adapter
        assert kwargs == {"network_readiness": True}
        calls.append(args[3])
        if args[3] == "download-then-scan":
            return Path("unused-download"), {
                "status": download_status,
                "operation_failed": terminal,
                "observation": {"http_status": 503} if terminal else {},
            }
        return Path("unused-p2"), {"status": "PASS"}

    monkeypatch.setattr(canary, "discovery", discovery)
    monkeypatch.setattr(scheduler, "schedule", schedule)
    monkeypatch.setattr(scheduler, "proof_use", proof)
    monkeypatch.setattr(canary, "identify_adapter", lambda *a: pytest.fail("inference forbidden"))
    monkeypatch.setattr(canary, "build_one", build)
    monkeypatch.setattr(canary, "equivalent", lambda *a: {"status": "PASS"})
    monkeypatch.setattr(scheduler, "execute_evidenced", lambda *a, **k: a[3]())
    result = canary.closure(
        transport,
        candidate,
        "fixture",
        scheduler,
        {},
        resume_discovery=True,
        repository=canary.CONFIGURED_REPOSITORY,
        roots=canary.CONFIGURED_ROOTS,
        adapter=adapter,
    )
    assert calls[:3] == ["discovery", "own_binding", "own_proof"]
    if proof_status == "PASS":
        assert calls[3:] == ["remote-stream-scan", "download-then-scan"]
        if terminal:
            assert result["status"] == download_status
            assert result["operation_failed"]
            assert result["observation"] == {"http_status": 503}
        elif download_status == "PASS":
            assert result["status"] == "PASS"
            assert result["equivalence"] == "PASS"
        else:
            assert result["status"] == "REMOTE_READY_DOWNLOAD_CAPACITY_BLOCKED"
    else:
        assert len(calls) == 3
        assert result["status"] == proof_status
        assert result["origin_phase"] == "own_registered_range"


@pytest.mark.parametrize(
    "provenance,dataset,success,padding",
    [
        ("gamecg-2D", "gamecg_v3", True, 0),
        ("gamecg-2D", "gamecg_v3", False, 2 << 20),
        ("somethingelse", "gamecg_v3", False, 0),
        ("gamecg-2D", "danbooru_v3", False, 0),
    ],
)
def test_configured_formal_build_and_reopen(
    twohop, monkeypatch, provenance, dataset, success, padding
):
    meta = nested()
    meta["source"]["dataset"] = provenance
    meta["padding"] = "x" * padding
    # Offline-only isolation; hard caps unchanged, no real root substitution.
    ledger = twohop[1]
    assert ledger.offline_mode
    _, ledger, transport, obj, scheduler, canary, identity = configure(
        twohop, monkeypatch, archive(meta)
    )
    root = Path(__file__).resolve().parents[1]
    registry = AdapterRegistry.from_dict(
        json.loads((root / "examples/webdataset-danbooru-v3.json").read_text(encoding="utf-8"))
    )
    adapter = replace(registry.get(dataset), storage_id="modelscope-c2")
    monkeypatch.setattr(canary, "identify_adapter", lambda *a: pytest.fail("inference forbidden"))
    stages = []
    original_audit = canary.audit_small_sample

    def capture_audit(stage, *args):
        stages.append(stage)
        return original_audit(stage, *args)

    monkeypatch.setattr(canary, "audit_small_sample", capture_audit)
    p2, report = canary.build_one(
        transport,
        obj,
        adapter,
        "remote-stream-scan",
        scheduler,
        identity,
        network_readiness=True,
    )
    if success:
        assert report["status"] == "PASS", report
        assert p2 is not None
        assert report["inventory_reopened"] and report["inventory_verified"]
        assert report["runtime"] == "NOT_REQUIRED_FOR_NETWORK_READINESS"
        assert not report["compiler_executed"] and not report["runtime_verified"]
        assert report["summary"]["errors"] == 0
        assert report["summary"]["samples"] == 1
        assert report["json_content_sha_independently_verified"]
        assert not report["whole_tar_spool"]
        assert report["audit_batch_rows"] == 3
        assert report["audit_json_cap_bytes"] == adapter.max_json_bytes
        real_flags = canary.validate_canary_rows(p2, report["summary"], offline_fixture=False)
        assert real_flags["offline_functional_fixture_only"] is False
        # Reuse actual durable P2 but enforce a deliberately smaller explicit
        # adapter policy: audit must reject before selecting its oversized blob.
        assert stages
        with pytest.raises(ValueError, match="adapter cap"):
            original_audit(stages[0], p2, replace(adapter, max_json_bytes=1))
        download, downloaded = canary.build_one(
            transport,
            obj,
            adapter,
            "download-then-scan",
            scheduler,
            identity,
            network_readiness=True,
        )
        assert downloaded["status"] == "PASS", downloaded
        assert downloaded["inventory_reopened"]
        assert downloaded["whole_tar_spool"]
        assert canary.equivalent(ledger, p2, download)["status"] == "PASS"
    else:
        assert report["status"] != "PASS", report
        assert not report.get("scan_p2_verified")
        if padding:
            assert report["status"] == "PRODUCTION_METADATA_CAPACITY_BLOCKED", report
            assert transport.last_result["production_error"] == "metadata_limit"
            assert report["production_json_cap_bytes"] == 1 << 20
            assert report["budget_after"]["pending_leases"] == 2
            assert report["budget_after"]["pending_unknown_body_bound"] > 0
        else:
            assert report.get("summary", {}).get("errors", 1) > 0
    assert ledger.status()["inflight"] == 0


def test_equivalence_oom_is_typed_and_settled(monkeypatch):
    _, canary = load_c2_helpers()
    settled = []
    ledger = SimpleNamespace(reserve=lambda r: "lease", settle=settled.append)

    def oom(*args):
        raise MemoryError("private error")

    monkeypatch.setattr(canary, "_equivalent_rows", oom)
    report = canary.equivalent(ledger, Path("remote"), Path("download"))
    assert report == {
        "status": "OOM_BLOCKED",
        "failure_kind": "memory_error",
        "operation_failed": True,
    }
    assert settled == ["lease"]


def test_shared_domain_capacity_rejects_before_builder(twohop, monkeypatch):
    meta = nested()
    meta["source"]["dataset"] = "gamecg-2D"
    _, ledger, transport, obj, scheduler, canary, identity = configure(
        twohop, monkeypatch, archive(meta)
    )
    from sakurapool.storage import budget

    # Deterministic physical-domain pressure, not a larger cap or fake admission.
    monkeypatch.setattr(budget, "_disk_usage", lambda root: ledger.limits["disk"])
    monkeypatch.setattr(transport, "build_stage", lambda *a, **k: pytest.fail("builder attempted"))
    attempts = ledger.status()["attempts"]
    pending = scheduler.core.snapshot(ledger)["pending_leases"]
    result, report = canary.build_one(
        transport,
        obj,
        canary.configured_adapter(),
        "remote-stream-scan",
        scheduler,
        identity,
        network_readiness=True,
    )
    assert result is None and report["status"] == "CAPACITY_BLOCKED"
    assert report["network_requests"] == 0
    assert ledger.status()["attempts"] == attempts
    assert scheduler.core.snapshot(ledger)["pending_leases"] == pending


def test_registry_is_identity_dependency():
    scheduler, _ = load_c2_helpers()
    assert "examples/webdataset-danbooru-v3.json" in scheduler.DEPENDENCIES


def test_configured_cli_routes_without_old_binding(monkeypatch):
    scheduler, _ = load_c2_helpers()
    import sys

    from sakurapool.storage import budget

    monkeypatch.setitem(sys.modules, scheduler.__name__, scheduler)
    monkeypatch.setattr(budget, "BudgetLedger", lambda: object())
    monkeypatch.setattr(scheduler, "code_identity", lambda: {})
    monkeypatch.setattr(scheduler, "load_token", lambda: "fixture")
    monkeypatch.setattr(scheduler.core, "audited_transport", lambda *a, **k: object())
    monkeypatch.setattr(scheduler, "schedule", lambda *a: pytest.fail("old binding requested"))
    calls = []
    original_exec = scheduler.importlib.util.spec_from_file_location

    def spec(*args, **kwargs):
        result = original_exec(*args, **kwargs)
        original_load = result.loader.exec_module

        def load(module):
            original_load(module)

            def closure(*a, **k):
                assert k["repository"] == "leafmoone/webdataset_danbooru_v3"
                assert k["roots"] == module.CONFIGURED_ROOTS
                assert k["adapter"].dataset == "gamecg_v3"
                assert k["resume_discovery"]
                calls.append(True)
                return {"status": "PASS"}

            module.closure = closure

        result.loader.exec_module = load
        return result

    monkeypatch.setattr(scheduler.importlib.util, "spec_from_file_location", spec)
    monkeypatch.setattr(scheduler, "execute_evidenced", lambda *a, **k: a[3]())
    assert scheduler.main(["--run-authorized-c2", "--configured-canary", "gamecg_v3"]) == 0
    assert calls == [True]


def test_public_summary_keeps_large_counts_without_bool():
    _, canary = load_c2_helpers()
    assert canary.public_summary(
        {
            "objects": 1,
            "samples": 100001,
            "annotations": 9000000,
            "errors": 0,
            "private": 12,
        }
    ) == {"objects": 1, "samples": 100001, "annotations": 9000000, "errors": 0}
    assert canary.public_summary({"objects": True, "samples": -1}) == {}


def test_configured_build_oom_stops_and_settles(twohop, monkeypatch):
    ledger = twohop[1]
    meta = nested()
    meta["source"]["dataset"] = "gamecg-2D"
    _, ledger, transport, obj, scheduler, canary, identity = configure(
        twohop, monkeypatch, archive(meta)
    )

    def oom(*a, **k):
        raise MemoryError("private")

    monkeypatch.setattr(canary, "audit_small_sample", oom)
    p2, report = canary.build_one(
        transport,
        obj,
        canary.configured_adapter(),
        "remote-stream-scan",
        scheduler,
        identity,
        network_readiness=True,
    )
    assert p2 is None
    assert report["status"] == "OOM_BLOCKED"
    assert report["operation_failed"] and report["failure_kind"] == "memory_error"
    assert ledger.status()["inflight"] == 0
