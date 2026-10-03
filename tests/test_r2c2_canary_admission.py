"""Input-aware bounded discovery, streaming coverage and resume gates."""

import json
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_r2_production import twohop as _twohop
from test_r2c2_binding import load_c2_helpers

twohop = _twohop


def ledger_view(**changes):
    used = dict(attempts=75, body=285843, disk=2917654528, inflight=0)
    used.update(changes)
    return SimpleNamespace(
        offline_mode=False,
        status=lambda: used,
        limits=dict(attempts=2000, body=8 << 30, disk=4 << 30, inflight=256 << 20),
    )


def test_remote_independent_of_download_spool_and_existing_reservations():
    _, canary = load_c2_helpers()
    ledger = ledger_view()
    size = 512 << 20
    remote = canary.builder_admission(ledger, size, "remote-stream-scan", binding_needed=True)
    download = canary.builder_admission(ledger, size, "download-then-scan", binding_needed=True)
    assert remote["admitted"] and not download["admitted"]
    assert "disk" in download["blocked_resources"]
    assert remote["additional_reservations"]["body"] == size + 1 + 65541
    assert remote["is_live_reservation"] is False
    ledger.status = lambda: dict(attempts=75, body=285843, disk=(4 << 30) - 1, inflight=0)
    assert not canary.builder_admission(ledger, size, "remote-stream-scan")["admitted"]


def test_body_and_memory_are_independent_hard_resources():
    _, canary = load_c2_helpers()
    for key, value in [("body", (8 << 30) - 1), ("inflight", (256 << 20) - 1)]:
        admission = canary.builder_admission(
            ledger_view(**{key: value}), 100 << 20, "remote-stream-scan"
        )
        assert not admission["admitted"] and key in admission["blocked_resources"]


def test_discovery_all_candidates_not_only_display_minimum5(monkeypatch):
    scheduler, canary = load_c2_helpers()
    from sakurapool.storage.modelscope import ListedFile, ModelScopeDataset
    from sakurapool.storage.transport import GuardedTransport

    monkeypatch.setattr(
        GuardedTransport, "__init__", lambda self, ledger, *a, **k: setattr(self, "ledger", ledger)
    )
    monkeypatch.setattr(GuardedTransport, "close", lambda self: None)
    monkeypatch.setattr(GuardedTransport, "_host", lambda self, url: "modelscope.cn")
    monkeypatch.setattr(ModelScopeDataset, "legacy_hub_id", lambda provider: 218032)
    files = [ListedFile(f"pre/{i}.tar", (70 + i) << 20, None, False, "a" * 40) for i in range(7)]
    monkeypatch.setattr(ModelScopeDataset, "legacy_tree_page", lambda *a, **k: (files, True))
    seen = []

    def admission(ledger, size, mode, **kwargs):
        seen.append((size, mode))
        return {"admitted": size == files[-1].size}

    monkeypatch.setattr(canary, "builder_admission", admission)
    events = []

    def evidence(ledger, operation, identity, action, **kwargs):
        result, report = action()
        events.append(report)
        return result, report

    monkeypatch.setattr(scheduler, "execute_evidenced", evidence)
    candidate, report = canary.discovery(
        ledger_view(), "synthetic", scheduler, {}, repository="leafmoone/game_cg_5M"
    )
    assert candidate.object_path == files[-1].path and candidate.object_size > 64 << 20
    assert report["tar_candidates"] == 7 and report["remote_admitted_candidates"] == 1
    assert len(report["minimum_candidates"]) == 5
    assert report["candidate_evaluation_complete_for_page"]
    assert all((f.size, "remote-stream-scan") in seen for f in files)
    assert "Signature" not in json.dumps(events)


def test_isolated_synthetic_formal_download_and_equivalence_always_pass(
    twohop, monkeypatch, tmp_path
):
    # Exclusive, short-lived SYNTHETIC accounting domain; never change or clear
    # the actual production root. Loopback transport and formal builders intact.
    from test_r2c2_binding import test_c2_small_canary_uses_own_proof_and_both_formal_builders

    import sakurapool.storage.budget as budget
    import sakurapool.storage.package as package
    import sakurapool.storage.remote_index as remote_index

    state, old_ledger, transport, candidate = twohop
    fixed = tmp_path / "synthetic-domain"
    root = fixed / "ledger"
    root.mkdir(parents=True)
    for module in (budget, package, remote_index):
        monkeypatch.setattr(module, "DEFAULT_WORK_ROOT", fixed)
    ledger = budget.BudgetLedger(root, _offline_test=True)
    transport.ledger = ledger
    test_c2_small_canary_uses_own_proof_and_both_formal_builders(
        (state, ledger, transport, candidate), monkeypatch
    )
    assert ledger.status()["records"] == 2
    assert ledger.status()["attempts"] == 10
    assert ledger.status()["inflight"] == 0
    assert canary_admitted(ledger, candidate.object_size)
    assert old_ledger.root != ledger.root


def canary_admitted(ledger, size):
    _, canary = load_c2_helpers()
    return canary.builder_admission(ledger, size, "download-then-scan")["admitted"]


def test_capacity_rejection_does_not_start_builder_or_make_attempt(tmp_path, monkeypatch):
    from dataclasses import replace

    scheduler, canary = load_c2_helpers()
    ledger = ledger_view(disk=(4 << 30) - 1)
    ledger.offline_mode = True
    ledger.root = tmp_path
    candidate = scheduler.core.candidate_identity()
    obj = replace(candidate, validator='"synthetic"', cdn_host="cdn.synthetic.invalid")
    transport = SimpleNamespace(
        ledger=ledger,
        verified_object=lambda candidate: obj,
        _host=lambda url: "modelscope.cn",
        build_stage=lambda *a, **k: pytest.fail("must reject before stage/network"),
    )
    monkeypatch.setattr(
        scheduler, "execute_evidenced", lambda ledger, operation, identity, action, **k: action()
    )
    path, report = canary.build_one(transport, candidate, None, "download-then-scan", scheduler, {})
    assert path is None and report["status"] == "CAPACITY_BLOCKED"
    assert report["network_requests"] == 0 and ledger.status()["attempts"] == 75
    assert ledger.status()["inflight"] == 0 and not list(tmp_path.iterdir())


def make_tables(root, count=1100):
    root.mkdir()
    tables = {
        "objects": [{"id": "object"}],
        "errors": [],
        "samples": [{"id": i, "caption": "x" * 1100} for i in range(count)],
        "annotations": [
            {"namespace": "tags", "tags": [{"value": "same", "category": "label"}]}
            for _ in range(count)
        ],
    }
    for name, rows in tables.items():
        table = (
            pa.Table.from_pylist(rows)
            if rows
            else pa.table({"unused": pa.array([], type=pa.int64())})
        )
        pq.write_table(table, root / f"one.{name}.parquet", row_group_size=97)
    return {name: len(rows) for name, rows in tables.items()}


def test_full_stream_above_old1000_1mib_and_late_equivalence_difference(tmp_path):
    _, canary = load_c2_helpers()
    left, right = tmp_path / "left", tmp_path / "right"
    summary = make_tables(left)
    make_tables(right)
    metrics = canary.validate_canary_rows(left, summary)
    assert metrics["serialized_bytes"] > 1 << 20 and metrics["rows"] > 1000
    assert metrics["full_table_counts_verified"] == summary and metrics["all_rows_streamed"]
    assert metrics["offline_functional_fixture_only"]
    assert canary._equivalent_rows(left, right)
    path = right / "one.samples.parquet"
    rows = pq.read_table(path).to_pylist()
    rows[-1]["caption"] = "late difference after 1000"
    pq.write_table(pa.Table.from_pylist(rows), path)
    assert not canary._equivalent_rows(left, right)
    with pytest.raises(ValueError, match="count differs"):
        canary.validate_canary_rows(left, {**summary, "samples": 1000})


def test_real_profile_stops_before_unbounded_extra_decode_or_compile(tmp_path, monkeypatch):
    from dataclasses import replace

    import sakurapool.runtime.compiler as compiler
    import sakurapool.runtime.inventory as inventory
    import sakurapool.storage.remote_index as remote_index

    scheduler, canary = load_c2_helpers()
    ledger = ledger_view(disk=0)
    ledger.root = tmp_path
    candidate = scheduler.core.candidate_identity()
    obj = replace(candidate, validator='"synthetic"', cdn_host="cdn.synthetic.invalid")
    stage = SimpleNamespace(content_sha256="c" * 64, members=4002)
    transport = SimpleNamespace(
        ledger=ledger,
        verified_object=lambda candidate: obj,
        _host=lambda url: "modelscope.cn",
        build_stage=lambda *a, **k: stage,
        release_committed_downloads=lambda *a: None,
    )
    monkeypatch.setattr(
        remote_index,
        "write_staged_v4",
        lambda *a, **k: dict(objects=1, samples=2001, annotations=2001, errors=0),
    )
    for target, name in [
        (compiler, "compile_runtime"),
        (inventory, "load_p2_inventory"),
        (canary, "validate_canary_rows"),
        (canary, "audit_small_sample"),
    ]:
        monkeypatch.setattr(
            target, name, lambda *a, **k: pytest.fail("unproven native workspace must not execute")
        )
    monkeypatch.setattr(
        scheduler, "execute_evidenced", lambda ledger, operation, identity, action, **k: action()
    )
    path, report = canary.build_one(transport, candidate, None, "remote-stream-scan", scheduler, {})
    assert path is not None and report["status"] == "COMPILER_INPUT_CAPACITY_BLOCKED"
    assert report["scan_p2_verified"] and report["summary"]["samples"] == 2001
    assert not report["compiler_executed"] and not report["extra_arrow_decode_performed"]
    assert not report["inventory_reopened"] and not report["runtime_verified"]


def test_resume_does_not_replay_initial_binding_or_range(monkeypatch):
    scheduler, canary = load_c2_helpers()
    monkeypatch.setattr(scheduler, "proof_use", lambda *a: pytest.fail("must not replay initial"))
    monkeypatch.setattr(
        canary, "discovery", lambda *a, **k: (None, {"status": "DISCOVERY_NOT_IDENTIFIED"})
    )
    transport = SimpleNamespace(ledger=ledger_view())
    result = canary.closure(
        transport,
        SimpleNamespace(repo_id="leafmoone/game_cg_5M"),
        "synthetic",
        scheduler,
        {},
        resume_discovery=True,
    )
    assert result["origin_phase"] == "discovery" and result["remote"] == "NOT_RUN"


@pytest.mark.parametrize("download_status", ["CAPACITY_BLOCKED", "COMPILER_INPUT_CAPACITY_BLOCKED"])
def test_runtime_blocked_remote_scan_still_attempts_independent_download(
    monkeypatch, download_status
):
    scheduler, canary = load_c2_helpers()
    candidate = scheduler.core.candidate_identity()
    transport = SimpleNamespace(ledger=ledger_view())
    monkeypatch.setattr(canary, "discovery", lambda *a, **k: (candidate, {"status": "PASS"}))
    monkeypatch.setattr(scheduler, "schedule", lambda *a: (transport, {"status": "PASS"}))
    monkeypatch.setattr(scheduler, "proof_use", lambda *a: {"status": "PASS"})
    monkeypatch.setattr(canary, "identify_adapter", lambda *a: (None, {"status": "PASS"}))
    monkeypatch.setattr(
        canary,
        "equivalent",
        lambda *a: pytest.fail("no full decode/equivalence with unbounded runtime input"),
    )
    modes = []
    summary = dict(objects=1, samples=2001, annotations=2001, errors=0)

    def build(transport, candidate, adapter, mode, *a):
        modes.append(mode)
        status = (
            "COMPILER_INPUT_CAPACITY_BLOCKED" if mode == "remote-stream-scan" else download_status
        )
        good = status == "COMPILER_INPUT_CAPACITY_BLOCKED"
        return None, {
            "status": status,
            "scan_p2_verified": good,
            "scan_mode": mode,
            "summary": summary if good else None,
        }

    monkeypatch.setattr(canary, "build_one", build)
    result = canary.closure(transport, candidate, "synthetic", scheduler, {}, resume_discovery=True)
    assert modes == ["remote-stream-scan", "download-then-scan"]
    assert result["status"] == "BLOCKED" and result["remote_scan_p2_verified"]
    assert result["download_scan_p2_verified"] == (
        download_status == "COMPILER_INPUT_CAPACITY_BLOCKED"
    )
    assert result["remote_runtime"] == result["download_runtime"] == "NOT_RUN"
    assert result["equivalence"] == "NOT_AVAILABLE" and not result["inventory_reopened"]


@pytest.mark.parametrize(
    "failure",
    [
        {"status": "SOURCE_OR_LEDGER_BLOCKED", "identity_mismatch": True},
        {"status": "SOURCE_OR_LEDGER_BLOCKED", "ledger_snapshot_failed": True},
        {
            "status": "SETTLEMENT_BLOCKED",
            "settlement_failed": True,
            "primary_status": "COMPILER_INPUT_CAPACITY_BLOCKED",
        },
        {"status": "RUNNER_OPERATION_BLOCKED", "operation_failed": True},
        {"status": "BLOCKED", "failure_kind": "provider_or_network"},
        {"status": "BLOCKED", "failure_kind": "unknown_unclassified"},
        {"status": "CAPACITY_BLOCKED", "operation_failed": True},
    ],
)
def test_runtime_blocked_remote_preserves_download_terminal_cause(monkeypatch, failure):
    scheduler, canary = load_c2_helpers()
    candidate = scheduler.core.candidate_identity()
    transport = SimpleNamespace(ledger=ledger_view())
    monkeypatch.setattr(canary, "discovery", lambda *a, **k: (candidate, {"status": "PASS"}))
    monkeypatch.setattr(scheduler, "schedule", lambda *a: (transport, {"status": "PASS"}))
    monkeypatch.setattr(scheduler, "proof_use", lambda *a: {"status": "PASS"})
    monkeypatch.setattr(canary, "identify_adapter", lambda *a: (None, {"status": "PASS"}))
    monkeypatch.setattr(canary, "equivalent", lambda *a: pytest.fail("must not compare"))

    def build(transport, candidate, adapter, mode, *a):
        if mode == "remote-stream-scan":
            return None, {
                "status": "COMPILER_INPUT_CAPACITY_BLOCKED",
                "scan_p2_verified": True,
                "summary": dict(objects=1, samples=2001, annotations=2001, errors=0),
            }
        return None, {**failure, "observation": {"cdn_http_status": 503}}

    monkeypatch.setattr(canary, "build_one", build)
    result = canary.closure(transport, candidate, "synthetic", scheduler, {}, resume_discovery=True)
    assert result["status"] == result["download"] == failure["status"]
    assert result["origin_phase"] == "download_builder"
    for key, value in failure.items():
        assert result[key] == value
    assert result["observation"] == {"cdn_http_status": 503}
    assert result["remote_scan_p2_verified"] and result["remote_runtime"] == "NOT_RUN"
    assert result["equivalence"] == "NOT_AVAILABLE"


def test_non_runtime_remote_failure_stops_before_download(monkeypatch):
    scheduler, canary = load_c2_helpers()
    candidate = scheduler.core.candidate_identity()
    transport = SimpleNamespace(ledger=ledger_view())
    monkeypatch.setattr(canary, "discovery", lambda *a, **k: (candidate, {"status": "PASS"}))
    monkeypatch.setattr(scheduler, "schedule", lambda *a: (transport, {"status": "PASS"}))
    monkeypatch.setattr(scheduler, "proof_use", lambda *a: {"status": "PASS"})
    monkeypatch.setattr(canary, "identify_adapter", lambda *a: (None, {"status": "PASS"}))

    def build(transport, candidate, adapter, mode, *a):
        assert mode == "remote-stream-scan"
        return None, {"status": "BLOCKED", "scan_p2_verified": False}

    monkeypatch.setattr(canary, "build_one", build)
    result = canary.closure(transport, candidate, "synthetic", scheduler, {}, resume_discovery=True)
    assert result["download"] == "NOT_RUN" and result["remote"] == "BLOCKED"


def test_prior_delivery_requires_same_scope_and_product(monkeypatch, tmp_path):
    scheduler, _ = load_c2_helpers()
    monkeypatch.setattr(scheduler, "REAL", tmp_path)
    binding = {
        "operation": "verify_conditions",
        "status": "PASS",
        "VERSION_BINDING": "PASS",
        "proof_recorded": True,
        "object_registered": True,
        "proof_key": "b" * 64,
        "repo_id": scheduler.core.REPO,
        "object_path": scheduler.core.TARGET,
        "object_size": scheduler.core.SIZE,
        "revision_candidate": scheduler.core.REVISION,
        "first_byte_sha256": "cdb4ee2aea69cc6a83331bbe96dc2caa9a299d21329efb0336fc02a82e1839a8",
        "code_commit": "a" * 40,
        "round": 8,
    }
    use = {**binding, "operation": "registered_proof_range", "round": 9, "accepted_bytes": 1}
    use.pop("proof_key")
    use.pop("revision_candidate")
    (tmp_path / "round-000008.json").write_text(json.dumps(binding))
    (tmp_path / "round-000009.json").write_text(json.dumps(use))
    ledger = SimpleNamespace(condition_proof=lambda key: binding["first_byte_sha256"])
    monkeypatch.setattr(scheduler.subprocess, "run", lambda *a, **k: SimpleNamespace(returncode=0))
    result = scheduler.prior_delivery_evidence(ledger)
    assert result["prior_binding_round"] == 8 and result["prior_proof_use_round"] == 9
    assert result["network_requests"] == 0 and result["new_candidate_requires_own_proof"]
    assert not result["prior_proof_use_full_scope_revalidated"]
    assert (
        result["resume_authorizes_discovery_only"] and result["prior_binding_digest_matches_ledger"]
    )
    ledger.condition_proof = lambda key: "wrong digest"
    with pytest.raises(ValueError):
        scheduler.prior_delivery_evidence(ledger)
    ledger.condition_proof = lambda key: binding["first_byte_sha256"]
    use["object_size"] += 1
    (tmp_path / "round-000009.json").write_text(json.dumps(use))
    with pytest.raises(ValueError):
        scheduler.prior_delivery_evidence(ledger)
