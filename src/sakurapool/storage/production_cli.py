"""Explicit admin entry points; no fullscan is invoked by inspect/query/fetch.

These are deployable interfaces, not authorization to run large-repo development jobs.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict
from pathlib import Path

from .budget import DEFAULT_WORK_ROOT, BudgetLedger, Reservation, is_reparse
from .modelscope import ModelScopeDataset
from .production import ProviderObject, RustProductionTransport
from .transport import BoundObject, GuardedTransport, RemoteIOError


def load_profile(config_path: Path, *, ledger=None):
    if Path(config_path).stat().st_size > 65536:
        raise ValueError("production config too large")
    config = json.loads(Path(config_path).read_bytes())
    allowed = {"profile", "origin", "repo_id", "revision", "worker", "object", "token_file"}
    if (
        not isinstance(config, dict)
        or set(config) - allowed
        or config.get("profile") != "modelscope_https_v1"
    ):
        raise ValueError("explicit production profile required")
    obj = ProviderObject(**config["object"])
    obj.validate()
    if (
        obj.origin != config["origin"]
        or obj.repo_id != config["repo_id"]
        or obj.revision != config["revision"]
    ):
        raise ValueError("configured object scope differs")
    token = os.environ.get("MODELSCOPE_API_TOKEN") or None
    if "token_file" in config:
        path = Path(config["token_file"])
        if (
            not path.is_file()
            or path.is_symlink()
            or is_reparse(path)
            or path.stat().st_size > 4096
        ):
            raise ValueError("credential file rejected")
        token = path.read_text(encoding="utf-8").strip()
    if token and (len(token) > 4096 or any(ord(c) < 0x21 or ord(c) >= 0x7F for c in token)):
        raise ValueError("credential malformed")
    ledger = ledger if ledger is not None else BudgetLedger(DEFAULT_WORK_ROOT)
    transport = RustProductionTransport(
        ledger,
        Path(config["worker"]),
        origin=obj.origin,
        token=token,
        same_origin_cookie=("m_session_id=" + token) if token else None,
    )
    return config, obj, transport


def verify_tree_object(config, obj, transport):
    """Provider metadata only, with existing Python control plane and bounded pages."""
    from urllib.parse import urlsplit

    with GuardedTransport(
        transport.ledger,
        trusted_hosts=frozenset({urlsplit(obj.origin).hostname}),
        token=transport._token,
        credential_origin=obj.origin,
        same_origin_cookie=transport._cookie,
    ) as control:
        dataset = ModelScopeDataset(control, obj.origin, obj.repo_id)
        hub_id = dataset.legacy_hub_id()
        root = obj.object_path.rpartition("/")[0] or "/"
        files, _complete = dataset.legacy_tree_page(hub_id, obj.revision, root=root, page_size=200)
        matches = [entry for entry in files if entry.path == obj.object_path]
        if len(matches) != 1 or matches[0].size != obj.object_size:
            raise RemoteIOError("current provider tree object differs from configured binding")
        described = ProviderObject.from_tree(dataset, matches[0])
        if asdict(described) | {"validator": obj.validator, "cdn_host": obj.cdn_host} != asdict(
            obj
        ):
            raise RemoteIOError("provider structured identity differs")
    return obj


def fetch(config_path, package_root, record_id, output):
    from .package import _fetch_loaded_package, admitted_package

    if not Path(package_root).is_dir() or not (Path(package_root) / "index-package.json").is_file():
        raise ValueError("local package unavailable; production fetch never scans")
    ledger = BudgetLedger(DEFAULT_WORK_ROOT)
    # Decode/verify once under an inflight lease, before config or credentials.
    with admitted_package(package_root, ledger) as package:
        config, obj, transport = load_profile(config_path, ledger=ledger)
        if (
            package.endpoint != obj.origin
            or package.repo_id != obj.repo_id
            or package.data_revision != obj.revision
        ):
            raise ValueError("package/config scope mismatch")
        transport.register(obj)
        dest = _fetch_loaded_package(package, record_id, output, transport)
    return {
        "status": "fetched",
        "record_id": record_id,
        "output": str(dest),
        "transport": "rust",
        "profile": config["profile"],
    }


def scan(config_path, plan_path, output, mode):
    from sakurapool.registry import DatasetAdapter

    from .production_resources import ProductionFootprint
    from .remote_index import OFFLINE_BUILD_ALLOWANCE, write_staged_v4

    config, obj, transport = load_profile(config_path)
    if Path(plan_path).stat().st_size > 65536:
        raise ValueError("admin plan too large")
    plan = json.loads(Path(plan_path).read_bytes())
    if (
        not isinstance(plan, dict)
        or set(plan) != {"adapter", "stage"}
        or obj.repo_id != "leafmoone/game_cg_5M"
    ):
        raise ValueError("explicit single-repository admin plan required")
    # Combined working-set and local paths must pass before even metadata HTTP.
    for path in (Path(output).absolute(), Path(plan["stage"]).absolute()):
        if (
            ".." in path.parts
            or not path.is_relative_to(transport.ledger.root)
            or os.path.lexists(path)
            or not path.parent.is_dir()
            or any(is_reparse(p) for p in path.parents)
        ):
            raise ValueError("fresh admin output inside budget root required")
    adapter = DatasetAdapter(**plan["adapter"])
    transport.register(obj)
    records = min(100_000, max(1, obj.object_size // 512))
    footprint = ProductionFootprint.admit(mode, obj.object_size)
    preflight = transport.ledger.reserve(Reservation(
        disk=footprint.admin_peak(OFFLINE_BUILD_ALLOWANCE),
        inflight=footprint.memory, records=records))
    transport.ledger.settle(preflight)
    # No fictitious sum of historical phases. Reserve durable only when it starts.
    build_lease = None
    delegated = False
    try:
        verify_tree_object(config, obj, transport)
        stage = transport.build_stage(obj, adapter, Path(plan["stage"]), mode=mode)
        dataset = ModelScopeDataset(transport, obj.origin, obj.repo_id)
        bound = BoundObject(
            dataset.download_url(obj.revision, obj.object_path),
            obj.object_size,
            obj.revision,
            obj.validator,
            repository=obj.repo_id,
        )
        build_lease = transport.ledger.reserve(
            Reservation(disk=OFFLINE_BUILD_ALLOWANCE, records=records))
        delegated = True
        summary = write_staged_v4(
            transport.ledger,
            [(obj.object_path, bound, stage)],
            Path(output),
            adapter,
            production_transport=transport,
            _build_lease=build_lease,
        )
        transport.release_committed_downloads(Path(output))
    finally:
        if not delegated and build_lease is not None:
            transport.ledger.settle(build_lease)
    return {
        "status": "indexed",
        "mode": mode,
        "summary": summary,
        "durable": str(output),
        "whole_tar_temporary_disk_spool": mode == "download-then-scan",
        "runtime_compile": "separately scheduled",
    }
