"""Strict reader for the committed P2 v4 fragment contract."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn

import pyarrow as pa
import pyarrow.parquet as pq

from ..indexer import (
    ANNOTATIONS_SCHEMA,
    BUILDER,
    ERRORS_SCHEMA,
    FORMAT_VERSION,
    OBJECTS_SCHEMA,
    SAMPLES_SCHEMA,
    _json,
    _sha,
)
from .errors import CorruptInputError

_FRAGMENT_SCHEMAS = {
    "objects": OBJECTS_SCHEMA,
    "samples": SAMPLES_SCHEMA,
    "annotations": ANNOTATIONS_SCHEMA,
    "errors": ERRORS_SCHEMA,
}


@dataclass(frozen=True)
class Fragment:
    dataset_id: str
    object_id: str
    name: str
    path: Path
    rows: int
    bytes: int
    sha256: str


@dataclass(frozen=True)
class P2Object:
    dataset_id: str
    object_id: str
    input: dict[str, Any]
    fragments: tuple[Fragment, ...]


@dataclass(frozen=True)
class P2Inventory:
    root: Path
    contract: dict[str, Any]
    objects: tuple[P2Object, ...]
    source_fingerprint: str

    @property
    def fragments(self) -> tuple[Fragment, ...]:
        return tuple(fragment for obj in self.objects for fragment in obj.fragments)


def combine_inventories(inventories: list[P2Inventory]) -> P2Inventory:
    """Merge multiple committed P2 indexes into one P3 compile input.

    Same-dataset partitions must have equal adapter/hash contracts and
    disjoint physical objects. Duplicate post ids are intentionally retained.
    """
    if not inventories:
        _fail("no P2 inputs provided")
    if len(inventories) == 1:
        return inventories[0]
    verified_inventories = []
    for inventory in inventories:
        # Public callers may construct dataclasses or mutate the input on disk.
        verified = load_p2_inventory(inventory.root)
        if verified.source_fingerprint != inventory.source_fingerprint:
            _fail("inventory changed before combine")
        verified_inventories.append(verified)
    return _combine_verified_inventories(verified_inventories)


def _combine_verified_inventories(inventories: list[P2Inventory]) -> P2Inventory:
    """Combine inputs validated in this operation, without a second disk scan.

    Only the loader and the revalidating public combine entry point use this
    helper; no caller-provided inventory is trusted without loading it first.
    """
    objects = []
    seen = set()
    paths = set()
    datasets = {}
    for verified in inventories:
        for dataset in {obj.dataset_id for obj in verified.objects}:
            contract = _json([verified.contract["adapter"], verified.contract["hash_images"]])
            if dataset in datasets and datasets[dataset] != contract:
                _fail(f"adapter/hash contract mismatch across inputs: {dataset}")
            datasets[dataset] = contract
        for obj in verified.objects:
            object_path = obj.object_id.rsplit("@sha256-", 1)[0]
            path_key = (obj.dataset_id, object_path)
            if path_key in paths:
                _fail(f"duplicate object_path across inputs: {object_path}")
            paths.add(path_key)
            key = (obj.dataset_id, obj.object_id)
            if key in seen:
                _fail(f"duplicate object across inputs: {obj.object_id}")
            seen.add(key)
            objects.append(obj)
    fingerprint = hashlib.sha256(
        _json(sorted(inventory.source_fingerprint for inventory in inventories))
    ).hexdigest()
    return P2Inventory(
        root=inventories[0].root,
        contract=dict(inventories[0].contract),
        objects=tuple(objects),
        source_fingerprint=fingerprint,
    )


def _fail(message: str) -> NoReturn:
    raise CorruptInputError(message)


def _shard_id(dataset: str, rel: str) -> str:
    return hashlib.sha256(_json([dataset, rel])).hexdigest()


def _regular_child(root: Path, relative: str) -> Path:
    if (
        not isinstance(relative, str)
        or not relative
        or "/" in relative
        or Path(relative).name != relative
    ):
        _fail(f"fragment path must be a direct child: {relative!r}")
    path = root / relative
    if path.is_symlink() or not path.is_file():
        _fail(f"fragment is not a regular file: {relative}")
    return path


def _check_row_ownership(
    path: Path, dataset: str, object_id: str, batch_size: int = 10_000
) -> None:
    """Every fragment row must belong to this commit's (dataset, object).

    Column-projected streaming so the check never loads whole fragments.
    """
    with pq.ParquetFile(path) as handle:
        # PyArrow 18 iter_batches requests row_group(-1) for footer-only files.
        if handle.num_row_groups == 0:
            return
        for batch in handle.iter_batches(
            batch_size=batch_size, columns=["dataset_id", "object_id"]
        ):
            rows = batch.to_pylist()
            for row in rows:
                if row["dataset_id"] != dataset:
                    _fail(
                        f"row dataset_id {row['dataset_id']!r} != commit {dataset!r} in {path.name}"
                    )
                if row["object_id"] != object_id:
                    _fail(
                        f"row object_id {row['object_id']!r} != commit {object_id!r} in {path.name}"
                    )


def _validate_fragment(path: Path, name: str, info: dict[str, Any]) -> int:
    if set(info) != {"path", "sha256", "bytes", "rows"}:
        _fail(f"invalid fragment inventory keys: {name}")
    if (
        info["path"] != path.name
        or type(info["bytes"]) is not int
        or type(info["rows"]) is not int
        or info["bytes"] < 0
        or info["rows"] < 0
    ):
        _fail(f"invalid fragment metadata: {name}")
    if info["bytes"] != path.stat().st_size or info["sha256"] != _sha(path):
        _fail(f"fragment hash/size mismatch: {name}")
    try:
        actual_rows = pq.ParquetFile(path).metadata.num_rows
        actual_schema = pq.read_schema(path)
    except (OSError, pa.ArrowException) as exc:
        _fail(f"invalid parquet fragment {name}: {exc}")
    if actual_schema != _FRAGMENT_SCHEMAS[name] or actual_rows != info["rows"]:
        _fail(f"fragment schema/rows mismatch: {name}")
    return actual_rows


def _load_directory(
    root: Path, dataset: str, row_batch_size: int = 10_000
) -> tuple[dict[str, Any], list[P2Object]]:
    if not root.is_dir():
        _fail(f"missing P2 index directory: {root}")
    input_path = root / "INPUT.json"
    if input_path.is_symlink() or not input_path.is_file():
        _fail(f"missing INPUT.json: {root}")
    try:
        contract = json.loads(input_path.read_bytes())
    except (OSError, json.JSONDecodeError) as exc:
        _fail(f"invalid INPUT.json: {exc}")
    if contract.get("format_version") != FORMAT_VERSION or contract.get("builder") != BUILDER:
        _fail(f"unsupported P2 contract: {root}")
    base_keys = {"format_version", "builder", "adapter", "hash_images", "inputs"}
    if set(contract) not in (base_keys, base_keys | {"build_metadata", "partition_manifest"}):
        _fail("unexpected INPUT.json keys")
    if "build_metadata" in contract:
        from ..local_builder import validate_build_metadata
        from ..partition import PartitionManifest

        try:
            validate_build_metadata(contract["build_metadata"])
            plan = contract["partition_manifest"]
            if any("local_path" in obj for obj in plan["objects"]):
                _fail("partition provenance must not persist local_path")
            restored = dict(
                plan,
                objects=[dict(obj, local_path=str(root / "unused.tar")) for obj in plan["objects"]],
            )
            parsed_plan = PartitionManifest.from_dict(restored)
            if (
                parsed_plan.dataset != dataset
                or parsed_plan.source != contract["adapter"]["source"]
            ):
                _fail("partition plan adapter identity mismatch")
            if {obj.repository_path for obj in parsed_plan.objects} != set(contract["inputs"]):
                _fail("partition plan input set mismatch")
            for obj in parsed_plan.objects:
                validator = contract["inputs"][obj.repository_path]
                if obj.declared_size != validator.get(
                    "size"
                ) or obj.provider_sha256 != validator.get("sha256"):
                    _fail("partition plan validator mismatch")
        except (KeyError, TypeError, ValueError) as exc:
            _fail(f"invalid partition build provenance: {exc}")
    if contract["adapter"].get("dataset") != dataset:
        _fail("P2 adapter.dataset does not match this input directory")
    if not isinstance(contract["inputs"], dict) or not contract["inputs"]:
        _fail("P2 INPUT.inputs must be a nonempty object")
    contract_hash = hashlib.sha256(_json(contract)).hexdigest()
    expected_markers = {f"{_shard_id(dataset, rel)}.COMMIT" for rel in contract["inputs"]}
    actual_markers = {
        p.name for p in root.iterdir() if p.name.endswith(".COMMIT") and not p.is_symlink()
    }
    if actual_markers != expected_markers:
        _fail("COMMIT marker set does not match derived shard ids")
    # Fail closed on anything not declared: stray partial/fragment/temp files
    # next to INPUT.json must never be accepted silently.
    allowed = {"INPUT.json"} | expected_markers
    allowed.update(
        f"{_shard_id(dataset, rel)}.{name}.parquet"
        for rel in contract["inputs"]
        for name in _FRAGMENT_SCHEMAS
    )
    for entry in root.iterdir():
        if entry.name not in allowed:
            _fail(f"unexpected file in P2 root (fail closed): {entry.name}")
    objects: list[P2Object] = []
    seen: set[tuple[str, str]] = set()
    for rel in sorted(contract["inputs"]):
        marker_path = root / f"{_shard_id(dataset, rel)}.COMMIT"
        try:
            commit = json.loads(marker_path.read_bytes())
        except (OSError, json.JSONDecodeError) as exc:
            _fail(f"invalid COMMIT {marker_path.name}: {exc}")
        if set(commit) != {
            "schema",
            "builder",
            "dataset_id",
            "object_id",
            "input",
            "files",
            "contract_sha256",
            "created_at",
        }:
            _fail(f"unexpected COMMIT keys: {marker_path.name}")
        planned = contract["inputs"][rel]
        actual = commit["input"]
        input_matches = actual == planned
        if (
            "build_metadata" in contract
            and planned.get("sha256") is None
            and isinstance(actual, dict)
        ):
            import re

            input_matches = (
                dict(actual, sha256=None) == planned
                and isinstance(actual.get("sha256"), str)
                and re.fullmatch(r"[0-9a-f]{64}", actual["sha256"]) is not None
            )
        if (
            commit["schema"] != FORMAT_VERSION
            or commit["builder"] != BUILDER
            or commit["contract_sha256"] != contract_hash
            or commit["dataset_id"] != dataset
            or not input_matches
        ):
            _fail(f"COMMIT identity/validator mismatch: {marker_path.name}")
        object_id = commit["object_id"]
        expected_object_id = f"{rel}@sha256-{commit['input']['sha256']}"
        if object_id != expected_object_id:
            _fail(
                f"commit object_id {object_id!r} is not rel@sha256(input) "
                f"({expected_object_id!r}): {marker_path.name}"
            )
        if (dataset, object_id) in seen:
            _fail(f"duplicate committed object: {dataset}/{object_id}")
        seen.add((dataset, object_id))
        if set(commit["files"]) != set(_FRAGMENT_SCHEMAS):
            _fail(f"fragment set mismatch: {marker_path.name}")
        fragments: list[Fragment] = []
        for name in _FRAGMENT_SCHEMAS:
            expected_fragment = f"{marker_path.stem}.{name}.parquet"
            info = commit["files"][name]
            path = _regular_child(root, info.get("path"))
            if path.name != expected_fragment:
                _fail(f"fragment name mismatch: {path.name}")
            _validate_fragment(path, name, info)
            if name in ("objects", "samples", "annotations"):
                _check_row_ownership(path, dataset, object_id, row_batch_size)
            fragments.append(
                Fragment(
                    dataset, object_id, name, path, info["rows"], info["bytes"], info["sha256"]
                )
            )
        objects.append(P2Object(dataset, object_id, commit["input"], tuple(fragments)))
    return contract, objects


def load_p2_inventory(
    roots: Path | str | list[Path | str] | tuple[Path | str, ...], *, _row_batch_size: int = 10_000
) -> P2Inventory:
    """Validate one or more P2 directories without glob-trusting Parquet files."""
    # str and Path are both accepted; a bare str must NOT be iterated
    # character by character (the old list(roots) did exactly that)
    if isinstance(roots, (str, Path)):
        roots = [roots]
    roots = [Path(r) for r in roots]
    if not roots:
        _fail("at least one P2 index directory is required")
    if len(roots) > 1:
        return _combine_verified_inventories([
            load_p2_inventory(root, _row_batch_size=_row_batch_size) for root in roots
        ])
    all_objects: list[P2Object] = []
    contracts: list[dict[str, Any]] = []
    for root_value in roots:
        root = Path(root_value).resolve()
        input_path = root / "INPUT.json"
        if input_path.is_symlink() or not input_path.is_file():
            _fail(f"missing INPUT.json: {root}")
        dataset = json.loads(input_path.read_bytes())["adapter"]["dataset"]
        contract, objects = _load_directory(root, dataset, _row_batch_size)
        contracts.append(contract)
        all_objects.extend(objects)
    ordered = sorted(
        all_objects, key=lambda v: (v.dataset_id, v.object_id, v.input.get("path", ""))
    )
    payload = [
        [
            o.dataset_id,
            o.object_id,
            o.input,
            [[f.name, f.sha256, f.bytes, f.rows] for f in o.fragments],
        ]
        for o in ordered
    ]
    fingerprint = hashlib.sha256(_json(["sakurapool-p3-source-v1", payload])).hexdigest()
    return P2Inventory(Path(roots[0]).resolve(), contracts[0], tuple(ordered), fingerprint)
