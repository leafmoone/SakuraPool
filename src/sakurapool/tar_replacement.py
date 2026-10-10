"""Offline, same-path TAR replacement with immutable old P2 inputs.

Only explicitly selected local TARs are scanned. P2 fragments are validated and
copied, never edited in place; remote provider identity is never inferred from
the local content hash. A fresh input set becomes visible in one no-replace move.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import tempfile
from pathlib import Path

from .fs_durability import publish_noreplace
from .fs_safety import OwnedStage, plain_entry, sync_directory
from .indexer import _json
from .local_builder import build_partition
from .partition import PartitionManifest, PartitionObject
from .records import canonical_object_id
from .registry import AdapterRegistry
from .runtime.inventory import _combine_verified_inventories, load_p2_inventory
from .storage.publication import bounded, map_rows, p2_roots

MAX_TARGETS = 65_536


def _key(obj):
    return obj.dataset_id, obj.object_id.rsplit("@sha256-", 1)[0]


def _combine(inventories):
    return (inventories[0] if len(inventories) == 1
            else _combine_verified_inventories(inventories))


def _targets(values):
    if not isinstance(values, (list, tuple)) or not 1 <= len(values) <= MAX_TARGETS:
        raise ValueError("replace-tars requires 1..65536 explicit TAR paths")
    result = []
    for value in values:
        canonical_object_id(value)
        if not value.endswith(".tar"):
            raise ValueError("replacement path must end in .tar")
        result.append(value)
    if len(set(result)) != len(result):
        raise ValueError("duplicate replacement TAR path")
    return sorted(result)


def _map(path, objects):
    """Check exact object coverage, size and any explicitly supplied digest."""
    result = {}
    for row in map_rows(path):
        key = row["dataset_id"], row["object_path"]
        obj = objects.get(key)
        if key in result or obj is None or row["object_size"] != obj.input["size"]:
            raise ValueError("remote-map duplicate, target or size mismatch")
        if row["provider_sha256"] not in (None, obj.input["sha256"]):
            raise ValueError("remote-map provider digest differs from indexed content")
        result[key] = row
    if set(result) != set(objects):
        raise ValueError("remote-map must cover exactly the specified objects")
    return result


def _write(owned, relative, data):
    path = owned.create(relative)
    with path.open("wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    return path


def _copy_subset(owned, relative, inventory, objects):
    """Retain Parquet bytes and record IDs, changing only the enclosing contract."""
    destination = owned.create(relative, directory=True)
    contract = dict(inventory.contract)
    paths = {_key(obj)[1] for obj in objects}
    contract["inputs"] = {p: value for p, value in contract["inputs"].items() if p in paths}
    if "partition_manifest" in contract:
        plan = dict(contract["partition_manifest"])
        plan["objects"] = [obj for obj in plan["objects"] if obj["repository_path"] in paths]
        contract["partition_manifest"] = plan
    contract_hash = hashlib.sha256(_json(contract)).hexdigest()
    _write(owned, relative / "INPUT.json", _json(contract))
    for obj in objects:
        shard = hashlib.sha256(_json(list(_key(obj)))).hexdigest()
        marker = shard + ".COMMIT"
        commit = bounded(inventory.root / marker)
        commit["contract_sha256"] = contract_hash
        for fragment in obj.fragments:
            target = owned.create(relative / fragment.path.name)
            with plain_entry(fragment.path).open("rb") as source, target.open("wb") as stream:
                shutil.copyfileobj(source, stream, length=1 << 20)
                stream.flush()
                os.fsync(stream.fileno())
        _write(owned, relative / marker, _json(commit))
    sync_directory(destination)
    return load_p2_inventory(destination)


def replace_tars(p2_list, dataset, tar_paths, local_root, worker, work_dir, output, *,
                 code_sha, remote_map=None, replacement_map=None, checkpoint=None):
    """Scan listed changed TARs and publish a new P2 input set, never old inputs.

    `checkpoint` is an in-process fault hook, not part of the CLI contract.
    Inputs must remain immutable during the operation and while referenced by
    the emitted root list. Failed private work/stages are retained for inspection.
    """
    checkpoint = checkpoint or (lambda _: None)
    canonical_object_id(dataset)
    selected = _targets(tar_paths)
    if not isinstance(code_sha, str) or re.fullmatch(r"[0-9a-f]{40}", code_sha) is None:
        raise ValueError("a full installed code SHA is required")
    local_root = plain_entry(local_root, directory=True)
    worker = plain_entry(worker)
    roots = p2_roots(p2_list)
    inventories = [load_p2_inventory(root) for root in roots]
    combined = _combine(inventories)
    objects = {_key(obj): obj for obj in combined.objects}
    selected_keys = {(dataset, path) for path in selected}
    if not selected_keys <= objects.keys():
        raise ValueError("replacement target is absent from the selected dataset/P2 inputs")
    output, work_dir = Path(output).absolute(), Path(work_dir).absolute()
    for path in (output, work_dir):
        plain_entry(path.parent, directory=True)
        if os.path.lexists(path):
            raise ValueError("fresh output and work directories required")
        for source in [*roots, local_root]:
            if path == source or path.is_relative_to(source) or source.is_relative_to(path):
                raise ValueError("replacement output/work must be disjoint from inputs")
    if output.is_relative_to(work_dir) or work_dir.is_relative_to(output):
        raise ValueError("replacement work and output must be disjoint")
    old_map = _map(remote_map, objects) if remote_map is not None else None
    if replacement_map is not None and old_map is None:
        raise ValueError("replacement-map requires the old full remote-map")
    bindings = {}
    if replacement_map is not None:
        for row in map_rows(replacement_map):
            key = row["dataset_id"], row["object_path"]
            if key not in selected_keys or key in bindings:
                raise ValueError("replacement-map has a wrong or duplicate target")
            old = old_map[key]
            if any(row[name] != old[name] for name in ("endpoint", "repo_id", "repo_type")):
                raise ValueError("same-path replacement cannot change repository identity")
            if row["revision_candidate"] == old["revision_candidate"]:
                raise ValueError("changed TAR requires a new immutable remote revision")
            bindings[key] = row
        if set(bindings) != selected_keys:
            raise ValueError("replacement-map must cover exactly the selected TARs")
    builds = []
    for inventory in inventories:
        changed = [obj for obj in inventory.objects if _key(obj) in selected_keys]
        if not changed:
            continue
        contract = inventory.contract
        if "partition_manifest" not in contract:
            raise ValueError("selected TAR requires build-partition provenance")
        adapter_data = dict(contract["adapter"])
        adapter_data.pop("dataset")
        adapter = AdapterRegistry.from_dict({"datasets": {dataset: adapter_data}}).get(dataset)
        if adapter.metadata_mode != "nested_json_v1" or contract["hash_images"] is not True:
            raise ValueError("replacement requires hashed nested_json_v1 inputs")
        plan = contract["partition_manifest"]
        partition_objects = []
        for obj in sorted(changed, key=_key):
            key = _key(obj)
            local = plain_entry(local_root / key[1])
            size = local.stat().st_size
            if size <= 0:
                raise ValueError("changed local TAR must be nonempty")
            binding = bindings.get(key)
            if binding is not None and (binding["object_size"] != size or
                                         binding["repo_id"] != plan["repository"]):
                raise ValueError("replacement remote binding size/repository mismatch")
            partition_objects.append(PartitionObject(key[1], local, size,
                None if binding is None else binding["provider_sha256"]))
        manifest = PartitionManifest.from_dict(dict(plan,
            objects=[obj.to_dict() for obj in partition_objects]))
        builds.append((manifest, adapter))
    checkpoint("PREFLIGHT")
    work_dir.mkdir()
    generated = []
    for number, (manifest, adapter) in enumerate(builds):
        durable = work_dir / f"changed-{number:06d}"
        result = build_partition(manifest, adapter, worker,
            work_dir / f"scan-{number:06d}", durable, code_sha=code_sha)
        if result["counts"]["errors"]:
            raise ValueError("changed TAR contains index errors; replacement not published")
        generated.append(load_p2_inventory(durable))
    new_objects = {_key(obj): obj for inv in generated for obj in inv.objects}
    if set(new_objects) != selected_keys:
        raise ValueError("built replacement coverage mismatch")
    if bindings and _map(replacement_map, new_objects) != bindings:
        raise ValueError("replacement-map changed during replacement")
    checkpoint("SCANNED")
    stage = Path(tempfile.mkdtemp(prefix=".tar-replacement-", dir=output.parent))
    owned = OwnedStage(stage)
    result_inventories, final_roots = [], []
    for number, inventory in enumerate(inventories):
        survivors = [obj for obj in inventory.objects if _key(obj) not in selected_keys]
        if len(survivors) == len(inventory.objects):
            result_inventories.append(inventory)
            final_roots.append(str(inventory.root))
        elif survivors:
            relative = Path(f"retained-{number:06d}")
            result_inventories.append(_copy_subset(owned, relative, inventory, survivors))
            final_roots.append(str(output / relative))
    for number, inventory in enumerate(generated):
        relative = Path(f"replacement-{number:06d}")
        result_inventories.append(_copy_subset(owned, relative, inventory, inventory.objects))
        final_roots.append(str(output / relative))
    composed = _combine(result_inventories)
    expected = dict(objects)
    expected.update(new_objects)
    if {_key(obj): obj.object_id for obj in composed.objects} != {
            key: obj.object_id for key, obj in expected.items()}:
        raise ValueError("composed inventory identity mismatch")
    checkpoint("COMPOSED")
    # Re-read old contracts/fragments: no concurrent mutation can be silently
    # accepted as the basis of the new input set during this operation.
    for inventory in inventories:
        again = load_p2_inventory(inventory.root)
        if (again.source_fingerprint != inventory.source_fingerprint or
                _json(again.contract) != _json(inventory.contract)):
            raise ValueError("old P2 input changed during replacement")
    full_map = None
    if old_map is not None and bindings:
        full_map = dict(old_map)
        full_map.update(bindings)
        _write(owned, Path("remote-map.jsonl"),
               b"".join(_json(full_map[key]) for key in sorted(full_map)))
        _map(stage / "remote-map.jsonl", expected)
    binding_status = "SUPPLIED" if full_map is not None else "PENDING"
    if full_map is None:
        _write(owned, Path("remote-bindings.pending.json"), _json({
            "format": "sakurapool-pending-remote-bindings-v1",
            "remote_binding_status": "UNRESOLVED",
            "reason": "explicit full remote bindings required before publication",
            "changed_objects": [{"dataset_id": key[0], "object_path": key[1],
                "old_object_id": objects[key].object_id,
                "new_object_id": new_objects[key].object_id,
                "object_size": new_objects[key].input["size"],
                "local_content_sha256": new_objects[key].input["sha256"]}
                for key in sorted(new_objects)]}))
    _write(owned, Path("p2-roots.json"), _json({
        "format": "sakurapool-p2-root-list-v1", "roots": final_roots}))
    receipt = {"format": "sakurapool-tar-replacement-v1", "dataset_id": dataset,
        "source_fingerprint": combined.source_fingerprint,
        "result_fingerprint": composed.source_fingerprint,
        "remote_binding_status": binding_status,
        "objects": [{"object_path": key[1], "old_object_id": objects[key].object_id,
                     "new_object_id": new_objects[key].object_id}
                    for key in sorted(new_objects)],
        "replaced_objects": len(new_objects), "retained_objects": len(objects) - len(new_objects)}
    raw = _json(receipt)
    _write(owned, Path("REPLACEMENT.json"), raw)
    _write(owned, Path("READY"), hashlib.sha256(raw).hexdigest().encode("ascii"))
    owned.complete()
    checkpoint("READY")
    owned.complete()
    sync_directory(stage)
    publish_noreplace(stage, output)
    return receipt
