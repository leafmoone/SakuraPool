"""At-most-two-file no-replace publication with typed, identity-bound intent."""

import hashlib
import os
import re
from dataclasses import dataclass
from itertools import islice

from ..download_naming import FLAT_POLICY, safe_basename, safe_output_path
from ..fs_durability import publish_noreplace as _publish_directory
from ..fs_durability import sync_directory
from ..fs_safety import plain_entry
from ..image_formats import SUPPORTED_IMAGE_EXTENSIONS


@dataclass(frozen=True)
class DeliveryMapping:
    stem: str
    image_suffix: str
    metadata: bool = False

    def __post_init__(self):
        safe_basename(self.stem)
        if self.image_suffix not in SUPPORTED_IMAGE_EXTENSIONS or type(self.metadata) is not bool:
            raise ValueError("OUTPUT_MAPPING_INVALID")
        for name in self.names:
            safe_basename(name)

    @property
    def names(self):
        return (self.stem + self.image_suffix,) + ((self.stem + ".json",) if self.metadata else ())

    @property
    def staged_names(self):
        return ("image" + self.image_suffix,) + (("metadata.json",) if self.metadata else ())

    def check_paths(self, output):
        for name in self.names:
            safe_output_path(output, name)


def identity(value):
    return type(value) is list and len(value) == 2 and all(type(v) is int and v >= 0 for v in value)


def file_proof(path, proof):
    path = plain_entry(path)
    info = path.stat()
    if info.st_nlink != 1 or [info.st_dev, info.st_ino] != proof["identity"]:
        raise ValueError("OUTPUT_REPLACED")
    if info.st_size != proof["bytes"]:
        raise ValueError("OUTPUT_CORRUPT")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        opened = os.fstat(stream.fileno())
        if opened.st_nlink != 1 or [opened.st_dev, opened.st_ino] != proof["identity"]:
            raise ValueError("OUTPUT_REPLACED")
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    after = plain_entry(path).stat()
    if (after.st_nlink != 1 or [after.st_dev, after.st_ino] != proof["identity"]
            or after.st_size != info.st_size or after.st_mtime_ns != info.st_mtime_ns):
        raise ValueError("OUTPUT_REPLACED")
    if digest.hexdigest() != proof["sha256"]:
        raise ValueError("OUTPUT_CORRUPT")


def validate_receipt(receipt, mapping):
    """Types and exact names are checked before accessing any filesystem path."""
    if (
        type(receipt) is not dict
        or receipt.get("layout") != FLAT_POLICY
        or receipt.get("stem") != mapping.stem
        or not identity(receipt.get("output_identity"))
        or not identity(receipt.get("stage_identity"))
        or type(receipt.get("stage_name")) is not str
        or not re.fullmatch(r"\.publication-fetch-[0-9a-f]{32}", receipt["stage_name"])
        or type(receipt.get("receipt")) is not dict
        or set(receipt["receipt"]) != set(mapping.names)
    ):
        raise ValueError("OUTPUT_CONFLICT")
    for name, staged in zip(mapping.names, mapping.staged_names):
        proof = receipt["receipt"][name]
        if (
            type(proof) is not dict
            or proof.get("staged_name") != staged
            or not identity(proof.get("identity"))
            or type(proof.get("bytes")) is not int
            or not 0 <= proof["bytes"] < (1 << 64)
            or type(proof.get("sha256")) is not str
            or not re.fullmatch("[0-9a-f]{64}", proof["sha256"])
        ):
            raise ValueError("OUTPUT_CONFLICT")
    return receipt


def check_root(output, receipt):
    info = plain_entry(output, directory=True).stat()
    if [info.st_dev, info.st_ino] != receipt["output_identity"]:
        raise ValueError("OUTPUT_REPLACED")


def check_stage(output, receipt, expected_names):
    stage = plain_entry(output / receipt["stage_name"], directory=True)
    info = stage.stat()
    if [info.st_dev, info.st_ino] != receipt["stage_identity"]:
        raise ValueError("OUTPUT_REPLACED")
    if expected_names is not None and {
        p.name for p in islice(stage.iterdir(), 3)
    } != set(expected_names):
        raise ValueError("OUTPUT_CONFLICT")
    return stage


def verify_final(output, receipt, mapping):
    validate_receipt(receipt, mapping)
    mapping.check_paths(output)
    check_root(output, receipt)
    for name in mapping.names:
        file_proof(output / name, receipt["receipt"][name])
    if os.path.lexists(output / receipt["stage_name"]):
        check_stage(output, receipt, [])
    check_root(output, receipt)
    return receipt


def publish_flat(output, receipt, mapping, *, hook=None, intent=False, published=()):
    """Resume exact intent only; never delete/replace existing final members."""
    validate_receipt(receipt, mapping)
    mapping.check_paths(output)
    if type(published) not in (tuple, list) or any(
        type(name) is not str or name not in mapping.names for name in published
    ) or len(set(published)) != len(published):
        raise ValueError("OUTPUT_CONFLICT")
    check_root(output, receipt)
    stage = output / receipt["stage_name"]
    if os.path.lexists(stage):
        check_stage(output, receipt, None)
    remaining, finals = [], []
    for name in mapping.names:
        proof = receipt["receipt"][name]
        in_stage = os.path.lexists(stage / proof["staged_name"])
        in_final = os.path.lexists(output / name)
        if in_stage == in_final or name in published and not in_final or not intent and in_final:
            raise ValueError("OUTPUT_UNCERTAIN")
        if in_stage:
            file_proof(stage / proof["staged_name"], proof)
            remaining.append(name)
        else:
            file_proof(output / name, proof)
            finals.append(name)
    # A completed operation may already have removed its empty private stage.
    if remaining or os.path.lexists(stage):
        check_stage(output, receipt,
                    [receipt["receipt"][name]["staged_name"] for name in remaining])
    if os.path.lexists(stage):
        sync_directory(stage)
    sync_directory(output)
    if not intent and hook:
        hook("PUBLISH_INTENT", {})
    # A crash can leave an unacknowledged, already-moved member. Ack its exact identity.
    for name in finals:
        if name not in published and hook:
            hook("MEMBER_PUBLISHED", {"name": name})
    for name in remaining:
        check_root(output, receipt)
        staged_names = [receipt["receipt"][n]["staged_name"] for n in remaining]
        check_stage(output, receipt, staged_names)
        proof = receipt["receipt"][name]
        file_proof(stage / proof["staged_name"], proof)
        # This primitive is equally no-replace for regular files and directories.
        _publish_directory(stage / proof["staged_name"], output / name)
        if hook:
            hook("MEMBER_PUBLISHED", {"name": name})
        remaining = [n for n in remaining if n != name]
    verify_final(output, receipt, mapping)
    sync_directory(output)
    if os.path.lexists(stage):
        sync_directory(stage)
    if hook:
        hook("PUBLISHED", {})
    if os.path.lexists(stage):
        check_stage(output, receipt, []).rmdir()
        sync_directory(output)
    return output / mapping.names[0]
