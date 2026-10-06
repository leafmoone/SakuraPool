"""Owned explicit workspace identity, layout and persisted resource policy."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass, field, replace
from pathlib import Path

from .capacity import CAPACITY_VERSION, CapacityConfig, ResourcePolicy
from .fs_safety import plain_entry, sync_directory

FORMAT = "sakurapool-workspace-v1"
DOWNLOAD_FORMAT = "sakurapool-workspace-v2"
MANIFEST_BYTES = 16384  # Fixed identity format implementation boundary.


def canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


def _root(path):
    path = Path(path).absolute()
    if ".." in path.parts:
        raise ValueError("noncanonical workspace path")
    return path


def _domain(root):
    info = root.stat()
    return {"path": os.path.normcase(str(root)), "device": info.st_dev, "inode": info.st_ino}


def _no_parent_domain(root):
    for ancestor in root.parents:
        if os.path.lexists(ancestor / "workspace.json"):
            raise ValueError("nested workspace physical domains forbidden")


@dataclass(frozen=True)
class Workspace:
    root: Path
    identity: str
    physical_domain: dict
    capacity: CapacityConfig
    initial_policy: ResourcePolicy
    ledger_lineage: str
    capacity_version: int = CAPACITY_VERSION
    _initializing: bool = field(default=False, compare=False, repr=False)
    lightweight: bool = False

    @property
    def state(self):
        return self.root / "state"

    @property
    def tasks(self):
        return self.root / "tasks"

    @property
    def tmp(self):
        return self.root / "tmp"

    @classmethod
    def init(cls, root, *, capacity=None, policy=None):
        """New workspaces contain identity and technical capacity, not a consumption ledger."""
        if policy is None:
            root = _root(root)
            capacity = CapacityConfig() if capacity is None else capacity
            if not isinstance(capacity, CapacityConfig):
                raise ValueError("typed workspace capacity required")
            plain_entry(root.parent, directory=True)
            _no_parent_domain(root)
            root.mkdir()
            for name in ("state", "tasks", "tmp"):
                (root / name).mkdir()
            data = {
                "format": DOWNLOAD_FORMAT,
                "identity": uuid.uuid4().hex,
                "capacity_version": CAPACITY_VERSION,
                "physical_domain": _domain(root),
                "capacity": capacity.to_dict(),
            }
            with (root / "workspace.json").open("xb") as stream:
                stream.write(canonical(data))
                stream.flush()
                os.fsync(stream.fileno())
            sync_directory(root)
            return cls.open(root)
        # Explicit historical administrative policy creation, not a download mode.
        from .storage.budget import DEFAULT_WORK_ROOT

        root = _root(root)
        capacity = capacity if capacity is not None else CapacityConfig()
        policy = policy if policy is not None else ResourcePolicy()
        if not isinstance(capacity, CapacityConfig) or not isinstance(policy, ResourcePolicy):
            raise ValueError("typed workspace configuration required")
        if capacity.ledger_effective_headroom > policy.inflight:
            raise ValueError("ledger working memory exceeds inflight policy")
        plain_entry(root.parent, directory=True)
        if root == DEFAULT_WORK_ROOT.absolute() or root.is_relative_to(
            DEFAULT_WORK_ROOT.absolute()
        ):
            raise ValueError("new workspace cannot reset a P4 legacy physical domain")
        _no_parent_domain(root)
        root.mkdir()  # Never adopt user data or overwrite any owned/legacy root.
        plain_entry(root, directory=True)
        for name in ("state", "tasks", "tmp"):
            (root / name).mkdir()
        data = {
            "format": FORMAT,
            "identity": uuid.uuid4().hex,
            "ledger_lineage": uuid.uuid4().hex,
            "capacity_version": CAPACITY_VERSION,
            "physical_domain": _domain(root),
            "capacity": capacity.to_dict(),
            "initial_policy": policy.to_dict(),
        }
        encoded = canonical(data)
        if len(encoded) > MANIFEST_BYTES:
            raise ValueError("workspace identity implementation boundary exceeded")
        with (root / "workspace.json").open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        sync_directory(root)
        workspace = cls.open(root)
        # Only this creator may bootstrap. Missing accounting later never resets.
        replace(workspace, _initializing=True).ledger()
        return workspace

    @classmethod
    def open(cls, root):
        root = plain_entry(_root(root), directory=True)
        _no_parent_domain(root)
        for name in ("state", "tasks", "tmp"):
            plain_entry(root / name, directory=True)
        path = plain_entry(root / "workspace.json")
        with path.open("rb") as stream:
            raw = stream.read(MANIFEST_BYTES + 1)
        if len(raw) > MANIFEST_BYTES:
            raise ValueError("workspace identity implementation boundary exceeded")
        data = json.loads(raw)
        if isinstance(data, dict) and data.get("format") == DOWNLOAD_FORMAT:
            if (
                set(data)
                != {"format", "identity", "capacity_version", "physical_domain", "capacity"}
                or data["capacity_version"] != CAPACITY_VERSION
                or type(data["capacity_version"]) is not int
                or not isinstance(data["identity"], str)
                or len(data["identity"]) != 32
                or any(c not in "0123456789abcdef" for c in data["identity"])
                or data["physical_domain"] != _domain(root)
            ):
                raise ValueError("workspace identity/version invalid")
            return cls(
                root,
                data["identity"],
                data["physical_domain"],
                CapacityConfig.from_dict(data["capacity"]),
                None,
                "",
                lightweight=True,
            )
        if (
            not isinstance(data, dict)
            or set(data)
            != {
                "format",
                "identity",
                "physical_domain",
                "capacity",
                "initial_policy",
                "capacity_version",
                "ledger_lineage",
            }
            or data["format"] != FORMAT
            or type(data["capacity_version"]) is not int
            or data["capacity_version"] != CAPACITY_VERSION
        ):
            raise ValueError("workspace identity/version invalid")
        for name in ("identity", "ledger_lineage"):
            if (
                not isinstance(data[name], str)
                or len(data[name]) != 32
                or any(c not in "0123456789abcdef" for c in data[name])
            ):
                raise ValueError("workspace identity/lineage invalid")
        domain = _domain(root)
        if data["physical_domain"] != domain:
            raise ValueError("workspace physical domain changed")
        return cls(
            root,
            data["identity"],
            domain,
            CapacityConfig.from_dict(data["capacity"]),
            ResourcePolicy.from_dict(data["initial_policy"]),
            data["ledger_lineage"],
            data["capacity_version"],
        )

    def check(self):
        from .storage.budget import BudgetCorrupt

        if self != type(self).open(self.root):
            raise ValueError("workspace identity/configuration changed")
        if (
            not self.lightweight
            and not self._initializing
            and not os.path.lexists(self.state / "workspace-budget.lock")
        ):
            raise BudgetCorrupt("workspace accounting lock missing; never reinitialize")
        return self

    def ledger(self):
        if self.lightweight:
            raise ValueError("lightweight workspace has no consumption ledger")
        from .storage.budget import BudgetLedger

        return BudgetLedger(workspace=self)

    @property
    def policy(self):
        return self.ledger().policy

    @property
    def policy_version(self):
        return self.ledger().policy_version

    def update_policy(self, policy, *, expected_version=None):
        return self.ledger().update_policy(policy, expected_version=expected_version)

    def inspect(self):
        if self.lightweight:
            self.check()
            return {
                "format": DOWNLOAD_FORMAT,
                "identity": self.identity,
                "root": str(self.root),
                "physical_domain": self.physical_domain,
                "capacity": self.capacity.to_dict(),
                "paths": {name: str(getattr(self, name)) for name in ("state", "tasks", "tmp")},
            }
        # Historical manifests are archives. Inspection must not initialize,
        # recover or mutate their ledger/policy, even when a lock is missing.
        return {
            "format": FORMAT,
            "identity": self.identity,
            "root": str(self.root),
            "ledger_lineage": self.ledger_lineage,
            "capacity_version": self.capacity_version,
            "paths": {name: str(getattr(self, name)) for name in ("state", "tasks", "tmp")},
            "physical_domain": self.physical_domain,
            "capacity": self.capacity.to_dict(),
            "read_only_legacy": True,
            "download_consumption": "NOT_USED",
        }

    def task_path(self, directory, *, must_exist=False, readonly=False):
        if readonly:
            if self != type(self).open(self.root):
                raise ValueError("workspace identity/configuration changed")
        else:
            self.check()
        directory = _root(directory)
        if not directory.is_relative_to(self.tasks) or directory == self.tasks:
            raise ValueError("task must be contained in workspace tasks")
        for path in (directory, *directory.parents):
            if os.path.lexists(path):
                plain_entry(path, directory=True)
            if path == self.root:
                break
        if must_exist:
            plain_entry(directory, directory=True)
        return directory

    def download_binding(self, directory):
        if not self.lightweight:
            raise ValueError("legacy workspace migration required")
        directory = self.task_path(directory)
        return {
            "format": DOWNLOAD_FORMAT,
            "workspace_id": self.identity,
            "physical_domain": self.physical_domain,
            "task_relative": directory.relative_to(self.root).as_posix(),
        }

    def task_binding(self, directory):
        directory = self.task_path(directory, readonly=True)
        return {
            "format": FORMAT,
            "workspace_id": self.identity,
            "ledger_lineage": self.ledger_lineage,
            "capacity_version": self.capacity_version,
            "physical_domain": self.physical_domain,
            "task_relative": directory.relative_to(self.root).as_posix(),
            "capacity": self.capacity.to_dict(),
        }

    def validate_task_binding(self, binding, directory):
        if binding != self.task_binding(directory):
            raise ValueError("task workspace binding conflict")
        return self

    @property
    def binding_hash(self):
        return hashlib.sha256(
            canonical(
                {
                    "identity": self.identity,
                    "ledger_lineage": self.ledger_lineage,
                    "capacity_version": self.capacity_version,
                    "physical_domain": self.physical_domain,
                    "capacity": self.capacity.to_dict(),
                }
            )
        ).digest()
