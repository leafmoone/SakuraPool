"""Frozen task capacity and authoritative workspace ownership."""

import os
from pathlib import Path

from ..capacity import CapacityConfig
from ..workspace import Workspace

LEGACY_CAPACITY = CapacityConfig()
WORKSPACE_FORMAT = "sakurapool-task-v2"


def effective_capacity(transport, fallback=LEGACY_CAPACITY):
    """Accept the storage capacity API without mutating a transport instance."""
    from .store import TaskError

    capacity = getattr(transport, "effective_capacity", None)
    if capacity is None:
        capacity = getattr(transport, "capacity", fallback)
    if not isinstance(capacity, CapacityConfig):
        raise TaskError("TASK_CAPACITY_CONFLICT", "preflight")
    return capacity


def bootstrap_workspace(directory, workspace=None):
    """Discover capacity from an owned ancestor, never from task JSON."""
    from .store import TaskError

    try:
        selected = None
        for ancestor in Path(directory).parents:
            if os.path.lexists(ancestor / "workspace.json"):
                selected = Workspace.open(ancestor)
                selected.task_path(directory, must_exist=True)
                break
        if workspace is not None:
            supplied = (
                Workspace.open(workspace)
                if isinstance(workspace, (str, Path))
                else Workspace.open(workspace.root)
            )
            if selected is None or supplied != selected:
                raise ValueError("workspace conflict")
        return selected
    except (ValueError, OSError, KeyError, TypeError, AttributeError):
        raise TaskError("TASK_WORKSPACE_CONFLICT", "plan") from None


def resolve_workspace(binding, directory, workspace=None):
    from .store import TaskError

    if binding is None:
        if workspace is not None:
            raise TaskError("TASK_WORKSPACE_CONFLICT", "plan")
        return None
    try:
        if not isinstance(binding, dict):
            raise ValueError("invalid binding")
        root = binding["physical_domain"]["path"]
        selected = Workspace.open(workspace) if isinstance(workspace, (str, Path)) else workspace
        selected = Workspace.open(root) if selected is None else selected
        selected.validate_task_binding(binding, directory)
        return selected
    except (ValueError, OSError, KeyError, TypeError):
        raise TaskError("TASK_WORKSPACE_CONFLICT", "plan") from None


def validate_ledger(task, ledger):
    from .store import TaskError

    workspace = getattr(ledger, "workspace", None)
    if task.workspace is None:
        if workspace is not None:
            raise TaskError("TASK_WORKSPACE_CONFLICT", "plan")
    elif workspace is None or workspace != task.workspace:
        raise TaskError("TASK_WORKSPACE_CONFLICT", "plan")


def remaining_limits(ledger, required):
    from ..capacity import UINT64_MAX

    status = ledger.status()  # Refreshes durable policy before reading limits.
    remaining = {
        key: max(
            0, (UINT64_MAX if ledger.limits[key] is None else ledger.limits[key]) - status[key]
        )
        for key in required
    }
    if "inflight" in remaining:
        remaining["inflight"] = max(
            0, remaining["inflight"] - getattr(ledger, "effective_headroom", 0)
        )
    return remaining


def chunk_plan(image_bytes, metadata_bytes, capacity):
    """Arithmetic only; streaming storage owns range issuance and assembly."""
    chunk = capacity.range_chunk_bytes
    counts = tuple((length + chunk - 1) // chunk for length in (image_bytes, metadata_bytes))
    return sum(counts), min(chunk, max(image_bytes, metadata_bytes))
