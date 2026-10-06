"""Workspace lightweight ownership, control and technical gates."""

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_lightweight_tasks import lightweight as lightweight_fixture

from sakurapool.capacity import CapacityConfig
from sakurapool.cli import main
from sakurapool.storage.retrieval import _real_output_root
from sakurapool.tasks.context import effective_capacity
from sakurapool.tasks.plan import SelectedRecord
from sakurapool.tasks.runner import create_task, run_task
from sakurapool.tasks.store import TaskDB
from sakurapool.workspace import Workspace

lightweight = lightweight_fixture


def test_output_gate_physical_domain(tmp_path):
    workspace = Workspace.init(tmp_path / "w")
    assert _real_output_root(workspace.tasks, physical_root=workspace.root) == workspace.tasks
    outside = tmp_path / "outside"
    outside.mkdir()
    with pytest.raises(ValueError):
        _real_output_root(outside, physical_root=workspace.root)
    with pytest.raises(ValueError):
        _real_output_root(workspace.tasks / ".." / "tasks", physical_root=workspace.root)


def test_capacity_api_frozen_without_quota(tmp_path):
    capacity = replace(CapacityConfig(), rpc_line_bytes=1024)
    workspace = Workspace.init(tmp_path / "w", capacity=capacity)
    assert effective_capacity(SimpleNamespace(capacity=capacity)) == capacity
    assert Workspace.open(workspace.root).capacity == capacity
    assert not list(workspace.state.iterdir())
    with pytest.raises(ValueError, match="no consumption ledger"):
        workspace.ledger()


def test_control_explicit_conflict_prewrite(tmp_path, capsys):
    capacity = replace(CapacityConfig(), task_db_bytes=1 << 20, task_journal_bytes=2 << 20)
    workspace = Workspace.init(tmp_path / "w", capacity=capacity)
    other = Workspace.init(tmp_path / "other")
    directory = workspace.tasks / "task"
    with TaskDB.create(
        directory,
        workspace,
        {},
        [SelectedRecord(0, "0" * 32, "s", "d", "0")],
        publication_path=tmp_path / "pub",
    ):
        pass
    before = (directory / "task.sqlite").read_bytes()
    for action in ("inspect", "pause", "cancel"):
        assert main(["task", action, str(directory), "--workspace", str(other.root)]) == 2
        assert json.loads(capsys.readouterr().out)["code"] == "TASK_WORKSPACE_CONFLICT"
        assert (directory / "task.sqlite").read_bytes() == before
    for action in ("pause", "cancel"):
        assert main(["task", action, str(directory), "--workspace", str(workspace.root)]) == 0
        assert json.loads(capsys.readouterr().out)["requested"] == action.upper()


def test_offline_help_no_workspace_quota_update(capsys):
    for argv in (["task", "--help"], ["workspace", "--help"], ["doctor", "--help"]):
        with pytest.raises(SystemExit) as caught:
            main(argv)
        assert caught.value.code == 0
        assert "max-output-bytes" not in capsys.readouterr().out
    with pytest.raises(SystemExit) as caught:
        main(["workspace", "update", "unused", "--config", "unused"])
    assert caught.value.code == 2


@pytest.mark.parametrize("workers", [1, 4])
@pytest.mark.parametrize("control_request", ["PAUSE", "CANCEL"])
def test_control_drains_admitted_only(lightweight, workers, control_request):
    env = lightweight
    with create_task(env.publication, env.directory, env.workspace, env.query):
        pass
    fired = []

    def hook(event, item):
        if event == "CLAIMED" and not fired:
            fired.append(True)
            with TaskDB(env.directory) as task:
                task.request(control_request)

    result = run_task(
        env.directory, env.transport(), workers=workers, control=object(), fault_hook=hook
    )
    assert result["state"] == ("PAUSED" if control_request == "PAUSE" else "CANCELLED")
    assert result["delivered_verified"] <= workers
    final = run_task(env.directory, env.transport(), workers=workers, control=object(), resume=True)
    assert final["delivered_verified"] == 4
