"""Formal default download entrypoints and legacy pre-write gates, offline only."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_lightweight_tasks import lightweight as lightweight_fixture

from sakurapool.cli import main
from sakurapool.image_formats import SUPPORTED_IMAGE_EXTENSIONS
from sakurapool.storage.publication import load_publication
from sakurapool.storage.publication_fetch import fetch_publication_sample
from sakurapool.storage.publication_session import PublicationSession
from sakurapool.tasks.plan import canonical, plan_digest
from sakurapool.tasks.profile import connect_profile
from sakurapool.tasks.store import TaskDB, TaskError
from sakurapool.workspace import Workspace

lightweight = lightweight_fixture


def test_default_direct_session_cli_task_create_run_export(lightweight, monkeypatch, capsys):
    env = lightweight
    direct = env.workspace.root / "direct"
    direct.mkdir()
    with load_publication(env.publication, full_verify=True) as pub:
        record = pub.runtime.resolve_one("synthetic", "0", "small").record_id
        assert fetch_publication_sample(
            pub, record, env.transport(), direct, control=object()
        ).is_file()
    session_root = env.workspace.root / "session"
    session_root.mkdir()
    with PublicationSession(env.publication, env.transport(), control=object()) as session:
        assert session.fetch(record, session_root, filename_index=1).is_file()
    query = env.workspace.root / "query.json"
    query.write_text(
        json.dumps(
            {"sources": ["synthetic"], "namespace": "danbooru_native", "all_tags": ["1girl"]}
        )
    )
    profile = env.workspace.root / "profile.json"
    profile.write_text(
        json.dumps(
            {
                "format": "sakurapool-task-connection-v1",
                "origin": "https://modelscope.cn",
                "repositories": ["synthetic/small"],
                "worker": str(Path(__file__).absolute()),
            }
        )
    )
    from sakurapool.tasks import profile as profile_module

    calls = []

    def connection(description, *, root, capacity=None):
        calls.append((Path(root), capacity))
        transport = env.transport()
        transport.__class__.__enter__ = lambda self: self
        transport.__class__.__exit__ = lambda self, *_args: self.close()
        return transport

    monkeypatch.setattr(profile_module, "connect_profile", connection)
    assert (
        main(
            [
                "task",
                "create",
                "--publication",
                str(env.publication),
                "--task-dir",
                str(env.directory),
                "--workspace",
                str(env.workspace.root),
                "--query",
                str(query),
                "--selection",
                "sample",
                "--limit",
                "2",
                "--seed",
                "frozen",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["format"] == "sakurapool-task-v4"
    assert (
        main(["task", "run", str(env.directory), "--profile", str(profile), "--workers", "4"]) == 0
    )
    assert json.loads(capsys.readouterr().out)["delivered_verified"] == 2
    assert (
        main(["task", "export", str(env.directory), "--manifest", str(env.directory / "cli.jsonl")])
        == 0
    )
    assert json.loads(capsys.readouterr().out)["exported"] == 2
    cli_output = env.workspace.root / "cli-direct"
    cli_output.mkdir()
    assert (
        main(
            [
                "publication",
                "fetch",
                "--publication",
                str(env.publication),
                "--record-id",
                record,
                "--output",
                str(cli_output),
                "--profile",
                str(profile),
                "--workspace",
                str(env.workspace.root),
            ]
        )
        == 0
    )
    assert Path(json.loads(capsys.readouterr().out)["output"]).is_file()
    assert all(root == env.workspace.root for root, _ in calls)
    assert not list(env.workspace.state.iterdir())


def test_connect_profile_explicit_no_ledger_and_root(tmp_path, monkeypatch):
    from sakurapool.tasks import profile as module

    seen = []

    def constructed(ledger, worker, **kwargs):
        assert ledger is None
        assert kwargs["root"] == tmp_path
        assert kwargs["token"] is None
        seen.append(kwargs)
        return SimpleNamespace(capacity=kwargs["capacity"])

    monkeypatch.setattr(module, "RustProductionTransport", constructed)
    result = connect_profile(
        {
            "format": "sakurapool-task-connection-v1",
            "origin": "https://modelscope.cn",
            "repositories": ["synthetic/small"],
            "worker": "unused",
        },
        root=tmp_path,
    )
    assert result.capacity == seen[0]["capacity"]


@pytest.mark.parametrize("version", [1, 2])
def test_legacy_read_all_writes_no_sidecars_ledger_or_profile(
    tmp_path, monkeypatch, version, capsys
):
    from sakurapool.capacity import ResourcePolicy
    from sakurapool.storage import budget
    from sakurapool.tasks import export as export_module
    from sakurapool.tasks import store

    workspace = Workspace.init(tmp_path / "workspace")
    directory = workspace.tasks / "legacy"
    with TaskDB.create(
        directory, workspace, {"metadata": False}, [], publication_path=tmp_path / "unused"
    ) as task:
        header = task.meta("header")
        if version == 1:
            header["format"] = "sakurapool-task-v1"
            header.pop("workspace_binding")
            header.pop("effective_capacity")
        else:
            data = json.loads((workspace.root / "workspace.json").read_bytes())
            data.update(
                format="sakurapool-workspace-v1",
                ledger_lineage="a" * 32,
                initial_policy=ResourcePolicy().to_dict(),
            )
            (workspace.root / "workspace.json").write_bytes(canonical(data))
            workspace = Workspace.open(workspace.root)
            header["format"] = "sakurapool-task-v2"
            header["workspace_binding"] = workspace.task_binding(directory)
        task.db.execute(f"PRAGMA user_version={version}")
        task.db.execute("ALTER TABLE items ADD COLUMN accounting TEXT DEFAULT 'NONE'")
        task.db.execute("UPDATE meta SET value=? WHERE key='header'", (canonical(header).decode(),))
        task.db.execute(
            "UPDATE meta SET value=? WHERE key='plan_digest'",
            (canonical(plan_digest(header)).decode(),),
        )
    if version == 1:
        moved = tmp_path / "standalone"
        directory.rename(moved)
        directory = moved

    def forbidden(*args, **kwargs):
        raise AssertionError("legacy read/writable gate touched a ledger")

    monkeypatch.setattr(budget.BudgetLedger, "__init__", forbidden)
    monkeypatch.setattr(Workspace, "ledger", forbidden)
    original = store._connect

    def guarded(path, **kwargs):
        assert kwargs.get("readonly") is True
        return original(path, **kwargs)

    monkeypatch.setattr(store, "_connect", guarded)
    before = (directory / "task.sqlite").read_bytes()
    policy_before = (workspace.root / "workspace.json").read_bytes()
    entries_before = {p.relative_to(directory) for p in directory.rglob("*")}
    monkeypatch.setattr(export_module, "load_publication", forbidden)
    if version == 2:
        assert Workspace.open(workspace.root).inspect()["read_only_legacy"] is True
        assert not list(workspace.state.iterdir())
    assert main(["task", "inspect", str(directory)]) == 0
    assert json.loads(capsys.readouterr().out)["migration_required"] is True
    for action in ("pause", "cancel", "run", "resume", "update", "export"):
        argv = ["task", action, str(directory)]
        if action in ("run", "resume"):
            argv += ["--profile", "MISSING_SECRET_PROFILE"]
        if action == "export":
            argv += ["--manifest", str(directory / "legacy.jsonl")]
        if action == "update":
            argv += [
                "--expected-settings-version",
                "0",
                "--image-extensions",
                ",".join(SUPPORTED_IMAGE_EXTENSIONS),
            ]
        assert main(argv) == 2
        assert json.loads(capsys.readouterr().out)["code"] == "LEGACY_TASK_MIGRATION_REQUIRED"
    with pytest.raises(TaskError, match="MIGRATION_REQUIRED"):
        TaskDB(directory)
    with pytest.raises(TaskError, match="MIGRATION_REQUIRED"):
        export_module.export_task(directory, directory / "api-legacy.jsonl")
    assert before == (directory / "task.sqlite").read_bytes()
    assert policy_before == (workspace.root / "workspace.json").read_bytes()
    assert entries_before == {p.relative_to(directory) for p in directory.rglob("*")}
    assert not list(workspace.state.iterdir())
    assert not list(directory.glob("task.sqlite-*"))
