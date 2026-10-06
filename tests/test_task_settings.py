"""Explicit lightweight format CAS, no removed output-budget semantics."""

import json
import sqlite3
from types import SimpleNamespace

import pytest
from test_lightweight_tasks import lightweight as lightweight_fixture

from sakurapool.image_formats import (
    DEFAULT_IMAGE_EXTENSIONS,
    SUPPORTED_IMAGE_EXTENSIONS,
    image_extensions,
)
from sakurapool.tasks.runner import create_task
from sakurapool.tasks.settings import update_task
from sakurapool.tasks.store import TaskDB, TaskError

lightweight = lightweight_fixture


@pytest.fixture
def task(lightweight):
    env = lightweight
    with create_task(env.publication, env.directory, env.workspace, env.query):
        pass
    return env


def test_atomic_cas_and_preserved_selection(task):
    directory = task.directory
    with TaskDB(directory, readonly=True) as db:
        header, digest = db.meta("header"), db.meta("plan_digest")
    result = update_task(
        directory, expected_settings_version=0, extensions=SUPPORTED_IMAGE_EXTENSIONS
    )
    assert result["settings_version"] == 1
    with TaskDB(directory, readonly=True) as db:
        assert db.meta("header") == header
        assert db.meta("plan_digest") == digest
        assert db.image_extensions == SUPPORTED_IMAGE_EXTENSIONS
        assert not db.db.execute("SELECT 1 FROM meta WHERE key='max_output_bytes'").fetchone()
        assert not db.db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='settings_events'"
        ).fetchone()
    with pytest.raises(TaskError, match="SETTINGS_VERSION_CONFLICT"):
        update_task(directory, expected_settings_version=0, extensions=SUPPORTED_IMAGE_EXTENSIONS)
    with pytest.raises(TaskError, match="IMAGE_EXTENSIONS_REMOVAL"):
        update_task(directory, expected_settings_version=1, extensions=DEFAULT_IMAGE_EXTENSIONS)
    assert not list(task.workspace.state.iterdir())


def test_pending_and_runner_busy(task):
    directory = task.directory
    with TaskDB(directory) as db, db.runner_lock():
        with pytest.raises(TaskError, match="RUNNER_BUSY"):
            update_task(
                directory, expected_settings_version=0, extensions=SUPPORTED_IMAGE_EXTENSIONS
            )
    with TaskDB(directory) as db:
        db.claim()
    with pytest.raises(TaskError, match="RUNNER_BUSY"):
        update_task(directory, expected_settings_version=0, extensions=SUPPORTED_IMAGE_EXTENSIONS)


@pytest.mark.parametrize("extensions", [[".exe"], ["../gif"], [".gif", ".gif"], []])
def test_unknown_extensions(extensions):
    with pytest.raises(ValueError):
        image_extensions(extensions)


def test_sql_rollback_preserves_both_settings(task):
    directory = task.directory
    with TaskDB(directory) as db:
        db.db.execute(
            "CREATE TRIGGER fail_settings BEFORE UPDATE ON meta "
            "WHEN NEW.key='image_extensions' BEGIN SELECT RAISE(ABORT,'fault'); END"
        )
    with pytest.raises(sqlite3.IntegrityError, match="fault"):
        update_task(directory, expected_settings_version=0, extensions=SUPPORTED_IMAGE_EXTENSIONS)
    with TaskDB(directory, readonly=True) as db:
        assert db.settings_version == 0
        assert db.image_extensions == DEFAULT_IMAGE_EXTENSIONS


@pytest.mark.parametrize("version", [True, -1, 1.0, "0", 1 << 63, (1 << 63) - 1])
def test_stored_version_boundaries(task, version):
    with TaskDB(task.directory) as db, db.transaction() as conn:
        db.set_meta(conn, "settings_version", version)
    code = "EXHAUSTED" if type(version) is int and version == (1 << 63) - 1 else "INVALID"
    with pytest.raises(TaskError, match="SETTINGS_VERSION_" + code):
        update_task(
            task.directory,
            expected_settings_version=version if code == "EXHAUSTED" else 0,
            extensions=SUPPORTED_IMAGE_EXTENSIONS,
        )


def test_workspace_update_cli(task, capsys):
    from sakurapool.tasks import cli

    assert (
        cli.command(
            SimpleNamespace(
                task_command="update",
                task_dir=str(task.directory),
                workspace=None,
                expected_settings_version=0,
                image_extensions=",".join(SUPPORTED_IMAGE_EXTENSIONS),
            )
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["settings_version"] == 1
    assert not list(task.workspace.state.iterdir())
