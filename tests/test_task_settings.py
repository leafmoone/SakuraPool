import pytest

from sakurapool.image_formats import (
    DEFAULT_IMAGE_EXTENSIONS,
    SUPPORTED_IMAGE_EXTENSIONS,
    image_extensions,
)
from sakurapool.storage.budget import Reservation
from sakurapool.tasks.settings import update_task
from sakurapool.tasks.store import TaskDB, TaskError

pytest_plugins = ['test_task_multiple']


def test_atomic_cas_and_preserved_selection(multiple):
    directory, ledger, _, _ = multiple
    before = ledger.status()
    with TaskDB(directory, readonly=True) as task:
        header, digest = task.meta('header'), task.meta('plan_digest')
    result = update_task(directory, ledger, expected_settings_version=0,
                         max_output_bytes=1536 << 20, extensions=SUPPORTED_IMAGE_EXTENSIONS)
    assert result['settings_version'] == 1
    assert {k: v for k, v in ledger.status().items() if k != 'disk'} == {
        k: v for k, v in before.items() if k != 'disk'
    }
    with TaskDB(directory, readonly=True) as task:
        assert task.meta('header') == header
        assert task.meta('plan_digest') == digest
        assert task.image_extensions == SUPPORTED_IMAGE_EXTENSIONS
        assert task.db.execute('select count(*) from settings_events').fetchone()[0] == 1
    with pytest.raises(TaskError, match='SETTINGS_VERSION_CONFLICT'):
        update_task(directory, ledger, expected_settings_version=0, max_output_bytes=1)
    with pytest.raises(TaskError, match='IMAGE_EXTENSIONS_REMOVAL'):
        update_task(directory, ledger, expected_settings_version=1, max_output_bytes=1,
                    extensions=DEFAULT_IMAGE_EXTENSIONS)
    with TaskDB(directory, readonly=True) as task:
        assert task.meta('max_output_bytes') == 1536 << 20
        assert task.settings_version == 1
    assert {k: v for k, v in ledger.status().items() if k != 'disk'} == {
        k: v for k, v in before.items() if k != 'disk'
    }


@pytest.mark.parametrize('value', [True, -1, 1 << 63, '4'])
def test_invalid_budget(multiple, value):
    directory, ledger, _, _ = multiple
    with pytest.raises(TaskError, match='OUTPUT_BUDGET_INVALID'):
        update_task(directory, ledger, expected_settings_version=0, max_output_bytes=value)


def test_pending_and_runner_busy(multiple):
    directory, ledger, _, _ = multiple
    lease = ledger.reserve(Reservation(disk=1))
    try:
        with pytest.raises(TaskError, match='ACCOUNTING_BUSY'):
            update_task(directory, ledger, expected_settings_version=0, max_output_bytes=0)
    finally:
        ledger.settle(lease)
    with TaskDB(directory) as task, task.runner_lock():
        with pytest.raises(TaskError, match='RUNNER_BUSY'):
            update_task(directory, ledger, expected_settings_version=0, max_output_bytes=0)


def test_confirmed_lower_bound_and_unknown(multiple):
    directory, ledger, _, _ = multiple
    with TaskDB(directory) as task, task.transaction() as db:
        task.set_meta(db, 'confirmed_output_bytes', 10)
    with pytest.raises(TaskError, match='OUTPUT_BUDGET_BELOW_CONFIRMED'):
        update_task(directory, ledger, expected_settings_version=0, max_output_bytes=9)
    update_task(directory, ledger, expected_settings_version=0, max_output_bytes=10)
    with TaskDB(directory) as task, task.transaction() as db:
        db.execute("update items set accounting='UNKNOWN' where seq=0")
    with pytest.raises(TaskError, match='ACCOUNTING_BUSY'):
        update_task(directory, ledger, expected_settings_version=1, max_output_bytes=20)


@pytest.mark.parametrize('extensions', [['.exe'], ['../gif'], ['.gif','.gif'], []])
def test_unknown_extensions(extensions):
    with pytest.raises(ValueError):
        image_extensions(extensions)


def test_sql_rollback_and_owned_settlement(multiple):
    directory, ledger, _, _ = multiple
    with TaskDB(directory) as task:
        task.db.execute("CREATE TRIGGER fail_settings BEFORE INSERT ON meta "
                        "WHEN NEW.key='settings_version' BEGIN SELECT RAISE(ABORT,'fault'); END")
    import sqlite3

    with pytest.raises(sqlite3.IntegrityError, match='fault'):
        update_task(directory, ledger, expected_settings_version=0,
                    max_output_bytes=100, extensions=SUPPORTED_IMAGE_EXTENSIONS)
    with TaskDB(directory, readonly=True) as task:
        assert task.settings_version == 0
        assert task.image_extensions == DEFAULT_IMAGE_EXTENSIONS
        assert task.meta('max_output_bytes') == 512 << 20
        assert not task.db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='settings_events'"
        ).fetchone()
    with ledger.quiescent_disk_operation(1024):
        pass


def test_settlement_unknown_retained(multiple, monkeypatch):
    directory, ledger, _, _ = multiple
    original = ledger._commit
    calls = []

    def fail_second(*args):
        calls.append(1)
        if len(calls) == 2:
            raise OSError('fault')
        return original(*args)

    monkeypatch.setattr(ledger, '_commit', fail_second)
    with pytest.raises(TaskError, match='TASK_RESOURCE_SETTLEMENT_UNKNOWN'):
        update_task(directory, ledger, expected_settings_version=0, max_output_bytes=100)
    with TaskDB(directory, readonly=True) as task:
        assert task.settings_version == 1
    assert ledger.status()['disk'] > 0
    # Do not clear the unresolved operation lease; subsequent update must refuse.
    with pytest.raises(TaskError, match='ACCOUNTING_BUSY'):
        update_task(directory, ledger, expected_settings_version=1, max_output_bytes=200)


def test_primary_and_settlement_fault(multiple, monkeypatch):
    directory, ledger, _, _ = multiple
    original = ledger._commit
    calls = []

    def fail_second(*args):
        calls.append(1)
        if len(calls) == 2:
            raise OSError('fault')
        return original(*args)

    monkeypatch.setattr(ledger, '_commit', fail_second)
    with pytest.raises(TaskError, match='SETTINGS_VERSION_CONFLICT') as raised:
        update_task(directory, ledger, expected_settings_version=1, max_output_bytes=10)
    assert raised.value.task_secondary == ('TASK_RESOURCE_SETTLEMENT_UNKNOWN',)
    with TaskDB(directory, readonly=True) as task:
        assert task.settings_version == 0
    with pytest.raises(TaskError, match='ACCOUNTING_BUSY'):
        update_task(directory, ledger, expected_settings_version=0, max_output_bytes=10)


@pytest.mark.parametrize('version', [True, -1, 1.0, '0', 1 << 63, (1 << 63) - 1])
def test_stored_version_boundaries(multiple, version):
    directory, ledger, _, _ = multiple
    with TaskDB(directory) as task, task.transaction() as db:
        task.set_meta(db, 'settings_version', version)
    code = 'EXHAUSTED' if type(version) is int and version == (1 << 63) - 1 else 'INVALID'
    with pytest.raises(TaskError, match='SETTINGS_VERSION_' + code):
        update_task(directory, ledger,
                    expected_settings_version=version if code == 'EXHAUSTED' else 0,
                    max_output_bytes=100)
    with ledger.quiescent_disk_operation(1024):
        pass


def test_bounded_quiescent_frames(tmp_path, monkeypatch):
    from sakurapool.storage.budget import BudgetCorrupt
    from sakurapool.workspace import Workspace

    ledger = Workspace.init(tmp_path / 'workspace').ledger()
    original = ledger._commit
    calls = []

    def fault(*args):
        calls.append(1)
        if len(calls) == 2:
            raise OSError('settlement fault')
        return original(*args)

    manager = ledger.quiescent_disk_operation(1024)
    with pytest.raises(BudgetCorrupt) as raised:
        with manager as lease:
            assert isinstance(lease, str)
            assert not {'used', 'leases', 'proofs', 'row', 'totals'} & set(
                manager.gen.gi_frame.f_locals
            )
            monkeypatch.setattr(ledger, '_commit', fault)
            # Force finish to be the second commit seen by the fault injector.
            calls.append(1)
    pending = [raised.value]
    while pending:
        error = pending.pop()
        trace = error.__traceback__
        while trace:
            if trace.tb_frame.f_code.co_name in ('_finish_quiescent_disk', 'fault', '_read_pair'):
                assert not trace.tb_frame.f_locals
            trace = trace.tb_next
        if error.__context__:
            pending.append(error.__context__)
    assert ledger.inspect_policy()['pending_count'] == 1


def test_workspace_update_cli_and_old_fallback(tmp_path, capsys):
    from types import SimpleNamespace

    from sakurapool.tasks import cli
    from sakurapool.tasks.store import WORKSPACE_FORMAT
    from sakurapool.workspace import Workspace

    workspace = Workspace.init(tmp_path / 'workspace')
    ledger = workspace.ledger()
    directory = workspace.root / 'tasks' / 'task'
    # TaskDB creates a valid synthetic empty frozen plan without publication IO.
    with TaskDB.create(directory, ledger, {'metadata': False}, [],
                       publication_path=tmp_path / 'unused') as task:
        with task.transaction() as db:
            db.execute("DELETE FROM meta WHERE key IN ('settings_version','image_extensions')")
        assert task.settings_version == 0
        assert task.image_extensions == DEFAULT_IMAGE_EXTENSIONS
        assert task.meta('header')['format'] == WORKSPACE_FORMAT
    before = ledger.inspect_policy()
    assert cli.command(SimpleNamespace(task_command='update', task_dir=str(directory),
        workspace=None, expected_settings_version=0, max_output_bytes=1536 << 20,
        image_extensions=','.join(SUPPORTED_IMAGE_EXTENSIONS))) == 0
    import json
    assert json.loads(capsys.readouterr().out)['settings_version'] == 1
    after = ledger.inspect_policy()
    assert after['pending_count'] == 0
    assert {k: v for k, v in after['usage'].items() if k != 'disk'} == {
        k: v for k, v in before['usage'].items() if k != 'disk'
    }
