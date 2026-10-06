import json
from types import SimpleNamespace

from sakurapool.tasks import cli, diagnostic
from sakurapool.tasks.store import TaskError


def test_sensitive_dynamic_and_deep_stack():
    secret = 'SECRET_TOKEN_DO_NOT_PRINT'
    namespace = {}
    source = ('def ' + secret + '(n):\n if n: return ' + secret + '(n-1)\n'
              ' raise ValueError("' + secret + '")\n')
    exec(compile(source, 'C:/secret/' + secret + '.py', 'exec'), namespace)
    try:
        namespace[secret](70)
    except ValueError as error:
        result = diagnostic.safe_diagnostic(error)
    payload = json.dumps(result)
    assert secret not in payload
    assert 'C:/' not in payload
    assert len(result['frames']) == 32
    assert all(x == {'file': 'untrusted', 'function': 'untrusted', 'line': 0}
               for x in result['frames'])
    assert len(payload.encode()) <= 8192


def test_replaced_code_identity_not_trusted():
    from types import FunctionType

    original = cli.command.__code__
    replaced = original.replace(co_filename='SECRET_TOKEN', co_name='SECRET_TOKEN')
    function = FunctionType(replaced, {})
    try:
        function(SimpleNamespace())
    except Exception as error:
        result = diagnostic.safe_diagnostic(error)
    assert 'SECRET' not in json.dumps(result)
    assert result['frames'][-1] == {'file': 'untrusted', 'function': 'untrusted', 'line': 0}


def test_dynamic_exception_type():
    kind = type('SECRET_EXCEPTION', (Exception,), {'__module__': 'builtins'})
    result = diagnostic.safe_diagnostic(kind('SECRET_MESSAGE'))
    assert result == {'exception_type': 'untrusted_exception', 'frames': []}


def test_generic_cli_exit_and_trusted_frame(monkeypatch, capsys):
    monkeypatch.setattr(cli.Workspace, 'open', lambda *args: (_ for _ in ()).throw(
        ValueError('SECRET_TOKEN')))
    assert cli.command(SimpleNamespace(task_command='create', workspace='SECRET_PATH')) == 2
    result = json.loads(capsys.readouterr().out)
    assert result['code'] == 'TASK_FAILED'
    assert result['phase'] == 'task'
    assert result['recoverable'] is False
    assert result['diagnostics']['exception_type'] == 'ValueError'
    assert any(x['file'] == 'cli.py' and x['function'] == 'command'
               for x in result['diagnostics']['frames'])
    assert 'SECRET' not in json.dumps(result)
    assert len(json.dumps(result).encode('ascii')) <= 8192


def test_taskerror_unchanged_and_diagnostic_failure(monkeypatch, capsys):
    error = TaskError('SAFE_CODE', 'task')
    monkeypatch.setattr(cli.Workspace, 'open', lambda *args: (_ for _ in ()).throw(error))
    assert cli.command(SimpleNamespace(task_command='create', workspace='unused')) == 2
    assert json.loads(capsys.readouterr().out) == error.public_diagnostic()
    monkeypatch.setattr(cli.Workspace, 'open', lambda *args: (_ for _ in ()).throw(ValueError()))
    monkeypatch.setattr(diagnostic, 'safe_diagnostic',
                        lambda *args: (_ for _ in ()).throw(RuntimeError()))
    assert cli.command(SimpleNamespace(task_command='create', workspace='unused')) == 2
    assert json.loads(capsys.readouterr().out)['diagnostics'] == {
        'exception_type': 'untrusted_exception', 'frames': []}
