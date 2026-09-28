"""P4_PARTIAL CLI boundary through the real module entrypoint, no network."""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest


@pytest.mark.parametrize("cluster", [4096, 8192, 65536, 1 << 20])
def test_inspect_plan_reservation_uses_actual_allocation_cluster(cluster):
    from sakurapool.storage.cli_ops import _MAX_PLAN, _plan_disk_reservation

    each = (_MAX_PLAN + cluster - 1) // cluster * cluster
    assert _plan_disk_reservation(cluster) == 2 * each + 2 * cluster
    with pytest.raises(ValueError, match="cluster"):
        _plan_disk_reservation(3 * cluster)


@pytest.mark.parametrize("operation", ["inspect", "scan-remote", "fetch"])
def test_remote_cli_invalid_profile_or_blocked_scan_no_output(tmp_path, operation):
    package = tmp_path / "package"
    package.mkdir()
    (package / "index-package.json").write_text("{}", encoding="utf-8")
    config = tmp_path / "unread-config.json"
    output = tmp_path / "must-not-create"
    argv = (['remote', 'inspect', '--config', str(config), '--output', str(output)]
            if operation == 'inspect' else
            ['index', 'scan-remote', '--config', str(config), '--plan',
             str(tmp_path / 'unread-plan.json'), '--output-package', str(output)]
            if operation == 'scan-remote' else
            ['remote', 'fetch', '--package', str(package), '--config', str(config),
             '--record-id', 'a' * 32, '--output', str(output)])
    env = os.environ.copy()
    env['PYTHONPATH'] = str(Path(__file__).resolve().parents[1] / 'src')
    finished = subprocess.run([sys.executable, '-m', 'sakurapool', *argv],
                              capture_output=True, text=True, env=env,
                              timeout=20, check=False)
    assert finished.returncode == (3 if operation == 'scan-remote' else 2)
    assert json.loads(finished.stdout)['status'] == (
        'BLOCKED' if operation == 'scan-remote' else 'ERROR')
    assert finished.stderr.strip().startswith('remote')
    assert not output.exists()


def test_profile_rejects_offline_external_and_production_root_swap(tmp_path):
    from sakurapool.storage.cli_ops import _profile
    from sakurapool.storage.modelscope import REPO_ID

    base = {"repo_id": REPO_ID, "endpoint": "https://modelscope.cn",
            "revision": "a" * 40, "trusted_hosts": ["modelscope.cn"],
            "work_root": str(tmp_path)}
    path = tmp_path / "profile.json"
    path.write_text(json.dumps(base), encoding="utf-8")
    with pytest.raises(ValueError, match="incorrect production/offline budget root"):
        _profile(path, offline_fixture=False)
    with pytest.raises(ValueError, match="literal 127"):
        _profile(path, offline_fixture=True)
    base["trusted_hosts"] = ["modelscope.cn", "cdn.example.invalid"]
    path.write_text(json.dumps(base), encoding="utf-8")
    with pytest.raises(ValueError, match="fixed audited origin"):
        _profile(path, offline_fixture=False)
    assert not (tmp_path / "p4-budget.sqlite").exists()


def test_production_inspect_session_mock_routes_without_socket(tmp_path, monkeypatch,
                                                                capsys):
    import io
    from dataclasses import dataclass, field
    from urllib.parse import urlsplit

    import requests

    from sakurapool.cli import main
    from sakurapool.storage import cli_ops

    @dataclass
    class MockProductionLedger:
        root: Path
        offline_mode: bool = False
        attempts: list = field(default_factory=list)
        def reserve(self, request):
            self.attempts.append(request)
            return str(len(self.attempts))
        def consume_body(self, *_args, **_kwargs):
            pass
        def settle(self, *_args, **_kwargs):
            pass

    class Raw:
        def __init__(self, data):
            self.io = io.BytesIO(data)
        def read(self, size, *, decode_content):
            assert decode_content is False
            return self.io.read(size)

    class Response:
        status_code = 200
        headers = {"Content-Encoding": "identity"}
        def __init__(self, data):
            self.raw = Raw(json.dumps({"Data": data}).encode())
        def close(self):
            pass

    ledger = MockProductionLedger(tmp_path)
    config = {"repo_id": "leafmoone/game_cg_5M",
              "endpoint": "https://modelscope.cn", "revision": "a" * 40,
              "trusted_hosts": ["modelscope.cn"], "work_root": str(tmp_path)}
    # Only the production ledger construction is mocked; real URL routing,
    # transport response accounting, provider parsing and CLI publication run.
    monkeypatch.setattr(cli_ops, "_profile", lambda *_args, **_kwargs: (config, ledger))
    monkeypatch.delenv("MODELSCOPE_API_TOKEN", raising=False)
    calls = []
    def no_socket_get(_session, url, *, headers, stream, allow_redirects, timeout):
        assert stream and not allow_redirects and timeout == (10, 60)
        assert urlsplit(url).hostname == "modelscope.cn"
        assert headers.get("Authorization") is None
        calls.append(url)
        if url.endswith('/revisions'):
            return Response({"RevisionMap": {"Tags": [], "Branches": [
                {"CommitId": config["revision"]}]}})
        return Response({"Total": 1, "Files": [{"Path": "small.tar",
                                                 "Size": 1024, "Type": "blob"}]})
    monkeypatch.setattr(requests.Session, "get", no_socket_get)
    output = tmp_path / "mock-production-inspect.json"
    assert main(["remote", "inspect", "--config", str(tmp_path / "mocked-config"),
                 "--output", str(output)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "inspected" and result["files_complete"]
    plan = json.loads(output.read_text(encoding="utf-8"))
    assert plan["requested_revision"] == config["revision"]
    assert plan["resolved_revision"] is None
    assert not plan["version_capability_verified"]
    assert not plan["conditional_capability_verified"]
    assert plan["source_and_tag_mapping"].startswith("unconfirmed")
    assert len(calls) == 2 and len(ledger.attempts) == 3


@pytest.mark.parametrize("error,expected", [
    ("http", {"code": "http_status", "phase": "metadata_headers", "http_status": 403}),
    ("shape", {"code": "provider_shape", "phase": "provider_revision_shape"}),
    ("unknown", {"code": "local_or_unclassified", "phase": "local"}),
])
def test_remote_cli_public_diagnostic_has_only_whitelisted_fields(
        tmp_path, monkeypatch, capsys, error, expected):
    from sakurapool.cli import main
    from sakurapool.storage import cli_ops
    from sakurapool.storage.transport import RemoteIOError

    secret = "SIGNED_URL_TOKEN_SUPER_SECRET"
    def denied(*_args, **_kwargs):
        if error == "http":
            raise RemoteIOError(secret, code="http_status", phase="metadata_headers",
                                http_status=403)
        if error == "shape":
            raise RemoteIOError(secret, code="provider_shape",
                                phase="provider_revision_shape")
        raise RuntimeError(secret)
    monkeypatch.setattr(cli_ops, "inspect", denied)
    assert main(["remote", "inspect", "--config", str(tmp_path / "missing"),
                 "--output", str(tmp_path / "out")]) == 2
    stdout, stderr = capsys.readouterr()
    assert secret not in stdout + stderr
    assert json.loads(stdout) == {"error": "remote operation failed", "status": "ERROR",
                                  **expected}
    assert stderr == "remote operation failed; no implicit scan or fallback\n"


@pytest.mark.parametrize("failure", ["headers", "shape", "write", "cancel", "user-temp"])
def test_inspect_failure_releases_only_owned_disk_and_protects_user_file(
        monkeypatch, failure):
    from sakurapool.storage import cli_ops
    from sakurapool.storage.budget import DEFAULT_WORK_ROOT, BudgetLedger
    from sakurapool.storage.transport import RemoteIOError

    with tempfile.TemporaryDirectory(prefix="offline-inspect-fail-",
                                     dir=DEFAULT_WORK_ROOT) as work:
        root = Path(work)
        ledger = BudgetLedger(root, _offline_test=True)
        config = {"repo_id": "leafmoone/game_cg_5M", "endpoint": "http://127.0.0.1:80",
                  "revision": None, "trusted_hosts": ["127.0.0.1"], "work_root": str(root)}
        monkeypatch.setattr(cli_ops, "_profile", lambda *_args, **_kwargs: (config, ledger))
        class Transport:
            def __enter__(self): return self
            def __exit__(self, *_args): return None
        monkeypatch.setattr(cli_ops, "_transport", lambda *_args: Transport())
        secret = "SIGNED_URL_TOKEN_SUPER_SECRET"
        class Provider:
            def __init__(self, *_args): pass
            def revisions(self):
                if failure == "headers":
                    raise RemoteIOError("static", code="http_status",
                                        phase="metadata_headers", http_status=401)
                if failure == "shape":
                    raise RemoteIOError("static", code="provider_shape",
                                        phase="provider_revision_shape")
                return ["a" * 40]
        monkeypatch.setattr(cli_ops, "ModelScopeDataset", Provider)
        user_file = root / (".sakurapool-inspect-" + "c" * 32)
        if failure == "user-temp":
            user_file.write_text(secret, encoding="utf-8")
            monkeypatch.setattr(cli_ops.secrets, "token_hex", lambda _n: "c" * 32)
        elif failure in {"write", "cancel"}:
            def break_link(*_args, **_kwargs):
                if failure == "cancel":
                    raise KeyboardInterrupt
                raise OSError(secret)
            monkeypatch.setattr(cli_ops.os, "link", break_link)
        output = root / "new-inspect-plan.json"
        before = ledger.status()
        with pytest.raises((RemoteIOError, OSError, KeyboardInterrupt, FileExistsError)):
            cli_ops.inspect(root / "ignored.json", output, offline_fixture=True)
        after = ledger.status()
        assert after["attempts"] == before["attempts"]
        assert after["body"] == after["metadata"] == 0
        with ledger._locked():
            _, (_generation, _used, pending, _proofs) = ledger._read_pair()
            assert not pending
        assert not output.exists()
        assert not [p for p in root.glob(".sakurapool-inspect-*")
                    if p != user_file]
        if failure == "user-temp":
            assert user_file.read_text(encoding="utf-8") == secret


@pytest.mark.parametrize("failure", ["after_publish_settle", "temp_cleanup"])
def test_inspect_post_publish_failure_keeps_plan_and_never_refunds_unknown(
        monkeypatch, failure):
    from sakurapool.storage import cli_ops
    from sakurapool.storage.budget import DEFAULT_WORK_ROOT, BudgetLedger

    with tempfile.TemporaryDirectory(prefix="offline-inspect-publish-",
                                     dir=DEFAULT_WORK_ROOT) as work:
        root = Path(work)
        ledger = BudgetLedger(root, _offline_test=True)
        config = {"repo_id": "leafmoone/game_cg_5M", "endpoint": "http://127.0.0.1:80",
                  "revision": None, "trusted_hosts": ["127.0.0.1"], "work_root": str(root)}
        monkeypatch.setattr(cli_ops, "_profile", lambda *_args, **_kwargs: (config, ledger))
        class Transport:
            def __enter__(self): return self
            def __exit__(self, *_args): return None
        class Provider:
            def __init__(self, *_args): pass
            def revisions(self): return ["a" * 40]
        monkeypatch.setattr(cli_ops, "_transport", lambda *_args: Transport())
        monkeypatch.setattr(cli_ops, "ModelScopeDataset", Provider)
        user_file = root / "user-file"
        user_file.write_bytes(b"NEVER_CHANGE")
        output = root / "published-inspect-plan.json"
        if failure == "after_publish_settle":
            def disk_settle_error(_lease):
                raise OSError("synthetic after-publication settle fault")
            monkeypatch.setattr(ledger, "settle", disk_settle_error)
        else:
            real_unlink = Path.unlink
            def cleanup_error(path, *args, **kwargs):
                if path.name.startswith(".sakurapool-inspect-"):
                    raise OSError("synthetic cleanup fault")
                return real_unlink(path, *args, **kwargs)
            monkeypatch.setattr(Path, "unlink", cleanup_error)
        with pytest.raises(OSError, match="fault"):
            cli_ops.inspect(root / "unused-profile", output, offline_fixture=True)
        assert json.loads(output.read_bytes())["requested_revision"] is None
        assert user_file.read_bytes() == b"NEVER_CHANGE"
        assert ledger.status()["attempts"] == 0
        with ledger._locked():
            _, (_, _, leases, _) = ledger._read_pair()
            if failure == "after_publish_settle":
                assert len(leases) == 1
                assert next(iter(leases.values()))["disk"] > 0
            else:
                assert not leases
                assert len(list(root.glob(".sakurapool-inspect-*"))) == 1
        if failure == "temp_cleanup":
            monkeypatch.setattr(Path, "unlink", real_unlink)


def test_remote_fetch_missing_package_never_scans(tmp_path):
    from sakurapool.cli import main

    missing = tmp_path / 'missing'
    result = main(['remote', 'fetch', '--package', str(missing), '--config',
                   str(tmp_path / 'unread-config'), '--record-id', 'a' * 32,
                   '--output', str(tmp_path / 'not-created')])
    assert result == 2
    assert not missing.exists()
    assert not (tmp_path / 'not-created').exists()
