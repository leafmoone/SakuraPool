"""Only synthetic credentials and guarded localhost/socket-free token routing."""

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from test_p4_transport import Handler
from test_p4_transport import http_and_budget as _synthetic_http

BRIDGE = Path(__file__).resolve().parents[1] / "tools" / "p4_token_bootstrap.py"
PYTHON = Path(sys.executable)
REPO = Path(__file__).resolve().parents[1]


@pytest.fixture
def local_fixture():
    yield from _synthetic_http.__wrapped__()


def _child(token_file: Path, command: list[str]) -> subprocess.CompletedProcess[str]:
    # Synthetic path only. Child's -I ignores all caller PYTHONPATH settings;
    # bridge independently asserts its installed source origin before runpy.
    code = """import importlib.util,sys,os
from pathlib import Path
s=importlib.util.spec_from_file_location('isolated_bridge',sys.argv[1])
m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
# Apply the fixture root only AFTER the bridge has validated source origin.
# No package pre-import: its fail-closed origin guard remains effective.
exclusive=os.environ.get('SAKURAPOOL_TEST_EXCLUSIVE_ROOT')
if exclusive:
    original=m.runpy.run_module
    def isolated_run(*args,**kwargs):
        import sakurapool.storage.budget as budget
        budget.DEFAULT_WORK_ROOT=Path(exclusive)
        return original(*args,**kwargs)
    m.runpy.run_module=isolated_run
raise SystemExit(m._run_cli(tuple(sys.argv[3:]),token_path=Path(sys.argv[2])))
"""
    env = os.environ.copy()
    env.pop("MODELSCOPE_API_TOKEN", None)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run([str(PYTHON), "-I", "-c", code, str(BRIDGE),
                           str(token_file), *command], cwd=REPO, env=env,
                          capture_output=True, text=True, timeout=20, check=False)


def test_isolated_runpy_executes_real_offline_cli_with_synthetic_token(local_fixture):
    base, ledger, _client = local_fixture
    token = ledger.root / "synthetic-token.txt"
    token.write_text("SYNTHETIC_ONLY_TOKEN\n", encoding="ascii")
    config = ledger.root / "offline-profile.json"
    config.write_text(json.dumps({"repo_id": "leafmoone/game_cg_5M",
                                  "endpoint": base, "revision": None,
                                  "trusted_hosts": ["127.0.0.1"],
                                  "work_root": str(ledger.root)}), encoding="utf-8")
    output = ledger.root / "new-offline-inspect.json"
    child = _child(token, ["remote", "inspect", "--config", str(config),
                           "--output", str(output), "--offline-fixture"])
    assert child.returncode == 0, (child.stdout, child.stderr)
    assert json.loads(child.stdout)["status"] == "inspected"
    assert "SYNTHETIC_ONLY_TOKEN" not in child.stdout + child.stderr
    plan = json.loads(output.read_text(encoding="utf-8"))
    assert plan["requested_revision"] is None
    assert not plan["version_capability_verified"]
    assert [name for name, *_ in Handler.calls] == [
        "api/v1/datasets/leafmoone/game_cg_5M/revisions"]
    assert Handler.calls[0][2] is None  # offline must NEVER forward the token
    assert ledger.status()["attempts"] == 1


@pytest.mark.parametrize("bad", [b"", b"name: secret", b"token with space",
                                 b"first\nsecond", b'{"token":"secret"}',
                                 b"KEY=VALUE", b"abc\x00def"])
def test_invalid_synthetic_token_stops_before_cli_or_http(local_fixture, bad):
    _, ledger, _client = local_fixture
    token = ledger.root / "bad-token.txt"
    token.write_bytes(bad)
    output = ledger.root / "must-not-exist.json"
    child = _child(token, ["remote", "inspect", "--config", "missing",
                           "--output", str(output), "--offline-fixture"])
    assert child.returncode == 64
    assert json.loads(child.stdout) == {
        "status": "ERROR", "error": "remote operation failed",
        "code": "credential_invalid", "phase": "preflight"}
    assert not child.stderr and not output.exists()
    assert ledger.status()["attempts"] == 0 and not Handler.calls


@pytest.mark.parametrize("fault", ["noninteger_exit", "interrupt"])
def test_isolated_wrapper_never_prints_unknown_exit_or_interrupt(tmp_path, fault):
    token = tmp_path / "synthetic-token"
    token.write_text("SYNTHETIC_ONLY_TOKEN", encoding="ascii")
    code = """import importlib.util,runpy,sys
from pathlib import Path
s=importlib.util.spec_from_file_location('isolated_bridge',sys.argv[1])
m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
def faulted_runpy(*_args,**_kwargs):
 if sys.argv[3]=='interrupt':raise KeyboardInterrupt('SYNTHETIC_ONLY_TOKEN')
 raise SystemExit('SYNTHETIC_ONLY_TOKEN')
runpy.run_module=faulted_runpy
raise SystemExit(m._run_cli(('remote','inspect'),token_path=Path(sys.argv[2])))
"""
    env = os.environ.copy()
    env.pop("MODELSCOPE_API_TOKEN", None)
    child = subprocess.run([str(PYTHON), "-I", "-c", code, str(BRIDGE),
                            str(token), fault], cwd=REPO, env=env,
                           capture_output=True, text=True, timeout=20, check=False)
    assert child.returncode == 70
    assert json.loads(child.stdout) == {
        "status": "ERROR", "error": "remote operation failed",
        "code": "local_or_unclassified", "phase": "local"}
    assert "SYNTHETIC_ONLY_TOKEN" not in child.stdout + child.stderr
    assert not child.stderr


@pytest.mark.parametrize("case", [
    "good", "revision", "repo", "endpoint", "trusted_hosts", "root", "bad_type",
    "bad_json", "oversized", "config_link", "output_link", "existing_output",
    "outside_output", "outside_config", "directory_config",
])
def test_production_main_guards_profile_before_loading_token(
        monkeypatch, tmp_path, capsys, case):
    spec = importlib.util.spec_from_file_location("production_bridge", BRIDGE)
    bridge = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bridge)
    # The production fixed-root path is never written by offline tests.
    root = tmp_path / "synthetic-work-root"
    root.mkdir()
    monkeypatch.setattr(bridge, "WORK_ROOT", root)
    config = root / "offline-preflight.json"
    output = root / "offline-preflight-output.json"
    profile = {"repo_id": "leafmoone/game_cg_5M",
               "endpoint": "https://modelscope.cn", "revision": None,
               "trusted_hosts": ["modelscope.cn"], "work_root": str(root)}
    if case == "revision":
        profile["revision"] = "a" * 40
    if case == "repo":
        profile["repo_id"] = "another/target"
    if case == "endpoint":
        profile["endpoint"] = "https://other.example"
    if case == "trusted_hosts":
        profile["trusted_hosts"] = ["modelscope.cn", "other"]
    if case == "root":
        profile["work_root"] = str(tmp_path)
    if case == "bad_type":
        profile["revision"] = 0
    if case == "outside_config":
        config = tmp_path / "outside-config.json"
    if case == "outside_output":
        output = tmp_path / "outside-output.json"
    existing = None
    calls = []
    monkeypatch.setattr(bridge, "_run_cli", lambda argv: calls.append(argv) or 0)
    monkeypatch.setattr(bridge, "_load_token", lambda *_args: pytest.fail(
        "production token must not be opened by main preflight"))
    if case == "directory_config":
        config.mkdir()
    else:
        content = ("{" if case == "bad_json" else "x" * (16 * 1024 + 1)
                   if case == "oversized" else json.dumps(profile))
        config.write_text(content, encoding="utf-8")
    if case == "config_link":
        # Simulate lstat reporting a reparse link without needing host symlink
        # privileges; direct real-link behavior is also checked by product.
        original_is_symlink = Path.is_symlink
        monkeypatch.setattr(Path, "is_symlink", lambda path: (
            path == config or original_is_symlink(path)))
    if case == "existing_output":
        output.write_text("USER_FILE", encoding="utf-8")
        existing = output
    if case == "output_link":
        # lexists must reject even a dangling link (no output is created).
        original_lexists = os.path.lexists
        monkeypatch.setattr(os.path, "lexists", lambda path: (
            Path(path) == output or original_lexists(path)))
    status = bridge.main(["--config", str(config), "--output", str(output)])
    public = capsys.readouterr()
    if case == "good":
        assert status == 0 and calls == [(
            "remote", "inspect", "--config", str(config),
            "--output", str(output))]
        assert public.out == public.err == ""
    else:
        assert status == 70 and not calls
        assert json.loads(public.out) == {
            "status": "ERROR", "error": "remote operation failed",
            "code": "local_or_unclassified", "phase": "local"}
        assert not public.err
    if existing is not None:
        assert existing.read_text(encoding="utf-8") == "USER_FILE"


def test_synthetic_origin_token_never_flows_to_trusted_redirect(monkeypatch, tmp_path):
    from sakurapool.storage.transport import GuardedTransport

    spec = importlib.util.spec_from_file_location("offline_bridge", BRIDGE)
    bridge = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bridge)
    token_path = tmp_path / "synthetic-token"
    token_path.write_text("SYNTHETIC_ORIGIN_TOKEN\r\n", encoding="ascii")
    token = bridge._load_token(token_path)
    class FakeProductionLedger:
        offline_mode = False
        def __init__(self): self.attempts = 0
        def reserve(self, _req):
            self.attempts += 1
            return str(self.attempts)
        def settle(self, _lease): pass
    class Response:
        status_code = 204
        headers = {}
        def close(self): pass
    ledger = FakeProductionLedger()
    origin, cdn = "https://modelscope.cn", "https://cdn.example.invalid"
    seen = []
    with GuardedTransport(ledger, trusted_hosts=frozenset({"modelscope.cn",
                                                            "cdn.example.invalid"}),
                          token=token, credential_origin=origin) as transport:
        def fake_get(url, *, headers, stream, allow_redirects, timeout):
            seen.append((url, headers.get("Authorization")))
            if url == origin + "/redirect":
                class Redirect:
                    status_code = 302
                    headers = {"Location": cdn + "/asset?signature=SYNTHETIC_SIGNATURE"}
                    def close(self): pass
                return Redirect()
            return Response()
        monkeypatch.setattr(transport.session, "get", fake_get)
        response, lease = transport._response(origin + "/redirect", max_body=1,
                                              metadata=False, inflight=0, headers={})
        assert response.status_code == 204
        ledger.settle(lease)
    assert seen == [(origin + "/redirect", "Bearer " + token),
                    (cdn + "/asset?signature=SYNTHETIC_SIGNATURE", None)]
    assert ledger.attempts == 2
