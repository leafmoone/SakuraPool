"""C0 regressions: synthetic configuration and owned loopback worker bytes only."""

import hashlib
import json
from collections import OrderedDict
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest
from test_r2_production import twohop as twohop_fixture
from test_task_runner import setup as task_setup_fixture

from sakurapool.runtime import RuntimeQuerySpec
from sakurapool.storage import publication_fetch
from sakurapool.storage.budget import BudgetLedger
from sakurapool.storage.modelscope import ModelScopeDataset
from sakurapool.storage.transport import BoundObject, GuardedTransport, RemoteIOError
from sakurapool.tasks import profile
from sakurapool.tasks.runner import create_task
from sakurapool.tasks.store import TaskDB, TaskError

twohop = twohop_fixture
setup = task_setup_fixture
ORIGINS = ("https://modelscope.cn", "https://www.modelscope.cn")


def config(tmp_path, **changes):
    worker = tmp_path / "worker.exe"
    worker.touch()
    return {"format": profile.FORMAT, "origin": ORIGINS[0],
            "repositories": ["synthetic/test"], "worker": str(worker), **changes}


def load(tmp_path, value):
    path = tmp_path / "profile.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    return profile.read_profile(path)


@pytest.mark.parametrize("origin", ORIGINS)
def test_publication_control_uses_exact_configured_hostname(tmp_path, monkeypatch, origin):
    # Real fetch entry and real control constructor, stopped before provider IO.
    ledger = object.__new__(BudgetLedger)
    ledger._offline_mode = True
    digest = b"x" * 32
    row = (origin, "synthetic/test", "modelscope_dataset_legacy", "b" * 40,
           "one.tar", (1024).to_bytes(8, "big"), digest, digest, 1)
    runtime = SimpleNamespace(
        resolve_record=lambda _: SimpleNamespace(rid=0),
        location=lambda _: {"object_idx": 0}, snapshot_id="synthetic",
        object_ref=lambda _: {"object_path": "one.tar", "object_size": 1024,
                              "object_version": digest.hex()},
    )
    pub = SimpleNamespace(_closed=False, full_verified=True, runtime=runtime,
                          catalog=SimpleNamespace(execute=lambda *a: SimpleNamespace(
                              fetchone=lambda: row)), content_digest="synthetic",
                          _verified=OrderedDict())

    class Transport:
        pass

    actual = Transport()
    actual.ledger, actual._token, actual._cookie = ledger, "synthetic-token", None
    seen = []

    def control(_ledger, **kwargs):
        result = GuardedTransport(SimpleNamespace(offline_mode=False), **kwargs)
        seen.append(result)
        return result

    class StopBeforeIO(Exception):
        pass

    def lookup(client, endpoint, *args):
        assert client.trusted_hosts == frozenset({urlsplit(origin).hostname})
        assert client.credential_origin == endpoint == origin
        assert client._host(origin + "/api") == urlsplit(origin).hostname
        with pytest.raises(RemoteIOError):
            client._host(ORIGINS[1 - ORIGINS.index(origin)] + "/api")
        raise StopBeforeIO

    monkeypatch.setattr(publication_fetch, "GuardedTransport", control)
    monkeypatch.setattr(publication_fetch, "exact_provider_lookup", lookup)
    monkeypatch.setattr(publication_fetch, "_real_output_root", lambda path, ledger: path)
    with pytest.raises(StopBeforeIO):
        publication_fetch.fetch_publication_sample(pub, "1" * 32, actual, tmp_path)
    assert len(seen) == 1


@pytest.mark.parametrize("origin", ORIGINS)
def test_profile_exact_origin_and_credentials_never_follow_host_cache(tmp_path, origin):
    value = load(tmp_path, config(tmp_path, origin=origin))
    assert value["origin"] == origin
    ledger = SimpleNamespace(offline_mode=False, reserve=lambda _: "synthetic")
    with GuardedTransport(ledger, trusted_hosts=frozenset({
        "modelscope.cn", "www.modelscope.cn", "cdn.example.invalid"}),
        credential_origin=origin, token="synthetic-token",
        same_origin_cookie="m_session_id=synthetic-token") as control:
        captured = []
        control.session.get = lambda url, **kw: captured.append(kw) or object()
        for target in (origin, ORIGINS[1 - ORIGINS.index(origin)],
                       "https://cdn.example.invalid", origin + ":443"):
            control._once(target + "/api", max_body=1, metadata=True,
                          inflight=1, headers={})
            headers = captured[-1]["headers"]
            assert ("Authorization" in headers) == (target == origin)
            assert ("Cookie" in headers) == (target == origin)
            assert captured[-1]["allow_redirects"] is False
        assert control.session.trust_env is False


@pytest.mark.parametrize("origin", ["https://cdn.example.invalid", "https://modelscope.cn:443",
    "https://www.modelscope.cn:444", "http://modelscope.cn", "https://user@modelscope.cn",
    "https://modelscope.cn/#fragment"])
def test_invalid_profile_origin_before_connect(tmp_path, origin):
    with pytest.raises(TaskError, match="PROFILE_INVALID"):
        load(tmp_path, config(tmp_path, origin=origin))


@pytest.mark.parametrize("kind", ["env", "file"])
@pytest.mark.parametrize("case", ["missing", "empty", "blank", "type", "readerror",
                                  "newline", "nonascii", "oversize"])
def test_explicit_credential_failure_is_safe_before_transport(tmp_path, monkeypatch, kind, case):
    monkeypatch.delenv("MODELSCOPE_API_TOKEN", raising=False)
    token_file = tmp_path / "token"
    ref = {kind: "MODELSCOPE_API_TOKEN" if kind == "env" else str(token_file)}
    if case == "type":
        ref[kind] = 123
    elif case != "missing":
        value = {"empty": "", "blank": "   ", "readerror": "synthetic-token",
                 "newline": "secret\nembedded", "nonascii": "secret\u00e9",
                 "oversize": "x" * 4097}[case]
        if kind == "env":
            monkeypatch.setenv("MODELSCOPE_API_TOKEN", value)
        else:
            token_file.write_text(value, encoding="utf-8")
    if case == "readerror":
        def fail(*args, **kwargs):
            raise OSError("SECRET_READ_ERROR")
        if kind == "file":
            monkeypatch.setattr(Path, "read_text", fail)
        else:
            monkeypatch.setattr(profile.os.environ, "get", fail)
    calls = []
    monkeypatch.setattr(profile, "RustProductionTransport", lambda *a, **kw: calls.append(kw))
    with pytest.raises(TaskError) as caught:
        profile.connect_profile(config(tmp_path, credential_ref=ref), object())
    assert caught.value.code in {"PROFILE_CREDENTIAL_INVALID", "PROFILE_CREDENTIAL_REF_INVALID"}
    assert "SECRET" not in str(caught.value.public_diagnostic())
    assert caught.value.__context__ is None
    assert calls == []


@pytest.mark.parametrize("kind", [None, "env", "file"])
def test_explicit_valid_or_anonymous_configuration(tmp_path, monkeypatch, kind):
    monkeypatch.setenv("MODELSCOPE_API_TOKEN", "synthetic-token")
    value = config(tmp_path)
    if kind == "env":
        value["credential_ref"] = {"env": "MODELSCOPE_API_TOKEN"}
    elif kind == "file":
        token_file = tmp_path / "token"
        token_file.write_text("synthetic-token\n", encoding="utf-8")
        value["credential_ref"] = {"file": str(token_file)}
    monkeypatch.setattr(profile, "RustProductionTransport", lambda *a, **kw: kw)
    result = profile.connect_profile(load(tmp_path, value), object())
    assert result["token"] == ("synthetic-token" if kind else None)
    assert result["same_origin_cookie"] == ("m_session_id=synthetic-token" if kind else None)


@pytest.mark.parametrize("kind", ["env", "file"])
def test_create_inspect_does_not_load_missing_credentials(setup, tmp_path, monkeypatch, kind):
    pub, directory, ledger, _, calls = setup
    monkeypatch.delenv("MODELSCOPE_API_TOKEN", raising=False)
    value = config(tmp_path, credential_ref={kind: "MODELSCOPE_API_TOKEN" if kind == "env"
                                           else str(tmp_path / "absent-secret")})
    assert load(tmp_path, value)["credential_ref"] == value["credential_ref"]

    def forbidden(*a, **kw):
        pytest.fail("create/inspect loaded credentials")

    monkeypatch.setattr(profile, "connect_profile", forbidden)
    with create_task(pub, directory, ledger, RuntimeQuerySpec()):
        pass
    with TaskDB(directory, readonly=True) as task:
        assert task.meta("header")
    assert calls == []


def bound_range(transport, candidate):
    obj = transport.verify_conditions(candidate)
    return BoundObject(ModelScopeDataset(transport, obj.origin, obj.repo_id).download_url(
        obj.revision, obj.object_path), obj.object_size, obj.revision, obj.validator,
        repository=obj.repo_id)


@pytest.mark.parametrize("consumer_error", [False, True])
def test_range_single_materialization_and_owner_lifetime(twohop, monkeypatch, consumer_error):
    state, ledger, transport, candidate = twohop
    bound = bound_range(transport, candidate)
    reads = []
    worker_results = []
    call = transport._call

    def observe_call(*args, **kwargs):
        result = call(*args, **kwargs)
        worker_results.append(result)
        return result

    monkeypatch.setattr(transport, "_call", observe_call)
    read_bytes = Path.read_bytes

    def observe(path):
        result = read_bytes(path)
        if path.name == "body":
            reads.append((path, result))
        return result

    monkeypatch.setattr(Path, "read_bytes", observe)
    baseline = ledger.status()["inflight"]

    class ConsumerError(Exception):
        pass

    def consume():
        with transport.read_range_owned(bound, 512, 15) as payload:
            assert type(payload) is bytes
            assert payload == state["raw"][512:527]
            assert hashlib.sha256(payload).hexdigest() == worker_results[-1]["sha256"]
            assert ledger.status()["inflight"] > baseline
            roots = list(ledger.root.glob("rust-transfer-*"))
            assert len(roots) == 1 and (roots[0] / "body").is_file()
            if consumer_error:
                raise ConsumerError

    if consumer_error:
        with pytest.raises(ConsumerError):
            consume()
    else:
        consume()
    assert ledger.status()["inflight"] == baseline
    assert not list(ledger.root.glob("rust-transfer-*"))
    assert len(reads) == 1, "verified body must be materialized once, not re-read for consumer"


@pytest.mark.parametrize("corruption", ["body", "worker_sha", "short"])
def test_range_independent_actual_bytes_and_worker_sha(twohop, monkeypatch, corruption):
    _, ledger, transport, candidate = twohop
    bound = bound_range(transport, candidate)
    call = transport._call

    def altered(obj, root, **kwargs):
        result = call(obj, root, **kwargs)
        if corruption == "worker_sha":
            result = dict(result, sha256="0" * 64)
        else:
            (root / "body").write_bytes(b"!" * (14 if corruption == "short" else 15))
        return result

    monkeypatch.setattr(transport, "_call", altered)
    with pytest.raises(RemoteIOError, match="artifact verification"):
        with transport.read_range_owned(bound, 512, 15):
            pytest.fail("unverified payload delivered")
    assert ledger.status()["inflight"] == 0
    assert not list(ledger.root.glob("rust-transfer-*"))
