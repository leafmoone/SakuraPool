"""Offline closure regression: real ledger/transport, no provider or secrets."""
import io
import json
import tempfile
from urllib.parse import parse_qs, urlsplit

import pytest
from test_task_runner import setup as task_setup_fixture

from sakurapool.runtime import RuntimeQuerySpec
from sakurapool.storage.budget import DEFAULT_WORK_ROOT, BudgetLedger
from sakurapool.storage.modelscope import ModelScopeDataset
from sakurapool.storage.transport import GuardedTransport, RemoteIOError
from sakurapool.tasks.plan import Selection
from sakurapool.tasks.runner import create_task, run_task
from sakurapool.tasks.store import TaskDB, TaskError

setup = task_setup_fixture


class Response:
    def __init__(self, status, payload, ambiguous=False):
        self.status_code, self.headers, self.raw = status, {}, self
        self.stream, self.ambiguous = io.BytesIO(payload), ambiguous

    def read(self, size, **kwargs):
        if self.ambiguous:
            raise OSError("SECRET_BODY")
        return self.stream.read(size)

    def close(self):
        pass


def control(ledger, monkeypatch, ambiguous=False):
    transport = GuardedTransport(ledger, trusted_hosts=frozenset({"127.0.0.1"}),
                                 allow_loopback_http=True)
    calls = []
    payload = json.dumps({"Data": {"Namespace": "synthetic", "Name": "test",
                                   "Id": 42, "Type": 4}}).encode().ljust(1584, b" ")

    def get(url, **kwargs):
        calls.append(url)
        if len(calls) % 2:
            return Response(200, payload)
        return Response(200 if ambiguous else 400, b"", ambiguous)

    monkeypatch.setattr(transport.session, "get", get)
    return transport, calls


@pytest.mark.parametrize("ambiguous", [False, True])
def test_metadata_operation_accounting_and_request_shape(monkeypatch, ambiguous):
    with tempfile.TemporaryDirectory(dir=DEFAULT_WORK_ROOT, prefix="offline-closure-") as raw:
        ledger = BudgetLedger(raw, _offline_test=True)
        transport, calls = control(ledger, monkeypatch, ambiguous)
        provider = ModelScopeDataset(transport, "http://127.0.0.1", "synthetic/test")
        try:
            hub = provider.legacy_hub_id()
            with pytest.raises(RemoteIOError) as caught:
                provider.find_legacy_file(hub, "b" * 40, root="gc5m", path="gc5m/one.tar")
            diagnostic = caught.value.public_diagnostic()
            assert diagnostic["accounting"] == ("UNKNOWN" if ambiguous else "CONFIRMED")
            assert diagnostic["phase"] == "provider_tree_request"
            assert diagnostic["code"] == ("network_ambiguous" if ambiguous else "http_status")
            if not ambiguous:
                assert diagnostic["http_status"] == 400
            assert "SECRET" not in str(diagnostic) and "synthetic" not in str(diagnostic)
            shape = urlsplit(calls[1])
            assert shape.path == "/api/v1/datasets/42/repo/tree"
            assert parse_qs(shape.query) == {"Revision": ["b" * 40], "Root": ["gc5m"],
                                             "Recursive": ["True"], "PageNumber": ["1"],
                                             "PageSize": ["200"]}
            assert ledger.status()["attempts"] == 2
            with ledger._locked():
                _, (_, used, pending, _) = ledger._read_pair()
            assert used["metadata"] == 1584
            assert len(pending) == int(ambiguous)
            if not ambiguous:
                assert ledger.status()["metadata"] == 1584
        finally:
            transport.close()


@pytest.mark.parametrize("ambiguous", [False, True])
def test_task_known_reject_only_explicit_resume(setup, monkeypatch, ambiguous):
    pub, directory, ledger, transport_type, _ = setup
    from sakurapool.storage import publication_fetch

    def lookup(transport, endpoint, repo, revision, path, size, digest):
        provider = ModelScopeDataset(transport, "http://127.0.0.1", "synthetic/test")
        hub = provider.legacy_hub_id()
        return provider.find_legacy_file(hub, revision, root="gc5m", path=path)

    monkeypatch.setattr(publication_fetch, "exact_provider_lookup", lookup)
    with create_task(pub, directory, ledger, RuntimeQuerySpec(), Selection("first", 1)):
        pass
    reader, calls = control(ledger, monkeypatch, ambiguous)
    try:
        with pytest.raises(TaskError) as caught:
            run_task(directory, transport_type(), control=reader)
        assert caught.value.public_diagnostic()["accounting"] == (
            "UNKNOWN" if ambiguous else "CONFIRMED")
        with TaskDB(directory, readonly=True) as task:
            item = task.db.execute("SELECT state,accounting,code FROM items").fetchone()
            assert tuple(item) == (("BLOCKED", "UNKNOWN", "network_ambiguous") if ambiguous
                                   else ("READY", "CONFIRMED", "http_status"))
            assert task.meta("state") == "BLOCKED"
            assert task.inspect()["unknown_accounting_count"] == int(ambiguous)
        count = len(calls)
        with pytest.raises(TaskError) as caught:
            run_task(directory, transport_type(), control=reader)
        assert caught.value.code == "EXPLICIT_RESUME_REQUIRED"
        assert len(calls) == count
        with pytest.raises(TaskError) as caught:
            run_task(directory, transport_type(), control=reader, resume=True)
        assert caught.value.code == ("BLOCKED_ACCOUNTING" if ambiguous else "http_status")
        assert len(calls) == count + (0 if ambiguous else 2)
    finally:
        reader.close()


@pytest.mark.parametrize("mode", ["json", "shape", "absent", "incomplete", "settle"])
def test_metadata_later_failures_use_explicit_settlement(monkeypatch, mode):
    with tempfile.TemporaryDirectory(dir=DEFAULT_WORK_ROOT, prefix="offline-closure-") as raw:
        ledger = BudgetLedger(raw, _offline_test=True)
        transport, _ = control(ledger, monkeypatch)
        provider = ModelScopeDataset(transport, "http://127.0.0.1", "synthetic/test")
        hub = provider.legacy_hub_id()
        entry = {"Path": "gc5m/other.tar", "Type": "blob", "Size": 5, "Revision": "b" * 40}
        payload = {"json": b"INVALID SECRET", "shape": b'{"Data":{"Files":null}}',
                   "absent": b'{"Data":{"Files":[],"Total":0}}',
                   "incomplete": json.dumps({"Data": {"Files": [entry], "Total": 2}}).encode(),
                   "settle": b""}[mode]
        monkeypatch.setattr(transport.session, "get", lambda *a, **k: Response(
            400 if mode == "settle" else 200, payload))
        if mode == "settle":
            monkeypatch.setattr(ledger, "settle", lambda *a, **k: (_ for _ in ()).throw(
                OSError("SECRET_SETTLE")))
        try:
            with pytest.raises(RemoteIOError) as caught:
                provider.find_legacy_file(hub, "b" * 40, root="gc5m", path="gc5m/one.tar",
                                          max_pages=1)
            diagnostic = caught.value.public_diagnostic()
            assert diagnostic["accounting"] == ("UNKNOWN" if mode == "settle" else "CONFIRMED")
            assert "SECRET" not in str(diagnostic)
            if mode == "settle":
                assert diagnostic["code"] == "http_status" and diagnostic["http_status"] == 400
                assert diagnostic["secondary"] == ["METADATA_FINALIZATION_FAILED"]
        finally:
            transport.close()


@pytest.mark.parametrize("credentialed", [False, True])
def test_credential_headers_only_exact_origin_without_socket(monkeypatch, credentialed):
    class Ledger:
        offline_mode = False

        def reserve(self, request):
            return "synthetic-lease"

        def settle(self, lease):
            pass

    token = "SYNTHETIC_TOKEN" if credentialed else None
    origin = "https://modelscope.cn"
    transport = GuardedTransport(Ledger(), trusted_hosts=frozenset({"modelscope.cn",
                                 "www.modelscope.cn", "cdn.example.invalid"}), token=token,
                                 credential_origin=origin if token else None,
                                 same_origin_cookie="m_session_id=" + token if token else None)
    captured = []

    def get(url, **kwargs):
        captured.append(dict(kwargs["headers"]))
        return Response(400, b"")

    monkeypatch.setattr(transport.session, "get", get)
    try:
        for url in (origin + "/repo", "https://cdn.example.invalid/asset",
                    "https://www.modelscope.cn/repo", origin + ":443/repo"):
            with pytest.raises(RemoteIOError):
                transport.read_metadata(url)
        for index, headers in enumerate(captured):
            if credentialed and index == 0:
                assert headers["Authorization"] == "Bearer " + token
                assert headers["Cookie"] == "m_session_id=" + token
            else:
                assert "Authorization" not in headers and "Cookie" not in headers
        with pytest.raises(RemoteIOError):
            transport.read_metadata("http://modelscope.cn/repo")
        assert len(captured) == 4
    finally:
        transport.close()


@pytest.mark.parametrize("initial_unknown", [False, True])
def test_retry_policy_error_retains_operation_evidence(monkeypatch, initial_unknown):
    with tempfile.TemporaryDirectory(dir=DEFAULT_WORK_ROOT, prefix="offline-closure-") as raw:
        ledger = BudgetLedger(raw, _offline_test=True)
        reader, calls = control(ledger, monkeypatch)

        def get(url, **kwargs):
            calls.append(url)
            if initial_unknown and len(calls) == 1:
                raise OSError("SECRET_CONNECTION")
            response = Response(429, b"")
            response.headers["Retry-After"] = "INVALID_SECRET"
            return response

        monkeypatch.setattr(reader.session, "get", get)
        monkeypatch.setattr("sakurapool.storage.transport.time.sleep", lambda *a: None)
        try:
            with pytest.raises(RemoteIOError) as caught:
                reader.read_metadata("http://127.0.0.1/repo")
            assert caught.value.public_diagnostic()["accounting"] == (
                "UNKNOWN" if initial_unknown else "CONFIRMED")
            assert "SECRET" not in str(caught.value.public_diagnostic())
            with ledger._locked():
                _, (_, _, pending, _) = ledger._read_pair()
            assert len(pending) == int(initial_unknown)
        finally:
            reader.close()


@pytest.mark.parametrize("mode", ["retry_confirmed", "plain_repository", "plain_tree",
                                  "prior_unknown_plain"])
def test_unknown_connection_retry_success_cannot_reach_proof_or_delivery(setup, monkeypatch, mode):
    from sakurapool.storage import publication_fetch

    pub, directory, ledger, transport_type, output_calls = setup
    reader, calls = control(ledger, monkeypatch)
    repository = json.dumps({"Data": {"Namespace": "synthetic", "Name": "test",
                                     "Id": 42, "Type": 4}}).encode()
    valid_tree = json.dumps({"Data": {"Files": [{"Path": "gc5m/one.tar", "Type": "blob",
                            "Size": 5, "Revision": "b" * 40}], "Total": 1}}).encode()

    def get(url, **kwargs):
        calls.append(url)
        if mode in {"retry_confirmed", "prior_unknown_plain"} and len(calls) == 1:
            raise OSError("SECRET_CONNECTION")
        return Response(200, valid_tree if "/repo/tree?" in url else repository)

    def lookup(transport, endpoint, repo, revision, path, size, digest):
        provider = ModelScopeDataset(transport, "http://127.0.0.1", "synthetic/test")
        hub = provider.legacy_hub_id()
        return provider.find_legacy_file(hub, "b" * 40, root="gc5m", path="gc5m/one.tar")

    monkeypatch.setattr(reader.session, "get", get)
    if mode != "retry_confirmed":
        read = reader.read_metadata

        def metadata(url):
            payload = read(url)
            if mode != "plain_tree" or "/repo/tree?" in url:
                return bytes(payload)
            return payload

        monkeypatch.setattr(reader, "read_metadata", metadata)
    monkeypatch.setattr("sakurapool.storage.transport.time.sleep", lambda *a: None)
    monkeypatch.setattr(publication_fetch, "exact_provider_lookup", lookup)
    with create_task(pub, directory, ledger, RuntimeQuerySpec(), Selection("first", 1)):
        pass
    try:
        with pytest.raises(TaskError) as caught:
            run_task(directory, transport_type(), control=reader)
        assert caught.value.public_diagnostic()["accounting"] == "UNKNOWN"
        assert caught.value.phase == ("provider_tree_request" if mode == "plain_tree"
                                      else "provider_repository_request")
        assert len(calls) == (1 if mode == "plain_repository" else 2) and output_calls == []
        with TaskDB(directory, readonly=True) as task:
            assert task.inspect()["unknown_accounting_count"] == 1
            assert task.inspect()["delivered_confirmed"] == 0
            assert task.db.execute("SELECT state FROM items").fetchone()[0] == "BLOCKED"
            assert task.db.execute("SELECT phase FROM attempts").fetchone()[0] != "SETTLED"
        with pytest.raises(TaskError, match="BLOCKED_ACCOUNTING"):
            run_task(directory, transport_type(), control=reader, resume=True)
        with ledger._locked():
            _, (_, _, pending, _) = ledger._read_pair()
        assert len(pending) == int(mode in {"retry_confirmed", "prior_unknown_plain"})
    finally:
        reader.close()


def test_plain_bytes_are_not_settlement_evidence(monkeypatch):
    from types import SimpleNamespace

    provider = ModelScopeDataset(SimpleNamespace(_host=lambda u: None,
                                read_metadata=lambda u: b"INVALID"),
                                "https://modelscope.cn", "synthetic/test")
    with pytest.raises(RemoteIOError) as caught:
        provider.legacy_hub_id()
    assert caught.value.public_diagnostic()["accounting"] == "UNKNOWN"
