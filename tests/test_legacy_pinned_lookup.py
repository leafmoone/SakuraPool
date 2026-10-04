"""Pinned legacy exact lookup uses request identity, not optional entry metadata."""

from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
from test_p5b_prerequisites import _blob, _provider

from sakurapool.storage import publication_fetch
from sakurapool.storage.transport import RemoteIOError


@pytest.mark.parametrize("revision", ["a" * 40, "a" * 64])
@pytest.mark.parametrize("candidate", ["", None, "missing", "a" * 40, "b" * 64])
def test_exact_lookup_optional_revision_keeps_pin(monkeypatch, revision, candidate):
    row = _blob()
    row["Revision"] = candidate
    if candidate == "missing":
        del row["Revision"]
    provider, calls = _provider(monkeypatch, [{"Files": [row], "Total": 1}])
    provider.repo_id = "test/repo"
    provider.base = "https://modelscope.cn/api/v1/datasets/test/repo"
    provider.transport = SimpleNamespace(ledger=SimpleNamespace(offline_mode=False))
    monkeypatch.setattr(provider, "legacy_hub_id", lambda: 7)
    monkeypatch.setattr(publication_fetch, "ModelScopeDataset", lambda *args: provider)
    found = publication_fetch.exact_provider_lookup(
        None, provider.endpoint, provider.repo_id, revision, row["Path"], 12, "c" * 64,
    )
    assert (found.revision, found.object_path, found.object_size) == (revision, row["Path"], 12)
    query = parse_qs(urlsplit(provider.download_url(found.revision, found.object_path)).query)
    assert query == {"Revision": [revision], "FilePath": [row["Path"]]}
    assert calls == [1]


@pytest.mark.parametrize("total", [0, None])
def test_exact_lookup_entire_empty_page_is_absent(monkeypatch, total):
    page = {"Files": []}
    if total is not None:
        page["Total"] = total
    provider, calls = _provider(monkeypatch, [page])
    monkeypatch.setattr(provider, "legacy_hub_id", lambda: 7)
    monkeypatch.setattr(publication_fetch, "ModelScopeDataset", lambda *args: provider)
    with pytest.raises(RemoteIOError) as caught:
        publication_fetch.exact_provider_lookup(
            None, provider.endpoint, "test/repo", "a" * 40, "gc5m/target.tar", 12, "c" * 64,
        )
    assert caught.value.public_diagnostic() == {
        "code": "provider_object_absent", "phase": "provider_exact_lookup", "accounting": "UNKNOWN",
    }
    assert calls == [1]


@pytest.mark.parametrize("candidate", ["", None, "missing"])
def test_exact_lookup_optional_revision_does_not_relax_digest(monkeypatch, candidate):
    row = _blob()
    row["Revision"] = candidate
    if candidate == "missing":
        del row["Revision"]
    provider, calls = _provider(monkeypatch, [{"Files": [row], "Total": 1}])
    monkeypatch.setattr(provider, "legacy_hub_id", lambda: 7)
    monkeypatch.setattr(publication_fetch, "ModelScopeDataset", lambda *args: provider)
    with pytest.raises(RemoteIOError) as caught:
        publication_fetch.exact_provider_lookup(
            None, provider.endpoint, "test/repo", "a" * 40, row["Path"], 12, "d" * 64,
        )
    assert caught.value.public_diagnostic() == {
        "code": "provider_shape", "phase": "provider_exact_lookup", "accounting": "UNKNOWN",
    }
    assert calls == [1]
