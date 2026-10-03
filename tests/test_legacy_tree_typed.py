"""Legacy rejection diagnostics preserve strict admission; offline only."""

import json

import pytest

from sakurapool.storage.modelscope import ModelScopeDataset
from sakurapool.storage.transport import RemoteIOError

A = "a" * 40
B = "b" * 40
DIGEST = "c" * 64


def parse(monkeypatch, files, revision=A, page_size=20):
    provider = object.__new__(ModelScopeDataset)
    provider.endpoint = "https://modelscope.cn"
    provider._legacy_verified_id = 7
    monkeypatch.setattr(
        provider,
        "_data",
        lambda *args, **kwargs: {
            "Files": files,
            "Total": len(files) if isinstance(files, list) else 0,
        },
    )
    return provider.legacy_tree_page(7, revision, root="gc5m", page_size=page_size)


def entry(**changes):
    return {
        "Type": "blob",
        "Path": "gc5m/test.tar",
        "Size": 12,
        "Revision": A,
        "Sha256": DIGEST,
    } | changes


@pytest.mark.parametrize(
    "files,code",
    [
        (None, "provider_page_shape"),
        ([entry(), entry(Path="gc5m/other.tar")], "provider_page_shape"),
        ([None], "provider_entry_shape"),
        ([entry(Type="unknown")], "provider_entry_type"),
        ([entry(Path="../private")], "provider_entry_path"),
        ([entry(), entry()], "provider_entry_duplicate"),
        ([entry(Path="outside/test.tar")], "provider_entry_scope"),
        ([entry(Size=True)], "provider_entry_size"),
        ([entry(Revision="private-revision")], "provider_entry_revision_shape"),
        ([entry(Sha256="private-digest")], "provider_entry_digest"),
    ],
)
def test_typed_rejections(monkeypatch, files, code):
    with pytest.raises(RemoteIOError) as caught:
        parse(monkeypatch, files, page_size=1 if code == "provider_page_shape" else 20)
    error = caught.value
    assert error.public_diagnostic() == {"code": code, "phase": "provider_listing_shape"}
    exposed = str(error) + json.dumps(error.public_diagnostic())
    for value in (A, B, DIGEST, "private", "gc5m/test.tar", "outside/test.tar"):
        assert value not in exposed


@pytest.mark.parametrize(
    "revision,candidate", [("master", B), (A, A), (A, B), ("master", "d" * 64)]
)
@pytest.mark.parametrize("sha", [None, DIGEST])
def test_accepted_unchanged(monkeypatch, revision, candidate, sha):
    rows, complete = parse(
        monkeypatch,
        [entry(Revision=candidate, Sha256=sha),
         {"Type": "directory", "Path": "gc5m/sub"},
         {"Type": "tree", "Path": "gc5m/other"}],
        revision=revision,
    )
    assert rows[0].revision_candidate == (candidate if revision == "master" else revision)
    assert rows[0].path == "gc5m/test.tar" and rows[0].size == 12
    assert rows[0].provider_sha256 == sha
    assert len(rows) == 1 and complete


@pytest.mark.parametrize("bad", ["master", "", "garbage", "a" * 39])
def test_pinned_still_requires_entry_revision_syntax(monkeypatch, bad):
    with pytest.raises(RemoteIOError) as caught:
        parse(monkeypatch, [entry(Revision=bad)])
    assert caught.value.public_diagnostic() == {
        "code": "provider_entry_revision_shape",
        "phase": "provider_listing_shape",
    }


def test_guard_order(monkeypatch):
    with pytest.raises(RemoteIOError) as caught:
        parse(monkeypatch, [entry(Size=True, Revision=B, Sha256="bad")])
    assert caught.value.code == "provider_entry_size"
