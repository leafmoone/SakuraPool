"""Legacy rejection diagnostics preserve strict admission; offline only."""

import json
from urllib.parse import parse_qs, urlsplit

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
    assert error.public_diagnostic() == {"code": code, "phase": "provider_listing_shape",
                                         "accounting": "UNKNOWN"}
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


@pytest.mark.parametrize("revision", [A, "d" * 64])
@pytest.mark.parametrize("candidate", ["", None, "missing", A, B, "e" * 64])
def test_pinned_optional_entry_revision_binds_download_to_request(monkeypatch, revision, candidate):
    row = entry(Revision=candidate)
    if candidate == "missing":
        del row["Revision"]
    rows, complete = parse(monkeypatch, [row], revision=revision)
    assert complete and len(rows) == 1
    listed = rows[0]
    assert listed.revision_candidate == revision
    assert (listed.path, listed.size, listed.provider_sha256, listed.lfs) == (
        "gc5m/test.tar", 12, DIGEST, False,
    )
    provider = object.__new__(ModelScopeDataset)
    provider.base = "https://modelscope.cn/api/v1/datasets/test/repo"
    query = parse_qs(urlsplit(provider.download_url(listed.revision_candidate, listed.path)).query)
    assert query == {"Revision": [revision], "FilePath": [listed.path]}


@pytest.mark.parametrize("revision", ["master", A, "d" * 64])
@pytest.mark.parametrize("bad", ["master", "garbage", "a" * 39, "a" * 41,
                                 "a" * 63, "a" * 65, "A" * 40, " ",
                                 "a" * 40 + "\n", False, 0, 123, [], {}])
def test_nonempty_invalid_entry_revision_rejected(monkeypatch, revision, bad):
    with pytest.raises(RemoteIOError) as caught:
        parse(monkeypatch, [entry(Revision=bad)], revision=revision)
    assert caught.value.public_diagnostic() == {
        "code": "provider_entry_revision_shape",
        "phase": "provider_listing_shape", "accounting": "UNKNOWN",
    }


@pytest.mark.parametrize("candidate", ["", None, "missing"])
def test_master_requires_entry_candidate(monkeypatch, candidate):
    row = entry(Revision=candidate)
    if candidate == "missing":
        del row["Revision"]
    with pytest.raises(RemoteIOError) as caught:
        parse(monkeypatch, [row], revision="master")
    assert caught.value.code == "provider_entry_revision_shape"


@pytest.mark.parametrize("revision", ["", None, False, 123, "main", "a" * 7,
                                     "a" * 39, "a" * 41, "a" * 63, "a" * 65,
                                     "A" * 40, "a" * 40 + "\n"])
def test_invalid_request_revision_rejected_before_metadata(monkeypatch, revision):
    provider = object.__new__(ModelScopeDataset)
    provider._legacy_verified_id = 7

    def unexpected_read(*args, **kwargs):
        pytest.fail("invalid request revision must not reach metadata HTTP")

    monkeypatch.setattr(provider, "_data", unexpected_read)
    with pytest.raises(ValueError, match="legacy tree scope invalid"):
        provider.legacy_tree_page(7, revision, root="gc5m")


def test_guard_order(monkeypatch):
    with pytest.raises(RemoteIOError) as caught:
        parse(monkeypatch, [entry(Size=True, Revision=B, Sha256="bad")])
    assert caught.value.code == "provider_entry_size"
