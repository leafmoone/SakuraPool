"""Offline provider route/shape tests; no ModelScope target requests."""

import json
import traceback
from urllib.parse import parse_qs, urlsplit

import pytest

from sakurapool.storage.modelscope import ModelScopeDataset
from sakurapool.storage.transport import RemoteIOError

REV = "a" * 40


class FakeTransport:
    def __init__(self, pages):
        self.pages = pages
        self.urls = []

    def _host(self, url):
        assert urlsplit(url).hostname == "localhost"
        return "localhost"

    def read_metadata(self, url):
        self.urls.append(url)
        if url.endswith("/revisions"):
            return json.dumps({"Data": {"RevisionMap": {
                "Branches": [{"Revision": "master", "CommitId": REV}],
                "Tags": []}}}).encode()
        query = parse_qs(urlsplit(url).query)
        assert query["Revision"] == [REV]
        return json.dumps({"Data": {"Files": self.pages[int(query["PageNumber"][0])-1],
                                    "Total": sum(len(p) for p in self.pages)}}).encode()


def test_official_dataset_routes_are_guarded_and_pinned():
    fake = FakeTransport([[{"Path": "dir/1.tar", "Size": 2048,
                            "Type": "blob", "Sha256": "0" * 64}]])
    provider = ModelScopeDataset(fake, "http://localhost")
    assert provider.revisions() == [REV]
    files, complete = provider.list_files(REV)
    assert complete and files[0].path == "dir/1.tar"
    url = provider.download_url(REV, files[0].path)
    assert parse_qs(urlsplit(url).query) == {"Revision": [REV], "FilePath": ["dir/1.tar"]}
    assert len(fake.urls) == 2
    assert fake.urls[0].endswith("/api/v1/datasets/leafmoone/game_cg_5M/revisions")
    assert "/repo/tree?" in fake.urls[1]
    with pytest.raises(ValueError, match="authorized"):
        ModelScopeDataset(fake, "http://localhost", "other/repo")
    with pytest.raises(ValueError, match="floating"):
        provider.download_url("master", "dir/1.tar")


@pytest.mark.parametrize("bad", [
    {"Path": "../escape.tar", "Size": 1},
    {"Path": "other.tar", "Size": -1},
    {"Path": "other.tar", "Size": "5"},
    {"Path": "other.tar", "Size": 1, "Type": "symlink"},
    {"Path": "other.tar", "Size": 1, "Sha256": "bogus"},
])
def test_fail_closed_on_invalid_remote_listing(bad):
    fake = FakeTransport([[bad]])
    provider = ModelScopeDataset(fake, "http://localhost")
    with pytest.raises(RemoteIOError):
        provider.list_files(REV)


@pytest.mark.parametrize("bad", [None, "not-a-list", {"items": []}, 7])
def test_malformed_revision_lists_are_redacted(bad):
    class BadTransport(FakeTransport):
        def read_metadata(self, url):
            return json.dumps({"Data": {"RevisionMap": {"Tags": bad,
                                                         "Branches": []}}}).encode()
    with pytest.raises(RemoteIOError) as caught:
        ModelScopeDataset(BadTransport([]), "http://localhost").revisions()
    assert "SECRET" not in "".join(traceback.format_exception(caught.value))


def test_malformed_metadata_and_endpoint_never_expose_signed_url():
    class BadTransport(FakeTransport):
        def read_metadata(self, url):
            return b'{"Data":SECRET_SIGNED_URL?token=SECRET}'
    with pytest.raises(RemoteIOError) as caught:
        ModelScopeDataset(BadTransport([]), "http://localhost").revisions()
    assert "SECRET" not in "".join(traceback.format_exception(caught.value))
    assert caught.value.__context__ is None
    malformed_endpoint = "http://[" + "SECRET_BAD_IPV6"
    with pytest.raises(ValueError) as caught:
        ModelScopeDataset(FakeTransport([]), malformed_endpoint)
    assert "SECRET" not in "".join(traceback.format_exception(caught.value))


def test_short_page_without_declared_total_never_claims_complete():
    class NoTotal(FakeTransport):
        def read_metadata(self, url):
            if url.endswith('/revisions'):
                return super().read_metadata(url)
            return json.dumps({"Data": {"Files": [{"Path": "a.tar", "Size": 1}]}}).encode()
    files, complete = ModelScopeDataset(NoTotal([]), "http://localhost").list_files(REV)
    assert len(files) == 1 and not complete


def test_declared_total_mismatch_rejected():
    class WrongTotal(FakeTransport):
        def read_metadata(self, url):
            return json.dumps({"Data": {"Total": 2, "Files": []}}).encode()
    with pytest.raises(RemoteIOError, match="ended before"):
        ModelScopeDataset(WrongTotal([]), "http://localhost").list_files(REV)


@pytest.mark.parametrize("kinds", [("tree", "tree"), ("tree", "blob")])
def test_tree_paths_cannot_duplicate_or_conflict_with_blob(kinds):
    same = [{"Path": "dir", "Size": 0, "Type": kind} for kind in kinds]
    with pytest.raises(RemoteIOError, match="duplicate"):
        ModelScopeDataset(FakeTransport([same]), "http://localhost").list_files(REV)


def test_cross_page_tree_duplicate_and_changed_total_refused():
    first = [{"Path": f"dir/{i:03d}", "Type": "tree"}
             for i in range(200)]
    again = {"Path": "dir/000", "Type": "tree"}
    with pytest.raises(RemoteIOError, match="duplicate"):
        ModelScopeDataset(FakeTransport([first, [again]]),
                          "http://localhost").list_files(REV)
    class ChangingTotal(FakeTransport):
        def read_metadata(self, url):
            page = int(parse_qs(urlsplit(url).query)["PageNumber"][0])
            return json.dumps({"Data": {"Total": 201 if page == 1 else 202,
                                        "Files": first if page == 1 else [
                                            {"Path": "other", "Size": 1}]}}).encode()
    with pytest.raises(RemoteIOError, match="total"):
        ModelScopeDataset(ChangingTotal([]), "http://localhost").list_files(REV)


def test_inspect_result_cap_stops_before_listing_entire_remote_repository():
    pages = [[{"Path": f"dir/{n:04d}.tar", "Size": 1024} for n in range(
        page * 200, (page + 1) * 200)] for page in range(5)]
    pages.append([{"Path": "dir/1000.tar", "Size": 1024}])
    fake = FakeTransport(pages)
    files, complete = ModelScopeDataset(fake, "http://localhost").list_files(REV)
    assert len(files) == 1000 and not complete
    assert len(fake.urls) == 5


def test_duplicate_path_fails_and_metadata_does_not_claim_completion():
    entry = {"Path": "a.tar", "Size": 1}
    fake = FakeTransport([[entry, entry]])
    with pytest.raises(RemoteIOError, match="duplicate"):
        ModelScopeDataset(fake, "http://localhost").list_files(REV)
