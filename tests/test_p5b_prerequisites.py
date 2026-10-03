"""Small regressions for P5-B prerequisite correctness boundaries."""

import os
import sqlite3

import pytest
from synthetic_p2 import ObjectSpec, SampleSpec
from test_publication import inputs as publication_inputs
from test_runtime_remediation import _compile_synthetic


@pytest.mark.parametrize("name", ["hash#root", "percent%23root", "中文 space", "query?root"])
def test_runtime_readonly_uri_special_path(tmp_path, name):
    if "?" in name and os.name == "nt":
        pytest.skip("Windows forbids literal question mark in file paths")
    parent = tmp_path / name
    parent.mkdir()
    summary, root = _compile_synthetic(parent, "uri", [
        ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("tag", None)])])
    ])
    before = set(parent.rglob("*"))
    from sakurapool.runtime import RuntimeSnapshot

    for path in (root, root / "snapshots" / summary.snapshot_id):
        with RuntimeSnapshot.open(path, full_verify=True) as runtime:
            assert runtime.rid_count == 1
            for db in (runtime._catalog, runtime._bitmaps):
                with pytest.raises(sqlite3.OperationalError, match="readonly"):
                    db.execute("CREATE TABLE forbidden(value)")
    assert set(parent.rglob("*")) == before


@pytest.fixture
def inputs(tmp_path):
    return publication_inputs.__wrapped__(tmp_path)


@pytest.mark.parametrize("name", ["hash#publication", "percent%23publication", "中文 space"])
def test_publication_runtime_special_path(inputs, name):
    from sakurapool.storage.publication import build_publication, load_publication

    rt, roots, mapping, out, _ = inputs
    build_publication(rt, roots, mapping, out)
    moved = out.with_name(name)
    out.rename(moved)
    before = set(moved.parent.rglob("*"))
    with load_publication(moved, full_verify=True) as pub:
        assert pub.runtime.rid_count > 0
        for db in (pub.catalog, pub.runtime._catalog, pub.runtime._bitmaps):
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                db.execute("CREATE TABLE forbidden(value)")
    assert set(moved.parent.rglob("*")) == before


def test_missing_runtime_database_never_creates_file(tmp_path):
    from sakurapool.runtime import RuntimeSnapshot

    summary, root = _compile_synthetic(tmp_path, "missing#db", [
        ObjectSpec("a.tar", [SampleSpec("1.jpg", "1", [("tag", None)])])
    ])
    catalog = root / "snapshots" / summary.snapshot_id / "catalog.sqlite"
    catalog.unlink()
    with pytest.raises(Exception):
        RuntimeSnapshot.open(root)
    assert not catalog.exists()


def _provider(monkeypatch, pages):
    from sakurapool.storage.modelscope import ModelScopeDataset
    provider = object.__new__(ModelScopeDataset)
    provider.endpoint = "https://modelscope.cn"
    provider._legacy_verified_id = 7
    calls = []

    def data(url, **kwargs):
        from urllib.parse import parse_qs, urlsplit
        page = int(parse_qs(urlsplit(url).query)["PageNumber"][0])
        calls.append(page)
        return pages[page - 1]

    monkeypatch.setattr(provider, "_data", data)
    return provider, calls


def _blob(path="gc5m/target.tar"):
    return {"Type": "blob", "Path": path, "Size": 12,
            "Revision": "b" * 40, "Sha256": "c" * 64}


def test_exact_lookup_continues_after_directory_only_page(monkeypatch):
    from types import SimpleNamespace

    from sakurapool.storage import publication_fetch
    provider, calls = _provider(monkeypatch, [
        {"Files": [{"Type": "directory", "Path": "gc5m/sub"}], "Total": 201},
        {"Files": [_blob()], "Total": 201},
    ])
    monkeypatch.setattr(provider, "legacy_hub_id", lambda: 7)
    monkeypatch.setattr(publication_fetch, "ModelScopeDataset", lambda *args: provider)
    monkeypatch.setattr(publication_fetch.ProviderObject, "from_tree",
                        lambda source, row: SimpleNamespace(path=row.path))
    found = publication_fetch.exact_provider_lookup(
        None, "https://modelscope.cn", "test/repo", "a" * 40,
        "gc5m/target.tar", 12, "c" * 64,
    )
    assert found.path == "gc5m/target.tar" and calls == [1, 2]


def test_production_cli_verifies_exact_object_on_later_page(monkeypatch):
    from contextlib import nullcontext
    from types import SimpleNamespace

    from sakurapool.storage import production_cli
    from sakurapool.storage.production import ProviderObject

    provider, calls = _provider(monkeypatch, [
        {"Files": [{"Type": "directory", "Path": "gc5m/sub"}], "Total": 2},
        {"Files": [_blob()], "Total": 2},
    ])
    provider.repo_id = "test/repo"
    provider.transport = SimpleNamespace(ledger=SimpleNamespace(offline_mode=False))
    monkeypatch.setattr(provider, "legacy_hub_id", lambda: 7)
    monkeypatch.setattr(production_cli, "ModelScopeDataset", lambda *args: provider)
    monkeypatch.setattr(production_cli, "GuardedTransport", lambda *a, **kw: nullcontext(None))
    obj = ProviderObject("test/repo", "modelscope_dataset_legacy", "https://modelscope.cn",
                         "a" * 40, "gc5m/target.tar", 12)
    transport = SimpleNamespace(ledger=None, _token=None, _cookie=None)
    assert production_cli.verify_tree_object({}, obj, transport) == obj
    assert calls == [1, 2]


@pytest.mark.parametrize("pages,count", [
    ([{"Files": [], "Total": 0}], 0),
    ([{"Files": [_blob()], "Total": 1}], 1),
    ([{"Files": [_blob("gc5m/first.tar")], "Total": 2},
      {"Files": [_blob()], "Total": 2}], 2),
])
def test_pagination_proves_only_requested_scope_completion(monkeypatch, pages, count):
    provider, calls = _provider(monkeypatch, pages)
    walked = list(provider.iter_legacy_pages(7, "a" * 40, root="gc5m", page_size=1))
    assert sum(page.raw_count for page in walked) == count
    assert calls == list(range(1, len(pages) + 1))
    assert walked[0] == (walked[0].files, walked[0].complete)


def test_directory_only_page_is_not_eof(monkeypatch):
    provider, calls = _provider(monkeypatch, [
        {"Files": [{"Type": "directory", "Path": "gc5m/sub"}], "Total": 2},
        {"Files": [_blob()], "Total": 2},
    ])
    first = provider.legacy_tree_page(7, "a" * 40, root="gc5m", page_size=1)
    assert first.raw_count == 1 and first.files == []
    assert first.continuation and not first.complete
    found = provider.find_legacy_file(7, "a" * 40, root="gc5m",
                                      path="gc5m/target.tar", page_size=1)
    assert found.path == "gc5m/target.tar"
    assert found.revision_candidate == "a" * 40
    assert calls == [1, 1, 2]


@pytest.mark.parametrize("pages", [
    [{"Files": [], "Total": 1}],
    [{"Files": [_blob()], "Total": 2, "TotalCount": 3}],
    [{"Files": [_blob("gc5m/first.tar")], "Total": 3},
     {"Files": [_blob()], "Total": 2}],
    [{"Files": [_blob("gc5m/first.tar")], "Total": 3},
     {"Files": [_blob("gc5m/first.tar")], "Total": 3}],
])
def test_pagination_contradictions_fail_closed(monkeypatch, pages):
    from sakurapool.storage.transport import RemoteIOError
    provider, _ = _provider(monkeypatch, pages)
    with pytest.raises(RemoteIOError) as caught:
        provider.find_legacy_file(7, "a" * 40, root="gc5m",
                                 path="gc5m/target.tar", page_size=1)
    repeat = len(pages) == 2 and pages[0] == pages[1]
    assert caught.value.public_diagnostic() == {
        "code": "provider_page_repeat" if repeat else "provider_total_conflict",
        "phase": "provider_listing_shape",
    }


@pytest.mark.parametrize("fault", ["unknown", "replace", "symlink", "unchanged"])
def test_cleanup_preserves_unknown_or_replaced_entry(tmp_path, fault):
    from sakurapool.storage.publication_fetch import _cleanup_owned_stage, _file_identity

    stage = tmp_path / "stage"
    stage.mkdir()
    info = stage.lstat()
    identity = (info.st_dev, info.st_ino)
    image = stage / "image.png"
    with image.open("xb") as stream:
        created = {image.name: _file_identity(stream)}
        stream.write(b"owned")
    if fault == "unknown":
        (stage / "unknown").write_bytes(b"foreign")
    elif fault == "replace":
        image.rename(tmp_path / "original")
        image.write_bytes(b"foreign")
    elif fault == "symlink":
        image.rename(tmp_path / "original")
        try:
            image.symlink_to(tmp_path / "original")
        except OSError:
            pytest.skip("symlink privilege unavailable")
    safe = _cleanup_owned_stage(stage, identity, created)
    assert safe == (fault == "unchanged")
    if fault != "unchanged":
        assert stage.is_dir() and image.exists()
        assert image.read_bytes() == (b"foreign" if fault == "replace" else b"owned")
    else:
        assert not stage.exists()


def test_page_bound_is_incomplete_not_absent(monkeypatch):
    from sakurapool.storage.transport import RemoteIOError
    provider, _ = _provider(monkeypatch, [
        {"Files": [{"Type": "directory", "Path": "gc5m/sub"}], "Total": 2},
    ])
    with pytest.raises(RemoteIOError, match="incomplete") as caught:
        provider.find_legacy_file(7, "a" * 40, root="gc5m",
                                 path="gc5m/target.tar", page_size=1, max_pages=1)
    assert caught.value.public_diagnostic() == {
        "code": "provider_listing_incomplete", "phase": "provider_exact_lookup",
    }


def test_absent_is_distinct_from_incomplete(monkeypatch):
    from sakurapool.storage.transport import RemoteIOError

    provider, _ = _provider(monkeypatch, [{"Files": [], "Total": 0}])
    with pytest.raises(RemoteIOError) as caught:
        provider.find_legacy_file(7, "a" * 40, root="gc5m", path="gc5m/target.tar")
    assert caught.value.public_diagnostic() == {
        "code": "provider_object_absent", "phase": "provider_exact_lookup",
    }


@pytest.mark.parametrize("entry", [{"Type": "directory"}, {"Type": "tree", "Path": None}])
def test_unidentified_directory_fails_closed(monkeypatch, entry):
    from sakurapool.storage.transport import RemoteIOError

    provider, _ = _provider(monkeypatch, [{"Files": [entry], "Total": 1}])
    with pytest.raises(RemoteIOError) as caught:
        provider.legacy_tree_page(7, "a" * 40, root="gc5m")
    assert caught.value.public_diagnostic() == {
        "code": "provider_entry_path", "phase": "provider_listing_shape",
    }


def test_repeated_directory_page_fails_closed(monkeypatch):
    from sakurapool.storage.transport import RemoteIOError

    page = {"Files": [{"Type": "directory", "Path": "gc5m/sub"}], "Total": 3}
    provider, _ = _provider(monkeypatch, [page, page])
    with pytest.raises(RemoteIOError) as caught:
        list(provider.iter_legacy_pages(7, "a" * 40, root="gc5m", page_size=1))
    assert caught.value.public_diagnostic() == {
        "code": "provider_page_repeat", "phase": "provider_listing_shape",
    }
