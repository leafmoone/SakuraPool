"""Synthetic integration-only gaps: no production data or credentials."""

from dataclasses import replace
from pathlib import Path

import pytest
from test_nested_metadata import adapter, metadata
from test_staged_lookup import _assert_indexed_seek

from sakurapool import indexer
from sakurapool.metadata import MISSING, MetadataInvalid, normalize_nested
from sakurapool.registry import DatasetAdapter
from sakurapool.storage.production_resources import DURABLE_SPOOL_LIMITS
from sakurapool.storage.remote_index import StagedArchive
from sakurapool.storage.rust_index import _write_stage
from sakurapool.storage.transport import BoundObject


@pytest.mark.parametrize("category", ["general", "artist", "character", "copyright"])
@pytest.mark.parametrize("value", [MISSING, None, [], [None], ["blue", None, "hair"]])
def test_all_tag_categories_optional_null_and_elements(category, value):
    record = metadata()
    record["tags"] = {} if value is MISSING else {category: value}
    result = normalize_nested(record, adapter(), "42", ".jpg")
    expected = (
        [{"value": "blue", "category": category}, {"value": "hair", "category": category}]
        if value == ["blue", None, "hair"]
        else []
    )
    assert result.tags == expected
    assert result.tags_state == ("known" if expected else "empty")
    assert all(tag["value"] != "null" for tag in result.tags)


@pytest.mark.parametrize("category", ["general", "artist", "character", "copyright"])
@pytest.mark.parametrize("value", [123, {}, "a", [123], [{}], [[]]])
def test_all_categories_invalid_nonnull_types_rejected(category, value):
    record = metadata()
    record["tags"] = {category: value}
    with pytest.raises(MetadataInvalid, match="tags_state=invalid"):
        normalize_nested(record, adapter(), "42", ".jpg")


@pytest.mark.parametrize(
    "caption", [MISSING, None, {}, {"nl2": None}, {"nl2": ""}, {"nl2": "caption"}]
)
def test_caption_missing_null_string_no_fallback(caption):
    record = metadata()
    if caption is MISSING:
        del record["captions"]
    else:
        record["captions"] = caption
    if isinstance(caption, dict):
        record["captions"]["nl3"] = "not primary"
    result = normalize_nested(record, adapter(), "42", ".jpg")
    expected = caption.get("nl2") if isinstance(caption, dict) else None
    assert result.text == expected and result.text != "not primary"


@pytest.mark.parametrize("name", ["width", "height"])
def test_uint32_maximum_dimension_is_known_not_zero(name):
    record = metadata()
    record["image"][name] = 2**32 - 1
    assert getattr(normalize_nested(record, adapter(), "42", ".jpg"), name) == 2**32 - 1


@pytest.mark.parametrize("primary", [False, True])
def test_cleanup_first_close_failure_retains_path_scope_and_retry(tmp_path, monkeypatch, primary):
    path = tmp_path / "owned-spool.sqlite"
    path.write_bytes(b"owned")
    untouched = tmp_path / "user.sqlite"
    untouched.write_bytes(b"untouched")
    calls = []

    class CloseOnceFails:
        def close(self):
            calls.append("close")
            if len(calls) == 1:
                raise OSError("synthetic first handle close failure")

    resource = indexer._SpoolState(path, CloseOnceFails())
    if primary:

        def fail_scan(*args):
            args[-1].add(resource)
            raise RuntimeError("synthetic primary scan failure")

        monkeypatch.setattr(indexer, "_scan_shard_impl", fail_scan)
        with pytest.raises(RuntimeError, match="primary scan failure") as caught:
            indexer._scan_shard(
                Path("unused.tar"), "unused.tar", DatasetAdapter("d", "s"), False, {}, {}
            )
        handle = caught.value.cleanup_handle
        assert any("first handle close failure" in note for note in caught.value.__notes__)
        scope = handle._scope
    else:
        scope = indexer._SpoolScope(limits=DURABLE_SPOOL_LIMITS)
        scope.add(resource)
        handle = scope.handle
        assert len(scope.cleanup()) == 1
    assert path.is_file() and resource in scope.resources
    assert handle.pending_resources[0]["path"] == path
    assert len(calls) == 1 and resource.handle is not None
    assert handle.retry() == []
    assert calls == ["close", "close"] and not path.exists()
    assert resource not in scope.resources and resource.handle is None
    assert untouched.read_bytes() == b"untouched"


def test_cleanup_normal_close_precedes_unlink(tmp_path):
    path = tmp_path / "owned.sqlite"
    path.write_bytes(b"owned")

    class NormalClose:
        def close(self):
            assert path.exists()

    scope = indexer._SpoolScope(limits=DURABLE_SPOOL_LIMITS)
    resource = indexer._SpoolState(path, NormalClose())
    scope.add(resource)
    assert scope.handle.retry() == []
    assert not path.exists() and not scope.resources


def test_production_callable_stage_preserves_both_indexes(tmp_path):
    import hashlib

    payload = b'{"tags":["a"]}'

    def rows(db):
        assert db.execute("PRAGMA journal_mode").fetchone()[0] == "off"
        yield ("42.jpg", "image", 512, 4, "a" * 64, None)
        yield ("42.json", "json", 1536, len(payload), hashlib.sha256(payload).hexdigest(), payload)

    bound = BoundObject("http://127.0.0.1/x.tar", 2048, strong_etag='"synthetic"')
    stage = _write_stage(
        {"whole_sha256": "b" * 64},
        rows,
        bound,
        DatasetAdapter("synthetic", "synthetic"),
        tmp_path / "stage",
        None,
    )
    _assert_indexed_seek(stage)
    with StagedArchive(stage, bounded=True) as archive:
        names = {row[1] for row in archive.db.execute("PRAGMA index_list(members)")}
        assert {"members_extents", "members_json_offset"} <= names
        plan = archive.db.execute(
            "EXPLAIN QUERY PLAN SELECT name FROM members ORDER BY offset_data,name"
        ).fetchall()
        assert any("members_extents" in row[3] for row in plan)
        assert not any("TEMP B-TREE" in row[3] for row in plan)


def test_null_caption_configured_text_path_never_nl3_fallback():
    record = metadata()
    record["captions"] = {"nl2": None, "nl3": "other"}
    assert (
        normalize_nested(record, replace(adapter(), text_path="captions.nl2"), "42", ".jpg").text
        is None
    )
