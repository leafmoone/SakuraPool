import copy

import pytest

from sakurapool.metadata import MISSING, MetadataInvalid, SourceMismatch, get_path, normalize_nested
from sakurapool.registry import DatasetAdapter


def adapter():
    return DatasetAdapter(dataset="danbooru_v3", source="danbooru", storage_id="local",
                          metadata_mode="nested_json_v1", numeric_post_id=True,
                          allowed_provenance=("danbooru",))


def metadata():
    return {"schema_version": 1, "id": 42, "source": {"dataset": "danbooru", "hash": "bad"},
            "image": {"width": 123, "height": 456, "format": "jpeg"},
            "tags": {"general": ["blue"], "artist": ["alice"],
                     "character": [], "copyright": ["series"]},
            "captions": {"nl2": "caption", "nl3": "not primary"},
            "character_core_tags": ["excluded"], "future": {"extra": True}}


def test_missing_and_null_are_distinct():
    assert get_path({"x": {"y": None}}, "x.y") is None
    assert get_path({"x": {}}, "x.y") is MISSING
    assert get_path({"x": None}, "x.y") is MISSING


def test_normalization_categories_and_no_extra_tags():
    result = normalize_nested(metadata(), adapter(), "42", ".jpg")
    assert (result.width, result.height, result.image_format) == (123, 456, "jpg")
    assert result.text == "caption"
    assert result.tags == [{"value": "blue", "category": "general"},
                           {"value": "alice", "category": "artist"},
                           {"value": "series", "category": "copyright"}]


@pytest.mark.parametrize("captions,text", [({}, None), ({"nl2": ""}, "")])
def test_text_no_fallback(captions, text):
    value = metadata()
    value["captions"] = captions | {"nl3": "must not fallback"}
    assert normalize_nested(value, adapter(), "42", ".jpeg").text == text


@pytest.mark.parametrize("path,bad", [
    ("schema_version", True), ("schema_version", 2), ("id", True), ("id", "42"),
    ("id", 43), ("source", "danbooru"), ("source.dataset", None),
    ("image.width", True), ("image.width", -1), ("image.height", 2**32),
    ("image.format", "png"), ("tags", None), ("tags.artist", [1]),
    ("tags.copyright", None), ("captions", None), ("captions.nl2", None),
])
def test_invalid_declared_core(path, bad):
    value = copy.deepcopy(metadata())
    parts = path.split(".")
    target = value
    for part in parts[:-1]:
        target = target[part]
    target[parts[-1]] = bad
    with pytest.raises(MetadataInvalid):
        normalize_nested(value, adapter(), "42", ".jpg")


def test_provenance_error_records_actual_value():
    value = metadata()
    value["source"]["dataset"] = "unallowed"
    with pytest.raises(SourceMismatch, match="unallowed"):
        normalize_nested(value, adapter(), "42", ".jpg")


def test_empty_missing_tags():
    value = metadata()
    value["tags"] = {key: [] for key in value["tags"]}
    assert normalize_nested(value, adapter(), "42", ".jpg").tags_state == "empty"
    del value["tags"]
    assert normalize_nested(value, adapter(), "42", ".jpg").tags_state == "missing"


@pytest.mark.parametrize("fmt", ["gif", "avif", "webp", "png"])
def test_formats(fmt):
    value = metadata()
    value["image"]["format"] = fmt
    assert normalize_nested(value, adapter(), "42", "." + fmt).image_format == fmt


def test_flat_canonical_contract_unchanged():
    flat = DatasetAdapter(dataset="flat", source="flat", storage_id="local")
    assert "metadata_mode" not in flat.to_dict()
    assert DatasetAdapter(**flat.to_dict()).to_dict() == flat.to_dict()
    assert DatasetAdapter(**adapter().to_dict()).to_dict() == adapter().to_dict()
