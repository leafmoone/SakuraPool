import copy
from dataclasses import replace

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


@pytest.mark.parametrize("captions,text", [({}, None), ({"nl2": None}, None), ({"nl2": ""}, "")])
def test_text_no_fallback(captions, text):
    value = metadata()
    value["captions"] = captions | {"nl3": "must not fallback"}
    assert normalize_nested(value, adapter(), "42", ".jpeg").text == text


@pytest.mark.parametrize("path,bad", [
    ("schema_version", True), ("schema_version", 2), ("id", True), ("id", "42"),
    ("id", 43), ("source", "danbooru"), ("source.dataset", None),
    ("image.width", True), ("image.width", -1), ("image.height", 2**32),
    ("image.format", "png"), ("tags", []), ("tags.artist", [1]),
    ("tags.copyright", False), ("captions", []), ("captions.nl2", []),
    ("captions.nl2", {}), ("captions.nl2", 123), ("captions.nl2", False),
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


@pytest.mark.parametrize("path", ["schema_version", "id", "source", "source.dataset",
                                  "image", "image.format"])
@pytest.mark.parametrize("null", [True, False], ids=["null", "absent"])
def test_required_null_and_absent_fail_as_missing(path, null):
    value = metadata()
    parts = path.split(".")
    target = value
    for part in parts[:-1]:
        target = target[part]
    if null:
        target[parts[-1]] = None
    else:
        del target[parts[-1]]
    with pytest.raises(MetadataInvalid, match=rf"^{path} is required but missing$"):
        normalize_nested(value, adapter(), "42", ".jpg")


@pytest.mark.parametrize("path", ["captions", "captions.nl2", "tags", "tags.artist",
                                  "tags.copyright", "future", "image.optional"])
def test_optional_null_equals_absent_without_losing_valid_fields(path):
    value = metadata()
    parts = path.split(".")
    target = value
    for part in parts[:-1]:
        target = target[part]
    target[parts[-1]] = None
    original = copy.deepcopy(value)
    result = normalize_nested(value, adapter(), "42", ".jpg")
    assert value == original
    del target[parts[-1]]
    assert result == normalize_nested(value, adapter(), "42", ".jpg")
    assert (result.width, result.height, result.image_format) == (123, 456, "jpg")
    if path != "tags":
        assert {"value": "blue", "category": "general"} in result.tags
    if path in ("captions", "captions.nl2"):
        assert result.text is None
    elif path == "tags":
        assert result.tags_state == "missing"
        assert result.tags is None
        assert result.text == "caption"


@pytest.mark.parametrize("tags", [{}, {"general": None}, {"general": [None]},
                                  {"general": [], "artist": None}])
def test_present_tags_without_valid_values_are_empty(tags):
    value = metadata()
    value["tags"] = tags
    result = normalize_nested(value, adapter(), "42", ".jpg")
    assert result.tags_state == "empty"
    assert result.tags == []


def test_null_tag_elements_do_not_stringify_or_drop_siblings():
    value = metadata()
    value["tags"]["general"] = [None, "blue", None, ""]
    result = normalize_nested(value, adapter(), "42", ".jpg")
    assert result.tags == [{"value": "blue", "category": "general"},
                           {"value": "", "category": "general"},
                           {"value": "alice", "category": "artist"},
                           {"value": "series", "category": "copyright"}]


@pytest.mark.parametrize("null_parent", [True, False])
def test_configured_optional_paths_use_same_null_rule(null_parent):
    value = metadata()
    value["optional"] = None if null_parent else {"text": None, "artist": None}
    configured = replace(adapter(), text_path="optional.text",
                         tag_fields=adapter().tag_fields | {"artist": "optional.artist"})
    result = normalize_nested(value, configured, "42", ".jpg")
    assert result.text is None
    assert result.tags == [{"value": "blue", "category": "general"},
                           {"value": "series", "category": "copyright"}]


@pytest.mark.parametrize("size", [None, {}, {"width": None}, {"width": 7}])
def test_configured_dimension_path_is_optional(size):
    value = metadata()
    value["size"] = size
    result = normalize_nested(value, replace(adapter(), width_path="size.width"), "42", ".jpg")
    assert result.width == (7 if size == {"width": 7} else None)
    assert result.height == 456


@pytest.mark.parametrize("width", [MISSING, None, 0, 123])
@pytest.mark.parametrize("height", [MISSING, None, 0, 456])
def test_dimensions_independently_unknown_or_known(width, height):
    value = metadata()
    for name, dimension in (("width", width), ("height", height)):
        if dimension is MISSING:
            del value["image"][name]
        else:
            value["image"][name] = dimension
    original = copy.deepcopy(value)
    result = normalize_nested(value, adapter(), "42", ".jpg")
    assert (result.width, result.height) == (
        None if width is MISSING else width, None if height is MISSING else height)
    assert value == original
    assert result.text == "caption"
    assert result.tags_state == "known"


@pytest.mark.parametrize("dimension", ["width", "height"])
@pytest.mark.parametrize("bad", [-1, 2**32, True, False, "123", "", 1.5, [], {}])
def test_nonnull_invalid_dimensions_fail(dimension, bad):
    value = metadata()
    value["image"][dimension] = bad
    with pytest.raises(MetadataInvalid,
                       match=rf"image\.{dimension} must be a uint32-compatible int"):
        normalize_nested(value, adapter(), "42", ".jpg")


def test_zero_dimensions_are_not_missing():
    value = metadata()
    value["image"].update(width=0, height=0)
    result = normalize_nested(value, adapter(), "42", ".jpg")
    assert (result.width, result.height) == (0, 0)


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
