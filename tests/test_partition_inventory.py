import pytest
from synthetic_p2 import ObjectSpec, SampleSpec, build_p2_directory

from sakurapool.runtime.errors import CorruptInputError
from sakurapool.runtime.inventory import combine_inventories, load_p2_inventory


def make(root, path, *, category="general"):
    build_p2_directory(root, dataset="danbooru_v3", source="danbooru",
                       tag_category=category,
                       objects=[ObjectSpec(path, [SampleSpec("42", "42", [("blue", category)])])])
    return load_p2_inventory(root)


def test_disjoint_same_dataset_duplicate_post_id_accepted(tmp_path):
    a = make(tmp_path / "part-000000", "danbooru/a.tar")
    b = make(tmp_path / "part-000001", "danbooru/b.tar")
    combined = combine_inventories([a, b])
    assert len(combined.objects) == 2
    assert load_p2_inventory([a.root, b.root]).source_fingerprint == combined.source_fingerprint


def test_adapter_mismatch_rejected(tmp_path):
    a = make(tmp_path / "a", "a.tar")
    b = make(tmp_path / "b", "b.tar", category="artist")
    with pytest.raises(CorruptInputError, match="contract mismatch"):
        combine_inventories([a, b])


def test_same_path_changed_content_rejected(tmp_path, monkeypatch):
    import synthetic_p2

    a = make(tmp_path / "a", "a.tar")
    monkeypatch.setattr(synthetic_p2, "_input_digest", lambda rel, seed: "f" * 64)
    b = make(tmp_path / "b", "a.tar")
    assert a.objects[0].object_id != b.objects[0].object_id
    with pytest.raises(CorruptInputError, match="duplicate object_path"):
        combine_inventories([a, b])


def test_duplicate_physical_object_rejected(tmp_path):
    a = make(tmp_path / "a", "a.tar")
    b = make(tmp_path / "b", "a.tar")
    with pytest.raises(CorruptInputError, match="duplicate object"):
        combine_inventories([a, b])
