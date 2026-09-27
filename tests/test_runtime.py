import json

import pytest
from test_indexer import make_tar

from sakurapool import indexer
from sakurapool.runtime.errors import CorruptInputError
from sakurapool.runtime.identity import assign_rids, check_rid_capacity
from sakurapool.runtime.inventory import load_p2_inventory


def test_committed_p2_inventory_and_fingerprint(tmp_path):
    source = tmp_path / "input"
    source.mkdir()
    make_tar(source / "a.tar", {"1.jpg": b"x", "1.json": b"{}"})
    output = tmp_path / "index"
    indexer.scan(source, output)
    inventory = load_p2_inventory(output)
    assert len(inventory.objects) == 1
    assert {fragment.name for fragment in inventory.fragments} == {
        "objects", "samples", "annotations", "errors"
    }
    relocated = tmp_path / "relocated"
    relocated.mkdir()
    for path in output.iterdir():
        path.rename(relocated / path.name)
    assert load_p2_inventory(relocated).source_fingerprint == inventory.source_fingerprint


def test_commit_allow_set_is_derived_not_globbered(tmp_path):
    source = tmp_path / "input"
    source.mkdir()
    make_tar(source / "a.tar", {"1.jpg": b"x", "1.json": b"{}"})
    output = tmp_path / "index"
    indexer.scan(source, output)
    load_p2_inventory(output)
    (output / ("deadbeef" + "0" * 56 + ".COMMIT")).write_text(json.dumps(
        {"schema": 4, "builder": "sakurapool-p2-v4", "dataset_id": "local",
         "object_id": "x", "input": {}, "files": {}, "contract_sha256": "0" * 64,
         "created_at": "now"}))
    with pytest.raises(CorruptInputError, match="COMMIT marker set"):
        load_p2_inventory(output)


def test_rids_are_canonical_and_bounded():
    rows = [
        {"dataset_id": "d", "object_id": "b", "sample_path": "2", "record_id": "f"},
        {"dataset_id": "d", "object_id": "a", "sample_path": "2", "record_id": "e"},
        {"dataset_id": "d", "object_id": "a", "sample_path": "1", "record_id": "d"},
    ]
    assigned = assign_rids(rows)
    assert [row["record_id"] for row in assigned] == ["d", "e", "f"]
    assert [row["rid"] for row in assigned] == [0, 1, 2]


@pytest.mark.parametrize("count", [2**32 - 1, 2**32])
def test_rid_capacity_accepts_boundary_counts_without_allocation(count):
    check_rid_capacity(count)


@pytest.mark.parametrize("count", [2**32 + 1, -(2**32)])
def test_rid_capacity_rejects_overflow_and_negative(count):
    with pytest.raises(ValueError, match="uint32"):
        check_rid_capacity(count)


def test_assign_rids_checks_capacity_before_sorting():
    class SmallRows:
        def __len__(self):
            return 2**32 + 1

        def __iter__(self):
            raise AssertionError("sort must never run past the capacity check")

    with pytest.raises(ValueError, match="uint32"):
        assign_rids(SmallRows())
