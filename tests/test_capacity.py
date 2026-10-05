import pytest

from sakurapool.capacity import IMPLEMENTATION_BOUNDARIES, CapacityConfig


def test_defaults_and_roundtrip():
    config = CapacityConfig()
    assert config.freeze_count == 100_000
    assert config.sample_heap_count == 10_000
    assert config.record_batch == 512
    assert CapacityConfig.from_dict(config.to_dict()) == config


def test_old_numbers_are_defaults_not_caps():
    config = CapacityConfig(freeze_count=200_000, sample_heap_count=20_000,
                            image_max_bytes=256 << 20, range_chunk_bytes=16 << 20,
                            task_db_bytes=64 << 20, task_journal_bytes=65 << 20)
    assert config.freeze_count == 200_000


@pytest.mark.parametrize("name", CapacityConfig.__dataclass_fields__)
@pytest.mark.parametrize("value", [None, True, 0, -1, 1 << 64])
def test_bad_capacities(name, value):
    with pytest.raises(ValueError):
        CapacityConfig(**{name: value})


def test_consistency_and_implementation_boundaries():
    with pytest.raises(ValueError):
        CapacityConfig(sample_heap_count=100_001)
    with pytest.raises(ValueError):
        CapacityConfig(task_db_bytes=4095)
    with pytest.raises(ValueError):
        CapacityConfig(task_journal_bytes=1)
    with pytest.raises(ValueError):
        CapacityConfig(task_db_bytes=IMPLEMENTATION_BOUNDARIES["sqlite_bytes"] + 1)
    with pytest.raises(ValueError):
        CapacityConfig.from_dict({"freeze_count": 1})
