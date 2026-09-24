import pytest

from sakurapool.models import ModelRegistry, ModelSpec
from sakurapool.registry import Registry
from sakurapool.schema import DEFAULT_SCHEMA


def test_schema_and_model_registries_reject_duplicates():
    registry = Registry()
    registry.register_schema("default", DEFAULT_SCHEMA)
    with pytest.raises(ValueError):
        registry.register_schema("default", DEFAULT_SCHEMA)
    models = ModelRegistry()
    model = ModelSpec("demo", "1", DEFAULT_SCHEMA, DEFAULT_SCHEMA)
    models.register(model)
    with pytest.raises(ValueError):
        models.register(model)
    models.get("demo", "1").validate_table(
        __import__("sakurapool").rows_to_table([], DEFAULT_SCHEMA)
    )
