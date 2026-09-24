"""Minimal model contracts and registry; execution engines are out of P1 scope."""

from dataclasses import dataclass, field

import pyarrow as pa


@dataclass(frozen=True)
class ModelSpec:
    name: str
    version: str
    input_schema: pa.Schema
    output_schema: pa.Schema

    def validate_table(self, table: pa.Table) -> None:
        missing = sorted(set(self.input_schema.names) - set(table.schema.names))
        if missing:
            raise ValueError(f"model {self.name} missing input columns: {missing}")
        for schema_field in self.input_schema:
            actual = table.schema.field(schema_field.name).type
            if actual != schema_field.type:
                raise ValueError(
                    f"model {self.name} column {schema_field.name!r} has type "
                    f"{actual}, expected {schema_field.type}"
                )


@dataclass
class ModelRegistry:
    models: dict[tuple[str, str], ModelSpec] = field(default_factory=dict)

    def register(self, model: ModelSpec) -> None:
        key = (model.name, model.version)
        if key in self.models:
            raise ValueError(f"model already registered: {model.name}@{model.version}")
        self.models[key] = model

    def get(self, name: str, version: str) -> ModelSpec:
        try:
            return self.models[(name, version)]
        except KeyError as exc:
            raise KeyError(f"unknown model: {name}@{version}") from exc
