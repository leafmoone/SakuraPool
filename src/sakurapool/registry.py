"""Schema registry with deterministic names and duplicate protection."""

from dataclasses import dataclass, field

import pyarrow as pa


@dataclass
class Registry:
    schemas: dict[str, pa.Schema] = field(default_factory=dict)

    def register_schema(self, name: str, schema: pa.Schema) -> None:
        if not name or name in self.schemas:
            raise ValueError(f"schema name is empty or already registered: {name!r}")
        self.schemas[name] = schema

    def get_schema(self, name: str) -> pa.Schema:
        try:
            return self.schemas[name]
        except KeyError as exc:
            raise KeyError(f"unknown schema: {name}") from exc

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self.schemas))
