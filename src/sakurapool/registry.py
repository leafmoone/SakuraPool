"""Schema registry and explicit dataset adapters; source never comes from paths."""

from dataclasses import asdict, dataclass, field
from pathlib import PurePosixPath
from typing import Any

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


@dataclass(frozen=True)
class DatasetAdapter:
    dataset: str
    source: str
    image_extensions: tuple[str, ...] = (".jpg", ".jpeg", ".png", ".webp")
    tags_field: str = "tags"
    text_field: str = "text"
    max_json_bytes: int = 16 * 1024 * 1024
    metadata_required: bool = True
    image_prefix: str = ""
    metadata_prefix: str = ""
    tag_namespace: str = "tags"
    tag_category: str = "general"
    numeric_post_id: bool = False
    ignored_names: tuple[str, ...] = ("README.md", "manifest.json")

    def __post_init__(self) -> None:
        for value in (self.dataset, self.source, self.tags_field, self.text_field,
                      self.tag_namespace, self.tag_category):
            if not isinstance(value, str) or not value.strip() or value != value.strip():
                raise ValueError("adapter names must be nonempty canonical strings")
        if type(self.max_json_bytes) is not int or self.max_json_bytes < 1:
            raise ValueError("max_json_bytes must be a positive integer")
        if type(self.metadata_required) is not bool or type(self.numeric_post_id) is not bool:
            raise ValueError("adapter flags must be boolean")
        for prefix in (self.image_prefix, self.metadata_prefix):
            if not isinstance(prefix, str) or (
                prefix and (not prefix.endswith("/") or prefix.startswith("/")
                            or "\\" in prefix or ":" in prefix
                            or any(p in ("", ".", "..") for p in prefix[:-1].split("/")))
            ):
                raise ValueError("pairing prefixes must be relative directories ending in /")
        if not isinstance(self.image_extensions, (tuple, list)) or not self.image_extensions:
            raise ValueError("image_extensions must be a nonempty array")
        if any(not isinstance(ext, str) or not ext.startswith(".") or ext != ext.lower()
               or ext == ".json" or "/" in ext or "\\" in ext
               for ext in self.image_extensions):
            raise ValueError("image_extensions must be lowercase suffixes, excluding .json")
        if not isinstance(self.ignored_names, (list, tuple)) or any(
            not isinstance(name, str) or not name for name in self.ignored_names
        ):
            raise ValueError("ignored_names must be an array of names")
        object.__setattr__(self, "image_extensions", tuple(self.image_extensions))
        object.__setattr__(self, "ignored_names", tuple(self.ignored_names))

    def pair_key(self, member: str) -> str:
        """Preserve full logical stem after removing explicit role-specific prefixes."""
        prefix = self.metadata_prefix if member.lower().endswith(".json") else self.image_prefix
        if prefix and member.startswith(prefix):
            member = member[len(prefix):]
        return str(PurePosixPath(member).with_suffix(""))

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["image_extensions"] = list(self.image_extensions)
        result["ignored_names"] = list(self.ignored_names)
        return result


@dataclass
class AdapterRegistry:
    adapters: dict[str, DatasetAdapter] = field(default_factory=dict)

    def register(self, adapter: DatasetAdapter) -> None:
        if adapter.dataset in self.adapters:
            raise ValueError(f"duplicate dataset: {adapter.dataset}")
        self.adapters[adapter.dataset] = adapter

    def get(self, dataset: str) -> DatasetAdapter:
        try:
            return self.adapters[dataset]
        except KeyError as exc:
            raise ValueError(f"unknown dataset: {dataset}") from exc

    @classmethod
    def local(cls) -> "AdapterRegistry":
        registry = cls()
        registry.register(DatasetAdapter("local", "local"))
        return registry

    @classmethod
    def from_dict(cls, config: Any) -> "AdapterRegistry":
        if not isinstance(config, dict) or set(config) != {"datasets"}:
            raise ValueError("config must contain only a datasets object")
        if not isinstance(config["datasets"], dict) or not config["datasets"]:
            raise ValueError("datasets must be a nonempty object")
        registry = cls()
        allowed = set(DatasetAdapter.__dataclass_fields__) - {"dataset"}
        for dataset, options in config["datasets"].items():
            if not isinstance(options, dict) or "source" not in options or set(options) - allowed:
                raise ValueError(f"invalid dataset options: {dataset}")
            registry.register(DatasetAdapter(dataset=dataset, **options))
        return registry
