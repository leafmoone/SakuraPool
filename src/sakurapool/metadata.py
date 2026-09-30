"""Declared nested metadata normalization; no source inference or text fallback."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .registry import DatasetAdapter

MISSING = object()


def get_path(metadata: dict[str, Any], path: str) -> Any:
    """Return MISSING for an absent path, preserving an explicit JSON null."""
    value: Any = metadata
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return MISSING
        value = value[part]
    return value


class MetadataInvalid(ValueError):
    """A declared metadata field violates its contract."""


class SourceMismatch(ValueError):
    """Provenance is outside the explicitly configured allowlist."""


@dataclass(frozen=True)
class NormalizedMetadata:
    width: int
    height: int
    image_format: str
    text: str | None
    tags_state: str
    tags: list[dict[str, str]] | None


def normalize_nested(metadata: dict[str, Any], adapter: DatasetAdapter,
                     post_id: str, extension: str) -> NormalizedMetadata:
    """Validate one nested_json_v1 record and preserve four tag categories."""
    if type(get_path(metadata, "schema_version")) is not int or get_path(
            metadata, "schema_version") != 1:
        raise MetadataInvalid("schema_version must be integer 1")
    for parent in ("source", "image", "captions"):
        if not isinstance(get_path(metadata, parent), dict):
            raise MetadataInvalid(f"{parent} must be an object")
    identifier = get_path(metadata, adapter.id_path)
    if type(identifier) is not int or str(identifier) != post_id:
        raise MetadataInvalid("JSON id must be an int matching filename post_id")
    provenance = get_path(metadata, adapter.source_provenance_path)
    if not isinstance(provenance, str):
        raise MetadataInvalid("source provenance must be a string")
    if provenance not in adapter.allowed_provenance:
        raise SourceMismatch(f"unallowed provenance: {provenance!r}")
    dimensions = []
    for path in (adapter.width_path, adapter.height_path):
        value = get_path(metadata, path)
        if type(value) is not int or not 0 <= value < 2**32:
            raise MetadataInvalid(f"{path} must be a uint32-compatible int")
        dimensions.append(value)
    declared = get_path(metadata, adapter.format_path)
    if not isinstance(declared, str):
        raise MetadataInvalid("image format must be a string")
    actual = extension.lower().lstrip(".")
    declared = declared.lower().lstrip(".")
    def normalize_format(value: str) -> str:
        return "jpg" if value in ("jpg", "jpeg") else value
    if declared not in ("jpg", "jpeg", "png", "webp", "avif", "gif") or (
            normalize_format(declared) != normalize_format(actual)):
        raise MetadataInvalid("image.format conflicts with member extension")
    text = get_path(metadata, adapter.text_path)
    if text is MISSING:
        text = None
    elif not isinstance(text, str):
        raise MetadataInvalid(f"{adapter.text_path} must be a string when present")
    tags = get_path(metadata, "tags")
    state = "missing" if tags is MISSING else "known"
    tag_rows: list[dict[str, str]] | None = None
    if tags is not MISSING:
        if not isinstance(tags, dict):
            raise MetadataInvalid("tags object invalid; tags_state=invalid")
        tag_rows = []
        for category, path in adapter.tag_fields.items():
            values = get_path(metadata, path)
            if not isinstance(values, list) or any(not isinstance(t, str) for t in values):
                raise MetadataInvalid(f"{path} invalid; tags_state=invalid")
            tag_rows.extend({"value": tag, "category": category} for tag in values)
        state = "known" if tag_rows else "empty"
    return NormalizedMetadata(dimensions[0], dimensions[1], normalize_format(actual),
                              text, state, tag_rows)
