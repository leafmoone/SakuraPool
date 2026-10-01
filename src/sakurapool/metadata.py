"""Nested JSON metadata validation and normalization."""

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


def _normalized_path(metadata: dict[str, Any], path: str) -> Any:
    """Treat null as missing only at the normalization boundary."""
    value = get_path(metadata, path)
    return MISSING if value is None else value


def _required_path(metadata: dict[str, Any], path: str) -> Any:
    value = _normalized_path(metadata, path)
    if value is MISSING:
        raise MetadataInvalid(f"{path} is required but missing")
    return value


class MetadataInvalid(ValueError):
    """A declared metadata field violates its contract."""


class SourceMismatch(ValueError):
    """Provenance is outside the explicitly configured allowlist."""


@dataclass(frozen=True)
class NormalizedMetadata:
    width: int | None
    height: int | None
    image_format: str
    text: str | None
    tags_state: str
    tags: list[dict[str, str]] | None


def normalize_nested(metadata: dict[str, Any], adapter: DatasetAdapter,
                     post_id: str, extension: str) -> NormalizedMetadata:
    """Keep unknown optional dimensions; fail closed on invalid declared fields."""
    version = _required_path(metadata, "schema_version")
    if type(version) is not int or version != 1:
        raise MetadataInvalid("schema_version must be integer 1")
    for parent in ("source", "image"):
        if not isinstance(_required_path(metadata, parent), dict):
            raise MetadataInvalid(f"{parent} must be an object")
    captions = _normalized_path(metadata, "captions")
    if captions is not MISSING and not isinstance(captions, dict):
        raise MetadataInvalid("captions must be an object")
    identifier = _required_path(metadata, adapter.id_path)
    if type(identifier) is not int or str(identifier) != post_id:
        raise MetadataInvalid("JSON id must be an int matching filename post_id")
    provenance = _required_path(metadata, adapter.source_provenance_path)
    if not isinstance(provenance, str):
        raise MetadataInvalid("source provenance must be a string")
    if provenance not in adapter.allowed_provenance:
        raise SourceMismatch(f"unallowed provenance: {provenance!r}")
    dimensions: list[int | None] = []
    for path in (adapter.width_path, adapter.height_path):
        value = _normalized_path(metadata, path)
        if value is MISSING:
            dimensions.append(None)
            continue
        if type(value) is not int or not 0 <= value < 2**32:
            raise MetadataInvalid(f"{path} must be a uint32-compatible int")
        dimensions.append(value)
    declared = _required_path(metadata, adapter.format_path)
    if not isinstance(declared, str):
        raise MetadataInvalid("image format must be a string")
    actual = extension.lower().lstrip(".")
    declared = declared.lower().lstrip(".")
    def normalize_format(value: str) -> str:
        return "jpg" if value in ("jpg", "jpeg") else value
    if declared not in ("jpg", "jpeg", "png", "webp", "avif", "gif") or (
            normalize_format(declared) != normalize_format(actual)):
        raise MetadataInvalid("image.format conflicts with member extension")
    text = _normalized_path(metadata, adapter.text_path)
    if text is MISSING:
        text = None
    elif not isinstance(text, str):
        raise MetadataInvalid(f"{adapter.text_path} must be a string when present")
    tags = _normalized_path(metadata, "tags")
    state = "missing" if tags is MISSING else "known"
    tag_rows: list[dict[str, str]] | None = None
    if tags is not MISSING:
        if not isinstance(tags, dict):
            raise MetadataInvalid("tags object invalid; tags_state=invalid")
        tag_rows = []
        for category, path in adapter.tag_fields.items():
            values = _normalized_path(metadata, path)
            if values is MISSING:
                continue
            if not isinstance(values, list) or any(
                    t is not None and not isinstance(t, str) for t in values):
                raise MetadataInvalid(f"{path} invalid; tags_state=invalid")
            tag_rows.extend({"value": tag, "category": category}
                            for tag in values if tag is not None)
        state = "known" if tag_rows else "empty"
    return NormalizedMetadata(dimensions[0], dimensions[1], normalize_format(actual),
                              text, state, tag_rows)
