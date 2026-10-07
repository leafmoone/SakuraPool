"""Frozen, platform-independent flat basenames; never paths or format expressions."""

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

DEFAULT_TEMPLATE = "{tag}_{index}"
FLAT_POLICY = "task-flat-no-overwrite-v1"
_FIELDS = re.compile(r"(\{tag\}|\{index\})")
_FORBIDDEN = frozenset('<>:"/\\|?*{}')
_DEVICES = {"con", "prn", "aux", "nul", "conin$", "conout$"} | {
    prefix + digit for prefix in ("com", "lpt") for digit in "123456789¹²³"
}


def safe_basename(value):
    if (
        type(value) is not str
        or not value
        or value != unicodedata.normalize("NFC", value)
        or value.endswith((" ", "."))
        or ".." in value
        or any(c in _FORBIDDEN or unicodedata.category(c).startswith("C") for c in value)
        or value.split(".")[0].casefold() in _DEVICES
        or len(value.encode("utf-8")) > 240
        or len(value.encode("utf-16-le")) // 2 > 180
    ):
        raise ValueError("FILENAME_INVALID")
    return value


def safe_output_path(root, name):
    safe_basename(name)
    if len(str(Path(root).absolute() / name).encode("utf-16-le")) // 2 > 240:
        raise ValueError("FILENAME_PATH_LIMIT")
    return Path(root) / name


def stem_key(stem):
    return unicodedata.normalize("NFC", safe_basename(stem).casefold())


def _tag_label(query):
    labels = set()

    def visit(branch, depth=0):
        if type(branch) is not dict or depth > 32:
            raise ValueError("FILENAME_QUERY_INVALID")
        for key in ("all_tags", "any_tags"):
            for tag in branch.get(key, ()):
                if type(tag) is str:
                    label = tag
                elif type(tag) is list and len(tag) == 2 and all(type(t) is str for t in tag):
                    label = "_".join(tag)
                else:
                    raise ValueError("FILENAME_QUERY_INVALID")
                label = unicodedata.normalize("NFC", label)
                label = "".join(
                    "_" if c in _FORBIDDEN or unicodedata.category(c).startswith("C") else c
                    for c in label
                ).strip(" .")
                label = label.replace("..", "__") or "image"
                labels.add(label)
        for child in branch.get("any_of", ()):
            visit(child, depth + 1)

    visit(query)
    label = "_".join(sorted(labels)) if labels else "image"
    if label.split(".")[0].casefold() in _DEVICES:
        label = "image_" + label
    return safe_basename(label)


@dataclass(frozen=True)
class FilenameConfig:
    template: str
    tag: str
    prefix: str | None = None

    def __post_init__(self):
        if type(self.template) is not str or len(self.template.encode("utf-8")) > 240:
            raise ValueError("FILENAME_TEMPLATE_INVALID")
        parts = _FIELDS.split(self.template)
        if self.template.count("{index}") != 1 or any(
            "{" in part or "}" in part for part in parts if part not in ("{tag}", "{index}")
        ):
            raise ValueError("FILENAME_TEMPLATE_INVALID")
        # A template describes a stem, not a caller-chosen suffix.
        if any("." in part for part in parts if part not in ("{tag}", "{index}")):
            raise ValueError("FILENAME_TEMPLATE_INVALID")
        safe_basename(self.tag)
        if self.prefix is not None:
            safe_basename(self.prefix)
            if self.prefix != self.tag:
                raise ValueError("FILENAME_CONFIG_INVALID")
        self.stem(0)

    @classmethod
    def resolve(cls, query=None, *, template=DEFAULT_TEMPLATE, prefix=None):
        if prefix is not None:
            prefix = safe_basename(prefix)
        return cls(template, prefix if prefix is not None else _tag_label(query or {}), prefix)

    @classmethod
    def from_dict(cls, value):
        keys = {"version", "template", "tag", "prefix", "index_base"}
        if type(value) is not dict or set(value) != keys:
            raise ValueError("FILENAME_CONFIG_INVALID")
        if type(value["version"]) is not int or value["version"] != 1:
            raise ValueError("FILENAME_CONFIG_INVALID")
        if type(value["index_base"]) is not int or value["index_base"] != 1:
            raise ValueError("FILENAME_CONFIG_INVALID")
        return cls(value["template"], value["tag"], value["prefix"])

    def to_dict(self):
        return {"version": 1, "template": self.template, "tag": self.tag,
                "prefix": self.prefix, "index_base": 1}

    def stem(self, seq):
        if type(seq) is not int or not 0 <= seq < (1 << 63) - 1:
            raise ValueError("FILENAME_INDEX_INVALID")
        name = self.template.replace("{tag}", self.tag).replace("{index}", str(seq + 1))
        return safe_basename(name)
