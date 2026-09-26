"""Malformed archive/config boundary behavior stays machine-readable."""

import json

import pytest
from test_indexer import make_tar

from sakurapool.cli import main
from sakurapool.registry import AdapterRegistry


@pytest.mark.parametrize(
    "config",
    [
        None,
        {},
        {"datasets": []},
        {"datasets": {}},
        {"datasets": {"x": {}}},
        {"datasets": {"x": {"source": " x "}}},
        {"datasets": {"x": {"source": "x", "unexpected": 1}}},
        {"datasets": {"x": {"source": "x", "max_json_bytes": 0}}},
    ],
)
def test_invalid_adapter_config(config):
    with pytest.raises(ValueError):
        AdapterRegistry.from_dict(config)


def test_truncated_tar_cli_json(tmp_path, capsys):
    source = tmp_path / "input"
    source.mkdir()
    archive = source / "a.tar"
    make_tar(archive, {"x.jpg": b"x" * 2048, "x.json": b"{}"})
    archive.write_bytes(archive.read_bytes()[:1024])
    assert main(["index", "scan", str(source), str(tmp_path / "out")]) == 2
    assert "error" in json.loads(capsys.readouterr().out)
    assert not list((tmp_path / "out").glob("*.COMMIT"))


def test_registry_duplicate():
    registry = AdapterRegistry.local()
    with pytest.raises(ValueError, match="duplicate"):
        registry.register(registry.get("local"))
