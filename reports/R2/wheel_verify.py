"""Installed R2 wheel acceptance; keeper Python -I, outside repository, no real HTTP.

Verify noneditable site-packages before executing exact current acceptance nodes.
The selected tests create only budgeted small loopback fixtures at the fixed root.
A complete source regression is separate; this does not label selection a full suite.
"""

from __future__ import annotations

import argparse
import importlib.metadata as metadata
import json
import os
import sys
from pathlib import Path
from zipfile import ZipFile

import pytest

import sakurapool


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--wheel", type=Path, required=True)
    args = parser.parse_args()
    repo = args.repository.absolute()
    assert sys.flags.isolated == 1
    assert "PYTHONPATH" not in os.environ
    assert not Path.cwd().absolute().is_relative_to(repo)
    module = Path(sakurapool.__file__).absolute()
    assert "site-packages" in module.parts and not module.is_relative_to(repo), module
    direct = json.loads(metadata.distribution("sakurapool").read_text("direct_url.json"))
    assert not direct.get("dir_info", {}).get("editable") and "archive_info" in direct
    count = 0
    with ZipFile(args.wheel) as wheel:
        for path in (repo / "src/sakurapool").rglob("*.py"):
            name = path.relative_to(repo / "src").as_posix()
            packed = wheel.read(name)
            assert packed.replace(b"\r\n", b"\n") == path.read_bytes().replace(b"\r\n", b"\n")
            installed = module.parent / path.relative_to(repo / "src/sakurapool")
            assert installed.read_bytes() == packed
            count += 1
        info_name = next(name for name in wheel.namelist() if name.endswith(".dist-info/METADATA"))
        info = wheel.read(info_name).decode()
        assert "Requires-Python: >=3.10" in info
    print(
        json.dumps(
            {
                "python": sys.version,
                "executable": sys.executable,
                "package_source": str(module),
                "wheel_sources_verified": count,
                "isolated": True,
                "editable": False,
                "real_network": False,
            }
        ),
        flush=True,
    )
    tests = repo / "tests/test_r2_production.py"
    nodes = [
        str(tests) + "::" + name
        for name in (
            "test_rust_auth_stripping_conditional_proof_and_real_bytes",
            "test_independent_cookie_value_echo_never_leaks_from_rust",
            "test_production_package_memory_and_validation_precede_credentials",
            "test_two_admin_modes_same_file_audit_and_stage_contract",
        )
    ]
    # Each mode includes a separate -I Python/worker exact image+JSON fetch.
    return pytest.main([*nodes, "-q", "-rA", "-p", "no:cacheprovider"])


if __name__ == "__main__":
    raise SystemExit(main())
