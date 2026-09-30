"""New integration wheel, outside repository -I; synthetic local and R2 only."""

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
from sakurapool.runtime import RUNTIME_COMPILER, RUNTIME_FORMAT_VERSION


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--wheel", type=Path, required=True)
    args = parser.parse_args()
    repo = args.repository.absolute()
    module = Path(sakurapool.__file__).absolute()
    assert sys.flags.isolated == 1 and "PYTHONPATH" not in os.environ
    assert not Path.cwd().absolute().is_relative_to(repo)
    assert "site-packages" in module.parts and not module.is_relative_to(repo)
    assert (RUNTIME_FORMAT_VERSION, RUNTIME_COMPILER) == (2, "sakurapool-p3-v2")
    direct = json.loads(metadata.distribution("sakurapool").read_text("direct_url.json"))
    assert not direct.get("dir_info", {}).get("editable") and "archive_info" in direct
    count = 0
    with ZipFile(args.wheel) as wheel:
        for source in (repo / "src/sakurapool").rglob("*.py"):
            relative = source.relative_to(repo / "src/sakurapool")
            packed = wheel.read("sakurapool/" + relative.as_posix())
            assert packed.replace(b"\r\n", b"\n") == source.read_bytes().replace(b"\r\n", b"\n")
            assert (module.parent / relative).read_bytes() == packed
            count += 1
    print(
        json.dumps(
            {
                "source": str(module),
                "verified_source_files": count,
                "isolated": True,
                "editable": False,
                "runtime_format": 2,
                "compiler": RUNTIME_COMPILER,
                "real_requests": 0,
            }
        ),
        flush=True,
    )
    tests = repo / "tests"
    nodes = [
        str(tests / "test_integration_index_builder_r2.py")
        + "::test_local_remote_stage_equivalence_v2",
        str(tests / "test_integration_index_builder_r2.py")
        + "::test_same_dataset_multipart_runtime_keeps_duplicate_posts",
        str(tests / "test_runtime_multicategory.py") + "::test_v2_identity_schema_and_v1_rejection",
        str(tests / "test_r2_production.py")
        + "::test_two_admin_modes_same_file_audit_and_stage_contract",
    ]
    return pytest.main([*nodes, "-q", "-p", "no:cacheprovider", "-ra"])


if __name__ == "__main__":
    raise SystemExit(main())
