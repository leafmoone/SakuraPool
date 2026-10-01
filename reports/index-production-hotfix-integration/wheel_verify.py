"""Fresh integration installed-wheel verification; synthetic/loopback only."""

import argparse
import importlib.metadata as metadata
import json
import os
import subprocess
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
    parser.add_argument("--code-commit", required=True)
    args = parser.parse_args()
    repo = args.repository.absolute()
    module = Path(sakurapool.__file__).absolute()
    assert sys.flags.isolated == 1 and "PYTHONPATH" not in os.environ
    assert not Path.cwd().absolute().is_relative_to(repo)
    assert "site-packages" in module.parts and not module.is_relative_to(repo)
    assert (RUNTIME_FORMAT_VERSION, RUNTIME_COMPILER) == (2, "sakurapool-p3-v2")
    direct = json.loads(metadata.distribution("sakurapool").read_text("direct_url.json"))
    assert not direct.get("dir_info", {}).get("editable") and "archive_info" in direct
    worker = Path(os.environ["SAKURAPOOL_RUST_WORKER"])
    assert worker.is_file() and worker.parent.name == "release"
    head = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    assert head == args.code_commit
    drift = subprocess.check_output(
        ["git", "-C", str(repo), "diff", "HEAD", "--", "src", "rust", "tests"], text=True
    )
    assert not drift
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
                "worker": str(worker),
                "runtime_format": 2,
                "p2_format": 4,
                "code_commit": head,
                "real_requests": 0,
            }
        ),
        flush=True,
    )
    tests = repo / "tests"
    nodes = [
        str(tests / "test_nested_metadata.py"),
        str(tests / "test_nested_null_indexer.py"),
        str(tests / "test_empty_fragments.py"),
        str(tests / "test_empty_package.py"),
        str(tests / "test_staged_lookup.py"),
        str(tests / "test_hotfix_integration.py") + "::test_cleanup_normal_close_precedes_unlink",
        str(tests / "test_hotfix_integration.py")
        + "::test_production_callable_stage_preserves_both_indexes",
        str(tests / "test_integration_index_builder_r2.py")
        + "::test_local_remote_stage_equivalence_v2",
        str(tests / "test_r2_production.py")
        + "::test_two_admin_modes_same_file_audit_and_stage_contract",
    ]
    return pytest.main([*nodes, "-q", "-p", "no:cacheprovider", "-ra"])


if __name__ == "__main__":
    raise SystemExit(main())
