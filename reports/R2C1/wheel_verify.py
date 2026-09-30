"""Fresh R2C1 installed wheel, outside repository and isolated Python; loopback only."""

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
    parser.add_argument("--headers-only", action="store_true")
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
                "compiler": RUNTIME_COMPILER,
                "real_requests": 0,
            }
        ),
        flush=True,
    )
    tests = repo / "tests"
    nodes = [
        str(tests / "test_integration_index_builder_r2.py"),
        str(tests / "test_runtime_multicategory.py") + "::test_v2_identity_schema_and_v1_rejection",
        str(tests / "test_r2_production.py")
        + "::test_two_admin_modes_same_file_audit_and_stage_contract",
        str(tests / "test_r2c1_resources.py") + "::test_limits_reject_before_unbounded_json_decode",
        str(tests / "test_r2c1_spools.py")
        + "::test_disposable_spool_process_death_never_creates_commit_or_journal",
    ]
    if args.headers_only:
        nodes = [
            str(tests / "test_r2_production.py")
            + "::test_rust_auth_stripping_conditional_proof_and_real_bytes",
            str(tests / "test_r2_production.py")
            + "::test_two_admin_modes_same_file_audit_and_stage_contract",
        ]
    return pytest.main([*nodes, "-q", "-p", "no:cacheprovider", "-ra"])


if __name__ == "__main__":
    raise SystemExit(main())
