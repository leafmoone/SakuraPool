"""Fresh final-C2 installed-wheel verification; synthetic/loopback only."""

import argparse
import importlib.metadata as metadata
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
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
        [
            "git",
            "-C",
            str(repo),
            "diff",
            "HEAD",
            "--",
            "src",
            "rust",
            "tests",
            "reports/R2C2/canary.py",
            "reports/R2C2/real_binding.py",
            "reports/R2C2/wheel_verify.py",
            "examples/webdataset-danbooru-v3.json",
        ],
        text=True,
    )
    assert not drift
    config_source = repo / "examples/webdataset-danbooru-v3.json"
    committed_config = subprocess.check_output(
        ["git", "-C", str(repo), "show", head + ":examples/webdataset-danbooru-v3.json"]
    )
    assert config_source.read_bytes().replace(b"\r\n", b"\n") == committed_config.replace(
        b"\r\n", b"\n"
    )
    helper_spec = importlib.util.spec_from_file_location(
        "wheel_configured_canary", repo / "reports/R2C2/canary.py"
    )
    helper = importlib.util.module_from_spec(helper_spec)
    helper_spec.loader.exec_module(helper)
    with tempfile.TemporaryDirectory(prefix="c2-wheel-config-") as directory:
        config_copy = Path(directory) / "registry.json"
        config_copy.write_bytes(committed_config)
        adapter = helper.configured_adapter(config_copy)
        assert adapter.dataset == "gamecg_v3" and adapter.allowed_provenance == ("gamecg-2D",)
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
        str(tests / "test_r2c2_configured.py"),
        str(tests / "test_r2c2_negative_body.py"),
        str(tests / "test_r2c2_canary_admission.py"),
        str(tests / "test_r2c2_binding.py")
        + "::test_verify_conditions_loopback_proof_and_exact_package_lookup",
        str(tests / "test_r2c2_binding.py") + "::test_binding_negative_matrix_fail_closed",
        str(tests / "test_r2c2_binding.py")
        + "::test_c2_small_canary_uses_own_proof_and_both_formal_builders",
        str(tests / "test_r2c2_identification.py")
        + "::test_actual_nested_nullable_identification_and_formal_scan",
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
