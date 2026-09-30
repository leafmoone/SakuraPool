"""Final selected-environment/source/wheel checks, without new log/file hashes."""

import email
import importlib.metadata as md
import json
import os
import re
import subprocess
import sys
import zipfile
from collections import Counter
from pathlib import Path

from inventory import cfgs

from sakurapool.storage.budget import DEFAULT_WORK_ROOT, _disk_usage
from sakurapool.storage.remote_index import OFFLINE_STAGE_ALLOWANCE

ROOT = Path("D:/SakuraTool/SakuraPool")
KEEP = Path("D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env")
IMPL = "278b59831aba82da19bb533763e86b68ac6c2a55"
TREE = "06dd1bba427d40849f3af4cab1a06c3fad6162e9"
WHEEL = KEEP.parent / "python-wheel/sakurapool-0.1.0-py3-none-any.whl"
PINS = {"pyarrow": "18.1.0", "numpy": "2.2.6", "pyroaring": "1.1.0",
        "pytest": "8.3.4", "ruff": "0.9.2", "build": "1.2.2.post1",
        "requests": "2.32.5", "modelscope-hub": "0.4.0", "setuptools": "75.6.0"}


def git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args])


def main():
    assert Path(sys.prefix) == KEEP and sys.version_info[:3] == (3, 13, 15)
    assert "PYTHONPATH" not in os.environ
    assert git("rev-parse", IMPL + "^{tree}").decode().strip() == TREE
    subprocess.run(["git", "-C", str(ROOT), "diff", IMPL, "--exit-code", "--",
                    "src", "rust", "tests", "pyproject.toml", "tools"], check=True)
    subprocess.run(["git", "-C", str(ROOT), "diff",
                    "66579d7f9e50575cefd17c6e56da87533af4bf5f", "--exit-code", "--",
                    "reports/R1-final"], check=True)
    envs, skipped = cfgs()
    assert envs == [KEEP] and not skipped, (envs, skipped)
    print("UNIQUE_CONFIRMED_PROJECT_VENV", KEEP)
    print("PYTHON", sys.version, "EXECUTABLE", sys.executable)
    print("VERSIONS", json.dumps({name: md.version(name) for name in PINS}))
    for name, version in PINS.items():
        assert md.version(name) == version
    dist = md.distribution("sakurapool")
    assert dist.metadata["Requires-Python"] == ">=3.10"
    assert json.loads(dist.read_text("direct_url.json"))["dir_info"]["editable"]
    with zipfile.ZipFile(WHEEL) as wheel:
        sources = [name for name in wheel.namelist()
                   if name.startswith("sakurapool/") and name.endswith(".py")]
        assert len(sources) == 28
        for name in sources:
            source = git("show", IMPL + ":src/" + name)
            assert wheel.read(name).replace(b"\r\n", b"\n") == source, name
        meta = email.message_from_bytes(wheel.read("sakurapool-0.1.0.dist-info/METADATA"))
        assert meta["Requires-Python"] == ">=3.10"
    print("WHEEL_28_SOURCES_MATCH_IMPLEMENTATION_BLOBS_WITH_CRLF_NORMALIZATION")
    print("WHEEL_METADATA_REQUIRES_PYTHON >=3.10")
    log = (Path(__file__).parent / "python313-full.log").read_text(encoding="utf-8")
    assert "HEAD " + IMPL in log and "HEAD_TREE " + TREE in log
    assert "EXIT_CODE=0" in log and "521 passed, 2 skipped" in log
    completed = re.findall(r"^(tests/.*?) (PASSED|SKIPPED)[ \t]+\[", log, re.MULTILINE)
    counts = Counter(state for _, state in completed)
    assert counts == {"PASSED": 521, "SKIPPED": 2} and len(completed) == 523
    print("FULL_SUITE_523_TERMINAL_ROWS", dict(counts))
    print("ACTUAL_SKIP_NODEIDS", [name for name, state in completed if state == "SKIPPED"])
    allocated = _disk_usage(DEFAULT_WORK_ROOT)
    print("FIXED_DATA_ROOT", DEFAULT_WORK_ROOT, "BUDGET_COUNTED_BYTES", allocated)
    print("UNCHANGED_DISK_CAP", 4 * (1 << 30), "STAGE_RESERVATION", OFFLINE_STAGE_ALLOWANCE,
          "CURRENT_STAGE_MARGIN", 4 * (1 << 30) - allocated - OFFLINE_STAGE_ALLOWANCE)
    print("OFFLINE_TWOHOP_COUNT", len(list(DEFAULT_WORK_ROOT.glob("offline-twohop-*"))))
    subprocess.run([sys.executable, "-m", "pip", "check"], check=True)
    subprocess.run([sys.executable, "-m", "ruff", "check", "."], cwd=ROOT, check=True)
    # Post-cleanup proof: current-interpreter token path and worker process lifecycle.
    subprocess.run([sys.executable, "-m", "pytest", "tests/test_p4_token_bootstrap.py",
                    "tests/test_r1_stdout_backpressure.py", "-q", "-rA"], cwd=ROOT, check=True)
    print("FINAL_KEEPER_VERIFICATION_PASS", IMPL, TREE)


if __name__ == "__main__":
    main()
