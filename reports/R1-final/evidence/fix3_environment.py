"""Read-only tool/artifact/environment and fixed data-root measurement."""

import hashlib
import importlib.metadata as md
import json
import os
import platform
import subprocess
import sys
from pathlib import Path

from sakurapool.storage.budget import DEFAULT_WORK_ROOT, _disk_usage
from sakurapool.storage.remote_index import OFFLINE_STAGE_ALLOWANCE


def main():
    print(f"PYTHON={sys.version}\nEXECUTABLE={sys.executable}\nPLATFORM={platform.platform()}")
    for name in ["sakurapool", "build", "pytest", "ruff", "requests", "modelscope-hub",
                 "pyarrow", "numpy", "pyroaring"]:
        print(f"VERSION {name}={md.version(name)}")
    for command in [["rustc", "--version"], ["cargo", "--version"], ["uv", "--version"],
                    ["gcc", "--version"], ["git", "--version"]]:
        subprocess.run(command, check=True)
    worker = Path(os.environ["SAKURAPOOL_RUST_WORKER"])
    for path in [Path("rust/Cargo.lock"), worker,
                 Path("D:/SakuraTool/SakuraPool-P4-work/rust-target/release/sakurapool-worker.exe")]:
        raw = path.read_bytes()
        print(f"ARTIFACT {path} bytes={len(raw)} sha256={hashlib.sha256(raw).hexdigest()}")
    wheel_root = Path("D:/SakuraTool/SakuraPool-Fix3-20260930a/wheels")
    if wheel_root.exists():
        for path in wheel_root.glob("*.whl"):
            raw = path.read_bytes()
            print(f"WHEEL {path} bytes={len(raw)} sha256={hashlib.sha256(raw).hexdigest()}")
    allocated = _disk_usage(DEFAULT_WORK_ROOT)
    print(f"FIXED_DATA_ROOT={DEFAULT_WORK_ROOT}\nFIXED_ALLOCATED={allocated}")
    print(f"STAGE_RESERVATION={OFFLINE_STAGE_ALLOWANCE}")
    print(f"STAGE_MARGIN={4 * (1 << 30) - allocated - OFFLINE_STAGE_ALLOWANCE}")
    names = sorted(path.name for path in DEFAULT_WORK_ROOT.glob("offline-twohop-*"))
    print(f"TWOHOP_EXISTING_COUNT={len(names)}")
    print(f"TWOHOP_NAMES_SHA256={hashlib.sha256(json.dumps(names).encode()).hexdigest()}")
    print(f"CARGO_TARGET_DIR={os.environ.get('CARGO_TARGET_DIR')}")
    print(f"PYTHONPATH={os.environ.get('PYTHONPATH', '<unset>')}")


if __name__ == "__main__":
    main()
