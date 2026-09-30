"""Explicit allowlist-only venv cleanup authorized by user/root, in two batches.

Never delete a root, junction, symlink, non-venv, active venv, keeper, target or
application directory. Audit goes to stdout BEFORE each removal. No file hashes.
First batch requires verified imports/pip/62-test bridge log; second also requires
successful complete Python 3.13 regression. The old Fix3 py312 remains fallback
until second-batch prerequisites are satisfied.
"""

import ctypes
import datetime
import json
import os
import shutil
import stat
import subprocess
import sys
from ctypes import wintypes
from pathlib import Path

from inventory import reparse

from sakurapool.storage.budget import DEFAULT_WORK_ROOT, _disk_usage

REPO = Path("D:/SakuraTool/SakuraPool")
TOOLS = Path("D:/SakuraTool/SakuraPool-Fix3-20260930a")
KEEP = TOOLS / "python-env"
EVIDENCE = Path(__file__).parent
P4 = DEFAULT_WORK_ROOT
FIRST = [P4 / name for name in [
    ".venv312", "p4-cert-56f40c4/venv310", "p4-cert-56f40c4/venv312",
    "p4-cert-56f40c4/venv313", "p4-cert-7f8afbc/venv312", "r1-final-py312",
    "r1-final-wheel", "r1-fresh-venv"]]
SECOND = [REPO / name for name in [
    ".venv", ".venv310s", ".venv312", ".venv312benchrepair", ".venv312finalr4",
    ".venv312r4", ".venv312w", ".venv312w2", ".venv312w3", ".venv313s"]]
SECOND += [TOOLS / "py312", TOOLS / "wheel312"]


def emit(label, record):
    print(label + " " + json.dumps(record, ensure_ascii=False), flush=True)


def prerequisite(batch):
    assert Path(sys.prefix) == KEEP, (sys.prefix, KEEP)
    assert sys.version_info[:3] == (3, 13, 15)
    for name in ["candidate-import-recheck.log", "candidate-pipcheck.log",
                 "candidate-bridge.log"] + (["python313-full.log"] if batch == "remaining" else []):
        text = (EVIDENCE / name).read_text(encoding="utf-8")
        assert "EXIT_CODE=0" in text, name
    assert "62 passed" in (EVIDENCE / "candidate-bridge.log").read_text(encoding="utf-8")
    if batch == "remaining":
        text = (EVIDENCE / "python313-full.log").read_text(encoding="utf-8")
        assert "521 passed, 2 skipped" in text
    subprocess.run([sys.executable, "-m", "pip", "check"], check=True)


def inspect(paths):
    total_allocated, releasable, logical = 0, 0, 0
    identities, audits = {}, []
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    allocated = kernel.GetCompressedFileSizeW
    allocated.argtypes = [wintypes.LPCWSTR, ctypes.POINTER(wintypes.DWORD)]
    allocated.restype = wintypes.DWORD
    for path in paths:
        assert path != KEEP and path.is_dir()
        for ancestor in [path, *path.parents]:
            assert not reparse(ancestor), ancestor
        cfg = path / "pyvenv.cfg"
        assert cfg.is_file() and not reparse(cfg)
        cfg_text = cfg.read_text(encoding="utf-8")
        assert "home = " in cfg_text and "version" in cfg_text, path
        record = dict(path=str(path), root_junction_or_reparse=False,
                      descendant_junction_or_reparse=False, cfg=cfg_text,
                      logical_bytes=0, allocated_file_bytes=0, files=0)
        for current, dirs, files in os.walk(path, followlinks=False):
            base = Path(current)
            for name in dirs:
                assert not reparse(base / name), base / name
            for name in files:
                file = base / name
                st = file.lstat()
                assert not reparse(file) and stat.S_ISREG(st.st_mode), file
                high = wintypes.DWORD()
                ctypes.set_last_error(0)
                low = allocated(str(file), ctypes.byref(high))
                assert low != 0xFFFFFFFF or not ctypes.get_last_error(), file
                size = (high.value << 32) | low
                record["logical_bytes"] += st.st_size
                record["allocated_file_bytes"] += size
                record["files"] += 1
                ident = (st.st_dev, st.st_ino)
                old = identities.setdefault(ident, dict(links=st.st_nlink, removed=0,
                                                       allocated=size))
                assert old["links"] == st.st_nlink and old["allocated"] == size
                old["removed"] += 1
        audits.append(record)
        total_allocated += record["allocated_file_bytes"]
        logical += record["logical_bytes"]
    for record in identities.values():
        assert record["removed"] <= record["links"]
        if record["removed"] == record["links"]:
            releasable += record["allocated"]
    return audits, dict(logical_bytes=logical, counted_allocated_file_bytes=total_allocated,
                        expected_releasable_file_extents_bytes=releasable,
                        note=("NTFS extents estimate; excludes directory metadata/"
                              "concurrent changes"))


def active(paths):
    proc = subprocess.run(["powershell.exe", "-NoProfile", "-File",
                           str(EVIDENCE / "process_snapshot.ps1")],
                          capture_output=True, text=True, timeout=60, check=True,
                          env=dict(os.environ, PROJECT_VENVS="|".join(map(str, paths))))
    snapshot = json.loads(proc.stdout)
    matched = [p for p in snapshot if p["matched_venvs"]]
    emit("PRE_DELETE_PROCESSES", matched)
    assert not matched, matched


def main():
    assert len(sys.argv) == 2 and sys.argv[1] in {"p4", "remaining"}
    batch = sys.argv[1]
    prerequisite(batch)
    paths = FIRST if batch == "p4" else SECOND
    audits, sizes = inspect(paths)
    active(paths)
    before = shutil.disk_usage("D:/").free
    root_before = _disk_usage(P4)
    names_before = sorted(p.name for p in P4.glob("offline-twohop-*"))
    emit("PRE_DELETE_BATCH", dict(timestamp=datetime.datetime.now().astimezone().isoformat(),
                                 batch=batch, keeper=str(KEEP), audits=audits, estimate=sizes,
                                 volume_free_bytes=before, p4_budget_counted_bytes=root_before,
                                 offline_twohop_count=len(names_before)))
    for path in paths:
        active([path])
        assert not reparse(path) and (path / "pyvenv.cfg").is_file()
        shutil.rmtree(path)
        assert not path.exists()
        emit("DELETED_CONFIRMED_VENV", str(path))
    names_after = sorted(p.name for p in P4.glob("offline-twohop-*"))
    assert names_after == names_before
    assert (KEEP / "pyvenv.cfg").is_file()
    emit("POST_DELETE_BATCH", dict(batch=batch, deleted_paths=list(map(str, paths)),
                                  volume_free_bytes=shutil.disk_usage("D:/").free,
                                  volume_free_delta_bytes=shutil.disk_usage("D:/").free - before,
                                  p4_budget_counted_bytes=_disk_usage(P4),
                                  offline_twohop_count=len(names_after),
                                  offline_twohop_names_unchanged=True))


if __name__ == "__main__":
    main()
