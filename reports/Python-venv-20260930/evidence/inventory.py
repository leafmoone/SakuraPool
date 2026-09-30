"""Read-only confirmed-project-venv inventory; NEVER follows reparse directories.

Find cfgs in selected project roots only. .git, Rust target and dependency caches
are not traversed. Script refs are a separate source-only pass; historical report
refs are recorded as history, not rewritten. No data/ledger payload is read.
"""

import datetime
import json
import os
import stat
import subprocess
from pathlib import Path

ROOT = Path("D:/SakuraTool/SakuraPool")
SEARCH = [ROOT, Path("D:/SakuraTool/SakuraPool-P4-work"),
          Path("D:/SakuraTool/SakuraPool-Fix3-20260930a"),
          Path("D:/SakuraTool/SakuraPool-p2-merge"),
          Path("D:/SakuraTool/P3-merge-a3a061-receipt")]
PRUNE = {".git", "rust-target", "target", "node_modules", "__pycache__"}
CODE = """
import sys,json,importlib.metadata as md
names=['sakurapool','pyarrow','numpy','pyroaring','pytest','ruff','build','requests','modelscope-hub','pip']
versions={}
for name in names:
 try: versions[name]=md.version(name)
 except md.PackageNotFoundError: versions[name]=None
try: direct=md.distribution('sakurapool').read_text('direct_url.json')
except md.PackageNotFoundError: direct=None
print(json.dumps(dict(version=sys.version,executable=sys.executable,prefix=sys.prefix,base_prefix=sys.base_prefix,versions=versions,direct_url=direct)))
"""


def reparse(path):
    return bool(path.lstat().st_file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT)


def cfgs():
    found, skipped = [], []
    for root in SEARCH:
        for current, dirs, files in os.walk(root, followlinks=False):
            base = Path(current)
            if "pyvenv.cfg" in files:
                found.append(base)
                dirs[:] = []
                continue
            keep = []
            for name in dirs:
                path = base / name
                if reparse(path):
                    skipped.append(str(path))
                elif name not in PRUNE:
                    keep.append(name)
            dirs[:] = keep
    return sorted(set(found)), skipped


def measure(root):
    logical, unique_logical, files_count, reparse_paths = 0, 0, 0, []
    for current, dirs, files in os.walk(root, followlinks=False):
        base = Path(current)
        keep = []
        for name in dirs:
            path = base / name
            if reparse(path):
                reparse_paths.append(str(path))
            else:
                keep.append(name)
        dirs[:] = keep
        for name in files:
            path = base / name
            st = path.lstat()
            if reparse(path):
                reparse_paths.append(str(path))
                continue
            logical += st.st_size
            unique_logical += st.st_size if st.st_nlink == 1 else 0
            files_count += 1
    return dict(logical_bytes=logical, unique_link_logical_bytes=unique_logical,
                files=files_count, descendant_reparse_paths=reparse_paths)


def main():
    envs, skipped = cfgs()
    records = []
    for env in envs:
        cfg = (env / "pyvenv.cfg").read_bytes()
        proc = subprocess.run([str(env / "Scripts/python.exe"), "-I", "-c", CODE],
                              capture_output=True, text=True, timeout=30)
        record = dict(path=str(env), root_reparse=reparse(env),
                      cfg=cfg.decode("utf-8"),
                      interpreter_exit=proc.returncode, **measure(env))
        if proc.returncode == 0:
            record["profile"] = json.loads(proc.stdout)
        else:
            record["interpreter_error"] = proc.stderr[:2000]
        records.append(record)
    ps = subprocess.run(["powershell.exe", "-NoProfile", "-File",
                         str(Path(__file__).with_name("process_snapshot.ps1"))],
                        capture_output=True, text=True, timeout=60, check=True,
                        env=dict(os.environ, PROJECT_VENVS="|".join(map(str, envs))))
    processes = json.loads(ps.stdout)
    for record in records:
        name = record["path"].lower()
        record["active_executable_pids"] = [p["pid"] for p in processes
            if p["executable"] and p["executable"].lower().startswith(name + "\\")]
    report = dict(timestamp=datetime.datetime.now().astimezone().isoformat(),
                  search_roots=list(map(str, SEARCH)), skipped_reparse_paths=skipped,
                  virtual_env_variable=os.environ.get("VIRTUAL_ENV"),
                  confirmed_count=len(records), environments=records,
                  related_processes=processes)
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
