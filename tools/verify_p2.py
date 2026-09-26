"""Reproducible offline evidence collection. Never downloads dependencies."""

import hashlib
import importlib.metadata
import json
import os
import shutil
import subprocess
import sys
import tempfile
import venv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "P2"
REPORT.mkdir(parents=True, exist_ok=True)
ENV = dict(
    os.environ, PYTHONPATH=str(ROOT / "src"), PIP_NO_INDEX="1", PIP_DISABLE_PIP_VERSION_CHECK="1"
)
records = []


def run(name, command, *, cwd=ROOT, env=ENV):
    result = subprocess.run(command, cwd=cwd, env=env, capture_output=True)
    data = result.stdout + result.stderr
    path = REPORT / f"{name}.log"
    path.write_bytes(data)
    records.append(
        dict(
            file=path.name,
            command=command,
            cwd=str(cwd),
            exit=result.returncode,
            bytes=len(data),
            sha256=hashlib.sha256(data).hexdigest(),
        )
    )
    print(f"{name}: exit={result.returncode} bytes={len(data)}", flush=True)
    return result.returncode


def main():
    failures = []
    commands = [
        ("pytest", [sys.executable, "-m", "pytest", "-v"]),
        (
            "targeted",
            [
                sys.executable,
                "-m",
                "pytest",
                "tests/test_indexer.py",
                "tests/test_indexer_recovery.py",
                "-v",
            ],
        ),
        ("ruff", [sys.executable, "-m", "ruff", "check", "src", "tests", "tools"]),
        ("diff-check", ["git", "diff", "--check"]),
        ("build", [sys.executable, "-m", "build", "--no-isolation"]),
        ("benchmark", [sys.executable, "tools/benchmark_indexer.py"]),
    ]
    for name, command in commands:
        if run(name, command):
            failures.append(name)
    wheel = ROOT / "dist" / "sakurapool-0.1.0-py3-none-any.whl"
    if wheel.exists():
        with tempfile.TemporaryDirectory(prefix="sakurapool-clean-wheel-") as temp:
            directory = Path(temp)
            target = directory / "venv"
            venv.EnvBuilder(with_pip=False).create(target)
            python = target / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
            # No network or global site-packages: transplant the installed Arrow dependency.
            # This is explicitly NOT evidence for the pinned Arrow 18.1.0 environment.
            site = target / (
                "Lib/site-packages"
                if os.name == "nt"
                else f"lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages"
            )
            dist = importlib.metadata.distribution("pyarrow")
            origin = Path(dist.locate_file(""))
            for entry in origin.glob("pyarrow*"):
                if entry.is_dir():
                    shutil.copytree(entry, site / entry.name)
            clean = dict(ENV)
            clean.pop("PYTHONPATH", None)
            if run(
                "wheel-install",
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "--python",
                    str(python),
                    "install",
                    "--no-index",
                    "--no-deps",
                    str(wheel),
                ],
                cwd=directory,
                env=clean,
            ):
                failures.append("wheel-install")
            code = (
                "import io,json,tarfile,pathlib,subprocess,sys,sakurapool,pyarrow;"
                "print('package',sakurapool.__file__,'arrow',pyarrow.__version__);"
                "p=pathlib.Path.cwd();(p/'input').mkdir();"
                "a=tarfile.open(p/'input'/'a.tar','w');"
                "exec(\"for n,d in [('x.jpg',b'not-an-image'),('x.json',b'{}')]:\\n"
                ' t=tarfile.TarInfo(n);t.size=len(d);a.addfile(t,io.BytesIO(d))");'
                "a.close();"
                "r=subprocess.run([sys.executable,'-I','-m','sakurapool','index','scan',"
                "'--input',str(p/'input'),'--output',str(p/'output')],capture_output=True,text=True);"
                "print(r.stdout,r.stderr);assert r.returncode==0;"
                "assert json.loads(r.stdout)['objects']==1;"
                "r=subprocess.run([sys.executable,'-I','-m','sakurapool','validate',sys.argv[1]],"
                "capture_output=True,text=True);print(r.stdout,r.stderr);assert r.returncode==0"
            )
            if run(
                "wheel-smoke",
                [str(python), "-I", "-c", code, str(ROOT / "examples/query.json")],
                cwd=directory,
                env=clean,
            ):
                failures.append("wheel-smoke")
            run(
                "wheel-dependency-check",
                [sys.executable, "-m", "pip", "--python", str(python), "check"],
                cwd=directory,
                env=clean,
            )
        data = wheel.read_bytes()
        records.append(
            dict(
                file=str(wheel.relative_to(ROOT)),
                bytes=len(data),
                sha256=hashlib.sha256(data).hexdigest(),
            )
        )
    metadata = dict(
        python=sys.version,
        environment={
            name: importlib.metadata.version(name)
            for name in ("pyarrow", "pytest", "ruff", "build", "setuptools")
        },
        environment_note="Arrow/pytest/ruff differ from pyproject pins; offline only",
        failures=failures,
        evidence=records,
    )
    (REPORT / "evidence-manifest.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=True) + "\n", encoding="utf-8"
    )
    return int(bool(failures))


if __name__ == "__main__":
    raise SystemExit(main())
