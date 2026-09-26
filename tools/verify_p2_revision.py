"""Revision evidence; preserves raw subprocess bytes, including failed installation."""

import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "reports" / "P2-final"
BASE = "52358d6fca728d2bba12814490e0974a6907b218"
PINNED_WHEELS = {
    "pyarrow-18.1.0-cp312-cp312-win_amd64.whl": (
        "0ad4892617e1a6c7a551cfc827e072a633eaff758fa09f21c4ee548c30bcaf99"
    ),
    "ruff-0.9.2-py3-none-win_amd64.whl": (
        "c5e1d6abc798419cf46eed03f54f2e0c3adb1ad4b801119dedf23fcaf69b55b5"
    ),
}


def _wheelhouse_args() -> list[str]:
    configured = os.environ.get("SAKURAPOOL_WHEELHOUSE")
    if not configured:
        return []
    root = Path(configured).resolve()
    paths = []
    for name, expected in PINNED_WHEELS.items():
        path = root / name
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != expected:
            raise ValueError(f"wheelhouse hash mismatch: {name}: {actual}")
        paths.append(str(path))
    return paths


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    entries = []
    env = dict(
        os.environ,
        PYTHONPATH=str(ROOT / "src"),
        UV_HTTP_TIMEOUT="120",
        UV_HTTP_RETRIES="2",
    )

    def run(name, argv, cwd=ROOT, timeout=150, process_env=env):
        process = subprocess.Popen(argv, cwd=cwd, env=process_env, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
        try:
            data, _ = process.communicate(timeout=timeout)
            code = process.returncode
        except subprocess.TimeoutExpired:
            if os.name == "nt":
                subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                               capture_output=True, timeout=15)
            else:
                process.kill()
            data, _ = process.communicate(timeout=15)
            data, code = data + b"\nHARNESS_TIMEOUT\n", 124
        (OUT / f"{name}.log").write_bytes(data)
        entries.append(dict(name=name, argv=[str(a) for a in argv], cwd=str(cwd), exit=code,
                            bytes=len(data), sha256=hashlib.sha256(data).hexdigest()))
        (OUT / "manifest.json").write_text(json.dumps(entries, indent=2), encoding="utf-8")
        print(f"{name}: exit={code}, bytes={len(data)}", flush=True)
        return code

    run("git", ["git", "log", "--format=%H %s", f"{BASE}..HEAD"])
    run("environment", [sys.executable, "-c", "import sys,platform,pyarrow;"
        "print(sys.executable);print(sys.version);print(platform.platform());"
        "print('Arrow',pyarrow.__version__)"])
    junit = OUT / "host-junit.xml"
    run("pytest", [sys.executable, "-m", "pytest", "-rA", f"--junitxml={junit}"])
    run("ruff", [sys.executable, "-m", "ruff", "check", "."])
    run("diff", ["git", "diff", "--check"])
    run("diff-base", ["git", "diff", f"{BASE}..HEAD", "--check"])
    wheel_dir = Path("/tmp/sakurapool-p2-wheel").resolve()
    run("build", [sys.executable, "-m", "build", "--wheel", "--outdir", str(wheel_dir)])
    run("benchmark", [sys.executable, "tools/benchmark_indexer.py"])
    run("extents", [sys.executable, "-c", "from tools.p2_extent_probe import main;main()"])
    # Same resolved directory for build and install, including Windows drive-root /tmp.
    with tempfile.TemporaryDirectory(prefix="sakurapool-p2-clean-") as directory:
        clean = Path(directory)
        venv = clean / "venv"
        code = run("venv", ["uv", "venv", "--python", "3.12", str(venv)])
        python = venv / "Scripts" / "python.exe" if os.name == "nt" else venv / "bin" / "python"
        wheel = wheel_dir / "sakurapool-0.1.0-py3-none-any.whl"
        if code == 0:
            code = run(
                "wheel-install",
                [
                    "uv",
                    "pip",
                    "install",
                    "--python",
                    str(python),
                    str(wheel),
                    *_wheelhouse_args(),
                    "pip",
                    "pytest==8.3.4",
                    "ruff==0.9.2",
                    "build==1.2.2.post1",
                ],
                timeout=600,
            )
        if code == 0:
            isolated = dict(os.environ, SAKURAPOOL_EXPECT_INSTALLED="1")
            isolated.pop("PYTHONPATH", None)
            run(
                "wheel-import",
                [
                    str(python),
                    "-c",
                    "import pathlib,sakurapool,sys; p=pathlib.Path(sakurapool.__file__).resolve(); "
                    "assert str(pathlib.Path(sys.prefix)) in str(p), p; assert not any("
                    f"str(pathlib.Path({str((ROOT / 'src').resolve())!r}).resolve()) "
                    "== str(pathlib.Path(x).resolve()) for x in sys.path if x)",
                ],
                cwd=clean,
                process_env=isolated,
            )
            run("pip-check", [str(python), "-m", "pip", "check"], cwd=clean,
                process_env=isolated)
            pinned_junit = OUT / "pinned-junit.xml"
            run(
                "pinned-pytest",
                [
                    str(python), "-m", "pytest", "-rA", f"--junitxml={pinned_junit}",
                    str(ROOT / "tests"),
                ],
                cwd=clean,
                process_env=isolated,
            )
            run("pinned-ruff", [str(python), "-m", "ruff", "check", str(ROOT)],
                cwd=clean, process_env=isolated)
            config = clean / "config.json"
            config.write_text('{"datasets":{"demo":{"source":"A"}}}')
            source = clean / "fixture.tar"
            with tarfile.open(source, "w") as tar:
                for name, payload in {"1.jpg": b"not an image", "1.json": b"{}"}.items():
                    member = tarfile.TarInfo(name)
                    member.size = len(payload)
                    tar.addfile(member, io.BytesIO(payload))
            sakura = python.parent / ("sakura.exe" if os.name == "nt" else "sakura")
            for name, args in [
                ("version", ["--version"]),
                ("config", ["config", "validate", "--config", str(config)]),
                ("scan", ["index", "scan", "--config", str(config), "--dataset", "demo",
                          "--input", str(source), "--output", str(clean / "out")]),
            ]:
                run("wheel-" + name, [str(sakura), *args], cwd=clean, process_env=isolated)
    for name, junit in (("host", OUT / "host-junit.xml"), ("pinned", OUT / "pinned-junit.xml")):
        if junit.exists():
            suite = ET.parse(junit).getroot()
            entries.append({"name": name + "-junit-statistics", "tests": int(suite.attrib["tests"]),
                            "failures": int(suite.attrib.get("failures", 0)),
                            "errors": int(suite.attrib.get("errors", 0))})
    run("status", ["git", "status", "--short", "--branch"])
    return 1 if any(entry.get("exit", 0) for entry in entries) else 0


if __name__ == "__main__":
    raise SystemExit(main())
