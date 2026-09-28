"""Isolated, in-process P4 inspect credential bridge for an explicitly approved batch.

Never print credential bytes or arbitrary exception messages. This entry point
performs no network itself. Real invocation requires separate batch approval;
unit tests pass a synthetic token path to _run_cli, not the fixed secret file.
Windows pinned Python 3.12 crashes on os.execve: use runpy in this child only.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import runpy
import stat
import sys
from pathlib import Path

TOKEN_FILE = Path("D:/sm_data/ms-token.tmp")
REPO_SRC = Path("D:/SakuraTool/SakuraPool/src")
WORK_ROOT = Path("D:/SakuraTool/SakuraPool-P4-work")


class _UnsafeCredential(Exception):
    """Only fixed-code outward handling; never include token/file contents."""


def _safe_error(code: str, phase: str) -> int:
    if (code, phase) not in {("credential_invalid", "preflight"),
                             ("local_or_unclassified", "local")}:
        raise AssertionError("unrecognized safe bootstrap error")
    print(json.dumps({"status": "ERROR", "error": "remote operation failed",
                      "code": code, "phase": phase}, sort_keys=True), flush=True)
    return 64 if code == "credential_invalid" else 70


def _load_token(path: Path) -> str:
    """Read one small, plain, unstructured ASCII token; never log its value."""
    try:
        before = path.stat(follow_symlinks=False)
        if (not stat.S_ISREG(before.st_mode) or before.st_size < 1
                or before.st_size > 4096
                or getattr(before, "st_file_attributes", 0)
                & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)):
            raise _UnsafeCredential
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_BINARY", 0))
        try:
            opened = os.fstat(fd)
            if (not stat.S_ISREG(opened.st_mode)
                    or (opened.st_dev, opened.st_ino, opened.st_size) != (
                        before.st_dev, before.st_ino, before.st_size)):
                raise _UnsafeCredential
            raw = os.read(fd, 4097)
        finally:
            os.close(fd)
        token = raw.strip()
        # '=' may only be terminal base64 padding; KEY=VALUE, JSON, quoted
        # strings, multiline files, spaces and controls are *not* tokens.
        if (len(raw) != before.st_size or not 1 <= len(token) <= 4096
                or re.fullmatch(rb"[A-Za-z0-9._~+/\-]{1,4096}={0,2}", token) is None):
            raise _UnsafeCredential
        return token.decode("ascii")
    except (OSError, UnicodeError, ValueError, _UnsafeCredential):
        raise _UnsafeCredential from None


def _run_cli(cli_args: tuple[str, ...], *, token_path: Path = TOKEN_FILE,
             repo_src: Path = REPO_SRC) -> int:
    """Load token and run the *real module CLI* in this process, not execve.

    Injection occurs after validating source origin and immediately before the
    entry point; no proxy/global environment settings are changed. Secrets
    never enter argv, source, logs, or an exception/traceback printed here.
    """
    try:
        token = _load_token(token_path)
    except BaseException:
        return _safe_error("credential_invalid", "preflight")
    try:
        source = repo_src.resolve(strict=True)
        if (not source.is_dir() or source != REPO_SRC.resolve(strict=True)
                or "sakurapool" in sys.modules):
            raise ValueError("unsafe module origin")
        sys.path.insert(0, str(source))
        spec = importlib.util.find_spec("sakurapool")
        expected = (source / "sakurapool" / "__init__.py").resolve(strict=True)
        if (spec is None or spec.origin is None or Path(spec.origin).resolve() != expected
                or not spec.submodule_search_locations
                or [Path(part).resolve() for part in spec.submodule_search_locations]
                != [expected.parent]):
            raise ValueError("untrusted imported package")
        sys.argv = [str(source / "sakurapool" / "__main__.py"), *cli_args]
        # No environment-derived credentials/proxies are changed; product
        # GuardedTransport has trust_env=False and sends auth only at origin.
        os.environ["MODELSCOPE_API_TOKEN"] = token
        try:
            runpy.run_module("sakurapool", run_name="__main__", alter_sys=False)
        except SystemExit as done:
            if type(done.code) is int and done.code in (0, 2, 3):
                return done.code
            return _safe_error("local_or_unclassified", "local")
        except BaseException:
            return _safe_error("local_or_unclassified", "local")
        return _safe_error("local_or_unclassified", "local")
    except BaseException:
        return _safe_error("local_or_unclassified", "local")
    finally:
        os.environ.pop("MODELSCOPE_API_TOKEN", None)


def _preflight_inspect_paths(args: list[str]) -> tuple[Path, Path]:
    """Inspect only one null-revision profile before even opening a token fd."""
    if (len(args) != 4 or args[0] != "--config" or args[2] != "--output"
            or not isinstance(args[1], str) or not isinstance(args[3], str)):
        raise ValueError("invalid inspect arguments")
    root = WORK_ROOT.absolute()
    if not root.is_dir() or root.resolve(strict=True) != root:
        raise ValueError("unsafe fixed work root")
    for ancestor in (root, *root.parents):
        attrs = ancestor.stat(follow_symlinks=False)
        if (ancestor.is_symlink() or not stat.S_ISDIR(attrs.st_mode)
                or getattr(attrs, "st_file_attributes", 0)
                & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)):
            raise ValueError("unsafe work-root parent")
    config, output = Path(args[1]).absolute(), Path(args[3]).absolute()
    # This narrow bootstrap publishes only at the root; it must not
    # traverse arbitrary operator-created subdirectories or aliases.
    if (config.parent != root or output.parent != root or config == output
            or os.path.lexists(output) or not config.is_file()):
        raise ValueError("unsafe inspect path")
    info = config.stat(follow_symlinks=False)
    if (not stat.S_ISREG(info.st_mode) or config.is_symlink()
            or getattr(info, "st_file_attributes", 0)
            & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
            or not 0 < info.st_size <= 16 * 1024
            or config.resolve(strict=True) != config
            or output.parent.resolve(strict=True) != root):
        raise ValueError("unsafe storage profile")
    with config.open("rb") as source:
        raw = source.read(16 * 1024 + 1)
    if len(raw) > 16 * 1024:
        raise ValueError("storage profile exceeds limit")
    profile = json.loads(raw)
    if (not isinstance(profile, dict) or set(profile) != {
            "repo_id", "endpoint", "revision", "trusted_hosts", "work_root"}
            or profile["repo_id"] != "leafmoone/game_cg_5M"
            or profile["endpoint"] != "https://modelscope.cn"
            or profile["revision"] is not None
            or profile["trusted_hosts"] != ["modelscope.cn"]
            or not isinstance(profile["trusted_hosts"], list)
            or not isinstance(profile["work_root"], str)
            or Path(profile["work_root"]).absolute() != root):
        raise ValueError("profile not limited to revision bootstrap")
    return config, output


def main(args: list[str]) -> int:
    """Production entry: bounded config guard, then one token-backed inspect."""
    try:
        config, output = _preflight_inspect_paths(args)
    except BaseException:
        # Invalid paths, JSON, permissions or KeyboardInterrupt remain local;
        # never print argv, token bytes, OS exception or chained traceback.
        return _safe_error("local_or_unclassified", "local")
    return _run_cli(("remote", "inspect", "--config", str(config),
                     "--output", str(output)))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
