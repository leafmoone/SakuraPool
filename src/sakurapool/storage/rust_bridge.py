"""Thin offline bridge from Python to the ``sakurapool-r1`` Rust worker.

Spawns the native worker process, exchanges NDJSON over stdin/stdout, and
never touches the network. Failure surfaces are static; no credential values
are recorded.
"""

from __future__ import annotations

import json
import os
import queue
import subprocess
import threading
from pathlib import Path

_WORKER_ENV = "SAKURAPPOOL_R1_WORKER"
_WORKER_NAME = "sakurapool-r1-worker"
_TIMEOUT_S = 60.0


class RustWorkerError(RuntimeError):
    """Static failure surface for the Rust worker bridge."""


def find_worker_binary() -> Path:
    """Locate the worker binary; raise with a static message when missing."""
    env = os.environ.get(_WORKER_ENV)
    if env:
        candidate = Path(env)
        if candidate.is_file():
            return candidate
        raise RustWorkerError("configured worker binary is missing")
    suffix = ".exe" if os.name == "nt" else ""
    name = _WORKER_NAME + suffix
    roots: list[Path] = []
    target_dir = os.environ.get("CARGO_TARGET_DIR")
    if target_dir:
        roots.append(Path(target_dir) / "release")
        roots.append(Path(target_dir) / "debug")
    repo_root = Path(__file__).resolve().parents[3]
    roots.append(repo_root / "rust" / "target" / "release")
    roots.append(repo_root / "rust" / "target" / "debug")
    for root in roots:
        candidate = root / name
        if candidate.is_file():
            return candidate
    raise RustWorkerError("worker binary not found")


class RustWorker:
    """One spawned worker with a persistent stdout reader thread."""

    def __init__(self, binary: Path | str | None = None) -> None:
        self.binary = Path(binary) if binary is not None else find_worker_binary()
        self._proc = subprocess.Popen(
            [str(self.binary)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self._lines: queue.Queue[str] = queue.Queue()
        self._reader = threading.Thread(target=self._drain, daemon=True)
        self._reader.start()

    def _drain(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        for raw in self._proc.stdout:
            self._lines.put(raw.decode("utf-8", errors="replace").rstrip("\n"))

    def call(self, op: str, **fields: object) -> str:
        if self._proc is None or self._proc.stdin is None:
            raise RustWorkerError("worker is closed")
        request = {"op": op, **fields}
        self._proc.stdin.write((json.dumps(request) + "\n").encode("utf-8"))
        self._proc.stdin.flush()
        try:
            raw = self._lines.get(timeout=_TIMEOUT_S)
        except queue.Empty as exc:
            raise RustWorkerError("worker timed out") from exc
        try:
            reply = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RustWorkerError("worker returned invalid json") from exc
        if not bool(reply.get("ok")):
            raise RustWorkerError("worker rejected request")
        return str(reply["result"])

    def hash_file(self, path: Path | str) -> str:
        return self.call("hash_file", path=str(path))

    def close(self) -> None:
        if self._proc is None:
            return
        if self._proc.stdin is not None:
            self._proc.stdin.close()
        self._proc.wait(timeout=_TIMEOUT_S)
        self._proc = None

    def __enter__(self) -> RustWorker:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
