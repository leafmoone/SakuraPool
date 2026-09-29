"""Shared pytest configuration for the SakuraPool test suite.

R1 environment gate: the Rust worker binary is resolved in exactly one
place (``resolve_r1_worker``). An explicit ``SAKURAPOOL_RUST_WORKER`` is
authoritative - a missing file named by it resolves to ``None`` (the R1
worker tests skip with a report) instead of silently falling back to a
stale binary. The session header always reports the R1 environment state.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

# Build output lives OUTSIDE the repository (fixed CARGO_TARGET_DIR).
R1_WORKER_CANDIDATES = (
    "D:/SakuraTool/SakuraPool-P4-work/rust-target/release/sakurapool-worker.exe",
    "D:/SakuraTool/SakuraPool-P4-work/rust-target/debug/sakurapool-worker.exe",
)


def resolve_r1_worker(environ: dict[str, str] | None = None) -> str | None:
    """Resolve the sakurapool-worker binary path, or ``None`` to skip R1.

    ``environ`` is injectable so tests can exercise both branches without
    touching the real environment.
    """
    env = os.environ if environ is None else environ
    explicit = env.get("SAKURAPOOL_RUST_WORKER", "")
    if explicit:
        return explicit if os.path.isfile(explicit) else None
    for candidate in R1_WORKER_CANDIDATES:
        if os.path.isfile(candidate):
            return candidate
    return None


def pytest_report_header(config) -> list[str]:
    path = resolve_r1_worker()
    if path is None:
        return [
            "R1 worker: NOT FOUND - R1 worker tests will be skipped",
            "  build it: cargo build --release with the fixed CARGO_TARGET_DIR "
            "outside the repo (D:\\SakuraTool\\SakuraPool-P4-work\\rust-target), "
            "or set SAKURAPOOL_RUST_WORKER explicitly",
        ]
    try:
        digest = hashlib.sha256(Path(path).read_bytes()).hexdigest()[:16]
    except OSError:
        digest = "unreadable"
    return [f"R1 worker: {path} (sha256:{digest})"]
