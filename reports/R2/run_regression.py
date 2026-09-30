"""One formal regression with durable terminal evidence; never retries.

Recovery runner for an earlier tool call whose terminal result was not received.
Writes one stdout/stderr log and one exit record, no hashes or new environment.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic


def main():
    repo = Path(__file__).absolute().parents[2]
    evidence = repo / "reports/R2"
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--final",
        action="store_true",
        help="certify the corrected tree, retaining the first failure",
    )
    args = parser.parse_args()
    suffix = "-final" if args.final else ""
    log = evidence / f"full-regression{suffix}.log"
    result = evidence / f"full-regression{suffix}-result.json"
    if log.exists() or result.exists():
        raise RuntimeError("existing formal evidence must not be overwritten")
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env["SAKURAPOOL_RUST_WORKER"] = (
        "D:/SakuraTool/SakuraPool-Fix3-20260930a/rust-target/release/sakurapool-worker.exe"
    )
    command = [sys.executable, "-m", "pytest", "-q", "-ra"]
    started = datetime.now(timezone.utc).isoformat()
    clock = monotonic()
    with log.open("xb") as stream:
        stream.write(
            ("COMMAND=" + json.dumps(command) + "\nSTARTED_UTC=" + started + "\n").encode()
        )
        stream.flush()
        child = subprocess.Popen(
            command, cwd=repo, env=env, stdout=stream, stderr=subprocess.STDOUT
        )
        print(
            json.dumps(
                {"started_utc": started, "pid": child.pid, "command": command, "log": str(log)}
            ),
            flush=True,
        )
        status = child.wait()
        elapsed = monotonic() - clock
        stream.write(f"\nFORMAL_EXIT_CODE={status}\nELAPSED_SECONDS={elapsed:.2f}\n".encode())
        stream.flush()
        os.fsync(stream.fileno())
    record = {
        "command": command,
        "cwd": str(repo),
        "started_utc": started,
        "finished_utc": datetime.now(timezone.utc).isoformat(),
        "pid": child.pid,
        "exit_code": status,
        "elapsed_seconds": round(elapsed, 2),
        "pythonpath": "unset",
        "worker": env["SAKURAPOOL_RUST_WORKER"],
        "log": str(log),
        "retries": 0,
    }
    with result.open("xb") as stream:
        stream.write((json.dumps(record, indent=2) + "\n").encode())
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps(record), flush=True)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
