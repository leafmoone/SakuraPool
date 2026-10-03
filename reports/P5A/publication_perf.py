"""Measure real product build on retained synthetic input, without provider IO."""

import argparse
import ctypes
import inspect
import json
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import sakurapool
from sakurapool.storage.publication import build_publication, load_publication

parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
parser.add_argument("--implementation-tree", required=True)
parser.add_argument("--repository", default=str(Path(__file__).resolve().parents[2]))
parser.add_argument("--runtime", default="D:/SakuraTool/SakuraPool-R2C3-scale-20261002/runtime")
parser.add_argument(
    "--p2-roots", default="D:/SakuraTool/SakuraPool-R2C3-scale-20261002/p2-roots.json"
)
parser.add_argument(
    "--remote-map", default="D:/SakuraTool/SakuraPool-R2C3-scale-20261002/remote-map.jsonl"
)
parser.add_argument("--expected-rids", type=int, default=1002848)
args = parser.parse_args()
repo = Path(args.repository).resolve()


def git(*arguments):
    return subprocess.check_output(["git", "-C", str(repo), *arguments], text=True).strip()


if git("write-tree") != args.implementation_tree:
    raise RuntimeError("index tree differs from supplied implementation tree")
if git("diff", "--name-only", "--", "src", "rust", "pyproject.toml"):
    raise RuntimeError("unstaged product changes invalidate measurement tree")
if git("ls-files", "--others", "--exclude-standard", "--", "src", "rust"):
    raise RuntimeError("untracked product source invalidates measurement tree")
source = Path(sakurapool.__file__).resolve()
if not source.is_relative_to(repo / "src"):
    raise RuntimeError("measurement import is not requested implementation")
output = Path(args.output)
assert not output.exists()
original = sqlite3.connect
stats = {}
trace_counts = {}
phase = "preflight"
phases = {}
phase_start = time.perf_counter()


def trace_sql(sql):
    key = (phase, sql.split()[0].upper())
    trace_counts[key] = trace_counts.get(key, 0) + 1


class MeasuredConnection(sqlite3.Connection):
    def executemany(self, sql, parameters):
        start = time.perf_counter()
        try:
            return super().executemany(sql, parameters)
        finally:
            key = (phase, "EXECUTEMANY " + sql)
            count, seconds = stats.get(key, (0, 0.0))
            stats[key] = (count + 1, seconds + time.perf_counter() - start)

    def execute(self, sql, parameters=()):
        start = time.perf_counter()
        try:
            return super().execute(sql, parameters)
        finally:
            key = (phase, sql)
            count, seconds = stats.get(key, (0, 0.0))
            stats[key] = (count + 1, seconds + time.perf_counter() - start)


def connect(*a, **kw):
    kw.setdefault("factory", MeasuredConnection)
    db = original(*a, **kw)
    db.set_trace_callback(trace_sql)
    return db


source_lines, first_line = inspect.getsourcelines(build_publication)
markers = [
    ("mapping", "for r in map_rows(remote_map)"),
    ("hash_sidecar", 'temp = owned.create("hashes.sqlite")'),
    ("fetchability", "with closing(readonly(cat))"),
    ("manifest", "m = dict("),
    ("build_full_verify", "with load_publication(stage"),
]
switches = {}
marker_lines = []
for name, marker in markers:
    matches = [first_line + i for i, line in enumerate(source_lines) if marker in line]
    if len(matches) != 1:
        raise RuntimeError(f"phase marker {name}: expected exactly one, got {matches}")
    switches[matches[0]] = name
    marker_lines.append(matches[0])
if marker_lines != sorted(set(marker_lines)):
    raise RuntimeError("phase marker order mismatch")
transitions = []


def stage_trace(frame, event, arg):
    global phase, phase_start
    if frame.f_code is build_publication.__code__ and event == "line":
        new = switches.get(frame.f_lineno)
        if new and new != phase:
            transitions.append(new)
            now = time.perf_counter()
            phases[phase] = phases.get(phase, 0) + now - phase_start
            phase, phase_start = new, now
    return stage_trace if frame.f_code is build_publication.__code__ else None


sqlite3.connect = connect
sys.settrace(stage_trace)
start = phase_start = time.perf_counter()
m = build_publication(args.runtime, args.p2_roots, args.remote_map, output)
end = time.perf_counter()
wall = end - start
sys.settrace(None)
phases[phase] = phases.get(phase, 0) + end - phase_start
if transitions != [name for name, _ in markers]:
    raise RuntimeError(f"actual phase transitions missing/reordered: {transitions}")
if abs(sum(phases.values()) - wall) > 1e-6:
    raise RuntimeError("build phase wall does not cover whole build wall")
phase = "standalone_full_verify"
start = time.perf_counter()
with load_publication(output, full_verify=True) as pub:
    assert pub.full_verified and pub.runtime.rid_count == args.expected_rids
verify = time.perf_counter() - start
phases["standalone_full_verify"] = verify


class MemoryCounters(ctypes.Structure):
    _fields_ = [("cb", ctypes.c_ulong), ("PageFaultCount", ctypes.c_ulong)] + [
        (name, ctypes.c_size_t)
        for name in (
            "PeakWorkingSetSize",
            "WorkingSetSize",
            "QuotaPeakPagedPoolUsage",
            "QuotaPagedPoolUsage",
            "QuotaPeakNonPagedPoolUsage",
            "QuotaNonPagedPoolUsage",
            "PagefileUsage",
            "PeakPagefileUsage",
        )
    ]


memory = MemoryCounters()
memory.cb = ctypes.sizeof(memory)
get_process = ctypes.windll.kernel32.GetCurrentProcess
get_process.restype = ctypes.c_void_p
get_memory = ctypes.windll.psapi.GetProcessMemoryInfo
get_memory.argtypes = [ctypes.c_void_p, ctypes.POINTER(MemoryCounters), ctypes.c_ulong]
if not get_memory(get_process(), ctypes.byref(memory), memory.cb):
    raise OSError("process peak working set measurement failed")
assert memory.PeakWorkingSetSize > 0
if git("write-tree") != args.implementation_tree or git(
    "diff", "--name-only", "--", "src", "rust", "pyproject.toml"
):
    raise RuntimeError("product tree drifted during measurement")
print(
    json.dumps(
        {
            "source": sakurapool.__file__,
            "implementation_tree": args.implementation_tree,
            "repository": str(repo),
            "base_head": git("rev-parse", "HEAD"),
            "phase_definition": (
                "contiguous instrumented build wall; transitions before mapping/hash/"
                "fetchability/manifest/build_verify; preflight includes source checks/"
                "copy/temp index; final phase includes fsync/publish; SQL execute time "
                "excludes cursor iteration; executemany counts Python calls, "
                "trace counts SQLite statements"
            ),
            "build_wall": wall,
            "full_verify_wall": verify,
            "peak_working_set_bytes": memory.PeakWorkingSetSize,
            "phase_wall": phases,
            "objects": m["object_count"],
            "rids": m["rid_count"],
            "sql_execute": [
                {"phase": p, "sql": q, "calls": c, "seconds": s} for (p, q), (c, s) in stats.items()
            ],
            "sqlite_trace_counts": [
                {"phase": p, "operation": q, "count": c} for (p, q), c in trace_counts.items()
            ],
        },
        sort_keys=True,
    ),
    flush=True,
)
