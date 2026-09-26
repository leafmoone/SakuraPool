"""Offline 10k synthetic benchmark; timings include validation and durable writes."""

import ctypes
import io
import json
import os
import platform
import sys
import tarfile
import tempfile
from pathlib import Path

import pyarrow

from sakurapool.indexer import scan


def peak_rss_bytes():
    if os.name == "nt":
        from ctypes import wintypes

        class Counters(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + [
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

        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.GetCurrentProcess.restype = wintypes.HANDLE
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(Counters),
            wintypes.DWORD,
        ]
        if not psapi.GetProcessMemoryInfo(
            kernel.GetCurrentProcess(), ctypes.byref(counters), counters.cb
        ):
            raise ctypes.WinError(ctypes.get_last_error())
        return counters.PeakWorkingSetSize
    import resource

    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


def main():
    with tempfile.TemporaryDirectory(prefix="sakurapool-benchmark-") as temporary:
        root = Path(temporary)
        source = root / "input"
        source.mkdir()
        with tarfile.open(source / "synthetic.tar", "w") as archive:
            for i in range(10000):
                for suffix, data in (
                    ("jpg", b"synthetic-not-decodable-image"),
                    ("json", b'{"tags":["synthetic"],"text":"benchmark"}'),
                ):
                    info = tarfile.TarInfo(f"{i:05d}.{suffix}")
                    info.size = len(data)
                    archive.addfile(info, io.BytesIO(data))
        result = scan(source, root / "output")
        assert result["objects"] == 10000 and result["errors"] == 0
        print(
            json.dumps(
                {
                    **result,
                    "peak_rss_bytes": peak_rss_bytes(),
                    "python": sys.version,
                    "pyarrow": pyarrow.__version__,
                    "platform": platform.platform(),
                    "fixture": "10000 synthetic pairs",
                    "rss_scope": "process peak including fixture generation and imports",
                    "total_scope": "scan including input SHA256, excluding fixture generation",
                },
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    main()
