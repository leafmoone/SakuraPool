"""No-replace publication and directory persistence barriers.

POSIX errors propagate: callers must retain publication intent after a move.
Windows keeps no-replace/process-crash recovery, but Python offers no portable
parent-directory fsync there; power-loss durability is not claimed on Windows.
"""

import ctypes
import os
import sys
from pathlib import Path

from .fs_safety import sync_directory


def publish_noreplace(stage: Path, final: Path) -> None:
    """Atomic no-replace move; persist namespace changes before returning."""
    stage, final = Path(stage), Path(final)
    if stage.is_dir():
        sync_directory(stage)
    sync_directory(stage.parent)
    if final.parent != stage.parent:
        sync_directory(final.parent)
    if os.name == "nt":
        os.rename(stage, final)
    elif sys.platform == "linux":
        libc = ctypes.CDLL(None, use_errno=True)
        libc.renameat2.argtypes = (
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        )
        libc.renameat2.restype = ctypes.c_int
        if libc.renameat2(-100, os.fsencode(stage), -100, os.fsencode(final), 1):
            raise OSError(ctypes.get_errno(), "atomic no-replace publish failed")
    else:
        raise RuntimeError("atomic no-replace directory publish is unavailable")
    # Destination first: no journal acknowledgement before either namespace
    # barrier. On error the move may have happened; never undo or replace it.
    sync_directory(final.parent)
    if stage.parent != final.parent:
        sync_directory(stage.parent)
