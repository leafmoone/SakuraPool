"""Private, uninterrupted create/run ownership of a fully verified publication."""

import os
import re
from pathlib import Path
from threading import get_ident

from ..fs_safety import plain_entry
from ..storage.publication import bounded
from .store import TaskError


def _fingerprint(root):
    """Fixed v2 inputs, including inode and change time, never an unbounded walk."""
    manifest = bounded(root / "PUBLICATION.json")
    snapshot = manifest.get("snapshot_id") if type(manifest) is dict else None
    if type(snapshot) is not str or re.fullmatch("[0-9a-f]{64}", snapshot) is None:
        raise ValueError("publication session identity")
    pinned = "runtime/snapshots/" + snapshot
    directories = (".", "runtime", "runtime/snapshots", pinned)
    files = ("PUBLICATION.json", "READY", "remote_objects.sqlite", "image_sha256.npy",
             "runtime/current.json", *(pinned + "/" + name for name in
             ("SNAPSHOT.json", "READY", "OWNER.json", "STAGE.txt", "catalog.sqlite",
              "bitmaps.sqlite", "locations.npy")))
    result = []
    for relative in (*directories, *files):
        path = plain_entry(root / relative, directory=relative in directories)
        info = path.stat()
        result.append((relative, info.st_dev, info.st_ino, info.st_mode, info.st_nlink,
                       info.st_size, info.st_mtime_ns, info.st_ctime_ns))
        if relative.endswith(".sqlite"):
            for suffix in ("-journal", "-wal", "-shm"):
                if os.path.lexists(str(path) + suffix):
                    raise ValueError("mutable publication sidecar")
    return tuple(result)


class _VerifiedPublication:
    """Not a reusable token: created and closed inside create_and_run_task only."""

    def __init__(self, root, capacity, loader):
        self.owner = get_ident()
        self.root = plain_entry(root, directory=True)
        self.capacity = capacity
        self._closed = False
        self._files = _fingerprint(self.root)
        self.publication = loader(self.root, full_verify=True)
        pub = self.publication
        self._objects = (pub, pub.runtime, pub.catalog, pub.hashes,
                         pub.runtime._catalog, pub.runtime._bitmaps, pub.runtime._locations)
        self._identity = (pub.content_digest, pub.runtime.snapshot_id)
        try:
            self.check(capacity, self.root)
        except BaseException as primary:
            self._close(primary)
            raise

    def check(self, capacity, root):
        try:
            pub = self.publication
            if (self._closed or get_ident() != self.owner or capacity != self.capacity
                    or Path(root).absolute() != self.root or pub._closed
                    or pub.full_verified is not True or pub.runtime._closed
                    or pub.root != self.root
                    or (pub.content_digest, pub.runtime.snapshot_id) != self._identity
                    or any(left is not right for left, right in zip(self._objects,
                        (pub, pub.runtime, pub.catalog, pub.hashes, pub.runtime._catalog,
                         pub.runtime._bitmaps, pub.runtime._locations)))
                    or pub.hashes._mmap.closed or pub.runtime._locations._mmap.closed
                    or _fingerprint(self.root) != self._files):
                raise ValueError("publication session changed")
            for db in (pub.catalog, pub.runtime._catalog, pub.runtime._bitmaps):
                db.execute("SELECT 1").fetchone()
        except Exception:
            raise TaskError("PUBLICATION_SESSION_INVALID", "preflight") from None
        return pub

    def __enter__(self):
        return self

    def _close(self, primary=None):
        self._closed = True
        try:
            self.publication.close()
        except BaseException:
            if primary is None:
                raise
            primary.task_secondary = (
                *getattr(primary, "task_secondary", ()), "PUBLICATION_CLOSE_FAILED")

    def __exit__(self, exc_type, primary, traceback):
        self._close(primary)
