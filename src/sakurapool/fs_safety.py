"""Narrow filesystem primitives, no runtime/publication dependency."""

import os
import stat
from pathlib import Path


def plain_entry(path, *, directory=False):
    path = Path(path).absolute()
    if ".." in path.parts:
        raise ValueError("noncanonical path")
    for entry in (path, *path.parents):
        info = entry.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise ValueError("reparse entry")
    mode = path.stat().st_mode
    if not (stat.S_ISDIR(mode) if directory else stat.S_ISREG(mode)):
        raise ValueError("entry type")
    return path


def sync_directory(path):
    """POSIX directory durability; Windows has no portable directory fsync here."""
    if os.name == "nt":
        return False
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return True


def owned_tree_identity(root):
    root = plain_entry(root, directory=True)
    entries = {}
    for path in (root, *root.rglob("*")):
        plain_entry(path, directory=path.is_dir())
        info = path.stat()
        entries[path.relative_to(root)] = (info.st_dev, info.st_ino, path.is_dir())
    return entries


class OwnedStage:
    """Register only entries created exclusively by this builder, never adopt a scan."""

    def __init__(self, root):
        self.root = plain_entry(root, directory=True)
        info = self.root.stat()
        self.entries = {Path("."): (info.st_dev, info.st_ino, True)}

    def check(self):
        info = plain_entry(self.root, directory=True).stat()
        if (info.st_dev, info.st_ino, True) != self.entries[Path(".")]:
            raise ValueError("owned stage replaced")
        for relative, identity in self.entries.items():
            path = self.root / relative
            info = plain_entry(path, directory=identity[2]).stat()
            if (info.st_dev, info.st_ino, identity[2]) != identity:
                raise ValueError("owned stage entry replaced")

    def create(self, relative, *, directory=False):
        self.check()
        relative = Path(relative)
        if relative.is_absolute() or ".." in relative.parts or not relative.parts:
            raise ValueError("owned relative path")
        path = self.root / relative
        if directory:
            path.mkdir()
        else:
            with path.open("xb"):
                pass
        info = path.stat()
        self.entries[relative] = (info.st_dev, info.st_ino, directory)
        self.check()
        return path

    def remove(self, relative):
        self.check()
        relative = Path(relative)
        if relative not in self.entries:
            raise ValueError("entry not owned")
        (self.root / relative).unlink()
        del self.entries[relative]

    def complete(self):
        self.check()
        if owned_tree_identity(self.root) != self.entries:
            raise ValueError("unknown stage entry")


def cleanup_owned_tree(root, entries):
    """Unknown/replaced entries preserve stage; never mask primary error."""
    if entries is None:
        return False
    try:
        if owned_tree_identity(root) != entries:
            return False
        for relative, (_, _, directory) in sorted(
            entries.items(), key=lambda item: len(item[0].parts), reverse=True
        ):
            path = Path(root) / relative
            if directory:
                path.rmdir()
            else:
                path.unlink()
        return True
    except (OSError, ValueError):
        return False
