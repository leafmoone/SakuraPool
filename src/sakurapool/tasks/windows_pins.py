"""Optional live Windows read leases; no persisted verification authority."""

import ctypes
import os

from ..fs_safety import plain_entry
from .store import TaskError


class _PinUnavailable(Exception):
    pass


class _FileId(ctypes.Structure):
    _fields_ = [("volume", ctypes.c_uint64), ("identifier", ctypes.c_ubyte * 16)]


class _Attributes(ctypes.Structure):
    _fields_ = [("attributes", ctypes.c_uint32), ("reparse_tag", ctypes.c_uint32)]


class _WindowsAPI:
    def __init__(self):
        if os.name != "nt":
            raise _PinUnavailable()
        api = ctypes.WinDLL("kernel32", use_last_error=True)
        self.create = api.CreateFileW
        self.create.argtypes = [ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32,
                               ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32,
                               ctypes.c_void_p]
        self.create.restype = ctypes.c_void_p
        self.info = api.GetFileInformationByHandleEx
        self.info.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32]
        self.info.restype = ctypes.c_int
        self.release = api.CloseHandle
        self.release.argtypes = [ctypes.c_void_p]
        self.release.restype = ctypes.c_int

    def open(self, path, directory):
        # GENERIC_READ, FILE_SHARE_READ, OPEN_EXISTING, non-inheritable handle.
        flags = 0x00200000 | (0x02000000 if directory else 0)  # OPEN_REPARSE_POINT/BACKUP_SEMANTICS
        handle = self.create(str(path), 0x80000000, 1, None, 3, flags, None)
        if handle in (None, ctypes.c_void_p(-1).value):
            raise _PinUnavailable()
        return handle

    def identity(self, handle, directory):
        attributes, identity = _Attributes(), _FileId()
        if (not self.info(handle, 9, ctypes.byref(attributes), ctypes.sizeof(attributes))
                or not self.info(handle, 18, ctypes.byref(identity), ctypes.sizeof(identity))
                or attributes.attributes & 0x400 or attributes.reparse_tag
                or bool(attributes.attributes & 0x10) != directory
                or not any(identity.identifier)):
            raise _PinUnavailable()
        return identity.volume, bytes(identity.identifier)

    def close(self, handle):
        if not self.release(handle):
            raise TaskError("PUBLICATION_PIN_CLOSE_FAILED", "finalize")


class _WindowsPins:
    def __init__(self, root):
        try:
            self.api = _WindowsAPI()
        except (OSError, AttributeError):
            raise _PinUnavailable() from None
        self.root = root
        self.entries = []
        self._extra = []
        self.closed = False
        try:
            # The manifest and root are already leased when selecting the fixed inventory.
            self._add(".", True)
            self._add("PUBLICATION.json", False)
            from .verified_session import _inventory

            directories, files = _inventory(root)
            for name in (*directories, *files):
                if name not in (".", "PUBLICATION.json"):
                    self._add(name, name in directories)
            self.check()
        except BaseException as primary:
            try:
                self.close()
            except BaseException:
                raise TaskError("PUBLICATION_PIN_CLOSE_FAILED", "finalize") from None
            if isinstance(primary, (OSError, ValueError, AttributeError, _PinUnavailable)):
                raise _PinUnavailable() from None
            raise

    def _add(self, relative, directory):
        path = plain_entry(self.root / relative, directory=directory)
        handle = self.api.open(path, directory)
        # Track ownership before any identity call can fail.
        self.entries.append((relative, directory, handle, None))
        identity = self.api.identity(handle, directory)
        self.entries[-1] = relative, directory, handle, identity

    def check(self):
        if self.closed:
            raise _PinUnavailable()
        for relative, directory, handle, expected in self.entries:
            path = plain_entry(self.root / relative, directory=directory)
            if self.api.identity(handle, directory) != expected:
                raise _PinUnavailable()
            current = self.api.open(path, directory)
            self._extra.append(current)
            try:
                if self.api.identity(current, directory) != expected:
                    raise _PinUnavailable()
            finally:
                self.api.close(current)
                self._extra.remove(current)

    def close(self):
        self.closed = True
        failed = []
        for entry in reversed(self.entries):
            try:
                self.api.close(entry[2])
            except BaseException:
                failed.append(entry)
        self.entries = list(reversed(failed))
        extra = []
        for handle in self._extra:
            try:
                self.api.close(handle)
            except BaseException:
                extra.append(handle)
        self._extra = extra
        if failed or extra:
            raise TaskError("PUBLICATION_PIN_CLOSE_FAILED", "finalize")
