"""Narrow regression coverage for task path/handle file identity comparisons.

Run with the existing Pool Python environment, for example:
    python -B -m unittest discover -s regression_tests -p test_task_file_identity.py -v

No existing asset, timestamp or lock metadata is repaired by this suite.
The hot-journal test exercises SQLite rollback only on a newly owned TaskDB.
All mutation tests use disposable owned files; the Windows mismatch is also
injected synthetically so compatibility and same-API ctime protection are
exercised on every platform.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

# Tests must use this checkout, not an unrelated installed Pool/compiler.
SOURCE = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SOURCE))
from sakurapool.tasks import store  # noqa: E402


def changed(info, **updates):
    fields = {name: getattr(info, name) for name in (
        "st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns", "st_nlink", "st_mode"
    )}
    fields.update(updates)
    return SimpleNamespace(**fields)


class TaskFileIdentityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="pool-file-identity-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.lock = self.root / "runner.lock"
        self.lock.write_bytes(b"T")
        self.main = self.root / "task.sqlite"
        self.journal = Path(str(self.main) + "-journal")
        self.main.write_bytes(b"owned database bytes")
        self.journal.write_bytes(b"owned journal bytes")
        self.capacity = SimpleNamespace(task_db_bytes=1024, task_journal_bytes=1024)

    def assert_task_error(self, code, call):
        with self.assertRaises(store.TaskError) as raised:
            call()
        self.assertEqual(raised.exception.code, code)
        return raised.exception

    def test_file_signature_keeps_all_five_fields(self):
        info = SimpleNamespace(st_dev=1, st_ino=2, st_size=3, st_mtime_ns=4, st_ctime_ns=5)
        self.assertEqual(store._file_signature(info), (1, 2, 3, 4, 5))

    def test_normal_runner_lock_and_release(self):
        for _ in range(3):
            with store._runner_lock(self.root) as check:
                check()
        self.assertEqual(self.lock.read_bytes(), b"T")

    def test_independent_process_cannot_acquire_owned_runner_lock(self):
        code = (
            "import sys; from pathlib import Path; sys.path.insert(0,sys.argv[1]); "
            "from sakurapool.tasks.store import _runner_lock,TaskError; "
            "directory=Path(sys.argv[2]);\n"
            "try:\n"
            "    with _runner_lock(directory): print('ACQUIRED')\n"
            "except TaskError as error:\n"
            "    print(error.code); sys.exit(2)\n"
        )
        with store._runner_lock(self.root):
            child = subprocess.run(
                [sys.executable, "-I", "-B", "-c", code, str(SOURCE), str(self.root)],
                capture_output=True, text=True, timeout=15, check=False,
            )
            self.assertEqual(child.returncode, 2, child.stderr)
            self.assertEqual(child.stdout.strip(), "RUNNER_BUSY")
        with store._runner_lock(self.root) as check:
            check()

    def test_runner_accepts_stable_different_cross_api_ctime(self):
        real_fstat = os.fstat
        with patch.object(store.os, "fstat", side_effect=lambda fd: changed(
                real_fstat(fd), st_ctime_ns=123456789)):
            with store._runner_lock(self.root) as check:
                check()

    def test_runner_still_rejects_every_cross_api_shared_field_mismatch(self):
        real_fstat = os.fstat
        for field in ("st_dev", "st_ino", "st_size", "st_mtime_ns"):
            with self.subTest(field=field):
                def fstat(fd):
                    info = real_fstat(fd)
                    return changed(info, **{field: getattr(info, field) + 1})
                def acquire():
                    with store._runner_lock(self.root):
                        self.fail("Cross-API identity mismatch was accepted")
                with patch.object(store.os, "fstat", side_effect=fstat):
                    self.assert_task_error("RUNNER_LOCK_INVALID", acquire)

    def test_runner_rejects_same_api_fstat_ctime_change(self):
        real_fstat = os.fstat
        state = {"changed": False}
        def fstat(fd):
            return changed(real_fstat(fd), st_ctime_ns=2 if state["changed"] else 1)
        with patch.object(store.os, "fstat", side_effect=fstat):
            with store._runner_lock(self.root) as check:
                state["changed"] = True
                self.assert_task_error("RUNNER_LOCK_INVALID", check)

    def test_runner_rejects_same_api_path_ctime_change(self):
        real_stat = Path.stat
        state = {"changed": False}
        def stat(path, *args, **kwargs):
            info = real_stat(path, *args, **kwargs)
            if path == self.lock:
                return changed(info, st_ctime_ns=2 if state["changed"] else 1)
            return info
        with patch.object(Path, "stat", stat):
            with store._runner_lock(self.root) as check:
                state["changed"] = True
                self.assert_task_error("RUNNER_LOCK_INVALID", check)

    def test_runner_rejects_same_content_path_replacement(self):
        # Windows normally forbids replacing an open CRT file. Simulate the
        # pathname resolving to a real, different owned inode with identical
        # bytes/size/mtime; this must not be accepted as the held lock.
        replacement = self.root / "replacement.lock"
        replacement.write_bytes(b"T")
        original = self.lock.stat()
        os.utime(replacement, ns=(original.st_atime_ns, original.st_mtime_ns))
        self.assertNotEqual(replacement.stat().st_ino, original.st_ino)
        real_plain = store.plain_entry
        state = {"replaced": False}
        def plain(path, **kwargs):
            return real_plain(replacement if Path(path) == self.lock and state["replaced"] else path, **kwargs)
        with patch.object(store, "plain_entry", side_effect=plain):
            with store._runner_lock(self.root) as check:
                state["replaced"] = True
                self.assert_task_error("RUNNER_LOCK_INVALID", check)
        self.assertEqual(self.lock.read_bytes(), replacement.read_bytes())

    def test_runner_actual_held_path_replace_is_os_blocked_or_identity_rejected(self):
        replacement = self.root / "new-owned-runner.lock"
        replacement.write_bytes(b"T")
        before = self.lock.stat()
        os.utime(replacement, ns=(before.st_atime_ns, before.st_mtime_ns))
        original_signature = store._file_signature(before)
        original_hash = hashlib.sha256(self.lock.read_bytes()).digest()
        with store._runner_lock(self.root) as check:
            try:
                os.replace(replacement, self.lock)
            except PermissionError:
                # This is a real OS refusal, not an adapter identity rejection.
                self.assertEqual(os.name, "nt")
                self.assertTrue(replacement.exists())
                self.assertEqual(store._file_signature(self.lock.stat()), original_signature)
                check()
                print("held-lock replacement: Windows OS refused; original identity retained")
            else:
                self.assertFalse(replacement.exists())
                self.assertNotEqual(self.lock.stat().st_ino, before.st_ino)
                self.assert_task_error("RUNNER_LOCK_INVALID", check)
                print("held-lock replacement: OS replaced path; runner identity check rejected")
        # Windows' active byte-range lock also prevents a second handle reading
        # byte T. Check the unchanged content after the real lock is released.
        self.assertEqual(hashlib.sha256(self.lock.read_bytes()).digest(), original_hash)

    def test_runner_rejects_truncation_or_content_mtime_change(self):
        real_stat = Path.stat
        for field, value in (("st_size", 0), ("st_mtime_ns", 17)):
            with self.subTest(field=field):
                state = {"changed": False}
                def stat(path, *args, **kwargs):
                    info = real_stat(path, *args, **kwargs)
                    return changed(info, **{field: value}) if path == self.lock and state["changed"] else info
                with patch.object(Path, "stat", stat):
                    with store._runner_lock(self.root) as check:
                        state["changed"] = True
                        self.assert_task_error("RUNNER_LOCK_INVALID", check)

    def test_runner_rejects_invalid_or_truncated_marker(self):
        for content in (b"", b"X", b"TT"):
            with self.subTest(content=content):
                self.lock.write_bytes(content)
                def acquire():
                    with store._runner_lock(self.root):
                        self.fail("Invalid marker was accepted")
                self.assert_task_error("RUNNER_LOCK_INVALID", acquire)

    def test_recovery_snapshot_retains_bytes_and_full_path_fingerprint(self):
        contents, fingerprint = store._recovery_snapshot(self.main, self.capacity)
        self.assertEqual(contents, [self.main.read_bytes(), self.journal.read_bytes()])
        self.assertEqual(fingerprint[0], [store._file_signature(p.stat()) for p in (self.main, self.journal)])
        self.assertEqual(fingerprint[1], [hashlib.sha256(data).digest() for data in contents])

    def test_recovery_accepts_stable_different_cross_api_ctime(self):
        real_fstat = os.fstat
        with patch.object(store.os, "fstat", side_effect=lambda fd: changed(
                real_fstat(fd), st_ctime_ns=123456789)):
            contents, fingerprint = store._recovery_snapshot(self.main, self.capacity)
        self.assertEqual(contents, [self.main.read_bytes(), self.journal.read_bytes()])
        self.assertEqual(fingerprint[0], [store._file_signature(p.stat()) for p in (self.main, self.journal)])

    def test_recovery_still_rejects_every_cross_api_shared_field_mismatch(self):
        real_fstat = os.fstat
        for field in ("st_dev", "st_ino", "st_size", "st_mtime_ns"):
            with self.subTest(field=field):
                def fstat(fd):
                    info = real_fstat(fd)
                    return changed(info, **{field: getattr(info, field) + 1})
                with patch.object(store.os, "fstat", side_effect=fstat):
                    self.assert_task_error("TASKDB_RECOVERY_RACE", lambda: store._recovery_snapshot(self.main, self.capacity))

    def test_recovery_rejects_same_api_fstat_ctime_change_during_read(self):
        real_fstat = os.fstat
        calls = {"count": 0}
        def fstat(fd):
            calls["count"] += 1
            return changed(real_fstat(fd), st_ctime_ns=calls["count"])
        with patch.object(store.os, "fstat", side_effect=fstat):
            error = self.assert_task_error("TASKDB_RECOVERY_RACE", lambda: store._recovery_snapshot(self.main, self.capacity))
        self.assertTrue(error.recoverable)

    def recovery_read_hook(self, after_read):
        real_open = Path.open
        target = self.main
        class Stream:
            def __init__(self, inner):
                self.inner = inner
            def __enter__(self):
                self.inner.__enter__()
                return self
            def __exit__(self, *args):
                return self.inner.__exit__(*args)
            def fileno(self):
                return self.inner.fileno()
            def read(self, count):
                data = self.inner.read(count)
                after_read()
                return data
        def open_file(path, *args, **kwargs):
            inner = real_open(path, *args, **kwargs)
            return Stream(inner) if path == target and args == ("rb",) else inner
        return patch.object(Path, "open", open_file)

    def test_recovery_rejects_same_size_content_change_during_read(self):
        original = self.main.stat()
        def mutate():
            self.main.write_bytes(b"X" * original.st_size)
            os.utime(self.main, ns=(original.st_atime_ns, original.st_mtime_ns + 1_000_000_000))
        with self.recovery_read_hook(mutate):
            self.assert_task_error("TASKDB_RECOVERY_RACE", lambda: store._recovery_snapshot(self.main, self.capacity))

    def test_recovery_rejects_truncation_during_read(self):
        with self.recovery_read_hook(lambda: self.main.write_bytes(b"")):
            self.assert_task_error("TASKDB_RECOVERY_RACE", lambda: store._recovery_snapshot(self.main, self.capacity))

    def test_recovery_rejects_same_content_path_replacement_after_read(self):
        replacement = self.root / "replacement.sqlite"
        replacement.write_bytes(self.main.read_bytes())
        original = self.main.stat()
        os.utime(replacement, ns=(original.st_atime_ns, original.st_mtime_ns))
        real_open = Path.open
        target = self.main
        class Stream:
            def __init__(self, inner):
                self.inner = inner
            def __enter__(self):
                self.inner.__enter__()
                return self.inner
            def __exit__(self, *args):
                result = self.inner.__exit__(*args)
                os.replace(replacement, target)
                return result
        def open_file(path, *args, **kwargs):
            inner = real_open(path, *args, **kwargs)
            return Stream(inner) if path == target and args == ("rb",) else inner
        with patch.object(Path, "open", open_file):
            self.assert_task_error("TASKDB_RECOVERY_RACE", lambda: store._recovery_snapshot(self.main, self.capacity))

    def test_real_hot_sqlite_task_readonly_snapshot_and_writable_rollback(self):
        # TaskDB.create is the public local storage constructor. No synthetic
        # schema, hand-built journal, publication fetch or download is involved.
        from sakurapool.runtime import RuntimeQuerySpec
        from sakurapool.tasks.plan import Selection, SelectedRecord, normalize_query

        directory = self.root / "hot-task"
        header = {
            "publication_digest": "a" * 64,
            "snapshot_id": "b" * 64,
            "query": normalize_query(RuntimeQuerySpec()),
            "selection": Selection("first", 2).header(),
            "metadata": False,
        }
        rows = [SelectedRecord(index, f"{index + 1:032x}", "owned", "tiny", str(index + 1))
                for index in range(2)]
        with store.TaskDB.create(directory, None, header, rows,
                                 publication_path=self.root / "unused-publication",
                                 filename_prefix="hot") as task:
            # Ensure the child updates many existing pages, not just newly
            # allocated pages. The cached identity/plan are real TaskDB values.
            with task.transaction() as db:
                db.executemany("INSERT INTO meta(key,value) VALUES(?,?)",
                               [(f"padding-{index}", '"' + "A" * 4096 + '"')
                                for index in range(96)])
            baseline = task.inspect()
            frozen = task.validate_plan()
        unknown = directory / "unknown-user-file.bin"
        unknown.write_bytes(b"must remain untouched")
        main = directory / "task.sqlite"
        journal = Path(str(main) + "-journal")
        baseline_bytes = main.read_bytes()
        code = '''import os,sqlite3,sys
from pathlib import Path
path=Path(sys.argv[1])
db=sqlite3.connect(path, isolation_level=None)
assert db.execute("PRAGMA journal_mode=DELETE").fetchone()[0]=="delete"
db.execute("PRAGMA synchronous=FULL")
db.execute("PRAGMA cache_size=2")
db.execute("PRAGMA cache_spill=ON")
db.execute("BEGIN IMMEDIATE")
db.execute("UPDATE meta SET value='{}' WHERE key='header'")
db.execute("UPDATE meta SET value='\\\"RUNNING\\\"' WHERE key='state'")
db.execute("UPDATE meta SET value='\\\"CANCEL\\\"' WHERE key='request'")
for index in range(96):
    db.execute("UPDATE meta SET value=? WHERE key=?", ('"'+'B'*4096+'"',f'padding-{index}'))
journal=Path(str(path)+'-journal')
assert journal.read_bytes()[:8]==b"\\xd9\\xd5\\x05\\xf9\\x20\\xa1\\x63\\xd7"
os._exit(73)
'''
        child = subprocess.run([sys.executable, "-I", "-B", "-c", code, str(main)],
                               capture_output=True, text=True, timeout=20, check=False)
        self.assertEqual(child.returncode, 73, child.stderr)
        self.assertTrue(journal.is_file())
        self.assertEqual(journal.read_bytes()[:8], b"\xd9\xd5\x05\xf9\x20\xa1\x63\xd7")
        self.assertGreater(len(journal.read_bytes()), 4096)
        self.assertNotEqual(main.read_bytes(), baseline_bytes,
                            "No pages spilled; this would not exercise real hot rollback")
        def original_files():
            return {str(path): (store._file_signature(path.stat()),
                                hashlib.sha256(path.read_bytes()).hexdigest())
                    for path in (main, journal, unknown, directory / "runner.lock")}
        before_readonly = original_files()
        with store.TaskDB(directory, readonly=True) as task:
            self.assertIsNotNone(task._shadow, "Must use a recovered readonly snapshot")
            self.assertEqual(task.inspect(), baseline)
            self.assertEqual(task.validate_plan(), frozen)
            self.assertEqual(task.db.execute("PRAGMA quick_check").fetchone()[0], "ok")
            self.assertEqual(task.db.execute("SELECT value FROM meta WHERE key='padding-0'").fetchone()[0],
                             '"' + "A" * 4096 + '"')
            self.assertEqual(original_files(), before_readonly)
        self.assertEqual(original_files(), before_readonly,
                         "Readonly recovery must not repair the original DB or journal")
        with store.TaskDB(directory) as task:
            self.assertIsNone(task._shadow, "Writable recovery must use SQLite on the original")
            self.assertEqual(task.inspect(), baseline)
            self.assertEqual(task.validate_plan(), frozen)
            self.assertEqual(task.db.execute("PRAGMA quick_check").fetchone()[0], "ok")
            self.assertEqual(task.db.execute("SELECT value FROM meta WHERE key='padding-95'").fetchone()[0],
                             '"' + "A" * 4096 + '"')
        self.assertFalse(journal.exists(), "SQLite DELETE journal should be finalized by rollback")
        self.assertEqual(main.read_bytes(), baseline_bytes)
        self.assertEqual(unknown.read_bytes(), b"must remain untouched")
        with store.TaskDB(directory, readonly=True) as task:
            self.assertEqual(task.inspect(), baseline)
            self.assertEqual(task.validate_plan(), frozen)

    def test_recovery_rejects_same_api_path_ctime_change_after_read(self):
        real_stat = Path.stat
        state = {"changed": False}
        def stat(path, *args, **kwargs):
            info = real_stat(path, *args, **kwargs)
            if path == self.main:
                return changed(info, st_ctime_ns=2 if state["changed"] else 1)
            return info
        with patch.object(Path, "stat", stat), self.recovery_read_hook(lambda: state.update(changed=True)):
            self.assert_task_error("TASKDB_RECOVERY_RACE", lambda: store._recovery_snapshot(self.main, self.capacity))


if __name__ == "__main__":
    unittest.main()
