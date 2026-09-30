"""Real-pipe bounded stdout / slow consumer / shutdown / exit tests.

A small Python child substitutes only the process transport; no Rust binary or
external service is needed. Constructor, pipe threads and lifecycle are product
RustWorker code. Synthetic files live in pytest's NEW temp directories.
"""

import json
import subprocess
import sys
import threading
import time

import pytest

from sakurapool.storage import rust_bridge as bridge
from sakurapool.storage.rust_bridge import RustWorker, RustWorkerError

_REAL_POPEN = subprocess.Popen
_READY = json.dumps({
    "type": "ready", "protocol_version": 1,
    "worker_version": "fixture", "capabilities": ["fixture"],
})


def spawn_fixture(monkeypatch, code, *, timeout=1.0):
    children = []

    def spawn(_argv, **kwargs):
        child = _REAL_POPEN([sys.executable, "-u", "-c", code], **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(bridge.subprocess, "Popen", spawn)
    worker = RustWorker(sys.executable, timeout_s=timeout)
    return worker, children[0]


def ready_code():
    return f"import sys, json, time\nsys.stdin.readline()\nprint({_READY!r}, flush=True)\n"


def wait_for_full(worker):
    deadline = time.monotonic() + 3
    while not worker._lines.full() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert worker._lines.full()
    assert worker._lines.maxsize == bridge._STDOUT_QUEUE_LINES
    assert worker._reader.is_alive()


def assert_reaped_and_closed(worker, child):
    assert worker.pid is None
    assert child.poll() is not None
    assert not worker._reader.is_alive()
    assert not worker._stderr_reader.is_alive()
    assert child.stdin.closed and child.stdout.closed and child.stderr.closed


def test_slow_consumer_bounded_queue_preserves_order(monkeypatch):
    code = ready_code() + "for i in range(200):\n print(json.dumps({'i': i}), flush=True)\n"
    worker, child = spawn_fixture(monkeypatch, code)
    try:
        wait_for_full(worker)
        assert worker._lines.qsize() == bridge._STDOUT_QUEUE_LINES
        for i in range(200):
            assert worker.read_raw() == {"i": i}
            assert worker._lines.qsize() <= bridge._STDOUT_QUEUE_LINES
        with pytest.raises(RustWorkerError, match="worker exited"):
            worker.read_raw()
    finally:
        worker.close()
    assert_reaped_and_closed(worker, child)


@pytest.mark.parametrize("action", ["cancel", "close"])
def test_shutdown_while_producer_blocked_on_full_queue(monkeypatch, action):
    code = ready_code() + (
        "line = json.dumps({'payload': 'x' * 32000})\n"
        "for i in range(64):\n print(line, flush=True)\n"
        "sys.stdin.read()\n"
    )
    worker, child = spawn_fixture(monkeypatch, code, timeout=2)
    wait_for_full(worker)
    shutdown = threading.Thread(target=getattr(worker, action))
    shutdown.start()
    shutdown.join(timeout=6)
    assert not shutdown.is_alive(), "shutdown must release queue producer then kill/wait/close"
    assert_reaped_and_closed(worker, child)
    worker.close()  # idempotent
    worker.cancel()


def test_cancel_unblocks_waiting_consumer(monkeypatch):
    worker, child = spawn_fixture(monkeypatch, ready_code() + "sys.stdin.read()\n", timeout=2)
    errors = []

    def receive():
        try:
            worker.read_raw()
        except RustWorkerError as error:
            errors.append(str(error))

    consumer = threading.Thread(target=receive)
    consumer.start()
    worker.cancel()
    consumer.join(timeout=3)
    assert not consumer.is_alive()
    assert errors == ["worker is closed"]
    assert_reaped_and_closed(worker, child)


@pytest.mark.parametrize("action", ["close", "cancel"])
def test_shutdown_releases_blocked_stdin_writer(monkeypatch, action):
    worker, child = spawn_fixture(monkeypatch, ready_code() + "time.sleep(30)\n", timeout=1)
    errors = []

    def flood_stdin():
        try:
            while True:
                worker.send_raw(b"x" * 60000 + b"\n")
        except RustWorkerError as error:
            errors.append(str(error))

    writer = threading.Thread(target=flood_stdin, daemon=True)
    writer.start()
    assert worker._writing.wait(timeout=2)
    shutdown = threading.Thread(target=getattr(worker, action), daemon=True)
    shutdown.start()
    shutdown.join(timeout=4)
    if shutdown.is_alive():  # keep a failing test from leaking the fixture process
        child.kill()
        child.wait(timeout=2)
        pytest.fail("shutdown deadlocked closing a pipe with a blocked writer")
    writer.join(timeout=2)
    assert not writer.is_alive()
    assert errors
    assert_reaped_and_closed(worker, child)


def test_eof_race_delivers_final_queued_line(monkeypatch):
    import queue

    worker, child = spawn_fixture(monkeypatch, ready_code() + "sys.stdin.read()\n")
    actual = worker._lines

    class FinalLineRace:
        def get(self, timeout):
            actual.put(b'{"final": true}\n')
            worker._reader_done.set()
            raise queue.Empty

        def get_nowait(self):
            return actual.get_nowait()

    worker._lines = FinalLineRace()
    try:
        assert worker.read_raw() == {"final": True}
    finally:
        worker._lines = actual
        worker.cancel()
    assert_reaped_and_closed(worker, child)


def test_eof_reports_exit_without_waiting_for_long_timeout(monkeypatch):
    worker, child = spawn_fixture(monkeypatch, ready_code(), timeout=10)
    start = time.monotonic()
    with pytest.raises(RustWorkerError, match="worker exited"):
        worker.read_raw()
    assert time.monotonic() - start < 3
    worker.close()
    assert_reaped_and_closed(worker, child)


def test_oversized_stdout_line_is_bounded_and_cancel_reaps(monkeypatch):
    code = ready_code() + (
        "sys.stdout.write('x' * (16 * 1024 * 1024))\n"
        "sys.stdout.flush()\n"
    )
    worker, child = spawn_fixture(monkeypatch, code, timeout=2)
    with pytest.raises(RustWorkerError, match="response line too long"):
        worker.read_raw()
    assert worker._lines.qsize() == 0
    worker.cancel()
    assert_reaped_and_closed(worker, child)


def test_failed_constructor_kills_waits_closes_pipes(monkeypatch):
    children = []

    def spawn(_argv, **kwargs):
        child = _REAL_POPEN([sys.executable, "-u", "-c", "import time; time.sleep(30)"], **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(bridge.subprocess, "Popen", spawn)
    with pytest.raises(RustWorkerError, match="worker timed out"):
        RustWorker(sys.executable, timeout_s=0.2)
    child = children[0]
    assert child.poll() is not None
    assert child.stdin.closed and child.stdout.closed and child.stderr.closed
