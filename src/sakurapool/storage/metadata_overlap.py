"""One lane-owned metadata helper; only the foreground may create byte proofs."""

import sys
import threading
import time
from contextlib import contextmanager

from .metadata_candidates import _error, _failure
from .transport import RemoteIOError

TEARDOWN_SECONDS = 5.0


def _uncertain(error):
    error.finalization_secondary = (
        *getattr(error, "finalization_secondary", ()), "METADATA_FINALIZATION_FAILED",
    )


class _Lookup:
    def __init__(self, shared, key, loader, control):
        self.cancelled = threading.Event()
        self.done = threading.Event()
        self.value = None
        self.failure = None
        self.finalization_failed = False
        self.control = control
        # The closure exists only for this active lane; Thread discards its target
        # on exit. Neither a raw exception nor its traceback is retained here.
        def run():
            try:
                self.value = shared.lookup(key, loader, cancelled=self.cancelled)
            except BaseException as error:
                self.failure = _failure(error)
                self.finalization_failed = bool(getattr(error, "finalization_secondary", ()))
            finally:
                self.done.set()

        self.thread = threading.Thread(target=run, name="sakura-metadata-overlap")
        try:
            self.thread.start()
        except BaseException as primary:
            # Thread.start can be interrupted after launching the target. Cancel
            # even before its first instruction; late lookup cannot initiate IO.
            self.close(primary)
            raise

    def result(self):
        # The loader retains its existing per-request/page bounds. Only shared
        # followers have a wait deadline; do not impose one on a valid leader.
        self.done.wait()
        if self.failure is not None:
            raise _error(self.failure, finalization_failed=self.finalization_failed)
        return self.value

    def close(self, primary):
        deadline = time.monotonic() + TEARDOWN_SECONDS
        failed = False
        if not self.done.is_set():
            self.cancelled.set()
            # This is this lane's own control, including for cache followers.
            # It can never cancel another lane's metadata leader.
            try:
                self.control.signal_cancel(deadline=deadline)
            except BaseException:
                failed = True
            try:
                self.control.close(deadline=deadline)
            except BaseException:
                failed = True
        try:
            self.thread.join(max(0.0, deadline - time.monotonic()))
        except BaseException:
            failed = True
        failed = failed or self.thread.is_alive() or self.finalization_failed
        if failed:
            if primary is not None:
                _uncertain(primary)
            else:
                error = RemoteIOError("Metadata helper finalization incomplete", lightweight=True)
                _uncertain(error)
                raise error
        self.control = None


@contextmanager
def pending_metadata(shared, key, loader, control):
    """At most one helper for this active lane; no submission queue or proof sharing."""
    lookup = _Lookup(shared, key, loader, control)
    try:
        yield lookup.result
    finally:
        lookup.close(sys.exc_info()[1])
