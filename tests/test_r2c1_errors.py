"""Stable public rejection, bounded diagnostics and conservative accounting."""

import json

import pytest
from test_r2_production import TOKEN
from test_r2_production import twohop as _twohop

from sakurapool.storage.rust_bridge import RustWorkerError
from sakurapool.storage.transport import RemoteIOError

twohop = _twohop


@pytest.mark.parametrize("complete", [True, False])
def test_ok_true_error_cannot_be_business_success(twohop, monkeypatch, complete):
    _, ledger, transport, obj = twohop
    from sakurapool.storage import production

    class Rejected:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def send_raw(self, line):
            self.request_id = json.loads(line)["request_id"]

        def read_raw(self):
            accounting = {"body": 1, "attempts": 2, "complete": complete}
            return {
                "type": "response",
                "request_id": self.request_id,
                "ok": True,
                "result": {
                    "production_error": "cdn_status",
                    "provider_raw": TOKEN,
                    "diagnostic": {
                        "phase": "cdn",
                        "http_status": 403,
                        "attempts": 2,
                        "body_bytes_observed": 1,
                        "accounting_complete": complete,
                        "content_length_present": True,
                        "content_range_present": False,
                        "etag_present": False,
                        "etag_is_strong": False,
                        "content_encoding_present": False,
                    },
                    "accounting": accounting,
                },
            }

    monkeypatch.setattr(production, "RustWorker", Rejected)
    with pytest.raises(RemoteIOError, match="rejected") as error:
        with transport.transfer(obj, condition="observe"):
            pytest.fail("rejected worker result reached consumer")
    assert TOKEN not in str(error.value)
    assert error.value.__cause__ is None and error.value.__context__ is None
    assert TOKEN not in json.dumps(transport.last_result)
    assert transport.last_result["diagnostic"]["http_status"] == 403
    assert ledger.status()["attempts"] == 2
    assert ledger.status()["body"] == (1 if complete else 2)
    assert ledger.status()["inflight"] == 0


@pytest.mark.parametrize("failure", ["timeout", "death", "malformed"])
def test_worker_unknown_failure_is_stable_and_never_refunds(twohop, monkeypatch, failure):
    _, ledger, transport, obj = twohop
    from sakurapool.storage import production

    class Failed:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            if failure != "malformed":
                raise RustWorkerError(TOKEN + " signed URL raw exception")
            return self

        def __exit__(self, *_args):
            pass

        def send_raw(self, line):
            self.request_id = json.loads(line)["request_id"]

        def read_raw(self):
            return {
                "type": "response",
                "request_id": self.request_id,
                "ok": True,
                "result": {
                    "production_error": TOKEN,
                    "accounting": {"body": "unknown", "attempts": 2, "complete": False},
                },
            }

    monkeypatch.setattr(production, "RustWorker", Failed)
    with pytest.raises(RemoteIOError, match="accounting uncertain") as error:
        with transport.transfer(obj, condition="observe"):
            pytest.fail("failed IPC reached consumer")
    assert TOKEN not in str(error.value)
    assert error.value.__cause__ is None and error.value.__context__ is None
    assert ledger.status()["body"] == 2
    assert ledger.status()["attempts"] == 2
    assert ledger.status()["inflight"] == 0
