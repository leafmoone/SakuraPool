"""Socket-free probe gate/label/privacy check; never invoke real_probe.main."""

import importlib.util
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sakurapool.storage.modelscope import ListedFile

spec = importlib.util.spec_from_file_location(
    "r2c1_probe", Path(__file__).with_name("real_probe.py")
)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
secret = "synthetic-secret?signature=private"
public = m.public_probe(
    {
        "etag": secret,
        "cdn_host": secret,
        "url": secret,
        "body": secret,
        "status": 206,
        "observation": {"content_length": secret},
        "diagnostic": {"phase": secret, "etag_is_strong": True},
    }
)
assert secret not in json.dumps(public)
ledger = SimpleNamespace(offline_mode=False, record_condition_proof=lambda *_: None)


class Control:
    def __init__(self, *_args, **_kwargs):
        self.logical = self.attempts = 3
        self.accepted_bytes = 100

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        pass


class Dataset:
    def __init__(self, _control, endpoint, repo):
        self.transport = SimpleNamespace(ledger=ledger)
        self.endpoint, self.repo_id = endpoint, repo

    def legacy_hub_id(self):
        return m.HUB_ID

    def legacy_tree_page(self, *_args, **_kwargs):
        return [ListedFile(m.TARGET, m.SIZE, None, False, m.REVISION)], False


for scenario, expected in (
    ("rangefail", ["observe"]),
    ("positivefail", ["observe", "match"]),
    ("negativefail", ["observe", "match", "wrong"]),
    ("success", ["observe", "match", "wrong"]),
):
    calls = []

    class Transport:
        def __init__(self, *_args, **_kwargs):
            self.last_result = {}

        @contextmanager
        def transfer(self, _obj, condition="observe"):
            calls.append(condition)
            n = 0 if condition == "wrong" else 1
            result = {
                "status": 412 if condition == "wrong" else 206,
                "bytes": n,
                "etag": '"fixture"',
                "cdn_host": "fixture.example",
                "sha256": "c" * 64,
                "accounting": {"complete": True, "body": n, "attempts": 2},
                "diagnostic": {"etag_is_strong": True, "accounting_complete": True, "phase": "cdn"},
                "untrusted": secret,
            }
            if scenario == "rangefail" and condition == "observe":
                result["status"] = 200
            if scenario == "positivefail" and condition == "match":
                result["cdn_host"] = "changed.example"
            self.last_result = result
            if scenario == "negativefail" and condition == "wrong":
                raise RuntimeError(secret)
            yield None, result

        @contextmanager
        def _capability_match(self, obj):
            with self.transfer(obj, condition="match") as (_, result):
                yield result

        def register(self, *_args):
            pass

    report = {
        "status": "BLOCKED",
        "byte_logical_operations": 0,
        "REAL_RANGE_CAPABILITY": "BLOCKED",
        "REAL_IF_MATCH_POSITIVE": "NOT_RUN",
        "REAL_IF_MATCH_NEGATIVE": "NOT_RUN",
        "VERSION_BINDING": "BLOCKED",
    }
    with (
        patch.object(m, "Control", Control),
        patch.object(m, "ModelScopeDataset", Dataset),
        patch.object(m, "RustProductionTransport", Transport),
        patch.object(m, "proof_key", lambda *_: "safe"),
    ):
        m.run_round(ledger, "synthetic", report)
    assert calls == expected, (scenario, calls)
    assert secret not in json.dumps(report)
    assert (report["status"] == "VERIFIED") == (scenario == "success")
    assert report["VERSION_BINDING"] == ("PASS" if scenario == "success" else "BLOCKED")
    if scenario == "rangefail":
        assert report["REAL_IF_MATCH_POSITIVE"] == report["REAL_IF_MATCH_NEGATIVE"] == "NOT_RUN"
    if scenario in ("positivefail", "negativefail"):
        assert report["if_match_limitation"] == "IF_MATCH_UNSUPPORTED_OR_UNVERIFIED"
print("PROBE_MOCK_CHECKS=5_PASS REAL_REQUESTS=0 CREDENTIAL_READ=NO SENTINEL_CREATED=NO")
