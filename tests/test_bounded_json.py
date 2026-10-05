import hashlib
import io
from pathlib import Path

import pytest
from test_stream_delivery import delivery

from sakurapool.storage import bounded_json as bounded
from sakurapool.storage import publication_fetch as fetch


def validate(tmp_path, raw):
    path = tmp_path / "input.json"
    path.write_bytes(raw)
    return bounded.validate_file(path, len(raw), max_bytes=len(raw))


@pytest.mark.parametrize("raw", [
    b"{}", b' {"x": [true,false,null,{},[]]} \r\n',
    b'{"x":0,"x":-0.5E+20}',
    b'{"s":"\\\"\\\\\\/\\b\\f\\n\\r\\t\\u0041\\ud800"}',
    '{"s":"é中😀"}'.encode(),
    b'{"n":' + b"9" * 10000 + b'}', b'{"n":1e' + b"9" * 10000 + b'}',
])
def test_valid_objects_no_payload(tmp_path, raw):
    assert validate(tmp_path, raw) is None


@pytest.mark.parametrize("raw", [
    b"", b" ", b"[]", b"null", b"1", b'"str"', b"\xef\xbb\xbf{}",
    b'{"a":NaN}', b'{"a":Infinity}', b'{"a":-Infinity}',
    b'{"a":01}', b'{"a":+1}', b'{"a":.1}', b'{"a":1.}',
    b'{"a":1e}', b'{"a":1e+}', b'{"a":--1}', b'{"a":1 2}',
    b'{"a":}', b'{"a" 1}', b'{a:1}', b'{"a":true,}',
    b'{"a":[1,]}', b'{"a":[,1]}', b'{"a":tru}', b'{"a":falsee}',
    b'{"a":"\\x"}', b'{"a":"\\u12xz"}', b'{"a":"\x00"}',
    b'{"a":"\x80"}', b'{"a":"\xc0\xaf"}', b'{"a":"\xed\xa0\x80"}',
    b'{"a":"\xf4\x90\x80\x80"}', b'{"a":"\xe2x\xa0"}',
    b'{"a":"\xe2\x82', b'{"a":"str', b'{"a":[1', b'{',
    b"{}{}", b"{} trailing", b"{}\v",
])
def test_rejects_invalid_json(tmp_path, raw):
    with pytest.raises(bounded.BoundedJSONError):
        validate(tmp_path, raw)


def test_dense_keys_node_bound(tmp_path, monkeypatch):
    raw = b"{" + b",".join([b'"":0'] * 20000) + b"}"
    assert validate(tmp_path, raw) is None
    monkeypatch.setattr(bounded, "MAX_NODES", 40000)
    with pytest.raises(bounded.BoundedJSONError, match="node/string"):
        validate(tmp_path, raw)


def test_depth_boundary(tmp_path):
    raw = b'{"a":' + b"[" * (bounded.MAX_DEPTH - 1) + b"0"
    assert validate(tmp_path, raw + b"]" * (bounded.MAX_DEPTH - 1) + b"}") is None
    with pytest.raises(bounded.BoundedJSONError, match="depth"):
        validate(tmp_path, b'{"a":' + b"[" * bounded.MAX_DEPTH + b"0"
                 + b"]" * bounded.MAX_DEPTH + b"}")


def test_extent_and_capacity(tmp_path):
    path = tmp_path / "input.json"
    path.write_bytes(b"{}")
    for extent, limit in [(3, 3), (1, 1), (2, 1), (-1, 2), (True, 2)]:
        with pytest.raises(bounded.BoundedJSONError):
            bounded.validate_file(path, extent, max_bytes=limit)
    path.write_bytes(b"{} ")
    with pytest.raises(bounded.BoundedJSONError, match="excess"):
        bounded.validate_file(path, 2, max_bytes=2)


def test_chunked_readinto_only_and_utf8_boundaries(monkeypatch):
    raw = b'{"s":"' + b"a" * (bounded.READ_BYTES - 7) + "😀".encode() + b'\\u0041"}'

    class ShortReads(io.BytesIO):
        def read(self, *args):
            pytest.fail("validator must not read/materialize")

        def readinto(self, buffer):
            assert len(buffer) <= bounded.READ_BYTES
            return super().readinto(buffer[:7])

    monkeypatch.setattr(Path, "open", lambda *args, **kwargs: ShortReads(raw))
    assert bounded.validate_file(Path("unused"), len(raw), max_bytes=len(raw)) is None


def test_error_traceback_releases_scratch(tmp_path):
    with pytest.raises(bounded.BoundedJSONError) as caught:
        validate(tmp_path, b'{"s":"secret\\x"}')
    tb = caught.value.__traceback__
    while tb:
        reader = tb.tb_frame.f_locals.get("reader")
        if reader is not None:
            assert not reader.buffer
        stack = tb.tb_frame.f_locals.get("stack")
        if stack is not None:
            assert not stack
        tb = tb.tb_next
    assert "secret" not in str(caught.value)


def test_fetch_fixed_validation_admission_and_receipt(tmp_path, monkeypatch):
    with delivery(tmp_path, monkeypatch) as (pub, record, transport, workspace):
        reservations, events = [], []
        reserve = transport.ledger.reserve

        def tracked(value):
            reservations.append(value)
            return reserve(value)

        monkeypatch.setattr(transport.ledger, "reserve", tracked)
        result = fetch._fetch_publication_sample(
            pub, record, transport, workspace.tmp, metadata=True,
            attempt_hook=lambda event, value: events.append((event, value)))
        assert any(r.inflight == bounded.VALIDATION_INFLIGHT_BYTES for r in reservations)
        assert bounded.VALIDATION_INFLIGHT_BYTES == (
            bounded.READ_BYTES + bounded.MAX_DEPTH + bounded.CONTROL_BYTES)
        assert bounded.VALIDATION_INFLIGHT_BYTES != len(transport.metadata) * 8
        receipt = dict(events)["PREPARED"]["receipt"]["metadata.json"]
        assert receipt["sha256"] == hashlib.sha256(transport.metadata).hexdigest()
        assert receipt["bytes"] == len(transport.metadata)
        assert (result / "metadata.json").read_bytes() == transport.metadata
        with transport.ledger._locked():
            _, (_, _, pending, _) = transport.ledger._read_pair()
        assert not pending


@pytest.mark.parametrize("failure", ["syntax", "memory", "empty"])
def test_fetch_validation_failure_preserves_unknown(tmp_path, monkeypatch, failure):
    with delivery(tmp_path, monkeypatch, "json" if failure == "syntax" else "none") as data:
        pub, record, transport, workspace = data
        if failure == "memory":
            def fail(*args, **kwargs):
                raise MemoryError("private allocation")
            monkeypatch.setattr(fetch, "validate_file", fail)
        elif failure == "empty":
            location = pub.runtime.location
            pub.runtime.location = lambda rid: dict(location(rid), metadata_size=0)
        with pytest.raises(fetch.PublicationFetchError) as caught:
            fetch._fetch_publication_sample(pub, record, transport, workspace.tmp, metadata=True)
        assert caught.value.code == "publication_range"
        assert caught.value.validation_failed
        assert caught.value.accounting_state == "UNKNOWN"
        assert not (workspace.tmp / record).exists()
        with transport.ledger._locked():
            _, (_, _, pending, _) = transport.ledger._read_pair()
        assert any(p["inflight"] == bounded.VALIDATION_INFLIGHT_BYTES for p in pending.values())
        assert "private" not in str(caught.value)


def test_fetch_absent_metadata_flag_skips_validator(tmp_path, monkeypatch):
    with delivery(tmp_path, monkeypatch) as (pub, record, transport, workspace):
        location = pub.runtime.location
        pub.runtime.location = lambda rid: dict(location(rid), flags=0)
        monkeypatch.setattr(fetch, "validate_file", lambda *a, **k: pytest.fail("absent metadata"))
        result = fetch._fetch_publication_sample(
            pub, record, transport, workspace.tmp, metadata=True)
        assert not (result / "metadata.json").exists()
