"""Current-dev canary schema/header discovery gaps; synthetic loopback only."""

import io
import json
import tarfile
from dataclasses import replace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_r2c2_binding import load_c2_helpers
from test_r2c2_binding import twohop as _twohop

twohop = _twohop


def nested():
    return {
        "schema_version": 1,
        "id": 42,
        "source": {"dataset": "observed-dataset"},
        "image": {"format": "png", "width": None, "height": None},
        "captions": None,
        "tags": {"general": ["blue", None, "hair"]},
    }


def archive(meta, json_first=False):
    output = io.BytesIO()
    pairs = [("42.png", b"not decoded image"), ("42.json", json.dumps(meta).encode())]
    with tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        for name, raw in reversed(pairs) if json_first else pairs:
            header = tarfile.TarInfo(name)
            header.size = len(raw)
            tar.addfile(header, io.BytesIO(raw))
    return output.getvalue()


def configure(twohop, monkeypatch, raw):
    state, ledger, transport, candidate = twohop
    state["raw"] = raw
    candidate = replace(candidate, object_size=len(raw))
    transport.verify_conditions(candidate)
    scheduler, canary = load_c2_helpers()
    identity = {"code_commit": "offline-identification", "code_tree": "offline-identification"}
    monkeypatch.setattr(scheduler, "code_identity", lambda: identity)
    original = scheduler.execute_evidenced
    monkeypatch.setattr(
        scheduler,
        "execute_evidenced",
        lambda *args, **kwargs: original(*args, **kwargs, directory=ledger.root),
    )
    return state, ledger, transport, candidate, scheduler, canary, identity


@pytest.mark.parametrize("json_first", [False, True])
def test_actual_nested_nullable_identification_and_formal_scan(twohop, monkeypatch, json_first):
    state, ledger, transport, obj, scheduler, canary, identity = configure(
        twohop, monkeypatch, archive(nested(), json_first)
    )
    before = ledger.status()["attempts"]
    adapter, report = canary.identify_adapter(transport, obj, scheduler, identity)
    assert report["status"] == "PASS"
    assert report["source_field_observed"] and report["schema_consistent"]
    assert report["image_header_observed"]
    assert report["source_policy_independently_verified"] is False
    assert "source_explicitly_verified" not in report
    assert not report["tar_validity_certified"] and report["headers_examined"] == 2
    assert adapter.source == "observed-dataset" and adapter.source not in obj.repo_id
    assert adapter.allowed_provenance == ("observed-dataset",)
    assert ledger.status()["attempts"] - before == 6
    assert ledger.status()["inflight"] == 0
    assert not any(
        "Authorization" in headers or "Cookie" in headers
        for hop, headers in state["calls"]
        if hop == "cdn"
    )
    root, built = canary.build_one(
        transport, obj, adapter, "remote-stream-scan", scheduler, identity
    )
    assert built["status"] == "PASS", built
    assert built["inventory_verified"] and built["runtime_verified"]
    assert built["rid_record_locations_verified"] == 1
    assert built["json_content_sha_independently_verified"]
    row = next(
        iter(pq.ParquetFile(next(root.rglob("*.samples.parquet"))).iter_batches())
    ).to_pylist()[0]
    assert row["width"] is row["height"] is row["text"] is None
    assert row["tags"] == [
        {"value": "blue", "category": "general"},
        {"value": "hair", "category": "general"},
    ]


@pytest.mark.parametrize("mutation", ["wrong_format", "null_required", "old_wrapper", "no_source"])
def test_unrecognized_metadata_does_not_guess_or_claim_invalid_tar(twohop, monkeypatch, mutation):
    meta = nested()
    if mutation == "wrong_format":
        meta["image"]["format"] = "jpg"
    if mutation == "null_required":
        meta["schema_version"] = None
    if mutation == "no_source":
        del meta["source"]
    if mutation == "old_wrapper":
        meta = {"source": "guessed", "data": meta}
    _, ledger, transport, obj, scheduler, canary, identity = configure(
        twohop, monkeypatch, archive(meta)
    )
    adapter, report = canary.identify_adapter(transport, obj, scheduler, identity)
    assert adapter is None and report["status"] == "ADAPTER_NOT_IDENTIFIED"
    assert report["scope_limited"] and ledger.status()["inflight"] == 0
    assert report["failure_kind"] in (
        "metadata_schema_unrecognized",
        "header_or_metadata_not_identified",
    )


@pytest.mark.parametrize(
    "shape", ["pax", "gnu_sparse", "truncated_padding", "bad_checksum", "json_too_large"]
)
def test_identification_extent_and_extension_boundaries_not_tar_authority(
    twohop, monkeypatch, shape
):
    header = tarfile.TarInfo("42.json")
    header.size = 1
    if shape == "pax":
        header.type = tarfile.XHDTYPE
    if shape == "gnu_sparse":
        header.type = tarfile.GNUTYPE_SPARSE
    if shape == "json_too_large":
        header.size = (1 << 20) + 1
    raw = header.tobuf(format=tarfile.GNU_FORMAT if shape == "gnu_sparse" else tarfile.USTAR_FORMAT)
    raw += b"\x00" * (((header.size + 511) // 512) * 512 + 1024)
    if shape == "truncated_padding":
        raw = raw[:513]
    if shape == "bad_checksum":
        raw = b"X" + raw[1:]
    _, ledger, transport, obj, scheduler, canary, identity = configure(twohop, monkeypatch, raw)
    adapter, report = canary.identify_adapter(transport, obj, scheduler, identity)
    assert adapter is None and report["status"] == "ADAPTER_NOT_IDENTIFIED"
    assert report["scope_limited"]
    assert ledger.status()["inflight"] == (1024 if shape == "bad_checksum" else 0)
    if shape == "bad_checksum":
        assert report["budget_after"]["pending_unknown_body_bound"] == 0
    assert report["headers_examined"] <= 1


def test_footer_only_canary_helpers_are_empty_not_iterator_error(tmp_path):
    _, canary = load_c2_helpers()
    roots = [tmp_path / "remote", tmp_path / "download"]
    schema = pa.schema([("unused", pa.int64())])
    for root in roots:
        root.mkdir()
        for name in ("objects", "samples", "annotations", "errors"):
            with pq.ParquetWriter(root / ("empty." + name + ".parquet"), schema):
                pass
        bounds = canary.validate_canary_rows(
            root, dict.fromkeys(("objects", "samples", "annotations", "errors"), 0)
        )
        assert bounds["rows"] == bounds["serialized_bytes"] == 0
        assert bounds["all_rows_streamed"] and bounds["arrow_batch_rows"] == 1
    assert canary._equivalent_rows(*roots)
