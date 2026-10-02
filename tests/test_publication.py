"""Publication structural tests; all data synthetic, no network."""

import hashlib
import json
import sqlite3
import tempfile
from contextlib import closing, contextmanager
from types import SimpleNamespace

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from synthetic_p2 import ObjectSpec, SampleSpec, build_p2_directory

from sakurapool.cli import main
from sakurapool.indexer import _json
from sakurapool.runtime.compiler import compile_runtime
from sakurapool.runtime.inventory import load_p2_inventory
from sakurapool.storage.publication import build_publication, load_publication, sha


@pytest.mark.parametrize(
    "failure",
    ["none", "provider", "proof", "image", "metadata", "empty_metadata", "publish", "settle"],
)
def test_fresh_fetch_requires_own_proof_and_sha(inputs, monkeypatch, failure):
    from sakurapool.storage import publication_fetch as fetch
    from sakurapool.storage.budget import DEFAULT_WORK_ROOT, BudgetLedger
    from sakurapool.storage.transport import RemoteIOError

    rt, roots, mapping, out, _ = inputs
    build_publication(rt, roots, mapping, out)
    calls = []
    with tempfile.TemporaryDirectory(dir=DEFAULT_WORK_ROOT, prefix="offline-publication-") as temp:
        ledger = BudgetLedger(temp, _offline_test=True)

        if failure == "settle":

            def reject_settle(*args, **kwargs):
                raise RemoteIOError("settlement failed after rename")

            monkeypatch.setattr(ledger, "settle", reject_settle)

        class FakeTransport:
            max_range_bytes = 8 << 20

            def __init__(self):
                self.ledger = ledger

            def _host(self, url):
                return "modelscope.cn"

            def verify_conditions(self, obj):
                calls.append("own-proof")
                if failure == "proof":
                    raise RemoteIOError("wrong If-Match semantics")
                return obj

            @contextmanager
            def read_range_owned(self, bound, offset, size):
                assert calls[:2] == ["provider", "own-proof"]
                calls.append("range")
                yield b"wrong" if failure == "image" else b"image"

        def lookup(*args):
            calls.append("provider")
            if failure == "provider":
                raise RemoteIOError("provider digest changed")
            return SimpleNamespace(
                repo_type="modelscope_dataset_legacy",
                origin="https://modelscope.cn",
                repo_id="synthetic/test",
                revision="b" * 40,
                object_path="gc5m/one.tar",
                validator='"fresh"',
            )

        monkeypatch.setattr(fetch, "exact_provider_lookup", lookup)
        if failure == "publish":

            def reject_publish(*args):
                raise RemoteIOError("atomic publish failed")

            monkeypatch.setattr(fetch, "_publish_directory", reject_publish)
        with load_publication(out, full_verify=True) as pub:
            record = (
                pub.runtime._catalog.execute("SELECT record_id FROM records").fetchone()[0].hex()
            )
            # Synthetic extent matches the synthetic payload; fresh binding remains mandatory.
            original = pub.runtime.location
            monkeypatch.setattr(
                pub.runtime,
                "location",
                lambda rid: dict(
                    original(rid),
                    image_size=5,
                    flags=1 if failure in ("metadata", "empty_metadata") else 0,
                    metadata_size=5 if failure == "metadata" else 0,
                    metadata_offset=1024,
                ),
            )
            if failure in ("none", "metadata", "empty_metadata"):
                result = fetch.fetch_publication_sample(
                    pub, record, FakeTransport(), temp, control=object(), metadata=True
                )
                assert next(result.glob("image.*")).read_bytes() == b"image"
                assert calls == ["provider", "own-proof", "range"] + (
                    ["range"] if failure == "metadata" else []
                )
                if failure in ("metadata", "empty_metadata"):
                    assert (result / "metadata.json").read_bytes() == (
                        b"image" if failure == "metadata" else b""
                    )
            else:
                with pytest.raises(RemoteIOError):
                    fetch.fetch_publication_sample(
                        pub, record, FakeTransport(), temp, control=object()
                    )
                delivered = (__import__("pathlib").Path(temp) / record).exists()
                assert delivered == (failure == "settle")
                # Rename succeeded before accounting failure: preserve verified output and pending.
                if failure == "settle":
                    assert ledger.status()["saved_samples"] > 0
            assert '"strong_etag"' not in (out / "PUBLICATION.json").read_text()


@pytest.mark.parametrize("field", ["digest", "path", "size", "revision"])
def test_exact_lookup_rejects_changed_provider(monkeypatch, field):
    from sakurapool.storage import publication_fetch as fetch
    from sakurapool.storage.transport import RemoteIOError

    expected = dict(
        path="gc5m/one.tar", size=10240, revision_candidate="b" * 40, provider_sha256="a" * 64
    )
    altered = dict(expected)
    key, value = {
        "digest": ("provider_sha256", "0" * 64),
        "path": ("path", "gc5m/two.tar"),
        "size": ("size", 1),
        "revision": ("revision_candidate", "c" * 40),
    }[field]
    altered[key] = value

    class Provider:
        def __init__(self, *args):
            pass

        def legacy_hub_id(self):
            return "synthetic/test"

        def legacy_tree_page(self, *args, **kwargs):
            assert kwargs["root"] == "gc5m"
            return [SimpleNamespace(**altered)], True

    monkeypatch.setattr(fetch, "ModelScopeDataset", Provider)
    with pytest.raises(RemoteIOError):
        fetch.exact_provider_lookup(
            object(),
            "https://modelscope.cn",
            "synthetic/test",
            "b" * 40,
            "gc5m/one.tar",
            10240,
            "a" * 64,
        )


@pytest.fixture
def inputs(tmp_path):
    p2 = tmp_path / "p2"
    build_p2_directory(
        p2,
        dataset="test",
        source="synthetic",
        objects=[
            ObjectSpec("gc5m/one.tar", [SampleSpec("one.png", "1", has_json=False)], size=10240)
        ],
    )
    contract = json.loads((p2 / "INPUT.json").read_bytes())
    contract["hash_images"] = True
    (p2 / "INPUT.json").write_bytes(_json(contract))
    for marker in p2.glob("*.COMMIT"):
        c = json.loads(marker.read_bytes())
        info = c["files"]["samples"]
        p = p2 / info["path"]
        t = pq.read_table(p)
        for name, v in [
            ("sha256", hashlib.sha256(b"image").hexdigest()),
            ("hash_source", "computed:sha256"),
            ("hash_kind", "sha256"),
        ]:
            i = t.schema.get_field_index(name)
            t = t.set_column(i, t.schema.field(i), pa.array([v], type=t.schema.field(i).type))
        pq.write_table(t, p)
        info["bytes"] = p.stat().st_size
        info["sha256"] = sha(p)
        c["contract_sha256"] = hashlib.sha256(_json(contract)).hexdigest()
        marker.write_bytes(_json(c))
    rt = tmp_path / "runtime"
    compile_runtime(load_p2_inventory(p2), rt)
    roots = tmp_path / "roots.json"
    roots.write_text(json.dumps(dict(format="sakurapool-p2-root-list-v1", roots=[str(p2)])))
    row = dict(
        dataset_id="test",
        endpoint="https://modelscope.cn",
        repo_id="synthetic/test",
        repo_type="modelscope_dataset_legacy",
        revision_candidate="b" * 40,
        object_path="gc5m/one.tar",
        object_size=10240,
        provider_sha256=contract["inputs"]["gc5m/one.tar"]["sha256"],
    )
    mapping = tmp_path / "map.jsonl"
    mapping.write_text(json.dumps(row) + "\n")
    return rt, roots, mapping, tmp_path / "publication", row


def test_build_open_cli(inputs):
    rt, roots, mapping, out, _ = inputs
    m = build_publication(rt, roots, mapping, out)
    assert m["durable_included"] is False and not (out / "durable").exists()
    assert m["rid_count"] == m["fetchable_rid_count"] == 1
    with load_publication(out, full_verify=True) as p:
        assert isinstance(p.hashes, np.memmap) and p.hashes.dtype == np.dtype("V32")
        assert p.hashes[0].tobytes() == hashlib.sha256(b"image").digest()
        with pytest.raises(sqlite3.OperationalError):
            p.catalog.execute("DELETE FROM objects")
    assert main(["publication", "inspect", str(out)]) == 0
    assert main(["publication", "verify", str(out), "--full"]) == 0


@pytest.mark.parametrize(
    "change",
    ["missing", "extra", "duplicate", "dataset", "path", "size", "revision", "repo", "provider"],
)
def test_map_closed(inputs, change):
    rt, roots, mapping, out, row = inputs
    rows = [row.copy()]
    if change == "missing":
        rows = []
    elif change == "extra":
        rows.append(dict(row, object_path="gc5m/extra.tar"))
    elif change == "duplicate":
        rows.append(row)
    else:
        key, val = {
            "dataset": ("dataset_id", "other"),
            "path": ("object_path", "gc5m/other.tar"),
            "size": ("object_size", 1),
            "revision": ("revision_candidate", "master"),
            "repo": ("repo_id", "invalid"),
            "provider": ("provider_sha256", "0" * 64),
        }[change]
        rows[0][key] = val
    mapping.write_text("".join(json.dumps(r) + "\n" for r in rows))
    with pytest.raises((ValueError, sqlite3.IntegrityError)):
        build_publication(rt, roots, mapping, out)
    assert not out.exists()


def test_null_provider(inputs):
    rt, roots, mapping, out, row = inputs
    row["provider_sha256"] = None
    mapping.write_text(json.dumps(row) + "\n")
    m = build_publication(rt, roots, mapping, out)
    assert m["fetchable_rid_count"] == m["fetchable_object_count"] == 0


@pytest.mark.parametrize(
    "change",
    [
        "unknown",
        "oversized",
        "snapshot",
        "catalog_hash",
        "array_hash",
        "dtype",
        "shape",
        "rid_count",
        "missing",
        "extra",
        "path",
        "size",
        "content",
        "provider",
    ],
)
def test_corruption(inputs, change):
    rt, roots, mapping, out, _ = inputs
    build_publication(rt, roots, mapping, out)
    m = json.loads((out / "PUBLICATION.json").read_bytes())
    if change == "unknown":
        m["unknown"] = 1
    elif change == "oversized":
        (out / "PUBLICATION.json").write_bytes(b" " * ((1 << 20) + 1))
    elif change == "snapshot":
        m["snapshot_id"] = "0" * 64
    elif change == "catalog_hash":
        m["remote_objects_sha256"] = "0" * 64
    elif change == "array_hash":
        m["image_sha256_sha256"] = "0" * 64
    elif change in ("dtype", "shape"):
        np.save(
            out / "image_sha256.npy",
            np.zeros((2 if change == "shape" else 1,), dtype="V32" if change == "shape" else "u1"),
        )
        m["image_sha256_bytes"] = (out / "image_sha256.npy").stat().st_size
        m["image_sha256_sha256"] = sha(out / "image_sha256.npy")
    elif change == "rid_count":
        m["rid_count"] += 1
    else:
        with closing(sqlite3.connect(out / "remote_objects.sqlite")) as db:
            if change == "missing":
                db.execute("DELETE FROM objects")
            elif change == "extra":
                db.execute(
                    'INSERT INTO objects SELECT 99,dataset_id,object_id||"extra",repo_idx,'
                    "revision_candidate,object_path,object_size,content_sha256,"
                    "provider_sha256,fetchable FROM objects"
                )
            elif change == "path":
                db.execute('UPDATE objects SET object_path="other.tar"')
            elif change == "size":
                db.execute("UPDATE objects SET object_size=?", (b"\0" * 8,))
            elif change == "content":
                db.execute("UPDATE objects SET content_sha256=?", (b"\0" * 32,))
            else:
                db.execute("UPDATE objects SET provider_sha256=?", (b"\0" * 32,))
            db.commit()
        m["remote_objects_bytes"] = (out / "remote_objects.sqlite").stat().st_size
        m["remote_objects_sha256"] = sha(out / "remote_objects.sqlite")
    if change != "oversized":
        (out / "PUBLICATION.json").write_text(json.dumps(m))
        (out / "READY").write_text(sha(out / "PUBLICATION.json"))
    with pytest.raises((ValueError, sqlite3.Error)):
        load_publication(out, full_verify=True)


def test_production_external_control_rejected_before_credentials(inputs, monkeypatch):
    from sakurapool.storage import publication_fetch as fetch
    from sakurapool.storage.budget import BudgetLedger
    from sakurapool.storage.production import RustProductionTransport
    from sakurapool.storage.publication import PublicationCorrupt

    rt, roots, mapping, out, _ = inputs
    build_publication(rt, roots, mapping, out)
    ledger = object.__new__(BudgetLedger)
    monkeypatch.setattr(BudgetLedger, "offline_mode", property(lambda self: False))
    transport = object.__new__(RustProductionTransport)
    transport.ledger, transport.origin, transport.production_profile = (
        ledger,
        "https://modelscope.cn",
        True,
    )
    with load_publication(out, full_verify=True) as pub:
        record = pub.runtime._catalog.execute("SELECT record_id FROM records").fetchone()[0].hex()
        with pytest.raises(PublicationCorrupt, match="external production control"):
            fetch.fetch_publication_sample(pub, record, transport, out, control=object())


def test_fast_meta_bounded_and_runtime_ready(inputs):
    rt, roots, mapping, out, _ = inputs
    build_publication(rt, roots, mapping, out)
    with closing(sqlite3.connect(out / "remote_objects.sqlite")) as db:
        db.execute("INSERT INTO meta SELECT * FROM meta")
        db.commit()
    with pytest.raises(ValueError):
        load_publication(out)
    with closing(sqlite3.connect(out / "remote_objects.sqlite")) as db:
        db.execute("DELETE FROM meta WHERE rowid=2")
        db.commit()
    current = json.loads((out / "runtime/current.json").read_bytes())
    (out / "runtime" / current["path"] / "READY").write_text("0" * 64)
    with pytest.raises(ValueError):
        load_publication(out)


@pytest.mark.parametrize(
    "suffix",
    [b"\n", b"x", b"\xff", b"x" * (1 << 20)],
    ids=["newline", "trailing", "invalid_encoding", "oversized"],
)
@pytest.mark.parametrize("full", [False, True])
def test_publication_ready_strict_bounded(inputs, suffix, full):
    rt, roots, mapping, out, _ = inputs
    build_publication(rt, roots, mapping, out)
    ready = out / "READY"
    ready.write_bytes(ready.read_bytes() + suffix)
    with pytest.raises(ValueError, match="not READY"):
        load_publication(out, full_verify=full)


def test_cli_sanitizes_unexpected_failure(monkeypatch, capsys):
    from sakurapool.storage import publication

    def fail(*args, **kwargs):
        raise RuntimeError("SECRET must not appear")

    monkeypatch.setattr(publication, "load_publication", fail)
    assert main(["publication", "inspect", "ignored"]) == 2
    assert "SECRET" not in capsys.readouterr().out
