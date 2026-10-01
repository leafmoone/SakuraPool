"""Package audit accepts a committed footer-only samples fragment."""

import json

import pyarrow.parquet as pq
import pytest
from test_p4_package import real_package as _real_package

from sakurapool import indexer
from sakurapool.runtime.compiler import compile_runtime
from sakurapool.runtime.inventory import load_p2_inventory
from sakurapool.storage.package import PackageCorrupt, _validate_package_chain, load_package


@pytest.fixture
def real_package():
    yield from _real_package.__wrapped__()


@pytest.mark.parametrize("footer_only", [True, False])
def test_empty_package_sample_audit(real_package, footer_only):
    import pyarrow as pa

    root, _ledger, _client = real_package
    package = load_package(root, allow_offline_loopback=True)
    manifest = json.loads((root / "index-package.json").read_bytes())
    for marker in (root / "durable").glob("*.COMMIT"):
        commit = json.loads(marker.read_bytes())
        for name in ("samples", "annotations", "errors"):
            info = commit["files"][name]
            path = root / "durable" / info["path"]
            with pq.ParquetWriter(path, indexer.SCHEMAS[name]) as writer:
                if not footer_only:
                    writer.write_table(pa.Table.from_batches([], schema=indexer.SCHEMAS[name]))
            info.update(rows=0, bytes=path.stat().st_size, sha256=indexer._sha(path))
        marker.write_bytes(indexer._json(commit))
    summary = compile_runtime(load_p2_inventory(root / "durable"), root / "empty-runtime")
    assert summary.rid_count == 0
    manifest.update(runtime="empty-runtime", snapshot_id=summary.snapshot_id)
    names = {item["path"] for item in manifest["files"]}
    _validate_package_chain(root, manifest, names, package.bindings, {})
    # Empty fragments must not suppress detection of orphan audited records.
    with pytest.raises(PackageCorrupt):
        _validate_package_chain(root, manifest, names, package.bindings, package.audit)
