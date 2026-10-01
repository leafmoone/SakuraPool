import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from synthetic_p2 import ObjectSpec, SampleSpec, build_p2_directory

from sakurapool import indexer
from sakurapool.runtime.compiler import _read_batches, compile_runtime
from sakurapool.runtime.errors import CorruptInputError
from sakurapool.runtime.inventory import (
    _check_row_ownership,
    _validate_fragment,
    load_p2_inventory,
)
from sakurapool.runtime.snapshot import RuntimeSnapshot


@pytest.mark.parametrize("name", list(indexer.SCHEMAS))
@pytest.mark.parametrize("footer_only", [True, False])
def test_empty_fragment_readers(tmp_path, name, footer_only):
    path = tmp_path / f"{name}.parquet"
    schema = indexer.SCHEMAS[name]
    with pq.ParquetWriter(path, schema) as writer:
        if not footer_only:
            writer.write_table(pa.Table.from_batches([], schema=schema))
    with pq.ParquetFile(path) as reader:
        assert reader.num_row_groups == (0 if footer_only else 1)
    info = {"path": path.name, "bytes": path.stat().st_size,
            "sha256": indexer._sha(path), "rows": 0}
    assert _validate_fragment(path, name, info) == 0
    assert indexer._verify_fragment(path, schema) == 0
    assert list(_read_batches(path)) == []
    if name != "errors":
        _check_row_ownership(path, "dataset", "object")
    with pytest.raises(CorruptInputError, match="schema/rows mismatch"):
        _validate_fragment(path, name, dict(info, rows=1))
    with pytest.raises(CorruptInputError, match="hash/size mismatch"):
        _validate_fragment(path, name, dict(info, sha256="0" * 64))


@pytest.mark.parametrize("with_sample", [False, True])
def test_footer_only_committed_fragments_compile(tmp_path, with_sample):
    root = tmp_path / "p2"
    samples = [SampleSpec("42", "42", [])] if with_sample else []
    build_p2_directory(root, dataset="synthetic", source="synthetic",
                       objects=[ObjectSpec("a.tar", samples)])
    # Model real P2 writers that close without writing a batch. Synthetic P2's
    # write_table(empty) normally emits one empty row group instead.
    marker = next(root.glob("*.COMMIT"))
    commit = json.loads(marker.read_bytes())
    replaced = []
    for name, info in commit["files"].items():
        if info["rows"] == 0:
            path = root / info["path"]
            with pq.ParquetWriter(path, indexer.SCHEMAS[name]):
                pass
            info.update(bytes=path.stat().st_size, sha256=indexer._sha(path))
            replaced.append(name)
    assert "errors" in replaced
    if not with_sample:
        assert "samples" in replaced and "annotations" in replaced
    marker.write_bytes(indexer._json(commit))
    inventory = load_p2_inventory(root)
    runtime_root = tmp_path / "runtime"
    summary = compile_runtime(inventory, runtime_root)
    assert summary.rid_count == int(with_sample)
    snapshot = RuntimeSnapshot.open(runtime_root)
    assert snapshot.snapshot_id == summary.snapshot_id
    assert compile_runtime(load_p2_inventory(root), runtime_root).snapshot_id == summary.snapshot_id
