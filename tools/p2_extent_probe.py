"""Print measured extents checked against TAR extraction and original bytes."""

import hashlib
import io
import json
import tarfile
import tempfile
from pathlib import Path

import pyarrow.parquet as pq

from sakurapool.indexer import scan
from sakurapool.registry import AdapterRegistry, DatasetAdapter


def main():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        source = root / "fixture.tar"
        members = {"1.jpg": b"one", "1.json": b'{"tags":["a"]}',
                   "2.webp": b"two", "2.json": b'{"tags":[]}',
                   "images/7.jpg": b"seven", "meta/7.json": b"{}"}
        with tarfile.open(source, "w") as tar:
            for name, payload in members.items():
                member = tarfile.TarInfo(name)
                member.size = len(payload)
                tar.addfile(member, io.BytesIO(payload))
        registry = AdapterRegistry()
        registry.register(DatasetAdapter("demo", "A", image_prefix="images/",
                                         metadata_prefix="meta/"))
        out = root / "out"
        result = scan(source, out, dataset="demo", registry=registry)
        extents = []
        with source.open("rb") as stream, tarfile.open(source, "r:") as tar:
            for row in pq.read_table(next(out.glob("*.samples.parquet"))).to_pylist():
                for path, offset, size in [
                    (row["image_path"], row["offset_data"], row["size"]),
                    (row["json_path"], row["json_offset_data"], row["json_size"]),
                ]:
                    stream.seek(offset)
                    payload = stream.read(size)
                    assert payload == members[path] == tar.extractfile(path).read()
                    extents.append(dict(member=path, offset=offset, length=size,
                                        sha256=hashlib.sha256(payload).hexdigest(), equal=True))
        print(json.dumps(dict(summary=result, extents=extents), indent=2))


if __name__ == "__main__":
    main()
