"""Small synthetic growth curve for bounded-spool scaling evidence."""
import io
import json
import tarfile
import tempfile
import time
from pathlib import Path

from sakurapool.indexer import scan


def main():
    results = []
    for count in (1025, 2051, 4102):
        with tempfile.TemporaryDirectory(prefix="sakurapool-growth-") as temp:
            root = Path(temp)
            source = root / "input"
            source.mkdir()
            with tarfile.open(source / "a.tar", "w") as archive:
                for i in range(count):
                    for suffix, data in (("jpg", b"x"), ("json", b'{"tags":["x"]}')):
                        info = tarfile.TarInfo(f"{i}.{suffix}")
                        info.size = len(data)
                        archive.addfile(info, io.BytesIO(data))
            started = time.perf_counter()
            result = scan(source, root / "output")
            results.append({"pairs": count, "seconds": time.perf_counter() - started,
                           "samples": result["samples"], "errors": result["errors"]})
    print(json.dumps(results, sort_keys=True))


if __name__ == "__main__":
    main()
