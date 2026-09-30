"""Fresh installed-wheel proof; run from outside repository with NO PYTHONPATH.

Synthetic TAR only, loopback HTTP; Rust Response Read scan -> product adapter/
shared stage -> real P2 durable v4 -> P3 runtime/query -> Rust gated Range.
Test raw is a SMALL fixture for existing Python audit; not a no-buffer Python
stage or production transport claim. NEW data root is kept, never cleaned.
"""

import argparse
import base64
import hashlib
import http.server
import importlib.metadata as md
import io
import json
import os
import sys
import tarfile
import threading
from pathlib import Path

import sakurapool
from sakurapool.registry import DatasetAdapter
from sakurapool.runtime.compiler import compile_runtime
from sakurapool.runtime.inventory import load_p2_inventory
from sakurapool.runtime.query import RuntimeQuerySpec
from sakurapool.runtime.snapshot import RuntimeSnapshot
from sakurapool.storage.budget import DEFAULT_WORK_ROOT, MIB, BudgetLedger, _disk_usage
from sakurapool.storage.remote_index import open_completed_stage, write_staged_v4
from sakurapool.storage.rust_bridge import RustWorker
from sakurapool.storage.rust_index import build_stage_from_scan
from sakurapool.storage.transport import BoundObject


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", required=True, type=Path)
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--implementation", required=True)
    parser.add_argument("--tree", required=True)
    args = parser.parse_args()
    assert "PYTHONPATH" not in os.environ
    package_path = Path(sakurapool.__file__).resolve()
    assert "site-packages" in package_path.parts, package_path
    direct = json.loads(md.distribution("sakurapool").read_text("direct_url.json"))
    assert "archive_info" in direct and not direct.get("dir_info", {}).get("editable")
    assert args.data_root.is_relative_to(DEFAULT_WORK_ROOT)
    args.data_root.mkdir(exist_ok=False)  # never overwrite/reuse any prior evidence root
    print(f"IMPLEMENTATION={args.implementation}\nTREE={args.tree}")
    print(f"PYTHON={sys.version}\nEXECUTABLE={sys.executable}\nPACKAGE={package_path}")
    print(f"PYTHONPATH=unset\nDIRECT_URL={json.dumps(direct, sort_keys=True)}")
    for name in ["sakurapool", "pyarrow", "numpy", "pyroaring", "requests", "modelscope-hub"]:
        print(f"VERSION {name}={md.version(name)}")
    print(f"WORKER={args.worker} SHA256={hashlib.sha256(args.worker.read_bytes()).hexdigest()}")
    print(f"DATA_ROOT={args.data_root}\nFIXED_ALLOCATED_BEFORE={_disk_usage(DEFAULT_WORK_ROOT)}")
    png = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJ"
        "AAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==")
    metadata = json.dumps({"text": "Fix3 installed wheel", "tags": ["fix3"],
                           "width": 1, "height": 1, "has_alpha": False}).encode()
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.GNU_FORMAT) as archive:
        for name, data in [("synthetic/1.png", png), ("synthetic/1.json", metadata)]:
            member = tarfile.TarInfo(name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
    raw = buffer.getvalue()
    hits = []

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def do_GET(self):
            requested = self.headers.get("Range")
            hits.append({"method": "GET", "path": self.path, "range": requested})
            if requested:
                start, end = map(int, requested.removeprefix("bytes=").split("-"))
                part = raw[start:end + 1]
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(raw)}")
            else:
                part = raw
                self.send_response(200)
            self.send_header("Content-Length", str(len(part)))
            self.send_header("Connection", "close")
            self.end_headers()
            # Small wire chunks make the stream path visible; no real archive.
            for pos in range(0, len(part), 173):
                self.wfile.write(part[pos:pos + 173])
                self.wfile.flush()

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_address[1]}/synthetic.tar"
    budget = {"body": 4 * MIB, "disk": 4 * MIB, "inflight": 8 * MIB, "attempts": 5}
    ledger_root = args.data_root / "ledger"
    ledger_root.mkdir()
    ledger = BudgetLedger(ledger_root, _offline_test=True,
                          _test_limits={"body": 4 * MIB, "inflight": 8 * MIB, "attempts": 5})
    try:
        with RustWorker(args.worker, job_budget=budget) as worker:
            scan = worker.request("scan_http_tar", budget={
                "body": len(raw), "disk": 0, "inflight": len(raw), "attempts": 1,
            }, payload={"url": url, "max_bytes": len(raw)})
            assert scan["whole_sha256"] == hashlib.sha256(raw).hexdigest()
            assert scan["size"] == len(raw) and len(scan["members"]) == 2
            print(f"HTTP_SCAN size={scan['size']} members=2 sha256={scan['whole_sha256']}")
            adapter = DatasetAdapter("fix3synthetic", "installed-wheel")
            bound = BoundObject(url, len(raw), strong_etag='"fix3-synthetic"')
            stage_path = args.data_root / "stage"
            stage = build_stage_from_scan(scan, raw, bound, adapter, stage_path, ledger)
            assert open_completed_stage(stage_path, bound, adapter) == stage
            summary = write_staged_v4(ledger, [("fix3/archive.tar", bound, stage)],
                                      ledger.root / "durable", adapter,
                                      audit_output=ledger.root / "audit.json")
            assert summary["samples"] == 1 and summary["errors"] == 0
            print(f"PRODUCT_ADAPTER={json.dumps(adapter.to_dict(), sort_keys=True)}")
            print(f"P2_V4 samples=1 errors=0 stage_members={stage.members}")
            runtime = args.data_root / "runtime"
            compiled = compile_runtime(load_p2_inventory(ledger.root / "durable"), runtime)
            assert compiled.rid_count == 1
            with RuntimeSnapshot.open(runtime) as snapshot:
                rids = snapshot.query(RuntimeQuerySpec(all_tags=[("tags", "fix3")])).limit(5)
                assert len(rids) == 1
                loc = snapshot.location(rids[0])
                obj = snapshot.object_ref(loc["object_idx"])
                assert obj["object_size"] == len(raw)
            print(f"P3_QUERY rid_count=1 offset={loc['image_offset']} size={loc['image_size']}")
            reply = worker.fetch_range_gated(url, loc["image_offset"],
                                            loc["image_offset"] + loc["image_size"] - 1,
                                            obj["object_size"], ledger=ledger,
                                            disk_reserve=loc["image_size"])
            assert reply == {"bytes": len(png), "sha256": hashlib.sha256(png).hexdigest()}
            assert raw[loc["image_offset"]:loc["image_offset"] + loc["image_size"]] == png
            print(f"RUST_RANGE={json.dumps(reply, sort_keys=True)}")
            print(f"LEDGER={json.dumps(ledger.status(), sort_keys=True)}")
            assert len(hits) == 2 and hits[0]["range"] is None
            expected_range = f"bytes={loc['image_offset']}-{loc['image_offset'] + len(png)-1}"
            assert hits[1]["range"] == expected_range
            print(f"HTTP_REQUESTS={json.dumps(hits, sort_keys=True)}")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
    print(f"FIXED_ALLOCATED_AFTER={_disk_usage(DEFAULT_WORK_ROOT)}")
    print("FRESH_WHEEL_PASS")


if __name__ == "__main__":
    main()
