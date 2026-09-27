"""Command-line boundary for queries, registry validation and local indexing."""

import argparse
import json
import tarfile
from pathlib import Path
from typing import Any

from .evaluator import ReferenceEvaluator
from .indexer import scan
from .query import QuerySpec
from .registry import AdapterRegistry
from .schema import DEFAULT_SCHEMA


def _load_json(path: str) -> Any:
    with Path(path).open(encoding="utf-8") as handle:
        return json.load(handle)


def _runtime_command(args: argparse.Namespace) -> int:
    from .runtime import (  # lazy: numpy/pyroaring are heavy
        RuntimeQuerySpec,
        RuntimeSnapshot,
        combine_inventories,
        compile_runtime,
        load_p2_inventory,
    )
    if args.runtime_command == "compile":
        combined = combine_inventories(
            [load_p2_inventory(directory) for directory in args.inputs])
        summary = compile_runtime(combined, args.output, chunk_size=args.chunk_size)
        print(json.dumps({
            "snapshot_id": summary.snapshot_id,
            "rid_count": summary.rid_count,
            "object_count": summary.object_count,
            "source_count": summary.source_count,
            "dataset_count": summary.dataset_count,
            "tag_count": summary.tag_count,
            "tag_memberships": summary.tag_memberships,
            "path": str(summary.path),
        }, sort_keys=True))
        return 0
    with RuntimeSnapshot.open(args.path, full_verify=args.full_verify) as rt:
        if args.runtime_command == "inspect":
            print(json.dumps({
                "snapshot_id": rt.snapshot_id,
                "rid_count": rt.rid_count,
                "root": str(rt.root),
                "path": str(rt.path),
            }, sort_keys=True))
            return 0
        if args.runtime_command == "verify":
            print(json.dumps({"verified": True,
                              "full": bool(args.full_verify),
                              "snapshot_id": rt.snapshot_id}, sort_keys=True))
            return 0
        if args.runtime_command == "lookup":
            if not args.source or not args.post_id:
                raise ValueError("lookup requires --source and --post-id")
            record = rt.resolve_one(args.source, args.post_id, args.dataset)
            location = rt.location(record.rid)
            print(json.dumps({
                "rid": record.rid,
                "record_id": record.record_id,
                "source_id": record.source_id,
                "dataset_id": record.dataset_id,
                "post_id": record.post_id,
                **location,
            }, sort_keys=True))
            return 0
    spec = RuntimeQuerySpec(
        sources=tuple(args.source or ()),
        datasets=tuple(args.dataset or ()),
        namespace=args.namespace,
        all_tags=tuple(args.all_tags or ()),
        any_tags=tuple(args.any_tags or ()),
        none_tags=tuple(args.none_tags or ()),
    )
    with RuntimeSnapshot.open(args.path, full_verify=args.full_verify) as rt:
        result = rt.query(spec)
        rids = result.limit(args.limit if args.limit is not None else 10)
        print(json.dumps({
            "snapshot_id": result.snapshot_id,
            "count": result.count(),
            "rids": rids,
            "truncated": result.count() > len(rids),
        }, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sakura")
    parser.add_argument("--version", action="version", version="sakurapool 0.1.0")
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate", help="validate a query JSON document")
    validate.add_argument("query", type=Path)
    evaluate = subparsers.add_parser("evaluate", help="evaluate a query against a JSON row array")
    evaluate.add_argument("query", type=Path)
    evaluate.add_argument("rows", type=Path)
    config = subparsers.add_parser("config", help="validate registry configuration")
    config_sub = config.add_subparsers(dest="config_command", required=True)
    config_validate = config_sub.add_parser("validate")
    config_validate.add_argument("--config", type=Path, required=True)
    runtime = subparsers.add_parser("runtime", help="P3 runtime snapshot operations")
    runtime_sub = runtime.add_subparsers(dest="runtime_command", required=True)
    rc = runtime_sub.add_parser("compile", help="compile P2 indexes into a snapshot")
    rc.add_argument(
        "paths", type=Path, nargs="+",
        metavar="P2_DIR|RUNTIME_ROOT",
        help="P2 index dirs; the last positional is RUNTIME_ROOT unless --output")
    rc.add_argument("--output", dest="named_output", type=Path)
    rc.add_argument("--chunk-size", type=int, default=500_000)
    ri = runtime_sub.add_parser("inspect", help="show the current snapshot meta")
    ri.add_argument("path", type=Path)
    ri.add_argument("--full-verify", action="store_true")
    rv = runtime_sub.add_parser("verify", help="verify the current snapshot")
    rv.add_argument("path", type=Path)
    rv.add_argument("--full-verify", action="store_true")
    rl = runtime_sub.add_parser("lookup", help="resolve (source, post_id) to rid+location")
    rl.add_argument("path", type=Path)
    rl.add_argument("--source", required=True)
    rl.add_argument("--post-id", required=True)
    rl.add_argument("--dataset")
    rl.add_argument("--full-verify", action="store_true")
    rq = runtime_sub.add_parser("query", help="run a runtime query")
    rq.add_argument("path", type=Path)
    rq.add_argument("--source", action="append", default=[])
    rq.add_argument("--dataset", action="append", default=[])
    rq.add_argument("--namespace")
    rq.add_argument("--all-tag", dest="all_tags", action="append", default=[])
    rq.add_argument("--any-tag", dest="any_tags", action="append", default=[])
    rq.add_argument("--none-tag", dest="none_tags", action="append", default=[])
    rq.add_argument("--limit", type=int)
    rq.add_argument("--full-verify", action="store_true")
    index = subparsers.add_parser("index", help="build a local index")
    index_subparsers = index.add_subparsers(dest="index_command", required=True)
    scan_command = index_subparsers.add_parser("scan", help="scan uncompressed TAR archives")
    scan_command.add_argument("root", type=Path, nargs="?")
    scan_command.add_argument("output_positional", type=Path, nargs="?")
    scan_command.add_argument("--input", type=Path)
    scan_command.add_argument("--output", type=Path)
    scan_command.add_argument("--config", type=Path)
    scan_command.add_argument("--dataset", default="local")
    scan_command.add_argument("--hash-images", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.command == "runtime":
            if args.runtime_command == "compile":
                if args.named_output:
                    args.inputs = list(args.paths)
                    args.output = args.named_output
                else:
                    if len(args.paths) < 2:
                        raise ValueError(
                            "compile requires at least one P2_DIR and an output root")
                    args.inputs = list(args.paths[:-1])
                    args.output = args.paths[-1]
            return _runtime_command(args)
        if args.command == "config":
            registry = AdapterRegistry.from_dict(_load_json(str(args.config)))
            print(json.dumps({"valid": True, "datasets": sorted(registry.adapters)}))
            return 0
        if args.command == "index":
            if (args.root and args.input) or (args.output_positional and args.output):
                raise ValueError("do not mix positional and named input/output")
            root = args.input or args.root
            output = args.output or args.output_positional
            if root is None or output is None:
                raise ValueError("index scan requires --input and --output")
            registry = (
                AdapterRegistry.from_dict(_load_json(str(args.config)))
                if args.config else AdapterRegistry.local()
            )
            summary = scan(
                root, output, hash_images=args.hash_images, dataset=args.dataset, registry=registry
            )
            print(json.dumps(summary, sort_keys=True))
            return 0 if summary["errors"] == 0 else 1
        query = QuerySpec.from_dict(_load_json(str(args.query)))
        query.validate(DEFAULT_SCHEMA)
        if args.command == "validate":
            print(json.dumps({"valid": True, "query": query.to_dict()}, sort_keys=True))
        else:
            result = ReferenceEvaluator().evaluate(_load_json(str(args.rows)), query)
            print(json.dumps(result.to_pylist(), sort_keys=True))
        return 0
    except (OSError, ValueError, TypeError, KeyError, tarfile.TarError) as exc:
        if args.command in ("index", "config", "runtime"):
            print(json.dumps({"error": str(exc)}, sort_keys=True))
            return 2
        parser.error(str(exc))
        return 2
