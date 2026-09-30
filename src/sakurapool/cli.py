"""Command-line boundary for queries, registry validation and local indexing."""

from __future__ import annotations

import argparse
import json
import sys
import tarfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .runtime import RuntimeQuerySpec

from .evaluator import ReferenceEvaluator
from .indexer import scan
from .query import QuerySpec
from .registry import AdapterRegistry
from .schema import DEFAULT_SCHEMA


def _load_json(path: str) -> Any:
    with Path(path).open(encoding="utf-8") as handle:
        return json.load(handle)


def _spec_tags(value: Any) -> tuple:
    tags = tuple(value or ())
    return tags


def _spec_from_dict(data: dict[str, Any]) -> "RuntimeQuerySpec":
    """Build a RuntimeQuerySpec from a --spec JSON document (any_of included)."""
    from .runtime import RuntimeQuerySpec
    if not isinstance(data, dict):
        raise ValueError("spec must be a JSON object")
    branches = []
    for branch in data.get("any_of", ()) or ():
        if not isinstance(branch, dict):
            raise ValueError("any_of branches must be objects")
        branches.append(_spec_from_dict(branch))
    known = {"sources", "datasets", "namespace", "all_tags", "any_tags",
             "none_tags", "any_of"}
    unknown = set(data) - known
    if unknown:
        raise ValueError(f"unknown spec keys: {sorted(unknown)}")
    return RuntimeQuerySpec(
        sources=tuple(data.get("sources", ()) or ()),
        datasets=tuple(data.get("datasets", ()) or ()),
        namespace=data.get("namespace"),
        all_tags=_spec_tags(data.get("all_tags")),
        any_tags=_spec_tags(data.get("any_tags")),
        none_tags=_spec_tags(data.get("none_tags")),
        any_of=tuple(branches),
    )


def _runtime_command(args: argparse.Namespace) -> int:
    from .runtime import (  # lazy: numpy/pyroaring are heavy
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
    path = args.snapshot if args.snapshot is not None else args.path
    if path is None:
        raise ValueError("a snapshot path (positional or --snapshot) is required")
    if args.runtime_command == "query" and args.spec:
        from .runtime import RuntimeQuerySpec  # noqa: F401 - import check
        spec_data = (_load_json(args.spec)
                     if Path(args.spec).is_file() else json.loads(args.spec))
        spec = _spec_from_dict(spec_data)
    else:
        spec = None
    with RuntimeSnapshot.open(path, full_verify=args.full_verify) as rt:
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
            # fall through: lookup returns before the query branch below
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
        if args.runtime_command != "query":
            raise ValueError(
                f"unhandled runtime command: {args.runtime_command}")
        if spec is not None:
            if any((args.source, args.dataset, args.namespace,
                    args.all_tags, args.any_tags, args.none_tags)):
                raise ValueError("--spec cannot be combined with term flags")
        else:
            from .runtime import RuntimeQuerySpec
            spec = RuntimeQuerySpec(
                sources=tuple(args.source or ()),
                datasets=tuple(args.dataset or ()),
                namespace=args.namespace,
                all_tags=_spec_tags(args.all_tags),
                any_tags=_spec_tags(args.any_tags),
                none_tags=_spec_tags(args.none_tags),
            )
        result = rt.query(spec)
        rids = result.limit(args.limit if args.limit is not None else 10)
        print(json.dumps({
            "snapshot_id": result.snapshot_id,
            "count": result.count(),
            "rids": rids,
            "truncated": result.count() > len(rids),
        }, sort_keys=True))
        return 0


def _scan_remote_blocked() -> int:
    """Only scan/compile is blocked until the working-set budget is proven."""
    print(json.dumps({"status": "BLOCKED", "command": "index scan-remote",
                      "reason": "explicit production mode/profile/binding/working set required"},
                     sort_keys=True))
    print("remote scan/compile blocked before HTTP or artifact creation", file=sys.stderr)
    return 3


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
        "paths", type=Path, nargs="*",
        metavar="P2_DIR|RUNTIME_ROOT",
        help="P2 index dirs; the last positional is RUNTIME_ROOT unless --output")
    rc.add_argument("--index", dest="index_dirs", action="append",
                    default=[], type=Path, help="P2 index dir (repeatable)")
    rc.add_argument("--output", dest="named_output", type=Path)
    rc.add_argument("--chunk-size", type=int, default=500_000)
    ri = runtime_sub.add_parser("inspect", help="show the current snapshot meta")
    ri.add_argument("path", type=Path, nargs="?")
    ri.add_argument("--snapshot", type=Path, default=None)
    ri.add_argument("--full-verify", action="store_true")
    ri.add_argument("--full", dest="full_verify", action="store_true")
    rv = runtime_sub.add_parser("verify", help="verify the current snapshot")
    rv.add_argument("path", type=Path, nargs="?")
    rv.add_argument("--snapshot", type=Path, default=None)
    rv.add_argument("--full-verify", action="store_true")
    rv.add_argument("--full", dest="full_verify", action="store_true")
    rl = runtime_sub.add_parser("lookup", help="resolve (source, post_id) to rid+location")
    rl.add_argument("path", type=Path, nargs="?")
    rl.add_argument("--snapshot", type=Path, default=None)
    rl.add_argument("--source", required=True)
    rl.add_argument("--post-id", required=True)
    rl.add_argument("--dataset")
    rl.add_argument("--full-verify", action="store_true")
    rl.add_argument("--full", dest="full_verify", action="store_true")
    rq = runtime_sub.add_parser("query", help="run a runtime query")
    rq.add_argument("path", type=Path, nargs="?")
    rq.add_argument("--snapshot", type=Path, default=None)
    rq.add_argument("--source", action="append", default=[])
    rq.add_argument("--dataset", action="append", default=[])
    rq.add_argument("--namespace")
    rq.add_argument("--all-tag", dest="all_tags", action="append", default=[])
    rq.add_argument("--any-tag", dest="any_tags", action="append", default=[])
    rq.add_argument("--none-tag", dest="none_tags", action="append", default=[])
    rq.add_argument("--spec",
                    help="query JSON document (file or inline) incl. any_of")
    rq.add_argument("--limit", type=int)
    rq.add_argument("--full-verify", action="store_true")
    rq.add_argument("--full", dest="full_verify", action="store_true")
    remote = subparsers.add_parser("remote", help="P4 remote operations (gated)")
    remote_sub = remote.add_subparsers(dest="remote_command", required=True)
    inspect = remote_sub.add_parser("inspect", help="guarded metadata inspection")
    inspect.add_argument("--config", type=Path, required=True)
    inspect.add_argument("--output", type=Path, required=True)
    inspect.add_argument("--offline-fixture", action="store_true",
                         help="literal 127.0.0.1 fixture only; no credentials")
    fetch = remote_sub.add_parser("fetch", help="fetch a packaged remote record")
    fetch.add_argument("--package", type=Path, required=True)
    fetch.add_argument("--config", type=Path, required=True)
    fetch.add_argument("--record-id", required=True)
    fetch.add_argument("--output", type=Path, required=True)
    fetch.add_argument("--offline-fixture", action="store_true",
                       help="literal 127.0.0.1 fixture only; no credentials")
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
    build = index_subparsers.add_parser(
        "build-partition", help="single-pass local Rust partition build")
    build.add_argument("--config", type=Path, required=True)
    build.add_argument("--manifest", type=Path, required=True)
    build.add_argument("--worker", type=Path, required=True)
    build.add_argument("--work-dir", type=Path, required=True)
    build.add_argument("--output", type=Path, required=True)
    build.add_argument("--code-sha", required=True,
                       help="full SHA of the installed candidate/approved build")
    scan_remote = index_subparsers.add_parser("scan-remote", help="gated P4 scan")
    scan_remote.add_argument("--config", type=Path, required=True)
    scan_remote.add_argument("--plan", type=Path, required=True)
    scan_remote.add_argument("--output-package", type=Path, required=True,
                             help="fresh P2 directory; runtime/package scheduled separately")
    scan_remote.add_argument("--mode", choices=("download-then-scan", "remote-stream-scan"),
                             help="explicit administrator production pipeline")
    args = parser.parse_args(argv)
    if (args.command == "runtime" and args.runtime_command == "compile"
            and not args.index_dirs and not args.paths):
        parser.error("runtime compile requires --index or P2_DIR arguments")
    try:
        if args.command == "remote":
            from .storage.cli_ops import fetch, inspect
            try:
                if args.remote_command == "fetch":
                    if not args.package.is_dir() or not (
                            args.package / "index-package.json").is_file():
                        raise ValueError("local package unavailable; fetch never scans")
                    result = fetch(args.package, args.config, args.record_id, args.output,
                                   offline_fixture=args.offline_fixture)
                else:
                    result = inspect(args.config, args.output,
                                     offline_fixture=args.offline_fixture)
            except Exception as exc:
                # Only vetted static codes cross the public boundary. Raw
                # exception strings/context, URLs, response bodies and tokens
                # must never enter either stream (including unexpected errors).
                from .storage.transport import RemoteIOError
                public = ({"error": "remote operation failed", "status": "ERROR"}
                          | (exc.public_diagnostic() if isinstance(exc, RemoteIOError)
                             else {"code": "local_or_unclassified", "phase": "local"}))
                print(json.dumps(public, sort_keys=True))
                print("remote operation failed; no implicit scan or fallback",
                      file=sys.stderr)
                return 2
            print(json.dumps(result, sort_keys=True))
            return 0
        if args.command == "index" and args.index_command == "build-partition":
            from .local_builder import build_partition
            from .partition import PartitionManifest
            manifest = PartitionManifest.from_dict(_load_json(str(args.manifest)))
            registry = AdapterRegistry.from_dict(_load_json(str(args.config)))
            result = build_partition(manifest, registry.get(manifest.dataset), args.worker,
                                     args.work_dir, args.output, code_sha=args.code_sha,
                                     completion=lambda value: print(json.dumps(value), flush=True))
            print(json.dumps(result, sort_keys=True))
            return 0 if result["counts"]["errors"] == 0 else 1
        if args.command == "index" and args.index_command == "scan-remote":
            if not args.mode:
                return _scan_remote_blocked()
            try:
                from .storage.production_cli import scan as production_scan

                result = production_scan(args.config, args.plan, args.output_package, args.mode)
                print(json.dumps(result, sort_keys=True))
                return 0
            except Exception:
                # No URL, token, response text or raw exception can reach the public boundary.
                print(json.dumps({"status": "BLOCKED", "error": "production build gate failed"}))
                return 3
        if args.command == "runtime":
            if args.runtime_command == "compile":
                if args.index_dirs and args.paths[:-1]:
                    raise ValueError(
                        "--index cannot be mixed with positional P2 dirs")
                if args.index_dirs:
                    if args.named_output:
                        if args.paths:
                            raise ValueError(
                                "--index + --output takes no positional args")
                        args.inputs = list(args.index_dirs)
                        args.output = args.named_output
                    elif args.paths:
                        args.inputs = list(args.index_dirs)
                        args.output = args.paths[-1]
                    else:
                        raise ValueError("--index requires --output or a RUNTIME_ROOT positional")
                elif args.named_output:
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
