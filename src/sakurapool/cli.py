"""Command line interface for query validation, evaluation, and local indexing."""

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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sakurapool")
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate", help="validate a query JSON document")
    validate.add_argument("query", type=Path)
    evaluate = subparsers.add_parser("evaluate", help="evaluate a query against a JSON row array")
    evaluate.add_argument("query", type=Path)
    evaluate.add_argument("rows", type=Path)
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
        if args.command == "index":
            if (args.root and args.input) or (args.output_positional and args.output):
                raise ValueError("do not mix positional and named input/output")
            root = args.input or args.root
            output = args.output or args.output_positional
            if root is None or output is None:
                raise ValueError("index scan requires --input and --output")
            registry = (
                AdapterRegistry.from_dict(_load_json(str(args.config)))
                if args.config
                else AdapterRegistry.local()
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
        if args.command == "index":
            print(json.dumps({"error": str(exc)}, sort_keys=True))
            return 2
        parser.error(str(exc))
        return 2
