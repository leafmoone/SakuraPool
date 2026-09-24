"""Command line interface for query validation and reference evaluation."""

import argparse
import json
from pathlib import Path
from typing import Any

from .evaluator import ReferenceEvaluator
from .query import QuerySpec
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
    args = parser.parse_args(argv)
    try:
        query = QuerySpec.from_dict(_load_json(str(args.query)))
        query.validate(DEFAULT_SCHEMA)
        if args.command == "validate":
            print(json.dumps({"valid": True, "query": query.to_dict()}, sort_keys=True))
        else:
            result = ReferenceEvaluator().evaluate(_load_json(str(args.rows)), query)
            print(json.dumps(result.to_pylist(), sort_keys=True))
        return 0
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
        return 2
