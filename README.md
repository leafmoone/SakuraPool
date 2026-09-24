# SakuraPool

SakuraPool P1 provides a small, deterministic Python reference layer for typed row datasets. It is intended to make query and model contracts testable before any production data-plane implementation.

## Included

- Pinned Python package metadata and `pyarrow` schema conversion.
- Structured `QuerySpec` and `FilterSpec` validation.
- Schema and model registries with duplicate protection.
- In-memory reference evaluation of `filter`, `select`, and `count` queries.
- CLI validation and evaluation using JSON files.

## Quick start

```console
python -m pip install -e '.[dev]'
sakurapool validate examples/query.json
sakurapool evaluate examples/query.json examples/rows.json
```

The evaluator preserves input order and applies filters before the optional limit. It is intentionally a small-data correctness reference, not a production execution engine.

## P1 boundary

TAR scanning, downloads, deduplication, runtime/model execution, and real data are explicitly out of scope. See [reports/P1.md](reports/P1.md) for assumptions and validation evidence.
