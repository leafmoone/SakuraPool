# SakuraPool P1 Submission Report

## Contract and scope

This submission implements the authorized P1 scope: `src` package layout, actual Arrow schema and validation logic, structured `QuerySpec`, schema/model registries, deterministic small-data set-reference evaluator, functional validation/evaluation CLI, tests, documentation, examples, and evidence. The plan contract is recorded in `reports/P1.md` (scope and exclusions) and `README.md`.

Excluded by contract: TAR scanner, runtime/download, deduplication, and real data access. No P2 work was started.

## Commits

- BASE_SHA: `a4895b9d36e553b8505be46b8e7216e168fabb96`
- IMPLEMENTATION_SHA: `8d3ff42b6544eb5da9d0e1cc4fa25ddee3fbdc52`
- SUBMISSION_SHA: recorded after this report commit

## Verification

- `PYTHONPATH=src python -m pytest -q`: 11 passed.
- `python -m ruff check .`: passed.
- `git diff --check`: passed.
- `python -m build --wheel --outdir /tmp/sakurapool-wheel`: passed.
- Installed wheel CLI smoke: passed; `validate` returned valid JSON and `evaluate` returned two expected rows.

Raw logs are in `reports/P1/*.log`; command metadata and SHA256 manifest are in `reports/P1/metadata.txt` and `reports/P1/evidence-manifest.json`. `main` remains at the empty BASE_SHA; all business files were created on `dev`.
