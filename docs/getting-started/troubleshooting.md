# Troubleshooting

[Documentation](../index.md) / [Getting Started](index.md) / Troubleshooting

Common setup and execution issues for the current prototype.

## Code Changes Do Not Show Up

The Docker Compose flow does not bind-mount the source tree into the container.

If you changed code and still see the old behavior, rebuild:

```bash
docker compose up --build
```

## DuckDB Snapshot Not Found

Check `DATABASE_PATH` in `.env`.

The application expects that path to exist on the host so Docker can bind-mount it into the container. The starter `.env.example` points at the tracked sample snapshot under `examples/data/products.duckdb`.

## DuckDB Schema Validation

The source reader validates the `products` table against the explicit raw input contract.

If you see missing-column errors, your snapshot does not match `openfoodfacts_data_quality.raw_products.RAW_INPUT_COLUMNS`. Use the bundled sample, regenerate a compatible sample, or align the snapshot schema before running parity.

## Missing Legacy Backend Modules

The parity application depends on the legacy backend runtime for reference-side enrichment and findings.

If you run outside the provided Docker flow and see Perl errors such as missing `ProductOpener` modules, switch back to the Docker flow or run from an environment that already provides the OFF backend runtime.

## Legacy Snippet Extraction

Report snippet extraction and legacy inventory export need access to the legacy Perl source tree, not only the runtime image.

Resolution order is:

1. `LEGACY_SOURCE_ROOT`
2. `../openfoodfacts-server`
3. `/opt/product-opener`

If none of those contain the expected Perl modules, legacy snippet extraction will fail.

## Stale Reference Results

Reference results are cached on disk because they are derived artifacts and expensive to recompute.

The cache key depends on:

- the source snapshot id
- the legacy backend fingerprint
- selected Python-side execution-contract files
- the optional manual cache salt

If you need a fresh reference-side materialization, either delete the cache artifact or set `REFERENCE_RESULT_CACHE_SALT` to a new value.

## Python-Only Tooling

Use a local virtual environment for linting, typing, focused tests, and utility scripts:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[app,dev]"
```

This is suitable for:

- `pytest`
- `ruff`
- `mypy`
- `pyright`
- `scripts/validate_dsl.py`

The parity-backed workflow itself is still best exercised through Docker.

## Python Version

The repository source targets Python 3.11-compatible code, but the current Docker image, `.python-version`, and GitHub Actions automation use Python 3.14.3.

Today:

- write repository code that stays within the documented Python 3.11 target
- expect local automation and containerized runs to use 3.14.3 today

## Next Reads

- [Local Development](../guides/local-development.md)
- [Configuration and Artifacts](../operations/configuration-and-artifacts.md)
- [CI and Releases](../operations/ci-and-releases.md)
