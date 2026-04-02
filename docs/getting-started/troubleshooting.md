# Troubleshooting

[Documentation](../index.md) / [Getting Started](index.md) / Troubleshooting

Common setup and execution issues.

## Code Changes

The Docker Compose flow does not mount the source tree into the container.

If you changed code and still see the old behavior, rebuild:

```bash
docker compose up --build
```

## DuckDB Snapshot

Check `DATABASE_PATH` in `.env`.

The application expects that path to exist on the host so Docker can mount it into the container. The starter `.env.example` points at the tracked sample snapshot under `examples/data/products.duckdb`.

## DuckDB Schema

The source reader validates the `products` table against the explicit [raw input contract](../architecture/data-contracts.md).

If you see missing column errors, your snapshot does not match `openfoodfacts_data_quality.raw_products.RAW_INPUT_COLUMNS`. Use the bundled sample, regenerate a compatible sample, or align the snapshot schema before running the application.

## Legacy Backend Modules

Compared runs and enriched application runs depend on the legacy backend environment for reference enrichment and findings.

If you run outside the documented [Docker flow](../guides/local-development.md) and see Perl errors such as missing `ProductOpener` modules, switch back to Docker or run from an environment that already provides the OFF backend runtime. A warm reference cache can avoid live backend execution, but the supported application flow still assumes that environment is available when reference materialization is needed.

## Legacy Snippet Extraction

Report snippet extraction and legacy inventory export need access to the legacy Perl source tree. The runtime image alone is not sufficient.

Resolution order is:

1. `LEGACY_SOURCE_ROOT`
2. `../openfoodfacts-server`
3. `/opt/product-opener`

If none of those contain the expected Perl modules, the report still renders and marks legacy source provenance as unavailable for the affected checks.

## Reference Results

Reference results are cached on disk because they are derived artifacts and expensive to recompute.

The cache key depends on:

- the source snapshot id
- the legacy backend fingerprint
- selected Python execution contract files
- the optional manual cache salt

If you need a fresh reference materialization, either delete the cache artifact or set `REFERENCE_RESULT_CACHE_SALT` to a new value.
Each cache DB also writes a `.meta.json` sidecar. If the runtime contract changes, startup fails with field level mismatch details instead of reusing the stale cache silently.

## Python Tooling

Use a local virtual environment for linting, typing, focused tests, and utility scripts:

```bash
python3.14 -m venv .venv
.venv/bin/python -m pip install -e ".[app,dev]"
```

Use this for:

- `.venv/bin/python -m pytest`
- `ruff`
- `mypy`
- `pyright`
- `.venv/bin/python scripts/validate_dsl.py`

Compared runs are still easiest to exercise through Docker.

## Python Version

The repository targets Python 3.14.3 across local automation, `.python-version`, Docker, and GitHub Actions.

- create local virtual environments with Python 3.14
- expect local automation and containerized runs to use 3.14.3

## Next

- [Local Development](../guides/local-development.md)
- [Configuration and Artifacts](../operations/configuration-and-artifacts.md)
- [CI and Releases](../operations/ci-and-releases.md)
