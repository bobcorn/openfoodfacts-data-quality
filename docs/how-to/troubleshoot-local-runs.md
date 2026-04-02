# Troubleshoot Local Runs

[Back to documentation](../index.md)

Fix common problems in Docker runs, report generation, and local Python tooling.

## Rebuild after code changes

The Docker Compose flow does not mount the source tree into the container.

If you changed code and still see old behavior, rebuild:

```bash
docker compose up --build
```

## Fix `DATABASE_PATH`

Check `DATABASE_PATH` in `.env`.

That path must exist on the host so Docker can mount it into the container. The starter `.env.example` points at the tracked [source snapshot](../reference/glossary.md#source-snapshot) under `examples/data/products.duckdb`.

## Fix DuckDB schema mismatches

The source reader validates the `products` table against the explicit [raw input contract](../reference/data-contracts.md#raw-rows).

If you see errors about missing columns, your snapshot does not match `openfoodfacts_data_quality.raw_products.RAW_INPUT_COLUMNS`. Use the bundled sample, regenerate a compatible sample, or align the snapshot schema before you run the application.

## Fix missing legacy backend modules

Compared runs and enriched application runs depend on the [reference path](../concepts/reference-and-parity.md#reference-path), which still needs the legacy backend environment for cache misses.

If you run outside the documented [Docker flow](run-the-project-locally.md) and see Perl errors such as missing `ProductOpener` modules, switch back to Docker or use an environment that already provides the OFF backend runtime.

A warm [reference cache](../reference/run-configuration-and-artifacts.md#reference-cache) can avoid live backend execution for covered products, but the supported application flow still assumes that environment is available when reference materialization is needed.

## Fix missing legacy snippets

Report snippet extraction needs access to the legacy Perl source tree. The runtime image alone is not enough.

The resolver checks these locations in order:

1. `LEGACY_SOURCE_ROOT`
2. `../openfoodfacts-server`
3. `/opt/product-opener`

If none of those paths contains the expected Perl modules, the report still renders and marks legacy source provenance as unavailable.

## Refresh reference results

[Reference results](../concepts/reference-and-parity.md#reference-path) are cached on disk because they are expensive to recompute.

The cache key depends on:

- the [source snapshot](../reference/glossary.md#source-snapshot) id
- the [legacy backend fingerprint](../reference/legacy-backend-image.md#cache-fingerprint)
- selected Python execution contract files
- the optional manual cache salt

If you need fresh reference materialization, delete the cache artifact or set `REFERENCE_RESULT_CACHE_SALT` to a new value.

Each cache database also writes a `.meta.json` sidecar. If the [runtime contracts](../reference/data-contracts.md) change, startup fails with detailed field mismatch information instead of reusing stale data.

## Set up Python tooling

Use a local virtual environment for linting, typing, and focused tests.

1. Create the virtual environment.

   ```bash
   python3.14 -m venv .venv
   ```

2. Install the repository with app and dev dependencies.

   ```bash
   .venv/bin/python -m pip install -e ".[app,dev]"
   ```

Use this environment for `pytest`, `ruff`, `mypy`, and `pyright`.

Compared runs are still easiest to exercise through Docker.

## Check the Python version

The repository targets Python 3.14.3 in local automation, `.python-version`, Docker, and GitHub Actions.

Create local virtual environments with Python 3.14. Expect local automation and containerized runs to use 3.14.3.

[Back to documentation](../index.md)
