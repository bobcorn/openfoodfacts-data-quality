[Back to documentation index](../index.md)

# Troubleshoot local runs

Use this guide to fix common problems in Docker runs, report generation, and
local tooling.

## Rebuild after code changes

The Docker Compose flow does not mount the source tree into the container.

If you changed code and still see old behavior, rebuild:

```bash
docker compose up --build
```

## Fix SOURCE_SNAPSHOT_PATH

Check `SOURCE_SNAPSHOT_PATH` in `.env`.

That path must exist on the host so Docker can mount it into the container. The
sample `.env.example` points at the tracked
[source snapshot](../reference/glossary.md#source-snapshot) under
`examples/data/products.jsonl`.

Local migration runs do not use a bundled source snapshot path. If
`SOURCE_SNAPSHOT_PATH` is unset or blank, startup fails and asks you to set it
explicitly. Use the
[migration demo image](../../README.md#run-the-migration-demo) when you want the bundled sample
without local configuration.

## Fix unknown profile names

Check these settings in `.env`:

- `CHECK_PROFILE`
- `SOURCE_DATASET_PROFILE`

Each one must match a profile defined in `config/check-profiles.toml` or
`config/dataset-profiles.toml`.

## Fix source snapshot input errors

The migration source reader accepts JSONL full product documents or a DuckDB
snapshot with a `products` table and a `code` column. It builds
[ProductDocument](../reference/data-contracts.md#productdocument) for the
reference path and [SourceProduct](../reference/data-contracts.md#sourceproduct)
for the migrated runtime.

When the path suffix is not enough, the runtime uses deterministic content
detection. If a DuckDB snapshot fails, make sure it exposes a `products`
table with a `code` column.

Rows with a missing or blank product code no longer fail the whole run. The
migration tooling skips them, logs a warning during preparation, and records the
skipped-row summary in the report and `run.json`.

## Fix a missing report at the preview URL

`http://localhost:8000` is only the default preview URL.

If the report does not open there, check `MIGRATION_PORT` in `.env`. In the
default Docker flow, that setting controls the published port on the host and
the preview server inside the container, so the preview URL is
`http://localhost:<MIGRATION_PORT>`.

If you changed `MIGRATION_BIND_HOST`, make sure you open the report on
that host instead of assuming `localhost`.

If you changed `MIGRATION_PORT`, open the report on that port instead of
forcing `8000`.
If you want the default URL back, restore `MIGRATION_BIND_HOST=127.0.0.1`
and `MIGRATION_PORT=8000` in `.env`, then restart the Docker flow so Compose
recreates the published port mapping:

```bash
docker compose up --build
```

## Understand worker-count warnings

If the logs warn that `LEGACY_BACKEND_WORKERS` exceeds `BATCH_WORKERS`, the
configuration is still valid.

That warning means the run cannot use more legacy backend workers than the
number of concurrent source batches. Lower `LEGACY_BACKEND_WORKERS`, raise
`BATCH_WORKERS`, or leave the settings as they are if you only need a small
local run.

## Fix missing legacy backend modules

Compared runs and enriched snapshot migration runs depend on the
[reference path](../explanation/reference-data-and-parity.md#why-the-reference-path-exists),
which still needs the
[legacy backend environment](../reference/legacy-backend-image.md) for cache
misses.

If you run outside the documented
[Docker flow](run-the-project-locally.md) and see Perl errors such as missing
`ProductOpener` modules, switch back to Docker or use an environment that
already provides the backend runtime.

A warm
[reference result cache](../reference/run-configuration-and-artifacts.md#reference-result-cache)
can avoid live backend execution for covered products, but the supported
migration flow still assumes that environment is available when reference
materialization is needed.

## Fix missing legacy snippets

Report snippet extraction needs access to the legacy Perl source tree. The
runtime image alone is not enough.

The resolver checks these locations in order:

1. `LEGACY_SOURCE_ROOT`
2. `../openfoodfacts-server`
3. `/opt/product-opener`

If none of those paths contains the expected Perl modules, the report still
renders and marks
[legacy source provenance](../reference/report-artifacts.md#snippetsjson) as
unavailable.

## Refresh reference results

[Reference results](../explanation/reference-data-and-parity.md#why-the-reference-path-exists)
are cached on disk because they are expensive to recompute.

The cache key depends on:

- the [source snapshot](../reference/glossary.md#source-snapshot) id
- the [legacy backend fingerprint](../reference/legacy-backend-image.md#cache-fingerprint)
- selected Python execution contract files
- the optional manual cache salt

If you need fresh reference materialization, delete the cache artifact or set
`REFERENCE_RESULT_CACHE_SALT` to a different value.

Each cache database also writes a `.meta.json` sidecar. If the
[runtime contracts](../reference/data-contracts.md) change, startup fails with
detailed field mismatch information instead of reusing stale data.

## Understand artifacts and parity store files

`artifacts/latest/` is rebuilt on every run. If you are looking for persistent
review history, check the configured
[parity store](../reference/run-configuration-and-artifacts.md#parity-store)
instead.

If you want to clear recorded run history, remove or replace the parity store
DuckDB file, which defaults to `data/parity_store/parity.duckdb`.

## Fix local Python tooling

If `.venv` commands fail or use the wrong interpreter, recreate the virtual
environment with Python 3.14 and reinstall the repository:

1. Create the virtual environment:

   ```bash
   python3.14 -m venv .venv
   ```

2. Install the repository with app and dev dependencies:

   ```bash
   .venv/bin/python -m pip install -e ".[app,dev]"
   ```

Use that environment for `pytest`, `ruff`, `mypy`, and `pyright`. For the
normal setup flow, see [Run the project locally](run-the-project-locally.md).

## Related information

- [Run the project locally](run-the-project-locally.md)
- [Run configuration and artifacts](../reference/run-configuration-and-artifacts.md)
- [Legacy backend image](../reference/legacy-backend-image.md)

[Back to documentation index](../index.md)
