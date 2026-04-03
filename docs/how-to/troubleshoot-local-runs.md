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
`examples/data/products.duckdb`.

Local runtime runs do not use a bundled DuckDB path. If
`SOURCE_SNAPSHOT_PATH` is unset or blank, startup fails and asks you to set it
explicitly. Use the
[demo image](../../README.md#run-the-demo) when you want the bundled sample
without local configuration.

## Fix unknown profile names

Check these settings in `.env`:

- `CHECK_PROFILE`
- `SOURCE_DATASET_PROFILE`

Each one must match a profile defined in `config/check-profiles.toml` or
`config/dataset-profiles.toml`.

If a check profile uses migration filters, make sure the run also has valid
`MIGRATION_INVENTORY_PATH` and, when needed,
`MIGRATION_ESTIMATION_SHEET_PATH` inputs.

## Fix expected differences registry errors

If startup fails while loading the expected differences registry:

- check that `PARITY_EXPECTED_DIFFERENCES_PATH` points to an existing file
- keep `schema_version = 1`
- use unique rule ids
- make sure rules do not overlap on the same concrete mismatch

Set `PARITY_EXPECTED_DIFFERENCES_PATH` to a blank value if you want to disable
automatic registry lookup for one run.

For the exact TOML contract, see
[Expected differences registry](../reference/run-configuration-and-artifacts.md#expected-differences-registry).

## Fix migration metadata input errors

If startup fails while loading migration metadata:

- check that `MIGRATION_INVENTORY_PATH` points to an existing JSON artifact
- keep the legacy inventory artifact on version `2`
- check that the estimation sheet has the required columns
- make sure every estimation-sheet `check_id` exists in the inventory artifact

Local commands look for the default artifact paths when those files
exist. Set the corresponding environment variable to a blank value if you want
to disable that lookup for one run.

For the exact JSON and CSV contracts, see
[Migration metadata inputs](../reference/run-configuration-and-artifacts.md#migration-metadata-inputs).

## Fix DuckDB schema mismatches

The source reader validates the `products` table against the explicit
[RawProductRow](../reference/data-contracts.md#rawproductrow) contract.

If you see errors about missing columns, your snapshot does not match
`openfoodfacts_data_quality.raw_products.RAW_INPUT_COLUMNS`. Use the tracked
sample, regenerate a compatible sample, or align the snapshot schema before you
run the application.

## Fix a missing report at the preview URL

`http://localhost:8000` is only the default preview URL.

If the report does not open there, check `PORT` in `.env`. In the default
Docker flow, that setting controls the published port on the host, so the preview
URL is `http://localhost:<PORT>`.

If you changed `PORT`, open the report on that port instead of forcing `8000`.
If you want the default URL back, restore `PORT=8000` in `.env` and restart the
Docker flow so Compose recreates the published port mapping:

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

Compared runs and enriched application runs depend on the
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
application flow still assumes that environment is available when reference
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
