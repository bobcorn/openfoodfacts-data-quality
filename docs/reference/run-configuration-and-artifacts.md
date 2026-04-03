[Back to documentation index](../index.md)

# Run configuration and artifacts

This page lists application settings, check profile sources, generated files,
and cache details.

## Core settings

- `DATABASE_PATH`: DuckDB [source snapshot](glossary.md#source-snapshot). This
  is required for local runtime runs.
- `PORT`: Preview port when you run `app.main` directly. In the shipped Docker
  flow, this value controls the host-side published port.
- `BATCH_SIZE`: Source batch size
- `BATCH_WORKERS`: Concurrent batch workers used by the application run loop
- `LEGACY_BACKEND_WORKERS`: Persistent backend workers used when cache misses
  need live materialization
- `MISMATCH_EXAMPLES_LIMIT`: Retained
  [mismatch examples](report-artifacts.md#check-cards) per side and check
- `CHECK_PROFILE`: Named
  [check profile](../explanation/migrated-checks.md#check-profiles) from
  `config/check-profiles.toml`

## Advanced settings

- `REFERENCE_RESULT_CACHE_DIR`: Location of persisted reference results
- `REFERENCE_RESULT_CACHE_SALT`: Manual cache invalidation salt
- `LEGACY_BACKEND_FINGERPRINT`: Explicit backend fingerprint override
- `LEGACY_SOURCE_ROOT`: Legacy source tree used by local fingerprinting and
  snippet extraction

## Check profiles

`config/check-profiles.toml` defines named run profiles.

A [check profile](../explanation/migrated-checks.md#check-profiles) is a
selection preset for one run. It sets the active checks,
[input surface](../explanation/runtime-model.md#input-surfaces), and
[parity baselines](../explanation/reference-data-and-parity.md#parity-baselines).

The shipped profiles are `full`, `raw_products`, and `focused`.

## Compose wiring

The shipped `compose.yaml` wires these settings into the local Docker flow:

- `DATABASE_PATH`: selects the host-side DuckDB file that Compose mounts at
  `/work/products.duckdb` inside the container
- `PORT`: controls the host-side published port
- `BATCH_SIZE`
- `BATCH_WORKERS`
- `LEGACY_BACKEND_WORKERS`
- `MISMATCH_EXAMPLES_LIMIT`
- `CHECK_PROFILE`
- `REFERENCE_RESULT_CACHE_DIR`

The local Docker flow requires `DATABASE_PATH` through `.env` because the bind
mount source is explicit and the runtime no longer falls back to a bundled
DuckDB path.

When a run needs reference results, values of `LEGACY_BACKEND_WORKERS` above
`BATCH_WORKERS` do not increase useful concurrency. The application logs a
warning in that case.

## Generated artifacts

The application writes run outputs under `artifacts/latest/`.

[Source snapshots](glossary.md#source-snapshot) can also carry a sidecar
`<name>.duckdb.snapshot.json` manifest with an explicit `source_snapshot_id`.
Refresh scripts write that manifest for generated sample DuckDB files, and the
runtime writes it when it has to hash a DuckDB file directly.

Main generated files:

- `site/index.html`
- `site/report.html`
- `site/run.json`
- `site/snippets.json`
- `site/openfoodfacts-data-quality-json.zip`
- `legacy-backend-stderr.log` when the backend worker starts

## Artifact usage

- Use the HTML report for human review. See
  [Review a run report](../how-to/review-a-run-report.md).
- Use `run.json` for structured run data.
- Use `snippets.json` for structured implementation and legacy source
  provenance.
- Use the ZIP archive when you want the JSON outputs in one bundle.
- Use the legacy backend stderr log when backend execution fails or behaves
  unexpectedly.

For detail about report fields, see [Report artifacts](report-artifacts.md).

`run.json` combines compared counts and counts for checks that run without
comparison when the active
[check profile](../explanation/migrated-checks.md#check-profiles) selects both.

Both `run.json` and `snippets.json` include root `kind` and `schema_version`
metadata.

## Reference result cache

The reference result cache is a derived optimization artifact. Each cache
database also writes a readable `.meta.json` sidecar with the
[source snapshot](glossary.md#source-snapshot) id, cache key, schema version,
and backend fingerprints used for that cache namespace.

By default, `app.main` stores that cache under `data/reference_result_cache/`.
The shipped Docker flow overrides the cache directory to `/cache`, which is
backed by the named `reference_result_cache` volume in `compose.yaml`.

**Warning:** Deleting the cache forces fresh reference materialization on the
next run. If cache metadata no longer matches the current contracts, startup
fails instead of reusing stale data.

The cache is used only for runs that need
[reference results](../explanation/reference-data-and-parity.md#why-the-reference-path-exists).
Raw runs that need no comparison can skip it entirely.

Its cache key depends on:

- the source snapshot id
- the [backend fingerprint](legacy-backend-image.md#cache-fingerprint)
- the Python reference execution contract and backend input projection
- the optional manual salt

In Docker runs, the backend fingerprint comes from the pinned legacy backend
image. See [Legacy backend image](legacy-backend-image.md).

## See also

- [About application runs](../explanation/application-runs.md)
- [Report artifacts](report-artifacts.md)
- [Troubleshoot local runs](../how-to/troubleshoot-local-runs.md)

[Back to documentation index](../index.md)
