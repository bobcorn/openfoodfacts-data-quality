# Run Configuration and Artifacts

[Back to documentation](../index.md)

Look here for application settings, check profiles, and generated files.

## Set run configuration

- `DATABASE_PATH`
  DuckDB [source snapshot](glossary.md#source-snapshot)
- `BATCH_SIZE`
  source batch size
- `BATCH_WORKERS`
  concurrent batch workers used by the application run loop
- `LEGACY_BACKEND_WORKERS`
  persistent backend workers used when cache misses need [legacy backend](../concepts/how-an-application-run-works.md#legacy-backend) materialization
- `MISMATCH_EXAMPLES_LIMIT`
  retained [mismatch examples](report-artifacts.md#check-cards) per side and check
- `CHECK_PROFILE`
  named [check profile](../concepts/check-model.md#check-profiles) from `config/check-profiles.toml`

## Use advanced settings

- `REFERENCE_RESULT_CACHE_DIR`
  location of persisted reference results
- `REFERENCE_RESULT_CACHE_SALT`
  manual cache invalidation salt
- `LEGACY_BACKEND_FINGERPRINT`
  explicit backend fingerprint override
- `LEGACY_SOURCE_ROOT`
  legacy source tree used by local fingerprinting and snippet extraction

## Choose a run profile

`config/check-profiles.toml` defines named run profiles.

A [check profile](../concepts/check-model.md#check-profiles) is a selection preset for one run. It sets the active checks, input surface, and parity baselines.

The shipped profiles are `full`, `raw_products`, and `focused`.

## Find generated artifacts

The application writes run outputs under `artifacts/latest/`.

[Source snapshots](glossary.md#source-snapshot) can also carry a sidecar `<name>.duckdb.snapshot.json` manifest with an explicit `source_snapshot_id`. Refresh scripts write that manifest for generated sample DuckDB files, and the runtime writes it when it has to hash a DuckDB file directly.

Main generated files:

- `site/index.html`
- `site/report.html`
- `site/run.json`
- `site/snippets.json`
- `site/openfoodfacts-data-quality-json.zip`
- `legacy-backend-stderr.log` when the backend worker starts
- `data/reference_result_cache/*.meta.json` beside cache DB files when reference cache is used

## Use each artifact

- Use the HTML report for human review. See [Review a Run Report](../how-to/review-a-run-report.md).
- Use `run.json` for structured run data.
- Use `snippets.json` for structured implementation and legacy source provenance.
- Use the ZIP archive when you want the JSON outputs in one bundle.
- Use the legacy backend stderr log when backend execution fails or behaves unexpectedly.

For detail about report fields, see [Report Artifacts](report-artifacts.md).

`run.json` combines compared counts and counts for checks that run without comparison when the active [check profile](../concepts/check-model.md#check-profiles) selects both.

Both `run.json` and `snippets.json` include root `kind` and `schema_version` metadata.

## Reference Cache

The reference result cache is a derived optimization artifact. Each cache database also writes a readable `.meta.json` sidecar with the source snapshot id, cache key, schema version, and backend fingerprints used for that cache namespace.

> Warning
> Deleting the cache forces fresh reference materialization on the next run. If cache metadata no longer matches the current contracts, startup fails instead of reusing stale data.

The cache is used only for runs that need [reference results](../concepts/reference-and-parity.md#reference-path). Raw runs that need no comparison can skip it entirely.

Its cache key depends on:

- the source snapshot id
- the backend fingerprint
- the Python reference execution contract and backend input projection
- the optional manual salt

In Docker runs, the backend fingerprint comes from the pinned legacy backend image. See [Legacy Backend Image](legacy-backend-image.md).

[Back to documentation](../index.md)
