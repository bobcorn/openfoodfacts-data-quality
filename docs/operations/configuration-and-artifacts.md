# Configuration and Artifacts

[Documentation](../index.md) / [Operations](index.md) / Configuration and Artifacts

Runtime configuration for one application run and the artifacts it produces. For the full execution sequence, see [Application Run Flow](../architecture/application-run-flow.md).

## Runtime Configuration

The main runtime inputs are:

- `DATABASE_PATH`
  Source DuckDB snapshot.
- `BATCH_SIZE`
  Source batch size.
- `BATCH_WORKERS`
  Concurrent batch workers used by the application run loop.
- `LEGACY_BACKEND_WORKERS`
  Number of persistent backend workers used when cache misses need backend materialization.
- `MISMATCH_EXAMPLES_LIMIT`
  Retained mismatch examples per side and check.
- `CHECK_PROFILE`
  Named check subset from `config/check-profiles.toml`.

## Advanced Configuration

- `REFERENCE_RESULT_CACHE_DIR`
  Where persisted reference results live.
- `REFERENCE_RESULT_CACHE_SALT`
  Manual cache invalidation salt.
- `LEGACY_BACKEND_FINGERPRINT`
  Explicit backend fingerprint override.
- `LEGACY_SOURCE_ROOT`
  Legacy source tree used by local fingerprinting and snippet extraction.

## Profiles

`config/check-profiles.toml` defines named run profiles.

A profile decides:

- which checks are active
- which input surface the run uses
- which parity baselines are in scope

The shipped profiles cover both legacy comparison and runtime only execution where the selected checks support those baselines:

- `full`
- `raw_products`
- `focused`

## Artifacts

The application writes run outputs under `artifacts/latest/`.

Source DuckDB snapshots can also carry a sidecar `<name>.duckdb.snapshot.json` manifest with an explicit `source_snapshot_id`. The refresh scripts write that manifest automatically for generated sample DuckDB files, and the runtime writes it the first time it falls back to hashing a DuckDB file directly.

Main generated files:

- `site/index.html`
- `site/report.html`
- `site/run.json`
- `site/snippets.json`
- `site/openfoodfacts-data-quality-json.zip`
- `legacy-backend-stderr.log` when the backend worker starts
- `data/reference_result_cache/*.meta.json` beside cache DB files when reference cache is used

## Usage

- Use the HTML report for human review. See [Reading The Report](../getting-started/reading-the-report.md).
- Use `run.json` for structured run data.
- Use `snippets.json` for structured implementation and legacy source provenance.
- Use the ZIP archive when you want to download the JSON outputs as one bundle.
- Use the legacy backend stderr log when present and backend execution fails or behaves unexpectedly.

`run.json` includes compared and runtime only counts in the same run summary when the active profile selects both.
Both `run.json` and `snippets.json` include root `kind` and `schema_version` metadata.

## Reference Cache

The reference result cache is a derived optimization artifact. It is safe to delete when you want a fresh reference materialization.
Each cache database also writes a readable `.meta.json` sidecar with the source snapshot id, cache key, schema version, and backend fingerprints used for that cache namespace.
If a cache file no longer matches the current runtime contract, startup fails with field level mismatch details instead of silently reusing stale data.

The cache is only used for runs that require reference results. Raw runtime only runs can skip it entirely.

Its cache key depends on:

- the source snapshot id
- the backend fingerprint
- the Python reference execution contract and backend input projection
- the optional manual salt

In Docker runs, the backend fingerprint comes from the pinned legacy backend image. See [Legacy Backend Image](legacy-backend-image.md).

## Next

- [Reading The Report](../getting-started/reading-the-report.md)
- [Local Development](../guides/local-development.md)
- [Legacy Backend Image](legacy-backend-image.md)
- [Application Run Flow](../architecture/application-run-flow.md)
