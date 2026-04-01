# Configuration and Artifacts

[Documentation](../index.md) / [Operations](index.md) / Configuration and Artifacts

Runtime configuration for one application run and the artifacts it produces.

## Runtime Configuration

The main runtime inputs are:

- `DATABASE_PATH`
  Source DuckDB snapshot.
- `BATCH_SIZE`
  Source batch size.
- `LEGACY_BACKEND_WORKERS`
  Number of persistent backend workers.
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

The shipped profiles focus on migration runs compared against legacy behavior:

- `full`
- `raw_products`
- `focused`

## Artifacts

The application writes run outputs under `artifacts/latest/`.

Main generated files:

- `site/index.html`
- `site/report.html`
- `site/parity.json`
- `site/snippets.json`
- `site/openfoodfacts-data-quality-json.zip`
- `legacy-backend-stderr.log`

## Artifact Use

- Use the HTML report for human review.
- Use `parity.json` for structured parity data.
- Use `snippets.json` for structured implementation provenance.
- Use the ZIP archive when you want to download the JSON outputs as one bundle.
- Use the legacy backend stderr log when backend execution fails or behaves unexpectedly.

## Reference Cache

The reference result cache is a derived optimization artifact. It is safe to delete when you want a fresh reference materialization.

Its cache key depends on:

- the source snapshot id
- the backend fingerprint
- the Python reference execution contract
- the optional manual salt

In Docker runs, the backend fingerprint comes from the pinned legacy backend image. See [Legacy Backend Image](legacy-backend-image.md).

## Next Reads

- [Reading The Report](../getting-started/reading-the-report.md)
- [Local Development](../guides/local-development.md)
- [Legacy Backend Image](legacy-backend-image.md)
- [Parity Pipeline](../architecture/parity-pipeline.md)
