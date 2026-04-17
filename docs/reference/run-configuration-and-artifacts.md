[Back to documentation index](../index.md)

# Run configuration and artifacts

Use this reference for migration settings and generated files. It also covers
the parity store and the cache.

## Source and execution settings

- `SOURCE_SNAPSHOT_PATH`: Full product
  [source snapshot](glossary.md#source-snapshot). This is required for local
  migration runs. It must point to a JSONL snapshot or a DuckDB snapshot with a
  `products` table.
- `SOURCE_SNAPSHOT_ID`: Optional explicit source snapshot id. When unset, the
  runtime uses a sidecar manifest or hashes the source snapshot file.
- `MIGRATION_BIND_HOST`: Host interface used by the repository Compose
  preview. The default is `127.0.0.1`.
- `MIGRATION_PORT`: Preview port used by the repository Compose preview. The
  Compose flow passes this value through to `PORT` inside the container.
- `PORT`: Preview port when you run `migration.cli` directly.
- `BATCH_SIZE`: Source batch size.
- `BATCH_WORKERS`: Concurrent batch workers used by the migration run loop.
- `LEGACY_BACKEND_WORKERS`: Persistent backend workers used when cache misses
  need backend materialization.
- `MISMATCH_EXAMPLES_LIMIT`: Retained
  [mismatch examples](report-artifacts.md#check-cards) for each side of each
  check.
- `LOG_INCLUDE_SOURCE`: When `true`, local application logs include logger
  name and line number.
- `CHECK_PROFILE`: Named
  [check profile](../explanation/migrated-checks.md#check-profiles) from
  `config/check-profiles.toml`.
- `SOURCE_DATASET_PROFILE`: Named
  [dataset profile](#dataset-profiles) from `config/dataset-profiles.toml`.

## Review settings

- `PARITY_STORE_PATH`: Path to the
  [parity store](#parity-store). When this environment variable is unset or
  blank, local commands use `data/parity_store/parity.duckdb`.

## Reference and legacy settings

- `REFERENCE_RESULT_CACHE_DIR`: Location of persisted reference results.
- `REFERENCE_RESULT_CACHE_SALT`: Manual cache invalidation salt.
- `LEGACY_BACKEND_FINGERPRINT`: Explicit backend fingerprint override.
- `LEGACY_SOURCE_ROOT`: Legacy source tree used by local fingerprinting and
  snippet extraction.

## Check profiles

`config/check-profiles.toml` defines named run profiles.

A [check profile](../explanation/migrated-checks.md#check-profiles) is a
selection preset for one run. It sets the active checks, the migration runtime
[context provider](../explanation/runtime-model.md#context-providers), and
[parity baselines](../explanation/reference-data-and-parity.md#parity-baselines).

Current profile validation accepts only
`check_context_provider = "enriched_snapshots"`.

The default profiles are:

- `full`
- `legacy`
- `focused`

`full` runs every shipped check.

`legacy` keeps only checks that still compare against legacy behavior.

`focused` uses the explicit include list from `config/check-profiles.toml`.

## Dataset profiles

`config/dataset-profiles.toml` defines named dataset presets for source selection.

A dataset profile controls which product documents from the source snapshot
enter one migration run. It does not change the
[check context provider](../explanation/runtime-model.md#context-provider-and-dataset-profile-are-different).

The default profiles are:

- `full`: Run the full source snapshot.
- `smoke`: Run a small deterministic sample.
- `validation`: Run a larger deterministic sample for parity checks.
- `benchmark_10k`: Run a deterministic 10k sample for benchmark loops.

The supported selection kinds are:

- `all_products`
- `stable_sample`
- `code_list`

`stable_sample` uses a deterministic seed and sample size. `code_list` can load
codes inline from the profile or from a referenced file path.

In the default config, `SOURCE_DATASET_PROFILE=smoke` uses the 50-row
deterministic sample. `validation` uses the 1000-row sample. `benchmark_10k`
uses the 10000-row sample.

When a run records data in the parity store, it also stores the resolved
selection fingerprint for that run.

## Compose wiring

The repository `compose.yaml` wires these settings into the local Docker flow:

The local stack name is `migration`, and Compose tags the built image as
`migration:local`.

- `SOURCE_SNAPSHOT_PATH`: selects the source snapshot file on the host that
  Compose mounts at `/work/source-snapshot` inside the container
- `MIGRATION_BIND_HOST`: controls the host interface used by the local
  preview
- `MIGRATION_PORT`: controls the preview port on the host and the value
  Compose passes to `PORT` inside the container
- `BATCH_SIZE`
- `BATCH_WORKERS`
- `LEGACY_BACKEND_WORKERS`
- `MISMATCH_EXAMPLES_LIMIT`
- `CHECK_PROFILE`
- `SOURCE_DATASET_PROFILE`
- `REFERENCE_RESULT_CACHE_DIR`
- `PARITY_STORE_PATH`

The local Docker flow requires `SOURCE_SNAPSHOT_PATH` through `.env` because
the mounted source path is explicit and the runtime no longer uses a bundled
source snapshot path.

`compose.yaml` mounts:

- the selected source snapshot
- `./data`
- `./artifacts`
- `./config`
- a named `reference_result_cache` volume at `/cache`

The repository Compose flow does not mount the source tree into the container.
Rebuild after code changes.

If a run needs reference results, values of `LEGACY_BACKEND_WORKERS` above
`BATCH_WORKERS` do not increase useful concurrency. The migration tooling logs a
warning in that case.

## Generated artifacts

The migration tooling writes run outputs under `artifacts/latest/`.

`artifacts/latest/` is recreated on every run. Treat it as ephemeral output,
not as persistent review history.

[Source snapshots](glossary.md#source-snapshot) can also include a sidecar
`<name>.<suffix>.snapshot.json` manifest with an explicit `source_snapshot_id`.
The repository does not ship those sidecars for the bundled example snapshots.
The runtime reads the sidecar when it exists and writes one when it has to hash
the source snapshot file directly.

Main generated files:

- `site/index.html`
- `site/report.html`
- `site/run.json`
- `site/snippets.json`
- `site/openfoodfacts-data-quality-json.zip`
- `legacy-backend-stderr.log` when the backend worker starts

`run.json` and `snippets.json` include root `kind` and `schema_version`
metadata.

## Parity store

The parity store is a DuckDB review store for completed
migration runs.

It stores review history across runs and report data that is not embedded
in `run.json`, such as batch telemetry and dataset profile metadata.

Benchmark tooling also reads run preparation timings from the parity
store. Those timings separate:

- `prepare_run_seconds`
- `source_snapshot_id_seconds`
- `dataset_profile_load_seconds`
- `source_row_count_seconds`

The benchmark summary keeps those values outside the batch stage timings so
source setup costs do not get mixed with batch execution costs.

For local commands, the default store path is
`data/parity_store/parity.duckdb`.

When the store is enabled, the migration tooling persists:

- run configuration and status
- batch telemetry
- concrete mismatches
- dataset profile metadata
- a serialized copy of `run.json`

If execution aborts before finalization, the store records the run as `failed`
or `incomplete`.

When a parity store is enabled, the report renderer reads the recorded store
snapshot first instead of relying on the run result kept in memory.

## Reference result cache

The reference result cache is a derived optimization artifact. Each cache
database also writes a readable `.meta.json` sidecar with the
[source snapshot](glossary.md#source-snapshot) id, cache key, schema version,
and backend fingerprints used for that cache namespace.

By default, `migration.cli` stores that cache under `data/reference_result_cache/`.
The default Docker flow overrides the cache directory to `/cache`, which is
backed by the named `reference_result_cache` volume in `compose.yaml`.

Deleting the cache forces fresh reference materialization on the next run. If
cache metadata no longer matches the contracts, startup fails instead
of reusing stale data.

The cache is used only for runs that need
[reference results](../explanation/reference-data-and-parity.md#why-the-reference-path-exists).
In principle, only runs that need neither reference check contexts nor
reference findings can skip it entirely. Current migration profile validation
does not create such runs because it fixes `check_context_provider` to
`enriched_snapshots`.

Its cache key depends on:

- the source snapshot id
- the [backend fingerprint](legacy-backend-image.md#cache-fingerprint)
- the Python reference execution contract and backend input projection
- the optional manual salt

In Docker runs, the backend fingerprint comes from the pinned legacy backend
image. See [Legacy backend image](legacy-backend-image.md).

## See also

- [About migration runs](../explanation/migration-runs.md)
- [Report artifacts](report-artifacts.md)
- [Troubleshoot local runs](../how-to/troubleshoot-local-runs.md)

[Back to documentation index](../index.md)
