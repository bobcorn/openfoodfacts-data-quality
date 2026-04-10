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
- `CHECK_PROFILE`: Named
  [check profile](../explanation/migrated-checks.md#check-profiles) from
  `config/check-profiles.toml`.
- `SOURCE_DATASET_PROFILE`: Named
  [dataset profile](#dataset-profiles) from `config/dataset-profiles.toml`.

## Review and governance settings

- `PARITY_STORE_PATH`: Path to the
  [parity store](#parity-store). When this environment variable is unset or
  blank, local commands use `data/parity_store/parity.duckdb`.
- `MIGRATION_INVENTORY_PATH`: Optional migration inventory artifact used to
  attach legacy family metadata to runs. When unset, local commands look for
  `artifacts/legacy_inventory/legacy_families.json` if it exists.
- `MIGRATION_ESTIMATION_SHEET_PATH`: Optional planning sheet joined onto the
  migration inventory. When unset, local commands look for
  `artifacts/legacy_inventory/estimation_sheet.csv` if it exists.

## Reference and legacy settings

- `REFERENCE_RESULT_CACHE_DIR`: Location of persisted reference results.
- `REFERENCE_RESULT_CACHE_SALT`: Manual cache invalidation salt.
- `LEGACY_BACKEND_FINGERPRINT`: Explicit backend fingerprint override.
- `LEGACY_SOURCE_ROOT`: Legacy source tree used by local fingerprinting and
  snippet extraction.

## Check profiles

`config/check-profiles.toml` defines named run profiles.

A [check profile](../explanation/migrated-checks.md#check-profiles) is a
selection preset for one run. It sets the active checks,
[context provider](../explanation/runtime-model.md#context-providers), and
[parity baselines](../explanation/reference-data-and-parity.md#parity-baselines).

The default profiles are:

- `full`
- `legacy`
- `source_products`
- `focused`

`full` runs every shipped check.

`legacy` keeps only checks that still compare against legacy behavior.

`source_products` keeps the `source_products` provider and the `legacy`
baseline, so it stays useful for provider-specific parity work.

`focused` uses the explicit include list from `config/check-profiles.toml`.

Profiles can also apply migration filters such as target implementation, size,
or risk when a migration catalog is configured. None of the shipped profiles
use those filters yet.

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

## Migration metadata inputs

`MIGRATION_INVENTORY_PATH` points to a legacy inventory JSON artifact.
`MIGRATION_ESTIMATION_SHEET_PATH` points to an optional CSV file that joins to
that inventory by `check_id`.

### Legacy inventory artifact

The loader reads these root fields:

| Field | Type | Notes |
| --- | --- | --- |
| `version` | integer | Keep `2`. |
| `families` | array of objects | Each object describes one legacy emission family. |

Each family record must use a unique `check_id`.

The loader reads these fields from each family record:

| Field | Type | Notes |
| --- | --- | --- |
| `check_id` | string | Canonical check id used by the selected checks. |
| `template_key` | string | Canonical template family key for the legacy emitted code template. |
| `code_templates` | string array | Concrete emitted code templates in the family. |
| `placeholder_names` | string array | Placeholder names used by the template family. |
| `placeholder_count` | integer | Number of placeholders in the family. |

The loader also reads these fields from the nested `features` object:

| Field | Type | Notes |
| --- | --- | --- |
| `has_loop` | boolean | Control flow flag for the family. |
| `has_branching` | boolean | Control flow flag for the family. |
| `has_arithmetic` | boolean | Expression flag for the family. |
| `helper_calls` | string array | Helper names observed in the source family. |
| `source_files_count` | integer | Number of source files in the family. |
| `source_subroutines_count` | integer | Number of source subroutines in the family. |
| `unsupported_data_quality_emission_count_total` | integer | Count of unsupported legacy emission sites in the family. |
| `line_span_max` | integer | Maximum line span for one source cluster in the family. |
| `statement_count_max` | integer | Maximum statement count for one source cluster in the family. |

The JSON artifact can contain extra keys. The loader ignores them.

### Estimation sheet

The estimation sheet is a CSV file joined by `check_id`.

The loader requires these columns:

| Column | Notes |
| --- | --- |
| `check_id` | Required. It must not be blank. It must be unique within the file and present in the inventory artifact. |
| `target_impl` | Required column. Allowed nonblank values: `dsl`, `python`. Blank is allowed. |
| `size` | Required column. Allowed nonblank values: `S`, `M`, `L`. Blank is allowed. |
| `risk` | Required column. Allowed nonblank values: `low`, `medium`, `high`. Blank is allowed. |
| `estimated_hours` | Required column. Free text. Blank is allowed. |
| `rationale` | Required column. Free text. Blank is allowed. |

The sample estimation sheet also includes source location columns from the
inventory export. The loader ignores those extra columns.

An assessment counts as complete when `target_impl`, `size`, `risk`, and
`rationale` are all populated. `estimated_hours` is stored when present, but it
does not decide whether the assessment is complete.

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
- `MIGRATION_INVENTORY_PATH`
- `MIGRATION_ESTIMATION_SHEET_PATH`

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
Refresh scripts write that manifest for generated sample snapshots, and the
runtime writes it when it has to hash a source snapshot file directly.

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
in `run.json`, such as batch telemetry and migration metadata.

Benchmark tooling also reads run-level preparation timings from the parity
store. Those timings separate:

- `prepare_run_seconds`
- `source_snapshot_id_seconds`
- `dataset_profile_load_seconds`
- `source_row_count_seconds`

The benchmark summary keeps those values outside the per-batch stage timings so
source setup costs do not get mixed with batch execution costs.

For local commands, the default store path is
`data/parity_store/parity.duckdb`.

When the store is enabled, the migration tooling persists:

- run configuration and status
- batch telemetry
- concrete mismatches
- dataset profile metadata
- active migration family metadata
- a serialized copy of `run.json`

If execution aborts before finalization, the store records the run as `failed`
or `incomplete`.

When a parity store is enabled, the report renderer reads the recorded store
snapshot first. This is how the HTML report receives store backed dataset and
migration metadata.

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
Only runs that need neither reference check contexts nor reference findings can
skip it entirely.

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
